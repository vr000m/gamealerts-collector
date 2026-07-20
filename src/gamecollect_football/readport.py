"""Football-profile adapter satisfying :class:`gamecollect.readport.MatchReadPort`.

Lives in the pack (not core) per the pack -> core dependency direction: core
never imports ``gamecollect_football``. Implements the generic Protocol over
the collector's own reader helpers (``gamecollect.db.reader``) plus this
pack's additive side tables (``football_venue``, ``football_roster``,
``football_lineups`` — see :mod:`gamecollect_football.pack`) and the
consumer-owned ``commentary`` table. Football-specific values (event type
strings, table/column names) live here, behind the sport-neutral Protocol
signatures — see ``readport.py`` (core) for the type contract this class
must satisfy.

Returns plain dicts/lists, the shapes ``MatchContext``/Q&A tools consume
directly — never ``gamecollect_football.operations`` dataclasses, which serve
a different (typed, football-specific) read op surface.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from gamecollect.client import GOAL_FAMILY_EVENT_TYPES, normalize_legacy_payload
from gamecollect.db import reader
from gamecollect.fold import fold
from gamecollect_football.operations import resolve_source
from gamecollect_football.reconcile import canonical_display_name, canonical_team_name

__all__ = ["FootballReadPort"]

# The football taxonomy's phase-marker event types (mirrors
# gamecollect_football.pack's ``compaction_boundaries``) — the source of the
# generic ``phase`` field on events/state. Football-specific VALUES, kept
# local to this adapter; the core Protocol's ``phase`` field stays untyped.
_PHASE_MARKER_TYPES = frozenset({"kickoff", "half_time", "full_time"})


class FootballReadPort:
    """Concrete :class:`~gamecollect.readport.MatchReadPort` over a football-pack DB.

    ``conn`` may be any open connection carrying the collector's core schema
    plus this pack's ``football_*`` side tables (a :func:`gamecollect.db.reader.open_reader`
    connection, or a writer's own connection in tests).
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def list_matches(
        self, *, source: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        rows = reader.list_matches(self._conn, source=source, status=status)
        return [self._project_summary(r) for r in rows]

    def _project_summary(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = reader.decode_payload(row.get("payload"))
        return {
            "match_id": row["match_id"],
            "source": row.get("source"),
            "status": row.get("status"),
            "kickoff_utc": row.get("kickoff_utc"),
            "participants": self._project_participants(row, payload),
        }

    def _project_participants(
        self, row: dict[str, Any], payload: dict[str, Any]
    ) -> list[dict[str, Any]]:
        participants: list[dict[str, Any]] = []
        home_name = payload.get("home_team")
        away_name = payload.get("away_team")
        # Defensively re-canonicalize the DISPLAY name on read, mirroring the
        # legacy-tolerance already in _lineup_rows/_roster_rows: every current
        # write path stamps canonical_display_name at the seam, but a row
        # written before that (or by legacy code) can carry a raw provider
        # alias (e.g. "Türkiye"). canonical_display_name (alias map, case
        # preserved) — NOT canonical_team_name (fold to a lowercase comparison
        # key) — keeps the participant name human-readable ("Turkey").
        if home_name:
            participants.append(
                {
                    "name": canonical_display_name(home_name),
                    "side": "home",
                    "score": row.get("score_home"),
                }
            )
        if away_name:
            participants.append(
                {
                    "name": canonical_display_name(away_name),
                    "side": "away",
                    "score": row.get("score_away"),
                }
            )
        return participants

    def latest_state(self, match_id: str) -> dict[str, Any] | None:
        row = reader.get_state(self._conn, match_id)
        if row is None:
            return None
        payload = reader.decode_payload(row.get("payload"))
        return {
            "match_id": row["match_id"],
            "status": row.get("status"),
            "minute": row.get("minute"),
            "period": row.get("period"),
            "display_clock": row.get("display_clock"),
            "kickoff_utc": row.get("kickoff_utc"),
            "phase": self._current_phase(match_id, row.get("status")),
            "participants": self._project_participants(row, payload),
            "extra": {
                "result_type": payload.get("result_type"),
                "stadium": payload.get("stadium"),
                "city": payload.get("city"),
            },
        }

    def _current_phase(self, match_id: str, status: Any) -> str | None:
        # latest_state() is a per-poll hot path; a targeted "last matching
        # event" query (get_latest_event_of_type, ORDER BY seq DESC LIMIT 1)
        # avoids fetching and decoding every event for the match just to find
        # the last phase marker.
        row = reader.get_latest_event_of_type(self._conn, match_id, _PHASE_MARKER_TYPES)
        if row is not None:
            return row["type"]
        # No phase-marker event yet (e.g. pre-kickoff): fall back to the
        # lowercased match `status` string. NOT itself a phase-marker type —
        # a consumer must not treat this fallback value as interchangeable
        # with a real `_PHASE_MARKER_TYPES` member just because it shares the
        # `phase` field name.
        return str(status).lower() if status else None

    def _project_event(self, row: dict[str, Any]) -> dict[str, Any]:
        event_type = row["type"]
        # Reuse the core client's legacy-payload normalizer (pack -> core import
        # is allowed) instead of a third hand-rolled goal-family copy: rows
        # written before the goal-event participant-contract rename
        # (docs/dev_plans/20260710-feature-goal-event-participants.md) stored
        # the scorer under "player"; normalize_legacy_payload rewrites it to
        # "scorer" exactly as gamecollect.engine._stored_events does. The
        # goal-family set is imported from the same module, so this adapter
        # cannot drift from the core taxonomy literal.
        payload = normalize_legacy_payload(event_type, reader.decode_payload(row.get("payload")))
        participant_key = "scorer" if event_type in GOAL_FAMILY_EVENT_TYPES else "player"
        player = payload.get(participant_key)
        return {
            "seq": row["seq"],
            "minute": row.get("minute"),
            "period": row.get("period"),
            "type": event_type,
            "phase": event_type if event_type in _PHASE_MARKER_TYPES else None,
            "team": payload.get("team"),
            "player": player,
            "assist": payload.get("assist"),
            "detail": row.get("detail"),
            "importance": row.get("importance"),
        }

    def events_for_match(self, match_id: str, since_seq: int = -1) -> list[dict[str, Any]]:
        rows = reader.get_events_since(self._conn, match_id, since_seq)
        return [self._project_event(r) for r in rows]

    def _lineup_rows(self, match_id: str, participant: str | None = None) -> list[dict[str, Any]]:
        source = resolve_source(self._conn, match_id)
        if source is None:
            return []
        rows = reader.get_side_table_rows(
            self._conn,
            "football_lineups",
            source,
            match_id,
            order_by="team, formation_place IS NULL, formation_place, athlete_id",
        )
        if participant is not None:
            # Compare through canonical_team_name (fold + alias) rather than
            # exact string equality: rows written before write-side team-name
            # canonicalization (or by legacy code) can carry a raw provider
            # name (e.g. "Türkiye") while ``participant`` is always the
            # canonical name (e.g. "Turkey") from latest_state(). An exact
            # match would silently drop legacy rows.
            target = canonical_team_name(participant)
            rows = [r for r in rows if canonical_team_name(r.get("team") or "") == target]
        return rows

    def _project_lineup(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "participant": row.get("team"),
            "player": row.get("display_name"),
            "player_id": row.get("athlete_id"),
            "position": row.get("position"),
            "jersey": row.get("jersey"),
            "starter": bool(row.get("starter")),
            "subbed_in": bool(row.get("subbed_in")),
            "subbed_out": bool(row.get("subbed_out")),
            "extra": {
                "home_away": row.get("home_away"),
                "formation": row.get("formation"),
            },
        }

    def lineup_for_match(
        self, match_id: str, participant: str | None = None
    ) -> list[dict[str, Any]]:
        return [self._project_lineup(r) for r in self._lineup_rows(match_id, participant)]

    def lineup_for_match_live(
        self, match_id: str, participant: str | None = None
    ) -> list[dict[str, Any]]:
        # No caching layer exists in this adapter, so "live" is identical to
        # the plain read — see the Protocol docstring for the contract.
        return self.lineup_for_match(match_id, participant)

    def venue_for_match(self, match_id: str) -> dict[str, Any] | None:
        source = resolve_source(self._conn, match_id)
        if source is None:
            return None
        row = reader.get_side_table_row(self._conn, "football_venue", source, match_id)
        if row is None:
            return None
        return {"stadium": row.get("stadium"), "city": row.get("city")}

    def _roster_rows(self, participant: str) -> list[dict[str, Any]]:
        # football_roster.team is always written through canonical_display_name
        # (pack.py's _persist_roster_for_team), so a raw provider alias (e.g.
        # "Türkiye") passed in here must be canonicalized before matching.
        # Compare through canonical_team_name (fold + alias), same as
        # _lineup_rows above, rather than canonical_display_name (alias-dict
        # lookup only, no casefold/diacritic-fold): a participant string that
        # folds to the same team but isn't byte-identical to a TEAM_ALIASES
        # key (e.g. a casing or diacritic variant not itself an alias key)
        # would still miss an exact-match query against the display name.
        target = canonical_team_name(participant)
        rows = reader.get_all_team_side_table_rows(
            self._conn,
            "football_roster",
            order_by="number",
        )
        return [r for r in rows if canonical_team_name(r.get("team") or "") == target]

    def roster_for_team(self, participant: str) -> list[dict[str, Any]]:
        return [
            {
                "player": row.get("player_name"),
                "player_id": None,
                "position": row.get("position"),
                "extra": {
                    "number": row.get("number"),
                    "fifa_code": row.get("fifa_code"),
                    "group_name": row.get("group_name"),
                    "date_of_birth": row.get("date_of_birth"),
                },
            }
            for row in self._roster_rows(participant)
        ]

    def roster_contains(self, participant: str, player_name: str) -> bool:
        target = fold(player_name)
        return any(row.get("name_folded") == target for row in self._roster_rows(participant))

    def player_in_lineup(self, match_id: str, player_name: str) -> dict[str, Any] | None:
        target = fold(player_name)
        for row in self._lineup_rows(match_id):
            if row.get("name_folded") == target:
                return self._project_lineup(row)
        return None

    def lineup_team_announced(self, match_id: str, participant: str) -> bool:
        return any(row.get("starter") for row in self._lineup_rows(match_id, participant))

    def recent_commentary(self, match_id: str, limit: int = 20) -> list[dict[str, Any]]:
        # The ``commentary`` table is consumer-owned (DESIGN.md §3): the
        # collector never writes it and core deliberately does not know its
        # name or column vocabulary. This adapter — which DOES own that
        # knowledge — supplies the table name, the newest-first ordering, and
        # the limit to the generic match-keyed reader, and tolerates the table
        # being absent on a collector-first boot (returns []).
        rows = reader.get_match_side_table_rows(
            self._conn,
            "commentary",
            match_id,
            order_by="created_at DESC, id DESC",
            limit=limit,
            tolerate_missing_table=True,
        )
        return [
            {
                "text": row.get("text"),
                "spoken": bool(row.get("spoken")),
                "created_at": row.get("created_at"),
            }
            for row in rows
        ]
