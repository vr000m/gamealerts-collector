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

# Goal-family event types: payload carries the scorer under "scorer", every
# other event type carries the actor under "player" (schema.sql events
# comment).
_GOAL_FAMILY_TYPES = frozenset({"goal", "own_goal"})


class FootballReadPort:
    """Concrete :class:`~gamecollect.readport.MatchReadPort` over a football-pack DB.

    ``conn`` may be any open connection carrying the collector's core schema
    plus this pack's ``football_*`` side tables (a :func:`gamecollect.db.reader.open_reader`
    connection, or a writer's own connection in tests).
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def latest_state(self, match_id: str) -> dict[str, Any] | None:
        row = reader.get_state(self._conn, match_id)
        if row is None:
            return None
        payload = reader.decode_payload(row.get("payload"))
        participants: list[dict[str, Any]] = []
        home_name = payload.get("home_team")
        away_name = payload.get("away_team")
        if home_name:
            participants.append({"name": home_name, "side": "home", "score": row.get("score_home")})
        if away_name:
            participants.append({"name": away_name, "side": "away", "score": row.get("score_away")})
        return {
            "match_id": row["match_id"],
            "status": row.get("status"),
            "minute": row.get("minute"),
            "period": row.get("period"),
            "display_clock": row.get("display_clock"),
            "kickoff_utc": row.get("kickoff_utc"),
            "phase": self._current_phase(match_id, row.get("status")),
            "participants": participants,
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
        return str(status).lower() if status else None

    def _project_event(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = reader.decode_payload(row.get("payload"))
        event_type = row["type"]
        if event_type in _GOAL_FAMILY_TYPES:
            # Rows written before the goal-event participant-contract rename
            # (docs/dev_plans/20260710-feature-goal-event-participants.md)
            # stored the scorer under "player" instead of "scorer". Mirror
            # gamecollect.client._normalize_legacy_payload's fallback so
            # historical rows still surface a scorer name here.
            player = payload.get("scorer") or payload.get("player")
        else:
            player = payload.get("player")
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
        # "Türkiye") passed in here must be canonicalized the same way before
        # the exact-match query. Round 1 fixed this exact bug class for
        # _lineup_rows but missed this sibling method, silently dropping rows
        # for a caller using an alias name.
        return reader.get_team_side_table_rows(
            self._conn,
            "football_roster",
            canonical_display_name(participant),
            order_by="number",
        )

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
        rows = reader.get_recent_commentary(self._conn, match_id, limit)
        return [
            {
                "text": row.get("text"),
                "spoken": bool(row.get("spoken")),
                "created_at": row.get("created_at"),
            }
            for row in rows
        ]
