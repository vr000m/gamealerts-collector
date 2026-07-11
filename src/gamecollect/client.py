"""Typed client library — the canonical read surface over the shared database.

DESIGN.md §6 pins the client library as canonical: the CLI is a thin ``main()``
over it, and the ``tools --json`` manifest is generated from the same operation
registry these functions register into (:mod:`gamecollect.registry`), so the
CLI cannot drift from the library by construction.

The four **core** read operations live here and wrap the untyped
:mod:`gamecollect.db.reader` rows in frozen dataclasses (``payload`` JSON
decoded to a Python object). They are sync, read-only, and safe to call while
collector daemons write (WAL): open the connection with
:func:`gamecollect.db.reader.open_reader`.

Reader contract (unchanged here): ``match_id`` is globally unambiguous, so
match-scoped ops (:func:`get_state`, :func:`get_events_since`) take only
``match_id`` — the reader resolves the partition internally. :func:`get_standings`
is keyed ``(source, group_key, entity_id)`` and has no ``match_id`` to resolve a
source from, so it takes ``source`` explicitly; :func:`list_matches` accepts an
optional ``source`` filter.

Pack-owned side tables (football squads/stats) are read by **pack-contributed**
operations implemented in ``gamecollect_football`` and registered at pack load —
this module never imports a pack (dependency direction is pack → core only).
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from gamecollect.db import reader

__all__ = [
    "MatchState",
    "Event",
    "Standing",
    "list_matches",
    "get_state",
    "get_events_since",
    "get_standings",
]


def _decode_payload(raw: Any) -> Any:
    """Decode a stored ``payload`` JSON string to a Python object.

    The reader hands back ``payload`` as raw text (or ``None``); the writer
    always JSON-encodes dict/list payloads, so decoding here yields the object
    the writer stored. A value that is already a Python object (some callers
    pass a live connection whose rows were never round-tripped through SQLite)
    passes through; an undecodable string is returned unchanged rather than
    dropped.
    """
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


# Event types whose participant lands under ``payload.scorer`` rather than
# ``payload.player`` — see ``gamecollect.engine._GOAL_FAMILY_EVENT_TYPES``.
# This is a second, deliberate copy of the same taxonomy literal (matching
# that constant's own "deliberate boundary erosion" precedent) rather than
# importing ``gamecollect.engine`` — a daemon module pulling in ``signal``,
# ``threading``, and provider/pack-loading machinery — into this lightweight,
# sync, read-only client. Kept from drifting by
# ``test_client_goal_family_matches_engine`` in ``tests/test_engine.py``.
_GOAL_FAMILY_EVENT_TYPES = frozenset({"goal", "own_goal"})


def _normalize_legacy_payload(event_type: str, payload: Any) -> Any:
    """Rewrite a legacy goal-family ``payload.player`` key to ``payload.scorer``.

    Rows written before the goal-event participant-contract rename
    (``docs/dev_plans/20260710-feature-goal-event-participants.md``) stored
    the scorer under ``payload.player``. ``engine._stored_events`` normalizes
    this internally for engine reconciliation, but that fallback never
    reaches the public read path — ``get_events_since``/``events --json`` are
    the only other place a stored row's payload is decoded, and until this
    normalization, a pre-rename row was exposed to every external consumer in
    the old shape forever, with no way to tell it apart from a genuinely
    keyless row. This closes that gap without a schema migration or a
    contract-version bump: it is a read-time-only rewrite of the decoded
    dict, exactly mirroring ``_stored_events``'s existing fallback.
    """
    if (
        event_type in _GOAL_FAMILY_EVENT_TYPES
        and isinstance(payload, dict)
        and "scorer" not in payload
        and "player" in payload
    ):
        payload = dict(payload)
        payload["scorer"] = payload.pop("player")
    return payload


@dataclass(frozen=True)
class MatchState:
    """Typed view of one ``matches`` row (``payload`` JSON-decoded)."""

    match_id: str
    source: str
    kickoff_utc: str | None
    home_entity: str | None
    away_entity: str | None
    status: str | None
    minute: int | None
    period: int | None
    score_home: int | None
    score_away: int | None
    display_clock: str | None
    payload: Any
    updated_at: str | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> MatchState:
        return cls(
            match_id=row["match_id"],
            source=row["source"],
            kickoff_utc=row.get("kickoff_utc"),
            home_entity=row.get("home_entity"),
            away_entity=row.get("away_entity"),
            status=row.get("status"),
            minute=row.get("minute"),
            period=row.get("period"),
            score_home=row.get("score_home"),
            score_away=row.get("score_away"),
            display_clock=row.get("display_clock"),
            payload=_decode_payload(row.get("payload")),
            updated_at=row.get("updated_at"),
        )


@dataclass(frozen=True)
class Event:
    """Typed view of one ``events`` row (``payload`` JSON-decoded)."""

    source: str
    match_id: str
    seq: int
    minute: int | None
    period: int | None
    type: str
    importance: int | None
    actor_entity: str | None
    target_entity: str | None
    detail: str | None
    payload: Any

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Event:
        event_type = row["type"]
        return cls(
            source=row["source"],
            match_id=row["match_id"],
            seq=row["seq"],
            minute=row.get("minute"),
            period=row.get("period"),
            type=event_type,
            importance=row.get("importance"),
            actor_entity=row.get("actor_entity"),
            target_entity=row.get("target_entity"),
            detail=row.get("detail"),
            payload=_normalize_legacy_payload(event_type, _decode_payload(row.get("payload"))),
        )


@dataclass(frozen=True)
class Standing:
    """Typed view of one ``standings`` row (``payload`` JSON-decoded)."""

    source: str
    group_key: str
    entity_id: str
    points: int | None
    rank: int | None
    payload: Any

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Standing:
        return cls(
            source=row["source"],
            group_key=row["group_key"],
            entity_id=row["entity_id"],
            points=row.get("points"),
            rank=row.get("rank"),
            payload=_decode_payload(row.get("payload")),
        )


def list_matches(
    conn: sqlite3.Connection,
    *,
    source: str | None = None,
    status: str | None = None,
) -> list[MatchState]:
    """Return matches (optionally filtered by ``source`` and/or ``status``)."""
    return [MatchState.from_row(r) for r in reader.list_matches(conn, source=source, status=status)]


def get_state(conn: sqlite3.Connection, match_id: str) -> MatchState | None:
    """Return the current state of ``match_id``, or ``None`` if unknown."""
    row = reader.get_state(conn, match_id)
    return MatchState.from_row(row) if row is not None else None


def get_events_since(conn: sqlite3.Connection, match_id: str, since_seq: int = -1) -> list[Event]:
    """Return ``match_id`` events with seq strictly greater than ``since_seq``.

    Ordered by seq; the default ``-1`` returns the full event list. A
    ``since_seq`` at or beyond the head returns an empty list (never an error).
    """
    return [Event.from_row(r) for r in reader.get_events_since(conn, match_id, since_seq)]


def get_standings(
    conn: sqlite3.Connection, source: str, group_key: str | None = None
) -> list[Standing]:
    """Return standings rows for ``source``, optionally scoped to one group."""
    return [Standing.from_row(r) for r in reader.get_standings(conn, source, group_key)]
