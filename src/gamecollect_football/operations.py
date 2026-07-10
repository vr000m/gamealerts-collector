"""Football pack-contributed read operations over the pack side tables.

``get_squad`` reads ``football_lineups`` and ``get_player_stats`` reads
``football_stats`` — both are pack-owned side tables (DESIGN.md §4), so these
ops live in the football pack and register into the core operation registry at
pack load (:attr:`gamecollect.packs.spec.SportPack.operations`). Core
``client.py``/``reader.py`` stay sport-agnostic and never import this module;
the dependency direction is pack → core only.

Both ops are **match-scoped**: ``match_id`` is globally unique, so the
partition ``source`` is resolved from the ``matches`` row (via the core reader)
rather than taken as a parameter — a caller holding a ``match_id`` need not know
which daemon wrote it. An unknown ``match_id`` yields an empty list (never an
error), mirroring the core reads' no-match posture.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from gamecollect.db import reader
from gamecollect.registry import Operation, ParamSpec

__all__ = [
    "SquadMember",
    "PlayerStats",
    "get_squad",
    "get_player_stats",
    "SQUAD_OPERATION",
    "PLAYER_STATS_OPERATION",
    "FOOTBALL_OPERATIONS",
]


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    """Run a SELECT and return dict rows regardless of the connection row_factory.

    Same shape helper the core reader uses; kept local so the pack does not
    reach into a core private.
    """
    try:
        cursor = conn.execute(sql, params)
    except sqlite3.OperationalError as exc:
        # A connection opened without this pack's side_table_ddl has no
        # football_* tables; honor the ops' never-error empty-list contract.
        if "no such table" in str(exc):
            return []
        raise
    names = [col[0] for col in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _resolve_source(conn: sqlite3.Connection, match_id: str) -> str | None:
    """Return the partition ``source`` that owns ``match_id``, or ``None``."""
    state = reader.get_state(conn, match_id)
    return state["source"] if state is not None else None


@dataclass(frozen=True)
class SquadMember:
    """Typed view of one ``football_lineups`` row (one player in a match squad)."""

    source: str
    match_id: str
    team: str
    athlete_id: str
    display_name: str
    name_folded: str | None
    jersey: str | None
    position: str | None
    starter: int
    subbed_in: int
    subbed_out: int
    formation_place: int | None
    home_away: str | None
    formation: str | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> SquadMember:
        return cls(
            source=row["source"],
            match_id=row["match_id"],
            team=row["team"],
            athlete_id=row["athlete_id"],
            display_name=row["display_name"],
            name_folded=row.get("name_folded"),
            jersey=row.get("jersey"),
            position=row.get("position"),
            starter=row.get("starter", 0),
            subbed_in=row.get("subbed_in", 0),
            subbed_out=row.get("subbed_out", 0),
            formation_place=row.get("formation_place"),
            home_away=row.get("home_away"),
            formation=row.get("formation"),
        )


@dataclass(frozen=True)
class PlayerStats:
    """Typed view of one ``football_stats`` row (a team's boxscore for a match)."""

    source: str
    match_id: str
    team: str
    possession: float | None
    shots: int | None
    shots_on_target: int | None
    corners: int | None
    fouls: int | None
    yellow_cards: int | None
    red_cards: int | None
    offsides: int | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> PlayerStats:
        return cls(
            source=row["source"],
            match_id=row["match_id"],
            team=row["team"],
            possession=row.get("possession"),
            shots=row.get("shots"),
            shots_on_target=row.get("shots_on_target"),
            corners=row.get("corners"),
            fouls=row.get("fouls"),
            yellow_cards=row.get("yellow_cards"),
            red_cards=row.get("red_cards"),
            offsides=row.get("offsides"),
        )


def get_squad(
    conn: sqlite3.Connection, match_id: str, team: str | None = None
) -> list[SquadMember]:
    """Return the recorded squad (lineup) for ``match_id``, optionally one team.

    Resolves the partition ``source`` from the ``matches`` row; returns an
    empty list for an unknown match or a match with no lineups recorded.
    """
    source = _resolve_source(conn, match_id)
    if source is None:
        return []
    sql = "SELECT * FROM football_lineups WHERE source = ? AND match_id = ?"
    params: list[Any] = [source, match_id]
    if team is not None:
        sql += " AND team = ?"
        params.append(team)
    sql += " ORDER BY team, formation_place IS NULL, formation_place, athlete_id"
    return [SquadMember.from_row(r) for r in _rows(conn, sql, tuple(params))]


def get_player_stats(
    conn: sqlite3.Connection, match_id: str, team: str | None = None
) -> list[PlayerStats]:
    """Return the recorded boxscore stats for ``match_id``, optionally one team.

    Resolves the partition ``source`` from the ``matches`` row; returns an
    empty list for an unknown match or a match with no stats recorded.
    """
    source = _resolve_source(conn, match_id)
    if source is None:
        return []
    sql = "SELECT * FROM football_stats WHERE source = ? AND match_id = ?"
    params: list[Any] = [source, match_id]
    if team is not None:
        sql += " AND team = ?"
        params.append(team)
    sql += " ORDER BY team"
    return [PlayerStats.from_row(r) for r in _rows(conn, sql, tuple(params))]


# ---------------------------------------------------------------------------
# Registry declarations (merged onto the core ops at pack load).
# Output schemas ARE the public contract for these ops (golden-file tested).
# ---------------------------------------------------------------------------

_STR = {"type": ["string", "null"]}
_INT = {"type": ["integer", "null"]}
_NUM = {"type": ["number", "null"]}

_SQUAD_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "source": {"type": "string"},
        "match_id": {"type": "string"},
        "team": {"type": "string"},
        "athlete_id": {"type": "string"},
        "display_name": {"type": "string"},
        "name_folded": _STR,
        "jersey": _STR,
        "position": _STR,
        "starter": {"type": "integer"},
        "subbed_in": {"type": "integer"},
        "subbed_out": {"type": "integer"},
        "formation_place": _INT,
        "home_away": _STR,
        "formation": _STR,
    },
}

_PLAYER_STATS_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "source": {"type": "string"},
        "match_id": {"type": "string"},
        "team": {"type": "string"},
        "possession": _NUM,
        "shots": _INT,
        "shots_on_target": _INT,
        "corners": _INT,
        "fouls": _INT,
        "yellow_cards": _INT,
        "red_cards": _INT,
        "offsides": _INT,
    },
}


SQUAD_OPERATION = Operation(
    name="squad",
    summary="Return the recorded squad (lineup) for a match, optionally one team.",
    params=(
        ParamSpec("match_id", "string", required=True, summary="Globally-unique match id."),
        ParamSpec("team", "string", required=False, summary="Restrict to one team's players."),
    ),
    output_schema={"type": "array", "items": _SQUAD_ITEM_SCHEMA},
    impl=get_squad,
)

PLAYER_STATS_OPERATION = Operation(
    name="player-stats",
    summary="Return the recorded boxscore stats for a match, optionally one team.",
    params=(
        ParamSpec("match_id", "string", required=True, summary="Globally-unique match id."),
        ParamSpec("team", "string", required=False, summary="Restrict to one team."),
    ),
    output_schema={"type": "array", "items": _PLAYER_STATS_ITEM_SCHEMA},
    impl=get_player_stats,
)

FOOTBALL_OPERATIONS: tuple[Operation, ...] = (SQUAD_OPERATION, PLAYER_STATS_OPERATION)
