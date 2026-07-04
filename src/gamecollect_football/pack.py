"""The football/FIFA WC2026 SportPack factory (entry point ``football-wc2026``).

Supplies all six DESIGN.md §5 contributions plus the pack-owned additive side
tables for football stats and lineups. The DDL is applied by the core via
``connect(path, side_table_ddl=pack.side_table_ddl)`` AFTER core
schema/migrations — additive and idempotent only; core tables are never
altered by a pack.
"""

from __future__ import annotations

import sqlite3

from gamecollect.db.writer import PartitionWriter
from gamecollect.fold import fold
from gamecollect.packs.spec import SportPack
from gamecollect.provider import NormalizedMatch
from gamecollect_football.espn import ESPNAdapter
from gamecollect_football.operations import FOOTBALL_OPERATIONS
from gamecollect_football.reconcile import seed_or_reconcile_match
from gamecollect_football.taxonomy import TAXONOMY

__all__ = ["FOOTBALL_SIDE_TABLE_DDL", "pack", "persist_football_side_tables"]

# Pack-owned side tables (typed homes for the football-specific shapes the
# adapter carries in NormalizedMatch.payload: boxscore stats and lineups).
# Additive + idempotent (CREATE TABLE IF NOT EXISTS); soft refs mirror the
# core posture — no FOREIGN KEY constraints in v1. `name_folded` on lineups
# preserves the fold-based player lookup gamealerts depends on (stamped with
# gamecollect.fold.fold by whoever writes the row).
FOOTBALL_SIDE_TABLE_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS football_stats (
        source          TEXT NOT NULL,
        match_id        TEXT NOT NULL,   -- soft ref -> matches.match_id
        team            TEXT NOT NULL,   -- team display name (boxscore key)
        possession      REAL,
        shots           INTEGER,
        shots_on_target INTEGER,
        corners         INTEGER,
        fouls           INTEGER,
        yellow_cards    INTEGER,
        red_cards       INTEGER,
        offsides        INTEGER,
        PRIMARY KEY (source, match_id, team)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS football_lineups (
        source          TEXT NOT NULL,
        match_id        TEXT NOT NULL,   -- soft ref -> matches.match_id
        team            TEXT NOT NULL,   -- team display name
        athlete_id      TEXT NOT NULL,   -- ESPN athlete id
        display_name    TEXT NOT NULL,
        name_folded     TEXT,            -- fold(display_name), for folded lookups
        jersey          TEXT,
        position        TEXT,
        starter         INTEGER NOT NULL DEFAULT 0,
        subbed_in       INTEGER NOT NULL DEFAULT 0,
        subbed_out      INTEGER NOT NULL DEFAULT 0,
        formation_place INTEGER,
        home_away       TEXT,
        formation       TEXT,
        PRIMARY KEY (source, match_id, team, athlete_id)
    );
    CREATE INDEX IF NOT EXISTS idx_football_lineups_folded
        ON football_lineups (source, name_folded);
    """,
)


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def persist_football_side_tables(
    conn: sqlite3.Connection,
    writer: PartitionWriter,
    match: NormalizedMatch,
    seeded_match_id: str,
) -> None:
    """Persist football detail payload extras into football-owned side tables."""
    payload = match.payload or {}
    stats = payload.get("stats")
    lineups = payload.get("lineups")

    with conn:
        if isinstance(stats, list):
            conn.execute(
                "DELETE FROM football_stats WHERE source = ? AND match_id = ?",
                (writer.source, seeded_match_id),
            )
            for row in stats:
                if not isinstance(row, dict) or not row.get("team"):
                    continue
                conn.execute(
                    "INSERT INTO football_stats "
                    "(source, match_id, team, possession, shots, shots_on_target, "
                    "corners, fouls, yellow_cards, red_cards, offsides) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(source, match_id, team) DO UPDATE SET "
                    "possession = excluded.possession, "
                    "shots = excluded.shots, "
                    "shots_on_target = excluded.shots_on_target, "
                    "corners = excluded.corners, "
                    "fouls = excluded.fouls, "
                    "yellow_cards = excluded.yellow_cards, "
                    "red_cards = excluded.red_cards, "
                    "offsides = excluded.offsides",
                    (
                        writer.source,
                        seeded_match_id,
                        row["team"],
                        row.get("possession"),
                        _to_int(row.get("shots")),
                        _to_int(row.get("shots_on_target")),
                        _to_int(row.get("corners")),
                        _to_int(row.get("fouls")),
                        _to_int(row.get("yellow_cards")),
                        _to_int(row.get("red_cards")),
                        _to_int(row.get("offsides")),
                    ),
                )

        if isinstance(lineups, list):
            conn.execute(
                "DELETE FROM football_lineups WHERE source = ? AND match_id = ?",
                (writer.source, seeded_match_id),
            )
            for lineup in lineups:
                if not isinstance(lineup, dict) or not lineup.get("team"):
                    continue
                team = lineup["team"]
                players = lineup.get("players") or []
                if not isinstance(players, list):
                    continue
                for player in players:
                    if not isinstance(player, dict):
                        continue
                    athlete_id = player.get("athlete_id")
                    display_name = player.get("display_name")
                    if athlete_id is None or not display_name:
                        continue
                    conn.execute(
                        "INSERT INTO football_lineups "
                        "(source, match_id, team, athlete_id, display_name, name_folded, "
                        "jersey, position, starter, subbed_in, subbed_out, "
                        "formation_place, home_away, formation) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(source, match_id, team, athlete_id) DO UPDATE SET "
                        "display_name = excluded.display_name, "
                        "name_folded = excluded.name_folded, "
                        "jersey = excluded.jersey, "
                        "position = excluded.position, "
                        "starter = excluded.starter, "
                        "subbed_in = excluded.subbed_in, "
                        "subbed_out = excluded.subbed_out, "
                        "formation_place = excluded.formation_place, "
                        "home_away = excluded.home_away, "
                        "formation = excluded.formation",
                        (
                            writer.source,
                            seeded_match_id,
                            team,
                            str(athlete_id),
                            display_name,
                            fold(display_name),
                            player.get("jersey"),
                            player.get("position"),
                            int(bool(player.get("starter"))),
                            int(bool(player.get("subbed_in"))),
                            int(bool(player.get("subbed_out"))),
                            _to_int(player.get("formation_place")),
                            lineup.get("home_away"),
                            lineup.get("formation"),
                        ),
                    )


def pack() -> SportPack:
    """Zero-arg entry-point factory returning the football/WC2026 SportPack."""
    return SportPack(
        name="football-wc2026",
        sport="football",
        # ESPNAdapter() defaults to stdlib urllib http_get and the fifa.world
        # tournament slug — a zero-arg construction, as the registry requires.
        provider_factory=ESPNAdapter,
        taxonomy=TAXONOMY,
        prompt_fragments={
            "sport_context": (
                "Association football (soccer): two teams of eleven, two 45-minute "
                "halves plus stoppage time. Goals are the only score; a match may "
                "end in a draw in the group stage."
            ),
            "event_call": (
                "Call goals immediately with scorer and new score. Mention own goals "
                "and converted penalties explicitly. Cards matter: a second yellow "
                "means a red and the team plays a player short."
            ),
        },
        preference_schema={
            "type": "object",
            "properties": {
                "followed_teams": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Canonical team display names to follow.",
                },
                "followed_players": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Player display names to follow (fold-matched).",
                },
            },
        },
        display_metadata={
            "sport_display": "Football",
            "tournament_display": "FIFA World Cup 2026",
            "provider": "espn",
            "tournament_slug": ESPNAdapter.DEFAULT_TOURNAMENT,
        },
        # Natural summarization/compaction boundaries in a football match: the
        # phase-marker event types.
        compaction_boundaries=["kickoff", "half_time", "full_time"],
        side_table_ddl=FOOTBALL_SIDE_TABLE_DDL,
        # Pack-contributed read ops (squad/player-stats) over the side tables
        # above; merged onto the core registry at load. Core never imports this
        # pack — the ops carry their own impls (dependency direction pack → core).
        operations=FOOTBALL_OPERATIONS,
        # Reconcile-aware seed hook (amended from the plan's direct
        # register_unreconciled_match wiring per code review): first resolves
        # the provider match against the canonical schedule via
        # resolve_canonical_match_id and upserts live state onto the canonical
        # row when it resolves — so events land there instead of on a duplicate
        # source-qualified strip — falling back to register_unreconciled_match
        # otherwise. Returns None only when nothing resolved AND identity
        # fields are missing; the engine then skips child writes for it. Its
        # (conn, writer, match) -> str | None signature (provider defaulted)
        # is the SportPack.seed_match contract, so it wires in directly.
        seed_match=seed_or_reconcile_match,
        persist_side_tables=persist_football_side_tables,
    )
