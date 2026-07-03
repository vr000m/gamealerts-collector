"""The football/FIFA WC2026 SportPack factory (entry point ``football-wc2026``).

Supplies all six DESIGN.md §5 contributions plus the pack-owned additive side
tables for football stats and lineups. The DDL is applied by the core via
``connect(path, side_table_ddl=pack.side_table_ddl)`` AFTER core
schema/migrations — additive and idempotent only; core tables are never
altered by a pack.
"""

from __future__ import annotations

from gamecollect.packs.spec import SportPack
from gamecollect_football.espn import ESPNAdapter
from gamecollect_football.operations import FOOTBALL_OPERATIONS
from gamecollect_football.reconcile import register_unreconciled_match
from gamecollect_football.taxonomy import TAXONOMY

__all__ = ["FOOTBALL_SIDE_TABLE_DDL", "pack"]

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
        # Reconcile-aware seed hook: register_unreconciled_match seeds a
        # source-qualified strip row (returning its id) or returns None when the
        # scoreboard match lacks identity fields — the engine then skips child
        # writes for it. Its (conn, writer, match) -> str | None signature is the
        # SportPack.seed_match contract exactly, so it wires in directly.
        seed_match=register_unreconciled_match,
    )
