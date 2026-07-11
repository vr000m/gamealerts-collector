-- gamecollect schema v1 (generic core; idempotent — CREATE IF NOT EXISTS throughout)
--
-- Contract (docs/DESIGN.md §3–4):
--   * One shared SQLite file, WAL mode; pragmas are set by connection.py, never here.
--   * Partition-per-writer: every row carries its `source` (tournament/provider
--     instance); PartitionWriter stamps it on every write.
--   * Generic core only — sport specifics live in `payload` JSON columns or in
--     pack-owned side tables (applied by connect() AFTER this file). Packs never
--     alter core tables.
--   * Entity refs are SOFT references in v1: nullable TEXT, no FOREIGN KEY
--     constraints — a match/event may legally land before its entities rows
--     exist (the ported pipeline is name-keyed). FK-hardening is a later minor.
--   * The `commentary` table deliberately does NOT exist here — prose belongs
--     to consuming apps (DESIGN.md §3).
--   * Version bookkeeping lives in `schema_meta`; the version row is stamped by
--     migrations.py (not here) so applied_at reflects the actual apply instant.

-- ---------------------------------------------------------------------------
-- Schema version (single row, id pinned to 1)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_meta (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    major       INTEGER NOT NULL,
    minor       INTEGER NOT NULL,
    applied_at  TEXT NOT NULL               -- ISO 8601 UTC
);

-- ---------------------------------------------------------------------------
-- Matches  (schedule + live state)
-- match_id is the PUBLIC identifier and is globally unambiguous: when a writer
-- creates an unreconciled canonical id it source-qualifies the provider-native
-- id, so client APIs that take only match_id never collide across sources.
-- Sport/schedule extras (round, group, city, stadium, HT scores, …) live in
-- `payload` JSON — not core columns, not a side table.
-- Upsert key: match_id
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS matches (
    match_id     TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    kickoff_utc  TEXT,                      -- ISO 8601 UTC datetime
    home_entity  TEXT,                      -- soft ref -> entities.entity_id
    away_entity  TEXT,                      -- soft ref -> entities.entity_id
    status       TEXT,                      -- MatchStatus value (SCHEDULED | IN_PLAY | ...)
    minute       INTEGER,
    period       INTEGER,
    score_home   INTEGER,
    score_away   INTEGER,
    display_clock TEXT,
    payload      TEXT,                      -- JSON: sport/schedule extras
    updated_at   TEXT                       -- ISO 8601 UTC
);

CREATE INDEX IF NOT EXISTS idx_matches_source_status ON matches (source, status);

-- ---------------------------------------------------------------------------
-- Events
-- `type` is a pack-declared string (validated by PartitionWriter when a
-- taxonomy is wired), NOT a core enum — the core stays sport-agnostic.
-- `seq` is provider-derived and monotonic per (source, match_id); re-polls are
-- idempotent via INSERT OR IGNORE on the PK.
-- Upsert key: (source, match_id, seq)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    source        TEXT NOT NULL,
    match_id      TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    minute        INTEGER,
    period        INTEGER,
    type          TEXT NOT NULL,
    importance    INTEGER,
    actor_entity  TEXT,                     -- reserved, unpopulated by the writer today; participant names live in payload.scorer/.player (see below)
    target_entity TEXT,                     -- reserved, unpopulated by the writer today; participant names live in payload.assist (see below)
    detail        TEXT,
    payload       TEXT,                     -- JSON: sport extras. team (always, when known); scorer/assist for
                                             -- goal-family events (goal, own_goal); player/assist for every
                                             -- other event type.
    PRIMARY KEY (source, match_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_match_seq ON events (match_id, seq);

-- ---------------------------------------------------------------------------
-- Entities  (kind-discriminated: team | player | driver | ...)
-- Replaces gamealerts' football `rosters`/team tables. `name_folded` is
-- stamped at upsert time with gamecollect.fold.fold() so folded lookups are
-- indexed equality with no per-row NFKD at read time (the gamealerts
-- rosters.player_name_folded pattern, generalized).
-- Upsert key: (source, entity_id)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entities (
    source        TEXT NOT NULL,
    entity_id     TEXT NOT NULL,
    kind          TEXT NOT NULL,            -- team | player | driver | ...
    display_name  TEXT NOT NULL,
    name_folded   TEXT,                     -- fold(display_name), stamped by the writer
    parent_entity TEXT,                     -- soft ref -> entities.entity_id (player -> team)
    payload       TEXT,                     -- JSON: sport extras (jersey, position, ...)
    PRIMARY KEY (source, entity_id)
);

-- name_folded leads: it is the one always-present predicate in
-- find_entities_by_name (source/kind are optional filters), so this ordering
-- serves all four filter combinations; (source, kind, ...) would force a
-- full scan for name-only lookups.
CREATE INDEX IF NOT EXISTS idx_entities_folded ON entities (name_folded, source, kind);

-- ---------------------------------------------------------------------------
-- Standings  (group tables, championship standings, ...)
-- Sport-specific columns (played/won/goal_diff/...) live in `payload` JSON.
-- Upsert key: (source, group_key, entity_id)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS standings (
    source     TEXT NOT NULL,
    group_key  TEXT NOT NULL,
    entity_id  TEXT NOT NULL,               -- soft ref -> entities.entity_id
    points     INTEGER,
    rank       INTEGER,
    payload    TEXT,                        -- JSON: sport extras
    PRIMARY KEY (source, group_key, entity_id)
);

-- ---------------------------------------------------------------------------
-- Provider ↔ canonical match-id map
-- Bridges provider-native ids to the canonical match_id. Gains a `source`
-- column vs gamealerts (NOT ported as-is): two sources collecting the same
-- provider-native id must not collide. Stamped by PartitionWriter like every
-- other table.
-- Upsert key: (source, provider, provider_match_id)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS provider_match_map (
    source            TEXT NOT NULL,
    provider          TEXT NOT NULL,
    provider_match_id TEXT NOT NULL,
    match_id          TEXT NOT NULL,        -- soft ref -> matches.match_id
    PRIMARY KEY (source, provider, provider_match_id)
);
