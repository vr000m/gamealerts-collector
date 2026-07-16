# GameWorker integration contract (collector side)

This document pins the cross-repo contract between this collector and the
gamealerts **GameWorker** (intelligence plane, `feature/game-worker`), per
[the dev plan](../dev_plans/20260707-feature-gameworker-integration.md). It
is the collector-side half of a two-repo change: everything here is shipped
and tested in this repo; the **Integration Seams** table at the end is the
gamealerts-side work list, tracked as a separate dev plan in that repo.

## 1. The read surface — `MatchReadPort`

`gamecollect.readport.MatchReadPort` (`src/gamecollect/readport.py`) is a
structural `typing.Protocol`, sport-neutral in its **type signatures** — no
football column names (`team1`/`team2`, `score_home`/`away`, `formation`)
appear anywhere in it; that invariant is enforced by a source/grep test, not
just structural `Protocol` satisfaction. `gamecollect_football.readport.FootballReadPort`
(`src/gamecollect_football/readport.py`) is the concrete adapter — it lives
in the pack, not core, per the pack → core dependency direction, and carries
all football-specific values (event type strings, table/column names) behind
the generic signatures.

Consumers inject an adapter and never touch a sport-specific store directly.
This is a 1:1 replacement for the GameWorker's current 10 football-coupled
DAO methods:

| Worker method              | Protocol method            |
|-----------------------------|------------------------------|
| `latest_state`              | `latest_state`               |
| `events_for_match`          | `events_for_match`           |
| `lineup_for_match`          | `lineup_for_match`           |
| `lineup_for_match_live`     | `lineup_for_match_live`      |
| `venue_for_match`           | `venue_for_match`            |
| `roster_for_team`           | `roster_for_team`            |
| `roster_contains`           | `roster_contains`            |
| `player_in_lineup`          | `player_in_lineup`           |
| `lineup_team_announced`     | `lineup_team_announced`      |
| `recent_commentary`         | `recent_commentary`          |

Return shapes generalize the two-team, single-phase assumptions baked into
the worker's current DAO:

- **`participants`** — a list of `{"name", "side", "score"}`, not a
  hardcoded pair of columns; any number of participants.
- **`phase`** — a field on state/events (adapter-populated from its own
  phase-marker event types), not a single `half_time` literal.
- **Lineup/roster entries** — keyed by `participant` (a display name), not
  by a per-side column.

Every method returns plain dicts/lists (or `bool`/`None`) — the shapes a
`MatchContext`/Q&A tool consumes directly, never a typed dataclass:

- `latest_state(match_id) -> dict | None` — `{"match_id", "status", "minute",
  "period", "display_clock", "kickoff_utc", "phase", "participants": [...],
  "extra": {...}}`. `extra` carries adapter/sport-specific additions (e.g.
  football's knockout `result_type`) that don't belong in the generic shape.
- `events_for_match(match_id, since_seq=-1) -> list[dict]` — per event:
  `{"seq", "minute", "period", "type", "phase", "team", "player", "assist",
  "detail", "importance"}`. `phase` is non-`None` only for phase-marker
  event types.
- `lineup_for_match(match_id, participant=None) -> list[dict]` /
  `lineup_for_match_live(...)` (same contract; a distinct method for callers
  that want an always-current read, no caching in the Protocol itself) — per
  entry: `{"participant", "player", "player_id", "position", "jersey",
  "starter", "subbed_in", "subbed_out", "extra": {...}}`.
- `venue_for_match(match_id) -> dict | None` — `{"stadium", "city"}`, or
  `None` pre-match.
- `roster_for_team(participant) -> list[dict]` — per entry: `{"player",
  "player_id", "position", "extra": {...}}`; empty when no roster recorded.
- `roster_contains(participant, player_name) -> bool` — adapters fold-match
  names (accent/case-insensitive).
- `player_in_lineup(match_id, player_name) -> dict | None` — a lineup entry
  (see above), or `None` if not recorded.
- `lineup_team_announced(match_id, participant) -> bool`.
- `recent_commentary(match_id, limit=20) -> list[dict]` — per entry:
  `{"text", "spoken", "created_at"}`, newest first. Empty list both when no
  commentary has been written yet AND when the `commentary` table does not
  exist yet — see §4. The adapter must tolerate the absent-table case, not
  raise.

## 2. Shared-file access: `data_dir`, lock, startup ordering

**One collector-owned SQLite file**, opened identically by both processes
(WAL, `busy_timeout=5000`, `foreign_keys=ON`).

- **`data_dir` resolution** (`gamecollect.db.paths`): `resolve_data_dir(explicit=None)`
  resolves, in order, an explicit argument, the `GAMECOLLECT_DATA_DIR` env
  var, else `~/.local/share/gamecollect`. `db_path(data_dir)` is
  `<data_dir>/gamecollect.db` — deliberately not `gamealerts.db`, so the two
  processes' default paths never collide even before either side sets the
  shared env var. `lock_dir(data_dir)` is `<data_dir>/locks/`, mirroring
  gamealerts' own convention.
- **`connect()`** (`gamecollect.db.connection`) accepts the original explicit
  `path` form unchanged, or (when `path` is omitted) resolves the shared file
  via `data_dir`/env/default. Both forms apply the same PRAGMAs and schema
  gate.
- **Lock protocol** (`gamecollect.db.locking`): `live_db_admission_lock(lock_dir)`
  returns an `AdvisoryLock` (`fcntl.flock`-backed) on
  `<lock_dir>/_live_db_admission.lock`. `exclusive=False` (default) acquires
  **SHARED** — every live-DB writer, collector or gamealerts' commentary
  writer, holds it SHARED for its whole write session; SHARED holders
  coexist and do not block each other. `exclusive=True` blocks until no
  other holder (shared or exclusive) remains and blocks any new SHARED
  acquire while held — reserved for a **future** collector-side destructive
  reset. No such EXCLUSIVE user exists yet: today the lock excludes nothing,
  and that must be described as forward-scaffolding, not active fencing.
  Actual write serialization is SQLite WAL + `busy_timeout=5000` at the
  transaction level, not this lock — do not treat SHARED acquisition as a
  substitute for a write mutex. Readers never acquire it.
- **Startup ordering (hard constraint): the collector creates and stamps the
  file before gamealerts opens it.** `open_reader` raises
  `SchemaVersionError` on an unstamped `schema_meta`
  (`gamecollect/db/reader.py`), and gamealerts' scoped `commentary` DDL does
  not stamp it. The `commentary → matches(match_id)` foreign key (with
  `foreign_keys=ON`) also requires the `matches` table to exist first. A
  `connect()` call — collector or explicit-path — creates + stamps
  `schema_meta` and creates `matches` as part of schema application, so the
  collector must run `connect()` at least once before gamealerts opens the
  file. gamealerts should retry/degrade if the file is absent, never create
  it itself.

## 3. Vocabulary — the `vocabulary` op

`vocabulary` (registry op; `gamecollect.client.get_vocabulary(conn, pack)`)
returns one installed pack's static manifest — it is not a database read
(`conn` is accepted only for calling-convention parity with every other
op). Shape: `Vocabulary(pack, taxonomy, prompt_fragments, display_metadata,
compaction_boundaries)`, where `taxonomy` maps event type → `TaxonomyEntry(display_name,
importance_default)`.

This is the data-driven replacement for the worker's hardcoded football
literals and its `event_type == "half_time"` phase-boundary check:
`compaction_boundaries` is the generic phase-boundary set the worker should
key compaction off instead. Exposed via the client library, the CLI
(`gamecollect vocabulary <pack> --json`), and the `tools --json` manifest
(carries `contract_version`), matching every other core op's wiring.

## 4. Commentary hosting

gamealerts owns **exactly one table** in the shared file — `commentary` —
and applies **only its own scoped `CREATE TABLE IF NOT EXISTS` DDL** for it,
through its own connection. It must never apply its full `schema.sql`:
doing so would collide (no-op but fragile) with 4 same-named,
different-shape collector tables (`matches`/`events`/`standings`/
`provider_match_map`) and pollute the file with 4 empty gamealerts-only
tables (`stadiums`/`stats`/`rosters`/`lineups`).

The collector never reads or writes `commentary`, and never routes fact
reads through it — this is asserted by regression tests: a coexisting
`commentary` table doesn't trip the collector's `schema_meta` gate, a full
collector write cycle creates zero `commentary` rows and none of the
gamealerts pollution tables, and the football adapter's `recent_commentary`
tolerates the table being entirely absent (collector-first boot, pre-match,
or replay) by returning `[]`, not raising `no such table`.

## 5. Integration Seams — gamealerts-side work list

This collector plan is complete when this contract doc plus the table below
give the gamealerts-side dev plan everything it needs (Protocol shapes,
lock/ordering rules, config + `clear_commentary` additions). None of the
rows below are implemented in this repo.

| # | gamealerts-side change required | Why |
|---|---|---|
| 1 | For fact reads, type the worker's `dao` param as the `MatchReadPort` **Protocol** and inject the collector adapter; keep the concrete `DAO` only for commentary writes on the shared file | Decouples fact reads from gamealerts' football store; its DAO can't read the collector's schema |
| 2 | Make `MatchContext`/FastLine **data-driven** over pack-supplied vocabulary (`taxonomy` display names + `prompt_fragments`) instead of literal `goal`/`card`/… dispatch | Multi-sport rendering |
| 3 | Generalize two-team (`team1`/`team2`/`score_home`/`away`) → `participants`; `half_time` literal → `compaction_boundaries`/`phase` | No schema lock-in |
| 4 | Apply **only the scoped `commentary` DDL** to the collector's file (NOT full `schema.sql`); hold `_live_db_admission.lock` **SHARED** (via the already-parameterized `live_db_admission_lock(collector_data_dir/"locks")`) for `append_commentary`/`mark_spoken` | Avoid table-name collisions + pollution; correct shared-writer fencing |
| 5 | Add a `GAMEALERTS_COLLECTOR_DATA_DIR` config value so gamealerts learns the collector's file + `locks/` path at startup | Both processes must open the same file / lock dir |
| 6 | Add a scoped `clear_commentary(match_id)` (or table-level clear) DAO/maintenance path | `reset_live` won't reach the collector's file, so replay/demo needs an explicit commentary clear |
| 7 | Open the collector's file only **after** it exists + is stamped (retry/degrade if absent); never create it via gamealerts' full `_apply_schema` — apply only the scoped `commentary` DDL | The collector must create+stamp `schema_meta` and `matches` first (FK + version-gate prerequisites); a gamealerts-first `connect()` would leave the file unstamped and pollute it |
