# Design contract (v0 — distilled from the 2026-07-02 rearchitecture discussion)

This document pins the decisions made while rearchitecting gamealerts, so they
survive into implementation. Each section notes *why*, because several choices
reverse plausible defaults.

## 1. Position in the overall architecture

gamealerts is being restructured into planes; this project is the **data plane**:

- **Data plane (this repo)**: sport packs → poll → normalize → shared SQLite;
  library + CLI for reads; record/replay.
- **Intelligence plane (gamealerts)**: one pipecat worker per live followed match
  (own LLM, tools, in-memory context; two-phase commentary: instant template
  line, then LLM enrichment from full context). Workers poll this project's
  client library (`get_events_since(match_id, seq)` every ~2–3 s) and make
  on-demand lookups (squad, standings, historical stats).
- **Voice plane (gamealerts)**: a main pipecat agent owning mic/PTT/STT/TTS,
  question routing, speech arbitration (priority queue), and worker lifecycle.
  It never touches game data.
- **UI plane (gamealerts)**: Swift menubar over the existing IPC.

Validated by two spikes on pipecat 1.4.0 (dynamic worker lifecycle, job
streaming, transport-agnostic bus bridging) and a replay of a recorded 6-goal
match through an isolated game worker (8/8 ground-truth checks, local LLM).

## 2. Polling, not push

Consumers poll. The collector itself polls upstream (ESPN et al.) every few
seconds, so a push channel would not improve end-to-end latency; polling keeps
the contract trivial for third-party packs and consumers. Push may be added
later as an optimization — it must never become the only path.

## 3. Shared database, partition-per-writer

All collector daemons write one SQLite file (WAL mode).

- **Writer discipline**: one writer per *partition* — a daemon owns the rows of
  the tournament/matches it collects (`source`/`tournament_id` keyed). This
  generalizes gamealerts' proven single-writer-per-table rule.
- Per-match `seq` is monotonic within its partition → `get-events-since`
  works regardless of which daemon wrote the rows.
- **Pragmas are contract**: WAL mode + busy_timeout are set by the core library
  (first-writer-wins rules live in one place), never by individual packs.
- **Schema is a versioned contract** owned by the core library: schema-version
  table + migrations; daemons refuse to start against a newer major version.
- The `commentary` table does NOT move here. Prose written by the intelligence
  plane stays in the consuming app's own storage; this DB is read-only to
  everyone but collector daemons.

## 4. Schema: generic core + sport-pack side tables

Generic core: `matches`, `events`, `entities` (teams/drivers/players),
`standings`, with a JSON payload column for sport specifics. Sport packs may
own typed side tables for what they need queryable in SQL (the consuming app's
voice tools do real typed queries today — pure-JSON would regress that).

## 5. Sport packs

A pack is a Python package (entry-point registered) supplying:

- a provider adapter (the `MatchDataProvider` ABC + `Normalized*` dataclasses
  pattern from gamealerts `data/provider.py` — the cleanest existing seam),
- an event taxonomy declaration (types, importance defaults, display metadata)
  — replaces gamealerts' hardcoded football enums,
- prompt fragments and domain vocabulary (consumed by the intelligence plane,
  carried here as data),
- a preference schema (football: followed teams; F1: followed drivers, event
  classes),
- display metadata (replaces the menubar's baked-in `teams.json`),
- compaction boundaries (half-time, innings, stints — used by workers to
  compact context).
- a `seed_match(conn, writer, match) -> str | None` hook: the collection engine
  is sport-agnostic and never imports a pack, so it reaches a pack's
  seed/reconcile logic only through this hook. It must ensure a `matches` row
  exists (owned by `writer.source`) before the engine appends the match's
  events and return the canonical `match_id` to write children against; `None`
  means the match could not be seeded (e.g. missing identity fields) and the
  engine skips its child writes for that poll. Football wires
  `register_unreconciled_match` (source-qualified strip rows); packs emitting
  already-canonical ids can rely on the core `default_seed_match`.

First pack: football / FIFA World Cup 2026 (ported from gamealerts). A second
tournament (e.g. Champions League) should be ~configuration on that pack.

## 6. Interfaces

- **Client library** (canonical): `list_matches`, `get_state`,
  `get_events_since`, `get_standings`, `get_squad`, `get_player_stats`,
  historical lookups. Sync, typed returns.
- **CLI** `gamecollect`: thin `main()` over the library, `--json` output whose
  schemas ARE the public contract. `gamecollect tools --json` emits a manifest
  (commands + output JSON schemas) for runtime discovery; consuming agents
  generate function-calling tool definitions from it.
- **MCP wrapper**: optional, later, generated from the manifest. Never the
  contract.

Requirement discovered during Q&A design: the surface needs *historical and
reference* endpoints (squads, player tournament stats, past meetings), not just
live state — upstream provider coverage for these must be verified per pack.

## 7. Record/replay is a provider, not test scaffolding

`ReplayProvider` implements the same provider ABC, reading recorded events with
original (or accelerated) timing. It ships in the core library because it is
the eval/hardening harness for any consumer: replay a recorded match, assert
consumer behavior against ground truth (facts-only checks against the event
log are the strongest hallucination guard). Recording is symmetric: any live
session can be captured as a replayable fixture.

Fixture format (`gamecollect.fixture_io`, one match per file, checked into git
as JSON): a `format_version` field, a `match` header (id, status, minute,
score, clock, team names, kickoff, payload), an `events` list in seq order
carrying the full `NormalizedEvent` shape, and nullable `entities`/`standings`
snapshots (a live `--record` session only sees `NormalizedMatch` state, so it
records those empty; the importer fills them from the source DB). The engine's
`--record` flag writes fixtures through the same module a replay reads.

Seed fixtures to import from gamealerts' DBs: Morocco–Haiti 4–2 (21 events +
21 commentary rows, real DB), Canada–Qatar 6–0 (fictional DB; Jonathan David
hat-trick — the brace/hat-trick context test), NZ–Belgium 1–5, Turkey–USA 3–2.

## 8. Sequencing (relative to gamealerts)

1. Game worker + replay harness + evals — in gamealerts, against its current
   store (hardens what the tool contract must be). *Parallel:* scaffold this
   repo (schema, pack SDK, football pack, CLI).
2. This project reaches contract stability; gamealerts' voice tools port to
   the client library.
3. gamealerts wires the multi-worker voice plane.
4. Publish to PyPI; gamealerts declares the dependency (never a
   `[tool.uv.sources]` local path — that bakes absolute paths into `uv.lock`
   and breaks CI; learned the hard way with the STT client).

## 9. Non-goals

- No LLM calls, no prose generation, no TTS/voice anything.
- No scheduling/supervision of consumers (gamealerts' supervisor owns that).
- No push/subscription API in v1.
