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

- **Client library** (canonical): the four **core** ops `list_matches`,
  `get_state`, `get_events_since`, `get_standings` (plus historical lookups,
  later) live in `gamecollect.client`. Sync, typed returns. Packs contribute
  further read ops over their side tables — football adds `get_squad` and
  `get_player_stats`, implemented in `gamecollect_football` — so core never
  imports a pack module.
- **Operation registry** (`gamecollect.registry`): the single source of truth.
  Every operation — core or pack-contributed — is declared once (name, params,
  summary, output JSON Schema); the CLI's argparse subcommands and the tools
  manifest are both generated from the merged registry, so the CLI surface,
  the library, and the manifest cannot drift (a registry-derivation equality
  test pins this).
- **CLI** `gamecollect`: thin `main()` over the library, `--json` output whose
  schemas ARE the public contract (versioned via the manifest's
  `contract_version`, golden-file tested). `gamecollect tools --json` emits a
  manifest (commands + output JSON schemas) for runtime discovery; consuming
  agents generate function-calling tool definitions from it.
- **MCP wrapper**: optional, later, generated from the manifest. Never the
  contract.

Requirement discovered during Q&A design: the surface needs *historical and
reference* endpoints (squads, player tournament stats, past meetings), not just
live state — upstream provider coverage for these must be verified per pack.

**Event `payload` sub-key contract.** `get_events_since`/`events --json` rows
carry a `payload` object; its sub-keys are the only currently-populated,
stable way to read event participants (`actor_entity`/`target_entity` are
reserved schema columns, unpopulated by the writer today). `payload.team` is
present whenever the event's team is known. For goal-family event types
(`goal`, `own_goal`), `payload.scorer`/`payload.assist` carry the scorer's and
assister's display names as plain strings; for every other event type
(`penalty`, `yellow`, `red`, `sub`, and any future taxonomy addition),
`payload.player`/`payload.assist` carry the same shape under the pre-existing
key names (e.g. the booked player on a card, or the two players on a
substitution). Keys are omitted entirely — never set to `null` — when the
underlying field has no value.

Two caveats consumers should know:

- **Own-goal team attribution is the benefiting team, not the scorer's own
  team.** On an `own_goal` row, `payload.scorer` names the player who scored
  into their own net, but the row's team attribution (`payload.team`/
  `event.team`) is the team that *benefited* from the goal — not the
  own-scorer's team. E.g. when Cabo Verde's Diney Borges scores an own goal,
  the row carries `team: "Argentina"` (the beneficiary), not `"Cabo Verde"`.
  This reflects ESPN's own team attribution in the raw feed, not
  collector-side logic: the adapter passes `team` through verbatim for all
  event types, and this convention is provider behavior not independently
  verified beyond the fixtures checked into this repo.
- **Two named assumptions, inherited from the ESPN adapter's parsing (not
  independently verified here):** (1) ESPN's `participants` array is ordered
  scorer-first, assist-second, inferred from a code comment with no explicit
  role field to confirm it, corroborated only by a small fixture sample; (2)
  the free-text content of `detail` (ESPN's `text`/`shortText`) is provider
  behavior this repo does not control — for goal-family events, treat
  `payload.scorer`/`.assist` as the authoritative source of participant
  identity, not `detail`.

**Finished-on-first-sight event backfill.** A match the collector first
observes already `FINISHED` — no in-memory baseline, no stored `matches` row,
never previously seen live — is backfilled once: the engine calls
`fetch_match_detail` for it and merges the result through the same
`_event_to_row`/writer path as a live-observed match, so its events land under
the identical `payload.scorer`/`payload.assist`/`payload.team` contract above,
not a bare scoreboard-only snapshot. This is current-slate-bounded by
construction, not by a new rate limiter: ESPN's default (no-`dates`)
scoreboard response only ever holds the current slate (verified live against
`fifa.world`), so a match that finished on a prior slate has already rolled
off the board and is out of scope for this backfill. A failed detail fetch is
retried on subsequent polls up to a pinned cap, `_BACKFILL_MAX_ATTEMPTS = 3`
(`gamecollect.engine`); once exhausted, the collector gives up and persists
the scoreboard-only snapshot (final score retained, no further retries for
that match). The behavior defaults on and can be disabled with
`collect --no-finished-backfill` (`backfill_finished_matches=False` on
`CollectorEngine`).

## 7. Record/replay is a provider, not test scaffolding

`ReplayProvider` (`gamecollect.replay`) implements the same provider ABC,
reading recorded events with original (or accelerated) timing. It ships in the
core library because it is the eval/hardening harness for any consumer: replay a
recorded match, assert consumer behavior against ground truth (facts-only checks
against the event log are the strongest hallucination guard). Recording is
symmetric: any live session can be captured as a replayable fixture.

`ReplayProvider(fixture, speed=1.0)` streams a fixture's recorded events out over
successive polls. Two pacing modes: **paced** (finite `speed`) reveals an event
recorded at game-minute *m* once `m*60/speed` wall-clock seconds have elapsed
(`speed=60` replays 90 minutes in ~90 seconds); **step** (`speed=math.inf`)
reveals exactly one more event per `fetch_live_matches`, deterministically and
with no clock — the mode the determinism/e2e tests use, driving the engine by
polling and watching `exhausted`. Because the sport-agnostic core cannot
recompute a running score (a goal incrementing the score is football knowledge),
every replay snapshot carries the fixture's recorded *final* header state
verbatim while the event list grows; the fully-revealed (terminal) snapshot
equals the fixture header exactly, which is what makes a record→replay round trip
reproduce the original fixture and makes `updated_at` the only field a
determinism check must exclude (the writer auto-stamps it wall-clock; the replay
path has no clock injection into `upsert_match`).

Fixture format (`gamecollect.fixture_io`, one match per file, checked into git
as JSON): a `format_version` field, a `match` header (id, status, minute,
score, clock, team names, kickoff, payload), an `events` list in seq order
carrying the full `NormalizedEvent` shape, and nullable `entities`/`standings`
snapshots (a live `--record` session only sees `NormalizedMatch` state, so it
records those empty; the importer fills them from the source DB). The engine's
`--record` flag writes fixtures through the same module a replay reads.

Seed fixtures, imported by `scripts/import_gamealerts_fixtures.py`. **Fixture
locations (correcting an earlier ambiguity here about which DB holds what):** the
three real matches live in `~/.local/share/gamealerts/gamealerts.db` — Morocco–
Haiti `espn:760464` 4–2 FINISHED (21 events), NZ–Belgium `espn:760477` 1–5
FINISHED (20 events), Turkey–USA `espn:760470` 3–2 FINISHED (18 events); Canada–
Qatar `2026-06-18_canada_vs_qatar_9` 6–0 FINISHED (6 events, Jonathan David
hat-trick — the brace/hat-trick context test) lives in the separate
`gamealerts-fictional.db`, whose **older schema shape** lacks
`rosters.player_name_folded` and has no `lineups` table. Commentary rows are NOT
imported (prose is out of scope, §3).

The importer opens each source DB READ-ONLY (the live DBs are mutable, with
active WAL) and tolerates both schema shapes (and both column namings — the
`matches`/`lineups` column names are not plan-pinned, so `team1`/`home_team`,
`score_home`/`home_score` etc. are read tolerantly). Event-field mapping
(gamealerts → collector): `type` → the football taxonomy key, `importance` from
the pack taxonomy default, `minute`/`detail`/`team`/`player`/`assist` preserved;
the derived collector columns are computed downstream from these preserved
fields (`period` from `minute`; `actor_entity`/`target_entity` from
`player`/`assist`; entity `parent` from `team`) — the `NormalizedEvent` fixture
shape has no slots for them, so the fixture is the faithful source record. At
import time it re-asserts the pinned ground truth (event count/score/status) and
fails loudly on drift, and fails if any distinct gamealerts `type` has no
taxonomy entry. `--check <dir>` re-validates that checked-in fixtures parse and
replay deterministically (CI-safe; never touches `~/.local/share`).

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
