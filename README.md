# gamealerts-collector

Sport-agnostic live match data collection: pluggable **sport packs** poll upstream
providers, normalize into a **common versioned SQLite schema**, and expose the data
through a **client library and CLI** — with **record/replay** built in so consumers
(agents, tests, evals) can re-run real matches deterministically.

Extracted from [gamealerts](https://github.com/vr000m/gamealerts) (a macOS menubar +
voice commentary app for the 2026 FIFA World Cup), where the collector, event
taxonomy, and provider adapters were football-hardcoded. This project is the
generalization: adding a tournament should be a config file; adding a sport
(F1, Champions League, cricket) should be one new package.

**Status: pre-alpha.** The foundation is implemented: package scaffold, schema v1 +
core DB layer, the sport-pack SDK, and the first pack (`football-wc2026`).
Interfaces (engine daemon, client library, real CLI, replay) are next — see
[docs/dev_plans/](docs/dev_plans/). See [docs/DESIGN.md](docs/DESIGN.md) for the
architecture contract. Not yet on PyPI; not yet consumed by gamealerts.

## Development

```sh
uv sync                 # install (incl. dev deps + in-repo test-fixture pack)
uv run pytest -q        # full test suite
uv run ruff format --check src tests && uv run ruff check src tests
uv run gamecollect tools --json   # inspect the operation manifest (see Usage below)
```

Two packages ship in one wheel: `gamecollect` (core + SDK) and
`gamecollect_football` (the WC2026 pack, registered under the
`gamecollect.packs` entry-point group).

## Usage

Install (until it lands on PyPI, from a checkout):

```sh
uv sync                 # or: pip install -e .
```

The `gamecollect` CLI is a thin `main()` over the client library. Its read
subcommands are derived from the operation registry (core ops, plus any
installed pack's ops), so the CLI surface and the library cannot drift.

**Discover the surface.** `tools --json` emits the machine-readable manifest —
`{contract_version, operations}` where each operation carries its params and its
`--json` output schema (the public contract). Agent harnesses discover
capabilities and generate function-calling tool definitions from it:

```sh
gamecollect tools --json
```

**Read operations** query a collector database. Each takes `--db <path>` and an
optional `--json` flag (human-readable text otherwise); the `--json` output is
the schema-pinned, golden-tested contract:

```sh
gamecollect matches --db games.db --json                 # list matches (opt. --source/--status)
gamecollect state <match_id> --db games.db --json        # one match's current state
gamecollect events <match_id> --db games.db --json       # events (opt. --since_seq N)
gamecollect standings <source> --db games.db --json      # standings (opt. --group_key)
```

With the `football-wc2026` pack installed, two more read ops appear
automatically (both match-scoped; `--team` optionally narrows them):

```sh
gamecollect squad <match_id> --db games.db --json        # recorded lineup
gamecollect player-stats <match_id> --db games.db --json # recorded boxscore stats
```

**Collect.** `collect` runs the collector engine for one source: it loads a
pack, polls its provider, and writes diffed match state into the shared database
until SIGTERM. `--record` captures the session as a replayable fixture:

```sh
gamecollect collect --pack football-wc2026 --db games.db --source wc2026-live \
    --record sessions/            # optional: write a replay fixture
```

> The `ReplayProvider` end-to-end flow and the checked-in seed fixtures land in
> the next interfaces phase (replay); see [docs/dev_plans/](docs/dev_plans/).

## Shape

```
collector core library     owns the schema (versioned), writer discipline,
        │                  ReplayProvider, sport-pack SDK
        ├── collector daemons (one per tournament) ──► shared SQLite
        │       sport packs plug in here                (WAL, partition-per-writer)
        ├── client library  ──► in-process consumers (poll + on-demand queries)
        ├── CLI (gamecollect …) ──► humans, external agents, scripts (JSON out)
        └── MCP wrapper (optional, later, generated from the same manifest)
```

Deliberately **no LLM and no prose** in this project: the collector is a pure data
plane. Commentary, alerts, and voice live in consuming applications.

## Why a CLI (and library) rather than an MCP server

- A CLI is hot-evolvable: new subcommands are visible on the next invocation —
  no session restart, unlike MCP tool lists in most clients today.
- `gamecollect tools --json` self-describes the surface (commands + JSON output
  schemas), so agent harnesses can discover capabilities per call and even
  generate function-calling tool definitions from the manifest.
- In-process consumers skip the subprocess entirely and use the client library —
  the CLI is a thin `main()` around it. The contract is the library API + the
  CLI's JSON output schemas; an MCP server is an optional adapter, not the contract.
