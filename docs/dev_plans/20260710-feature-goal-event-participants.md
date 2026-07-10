# Task: Goal-event participant contract — typed `payload.scorer`/`payload.assist`

**Status**: Not Started
**Component**: collector
**Assigned to**: Claude
**Priority**: High
**Branch**: feature/goal-event-participants (branched from `main` at `46e9ca8`, post PR #2 merge)
**Created**: 2026-07-10
**Completed**: (pending)

## Objective

Give consumers a stable, typed way to read "who scored / assisted" off `get_events_since` before `contract_version 1` freezes, by writing `payload.scorer` / `payload.assist` (plain display-name strings) on events that carry a scorer/assist, instead of the currently-unimplemented `actor_entity`/`target_entity` entity-ref columns.

**Depends on:** PR #2 (`feature/collector-interfaces`) — merged to `main` 2026-07-10. This branch is cut from `main` post-merge, so the file/line references below are current.

## Context

The gamealerts game-worker (PR #33, the per-match commentary/Q&A consumer) flagged a real inconsistency in PR #2's review (`/review 2`) ahead of freezing `contract_version 1`: `get_events_since` is the worker's core read, and goal-event scorer/assist fidelity is load-bearing for it.

Verified against the current `feature/collector-interfaces` tree:

- `NormalizedEvent` (`src/gamecollect/provider.py:70-88`) carries `player`/`assist` as plain `str | None` fields; the ESPN adapter (`src/gamecollect_football/espn.py:641-721`) parses them from ESPN's `participants` array at lines 664-672.
- `src/gamecollect/engine.py`'s `_event_to_row` (lines 125-148) projects a `NormalizedEvent` onto a writer `events` row. It builds `payload = {key: value for key, value in (("team", event.team), ("player", event.player), ("assist", event.assist)) if value is not None}` (lines 134-138) — note the payload key is currently `"player"`, not `"scorer"`. It does **not** populate `actor_entity`/`target_entity` (the docstring at line 131 says so explicitly).
- `src/gamecollect/db/schema.sql:71-72` documents `actor_entity`/`target_entity` as "soft ref -> entities.entity_id (scorer, booked player, ...)" / "(assist, subbed-off, ...)" — a comment describing behavior that does not exist in `_event_to_row`.
- `tests/test_cli.py`'s `_seed_golden_db` (lines 61-125) hand-seeds CLI golden data with a **third**, different convention: `actor_entity` = the team id (e.g. `"t-can"`), the scorer's name in `detail` (`"Jonathan David"`), and `payload = {"assist": "Alphonso Davies"}` on only one of two goal rows. `tests/golden/events.json` matches these hand-seeded values verbatim — it is checked-in CLI-golden test fixture data, not real pipeline output, and it directly contradicts both the schema comment (actor_entity=team, not scorer) and the real writer (`_event_to_row` never sets `actor_entity` at all).
- ESPN's adapter sets `detail = raw.get("text") or raw.get("shortText")` identically for every event type (`espn.py:676`); the goal-vs-card difference in what `detail` ends up containing (scorer name vs. foul reason) is an artifact of what ESPN's raw text happens to say per event type, not of branching logic in this codebase.
- `registry.py`'s `_EVENT_ITEM_SCHEMA` (lines 208-223) types `payload` as an opaque object/array/null with no documented sub-keys — there is currently no contract for what a consumer will find inside it.
- `client.py`'s `Event` dataclass (lines 104-132) exposes `payload: Any`, JSON-decoded, with no sub-typing.

**Decision (made in `/review 2` follow-up discussion, not re-litigated here):** do not implement `actor_entity`/`target_entity` entity-ref resolution. The `entities` table (`kind TEXT` — `team|player|driver`) is already sport-agnostic core schema, so entity resolution is *possible* without new tables, but it requires player-entity-row creation logic in the football pack, plus id→name denormalization in `client.py`/CLI for every consumer to re-derive names (the note's ask #2) — bigger surface for less immediate value than a typed name field. Instead: write `payload.scorer`/`payload.assist` as display-name strings. This closes ask #2 (name resolution) for free, since payload already carries names, no `entities` join required.

### Explicitly out of scope (follow-ups, not phases here)

The game-worker note raised two more items and explicitly called them "fine as follow-ups," not blocking for `contract_version 1`:

- **Alias-map exposure** (note ask #3): `TEAM_ALIASES` (`src/gamecollect_football/reconcile.py:70-76`, e.g. `"Türkiye": "Turkey"`) is already applied at the write seam via `canonical_display_name` (`reconcile.py:79`, used at lines 454-455/815/819), so `list_matches`/`get_state` already return canonicalized display names. The alias *map* itself isn't queryable by consumers. Real but small gap — candidate for a future plan if a consumer needs to do their own name→match resolution against un-canonicalized input.
- **Finished-on-first-sight backfill** (note ask #4): a match discovered already `FINISHED` with no live in-memory baseline skips `fetch_match_detail` entirely (`src/gamecollect/engine.py`, the `_fetch_poll_snapshots` function, `if match.status not in LIVE_STATUSES and not was_live: hydrated.append(match); continue`, around lines 1026-1027) — so a match the collector starts watching after it has already ended gets a final score but never its event list. This is a genuine, separate behavior gap in the poll loop, unrelated to event-payload shape, and warrants its own plan (it touches engine transition/backfill logic, not the writer's event projection).

Both are noted here for traceability back to the originating review; neither is a phase of this plan.

## Requirements

- `_event_to_row` (`engine.py`) writes `payload["scorer"]` (renamed from `"player"`) and `payload["assist"]` for any event carrying those `NormalizedEvent` fields; `payload["team"]` is unchanged. Keys are omitted (not set to `null`) when the underlying field is `None`, matching the existing omission behavior for `"team"`/`"assist"`.
- No change to `NormalizedEvent`, `NormalizedMatch`, or the ESPN adapter's parsing (`espn.py:641-721`) — the participant data is already parsed correctly; only its landing key in the stored payload changes.
- `detail` keeps carrying whatever raw text the provider sends for that event type (unchanged behavior — no attempt to strip or replace the scorer's name out of ESPN's goal text). Its contract is documented as **descriptive provider text, not to be parsed for participant identity** — this is what resolves the note's "consumers can't read it uniformly" complaint: identity now has one authoritative source (`payload.scorer`/`payload.assist`), so nothing about `detail` needs to be uniform across event types anymore.
- `schema.sql`'s `actor_entity`/`target_entity` column comments are corrected to state they are reserved/unpopulated by the core writer today, and point to `payload.scorer`/`payload.assist` as the actual current source of participant names.
- `tests/test_cli.py`'s `_seed_golden_db` and `tests/golden/events.json` are updated to a single, real convention: `actor_entity`/`target_entity` left `NULL` (matching real `_event_to_row` output), `payload` carries `scorer`/`assist` consistently across both seeded goal rows (not just one), `detail` is whatever free-text value the test chooses to represent provider text (documented as prose, not identity).
- `docs/DESIGN.md` and `README.md` gain a short documented convention for event `payload` sub-keys (`team`, `scorer`, `assist` — which event types populate which), so this becomes part of the reviewable public contract rather than an implicit test-fixture convention.
- Audit existing tests for any assertion on `payload["player"]` / `.get("player")` and update them to `"scorer"` — do not leave a stale key name silently passing because a test happens not to check that field.

## Review Focus

- Payload key rename (`"player"` → `"scorer"`) is a breaking change to anyone already depending on the pre-freeze shape — verify no shipped/tagged release exists yet (`contract_version 1` is explicitly not frozen; confirm via `git tag` / CHANGELOG before assuming this is safe) and that this plan's PR description says so plainly.
- `schema.sql` comment correction must not overstate the fix — it should say `actor_entity`/`target_entity` are unpopulated/reserved, not imply they will be filled by a specific future phase that doesn't exist yet (avoid repeating the "Phase 4" pattern that caused this gap in the first place).
- Golden-fixture correctness: `tests/golden/events.json` must be regenerated from/kept consistent with the real `_seed_golden_db` values, not hand-edited to merely look right — re-run the CLI golden test after editing the seed, don't hand-patch the JSON.
- Every place `NormalizedEvent.player`/`.assist` flows into a stored or displayed payload (grep beyond `engine.py` — check `fixture_io.py`/`replay.py`/the importer script, since fixtures round-trip through the same event shape) picks up the rename consistently; a partial rename (engine writes `"scorer"` but the importer still emits `"player"` into fixture JSON) would silently reintroduce the inconsistency this plan exists to fix.

## Implementation Checklist

### Phase 1: `payload.scorer`/`payload.assist` + contract cleanup

**Impl files:** `src/gamecollect/engine.py, src/gamecollect/db/schema.sql, src/gamecollect/fixture_io.py, scripts/import_gamealerts_fixtures.py, docs/DESIGN.md, README.md`
**Test files:** `tests/test_engine.py, tests/test_cli.py, tests/golden/events.json, tests/test_fixture_io.py, tests/test_importer.py, tests/football/test_espn_shootout_and_state.py, tests/football/test_knockout_fixtures.py`
**Test command:** `uv run pytest -q`
**Validation cmd:** `uv run gamecollect events <match_id> --db <seeded-db> --json | uv run python -m json.tool > /dev/null`

- `engine.py`: rename `_event_to_row`'s payload key from `"player"` to `"scorer"` (line ~136); leave `"team"`/`"assist"` keys and the `None`-omission behavior unchanged.
- `schema.sql`: rewrite the `actor_entity`/`target_entity` column comments (lines 71-72) to state they are reserved and unpopulated by the current writer, and cross-reference `events.payload.scorer`/`.assist` as where participant names actually live today.
- Grep `fixture_io.py`, `replay.py`, and `scripts/import_gamealerts_fixtures.py` for any place that reads/writes an event payload's `"player"` key (the importer's docstring already documents an intended `player`→entity mapping that was never implemented — correct that docstring too, not just the code) and update to `"scorer"` so fixtures, replay, and live writes all agree.
- `tests/test_cli.py`: update `_seed_golden_db` to stop setting `actor_entity` to a team id; seed `payload.scorer`/`payload.assist` consistently on both goal rows (not just one); pick one free-text convention for `detail` across goal/card rows and document in a comment that it represents opaque provider text, not a structured field.
- Regenerate `tests/golden/events.json` from the corrected seed (via the existing `UPDATE_GOLDENS=1` mechanism documented in `test_cli.py`'s module docstring) — do not hand-edit the JSON.
- Grep the full test suite for `payload["player"]` / `payload.get("player")` / equivalent and update every hit to `"scorer"`.
- `docs/DESIGN.md`: add a short "event payload contract" note — `payload` may carry `team` (team display name), `scorer` (goal events), `assist` (goal events, when provided) as plain display-name strings; `actor_entity`/`target_entity` are reserved, not currently populated.
- `README.md`: update any usage example that shows `events --json` output to reflect the corrected payload shape.
- Tests: unit test on `_event_to_row` directly asserting a goal-event `NormalizedEvent` with `player`/`assist` set produces `payload == {"team": ..., "scorer": ..., "assist": ...}` and that an event with only `team` set omits `scorer`/`assist` entirely (not `null`). CLI golden re-run confirms `events.json` matches. Football adapter/knockout tests continue to pass unchanged (they assert on `NormalizedEvent.player`/`.assist`/`.detail`, which are not renamed — only the stored payload key changes).

## Technical Specifications

### Files to Modify
- `src/gamecollect/engine.py` — `_event_to_row` payload key rename.
- `src/gamecollect/db/schema.sql` — `actor_entity`/`target_entity` comment correction.
- `src/gamecollect/fixture_io.py`, `src/gamecollect/replay.py`, `scripts/import_gamealerts_fixtures.py` — audit and align any `"player"` payload key usage; correct the importer's docstring claim about a `player`→`actor_entity` mapping.
- `tests/test_cli.py`, `tests/golden/events.json` — golden seed/data correction.
- `docs/DESIGN.md`, `README.md` — document the `payload` sub-key contract.

### New Files to Create
- None — this phase only touches existing files.

### Architecture Decisions
- **Typed payload fields over entity refs, for now**: `entities` (kind `team|player|driver`) is already sport-agnostic core schema and could host resolved player rows without new tables, but doing so requires pack-side entity-row creation plus consumer-side id→name denormalization — more surface than the near-term ask needs. Revisit if/when a consumer needs to correlate the same player entity across matches/sources (a typed name string can't do that; an entity id can).
- **`detail` keeps its current content, only its contract changes**: rather than stripping the scorer's name out of ESPN's goal-event text (which would require per-event-type special-casing the adapter never had to do before), `detail` stays raw provider text for every event type. The fix is giving consumers an authoritative alternative (`payload.scorer`/`.assist`), not scrubbing `detail`. This keeps the change minimal-impact — no `espn.py` parsing changes at all.
- **No new schema, no migration**: `contract_version 1` has not shipped/frozen yet (verify via `git tag`/CHANGELOG before implementation — Review Focus item), so the `"player"`→`"scorer"` payload key rename is a pre-freeze correction, not a breaking change requiring a version bump or migration path.

### Dependencies
- None new. Runtime remains stdlib-only per the interfaces plan.

### Integration Seams

| Seam | Writer (task) | Caller (task) | Contract |
|------|---------------|---------------|----------|
| event payload shape | Phase 1 `_event_to_row` | `client.py` `Event.payload`, CLI `events --json`, external consumers (gamealerts game-worker) | `payload` may carry `team`/`scorer`/`assist` as display-name strings, keys omitted (not null) when absent; `actor_entity`/`target_entity` remain reserved/unpopulated |
| fixture/replay round-trip | `fixture_io.py`, `replay.py`, importer script | Phase 1 (this plan) | Any code path that serializes/deserializes an event payload must use the same `"scorer"` key as the live writer, or replay/import diverges silently from live collection |

## Testing Notes

### Test Approach
- [ ] Direct unit test on `_event_to_row` for payload key correctness (present + omitted cases)
- [ ] CLI golden regeneration + re-run (`events.json` reflects corrected seed)
- [ ] Full-suite grep-and-fix pass for any residual `"player"` payload key usage (fixture_io/replay/importer)
- [ ] Football adapter/knockout tests re-run unchanged (they operate on `NormalizedEvent` fields, not the stored payload key) to confirm no regression

### Test Results
- (pending)

### Edge Cases Tested
- (pending) Goal event with scorer only (no assist) → `payload` omits `"assist"` key entirely
- (pending) Card event (no scorer/assist) → `payload` contains at most `"team"`, no `"scorer"`/`"assist"` keys
- (pending) Round-trip: record a live session with `--record`, replay it, confirm the replayed event payload still uses `"scorer"` (catches a partial rename in `fixture_io.py`/`replay.py`)

## Acceptance Criteria

- `_event_to_row` writes `payload.scorer`/`payload.assist` (not `payload.player`) for events with those fields set; omitted, not null, when absent.
- `schema.sql`'s `actor_entity`/`target_entity` comments no longer promise unimplemented behavior.
- `tests/golden/events.json` reflects a single consistent convention (no team-as-actor_entity, no single-row-only assist) and is regenerated via the golden-update mechanism, not hand-edited.
- No residual `"player"` payload key anywhere in `fixture_io.py`/`replay.py`/the importer script/its docstring.
- `docs/DESIGN.md`/`README.md` document the `payload` sub-key contract.
- Full suite green (`uv run pytest -q`), ruff clean.

<!-- not yet reviewed — run /review-plan before /conduct -->

## Progress

- [ ] Phase 1: `payload.scorer`/`payload.assist` + contract cleanup

## Findings

- (none yet)

## Issues & Solutions

- (none yet)

## Final Results

- (pending)
