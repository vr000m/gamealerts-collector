# Task: Goal-event participant contract — typed `payload.scorer`/`payload.assist`

**Status**: Complete
**Component**: collector
**Assigned to**: Claude
**Priority**: High
**Branch**: feature/goal-event-participants (branched from `main` at `46e9ca8`, post PR #2 merge)
**Created**: 2026-07-10
**Completed**: 2026-07-10

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

**Revised after `/review-plan` (2026-07-10):** the first draft of this plan proposed renaming `payload["player"]` → `payload["scorer"]` unconditionally for every event, and renaming the same key wherever `"player"` appeared across `fixture_io.py`/`replay.py`/the importer. Both were wrong and are corrected below:

- **`NormalizedEvent.player`/`.assist` are populated for every ESPN event type carrying ESPN `participants` entries, not just goals** — `espn.py:664-674` parses them generically before the type-branch, and `_ESPN_EVENT_TYPE_MAP` (`espn.py:460-468`) plus `_classify_goal_slug` (`espn.py:484-496`) together emit `goal`, `own_goal`, `penalty` (awarded/missed, not scored), `yellow`, `red`, and `sub`. A blanket rename would relabel a substitution's subbed-off/subbed-on players, or a booked player on a card, as `payload.scorer`/`.assist` — a domain-meaning error, not a formatting one. **Fix:** the `"scorer"`/`"assist"` payload keys are written **only** when `event.event_type in {"goal", "own_goal"}`; every other event type keeps today's `"player"`/`"assist"` payload keys completely unchanged. This is a strictly additive, narrower change than originally scoped.
- **`fixture_io.py`, `replay.py`, and the importer's `row["player"]` reads are a different `"player"` than the one being renamed** — `fixture_io.py:99`/`:200` and the importer's `SELECT ... player ...` (`import_gamealerts_fixtures.py:192`/`:203`) serialize/read the `NormalizedEvent.player` **field name** (which this plan does not rename) and the external gamealerts DB's **column name** (an external contract, per the importer's own docstring), respectively — neither is the `events.payload` JSON sub-key this plan is renaming. Replay re-projects `NormalizedEvent` through `_event_to_row` on every poll, so `payload.scorer` regenerates from `.player` automatically; fixtures storing `"player"` need no change and renaming them would break importing real gamealerts DBs. **Fix:** `fixture_io.py`, `replay.py`, and the importer's code are removed from this plan's scope entirely. Only the importer's *docstring* (which incorrectly claims the writer lands `player` in the payload) is corrected, as a doc-only edit.
- **`engine.py`'s `_stored_events` (line 884) reads the same stored payload back** via `extras.get("player")`/`extras.get("assist")` on the resumption/baseline-reconstruction path (`engine.py:862-889`) and was missing from the original scope — it must change symmetrically with `_event_to_row` in the same commit, gated on the same `event_type` check, or scorer data silently vanishes (becomes `None`) after any engine restart.
- **ESPN's participant ordering (`participants[0]`=scorer, `[1]`=assist) is asserted only by a code comment** (`espn.py:665`), not a documented ESPN contract or an explicit role field (`participants[].type` is `null` in every sampled fixture). This is accepted as a named assumption, not re-verified here — see Requirements.
- **Own-goal scorer attribution is inverted relative to a normal goal**: for `own_goal`, `participants[0]` is the player who scored into their own net, and `event.team` is that player's team — the team that did *not* benefit from the goal. `payload.scorer` on an `own_goal` row therefore names a player under the "wrong" (conceding) team by normal-goal convention. Documented explicitly in Requirements/DESIGN.md rather than special-cased away, since the underlying `NormalizedEvent.team`/`.player` semantics for own goals are pre-existing behavior this plan does not change.

### Explicitly out of scope (follow-ups, not phases here)

The game-worker note raised two more items and explicitly called them "fine as follow-ups," not blocking for `contract_version 1`:

- **Alias-map exposure** (note ask #3): `TEAM_ALIASES` (`src/gamecollect_football/reconcile.py:70-76`, e.g. `"Türkiye": "Turkey"`) is already applied at the write seam via `canonical_display_name` (`reconcile.py:79`, used at lines 454-455/815/819), so `list_matches`/`get_state` already return canonicalized display names. The alias *map* itself isn't queryable by consumers. Real but small gap — candidate for a future plan if a consumer needs to do their own name→match resolution against un-canonicalized input.
- **Finished-on-first-sight backfill** (note ask #4): a match discovered already `FINISHED` with no live in-memory baseline skips `fetch_match_detail` entirely (`src/gamecollect/engine.py`, the `_fetch_poll_snapshots` function, `if match.status not in LIVE_STATUSES and not was_live: hydrated.append(match); continue`, around lines 1026-1027) — so a match the collector starts watching after it has already ended gets a final score but never its event list. This is a genuine, separate behavior gap in the poll loop, unrelated to event-payload shape, and warrants its own plan (it touches engine transition/backfill logic, not the writer's event projection).

Both are noted here for traceability back to the originating review; neither is a phase of this plan.

## Requirements

- `_event_to_row` (`engine.py`) writes `payload["scorer"]`/`payload["assist"]` **only when `event.event_type in {"goal", "own_goal"}`**; for every other emitted event type (`penalty`, `yellow`, `red`, `sub`, and any future taxonomy addition), `payload["player"]`/`payload["assist"]` are written exactly as today — unchanged key names, unchanged behavior. `payload["team"]` is unchanged in both branches. Keys are omitted (not set to `null`) when the underlying field is `None`, matching the existing omission behavior.
- `_stored_events` (`engine.py:862-889`) is updated symmetrically: it must read back `extras.get("scorer")`/`extras.get("assist")` when reconstructing a `goal`/`own_goal` row, and `extras.get("player")`/`extras.get("assist")` for every other `event_type`, so the resumption/baseline-reconstruction path reconstructs an equivalent `NormalizedEvent` for both branches. This is not optional/deferred — it lands in the same commit as `_event_to_row`.
- No change to `NormalizedEvent`, `NormalizedMatch`, or the ESPN adapter's parsing (`espn.py:641-721`) — the participant data is already parsed correctly; only its landing key in the stored payload changes, and only for goal-family events.
- `detail` keeps carrying whatever raw text the provider sends for that event type (unchanged behavior — no attempt to strip or replace the scorer's name out of ESPN's goal text). Its contract is documented as **descriptive provider text, not to be parsed for participant identity** for goal-family events (where `payload.scorer`/`.assist` is now the authoritative source); non-goal `detail` content is unaffected by this plan.
- `schema.sql`'s `actor_entity`/`target_entity` column comments, and `engine.py:131`'s `_event_to_row` docstring (which repeats the same stale "pack importer's job in Phase 4" claim), are both corrected to state they are reserved/unpopulated by the core writer today, and point to `payload.scorer`/`payload.assist` (goal-family events only) as the actual current source of participant names.
- `tests/test_cli.py`'s `_seed_golden_db` and `tests/golden/events.json` are updated to a single, real convention for the seeded goal rows: `actor_entity`/`target_entity` left `NULL` (matching real `_event_to_row` output), `payload` carries `scorer`/`assist` consistently across both seeded goal rows (not just one), `detail` is whatever free-text value the test chooses to represent provider text (documented as prose, not identity). This golden data remains intentionally hand-seeded/synthetic for CLI-shape testing (it always was — the plan does not change that); the guarantee that the real writer actually produces this shape is asserted by the new direct engine test below, not by the golden fixture.
- `docs/DESIGN.md` §6 and `README.md` gain a short documented convention for event `payload` sub-keys: `team` (always, when known), `scorer`/`assist` (goal-family events — `goal`, `own_goal` — as plain display-name strings), `player`/`assist` (every other event type, unchanged pre-existing behavior). Explicitly document the own-goal attribution quirk: on an `own_goal` row, `payload.scorer` names the player who scored into their own net, and `event.team`/the row's team attribution is that player's team — the team that did *not* benefit from the goal.
- Explicitly name two assumptions in `docs/DESIGN.md` (not verifiable from this codebase, carried forward from the original ESPN parsing this plan does not change): (1) ESPN orders goal `participants` scorer-first, assist-second, with no explicit role field to confirm it — inferred from a code comment (`espn.py:665`) and corroborated only by a small fixture sample (~6 goals, no observed own-goal-with-assist case); (2) the specific free-text content of ESPN's `detail`/`shortText` per event type is provider behavior this repo does not control or verify.
- Audit existing tests for any assertion on `payload["player"]`/`.get("player")` on a **goal or own_goal** `NormalizedEvent` and update them to `"scorer"`; assertions against `payload["player"]` on non-goal event types (subs, cards) are correct as-is and must NOT change.
- Add a direct unit test on the real `_event_to_row` (not the CLI golden path) asserting: (a) a `goal`/`own_goal` `NormalizedEvent` with `player`/`assist` set produces `payload == {"team": ..., "scorer": ..., "assist": ...}`; (b) a `sub`/`yellow`/`red`/`penalty` `NormalizedEvent` with the same fields set produces `payload == {"team": ..., "player": ..., "assist": ...}` (unchanged); (c) `_stored_events` round-trips both shapes back to an equivalent `NormalizedEvent`.
- `fixture_io.py`, `replay.py`, and the importer script's **code** are explicitly out of scope — see Context's "Revised after `/review-plan`" note. Only the importer's docstring (its stale claim about where `player`/`assist` land) and `README.md`'s usage example are doc-only edits.

## Review Focus

- The goal-family gate (`event_type in {"goal", "own_goal"}`) must be applied identically in `_event_to_row` and `_stored_events` — a mismatch between the two (e.g. writer gates on the check but the reader doesn't) reintroduces exactly the resumption-path data loss this plan's `/review-plan` pass caught.
- Confirm `fixture_io.py`/`replay.py`/the importer's code genuinely need no change: verify `ReplayProvider` reconstructs `NormalizedEvent` objects that flow back through the real `_event_to_row` (not a cached/precomputed payload) on every poll, so `payload.scorer` regenerates correctly without any fixture-format change.
- Payload key rename (`"player"` → `"scorer"`, goal-family only) is a breaking change to anyone already depending on the pre-freeze shape — verify no shipped/tagged release exists yet (`contract_version 1` is explicitly not frozen; confirmed via `git tag`/CHANGELOG during `/review-plan` — empty tag list, no CHANGELOG file) and that this plan's PR description says so plainly.
- `schema.sql`/`engine.py:131` comment corrections must not overstate the fix — state `actor_entity`/`target_entity` as unpopulated/reserved, not imply they will be filled by a specific future phase that doesn't exist yet (avoid repeating the "Phase 4" pattern that caused this gap in the first place).
- Golden-fixture correctness: `tests/golden/events.json` must be regenerated from/kept consistent with the real `_seed_golden_db` values, not hand-edited to merely look right — re-run the CLI golden test after editing the seed, don't hand-patch the JSON. The golden path remains synthetic by design; do not conflate it with proof that the real writer produces this shape (that's the new direct engine test's job).

## Implementation Checklist

### Phase 1: `payload.scorer`/`payload.assist` (goal-family only) + contract cleanup

**Impl files:** `src/gamecollect/engine.py, src/gamecollect/db/schema.sql, docs/DESIGN.md, README.md, scripts/import_gamealerts_fixtures.py`
**Test files:** `tests/test_engine.py, tests/test_cli.py, tests/golden/events.json, tests/football/test_espn_shootout_and_state.py, tests/football/test_knockout_fixtures.py`
**Test command:** `uv run pytest -q`
**Validation cmd:** `uv run gamecollect events <match_id> --db <seeded-db> --json | uv run python -m json.tool > /dev/null`

- `engine.py`: change `_event_to_row`'s payload-building dict comprehension (line ~136) to a conditional — when `event.event_type in {"goal", "own_goal"}`, build `payload` with keys `"team"`/`"scorer"`/`"assist"`; otherwise (every other `event_type`), keep today's `"team"`/`"player"`/`"assist"` keys exactly as-is. Leave the `None`-omission behavior unchanged in both branches.
- `engine.py`: update `_stored_events` (lines 862-889) symmetrically — reconstruct `NormalizedEvent.player` from `extras.get("scorer")` when `event_type in {"goal", "own_goal"}`, else from `extras.get("player")` as today. This must land in the same commit as the `_event_to_row` change; a test asserting round-trip equivalence for both branches is required (see Testing Notes).
- `engine.py`: also correct line 131's `_event_to_row` docstring (the stale "pack importer's job in Phase 4" claim) alongside the `schema.sql` fix below.
- `schema.sql`: rewrite the `actor_entity`/`target_entity` column comments (lines 71-72) to state they are reserved and unpopulated by the current writer, and cross-reference `events.payload.scorer`/`.assist` (goal-family events only) as where participant names actually live today.
- `scripts/import_gamealerts_fixtures.py`: **doc-only** — correct the docstring (lines ~28-33) that claims "the engine's writer lands `team`/`player`/`assist` in the event payload"; the importer's SQL (`row["player"]`, `SELECT ... player ...`) reads a column in the external gamealerts DB schema and stays unchanged — it is not the same `"player"` this plan renames. Do **not** touch `fixture_io.py` or `replay.py` — they serialize/read the un-renamed `NormalizedEvent.player` field, not `events.payload`, and need no change (see Context's "Revised after `/review-plan`" note).
- `tests/test_cli.py`: update `_seed_golden_db` to stop setting `actor_entity` to a team id; seed `payload.scorer`/`payload.assist` consistently on both goal rows (not just one); pick one free-text convention for `detail` across goal/card rows and document in a comment that it represents opaque provider text, not a structured field.
- Regenerate `tests/golden/events.json` from the corrected seed (via the existing `UPDATE_GOLDENS=1` mechanism documented in `test_cli.py`'s module docstring) — do not hand-edit the JSON.
- Grep the full test suite for `payload["player"]`/`payload.get("player")` asserted against a **goal or own_goal** `NormalizedEvent` and update those hits to `"scorer"`. Leave every non-goal-type assertion (subs, cards) untouched — they are correct as-is.
- `docs/DESIGN.md` §6: add the "event payload contract" note (goal-family `scorer`/`assist` vs. everything-else `player`/`assist`, the own-goal attribution quirk, and the two named ESPN-behavior assumptions) per Requirements.
- `README.md`: update any usage example that shows `events --json` output to reflect the corrected payload shape for a goal event.
- Tests: (1) direct unit test on `_event_to_row` for a `goal`/`own_goal` event asserting `payload == {"team": ..., "scorer": ..., "assist": ...}` with keys omitted (not null) when absent; (2) direct unit test on `_event_to_row` for a `sub`/`yellow`/`red`/`penalty` event asserting `payload == {"team": ..., "player": ..., "assist": ...}` unchanged; (3) `_stored_events` round-trip test for both branches; (4) CLI golden re-run confirms `events.json` matches; (5) football adapter/knockout tests continue to pass unchanged (they assert on `NormalizedEvent.player`/`.assist`/`.detail`, which are not renamed — only the stored payload key changes, and only for goal-family events).

## Technical Specifications

### Files to Modify
- `src/gamecollect/engine.py` — `_event_to_row` payload key rename (goal-family gated), symmetric `_stored_events` update, stale `Phase 4` docstring fix (line 131).
- `src/gamecollect/db/schema.sql` — `actor_entity`/`target_entity` comment correction.
- `scripts/import_gamealerts_fixtures.py` — docstring-only correction; its SQL/column reads are unchanged (see Architecture Decisions).
- `tests/test_cli.py`, `tests/golden/events.json` — golden seed/data correction.
- `docs/DESIGN.md`, `README.md` — document the `payload` sub-key contract, including the goal-family gate and the own-goal attribution quirk.

**Explicitly not modified** (in scope in the original draft, removed after `/review-plan`): `src/gamecollect/fixture_io.py`, `src/gamecollect/replay.py`. Both serialize/read the un-renamed `NormalizedEvent.player` field, not `events.payload` — see Context.

### New Files to Create
- None — this phase only touches existing files.

### Architecture Decisions
- **Typed payload fields over entity refs, for now**: `entities` (kind `team|player|driver`) is already sport-agnostic core schema and could host resolved player rows without new tables, but doing so requires pack-side entity-row creation plus consumer-side id→name denormalization — more surface than the near-term ask needs. Revisit if/when a consumer needs to correlate the same player entity across matches/sources (a typed name string can't do that; an entity id can).
- **`detail` keeps its current content, only its contract changes**: rather than stripping the scorer's name out of ESPN's goal-event text (which would require per-event-type special-casing the adapter never had to do before), `detail` stays raw provider text for every event type. The fix is giving consumers an authoritative alternative (`payload.scorer`/`.assist`, goal-family only), not scrubbing `detail`. This keeps the change minimal-impact — no `espn.py` parsing changes at all.
- **Goal-family gate, not a universal rename**: `NormalizedEvent.player`/`.assist` carry different domain meanings depending on `event_type` — scorer/assist on `goal`/`own_goal`, but a booked player on `yellow`/`red`, or the two swapped players on `sub`. Renaming the payload key universally would relabel non-goal participants as "scorer". Gating on `event_type in {"goal", "own_goal"}` is the minimal-impact fix: goal-family events get the new, correctly-named keys; every other event type is untouched, preserving exactly today's behavior.
- **`fixture_io.py`/`replay.py`/importer code stay untouched**: the `"player"` token in each of these three places is a *different* thing from the `events.payload` sub-key being renamed — a dataclass field-mirror key (`fixture_io.py`), a whole-payload passthrough with no `"player"` reference at all (`replay.py`), and an external gamealerts DB column name (the importer, per its own docstring: "the gamealerts events schema is external to this repo, so this map is the contract"). `ReplayProvider` reconstructs `NormalizedEvent` objects that flow back through the real `_event_to_row` on every poll, so `payload.scorer` regenerates correctly with zero fixture-format changes. Renaming any of the three would either be a no-op (`replay.py`) or actively break importing real gamealerts DBs (the importer).
- **No new schema, no migration**: `contract_version 1` has not shipped/frozen yet (confirmed via `git tag`/CHANGELOG during `/review-plan` — empty tag list, no CHANGELOG file), so the goal-family `"player"`→`"scorer"` payload key change is a pre-freeze correction, not a breaking change requiring a version bump or migration path.
- **No `## Architecture & Call Flow` section**: after narrowing scope to `engine.py` only (writer `_event_to_row` + reader `_stored_events`, both in the same process, same file), this plan touches a single execution component — not the 2+ independently-executing components that section exists to diagram. `fixture_io.py`/`replay.py`/the importer, which would have made this multi-component, are explicitly out of scope (see above).

### Dependencies
- None new. Runtime remains stdlib-only per the interfaces plan.

### Integration Seams

| Seam | Writer (task) | Caller (task) | Contract |
|------|---------------|---------------|----------|
| event payload shape | Phase 1 `_event_to_row` (+ symmetric `_stored_events`) | `client.py` `Event.payload`, CLI `events --json`, external consumers (gamealerts game-worker) | `payload` carries `team`/`scorer`/`assist` (goal-family: `goal`, `own_goal`) or `team`/`player`/`assist` (every other event type, unchanged), display-name strings, keys omitted (not null) when absent; `actor_entity`/`target_entity` remain reserved/unpopulated |
| fixture/replay round-trip | `fixture_io.py`, `replay.py` (unmodified) | `ReplayProvider` → engine → `_event_to_row` | The round-trip contract is the un-renamed `NormalizedEvent.player` field, not a payload key — fixtures/replay need no change because the engine re-projects every replayed event through the same `_event_to_row` a live poll uses |

## Testing Notes

### Test Approach
- [ ] Direct unit test on `_event_to_row` for goal-family payload key correctness (`scorer`/`assist` present/omitted cases)
- [ ] Direct unit test on `_event_to_row` for non-goal event types confirming `payload["player"]` is unchanged (regression guard against the goal-family gate leaking)
- [ ] `_stored_events` round-trip test for both branches (goal-family and non-goal), proving resumption/baseline reconstruction is symmetric with the writer
- [ ] CLI golden regeneration + re-run (`events.json` reflects corrected seed)
- [ ] Full-suite grep-and-fix pass for any `payload["player"]` assertion against a goal/own_goal event specifically (leave non-goal assertions untouched)
- [ ] Football adapter/knockout tests re-run unchanged (they operate on `NormalizedEvent` fields, not the stored payload key) to confirm no regression
- [ ] Confirm `fixture_io.py`/`replay.py` genuinely need no change: a replay round-trip test showing a replayed goal event's stored payload uses `"scorer"` (proving the engine's re-projection, not a fixture-format change, is what makes this work)

### Test Results
- (pending)

### Edge Cases Tested
- (pending) Goal event with scorer only (no assist) → `payload` omits `"assist"` key entirely
- (pending) Own-goal event → `payload.scorer` present per the documented (team-inverted) attribution convention, not special-cased or dropped
- (pending) Substitution/card event with two participants set (`.player`/`.assist` both non-None) → `payload` still uses `"player"`/`"assist"` keys, NOT `"scorer"` — the negative case proving the gate doesn't leak
- (pending) Engine restart mid-match (baseline rebuilt via `_stored_events`) for a goal event → reconstructed `NormalizedEvent.player`/`.assist` match the original, proving the resumption path isn't silently dropping scorer data
- (pending) Round-trip: record a live session with `--record`, replay it, confirm the replayed goal event's stored payload still uses `"scorer"` (proving engine re-projection makes `fixture_io.py`/`replay.py` changes unnecessary, not that they were made)

## Acceptance Criteria

- `_event_to_row` writes `payload.scorer`/`payload.assist` for `goal`/`own_goal` events with those fields set (omitted, not null, when absent); every other event type's payload is byte-for-byte unchanged (`payload.player`/`.assist`, as today).
- `_stored_events` reconstructs both branches symmetrically; a resumption/restart does not lose scorer data on goal-family events.
- `schema.sql`'s and `engine.py:131`'s `actor_entity`/`target_entity`/"Phase 4" comments no longer promise unimplemented behavior.
- `tests/golden/events.json` reflects a single consistent convention (no team-as-actor_entity, no single-row-only assist) and is regenerated via the golden-update mechanism, not hand-edited.
- `fixture_io.py` and `replay.py` are unmodified; the importer's docstring no longer misdescribes the payload contract.
- `docs/DESIGN.md`/`README.md` document the `payload` sub-key contract, the goal-family gate, the own-goal attribution quirk, and the two named ESPN-behavior assumptions (participant ordering, per-type `detail` text content).
- Full suite green (`uv run pytest -q`), ruff clean.

<!-- reviewed: 2026-07-10 @ 57552600df25ce5c2655c34a9f627f399df1beeb -->

## Progress

- [x] Phase 1: `payload.scorer`/`payload.assist` + contract cleanup

## Findings

- Mid-phase advisory review (opus) found no issues: goal-family gate is symmetric between `_event_to_row`/`_stored_events`, `fixture_io.py`/`replay.py` genuinely unmodified, non-goal event types unchanged, comments don't overstate the fix, golden fixture regenerated (not hand-patched) via `UPDATE_GOLDENS=1`.

## Issues & Solutions

- Test-writer found and fixed an out-of-scope gap: `tests/test_replay.py`'s `_event_player()` helper read stored payload via `"player"` only; since `ReplayProvider` re-projects events through the real (changed) `_event_to_row`, its end-to-end replay assertion broke once the goal-family key rename landed. Fixed by checking `"scorer"` before falling back to `"player"`/`actor_entity`.

## Final Results

- Phase 1 complete (commit `9425e03`). `_event_to_row` writes `payload.scorer`/`payload.assist` for `goal`/`own_goal` events (keys omitted, not null, when absent); every other event type's payload is unchanged (`payload.player`/`.assist`). `_stored_events` reconstructs both branches symmetrically. `schema.sql` and `engine.py`'s `_event_to_row` docstring no longer promise unimplemented `actor_entity`/`target_entity` resolution. `tests/golden/events.json` regenerated to a single consistent convention. `fixture_io.py`/`replay.py` unmodified; importer docstring corrected. `docs/DESIGN.md` §6/`README.md` document the payload sub-key contract, the goal-family gate, the own-goal attribution quirk, and the two named ESPN assumptions. Full suite green: 495 passed, 2 skipped. No further phases in this plan.
