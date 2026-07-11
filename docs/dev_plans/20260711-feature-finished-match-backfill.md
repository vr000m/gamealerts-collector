# Task: Finished-on-first-sight event backfill

**Status**: Not Started
**Component**: collector
**Assigned to**: Claude
**Priority**: High
**Branch**: feature/finished-match-backfill (branched from `main` at `f7c9db9`, post PR #3 merge)
**Created**: 2026-07-11

## Objective

When the collector's poll loop first sees a match that is **already `FINISHED`** — no in-memory baseline, no stored row, never previously seen live — fetch its full event list once and write it through the normal reconcile/writer path, instead of silently recording only the scoreboard snapshot (score + status, zero events) as happens today.

## Context

The poll loop only calls `fetch_match_detail` (the event-carrying endpoint) for a match that is currently live, or was live and is transitioning to a terminal state (`was_live` / `in_transition`, `src/gamecollect/engine.py:1044-1092`). A match first observed already `FINISHED` — the collector started late, or the match ended before it was ever polled — skips straight past that branch:

```python
# src/gamecollect/engine.py:1067-1069
if match.status not in LIVE_STATUSES and not was_live:
    hydrated.append(match)
    continue
```

`match` here is the bare scoreboard snapshot from `fetch_live_matches` — for the real ESPN provider its `events` list is always empty (`_normalize_scoreboard_event`, `src/gamecollect_football/espn.py:862-951`, builds a `NormalizedMatch` with no `events=` kwarg, docstring "score only"). So a match that finished before the collector was watching gets a score/status row and **no goal/card events, ever** — the collector has already committed to never re-fetching this match's detail once a stored row exists, since it is no longer `was_live` on any future poll either.

**Why it matters:** the gamealerts intelligence-plane game-worker (PR #33) consumes `get_events_since` to answer "who scored / assists / highlights." A match that ended before the collector watched it yields only the scoreboard tier, leaving every event-level question about that match permanently unanswerable — there is no later point at which the engine would revisit it. This was raised as review note ask #4 on PR #2 and explicitly deferred in `docs/dev_plans/20260710-feature-goal-event-participants.md` (Final Results) as "a genuine, separate behavior gap in the poll loop... warrants its own plan." This is that plan.

**Depends on / reuses:** `docs/dev_plans/20260710-feature-goal-event-participants.md` (Complete) — this plan's backfilled events must land with that plan's `payload.scorer`/`payload.assist`/`payload.team` contract on goal-family types, by reusing the *same* `_event_to_row`/writer path a live match's events already go through. No new payload shape, no side path.

## Requirements

- **Detection**: on a poll where a match is `FINISHED` (or otherwise terminal) with `previous is None`, `was_live is False`, and the bare scoreboard snapshot carries no events (`not match.events`) and no stored `matches` row exists for this provider id (via the existing `_stored_rows_for_provider` lookup, narrow-columns mode) — this is "first sight of an already-finished match." Gating on `not match.events` (rather than a provider-type check) is what confines the new behavior to real ESPN-shaped providers without the core engine knowing about provider identity — see Technical Specifications.
- **One-time fetch, normal path**: on detection, call `fetch_match_detail` once and merge exactly as the existing live/transition branch does (`_merge_detail`), then `hydrated.append(merged)` so the normal `diff_matches` → `_apply` → `_event_to_row` → `PartitionWriter.append_events` chain persists events with the current payload contract. No bespoke write path.
- **Idempotent, one-shot, no double-fetch**: once a match has a stored `matches` row (success or exhausted give-up — see below), it must never be re-fetched for this reason again. A match seen live and *then* finishing is already excluded by construction (`was_live` is `True` for it, so it never satisfies the "first sight" detection above; it stays on the existing live→transition path unchanged).
- **Bounded, non-fatal failure handling**: a `ProviderUnavailableError`/`ShapeDriftError` on this one-time fetch must not crash the poll or the match. Do not immediately persist the bare snapshot on failure (that would create a stored row and permanently foreclose the retry this same "no stored row" detection depends on). Instead retry on subsequent polls up to a small bounded cap, then give up and persist the best-known (scoreboard-only) snapshot — mirroring the existing `_TransitionTracker`/`_emit_give_up`/cooldown shape (`consecutive_failures`, `total_attempts`, a cap, a give-up that persists a merged-never-raw snapshot) rather than inventing a new failure-handling shape. **Implementation MUST use a tracker container separate from `self._transitions`** (e.g. a new `self._backfill: dict[str, _TransitionTracker]`) — `self._transitions` membership feeds the `was_live` computation (`engine.py:1058-1060`) and is separately consumed by `_hydrate_vanished_matches` and `_resolve_tracker`/`_emit_give_up`; parking a backfill retry entry in that same dict would flip `was_live` to `True` on the very next poll, misrouting the match onto the live→terminal transition/give-up path instead of retrying as first-sight. To satisfy this *without* duplicating the give-up/retry logic (see Review Focus), the give-up/resolve helpers (`_emit_give_up`, `_resolve_tracker`, and any cap/fallback-accrual helper) MUST be parametrized over which tracker dict they operate on (e.g. an explicit `tracker_map` argument), so the same code runs against either `self._transitions` or `self._backfill` — this is not left to the implementer; it is the required design.
- **Cold-start cost is bounded by construction, not by a new rate limiter — pending live-network verification**: `fetch_live_matches` returns ESPN's scoreboard for **the current day's slate only**, per the `fetch_schedule`/`_fetch_scoreboard` docstrings (`src/gamecollect_football/espn.py:809-843` — no `dates` param means "current day"). This is the code's own documented intent, but the actual size of ESPN's default scoreboard response is unverified ESPN backend behavior, not a code fact — **named as an assumption, verified in Phase 1** (see Phase 1's live-network gate). If verification shows the slate is not small, the "no new per-poll cap" Architecture Decision below must be revisited before Phase 1 proceeds further. Each first-sight-finished match triggers exactly one `fetch_match_detail` call, structurally identical in cost to an existing live→transition detail fetch, processed sequentially within the same poll exactly as concurrent transitions already are today. No new per-poll cap or queue is introduced by default — see Architecture Decisions for why a naive per-poll cap would break the one-shot/idempotency contract (deferred candidates would get a stored row written anyway via the existing skip-branch fallback, which would then block their retry forever).
- **Opt-out escape hatch, landing in Phase 1 (not deferred)**: add a `backfill_finished_matches: bool = True` keyword to `CollectorEngine.__init__`, defaulting ON, wired to a `--no-finished-backfill` flag on the `collect` CLI subcommand (`src/gamecollect/cli.py`) for an operator who wants to explicitly disable this behavior (e.g. a source known to have a very large historical same-day slate, or a debugging need to reproduce pre-this-plan behavior). Both the constructor flag and the CLI flag ship together in Phase 1, since the feature is ON by default from the moment Phase 1 lands — there must be no window where the behavior is live with no kill switch. No recency-window knob beyond "today's slate" is introduced — out of scope; see Follow-up Work.
- **Two live-network prerequisites, both unverified against live ESPN, both gating Phase 1 with an explicit STOP if either fails** (this is a hard go/no-go, not an assumption to carry forward silently — see Phase 1's first task and Review Focus):
  1. **Scoreboard inclusion**: does ESPN's default (no-`dates`) scoreboard response actually list a match that finished earlier the same day, or does ESPN drop finished matches from the live scoreboard? If it drops them, this feature's entire trigger is dead code — `fetch_live_matches` would never surface the match for the detection branch to see. `fetch_schedule` (ESPN-only, not on the `MatchDataProvider` ABC — `ReplayProvider` has no equivalent) is a possible fallback data source, but adopting it would mean widening the provider ABC, which is out of scope for this plan. **If scoreboard inclusion fails, STOP and return to `/dev-plan update` before proceeding — do not silently degrade or improvise an ABC change inline.**
  2. **Detail-fetch shape**: no existing test exercises `fetch_match_detail` against a genuinely `FINISHED` match id, and a 200 response is necessary but not sufficient — `assert_espn_summary_shape` (`espn.py:388-435`) additionally requires `boxscore.teams` to carry a `possessionPct` stat, plus non-trivial `keyEvents`, `rosters`, and `commentary`. An old/finished match's summary could 200 and still fail this shape gate (routing straight to Phase 2's give-up path despite a "successful" fetch), or 200 with an empty `keyEvents` (no `ShapeDriftError`, but nothing to backfill either). **Phase 1 must verify a live finished-match summary passes the shape assertion AND yields non-empty `keyEvents` — record both outcomes in `## Findings`, and STOP if either check fails.**
- **Replay/fixtures are unaffected by construction, not by a special case**: `ReplayProvider.fetch_match_detail` returns the exact same `_snapshot()` as `fetch_live_matches` (`src/gamecollect/replay.py:116-141`) — for a fixture recorded as `FINISHED`, `_snapshot().status` is the fixture's final recorded status *from the very first poll* (`_snapshot`, `replay.py:179-186`, "fixture header state verbatim"), so replay's bare scoreboard snapshot already carries the currently-revealed event prefix directly on `match.events`. The `not match.events` detection gate is therefore naturally `False` for every ReplayProvider poll with any revealed events, and `True` only for a genuinely zero-event first poll (e.g. a step-mode fixture's very first reveal, or a `SCHEDULED`/zero-event fixture) — in that edge case an extra `fetch_match_detail` call is harmless (it does **not** advance the step-mode `_revealed` cursor, `replay.py:124-129` vs `131-141` — only `fetch_live_matches` does) but is still one avoidable call. Confirm in Phase 1 testing whether existing replay/determinism tests assert exact `fetch_match_detail` call counts; if any do, either accept the (harmless, no-op-merge) extra call and update the assertion, or gate the new branch on `provider is not ReplayProvider`-style exclusion as a fallback — decide from what Phase 1 testing actually finds, do not guess ahead of it.

## Review Focus

- **One-shot correctness under partial-poll failure**: verify no code path can create a stored `matches` row for a first-sight-finished match without either (a) a successful detail fetch merged in, or (b) an exhausted retry-cap give-up. Any path that persists the bare snapshot on the *first* failed attempt silently forecloses all future backfill for that match (the "no stored row" detection would never re-fire). Verify this with a **direct** assertion — after the first failed poll, `_stored_rows_for_provider(match_id)` returns empty — not only via the eventual fail-then-succeed end state.
- **No double-fetch for a live→finished match**: confirm the detection condition (`previous is None and not was_live`) is structurally exclusive with the existing `in_transition` (`was_live and not live`) branch — a match must never satisfy both in the same poll.
- **Separate tracker container, shared give-up/retry code**: the backfill retry tracker MUST live in a container separate from `self._transitions` (its membership feeds `was_live`, `_hydrate_vanished_matches`, and `_resolve_tracker` — reusing that exact dict for backfill entries would misroute a retrying first-sight match onto the live→terminal transition path on its very next poll). At the same time, the give-up/retry *code* (cap, fallback-accrual, give-up-persists-merged-never-raw) must be reused via a shared helper parametrized over which tracker dict it operates on — not reimplemented in parallel with subtly different semantics. Both halves of this constraint are required; satisfying only one (same dict, or a hand-duplicated give-up path) is a defect.
- **Restart interaction**: a first-sight-finished match whose backfill retry is exhausted-but-not-yet-given-up when the collector restarts has no durable tracker (trackers are in-memory only, matching today's transition-tracker posture) — confirm the plan's chosen behavior (restart resets its retry budget, i.e. treated as first-sight again) is an accepted, bounded edge case consistent with existing tracker-restart behavior, not a new hidden regression.
- **Live-network prerequisites are a hard gate, not a soft caveat**: Phase 1's first task must have a defined STOP outcome (return to `/dev-plan update`, do not proceed) if either the scoreboard-inclusion or detail-fetch-shape prerequisite (Requirements) fails — a check with no failure branch is not a gate.

## Implementation Checklist

### Phase 1: Live-network gate + detection + one-time fetch (happy path)

**Impl files:** `src/gamecollect/engine.py`, `src/gamecollect/cli.py`
**Test files:** `tests/test_engine.py`, `tests/test_cli.py`
**Test command:** `uv run pytest tests/test_engine.py tests/test_cli.py -q`
**Validation cmd:** `uv run pytest -q`
**Goal:** A genuinely first-sight `FINISHED` match gets exactly one `fetch_match_detail` call and its events land via the existing `_event_to_row`/writer path with the PR #3 payload contract; a match seen live before finishing is untouched; the feature ships with its kill switch, not after it. Until Phase 2 lands, a fetch failure on a first-sight-finished match falls through to the pre-existing bare-snapshot persist (`engine.py:1116-1120`) and forecloses retry — this is no regression vs. today, but the feature's failure-tolerance guarantee is not met until Phase 2; do not treat Phase 1 alone as feature-complete.

- **Hard gate, STOP on failure**: verify against live ESPN (manual/live-network check, or existing fixture evidence if any surfaces) both (a) that a match finished earlier the same day actually appears in `fetch_live_matches`'s default scoreboard response, and (b) that its `fetch_match_detail` summary passes `assert_espn_summary_shape` and yields non-empty `keyEvents`. Record both outcomes in `## Findings`. **If either check fails, STOP — do not proceed with the detection wiring below; return to `/dev-plan update` to redesign the trigger (e.g. `fetch_schedule`, which requires widening the provider ABC) before continuing.**
- Add the detection condition at the existing skip branch (`engine.py:1067-1069`): `match.status not in LIVE_STATUSES and not was_live and not match.events and` no stored row for this provider id (reuse `_stored_rows_for_provider(match.match_id, columns=_STORED_LIVENESS_COLUMNS)` returning empty).
- On detection, call `fetch_match_detail`, merge via the existing `_merge_detail`, and `hydrated.append(merged)` — reuse the identical success path the live/transition branch already uses (do not duplicate merge logic).
- Add `backfill_finished_matches: bool = True` to `CollectorEngine.__init__`; when `False`, the new detection condition is never evaluated (behavior is byte-for-byte today's skip branch).
- Add `--no-finished-backfill` to the `collect` subparser (`cli.py:224-239`), wired to `CollectorEngine(..., backfill_finished_matches=not args.no_finished_backfill)` — lands here, alongside the constructor flag, not deferred to a later phase.
- Unit tests: first-sight-`FINISHED` match with no stored row triggers exactly one `fetch_match_detail` call and its events are persisted, and re-polling the same finished match a second time does not re-fetch (stored-row gate) or duplicate; a first-sight-finished match whose events include a goal-family event lands rows whose `payload` carries `scorer`/`assist`/`team` (the determinism fixture used must include at least one goal with participants); a match observed live then finishing does **not** trigger this new branch (assert `fetch_match_detail` call count unchanged from today's behavior for that case); `backfill_finished_matches=False` reproduces exactly today's skip-branch behavior; a `collect --no-finished-backfill` CLI invocation constructs the engine with `backfill_finished_matches=False`, and its absence defaults to `True` (mirror the existing `test_collect_wires_pack_provider_engine_and_runs_one_poll` pattern, `tests/test_cli.py:701`).
- The `Validation cmd` (full suite) is a deliberate early smoke check for replay/determinism-suite regressions this branch could cause, ahead of Phase 3's full verification — see Requirements' Replay/fixtures bullet.

### Phase 2: Bounded failure tolerance

**Impl files:** `src/gamecollect/engine.py`
**Test files:** `tests/test_engine.py`
**Test command:** `uv run pytest tests/test_engine.py -q`
**Validation cmd:** `uv run pytest -q`
**Goal:** A finished match whose one-time detail fetch fails is tolerated — no crash, final score eventually retained, no infinite retry — via a bounded retry-then-give-up shape consistent with `_TransitionTracker`'s existing lifecycle invariants, held in a container separate from `self._transitions`.

- Introduce `self._backfill: dict[str, _TransitionTracker]` — a container separate from `self._transitions` (see Requirements and Review Focus for why: `_transitions` membership feeds `was_live` and is consumed by `_hydrate_vanished_matches`/`_resolve_tracker`, so parking backfill retries there would misroute a retrying match onto the live→terminal path). Parametrize `_emit_give_up`/`_resolve_tracker` (and any shared cap/fallback-accrual helper) over which tracker dict they operate on, so the give-up/retry code itself is reused, not reimplemented, against `self._backfill`.
- Wire the `except (ProviderUnavailableError, ShapeDriftError)` branch for this new detection path: do not `hydrated.append(match)` on the first failure (that would create a stored row and foreclose retry); retry next poll up to the cap; at cap, give up and persist the scoreboard-only snapshot (final state, matching existing give-up semantics elsewhere in this function).
- Unit tests: a finished match whose detail fetch fails once then succeeds retries correctly and ends up with full events, **and** a direct assertion that immediately after the first failed poll, `_stored_rows_for_provider(match_id)` returns empty (the load-bearing invariant, not just the eventual end state); a finished match whose detail fetch fails every attempt up to the cap gives up, persists the scoreboard-only snapshot (final score retained), and stops retrying on all subsequent polls (no infinite retry, no crash); a given-up first-sight-finished match, after a simulated collector restart, is not re-fetched (its stored row now makes detection `False`).

### Phase 3: Docs and replay verification

**Impl files:** `docs/DESIGN.md`, `README.md`
**Test files:** `tests/football/test_espn_event_mapping.py` (or a new replay-specific test module if warranted by Phase 1 findings)
**Test command:** `uv run pytest -q`
**Goal:** The behavior is documented next to the existing payload-contract documentation, and replay/determinism tests are confirmed unaffected (or updated with an explicit, understood reason if not) as a final, full-suite regression gate.

- Run the full replay/determinism test suite and confirm no test asserts an exact `fetch_match_detail` call count that this plan's behavior changes; if any do, resolve per the Requirements section's replay guidance and record the resolution in `## Findings`.
- Document the behavior in `docs/DESIGN.md` (near the existing payload sub-key contract from the goal-event-participants plan) and `README.md` if `README.md`'s `collect` usage section exists: on first sight of an already-finished match, the collector backfills its full event list once; the events land under the same `payload.scorer`/`payload.assist`/`payload.team` contract as live-observed events, current-day-slate-bounded by construction (pending Phase 1's live verification), opt-out via `--no-finished-backfill`.
- Full-suite regression run.

## Technical Specifications

### Files to Modify
- `src/gamecollect/engine.py` — new detection branch at the existing skip point (`_fetch_poll_snapshots`, `engine.py:1067-1069`), new `backfill_finished_matches` constructor flag, a new `self._backfill` tracker container (separate from `self._transitions`) plus parametrized give-up/resolve helpers for this path's bounded-retry failures.
- `src/gamecollect/cli.py` — `--no-finished-backfill` flag on the `collect` subparser, threaded into `_run_collect`'s `CollectorEngine(...)` construction (Phase 1).
- `docs/DESIGN.md`, `README.md` — document the backfill behavior and its bound.

### New Files to Create
- None expected. A new replay-specific test module may be warranted depending on Phase 1/3 findings (see Phase 3) — not committed to in advance.

### Architecture Decisions
- **Detection reuses the existing stored-row lookup (`_stored_rows_for_provider`) as the one-shot source of truth, rather than introducing new durable state.** Once a first-sight-finished match's events are (successfully) merged and applied, its stored `matches` row exists, which is itself sufficient to make the detection condition `False` on every future poll — no new column, table, or marker is needed for the success path.
- **Failure handling deliberately does NOT persist a stored row on the first attempt**, unlike the existing skip branch's default (`hydrated.append(match)`). This is the key divergence from "just fall back like everything else" — falling back immediately would durably foreclose the retry the detection condition depends on. This is why Phase 2 needs an explicit bounded-retry tracker rather than simply reusing today's per-match isolation `except` clause verbatim.
- **The bounded-retry tracker MUST be a container separate from `self._transitions`, with the give-up/retry code shared via parametrization, not the container.** `self._transitions` membership is read at three other sites — `was_live` (`engine.py:1058-1060`), `_hydrate_vanished_matches` (`engine.py:1225`), and `_resolve_tracker`/`_emit_give_up` (`engine.py:1737` and friends) — all of which treat membership as "this match was live." Storing a backfill retry entry there would make a retrying first-sight match satisfy `was_live` on its next poll, misrouting it onto the live→terminal transition path and corrupting the very retry loop Phase 2 exists to build. The fix is a new `self._backfill` dict plus parametrizing the give-up/resolve helpers over which dict they act on — this reuses the *logic* (satisfying Review Focus's "not a parallel reimplementation") while keeping the *state* separate (satisfying "must not corrupt `was_live`"). Both properties are required simultaneously; neither alone is sufficient.
- **No new per-poll fetch cap / rate limiter, pending Phase 1's live verification.** A naive "process at most N first-sight-finished matches per poll, defer the rest" cap was considered and rejected: deferring a candidate within a poll still requires *some* disposition for it that poll (skip it entirely, meaning it falls through to the existing `hydrated.append(match)` skip branch, which persists a stored row and forecloses its own retry — defeating the cap's purpose). Bounding via "ESPN's scoreboard is inherently one day's slate" avoids this correctness trap entirely — but that slate-size claim is itself an unverified assumption about ESPN's actual response, not a guaranteed code fact (the cited docstring only states the code's own intent). Phase 1 verifies it live; if the slate turns out to be large, this decision needs revisiting before Phase 1 is considered done. See Requirements.
- **`not match.events` as the provider-agnostic replay guard**, not a `isinstance(provider, ...)` check — keeps `engine.py` provider-agnostic per existing design (DESIGN.md §7: replay is a first-class provider the engine cannot distinguish from live). See Requirements' Replay/fixtures bullet for the underlying fact (real ESPN board snapshots are always event-empty; replay board snapshots already carry revealed events).
- **If either Phase 1 live-network prerequisite fails, this plan does not silently degrade.** A dropped-scoreboard-inclusion finding would require widening the provider ABC (`fetch_schedule` is ESPN-only today) — out of scope for this plan as written. The plan's response to that outcome is to STOP and return to `/dev-plan update`, not to improvise an ABC change inline.

### Dependencies
- None new.

### Integration Seams

Single-component change (`CollectorEngine`'s internal poll loop plus CLI flag plumbing) — no `## Architecture & Call Flow` section per this repo's dev-plan convention (only included for 2+ independently-executing components).

| Seam | Writer (task) | Caller (task) | Contract |
|------|---------------|---------------|----------|
| First-sight-finished detection → normal reconcile/writer path | Phase 1 (`_fetch_poll_snapshots`) | `poll_once` → `_apply` → `_event_to_row` → `PartitionWriter.append_events` (all pre-existing, unmodified) | A `merged` `NormalizedMatch` appended to `hydrated` from the new branch must be indistinguishable, downstream, from one appended by the existing live/transition branch — same payload contract, same idempotency (`INSERT OR IGNORE` on `(source, match_id, seq)`). |
| Bounded-retry failure tracking → give-up persistence | Phase 2 | Phase 1's `hydrated` accumulation | A give-up must persist a merged-never-raw snapshot (mirrors existing `_emit_give_up` contract) and must be the ONLY way a stored row gets written for a match whose first fetch attempt failed. |
| `self._backfill` tracker container → `was_live` computation | Phase 2 | `engine.py:1058-1060`, `_hydrate_vanished_matches` (`1225`), `_resolve_tracker`/`_emit_give_up` | `self._backfill` membership must NEVER be read by `was_live`, `_hydrate_vanished_matches`, or any other `self._transitions` consumer — only `self._transitions` itself feeds those. The give-up/resolve *code* is shared via parametrization; the *state* is not. |

## Testing Notes

### Test Approach
- [ ] Unit tests on `_fetch_poll_snapshots` (or its constituent decision logic) for: first-sight-`FINISHED` match triggers exactly one `fetch_match_detail` call and appends events; re-polling does not re-fetch (the stored-row-existence gate short-circuits detection — no second `fetch_match_detail` call happens at all) and therefore appends no duplicate events.
- [ ] Unit test: a first-sight-finished match whose events include a goal-family event lands rows whose `payload` carries `scorer`/`assist`/`team` — the PR #3 payload contract, not just event-count equality.
- [ ] Unit test: a match observed live, then finishing, does **not** trigger the new backfill branch (no double-fetch) — assert call count / branch taken is identical to pre-this-plan behavior for that case.
- [ ] Unit test: a finished match whose detail fetch fails is tolerated (no crash), retried up to the cap, then gives up with the final score retained and no further retries (no infinite retry).
- [ ] Unit test: immediately after the first failed poll (before any retry), `_stored_rows_for_provider(match_id)` returns empty — a **direct** assertion of the plan's central invariant, not just the eventual fail-then-succeed end state.
- [ ] Unit test: `collect --no-finished-backfill` constructs the engine with `backfill_finished_matches=False`; its absence defaults to `True`.
- [ ] Determinism check against an existing fixture (or a new small fixture, if none of the existing ones exercise a first-sight-`FINISHED` scenario at poll 1) where the full event list is known, confirming backfilled events match the oracle exactly; the fixture used must include at least one goal-family event with participants.
- [ ] `backfill_finished_matches=False` reproduces byte-for-byte today's skip-branch behavior (regression guard).
- [ ] Full replay/determinism suite passes unmodified, or with an explicitly understood and documented change (see Phase 3).

### Test Results
- [ ] All existing tests pass
- [ ] New tests added and passing
- [ ] Manual/live-network verification (Phase 1's hard gate): ESPN's default scoreboard includes an already-finished same-day match, AND a finished match's `fetch_match_detail` summary passes `assert_espn_summary_shape` with non-empty `keyEvents` — both recorded in `## Findings`; if either fails, Phase 1 stops per Requirements/Architecture Decisions rather than proceeding

### Edge Cases Tested
- [ ] A match with a genuinely empty event list (0-0, no cards) that is first-sight-finished — must NOT be mistaken for "not yet backfilled" on a later poll (the stored-row-existence check, not an event-count check, is what makes this safe — confirm the test covers it).
- [ ] Collector restart between a failed first attempt and the retry cap being reached — confirm the accepted behavior (retry budget resets, bounded per-restart) matches Review Focus's restart-interaction note.
- [ ] Collector restart *after* a give-up has persisted the scoreboard-only row — confirm the match is NOT re-fetched (stored row makes detection `False`), distinct from the pre-give-up restart case above.
- [ ] A first-sight-finished match on the `ReplayProvider` path (see Requirements' Replay/fixtures bullet).

## Acceptance Criteria

- Phase 1's live-network gate (scoreboard inclusion + detail-fetch shape) is verified and recorded in `## Findings` before detection wiring proceeds; if either check fails, the plan is returned to `/dev-plan update` rather than implemented as scoped.
- A match first observed as `FINISHED` with no prior stored row triggers exactly one `fetch_match_detail` call and its full event list is persisted, with a test directly asserting the `payload.scorer`/`payload.assist`/`payload.team` contract on a backfilled goal-family event (not just event-count equality against an oracle).
- A match observed live and then finishing is unaffected — no double-fetch, no behavior change from today.
- The backfill retry tracker lives in a container separate from `self._transitions` (verified: a retrying first-sight match's `was_live` stays `False` across polls), while the give-up/retry code itself is shared with `_TransitionTracker`'s existing helpers via parametrization, not reimplemented.
- A finished match whose detail fetch fails is tolerated: no crash, no infinite retry, final score retained after the retry cap is exhausted, with a direct test that no stored row exists between the first failure and the eventual give-up/success.
- `backfill_finished_matches=False` (and `--no-finished-backfill`, shipped in Phase 1 alongside the constructor flag) reproduces today's behavior exactly, with both the engine-level and CLI-level wiring under test.
- `docs/DESIGN.md`/`README.md` document the backfill behavior, its current-day-slate bound, and the opt-out flag.
- Full test suite green (`uv run pytest -q`).
- `/review-plan` run and findings addressed before implementation begins.

<!-- reviewed: 2026-07-12 @ a3c1f3c6ede3160f27d53f808c5ca3766a38c082 -->

<!-- /review-plan writes the marker line above. Everything below is the workspace: edits here do NOT invalidate the marker. -->

## Progress

- [ ] Phase 1: Live-network gate + detection + one-time fetch (happy path)
- [ ] Phase 2: Bounded failure tolerance
- [ ] Phase 3: Docs and replay verification

## Findings

- (append findings here as work proceeds)

## Issues & Solutions

(none yet)

## Final Results

(fill in when complete)
