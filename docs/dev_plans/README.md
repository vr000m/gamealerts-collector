# Development Plans

Index of dev plans for gamealerts-collector. The plan file is the source of truth; this table is derived.

## Current Tasks

| Plan | Comp | Status | Branch | Notes |
|------|------|--------|--------|-------|
| [20260718-feature-historical-backfill](20260718-feature-historical-backfill.md) | collector | Not Started | feature/gameworker-integration | One-shot `backfill` CLI for prior-day matches via `fetch_schedule`; closes the Follow-up Work gap left by `20260711-feature-finished-match-backfill`. Lands on PR #5. |

## Completed Tasks

| Plan | Comp | Completed | Outcome |
|------|------|-----------|---------|
| [20260702-feature-collector-foundation](20260702-feature-collector-foundation.md) | collector | 2026-07-03 | Shipped — scaffold, schema v1, pack SDK, football/WC2026 pack. Merged in PR #1. |
| [20260702-feature-collector-interfaces](20260702-feature-collector-interfaces.md) | collector | 2026-07-10 | Engine daemon, client library, CLI + manifest, ReplayProvider + fixtures. 15 rounds engine hardening (480 tests). Merged in PR #2. |
| [20260710-feature-goal-event-participants](20260710-feature-goal-event-participants.md) | collector | 2026-07-10 | `payload.scorer`/`payload.assist` typed event fields, raised by gamealerts game-worker (PR #33) in `/review 2`. |
| [20260711-feature-finished-match-backfill](20260711-feature-finished-match-backfill.md) | collector | 2026-07-15 | Backfill a match's full event list on first sight if it's already FINISHED, with bounded retry/give-up and 3 rounds of post-merge adversarial-review hardening (548 tests). Raised by gamealerts game-worker (PR #2 note ask #4), triaged out of `20260710-feature-goal-event-participants`. |
| [20260707-feature-gameworker-integration](20260707-feature-gameworker-integration.md) | collector | 2026-07-17 | Generic `MatchReadPort` read contract, single collector-owned shared-file write model, pack-manifest `vocabulary` op; cross-repo contract doc; 3 rounds of post-completion review-gauntlet hardening (670 passed/1 skipped). PR #5. |
