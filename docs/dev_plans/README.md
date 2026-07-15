# Development Plans

Index of dev plans for gamealerts-collector. The plan file is the source of truth; this table is derived.

## Current Tasks

| Plan | Comp | Status | Branch | Notes |
|------|------|--------|--------|-------|
| [20260711-feature-finished-match-backfill](20260711-feature-finished-match-backfill.md) | collector | In Review | feature/finished-match-backfill | Backfill a match's full event list on first sight if it's already FINISHED. Raised by gamealerts game-worker (PR #2 note ask #4), triaged out of `20260710-feature-goal-event-participants`. |

## Completed Tasks

| Plan | Comp | Completed | Outcome |
|------|------|-----------|---------|
| [20260702-feature-collector-foundation](20260702-feature-collector-foundation.md) | collector | 2026-07-03 | Shipped — scaffold, schema v1, pack SDK, football/WC2026 pack. Merged in PR #1. |
| [20260702-feature-collector-interfaces](20260702-feature-collector-interfaces.md) | collector | 2026-07-10 | Engine daemon, client library, CLI + manifest, ReplayProvider + fixtures. 15 rounds engine hardening (480 tests). Merged in PR #2. |
| [20260710-feature-goal-event-participants](20260710-feature-goal-event-participants.md) | collector | 2026-07-10 | `payload.scorer`/`payload.assist` typed event fields, raised by gamealerts game-worker (PR #33) in `/review 2`. |
