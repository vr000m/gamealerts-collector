"""The collection engine daemon: one process per tournament (source).

Load a pack, poll its provider, diff each snapshot against the last-written
state, and persist the delta through a source-scoped
:class:`~gamecollect.db.writer.PartitionWriter`. The mechanics are ported from
gamealerts' ``collector/session.py`` poll loop but written self-contained here
(that file is not in this repo) and stripped of everything prose/voice — no
summarizer, no IPC, facts only.

Loop posture (the contract the tests pin):

* **Jittered interval** — each healthy poll sleeps ``poll_interval`` ± 20 %
  (uniform), so a fleet of daemons does not thundering-herd the provider.
* **Exponential backoff** on provider errors — base = ``poll_interval``,
  doubling each consecutive failure, capped at 8×, reset on the first success.
  :class:`~gamecollect.provider.ProviderUnavailableError` backs off quietly;
  :class:`~gamecollect.provider.ShapeDriftError` backs off *and* logs loudly
  with payload context. Both are non-fatal — the loop survives.
* **Per-match drift isolation** — a re-sent seq whose fingerprint mutated
  raises :class:`~gamecollect.db.writer.SequenceError`; the engine reconciles
  against the ACTUALLY-STORED rows: events strictly beyond the stored head
  still land (no wedge), conflicted or retroactively-inserted seqs at-or-below
  the head are never written (loud ERROR on every poll they persist — stored
  rows never mutated), and the diff baseline is rebuilt from the stored rows
  so persistent drift keeps re-surfacing instead of being silently baselined
  away. Other matches and the daemon are unaffected.
* **Seed-before-child-write** — the engine never calls ``append_events``
  before the match row exists; it seeds through the pack's ``seed_match`` hook
  (core never imports a pack) and skips child writes when the hook returns
  ``None`` (missing identity).
* **Graceful shutdown** — SIGTERM sets a stop event that interrupts the sleep;
  the loop finishes the current iteration, flushes any ``--record`` fixture,
  and closes the connection.
"""

from __future__ import annotations

import json
import logging
import random
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from gamecollect.db import reader
from gamecollect.db.connection import connect
from gamecollect.db.writer import (
    CrossPartitionError,
    PartitionWriter,
    SequenceError,
    TaxonomyError,
    UnseededMatchError,
)
from gamecollect.diffing import MatchDiff, diff_matches
from gamecollect.packs.spec import SportPack, default_seed_match
from gamecollect.provider import (
    LIVE_STATUS_VALUES,
    LIVE_STATUSES,
    MatchDataProvider,
    MatchStatus,
    NormalizedEvent,
    NormalizedMatch,
    ProviderUnavailableError,
    ShapeDriftError,
    merge_payload_preserving_richer,
)
from gamecollect.record import RecordWriter

__all__ = ["CollectorEngine"]

log = logging.getLogger(__name__)

# Backoff ceiling: base (poll_interval) × this multiplier.
_MAX_BACKOFF_MULTIPLIER = 8
# Healthy-poll jitter as a fraction of poll_interval (± this).
_JITTER_FRACTION = 0.20
# Consecutive detail-fetch failures tolerated on a live→terminal transition
# before the engine gives up retrying and persists the best-known terminal
# state (a provider whose detail endpoint permanently 404s post-FT must not
# wedge the match on the slate forever).
_MAX_TRANSITION_DETAIL_FAILURES = 3
# Total hydration attempts (fetch failures PLUS non-terminal outcomes: a
# still-live detail after the slate dropped the match, or a SCHEDULED/UNKNOWN
# provider status reset) tolerated per transition before the engine gives up:
# a detail endpoint that never turns terminal must not be re-fetched and
# WARNed about every poll forever.
_MAX_TRANSITION_TOTAL_ATTEMPTS = 10
# Give-up snapshot emissions tolerated before the tracker is abandoned: an
# UNPERSISTABLE give-up snapshot (e.g. ``seed_match`` returns ``None`` on
# missing identity, so ``_apply`` returns ``None`` every poll) must not
# re-emit forever. After this many emissions whose apply never landed, ONE
# final ERROR is logged and the tracker is dropped.
_MAX_GIVE_UP_EMISSIONS = 3
# Abandonment COOLDOWN (see :class:`_Cooldown` and the ``_TransitionTracker``
# docstring): after a match is abandoned it may be re-tracked only once this
# many polls have elapsed, doubling per successive abandonment epoch (8, 16,
# 32 … capped) so a match whose apply persistently fails retries at
# exponentially spaced intervals instead of hot-looping a fresh give-up every
# poll. The cooldown clears ONLY on a genuinely durable apply for the match.
_COOLDOWN_BASE_POLLS = 8
_COOLDOWN_MAX_POLLS = 256


def _event_to_row(event: NormalizedEvent) -> dict[str, Any]:
    """Project a :class:`NormalizedEvent` onto a writer ``events`` row.

    Core-agnostic and lossless: ``seq``/``type``/``minute``/``importance``/
    ``detail`` map onto their columns; the sport-shaped ``team``/``player``/
    ``assist`` ride in ``payload`` (the engine does not synthesize
    ``actor_entity`` refs — that is the pack importer's job in Phase 4). ``period``
    is left NULL: ``NormalizedEvent`` carries no authoritative period.
    """
    payload = {
        key: value
        for key, value in (("team", event.team), ("player", event.player), ("assist", event.assist))
        if value is not None
    }
    row: dict[str, Any] = {
        "seq": event.seq,
        "type": event.event_type,
        "minute": event.minute,
        "importance": event.importance,
        "detail": event.detail,
    }
    if payload:
        row["payload"] = payload
    return row


# NormalizedMatch fields where ``None`` unambiguously means "not provided".
# A detail endpoint that omits one must not wipe the scoreboard's value:
# detail wins where it has a value; the scoreboard fills detail's Nones.
# Scores are listed too: they are cumulative facts that are never legitimately
# "cleared" mid- or post-match, so a detail ``None`` score must not null a
# known scoreboard score in ANY branch — the equal-rank live poll and the
# FINISHED transition poll included (the lagging-detail branch additionally
# restores non-``None`` scoreboard scores over stale detail values). Clock
# fields (``minute``/``display_clock``) are deliberately NOT filled: at HT/FT
# a detail endpoint may legitimately null the clock, and backfilling would
# freeze a stale live minute in stored state — detail wins for those,
# including ``None``. ``status`` is a non-optional enum (merged with the
# forward-only rule below) and ``events``/``payload`` merge separately, so
# they are not listed.
_MERGE_FILL_FIELDS = (
    "home_team",
    "away_team",
    "kickoff_utc",
    "score_home",
    "score_away",
)

# Sentinel distinguishing "stored row not supplied" (fetch it) from an explicit
# ``None`` (no stored row) in :meth:`CollectorEngine._nonregress_over_stored`,
# whose ``stored`` is threaded in once per emission but omitted by direct callers.
_UNSET: Any = object()

# Lifecycle rank for the merge's forward-only status rule: when scoreboard and
# detail describe the SAME poll, status may only move forward — a cached or
# lagging detail endpoint still saying live must never regress the scoreboard's
# FINISHED (the match may drop off the slate right after, leaving the DB
# in-progress forever). IN_PLAY/PAUSED share a rank (either direction is a
# legal live oscillation, detail wins); UNKNOWN never beats a known status.
_STATUS_RANK: dict[MatchStatus, int] = {
    MatchStatus.UNKNOWN: -1,
    MatchStatus.SCHEDULED: 0,
    MatchStatus.IN_PLAY: 1,
    MatchStatus.PAUSED: 1,
    MatchStatus.FINISHED: 2,
}

# Terminal lifecycle statuses: a match in one of these has reached an end state
# and will not legitimately go live again. Today only FINISHED is terminal, but
# every give-up / fallback decision tests membership in THIS set (never ``is
# FINISHED``) so a future terminal kind (AET, penalties, abandoned) is carried by
# the same monotonicity rules without hunting down every call site.
_TERMINAL_STATUSES: frozenset[MatchStatus] = frozenset({MatchStatus.FINISHED})


def _is_terminal(status: MatchStatus) -> bool:
    """True when ``status`` is a terminal (end-of-match) lifecycle state."""
    return status in _TERMINAL_STATUSES


def _merge_payload_preserving_richer(base: dict, incoming: dict) -> dict:
    """Merge ``incoming`` over ``base`` with the preserve-richer rule.

    A non-empty ``incoming`` value always wins, and a key absent from ``base`` is
    always added, but an explicitly-empty ``incoming`` value
    (:func:`is_empty_payload_value` — ``None``/``""``/``[]``/``{}``) NEVER clobbers
    a key ``base`` already carries with a non-empty value. Shared by
    :func:`_merge_detail` (same-poll scoreboard→detail) and
    :func:`_merge_over_base` (cross-poll base→snapshot) so a cumulative sport-extra
    the scoreboard populated (e.g. a live penalty-shootout score the summary
    endpoint has not caught up on) is not lost when the other snapshot omits it.

    Thin wrapper over :func:`gamecollect.provider.merge_payload_preserving_richer`
    (the single source of truth for the rule); kept for the local call-site
    docstring cross-references.
    """
    return merge_payload_preserving_richer(base, incoming)


def _merge_detail(scoreboard: NormalizedMatch, detail: NormalizedMatch) -> NormalizedMatch:
    """Merge a detail snapshot over its scoreboard snapshot, field by field.

    Payload keys merge PRESERVE-RICHER (:func:`_merge_payload_preserving_richer`):
    a detail value wins whenever it is non-empty, but a detail that supplies an
    empty value for a key the scoreboard already populated does NOT wipe it — a
    cumulative sport-extra like a live penalty-shootout score, read on the
    scoreboard path, must survive a summary poll that has not yet caught up (or a
    version-skewed detail that omits ``shootoutScore``). Identity fields and
    scores take the detail value unless it is ``None``, in which case the
    scoreboard value fills it — a detail endpoint omitting ``kickoff_utc``/
    ``home_team``/``away_team`` must not wipe stored identity to NULL (or make
    ``seed_match`` return ``None`` and drop the poll's events), and a detail
    omitting ``score_home``/``score_away`` must not null a known score (scores are
    cumulative facts; only ``minute``/``display_clock`` are legitimately
    nulled at HT/FT, so those stay detail-wins including ``None``).

    Status is forward-only (see ``_STATUS_RANK``): when the detail's status
    lags BEHIND the scoreboard's, the scoreboard's status wins, and so do its
    ``minute``/``display_clock`` (a lagging detail's clock is stale live state
    by definition) and its non-``None`` scores. The detail's events/payload
    are still taken — that is the whole point of hydrating the transition poll.
    """
    payload = _merge_payload_preserving_richer(scoreboard.payload, detail.payload)
    fills = {
        name: getattr(scoreboard, name)
        for name in _MERGE_FILL_FIELDS
        if getattr(detail, name) is None
    }
    merged = replace(detail, payload=payload, **fills)
    if _STATUS_RANK[detail.status] < _STATUS_RANK[scoreboard.status]:
        overrides: dict[str, Any] = {
            "status": scoreboard.status,
            "minute": scoreboard.minute,
            "display_clock": scoreboard.display_clock,
        }
        for name in ("score_home", "score_away"):
            value = getattr(scoreboard, name)
            if value is not None:
                overrides[name] = value
        merged = replace(merged, **overrides)
    return merged


def _merge_over_base(base: NormalizedMatch, snapshot: NormalizedMatch) -> NormalizedMatch:
    """Merge ``snapshot`` over a BASE from an earlier poll (baseline/fallback/stored).

    Unlike :func:`_merge_detail` there is NO forward-only status rule here: a
    base is prior state, not a same-poll scoreboard, so a still-live snapshot
    must not be force-closed by a terminal base — the snapshot's own
    ``status``/``minute``/``display_clock`` always stand (a terminal status is
    only ever accepted from the detail itself, or from the fallback at
    give-up). The base underlays the payload and fills identity and score
    ``None``s, exactly like the scoreboard does in :func:`_merge_detail`.

    The payload merge is PRESERVE-RICHER, not last-wins
    (:func:`_merge_payload_preserving_richer`): an incoming key only overwrites
    the base when its value is non-empty, or the base doesn't have the key at all
    — an explicitly-present empty value (``""``/``None``/``[]``/``{}``) in a
    sparse snapshot must not clobber a richer base payload value. This mirrors the
    DB-side ``_merge_preserving_richer`` in ``gamecollect_football.reconcile``
    (which protects the stored row); this in-memory baseline needs the same
    guarantee independently, since a baseline/fallback carried between polls is
    not read back through that DB path. (:func:`_merge_detail` shares the same
    preserve-richer helper for its payload merge.)
    """
    payload = _merge_payload_preserving_richer(base.payload, snapshot.payload)
    fills = {
        name: getattr(base, name) for name in _MERGE_FILL_FIELDS if getattr(snapshot, name) is None
    }
    return replace(snapshot, payload=payload, **fills)


@dataclass
class _Cooldown:
    """Per-match abandonment cooldown (see ``_TransitionTracker`` docstring).

    ``remaining`` is the number of polls left before the id may be re-tracked;
    it is decremented once per poll and floors at 0 (the entry lingers at 0,
    remembering ``epoch``, so the NEXT abandonment doubles the interval rather
    than restarting at the base). ``epoch`` counts successive abandonments and
    drives both the exponential ``remaining`` and the ERROR→WARN log downgrade.
    The entry is normally popped on a genuinely durable apply for the match; to
    bound the dict when that apply never comes (a match that simply ended and
    dropped off the slate), ``expired_unseen`` counts consecutive polls the entry
    has been expired (``remaining == 0``) AND its match unseen (not on the slate,
    not tracked). It resets to 0 whenever the match is seen or the cooldown is
    still counting down, and once it reaches ``_COOLDOWN_MAX_POLLS`` the entry is
    purged (:meth:`_age_cooldowns`) — the epoch/backoff memory is lost, which is
    fine: a match reappearing after a full cap-length quiet window deserves a
    fresh epoch.

    ``fallback`` holds the accumulated best-known non-live state accrued WHILE
    cooling — the give-up machinery does NOT get-or-create a tracker during a
    cooldown (that would re-arm the very hot-loop the cooldown suppresses), so a
    richer board seen while cooling (a 2-0 detail over a stale 1-0) accrues here
    field-wise via :func:`_accrue_fallback` instead. When the cooldown lapses and
    re-tracking begins, the fresh tracker's ``fallback`` is SEEDED from this slot
    (see :meth:`_tracker`) so a rich terminal state observed during the quiet
    window still closes the eventual give-up. ``drift_logged`` damps the
    schema-drift ERROR to ONCE per epoch per match (:meth:`_log_drift_while_cooling`);
    it is reset by :meth:`_enter_cooldown` when a new epoch starts.

    GATE vs MEMORY: ``remaining`` is the SUPPRESSION GATE (re-tracking is blocked
    only while ``remaining > 0``); ``epoch``/``fallback`` are the MEMORY. A
    durable state-advanced LIVE apply clears only the gate (zeroes ``remaining``,
    see :meth:`_on_durable_snapshot`) but PRESERVES the epoch, so a still-failing
    terminal write keeps exponential backoff and the WARN-after-first-epoch
    damping. The ENTRY itself is removed only by a terminal resolution
    (:meth:`_resolve_tracker`) or the expired-and-unseen purge.
    """

    remaining: int
    epoch: int
    expired_unseen: int = 0
    fallback: NormalizedMatch | None = None
    drift_logged: bool = False


@dataclass
class _TransitionTracker:
    """Per-match bookkeeping for a live→terminal transition in flight.

    One tracker per provider-native match id, held in
    ``CollectorEngine._transitions``. Lifecycle invariants (enforced by
    construction — every round-5 review defect traced to violating one):

    * **Created/updated** whenever a live-tracked match needs terminal
      hydration it did not durably get this poll: a live→non-live transition
      poll whose detail fetch failed (the non-live scoreboard snapshot is
      offered as ``fallback``), a previously-live id absent from the slate, a
      restart gap (stored row live, no in-memory baseline), or a non-terminal
      detail (still live, or a SCHEDULED/UNKNOWN provider reset) on any of
      those. A tracker existing is itself a "was live" marker, so the match
      can never fall out of tracking while unresolved.
    * ``fallback`` keeps the RICHEST non-live scoreboard snapshot seen so far
      (never overwritten by a sparser one) — the best-known terminal state to
      persist at give-up.
    * **Consumed (popped) ONLY after ``_apply`` of a non-live snapshot for
      this match returned durably** (``CollectorEngine._resolve_tracker``); an
      apply failure retains the tracker so the snapshot is retried next poll —
      a transient DB error must not lose the captured final state. Trackers
      are never pruned by slate absence alone.
    * ``consecutive_failures`` counts detail-FETCH failures (reset by any
      successful fetch); ``total_attempts`` counts every hydration attempt
      that did not yield a terminal snapshot (fetch failures, still-live
      details, status resets) and is never reset. Reaching either cap
      triggers a give-up that persists the best-known state (merged, never
      raw) and sets ``give_up_pending``.
    * **On a live slate reappearance** (a slate flicker or a genuine
      resumption, which one poll cannot distinguish) the consumed retry BUDGET
      is reset at DETECTION, unconditionally, by replacing the tracker with a
      fresh one carrying only the ``fallback``
      (``CollectorEngine._reset_resumption_budget``) — so an exhausted episode
      can never inherit into (and force-close) a genuinely live match, even if
      this poll's apply then fails. Whether the ``fallback`` survives is a
      separate question decided ONLY against the DURABLY APPLIED live state
      (``CollectorEngine._reconcile_live_apply``), never the raw slate board
      whose scores are often NULL until the detail merge: the tracker is
      RETAINED while the fallback is still worth persisting (events, a known
      score, or FINISHED) AND the applied live score has NOT climbed past it
      (scores are cumulative, so a strictly higher applied score proves the
      fallback stale); otherwise the tracker is dropped. This keeps a rich
      full-time fallback across a one-poll flicker without letting a stale one
      beat a genuinely higher later score at give-up.
    * ``give_up_pending`` short-circuits further detail fetches: the give-up
      snapshot is re-emitted every poll until its apply lands, and the ERROR
      log stating what was persisted fires only after that durable success.
      ``give_up_emissions`` bounds the re-emission: an UNPERSISTABLE snapshot
      (identity missing, ``seed_match`` → ``None`` forever) is abandoned with
      one final ERROR after ``_MAX_GIVE_UP_EMISSIONS`` emissions.
    * **Abandonment enters a capped-exponential COOLDOWN, not a permanent
      boolean latch** (round-9 redesign — a boolean could not satisfy the
      behaviour matrix below). When a give-up snapshot's apply never lands
      (``_MAX_GIVE_UP_EMISSIONS`` exhausted, or nothing was ever known to
      persist) the tracker is popped AND a :class:`_Cooldown` is entered for the
      id in ``CollectorEngine._cooldowns``. While the cooldown's ``remaining``
      poll count is > 0 the id is NOT re-tracked — neither the off-slate
      vanished path (``_hydrate_vanished_matches``) nor the on-slate transition
      path builds a fresh tracker or re-emits a give-up for it — so a match
      whose apply persistently fails cannot hot-loop a give-up every poll. Each
      successive abandonment DOUBLES the cooldown (``epoch`` drives 8, 16, 32 …
      capped at ``_COOLDOWN_MAX_POLLS``) and downgrades the abandonment log from
      ERROR (first epoch) to a single WARN (later epochs). The cooldown does NOT
      gate a genuinely recoverable board: an on-slate board whose detail fetch
      SUCCEEDS is merged/applied normally even while cooling (a live board
      resumes it, a FINISHED board closes it); only the give-up machinery
      (fetch-failure/non-terminal fallback accrual + re-emission) is suppressed —
      and while cooling that accrual lands on the :class:`_Cooldown` entry, NOT a
      tracker (a tracker is never get-or-created during a cooldown). GATE vs
      MEMORY: a durable apply CLEARS THE COOLING GATE (``remaining`` → 0, so
      re-tracking resumes) on ANY genuinely durable apply for the match — a
      terminal apply via ``_resolve_tracker``, or ANY ``state_advanced`` live
      apply via ``_on_durable_snapshot`` (``_reconcile_live_apply`` handles
      resumption tracker semantics only for a match live on the slate this poll,
      but the gate clear itself is unconditional on ``state_advanced`` — a live
      DETAIL recovery reached through the cooling hydration path, absent from
      ``live_resumptions``, clears the gate too). The ENTRY is REMOVED only by a
      terminal resolution (``_resolve_tracker``) or the expired-and-unseen purge:
      a live recovery preserves the epoch/backoff memory so a still-failing
      terminal write path keeps its exponential spacing. A no-change snapshot
      (``diff.has_changes`` False) advanced nothing, so it is NOT routed through
      the cooldown-clearing path (see ``_on_durable_snapshot``'s
      ``state_advanced`` gate) — a live flicker that writes nothing must not
      re-arm re-tracking. ACCEPTED judgment call: a live board whose state
      literally never advances (a frozen provider cache emitting an identical
      snapshot every poll) cannot clear its cooldown via this no-change path —
      it is indistinguishable from a stale flicker. A genuinely live match
      clears the cooling gate on its next real state change (a minute tick, a
      score, a status move); a truly frozen id simply ages out via the bounded
      expired-and-unseen purge (:meth:`_age_cooldowns`) once it also leaves the
      slate. The behaviour matrix this design must satisfy (all hold):

        a. persistent outage, on-slate terminal board whose apply keeps failing
           → retry cycles exist but exponentially spaced; bounded log noise.
        b. abandoned → live-during-outage (apply fails) → vanishes → outage
           lifts → a later cooldown epoch re-tracks and persists the final
           state (no permanent loss).
        c. no-changes live flicker → cooldown state untouched (gate and memory).
        d. recoverable FINISHED board after outage lifts → applied, persisted,
           ENTRY REMOVED by the terminal resolution (immediately when its detail
           fetch succeeds).
        e. durable live recovery → normal collection, cooling GATE cleared
           (re-tracking resumes) but the epoch/backoff MEMORY preserved, so a
           terminal write that keeps failing afterwards keeps its exponential
           spacing (L1).
        f. transient live flicker with failing apply → no immediate re-track
           (cooldown holds).
    * ``last_reported_status`` dedupes the non-terminal WARN: once per detail
      status change, not once per poll.
    """

    fallback: NormalizedMatch | None = None
    consecutive_failures: int = 0
    total_attempts: int = 0
    give_up_pending: bool = False
    give_up_emissions: int = 0
    last_reported_status: MatchStatus | None = None


# --- pure match-merge / comparison helpers ---
def _tracker_at_cap(tracker: _TransitionTracker) -> bool:
    """True when the tracker owes a give-up (pending or a cap reached)."""
    return (
        tracker.give_up_pending
        or tracker.consecutive_failures >= _MAX_TRANSITION_DETAIL_FAILURES
        or tracker.total_attempts >= _MAX_TRANSITION_TOTAL_ATTEMPTS
    )


def _live_supersedes_cooldown_fallback(applied: NormalizedMatch, fallback: NormalizedMatch) -> bool:
    """True when a durable live apply proves a cooldown's terminal fallback stale.

    Thin wrapper over the shared :func:`_live_supersedes_captured` policy — see
    that method for the single documented supersession rule (Z2 unified the two
    near-duplicate twins that had already silently diverged once).
    """
    return _live_supersedes_captured(applied, fallback)


def _live_supersedes_captured(applied: NormalizedMatch, fallback: NormalizedMatch) -> bool:
    """SHARED policy: does a durable live ``applied`` state prove a captured
    terminal ``fallback`` stale? (Z2 — one helper for both supersession sites.)

    Both the cooldown fallback (:func:`_live_supersedes_cooldown_fallback`) and
    the resumption-tracker fallback (:func:`_live_supersedes_fallback`) are judged
    against a DURABLE applied live state carrying a real minute (not a NULL slate
    board), so they share ONE rule. Supersession holds under EITHER condition:

    1. STRICT score advance with NO regression: the live board advanced STRICTLY
       PAST the fallback's known score on at least one side, and regressed on NO
       side — the match is genuinely still live and the pre-recovery terminal
       fallback stale. A regression on ANY side (e.g. a swapped-score board 2-1
       vs a fallback 1-2) proves nothing and does NOT supersede (Z2 — the tracker
       twin previously superseded on a strict advance even when the OTHER side
       regressed, silently dropping a genuine fallback). A NULL applied score on a
       side the fallback knows also proves nothing (conservative — keep).
    2. EQUAL scores at a STRICTLY LATER minute (Y3): every side's scores are known
       and equal AND the applied live minute is strictly greater than the
       fallback's (both known). Equal scores ALONE are not proof — a lagging
       scoreboard still showing the genuine final 2-1 must not drop a real
       FINISHED 2-1 fallback (round 13: ``>=`` wrongly dropped it). But equal
       scores at a demonstrably later minute ARE proof: a false pre-recovery
       terminal (FINISHED 1-0 min 55) on a match that plays on and never scores
       again would otherwise live forever (round 14: strict ``>`` wrongly kept it).
       Null minutes are conservative — keep the fallback.

    ACCEPTED JUDGMENT CALL (residual): the equal-score minute rule may drop a
    GENUINE final when a provider encodes stoppage time as ``minute > 90`` on a
    lagging live board — e.g. a fallback FINISHED at minute 90 vs a live board at
    minute 93 at the SAME score reads as "play continued" and supersedes. The loss
    is BOUNDED: the scores stay correct (via G1 / the stored terminal row), only
    the terminal's events / clock richness is lost. This residual is accepted
    because the dual — keeping FALSE terminals alive at the final score — was
    judged worse in round 14.
    """
    strictly_advanced = False
    both_sides_known = True
    for field in ("score_home", "score_away"):
        live = getattr(applied, field)
        captured = getattr(fallback, field)
        if captured is None:
            both_sides_known = False
            continue
        # A regression on ANY known side, or a NULL applied score on a side the
        # fallback knows, proves nothing → not stale (Z2: no-regression required).
        if live is None or live < captured:
            return False
        if live > captured:
            strictly_advanced = True
    if strictly_advanced:
        return True
    # Y3: no strict advance and no regression → every known side is EQUAL. Equal
    # scores alone are not proof of liveness (a lagging board at the genuine
    # final), but equal scores at a STRICTLY LATER minute are: play demonstrably
    # continued past the fallback's capture. Null minutes are conservative — keep.
    return (
        both_sides_known
        and applied.minute is not None
        and fallback.minute is not None
        and applied.minute > fallback.minute
    )


def _fallback_worth_retaining(fallback: NormalizedMatch) -> bool:
    """True when a fallback carries terminal richness worth keeping.

    A fallback with events, a known score, or a terminal status is a
    best-known terminal state that must survive a bogus live flicker; a
    sparse reset board (no events, NULL scores, non-terminal) carries
    nothing a fresh transition would not rebuild, so its tracker is dropped.
    """
    return bool(
        fallback.events
        or fallback.score_home is not None
        or fallback.score_away is not None
        or _is_terminal(fallback.status)
    )


def _live_supersedes_fallback(applied: NormalizedMatch, fallback: NormalizedMatch) -> bool:
    """True when a live board proves the captured resumption fallback is stale.

    Thin wrapper over the shared :func:`_live_supersedes_captured` policy (Z2).
    The resumption-tracker fallback and the cooldown fallback are judged against
    the same durable applied live state, so they now use ONE rule — including the
    no-regression requirement that keeps a swapped-score board (2-1 vs a fallback
    1-2) from wrongly superseding a genuine fallback on a one-sided advance.
    """
    return _live_supersedes_captured(applied, fallback)


def _accrue_fallback(old: NormalizedMatch | None, new: NormalizedMatch) -> NormalizedMatch:
    """Field-wise ACCUMULATOR of the best-known fallback state (M1).

    The fallback is no longer "one chosen snapshot" but the accumulated
    best-known state across every non-live observation seen for the match —
    replacing the old ``_richest_fallback`` SELECTION comparator, which
    picked one whole snapshot and so was inherently lossy (a fresh FINISHED
    board carrying a NULL score would win outright over a 2-0 incumbent and
    drop the score; a bare higher-score board would win and drop the
    incumbent's captured events). Each field is merged independently:

    * SCORES: per side the MAX of the known values; a ``None`` NEVER replaces
      a known value (a FINISHED None-0 board over a 2-0 incumbent yields 2-0).
    * REGRESSION VETO (X3, terminality-classed per Y1): when the NEWER side
      (``new``) regresses a known incumbent score on either side (both values
      known, ``new < old``) AND both sides share a terminality class (both live
      or both terminal), the new side is a bogus/reset board — it is
      untrustworthy for every field, so the incumbent keeps status, clock,
      events, payload, and IDENTITY (Y2) precedence and the new side contributes
      ONLY where the incumbent has nothing. The per-side score max still holds
      (it already keeps the higher incumbent).
    * TERMINALITY PREFERENCE PRECEDES THE VETO ACROSS CLASSES (Y1): the veto
      fires ONLY within one terminality class. When the sides differ in class —
      a transient inflated live incumbent (a LIVE 3-0 glitch) versus a genuinely
      FINISHED 2-0 provider final — the terminal side is the provider's final
      view and wins status, clock, events, payload, and identity REGARDLESS of
      the score regression, so its authoritative final events are never dropped.
      Judgment call / residual: the per-side score max still keeps the inflated
      3 (an inflated transient score is indistinguishable from a real one under
      the scores-are-cumulative doctrine), so the final reads FINISHED 3-0.
    * STATUS: prefer the genuinely-terminal side's status; if neither or both
      are terminal the newer side's status wins (unless vetoed above).
    * CLOCK (X1 + Y4 per-field fill): ``minute``/``display_clock`` are sourced
      from status-ELIGIBLE sides only — the status winner, or the OTHER side
      only when it shares the winning status (or, when the winner is terminal,
      is also terminal). A live side's mid-match clock never fills an accumulated
      terminal (an INELIGIBLE side contributes nothing). Y4 relaxes the old
      all-or-nothing PAIR rule to a PER-FIELD fill AMONG ELIGIBLE SIDES: a null
      winner field is filled independently from the eligible other side, so a
      winner's minute 90 / null display_clock merges with an eligible side's
      "90'+3" to 90/"90'+3" (same-status mixing is fine). If no eligible side has
      a clock BOTH stay null (doctrine: a null clock at FT is legitimate —
      emission-time G3 fills from a stored terminal row).
    * EVENTS (X4): a NEWER side that is genuinely terminal with a non-empty
      list is the provider's FINAL authoritative view — its list wins outright,
      even if shorter (a corrected terminal list drops rescinded events).
      Otherwise keep the non-empty list, and the LONGER when both are non-empty
      (events accumulate). The regression veto (X3) overrides this entirely.
    * IDENTITY (home_team/away_team/kickoff_utc): fill ``None``s from either
      side (the newer value wins when both are present) — EXCEPT under the veto
      (Y2), where the incumbent's non-None identity wins and the new side fills
      only where the incumbent is None. ACCEPTED JUDGMENT CALL: under a same-class
      score-regression veto a legitimate identity CORRECTION riding the vetoed
      board is DEFERRED, not lost — it heals on the next non-regressed observation,
      where newer non-None identity wins normally.
    * PAYLOAD: preserve-richer merge (:func:`is_empty_payload_value`) — the
      newer value wins unless empty over a non-empty incumbent; under the
      regression veto the precedence flips so the incumbent's non-empty values
      win and the new side only fills keys the incumbent lacks.
    """
    if old is None:
        return new
    score_home = _max_known(old.score_home, new.score_home)
    score_away = _max_known(old.score_away, new.score_away)
    # X3: the newer side regresses a known incumbent score on either side.
    regressed = any(
        (o := getattr(old, field)) is not None and (n := getattr(new, field)) is not None and n < o
        for field in ("score_home", "score_away")
    )
    # Y1: the regression veto only fires BETWEEN SIDES OF THE SAME TERMINALITY
    # CLASS. Across classes (one live, one terminal) the genuinely-terminal side
    # is the provider's final view and wins status/clock/events/payload/identity
    # regardless of a score regression; per-side max still keeps the higher
    # score, so an inflated transient score itself persists (see Y1 in docstring).
    vetoed = regressed and (_is_terminal(new.status) == _is_terminal(old.status))
    if vetoed:
        winner, other = old, new
    elif _is_terminal(new.status) and not _is_terminal(old.status):
        winner, other = new, old
    elif _is_terminal(old.status) and not _is_terminal(new.status):
        winner, other = old, new
    else:
        # Both terminal or neither terminal (and not vetoed): the newer side wins.
        winner, other = new, old
    status = winner.status
    # Z3: a vetoed side (the bogus reset ``new``, now ``other``) is untrustworthy
    # for EVERY field — it contributes no clock, so the trusted incumbent's null
    # minute is never filled from the reset board's mid-match clock.
    minute, display_clock = _paired_clock(winner, other, other_vetoed=vetoed)
    # Z1: whenever the winner is AUTHORITATIVE — either the same-class regression
    # veto (X3) fired, or the sides differ in terminality class so the genuinely
    # terminal side was chosen as winner — that winner wins events, payload, AND
    # identity precedence SYMMETRICALLY (whichever side is newer is irrelevant; the
    # authoritative side wins), and the other side contributes ONLY the fields the
    # winner lacks. This closes the round-14 leak where a score-regressed bogus
    # LIVE board still fed junk events/identity/payload to a terminal incumbent
    # across classes (only status/clock were routed to the terminal side before).
    winner_authoritative = vetoed or (_is_terminal(winner.status) != _is_terminal(other.status))
    if winner_authoritative:
        # The authoritative winner keeps its events; the other side contributes
        # only when the winner has none. (X4's "newer terminal non-empty list wins
        # outright" is the same principle — the terminal side is the winner here.)
        events = winner.events if winner.events else other.events
    elif new.events and _is_terminal(new.status):
        events = new.events  # X4: newer terminal list is authoritative
    elif not new.events:
        events = old.events
    elif not old.events:
        events = new.events
    else:
        events = new.events if len(new.events) >= len(old.events) else old.events
    if winner_authoritative:
        # Authoritative-winner payload precedence — winner's non-empty values win,
        # the other side only fills keys the winner lacks (or where it is empty).
        payload = merge_payload_preserving_richer(other.payload, winner.payload)
    else:
        payload = merge_payload_preserving_richer(old.payload, new.payload)
    if winner_authoritative:
        # Identity fields also keep the authoritative winner's values, filling
        # only where the winner has None.
        home_team = winner.home_team if winner.home_team is not None else other.home_team
        away_team = winner.away_team if winner.away_team is not None else other.away_team
        kickoff_utc = winner.kickoff_utc if winner.kickoff_utc is not None else other.kickoff_utc
    else:
        home_team = new.home_team if new.home_team is not None else old.home_team
        away_team = new.away_team if new.away_team is not None else old.away_team
        kickoff_utc = new.kickoff_utc if new.kickoff_utc is not None else old.kickoff_utc
    return replace(
        new,
        status=status,
        minute=minute,
        score_home=score_home,
        score_away=score_away,
        display_clock=display_clock,
        events=events,
        home_team=home_team,
        away_team=away_team,
        kickoff_utc=kickoff_utc,
        payload=payload,
    )


def _paired_clock(
    winner: NormalizedMatch, other: NormalizedMatch, *, other_vetoed: bool = False
) -> tuple[int | None, str | None]:
    """X1 + Y4 + Z3/Z4: minute/display_clock filled PER-FIELD among ELIGIBLE sides.

    The clock is sourced from status-eligible sides only, never from an
    ineligible one. The ``winner`` (which defines the winning status) supplies
    every field it knows; each field the winner leaves null is then filled
    INDEPENDENTLY (Y4) from the ``other`` side, but ONLY when ``other`` is
    eligible. Eligibility now has two gates:

    * STATUS (X1): ``other`` shares the winning status, or (when the winner is
      terminal) is itself terminal. A live side's mid-match clock can never leak
      into an accumulated terminal.
    * NOT VETOED (Z3): when ``other`` is the score-regression-vetoed reset board
      (``other_vetoed``), it is untrustworthy for EVERY field and contributes NO
      clock at all — its minute 3 never fills the trusted incumbent's null minute
      (which would otherwise pair minute 3 with the incumbent's display_clock).

    Even among eligible sides, a field is filled only when it does NOT CONTRADICT
    the winner's own clock (Z4 — no self-contradictory mixed-time pairs):

    * display_clock is filled from ``other`` only when ``other.minute`` does not
      contradict the winner's minute (``other.minute`` is None, or the winner has
      no minute, or they are equal). So winner (90, None) + other (None, "90'+3")
      → (90, "90'+3") [Y4], but winner (90, None) + other (87, "87'") → (90, None)
      [Z4: 87 ≠ 90 contradicts, so no fill].
    * minute is filled from ``other`` only when ``other.display_clock`` does not
      contradict the winner's display_clock (``other.display_clock`` is None, or
      the winner has none, or they are equal).

    The winner's own known field is never overwritten; guards compare against the
    winner's ORIGINAL clock so the two fills cannot contaminate each other.
    """
    minute = winner.minute
    display_clock = winner.display_clock
    if minute is not None and display_clock is not None:
        return minute, display_clock
    if other_vetoed:  # Z3: a vetoed reset board contributes no clock, period.
        return minute, display_clock
    other_eligible = other.status == winner.status or (
        _is_terminal(winner.status) and _is_terminal(other.status)
    )
    if not other_eligible:
        return minute, display_clock
    # Y4 + Z4: fill each null field independently from the eligible other side, but
    # ONLY when the borrowed field does not contradict the winner's own clock.
    if minute is None and other.minute is not None:
        if (
            winner.display_clock is None
            or other.display_clock is None
            or other.display_clock == winner.display_clock
        ):
            minute = other.minute
    if display_clock is None and other.display_clock is not None:
        if winner.minute is None or other.minute is None or other.minute == winner.minute:
            display_clock = other.display_clock
    return minute, display_clock


def _max_known(a: int | None, b: int | None) -> int | None:
    """Per-side score max where ``None`` (unknown) never replaces a known value."""
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def _non_regressing_score(candidate: int | None, stored: int | None) -> int | None:
    """Per-side max with ``None``-fill (G1): never below the stored value."""
    if stored is None:
        return candidate
    if candidate is None:
        return stored
    return max(candidate, stored)


class CollectorEngine:
    """Polls one pack's provider and writes deltas into the shared database.

    Parameters
    ----------
    pack:
        The loaded :class:`~gamecollect.packs.spec.SportPack`; supplies the
        provider factory, side-table DDL, taxonomy, and the ``seed_match`` hook.
    db_path:
        SQLite file to open (created if absent) with the pack's side tables.
    source:
        The partition this daemon owns; stamped on every write.
    poll_interval:
        Seconds between healthy polls (before jitter) and the backoff base.
    provider:
        Provider to poll. Defaults to ``pack.provider_factory()``; tests and the
        replay harness inject one (a live vs. replay provider is invisible to
        the loop).
    record_path:
        When set, accumulate every polled snapshot and, on shutdown, write one
        fixture JSON per match here. A ``.json`` path names a single fixture
        file for one match, or (with several matches) sibling
        ``<stem>-<match_id>.json`` files next to it; any other path is treated
        as a directory of ``<match_id>.json``. This is the ``--record`` writer.
    rng:
        Source of jitter (injectable for deterministic tests).
    sleep:
        Delay function ``(seconds) -> None``. Defaults to a stop-event wait so
        SIGTERM interrupts it; tests inject a hook that records delays and stops
        the loop.
    """

    def __init__(
        self,
        pack: SportPack,
        db_path: str | Path,
        source: str,
        poll_interval: float = 30.0,
        *,
        provider: MatchDataProvider | None = None,
        record_path: str | Path | None = None,
        rng: random.Random | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self._pack = pack
        self._db_path = Path(db_path)
        self._source = source
        self._poll_interval = float(poll_interval)
        self._provider = provider if provider is not None else pack.provider_factory()
        # Fail fast on an unusable --record target BEFORE any polling (in
        # RecordWriter.__init__): a bad path discovered only at shutdown would
        # lose the whole recorded session (close() flushes at most once).
        self._recorder: RecordWriter | None = (
            RecordWriter(Path(record_path)) if record_path is not None else None
        )
        self._rng = rng if rng is not None else random.Random()
        self._stop_event = threading.Event()
        self._sleep = sleep if sleep is not None else self._stop_event.wait

        self._conn = connect(self._db_path, side_table_ddl=pack.side_table_ddl)
        self._writer = PartitionWriter(self._conn, source, taxonomy=pack.taxonomy)

        # Last-written provider snapshot per provider-native match_id (the diff
        # baseline) and, when recording, the merged fixture accumulator.
        self._last: dict[str, NormalizedMatch] = {}
        # One tracker per in-flight live→terminal transition, keyed by
        # provider-native match id (see _TransitionTracker for the lifecycle
        # invariants). Popped ONLY by _resolve_tracker after a non-live
        # snapshot for the match was durably applied.
        self._transitions: dict[str, _TransitionTracker] = {}
        # Match ids in an abandonment COOLDOWN (see _Cooldown / _emit_give_up):
        # a match whose give-up snapshot could not be durably applied is
        # re-tracked only after its cooldown GATE (remaining polls) lapses, the
        # interval doubling per successive abandonment. Gates BOTH the off-slate
        # vanished re-tracking path and the on-slate give-up machinery so a
        # persistently-unapplyable match retries at exponentially spaced intervals
        # instead of hot-looping a fresh give-up every poll; a board whose detail
        # fetch SUCCEEDS is still applied normally while cooling, and richer
        # observations accrue into the entry's `fallback` (no tracker is created
        # while cooling). The GATE is cleared (remaining → 0, epoch preserved) by
        # the next durable state-advancing apply (_on_durable_snapshot); the ENTRY
        # is removed only by a terminal resolution (_resolve_tracker) or the
        # expired-and-unseen purge (_age_cooldowns).
        self._cooldowns: dict[str, _Cooldown] = {}
        self._backoff_multiplier = 1
        # One-time restart scan latch: on the FIRST successful poll the stored
        # matches table is scanned for rows this source left live at shutdown
        # whose provider id never reappears on the slate, and trackers are
        # seeded for them (see _seed_restart_trackers). Never re-run after it
        # completes — steady-state polls must not pay a table scan.
        self._restart_scan_done = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Block polling until :meth:`stop` (or SIGTERM); always closes cleanly.

        Installs a SIGTERM handler when called on the main thread (a no-op
        elsewhere, e.g. under a test harness). Each iteration polls, then sleeps
        the jittered interval on success or the current backoff on a provider
        error; the injected ``sleep`` can end the loop by calling :meth:`stop`.
        """
        self._install_signal_handler()
        try:
            while not self._stop_event.is_set():
                delay = self._poll_and_next_delay()
                if self._stop_event.is_set():
                    break
                self._sleep(delay)
        finally:
            self.close()

    def _poll_and_next_delay(self) -> float:
        """Run one poll; return the delay to sleep before the next.

        Provider unavailability/shape drift is absorbed here into backoff so the
        loop never sees an exception; a successful poll resets backoff and
        returns the jittered healthy interval.
        """
        try:
            self.poll_once()
        except ShapeDriftError as exc:
            log.error(
                "provider shape drift on source %s: %s (backing off, loop continues)",
                self._source,
                exc,
                exc_info=True,
            )
            return self._next_backoff_delay()
        except ProviderUnavailableError as exc:
            log.warning(
                "provider unavailable on source %s: %s (backing off, loop continues)",
                self._source,
                exc,
            )
            return self._next_backoff_delay()
        self._backoff_multiplier = 1
        return self._jittered_interval()

    def poll_once(self) -> None:
        """Fetch, diff, and write one poll's worth of matches.

        Provider errors from ``fetch_live_matches`` propagate (the loop turns
        them into backoff); any per-match writer rejection
        (:class:`~gamecollect.db.writer.SequenceError` and its siblings —
        taxonomy/unseeded/cross-partition) is caught and isolated so one bad
        match cannot wedge the others or the daemon.

        The diff baseline (``self._last``) is advanced only when ``_apply``
        reports the match was actually written: a match whose ``seed_match``
        returned ``None`` (missing identity, child writes skipped) or whose
        write raised leaves the baseline untouched, so a later poll re-diffs
        the FULL event list and catches up rather than baking the skipped
        events into the baseline and losing them forever. ``_apply`` returns
        the baseline snapshot to store — normally the incoming snapshot, but
        after a sequence-drift reconciliation one whose event list is rebuilt
        from the ACTUALLY-STORED rows, so unresolved drift (a conflicted or
        retroactively-inserted seq) keeps re-diffing and re-logging instead of
        being silently baselined away.
        """
        matches, live_resumptions = self._fetch_poll_snapshots()
        if self._recorder is not None:
            self._recorder.accumulate(matches)
        for diff in diff_matches(matches, self._last):
            if not diff.has_changes:
                # Stored state already durably matches this snapshot. For a
                # non-live snapshot this is a genuine durable state (the row IS
                # persisted terminal), so a pending terminal tracker resolves
                # and its cooldown clears. But a no-change LIVE snapshot
                # advanced NOTHING (snapshot == baseline, zero writes): it must
                # NOT clear a cooldown or drop a resumption's tracker, so it is
                # routed with ``state_advanced=False`` (its live branch is a
                # no-op) — see _on_durable_snapshot.
                self._on_durable_snapshot(
                    diff.match.match_id,
                    diff.match,
                    live_resumptions=live_resumptions,
                    state_advanced=False,
                )
                continue
            try:
                baseline = self._apply(diff)
            except SequenceError as exc:
                # Defensive net: _apply reconciles fingerprint drift itself
                # against the stored rows, so this only fires for a batch that
                # failed pre-insert validation in a way the reconciliation also
                # could not absorb. Leave self._last unchanged so a corrected
                # list re-syncs.
                log.error(
                    "event drift for match %s on source %s: %s (skipping match this poll)",
                    diff.match.match_id,
                    self._source,
                    exc,
                )
                continue
            except (TaxonomyError, UnseededMatchError, CrossPartitionError) as exc:
                # Undeclared type / unseeded / foreign-partition write: a bad
                # match must not crash the daemon. Log loudly and skip; the
                # baseline is untouched so a corrected poll re-tries the full
                # list. Non-writer exceptions stay fatal (they propagate).
                log.error(
                    "writer rejected match %s on source %s: %s (skipping match this poll)",
                    diff.match.match_id,
                    self._source,
                    exc,
                )
                continue
            if baseline is not None:
                self._last[diff.match.match_id] = baseline
                # CONSUME point of the transition state machine: a DURABLE apply
                # (a real write landed, ``state_advanced=True``) resolves a
                # pending terminal tracker (and clears any abandonment cooldown),
                # or reconciles a live resumption's fallback against the applied
                # state. An apply failure above (baseline None / exception)
                # retains the tracker so the captured terminal state is retried
                # next poll.
                self._on_durable_snapshot(
                    diff.match.match_id,
                    baseline,
                    live_resumptions=live_resumptions,
                    state_advanced=True,
                )

    def _apply(self, diff: MatchDiff) -> NormalizedMatch | None:
        """Seed the match through the pack hook, then append its new events.

        Returns the snapshot the caller should advance the diff baseline to:
        the incoming snapshot on a clean write, a snapshot whose event list is
        rebuilt from the ACTUALLY-STORED rows after a sequence-drift
        reconciliation (so unresolved drift keeps re-surfacing), or ``None``
        when ``seed_match`` returned ``None`` and child writes were skipped —
        a skipped match must NOT be baselined, or its events would be treated
        as already-written on the next seedable poll. ``None`` is also
        returned when the pack's ``persist_side_tables`` hook raises: the
        failure is logged (never fatal to the daemon) and the un-advanced
        baseline gives side-table persistence a retry on the next poll.
        """
        match = diff.match
        seeded_id = self._pack.seed_match(self._conn, self._writer, match)
        if seeded_id is None:
            # Missing identity: the row cannot be seeded, so child writes would
            # raise UnseededMatchError. Skip them (state was not written either);
            # a later poll with identity fields will seed and catch up.
            log.info(
                "seed_match returned None for match %s on source %s; skipping child writes",
                match.match_id,
                self._source,
            )
            return None
        baseline = match
        if diff.new_events:
            rows = [_event_to_row(e) for e in diff.new_events]
            try:
                self._writer.append_events(seeded_id, rows)
            except SequenceError:
                # At least one incoming seq conflicts with the stored stream
                # (an in-place correction, a retroactive insert below the
                # stored head, or a positional shift). append_events is
                # all-or-nothing, so the whole batch — including genuinely NEW
                # events — was rejected. Reconcile against the stored rows:
                # only seqs strictly beyond the stored head are written, the
                # conflicts stay loud, and the baseline is rebuilt from what
                # is ACTUALLY stored so persistent drift re-diffs (and
                # re-logs) every poll instead of being silently accepted.
                stored_events = self._reconcile_sequence_conflict(seeded_id, match.match_id, rows)
                baseline = replace(match, events=stored_events)
        try:
            self._pack.persist_side_tables(self._conn, self._writer, match, seeded_id)
        except Exception as exc:
            # A pack hook must never kill the daemon (a malformed payload
            # value binding into sqlite raises InterfaceError, for example).
            # Core writes above already committed in their own transactions,
            # so failing here would otherwise leave durable core state with
            # missing/stale side tables and NO retry path. Returning None
            # keeps the baseline un-advanced: the next poll re-diffs the full
            # match, the idempotent core appends no-op, and the side-table
            # hook gets a natural retry.
            log.error(
                "persist_side_tables failed for match %s (seeded id %s) on source %s: %s "
                "(baseline not advanced; side tables retried next poll)",
                match.match_id,
                seeded_id,
                self._source,
                exc,
                exc_info=True,
            )
            return None
        return baseline

    def _reconcile_sequence_conflict(
        self, seeded_id: str, provider_match_id: str, rows: list[dict[str, Any]]
    ) -> list[NormalizedEvent]:
        """Resolve a rejected batch against the stored rows; return what is stored.

        The stored seq set is read from the database (not guessed from engine
        memory). Policy, per seq in ascending order:

        * seq at-or-below the stored head and STORED — attempt a
          single-row append: an identical fingerprint is the writer's
          idempotent no-op; a mutated fingerprint (in-place provider
          correction) raises :class:`~gamecollect.db.writer.SequenceError`,
          logged at ERROR — the stored row is never mutated (append-only).
        * seq at-or-below the stored head and NOT stored — a retroactive
          insertion (e.g. a VAR-restored event): never written (it would
          violate the writer's monotonic invariant and could interleave two
          timelines), logged at ERROR. Because the returned baseline excludes
          it, it re-diffs and re-logs on every poll it persists —
          loud-but-alive, not silently swallowed.
        * seq strictly beyond the stored head — appended one row at a time in
          ascending order, so the writer's strictly-increasing invariant holds
          for every insert it performs. Appended seqs join the stored set, so
          a seq the SAME snapshot carries twice is classified as a
          duplicate-in-snapshot (WARN, second copy never written), not as a
          retroactive insertion.

        Other writer rejections stay fatal to the poll for this match (they
        propagate to ``poll_once``'s per-match isolation). Returns the events
        stored AFTER reconciliation, for the caller to baseline from.
        """
        stored_seqs = {event.seq for event in self._stored_events(seeded_id)}
        head = max(stored_seqs) if stored_seqs else None
        appended_this_batch: set[int] = set()
        for row in rows:
            seq = row.get("seq")
            if head is not None and isinstance(seq, int) and seq <= head:
                if seq in appended_this_batch:
                    # The SAME snapshot carried this seq twice and the first
                    # copy was just appended: a duplicate-in-snapshot, not a
                    # retroactive insertion below a pre-existing head. Never
                    # written twice; WARN, not ERROR.
                    log.warning(
                        "duplicate seq %s within one snapshot for match %s on "
                        "source %s: first copy appended this poll, duplicate "
                        "NOT written",
                        seq,
                        provider_match_id,
                        self._source,
                    )
                    continue
                if seq in stored_seqs:
                    try:
                        self._writer.append_events(seeded_id, [row])
                    except SequenceError as exc:
                        log.error(
                            "event drift for match %s seq %s on source %s: %s "
                            "(keeping stored row, skipping this event)",
                            provider_match_id,
                            seq,
                            self._source,
                            exc,
                        )
                else:
                    log.error(
                        "event drift for match %s seq %s on source %s: retroactive "
                        "event insertion below stored head %s; event NOT written "
                        "(re-logged every poll it persists)",
                        provider_match_id,
                        seq,
                        self._source,
                        head,
                    )
                continue
            try:
                self._writer.append_events(seeded_id, [row])
            except SequenceError as exc:
                # Defensive: a malformed seq (non-int, duplicate) the head
                # check above could not classify.
                log.error(
                    "event drift for match %s seq %s on source %s: %s (skipping this event)",
                    provider_match_id,
                    seq,
                    self._source,
                    exc,
                )
                continue
            if isinstance(seq, int):
                head = seq if head is None else max(head, seq)
                stored_seqs.add(seq)
                appended_this_batch.add(seq)
        return self._stored_events(seeded_id)

    def _stored_events(self, seeded_id: str) -> list[NormalizedEvent]:
        """Read the ACTUALLY-STORED event rows back as :class:`NormalizedEvent`s.

        The inverse of :func:`_event_to_row` (``team``/``player``/``assist``
        ride in the JSON payload), so a reconstructed baseline compares equal
        to an unchanged incoming event under the diffing fingerprint.
        """
        rows = self._conn.execute(
            "SELECT seq, type, minute, importance, detail, payload FROM events "
            "WHERE source = ? AND match_id = ? ORDER BY seq",
            (self._source, seeded_id),
        ).fetchall()
        events: list[NormalizedEvent] = []
        for seq, event_type, minute, importance, detail, payload in rows:
            extras = json.loads(payload) if payload else {}
            events.append(
                NormalizedEvent(
                    seq=seq,
                    minute=minute,
                    event_type=event_type,
                    importance=importance,
                    team=extras.get("team"),
                    player=extras.get("player"),
                    assist=extras.get("assist"),
                    detail=detail,
                )
            )
        return events

    def _log_detail_fetch_failure(
        self, match_id: str, exc: ProviderUnavailableError | ShapeDriftError
    ) -> None:
        """Log a detail-fetch failure: ERROR for shape drift, WARN for outage."""
        if isinstance(exc, ShapeDriftError):
            log.error(
                "detail shape drift for match %s on source %s: %s",
                match_id,
                self._source,
                exc,
                exc_info=True,
            )
        else:
            log.warning(
                "detail fetch unavailable for match %s on source %s: %s",
                match_id,
                self._source,
                exc,
            )

    def _log_drift_while_cooling(self, match_id: str, exc: ShapeDriftError) -> None:
        """Log a schema-drift failure seen while cooling, DAMPED to once/epoch (L3).

        Schema drift is a PROVIDER-level signal, so it must surface even while a
        match is cooling — but a per-poll ERROR (with traceback) for up to
        ``_COOLDOWN_MAX_POLLS`` polls is noise, not signal. The FIRST drift of a
        cooldown epoch logs the full ERROR (via
        :meth:`_log_detail_fetch_failure`); later drifts in the SAME epoch drop to
        DEBUG. The per-epoch flag lives on the :class:`_Cooldown` entry and is
        reset by :meth:`_enter_cooldown` when a new epoch begins.
        """
        cooldown = self._cooldowns.get(match_id)
        if cooldown is not None and not cooldown.drift_logged:
            cooldown.drift_logged = True
            self._log_detail_fetch_failure(match_id, exc)
            return
        log.debug(
            "detail shape drift for match %s on source %s while cooling "
            "(already logged this cooldown epoch): %s",
            match_id,
            self._source,
            exc,
        )

    def _fetch_poll_snapshots(self) -> tuple[list[NormalizedMatch], set[str]]:
        """Fetch the live slate, replacing in-progress rows with full detail.

        The provider contract allows ``fetch_live_matches`` to return a
        scoreboard-level snapshot whose event list is empty or partial. The
        engine must diff/write the detail snapshot for live matches so adapters
        such as ESPN can persist events and payload extras that only exist on a
        per-match summary endpoint. ``fetch_match_detail`` is a pure current-state
        read for replay/fakes, so this does not advance replay beyond the single
        ``fetch_live_matches`` call that defines a poll.

        A match whose PREVIOUS baseline was live is also hydrated even when the
        fresh status is not: the transition poll's final scoreboard snapshot
        carries ``events=[]`` (the documented ESPN shape), so skipping detail
        there would permanently lose stoppage-time events. The live baseline
        may also live only in STORAGE: after a daemon restart ``self._last``
        is empty, so a non-live slate match with no in-memory baseline
        consults the stored ``matches`` row — a stored live status means the
        live→final transition happened across the restart and the match is
        hydrated all the same.

        One match's failed detail fetch must not abort the slate: it is logged
        (WARNING for unavailability, ERROR for shape drift) and that match
        falls back to its scoreboard snapshot while the others proceed —
        per-match isolation. EXCEPT on a live→terminal transition poll: there
        the sparse fallback snapshot would become the final baseline and the
        detail would never be re-fetched, so the match is skipped this poll
        (live baseline kept, the non-live scoreboard snapshot captured as the
        tracker's fallback) and the hydration retried while the match stays
        on the slate. Retry bookkeeping lives in one ``_TransitionTracker``
        per match (see its docstring for the lifecycle invariants); at either
        cap the engine emits a best-known give-up snapshot (merged over the
        baseline/fallback/stored base — never raw) and the tracker is popped
        only after that snapshot is durably applied. Slate-level errors from
        ``fetch_live_matches`` still propagate to the loop's backoff.

        A transition hydration that succeeds but yields a NON-terminal merged
        status is not persisted when non-live (a SCHEDULED/UNKNOWN provider
        reset must not clobber live state or drop the match out of tracking)
        and counts toward the tracker's total-attempts cap either way, so a
        provider that never turns terminal cannot loop forever.

        Previously-live matches that dropped OFF the slate entirely are
        hydrated too (see :meth:`_hydrate_vanished_matches`): a scoreboard may
        drop a match without ever showing its final status, and the stored
        row must not stay in-play forever.
        """
        matches = self._provider.fetch_live_matches()
        slate_ids = {m.match_id for m in matches}
        # Provider ids that appeared LIVE on the slate this poll. Poll-scoped
        # (returned to poll_once, never held as instance state) so a fetch
        # exception cannot leak the previous poll's ids into this one's apply
        # reconciliation. Only these are treated as resumptions when their apply
        # lands durably; a still-live vanished-match progress snapshot flows
        # through the same apply path but must NOT reset its tracker.
        live_resumptions: set[str] = set()
        # One poll elapsed: age every active abandonment cooldown before this
        # poll's re-track gating reads it (an expired cooldown re-tracks now),
        # purging any long-expired entry whose match has vanished for good.
        self._age_cooldowns(slate_ids)
        if not self._restart_scan_done:
            # First successful poll after (re)start: seed trackers for rows
            # left live in storage whose provider id is off the slate. Runs
            # AFTER fetch_live_matches so a provider outage at startup does
            # not consume the one-shot scan on a slate we never saw.
            self._seed_restart_trackers(slate_ids)
            self._restart_scan_done = True
        hydrated: list[NormalizedMatch] = []
        for match in matches:
            if match.status in LIVE_STATUSES:
                # Live board on the slate: reset any tracker's retry BUDGET now,
                # unconditionally, so an exhausted episode can never inherit into
                # — and force-close — a genuinely live match, even if this poll's
                # apply later fails. The fallback-supersession decision and the
                # abandonment-cooldown clear both wait for the DURABLY APPLIED state
                # (:meth:`_reconcile_live_apply`, from ``poll_once``): the raw
                # slate board's scores are often NULL (they arrive only from
                # ``fetch_match_detail``, merged below), so neither can be judged
                # here.
                live_resumptions.add(match.match_id)
                self._reset_resumption_budget(match.match_id)
            previous = self._last.get(match.match_id)
            was_live = (
                previous is not None and previous.status in LIVE_STATUSES
            ) or match.match_id in self._transitions
            if not was_live and previous is None and match.status not in LIVE_STATUSES:
                # No in-memory baseline (typically the first poll after a
                # restart): the transition may have happened while we were
                # down — the stored row still says live. Cheap: fires only
                # until the match is first baselined or tracker'd.
                was_live = self._stored_status_is_live(match.match_id)
            if match.status not in LIVE_STATUSES and not was_live:
                hydrated.append(match)
                continue
            in_transition = was_live and match.status not in LIVE_STATUSES
            # A recoverable board is ALWAYS processed (fetch/merge/apply): a
            # FINISHED board closes an abandoned row (removing the cooldown entry
            # via _resolve_tracker), a live board resumes it (clearing the cooling
            # gate, epoch preserved). The cooldown gates only the give-up
            # MACHINERY (fallback accrual + re-emission) for a transition whose
            # detail fetch/apply keeps failing — while cooling we do not rebuild a
            # tracker or re-emit a give-up (accrual lands on the cooldown entry),
            # so an unapplyable match cannot hot-loop; the next retry waits for the
            # cooldown to lapse (:meth:`_cooling_down`).
            cooling = self._cooling_down(match.match_id)
            tracker = self._transitions.get(match.match_id)
            if in_transition and tracker is not None and _tracker_at_cap(tracker):
                if cooling:
                    # A leftover at-cap tracker while cooling would only re-emit
                    # a give-up: suppress it (abandonment already popped the
                    # tracker, so this is defensive).
                    continue
                # A give-up is already due (typically re-emitted because its
                # apply did not land last poll): skip the fetch, retry the
                # durable write.
                self._emit_give_up(hydrated, match.match_id, tracker, fresh=match)
                continue
            try:
                detail = self._provider.fetch_match_detail(match.match_id)
            except (ProviderUnavailableError, ShapeDriftError) as exc:
                if in_transition and cooling:
                    # In cooldown: the fetch is STILL attempted (so a recovered
                    # board applies immediately — matrix (d)), but a persistent
                    # ordinary unavailability neither advances a give-up nor
                    # (per L3) spams its ERROR, so an unresolvable board makes
                    # bounded noise — matrix (a). No tracker is created while
                    # cooling; instead the non-live slate board accrues into the
                    # cooldown's own `fallback` (the R1 comparator keeps the
                    # richest) so an observation seen during the quiet window is
                    # not lost from the eventual post-cooldown give-up. Schema
                    # drift is a PROVIDER-level signal (not per-match flapping),
                    # so it is logged — but DAMPED to once per epoch
                    # (:meth:`_log_drift_while_cooling`) rather than a per-poll
                    # ERROR storm. The retry cycle resumes when the gate lapses.
                    cooldown = self._cooldowns[match.match_id]
                    cooldown.fallback = _accrue_fallback(cooldown.fallback, match)
                    if isinstance(exc, ShapeDriftError):
                        self._log_drift_while_cooling(match.match_id, exc)
                    continue
                self._log_detail_fetch_failure(match.match_id, exc)
                if not in_transition:
                    # Live match, per-match isolation: fall back to the
                    # scoreboard snapshot; the others proceed.
                    hydrated.append(match)
                    continue
                tracker = self._tracker(match.match_id)
                tracker.fallback = _accrue_fallback(tracker.fallback, match)
                tracker.consecutive_failures += 1
                tracker.total_attempts += 1
                if _tracker_at_cap(tracker):
                    self._emit_give_up(hydrated, match.match_id, tracker, fresh=match)
                    continue
                log.warning(
                    "final-detail fetch failed on live→final transition for "
                    "match %s on source %s (attempt %d/%d): skipping match "
                    "this poll, keeping live baseline; hydration retried "
                    "next poll",
                    match.match_id,
                    self._source,
                    tracker.consecutive_failures,
                    _MAX_TRANSITION_DETAIL_FAILURES,
                )
                continue
            if tracker is not None:
                tracker.consecutive_failures = 0
            merged = _merge_detail(match, detail)
            if in_transition and not _is_terminal(merged.status):
                # The transition hydration succeeded but did NOT yield a
                # terminal snapshot: either the detail still says live over a
                # non-live board, or both report a SCHEDULED/UNKNOWN provider
                # reset. Persist live progress, but never a non-live
                # non-terminal reset.
                if cooling:
                    # In cooldown: accrue the richest observation into the
                    # COOLDOWN entry (NOT a tracker — creating one here would
                    # re-arm the give-up machinery the cooldown exists to
                    # suppress) so a richer observation during the quiet window —
                    # e.g. a 2-0 detail over a 1-0 baseline — is not dropped and
                    # the eventual post-cooldown give-up does not close with a
                    # stale score. Persist live progress (a durable live apply
                    # clears the cooling gate) but do NOT advance any attempt
                    # count or emit a give-up off a non-terminal reset.
                    cooldown = self._cooldowns[match.match_id]
                    cooldown.fallback = _accrue_fallback(cooldown.fallback, merged)
                    if merged.status in LIVE_STATUSES:
                        hydrated.append(merged)
                    continue
                # Count the attempt (bounded by the total cap) and accrue the
                # detail-MERGED snapshot into the fallback either way (M4): the
                # sparse slate board would drop the detail-only score/events the
                # adjacent cooling branch already accrues via ``merged``.
                tracker = self._tracker(match.match_id)
                tracker.fallback = _accrue_fallback(tracker.fallback, merged)
                tracker.total_attempts += 1
                self._warn_nonterminal(match.match_id, tracker, merged.status)
                if _tracker_at_cap(tracker):
                    self._emit_give_up(hydrated, match.match_id, tracker, fresh=merged)
                    continue
                if merged.status in LIVE_STATUSES:
                    hydrated.append(merged)
                continue
            hydrated.append(merged)
        hydrated.extend(self._hydrate_vanished_matches(slate_ids))
        return hydrated, live_resumptions

    def _hydrate_vanished_matches(self, slate_ids: set[str]) -> list[NormalizedMatch]:
        """Hydrate previously-live matches that dropped off the slate.

        A live match can simply vanish from ``fetch_live_matches`` without
        ever showing a final status on the slate (an early scoreboard drop),
        or drop off mid transition-retry. Without an explicit detail fetch its
        stored row would stay live forever and final-only events (stoppage
        winners, full-time) would be lost.

        Tracking: a match is vanished-tracked while its diff baseline in
        ``self._last`` is still live, OR while it has a pending
        ``_TransitionTracker`` (the restart-gap case tracks in storage, not
        ``self._last``), and its id is absent from the current slate. Each
        tracked match holds one tracker; a per-match isolated
        ``fetch_match_detail`` is attempted unless a give-up is already due.
        A terminal detail is merged over the best available base (in-memory
        baseline, else the tracker's fallback, else the stored row) and flows
        through the normal diff/apply path — the tracker pops only once that
        apply lands. A still-live detail keeps the match tracked (the
        scoreboard may have dropped it early) and its merged progress is
        persisted WITHOUT letting a terminal fallback base force-close it; a
        SCHEDULED/UNKNOWN reset persists nothing and keeps the fallback. Both
        count toward the total-attempts cap; fetch failures also count toward
        the consecutive-failures cap. At either cap the engine gives up
        LOUDLY, persisting the best-known terminal state (the merged fallback
        when one exists, else the baseline/stored row force-closed to
        FINISHED) — the give-up ERROR states what was persisted and fires
        only after the apply succeeds. A match that vanished ACROSS a daemon
        restart (stored row live, never reappearing on the slate, so both
        ``self._last`` and the trackers start empty) is caught by the
        one-time restart scan (:meth:`_seed_restart_trackers`), which seeds a
        tracker for it on the first poll so this path hydrates it like any
        other vanished match. Residual gap: a canonical/reconciled stored row
        with no ``provider_match_map`` entry cannot be resolved back to a
        provider id — the scan WARNs and leaves it alone (same as pre-fix)
        rather than detail-fetching a non-provider id into a forced close.
        """
        tracked = {
            match_id
            for match_id, previous in self._last.items()
            if match_id not in slate_ids and previous.status in LIVE_STATUSES
        }
        # A pending tracker also marks an unresolved transition (restart-gap
        # or mid-retry): never dropped by slate absence alone.
        tracked.update(m for m in self._transitions if m not in slate_ids)
        # Cooling-down ids must not be re-tracked off a stale live baseline that
        # the failed give-up never advanced — that is the give-up hot-loop. The
        # cooldown lapses after N polls (:meth:`_cooling_down`), so a genuinely
        # unresolved vanished match is retried, just at exponentially spaced
        # intervals rather than every poll.
        tracked -= {m for m in tracked if self._cooling_down(m)}
        snapshots: list[NormalizedMatch] = []
        for match_id in sorted(tracked):
            tracker = self._tracker(match_id)
            if _tracker_at_cap(tracker):
                self._emit_give_up(snapshots, match_id, tracker, fresh=None)
                continue
            try:
                detail = self._provider.fetch_match_detail(match_id)
            except (ProviderUnavailableError, ShapeDriftError) as exc:
                tracker.consecutive_failures += 1
                tracker.total_attempts += 1
                if _tracker_at_cap(tracker):
                    self._emit_give_up(snapshots, match_id, tracker, fresh=None)
                    continue
                log.warning(
                    "match %s vanished from the slate on source %s while live and "
                    "its detail fetch failed (attempt %d/%d): %s — terminal "
                    "hydration retried next poll",
                    match_id,
                    self._source,
                    tracker.consecutive_failures,
                    _MAX_TRANSITION_DETAIL_FAILURES,
                    exc,
                )
                continue
            tracker.consecutive_failures = 0
            base = self._transition_base(match_id, tracker)
            merged = _merge_over_base(base, detail) if base is not None else detail
            if _is_terminal(detail.status):
                # Terminal from the detail itself: persist the merge; the
                # tracker pops only after the apply lands (_resolve_tracker).
                snapshots.append(merged)
                continue
            tracker.total_attempts += 1
            # Feed MERGED status, same as the on-slate path: one consistent
            # dedupe key and log content across both hydration paths.
            self._warn_nonterminal(match_id, tracker, merged.status)
            if _tracker_at_cap(tracker):
                self._emit_give_up(
                    snapshots,
                    match_id,
                    tracker,
                    fresh=merged if detail.status in LIVE_STATUSES else None,
                )
                continue
            if detail.status in LIVE_STATUSES:
                # Still live: persist the merged progress (never force-closed
                # by a terminal fallback base) and stay tracked.
                snapshots.append(merged)
            # SCHEDULED/UNKNOWN reset: persist nothing — the fallback and the
            # live-tracked baseline are kept; the total cap bounds the loop.
        return snapshots

    # ------------------------------------------------------------------
    # Transition-tracker machinery
    # ------------------------------------------------------------------

    def _tracker(self, match_id: str) -> _TransitionTracker:
        """Get-or-create the transition tracker for ``match_id``.

        When a fresh tracker is created and a (possibly lapsed) cooldown entry
        still carries a ``fallback`` — the richest state accrued WHILE cooling,
        since no tracker existed then — the new tracker is SEEDED from it, so a
        rich terminal observation from the quiet window still closes the eventual
        give-up (round-11 R2). Get-or-create is only reached OUTSIDE an active
        cooldown (the cooling branches accrue into the entry directly and never
        call this), so this cannot re-arm a still-suppressed give-up.
        """
        tracker = self._transitions.get(match_id)
        if tracker is None:
            tracker = self._transitions[match_id] = _TransitionTracker()
            cooldown = self._cooldowns.get(match_id)
            if cooldown is not None and cooldown.fallback is not None:
                tracker.fallback = _accrue_fallback(tracker.fallback, cooldown.fallback)
        return tracker

    def _age_cooldowns(self, slate_ids: set[str]) -> None:
        """Decrement every active abandonment cooldown by one poll (floor 0).

        Run once per poll BEFORE the re-track gating reads the cooldowns, so an
        entry that reaches 0 permits a re-track this poll. Expired entries linger
        at 0 (normally popped only by a durable apply) so the ``epoch`` of the
        NEXT abandonment keeps growing the interval.

        To keep ``_cooldowns`` bounded when that durable apply never comes — a
        match that ended and dropped off the slate — an expired entry whose match
        is UNSEEN (absent from ``slate_ids`` and untracked) for a full cap-length
        window (``_COOLDOWN_MAX_POLLS`` consecutive polls) is purged. Losing its
        epoch/backoff memory after so long a silence is acceptable: a match that
        reappears after 256+ quiet polls deserves a fresh epoch. A still-counting
        entry, or one whose match is seen again, resets the unseen streak.
        """
        purge: list[str] = []
        for match_id, cooldown in self._cooldowns.items():
            if cooldown.remaining > 0:
                cooldown.remaining -= 1
                cooldown.expired_unseen = 0
                continue
            if match_id in slate_ids or match_id in self._transitions:
                cooldown.expired_unseen = 0
                continue
            cooldown.expired_unseen += 1
            if cooldown.expired_unseen >= _COOLDOWN_MAX_POLLS:
                purge.append(match_id)
        for match_id in purge:
            del self._cooldowns[match_id]

    def _cooling_down(self, match_id: str) -> bool:
        """True while ``match_id`` is within an unexpired abandonment cooldown."""
        cooldown = self._cooldowns.get(match_id)
        return cooldown is not None and cooldown.remaining > 0

    def _enter_cooldown(self, match_id: str, fallback: NormalizedMatch | None = None) -> int:
        """Abandon ``match_id`` into a fresh cooldown; return the new epoch.

        Each successive abandonment increments the epoch and DOUBLES the poll
        interval (``_COOLDOWN_BASE_POLLS`` × 2^(epoch-1), capped at
        ``_COOLDOWN_MAX_POLLS``), so a match whose apply persistently fails is
        retried at exponentially spaced intervals. A prior entry (an expired one,
        or one whose gate a live recovery zeroed but whose memory survived, see
        L1) supplies the epoch to grow from; the ENTRY is removed only by a
        terminal resolution or the expired-unseen purge, never here.

        ``fallback`` — the abandoned tracker's best-known state — is carried onto
        the new entry (merged with any already accrued, richest kept) so the
        richest terminal observation survives into the cooldown and seeds the
        re-track after the gate lapses. The fresh entry starts ``drift_logged``
        False, arming one schema-drift ERROR for the new epoch (L3).
        """
        previous = self._cooldowns.get(match_id)
        epoch = previous.epoch + 1 if previous is not None else 1
        remaining = min(_COOLDOWN_BASE_POLLS * (2 ** (epoch - 1)), _COOLDOWN_MAX_POLLS)
        carried = fallback
        if previous is not None and previous.fallback is not None:
            carried = (
                _accrue_fallback(previous.fallback, fallback)
                if fallback is not None
                else previous.fallback
            )
        self._cooldowns[match_id] = _Cooldown(remaining=remaining, epoch=epoch, fallback=carried)
        return epoch

    def _on_durable_snapshot(
        self,
        match_id: str,
        snapshot: NormalizedMatch,
        *,
        live_resumptions: set[str],
        state_advanced: bool,
    ) -> None:
        """Route a durably-applied snapshot to the right tracker handler.

        A non-live apply resolves a pending terminal tracker and REMOVES the
        cooldown entry — a stored row already matching the terminal snapshot
        (no-change) IS durably terminal, so this runs regardless of
        ``state_advanced``.

        ANY genuinely durable (``state_advanced``) live apply clears the cooling
        GATE — but NOT the entry (L1): normal collection has demonstrably
        resumed, so re-tracking must no longer be suppressed, yet the epoch /
        backoff memory is PRESERVED so a terminal write that keeps failing after
        the live recovery keeps its exponential spacing (and the WARN-after-first-
        epoch damping) instead of restarting the ladder. The entry is removed
        only by a terminal resolution or the expired-unseen purge. This gate clear
        is the SINGLE owner of the live-side clear (L2): a live DETAIL recovery
        reached via the cooling hydration path (slate board SCHEDULED/UNKNOWN,
        detail returns IN_PLAY) is absent from ``live_resumptions`` (populated only
        from slate-LIVE boards), so gating on membership would leave it cooling
        forever — the gate clear is unconditional on ``state_advanced``. The
        ``state_advanced`` guard is load-bearing: a no-change live flicker
        (snapshot == baseline, zero writes) must NOT clear the gate — without proof
        the state advanced, a bogus flicker would re-arm re-tracking on a match
        that never actually recovered.

        ``live_resumptions`` gates ONLY the resumption tracker semantics
        (:meth:`_reconcile_live_apply`: budget reset / fallback retention): a
        still-live vanished-match progress snapshot is NOT a resumption (absent
        from ``live_resumptions``) — resetting its tracker would stop the
        total-attempts cap from ever force-closing a match whose detail never
        turns terminal.
        """
        if snapshot.status in LIVE_STATUSES:
            if state_advanced:
                self._clear_cooling_gate(match_id, snapshot)
                if match_id in live_resumptions:
                    self._reconcile_live_apply(match_id, snapshot)
            return
        self._resolve_tracker(match_id, snapshot)

    def _clear_cooling_gate(self, match_id: str, applied: NormalizedMatch) -> None:
        """Zero a cooldown's suppression gate while PRESERVING its memory (L1).

        Stops suppressing re-tracking (``remaining`` → 0) but keeps the entry, so
        its ``epoch`` (and any accrued ``fallback``) survives — a terminal write
        that keeps failing after a live recovery keeps its exponential backoff.
        A no-op when the match is not cooling.

        M5: a durable state-advanced live ``applied`` state that reaches or passes
        the cooldown ``fallback``'s known per-side scores proves that fallback
        stale — a bogus pre-recovery terminal board (e.g. FINISHED 1-0 min 88)
        must not survive a durable IN_PLAY 2-1 apply and later seed a re-tracked
        tracker whose stale terminal status/clock would win at give-up. We DROP
        it rather than accrue the live state in: accruing would keep the stale
        terminal side's status/clock (the accumulator prefers the terminal side),
        which is exactly what must not win later. This mirrors the tracker-side
        supersession in :meth:`_reconcile_live_apply`.
        """
        cooldown = self._cooldowns.get(match_id)
        if cooldown is None:
            return
        cooldown.remaining = 0
        if cooldown.fallback is not None and _live_supersedes_cooldown_fallback(
            applied, cooldown.fallback
        ):
            cooldown.fallback = None

    def _reset_resumption_budget(self, match_id: str) -> None:
        """Reset a tracker's retry BUDGET on a live slate reappearance.

        A live board is either a genuine resumption or a one-poll slate
        flicker, and a single poll cannot tell them apart — but either way the
        consumed retry budget must not carry into a later genuine transition,
        or an exhausted episode could force-close a genuinely live match. So
        the budget is reset UNCONDITIONALLY here, at detection, before this
        poll's apply is even attempted (an apply failure must never skip the
        reset). The retained ``fallback`` carries over into the fresh tracker;
        whether it is still worth keeping is a separate question decided against
        the DURABLY APPLIED state (:meth:`_reconcile_live_apply`), not the raw
        slate board whose scores are often NULL. Constructing a fresh tracker
        (rather than clearing fields in place) keeps the budget defaults in one
        place, so a future field cannot silently leak across a resumption.
        """
        tracker = self._transitions.get(match_id)
        if tracker is None:
            return
        self._transitions[match_id] = _TransitionTracker(fallback=tracker.fallback)

    def _reconcile_live_apply(self, match_id: str, applied: NormalizedMatch) -> None:
        """Reconcile a tracked match's RESUMPTION fallback after a DURABLE live apply.

        This handles ONLY the resumption tracker semantics; the cooling GATE was
        already cleared by :meth:`_on_durable_snapshot` (the single owner, L2) —
        this method no longer touches ``_cooldowns`` at all. Only reached with
        ``state_advanced`` (a no-change flicker never gets here) and only for a
        match live on the slate this poll. The retained ``fallback`` is judged
        against the APPLIED state — not the raw slate board, whose scores are
        often NULL until the detail merge lands: a fallback too sparse to persist,
        or one the applied live score has already climbed past, is stale, so its
        tracker is dropped; a rich fallback the live score has NOT surpassed is
        kept (with the freshly reset budget) across a one-poll flicker until the
        match vanishes again — a genuine fresh transition rebuilds the tracker.
        """
        tracker = self._transitions.get(match_id)
        if tracker is None:
            return
        fallback = tracker.fallback
        if (
            fallback is None
            or not _fallback_worth_retaining(fallback)
            or _live_supersedes_captured(applied, fallback)
        ):
            self._transitions.pop(match_id, None)

    def _transition_base(
        self, match_id: str, tracker: _TransitionTracker
    ) -> NormalizedMatch | None:
        """Best available merge base: baseline, else fallback, else stored row."""
        base = self._last.get(match_id)
        if base is None:
            base = tracker.fallback
        if base is None:
            base = self._stored_match_snapshot(match_id)
        return base

    def _warn_nonterminal(
        self, match_id: str, tracker: _TransitionTracker, status: MatchStatus
    ) -> None:
        """WARN about a non-terminal hydration outcome once per state change."""
        if tracker.last_reported_status is status:
            return
        tracker.last_reported_status = status
        if status in LIVE_STATUSES:
            log.warning(
                "match %s on source %s left the live slate but its detail still "
                "says live; keeping it tracked for terminal hydration "
                "(attempt %d/%d)",
                match_id,
                self._source,
                tracker.total_attempts,
                _MAX_TRANSITION_TOTAL_ATTEMPTS,
            )
        else:
            log.warning(
                "match %s on source %s reports non-terminal status %s after being "
                "live (provider status reset?); keeping the captured fallback and "
                "retrying terminal hydration (attempt %d/%d)",
                match_id,
                self._source,
                status.value,
                tracker.total_attempts,
                _MAX_TRANSITION_TOTAL_ATTEMPTS,
            )

    def _emit_give_up(
        self,
        out: list[NormalizedMatch],
        match_id: str,
        tracker: _TransitionTracker,
        fresh: NormalizedMatch | None,
    ) -> None:
        """Queue the best-known give-up snapshot for the normal diff/apply path.

        ``give_up_pending`` stays set until :meth:`_resolve_tracker` observes
        the durable apply, so a failed write re-emits the SAME snapshot next
        poll instead of losing the captured terminal state. When nothing at
        all is known to persist the tracker is abandoned (there is no write
        that could ever succeed). Re-emission is BOUNDED
        (``_MAX_GIVE_UP_EMISSIONS``): a snapshot whose apply never lands
        (e.g. missing identity — ``seed_match`` returns ``None`` every poll)
        is abandoned instead of re-emitting forever. Abandonment pops the
        tracker and enters a capped-exponential cooldown (:meth:`_enter_cooldown`)
        so the give-up cannot hot-loop every poll; the abandonment log is ERROR
        on the first epoch, a single WARN on later epochs.
        """
        snapshot = self._build_give_up_snapshot(match_id, tracker, fresh)
        if snapshot is None:
            self._abandon(
                match_id,
                "giving up on terminal detail hydration for match %s on source %s "
                "after %d consecutive fetch failures / %d total attempts, and no "
                "baseline, fallback, or stored row is known: nothing to persist",
                match_id,
                self._source,
                tracker.consecutive_failures,
                tracker.total_attempts,
            )
            return
        tracker.give_up_emissions += 1
        if tracker.give_up_emissions > _MAX_GIVE_UP_EMISSIONS:
            self._abandon(
                match_id,
                "could not persist terminal state for match %s on source %s: "
                "the give-up snapshot's apply never landed after %d emissions "
                "(e.g. identity missing, seed_match returns None every poll) — "
                "abandoning tracker",
                match_id,
                self._source,
                _MAX_GIVE_UP_EMISSIONS,
            )
            return
        tracker.give_up_pending = True
        out.append(snapshot)

    def _abandon(self, match_id: str, message: str, *log_args: Any) -> None:
        """Pop the tracker, enter a cooldown, and log at the epoch-based level.

        The popped tracker's ``fallback`` — the best-known terminal state — is
        carried onto the cooldown entry (R2) so a rich observation is not lost
        while cooling and seeds the re-track once the gate lapses. The FIRST
        abandonment (epoch 1) logs ERROR; each later epoch logs a single WARN
        (one abandonment per epoch by construction), so a match that keeps
        failing to persist does not spam ERROR every retry cycle while still
        surfacing loudly the first time.
        """
        tracker = self._transitions.pop(match_id, None)
        fallback = tracker.fallback if tracker is not None else None
        epoch = self._enter_cooldown(match_id, fallback=fallback)
        log.log(logging.ERROR if epoch == 1 else logging.WARNING, message, *log_args)

    def _build_give_up_snapshot(
        self, match_id: str, tracker: _TransitionTracker, fresh: NormalizedMatch | None
    ) -> NormalizedMatch | None:
        """Best-known terminal snapshot at give-up — merged, never raw.

        M2: emission is monotonic across the fresh same-poll snapshot, the
        tracker's accumulated fallback, AND the stored row — no lossy SELECTION
        of one. The fresh snapshot and the tracker fallback are accumulated
        field-wise (:func:`_accrue_fallback`) into ONE candidate, so a fresh
        terminal 1-0 board at the cap can never regress a 2-0 that exists only in
        fallback memory (the old code picked the fresh terminal candidate INSTEAD
        of the fallback and only maxed against the stored row). When neither
        exists the candidate falls back to the in-memory baseline, else the stored
        row. The candidate is then merged over the best remaining base
        (baseline → fallback → stored row) so known identity/payload are never
        NULLed by a sparse snapshot; ``_merge_over_base`` only fills ``None``s, so
        it never regresses the accumulated scores.

        The single emission choke point is :meth:`_nonregress_over_stored`, which
        applies the give-up decision table (G0 terminality + clock null, G1 score
        monotonicity, G2 terminal-status precedence, G3 clock monotonicity)
        against the DURABLY-STORED row (fetched once here and threaded through,
        M7) — coercing a still-live or reset candidate to a terminal, clock-null
        close and never regressing a higher already-stored score.
        """
        baseline = self._last.get(match_id)
        stored = self._stored_match_snapshot(match_id)
        # M2: accumulate the fresh same-poll snapshot and the tracker fallback
        # into ONE monotonic candidate — the give-up must not regress below
        # either. Order (fallback, then fresh) is immaterial to the field-wise
        # max/terminal-wins merge.
        candidate = tracker.fallback
        if fresh is not None:
            candidate = _accrue_fallback(candidate, fresh) if candidate is not None else fresh
        if candidate is None:
            candidate = baseline
        if candidate is None:
            candidate = stored
        if candidate is None:
            return None
        base = next(
            (
                c
                for c in (baseline, tracker.fallback, stored)
                if c is not None and c is not candidate
            ),
            None,
        )
        merged = _merge_over_base(base, candidate) if base is not None else candidate
        return self._nonregress_over_stored(match_id, merged, stored)

    def _nonregress_over_stored(
        self, match_id: str, snap: NormalizedMatch, stored: NormalizedMatch | None = _UNSET
    ) -> NormalizedMatch:
        """Round-11 give-up decision table: emit a terminal, score/clock-monotonic close.

        The single emission choke point. Combines the candidate ``snap`` with the
        durably-stored row per the prescribed table (a give-up MUST be terminal so
        the non-live consume point :meth:`_resolve_tracker` can resolve it — a
        live emission would never pop the tracker, never clear the gate, and
        re-emit forever):

        G0 TERMINALITY: the emitted status MUST be terminal. If the candidate's
           status is not terminal it is coerced to FINISHED (the accepted loud
           force-close) AND its minute/display_clock are NULLed — a coerced close
           has no authoritative clock (mirrors the detail-merge doctrine that
           clocks are legitimately null at FT), so it can never ship a stale live
           clock like "80'". A genuinely-terminal candidate keeps its own
           status and clock.
        G1 SCORES: per side the emitted score is the MAX of candidate and stored
           (``None`` = unknown, filled from the other) — a stored non-null score
           is never regressed (cumulative facts).
        G2 STATUS between terminals: a GENUINELY-terminal candidate's status wins
           even over a stored terminal (fresher terminal knowledge repairs stale
           stored state); a COERCED candidate defers to a stored TERMINAL status
           (the store knew the real terminal kind).
        G3 CLOCK: a null candidate clock fills from a stored TERMINAL row's
           non-null minute/display_clock — in BOTH branches (M3): a coerced
           candidate whose clock G0 nulled, AND a genuinely-terminal candidate
           whose detail nulled the clock (a FINISHED null-clock candidate must
           not erase a stored terminal row's real 90/FT clock). A non-null
           candidate clock always stands (repairs a stale stored clock) and is
           never overwritten with the other side's null; both null emits null.

        ``stored`` is threaded in from :meth:`_build_give_up_snapshot` (M7,
        fetched once per emission); direct callers omit it and it is fetched here.
        """
        if stored is _UNSET:
            stored = self._stored_match_snapshot(match_id)
        coerced = not _is_terminal(snap.status)
        if coerced:
            status = MatchStatus.FINISHED  # G0
            minute: int | None = None
            display_clock: str | None = None
        else:
            status = snap.status
            minute = snap.minute
            display_clock = snap.display_clock
        stored_home = stored.score_home if stored is not None else None
        stored_away = stored.score_away if stored is not None else None
        score_home = _non_regressing_score(snap.score_home, stored_home)  # G1
        score_away = _non_regressing_score(snap.score_away, stored_away)  # G1
        if stored is not None and _is_terminal(stored.status):
            if coerced:
                status = stored.status  # G2: stored terminal kind wins over a coerced close
            # G3: a null candidate clock (coerced, or a genuinely-terminal
            # candidate whose detail nulled it) fills from the stored terminal
            # row; a non-null candidate clock stands.
            if minute is None and stored.minute is not None:
                minute = stored.minute
            if display_clock is None and stored.display_clock is not None:
                display_clock = stored.display_clock
        return replace(
            snap,
            status=status,
            minute=minute,
            display_clock=display_clock,
            score_home=score_home,
            score_away=score_away,
        )

    def _resolve_tracker(self, match_id: str, snapshot: NormalizedMatch) -> None:
        """CONSUME point: pop the tracker after a DURABLE non-live apply.

        Called from ``poll_once`` only once ``_apply`` returned a baseline
        (or the snapshot produced no diff because stored state already
        matches). A pending give-up logs its ERROR here — persistence is
        claimed only after the write actually landed. A durable non-live apply
        REMOVES the cooldown entry entirely (the terminal resolution is one of
        the only two entry-removal points, alongside the expired-unseen purge):
        the row has resolved terminally, so the epoch/backoff memory is moot and
        must not linger and block later re-tracking.
        """
        if snapshot.status in LIVE_STATUSES:
            return
        self._cooldowns.pop(match_id, None)
        tracker = self._transitions.pop(match_id, None)
        if tracker is None or not tracker.give_up_pending:
            return
        log.error(
            "giving up on terminal detail hydration for match %s on source %s "
            "after %d consecutive fetch failures / %d total attempts: persisted "
            "best-known terminal state (status %s, score %s-%s) — final-whistle "
            "events may be lost",
            match_id,
            self._source,
            tracker.consecutive_failures,
            tracker.total_attempts,
            snapshot.status.value,
            snapshot.score_home,
            snapshot.score_away,
        )

    # Full row shape for :meth:`_stored_match_snapshot`.
    _STORED_MATCH_COLUMNS: tuple[str, ...] = (
        "status",
        "minute",
        "score_home",
        "score_away",
        "display_clock",
        "kickoff_utc",
        "payload",
        "updated_at",
    )
    # Narrow shape for the :meth:`_stored_status_is_live` hot path: no payload
    # JSON blob, just what deciding + ordering needs.
    _STORED_LIVENESS_COLUMNS: tuple[str, ...] = ("status", "updated_at")

    def _stored_rows_for_provider(
        self, provider_match_id: str, *, columns: tuple[str, ...] = _STORED_MATCH_COLUMNS
    ) -> list[tuple]:
        """Candidate stored ``matches`` rows for a provider id, best-first.

        Gathers every stored row this source could hold for the match under
        all THREE id forms core knows — the bare provider id, the
        source-qualified ``f"{source}:{id}"`` stub, and the canonical
        (reconciled) row reached through ``provider_match_map`` — and returns
        the single BEST row (``LIMIT 1``): both callers
        (:meth:`_stored_match_snapshot`, :meth:`_stored_status_is_live`) read
        only ``rows[0]``. The map join is skipped for ``default_seed_match``
        packs: that hook writes provider-native ids verbatim, so no
        ``provider_match_map`` entry can exist and the join would only cost a
        scan (mirrors :meth:`_resolve_stored_provider_id`'s discrimination).
        This also means a source that switches from a custom-seed pack to
        ``default_seed_match`` over an existing partition will not see
        map-reachable canonical rows written under the old strategy — accepted:
        the partition-per-writer contract does not support switching seeding
        strategies over an existing source partition.

        Ordering is AUTHORITY-FIRST — ``rank ASC, updated_at DESC, match_id
        ASC`` — not freshest-first. Rationale: after reconciliation every write
        lands on the canonical (map-resolved) row, so it is AUTHORITATIVE
        whenever it exists; a stub is meaningful only when no canonical row
        exists. Authority-first means a still-LIVE canonical row is never masked
        by a fresher non-live stub (transition hydration would wrongly be skipped
        and final events lost), and an older live stub never overrides a fresher
        non-live canonical row. Freshness (``updated_at DESC``) is the secondary
        key WITHIN a rank tier; ``match_id ASC`` is a final STABLE tiebreak so two
        distinct canonical rows of the same rank that also tie on ``updated_at``
        (e.g. two providers' rows reconciled under this source) resolve
        deterministically rather than by arbitrary row order. Rank tiers, fully
        deterministic so no tie is left to arbitrary row order:

        * custom-seed packs — canonical (map) 0, source-qualified stub 1, bare
          provider-id stub 2 (writes land on the qualified/canonical form, so
          the bare id is the least authoritative);
        * ``default_seed_match`` packs — bare provider id 0, source-qualified
          stub 1 (this hook writes bare ids verbatim, so the bare row is where
          writes land).

        A row reachable through BOTH the stub arm and the map arm (its canonical
        id happens to equal the bare/qualified id) is returned exactly ONCE via
        ``GROUP BY match_id`` picking ``MIN(rank)`` — plain ``UNION ALL`` would
        otherwise duplicate it, and (per SQLite's single-``MIN`` bare-column
        rule) the grouped row's columns come from that lowest-rank arm.

        ``columns`` selects the row shape: pass :data:`_STORED_MATCH_COLUMNS`
        (default) for the full row :meth:`_stored_match_snapshot` needs, or
        :data:`_STORED_LIVENESS_COLUMNS` for the narrow liveness probe that
        skips the payload JSON blob. Both column sets share this one query
        builder — the three-id-form SQL is never duplicated. ``columns`` MUST
        include ``updated_at`` (the secondary ordering key).
        """
        cols = ", ".join(columns)
        qualified = f"{self._source}:{provider_match_id}"
        if self._pack.seed_match is default_seed_match:
            return self._conn.execute(
                f"SELECT {cols}, CASE WHEN match_id = ? THEN 0 ELSE 1 END AS rank "
                "FROM matches WHERE source = ? AND match_id IN (?, ?) "
                "ORDER BY rank ASC, updated_at DESC, match_id ASC "
                "LIMIT 1",
                (provider_match_id, self._source, provider_match_id, qualified),
            ).fetchall()
        m_cols = ", ".join(f"m.{c}" for c in columns)
        return self._conn.execute(
            f"SELECT {cols}, MIN(rank) AS rank FROM ("
            f"SELECT match_id, {cols}, CASE WHEN match_id = ? THEN 2 ELSE 1 END AS rank "
            "FROM matches WHERE source = ? AND match_id IN (?, ?) "
            "UNION ALL "
            f"SELECT m.match_id, {m_cols}, 0 AS rank "
            "FROM provider_match_map pm "
            "JOIN matches m ON m.source = pm.source AND m.match_id = pm.match_id "
            "WHERE pm.source = ? AND pm.provider_match_id = ?"
            ") candidates "
            "GROUP BY match_id "
            "ORDER BY rank ASC, updated_at DESC, match_id ASC "
            "LIMIT 1",
            (
                provider_match_id,
                self._source,
                provider_match_id,
                qualified,
                self._source,
                provider_match_id,
            ),
        ).fetchall()

    def _stored_match_snapshot(self, provider_match_id: str) -> NormalizedMatch | None:
        """Rebuild a merge base from the stored ``matches`` row, if any.

        Used when neither an in-memory baseline nor a captured fallback
        exists (a restart gap): the stored row supplies identity
        (``home_team``/``away_team`` ride in the payload JSON per the
        ``default_seed_match`` convention, when present) and last-known
        scores so a sparse terminal snapshot does not seed identity-less or
        NULL known scores. Candidate rows are gathered across all THREE id
        forms core knows (via :meth:`_stored_rows_for_provider`): the bare
        provider id, the source-qualified stub, and the canonical
        (reconciled) row resolved through ``provider_match_map``; a
        restart-seeded reconciled match whose detail keeps failing must
        find its canonical row here, or the give-up would have nothing to
        persist and the row would stay in-play forever. Whenever more than
        one id form has a row (e.g. a stale pre-reconciliation ``source:id``
        stub alongside a canonical row), the AUTHORITATIVE row wins — the
        canonical (map-resolved) row when it exists, else the higher-ranked
        stub — with freshness (``updated_at`` DESC) breaking ties only within
        a rank tier (see :meth:`_stored_rows_for_provider`'s authority-first
        order).
        """
        rows = self._stored_rows_for_provider(provider_match_id)
        if not rows:
            return None
        (
            status_value,
            minute,
            score_home,
            score_away,
            display_clock,
            kickoff_utc,
            payload,
            _updated_at,
            _rank,
        ) = rows[0]
        extras = reader.decode_payload(payload)
        try:
            status = MatchStatus(status_value)
        except ValueError:
            status = MatchStatus.UNKNOWN
        return NormalizedMatch(
            match_id=provider_match_id,
            status=status,
            minute=minute,
            score_home=score_home,
            score_away=score_away,
            display_clock=display_clock,
            events=[],
            home_team=extras.get("home_team"),
            away_team=extras.get("away_team"),
            kickoff_utc=kickoff_utc,
        )

    def _stored_status_is_live(self, provider_match_id: str) -> bool:
        """True when the AUTHORITATIVE stored ``matches`` row for this match is live.

        Consulted only for a non-live slate match with no in-memory diff
        baseline (a restart gap). The row is looked up under this source by
        ALL THREE id forms core can know about: the provider-native id (the
        ``default_seed_match`` shape), the source-qualified
        ``f"{source}:{id}"`` unreconciled form (the reader-contract shape
        packs write for unreconciled live matches), and the canonical
        (reconciled) id resolved through ``provider_match_map`` — a pack
        that reconciles provider ids onto canonical rows must still get its
        transition-across-restart hydrated. The candidate rows come from the
        shared :meth:`_stored_rows_for_provider` lookup (narrow-columns mode,
        skipping the payload JSON blob on this hot path — it runs for every
        non-baselined non-live slate match), which also skips the map join
        for ``default_seed_match`` packs, where no map entry can exist.

        Evaluates ONLY the best (authority-first, then freshest) row's status
        — consistent with :meth:`_stored_match_snapshot`, which merges from that
        same row. Scanning "is ANY candidate row live" would let an older live
        stub override a fresher non-live canonical row, wrongly classifying a
        restart-gap match as a live→terminal transition when the canonical row
        already recorded the terminal state; freshest-first would inversely let
        a fresher non-live stub mask a still-live canonical row.
        """
        rows = self._stored_rows_for_provider(
            provider_match_id, columns=self._STORED_LIVENESS_COLUMNS
        )
        if not rows:
            return False
        return rows[0][0] in LIVE_STATUS_VALUES

    def _seed_restart_trackers(self, slate_ids: set[str]) -> None:
        """One-time restart scan: track stored-live matches gone from the slate.

        After a daemon restart ``self._last`` and ``self._transitions`` are
        empty, so a match that was live at shutdown and NEVER reappears on
        the slate would be invisible to both hydration paths — its stored
        row would stay in-play forever and final-only events would be lost.
        On the first successful poll this scans the ``matches`` table for
        rows under this source with a live status and seeds a
        ``_TransitionTracker`` (keyed by PROVIDER id — the id
        ``fetch_match_detail`` takes) for each row genuinely vanished; the
        normal tracker machinery then hydrates it
        (:meth:`_hydrate_vanished_matches`, this same poll).

        Two guards keep the scan from doing harm:

        * A stored row ANY of whose mapped provider ids is on the current
          slate is skipped — the match is still being collected normally
          under one of its ids (or its transition is on the slate for the
          regular paths). Checking the WHOLE mapped set, not just the chosen
          first pick, matters for a multi-provider row: seeding a tracker
          keyed to a non-slate id whose match is actually live under a
          SIBLING id would wedge the row (the live-resumption pop only ever
          clears the id the slate carries) and the give-up could force-CLOSE
          an actively-live canonical row.
        * A row whose provider id cannot be resolved
          (:meth:`_resolve_stored_provider_id` returned ``None``) is skipped
          with the WARNING logged there — detail-fetching a non-provider id
          would only fail to the cap and force-close a possibly-live match,
          worse than leaving the row as-is.
        """
        placeholders = ", ".join("?" for _ in LIVE_STATUS_VALUES)
        rows = self._conn.execute(
            f"SELECT match_id FROM matches WHERE source = ? AND status IN ({placeholders})",
            (self._source, *sorted(LIVE_STATUS_VALUES)),
        ).fetchall()
        for (stored_id,) in rows:
            provider_id, mapped_ids = self._resolve_stored_provider_id(stored_id)
            if provider_id is None or mapped_ids & slate_ids:
                continue
            if provider_id in self._transitions:
                continue
            log.warning(
                "match %s (stored id %s) on source %s was live at the last "
                "shutdown and is absent from the slate after restart: seeding "
                "terminal-hydration tracking",
                provider_id,
                stored_id,
                self._source,
            )
            self._tracker(provider_id)

    def _resolve_stored_provider_id(
        self, stored_match_id: str
    ) -> tuple[str | None, frozenset[str]]:
        """Map a stored ``matches.match_id`` back to its provider-native id.

        Returns ``(chosen_id, mapped_ids)``: the id to hydrate under, plus the
        FULL set of distinct provider ids that could reach this stored row. The
        restart scan skips the row when ANY ``mapped_ids`` is on the slate — see
        the multi-provider note below. ``chosen_id`` is ``None`` (and
        ``mapped_ids`` empty) when no provider id can be trusted.

        ``matches.match_id`` comes in three shapes (schema comment;
        :meth:`_stored_status_is_live`), while the slate and
        ``fetch_match_detail`` speak PROVIDER ids only:

        * source-qualified unreconciled stub ``f"{source}:{provider_id}"`` →
          the suffix after the source prefix (``mapped_ids`` is that one id);
        * bare provider id (the :func:`default_seed_match` shape — that hook
          writes provider-native ids verbatim, so when the pack uses it every
          stored id under this source IS a provider id) → itself;
        * canonical/reconciled id (e.g. a schedule slug, written by a pack's
          own seed hook) → looked up in ``provider_match_map``; with no map
          entry the id is NOT a provider id and cannot be trusted as one, so
          the caller must skip it (WARNING logged here) — the residual gap
          the vanished-hydration docstring documents.

        SINGLE-PROVIDER ASSUMPTION: a source is expected to map each
        canonical row through exactly one provider. When the map holds more
        than one DISTINCT provider id for the row (a multi-provider source),
        this WARNs naming all of them and returns the first in
        ``(provider, provider_match_id)`` order as ``chosen_id`` alongside the
        whole set. Returning the whole set is what lets the restart scan skip
        the row when the live id is a SIBLING of the first pick: keying a
        tracker to a non-slate id whose match is live under another id would
        wedge the row (the live-resumption pop never clears the wrong key) and
        the give-up could force-CLOSE an actively-live canonical row. For rows
        genuinely off-slate the chosen first pick still hydrates, and even if
        its detail fetch never succeeds the give-up path closes the row from
        the stored base (:meth:`_stored_match_snapshot` resolves it via the
        map).
        """
        prefix = f"{self._source}:"
        if stored_match_id.startswith(prefix):
            provider_id = stored_match_id[len(prefix) :]
            return provider_id, frozenset({provider_id})
        if self._pack.seed_match is default_seed_match:
            return stored_match_id, frozenset({stored_match_id})
        rows = self._conn.execute(
            "SELECT DISTINCT provider, provider_match_id FROM provider_match_map "
            "WHERE source = ? AND match_id = ? "
            "ORDER BY provider, provider_match_id",
            (self._source, stored_match_id),
        ).fetchall()
        if rows:
            distinct_ids = {provider_id for _, provider_id in rows}
            if len(distinct_ids) > 1:
                log.warning(
                    "stored match %s on source %s maps to multiple provider ids (%s); "
                    "proceeding with %s from provider %s — single-provider-per-source "
                    "assumption violated; a failed hydration still closes the row "
                    "from the stored base at give-up",
                    stored_match_id,
                    self._source,
                    ", ".join(f"{provider}:{provider_id}" for provider, provider_id in rows),
                    rows[0][1],
                    rows[0][0],
                )
            return rows[0][1], frozenset(distinct_ids)
        log.warning(
            "stored match %s on source %s is live from before a restart and "
            "absent from the slate, but has no provider_match_map entry to "
            "resolve a provider id: cannot hydrate its terminal state — the "
            "row is left as-is",
            stored_match_id,
            self._source,
        )
        return None, frozenset()

    def stop(self) -> None:
        """Signal the loop to finish its current iteration and shut down."""
        self._stop_event.set()

    def close(self) -> None:
        """Flush any recorded fixture and close the database connection (idempotent).

        The flush is attempted at most once (a failure latches on the recorder
        so a second ``close()`` does not re-raise) and the connection is closed
        unconditionally in a ``finally`` — a ``write_fixture`` failure must not
        leak the WAL connection, and the CLI's redundant second ``close()`` must
        be a no-op that re-raises nothing. The recorder's buffer is cleared only
        AFTER a successful flush: a failed flush preserves the accumulated
        session in memory instead of destroying it.
        """
        try:
            if self._recorder is not None:
                self._recorder.flush()
        finally:
            conn = getattr(self, "_conn", None)
            if conn is not None:
                conn.close()
                self._conn = None  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Backoff / jitter
    # ------------------------------------------------------------------

    def _jittered_interval(self) -> float:
        return self._poll_interval * (1.0 + self._rng.uniform(-_JITTER_FRACTION, _JITTER_FRACTION))

    def _next_backoff_delay(self) -> float:
        """Current backoff delay, then advance the multiplier (capped)."""
        delay = self._poll_interval * self._backoff_multiplier
        self._backoff_multiplier = min(self._backoff_multiplier * 2, _MAX_BACKOFF_MULTIPLIER)
        return delay

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    def _install_signal_handler(self) -> None:
        try:
            signal.signal(signal.SIGTERM, self._handle_sigterm)
        except ValueError:
            # Not the main thread (e.g. a test driver): SIGTERM handling is a
            # convenience, not a correctness requirement — stop() still works.
            pass

    def _handle_sigterm(self, signum: int, frame: Any) -> None:
        log.info("SIGTERM received on source %s; shutting down after this poll", self._source)
        self.stop()
