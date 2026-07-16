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
from gamecollect.fallback_merge import (
    accrue_fallback as _accrue_fallback,
)
from gamecollect.fallback_merge import (
    fallback_worth_retaining as _fallback_worth_retaining,
)
from gamecollect.fallback_merge import (
    is_terminal as _is_terminal,
)
from gamecollect.fallback_merge import (
    live_supersedes_cooldown_fallback as _live_supersedes_cooldown_fallback,
)
from gamecollect.fallback_merge import (
    live_supersedes_fallback as _live_supersedes_fallback,
)
from gamecollect.fallback_merge import (
    non_regressing_score as _non_regressing_score,
)
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
# Consecutive detail-fetch failures tolerated on a first-sight-finished
# backfill attempt (see ``_first_sight_finished``) before the engine gives up
# and persists the best-known (scoreboard-only) snapshot. Held on a separate
# ``self._backfill`` tracker, never ``self._transitions`` (see
# ``CollectorEngine._backfill`` and ``_backfill_tracker_at_cap`` for why): a
# backfill retry has no "total attempts" distinct concept (every attempt is a
# detail-fetch attempt), so one cap suffices, mirroring the order of
# magnitude of ``_MAX_TRANSITION_DETAIL_FAILURES``.
_BACKFILL_MAX_ATTEMPTS = 3
# Per-poll cap on ``is_backfill`` detail fetches: a cold start against a
# slate with many already-finished matches (or a provider outage where every
# fetch times out) must not serialize an unbounded burst of
# ``fetch_match_detail`` calls inside one poll and block the daemon. Shared
# by both first-sight and pending-retry candidates (one poll-scoped budget;
# see ``_fetch_poll_snapshots``'s local ``backfill_fetches_this_poll``
# counter). Bounds worst-case per-poll fetch volume at the cost of a possibly
# multi-poll delay before a large finished slate is fully backfilled —
# acceptable, since backfill is best-effort enrichment, not the live-match
# critical path. Also shared with
# :meth:`CollectorEngine._hydrate_vanished_backfill_matches`'s vanished-match
# backfill pass, threaded in as the REMAINING budget left over from this
# poll's on-slate loop — see that method's docstring for why it is bounded.
_BACKFILL_MAX_FETCHES_PER_POLL = 5
# Abandonment COOLDOWN (see :class:`_Cooldown` and the ``_TransitionTracker``
# docstring): after a match is abandoned it may be re-tracked only once this
# many polls have elapsed, doubling per successive abandonment epoch (8, 16,
# 32 … capped) so a match whose apply persistently fails retries at
# exponentially spaced intervals instead of hot-looping a fresh give-up every
# poll. The cooldown clears ONLY on a genuinely durable apply for the match.
_COOLDOWN_BASE_POLLS = 8
_COOLDOWN_MAX_POLLS = 256
# Event types whose ``player``/``assist`` NormalizedEvent fields carry a
# scorer/assist rather than a generic participant, and therefore land under
# the ``scorer``/``assist`` payload keys instead of ``player``/``assist``.
# NOTE (deliberate boundary erosion): this is a sport-specific taxonomy
# literal living in the core engine. See ``_event_to_row``'s docstring for
# why it wasn't lifted into a pack-declared ``EventTypeDecl`` field.
_GOAL_FAMILY_EVENT_TYPES = frozenset({"goal", "own_goal"})


def _participant_key(event_type: str) -> str:
    """The payload key that carries the participant for ``event_type``.

    ``"scorer"`` for goal-family types (see ``_GOAL_FAMILY_EVENT_TYPES``),
    ``"player"`` otherwise. Shared by ``_event_to_row`` (write path) and
    ``_stored_events`` (read path) so the two stay in lockstep.
    """
    return "scorer" if event_type in _GOAL_FAMILY_EVENT_TYPES else "player"


def _event_to_row(event: NormalizedEvent) -> dict[str, Any]:
    """Project a :class:`NormalizedEvent` onto a writer ``events`` row.

    Mostly core-agnostic and lossless: ``seq``/``type``/``minute``/
    ``importance``/``detail`` map onto their columns; the sport-shaped
    ``team``/``player``/``assist`` ride in ``payload`` (``actor_entity``/
    ``target_entity`` remain reserved and unpopulated by this writer — see
    ``schema.sql``). For goal-family events (``goal``, ``own_goal``),
    ``player``/``assist`` land under the ``scorer``/``assist`` payload keys
    instead, since those fields carry a scorer/assist for that event type
    specifically; every other event type keeps the ``player``/``assist``
    keys. This one exception is a deliberate, visible boundary erosion: the
    ``goal``/``own_goal`` literals (``_GOAL_FAMILY_EVENT_TYPES``) are a
    sport-specific taxonomy check hardcoded into this otherwise sport-agnostic
    core function, rather than being declared per-pack via ``EventTypeDecl``
    (``packs/spec.py``). Revisit if a future pack needs a different
    participant-key convention. ``period`` is left NULL: ``NormalizedEvent``
    carries no authoritative period.
    """
    participant_key = _participant_key(event.event_type)
    payload = {
        key: value
        for key, value in (
            ("team", event.team),
            (participant_key, event.player),
            ("assist", event.assist),
        )
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
# the same monotonicity rules without hunting down every call site. Defined in
# :mod:`gamecollect.fallback_merge` (imported below as ``_is_terminal``) since
# the merge/comparison cluster that mostly consumes it lives there.


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
    construction, since violating any one of them corrupts the tracker):

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
      boolean latch**, since a boolean cannot satisfy the behaviour matrix
      below. When a give-up snapshot's apply never lands
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


def _backfill_tracker_at_cap(tracker: _TransitionTracker) -> bool:
    """True when a backfill retry tracker owes a give-up (pending or capped).

    Mirrors :func:`_tracker_at_cap`'s shape but against ``_BACKFILL_MAX_ATTEMPTS``
    and ``consecutive_failures`` only — a backfill tracker has no non-terminal
    "total attempts" concept distinct from a fetch failure (every attempt is a
    detail fetch that either succeeds or raises).
    """
    return tracker.give_up_pending or tracker.consecutive_failures >= _BACKFILL_MAX_ATTEMPTS


def _first_sight_finished(
    *,
    previous: NormalizedMatch | None,
    status: MatchStatus,
    was_live: bool,
    has_stored_row: bool,
) -> bool:
    """True on first sight of an already-finished (i.e. terminal) match.

    Four conjuncts: no in-memory baseline, a genuinely TERMINAL status (per
    :func:`_is_terminal`, not merely "not live" — a match must be checked
    against the full terminal-status set, not just excluded from the live
    set), never live this poll (neither via the baseline nor a stored row),
    and no existing stored row. Using ``_is_terminal`` instead of
    ``status not in LIVE_STATUSES`` matters: the latter also matches
    SCHEDULED/UNKNOWN, which would detail-fetch every upcoming/unknown
    fixture on a cold start — a scope leak beyond "`FINISHED` (or otherwise
    terminal)". Deliberately excludes any ``events`` conjunct: the provider
    ABC (``MatchDataProvider.fetch_live_matches``) explicitly permits a
    scoreboard-only snapshot to carry a partial event list, so a genuinely
    first-sight FINISHED match whose bare board already shows some events
    must still trigger the full detail fetch — treating it as "nothing to
    do" would persist the partial list directly and permanently foreclose
    hydration via ``has_stored_row``. Deliberately excludes any give-up/
    at-cap term — that is Phase 2's pre-fetch short-circuit, evaluated
    separately — and deliberately excludes the ``backfill_finished_matches``
    kill switch, which the caller ANDs in on top of this predicate.
    Likewise deliberately excludes any cooldown term: the
    caller ANDs in ``not self._cooling_down(...)`` on top of this
    predicate too, so an unpersistable give-up's abandonment cooldown
    (:meth:`CollectorEngine._emit_backfill_give_up`) can suppress
    re-detection without this pure function knowing about
    ``self._cooldowns`` at all.
    """
    return previous is None and _is_terminal(status) and not was_live and not has_stored_row


def _live_to_terminal_transition(was_live: bool, status: MatchStatus) -> bool:
    """True when a match was live and this poll's status is non-live.

    Structurally exclusive with :func:`_first_sight_finished`: the latter
    requires ``not was_live``, this requires ``was_live`` — both read
    ``was_live`` with opposite polarity by construction.
    """
    return was_live and status not in LIVE_STATUSES


def _rows_report_live(rows: list[tuple]) -> bool:
    """True when the best (authority-first, then freshest) stored row is live.

    Shared derivation for :meth:`CollectorEngine._stored_liveness_rows`
    readers, notably the restart-gap ``was_live``/``has_stored_row``
    computation in :meth:`CollectorEngine._fetch_poll_snapshots`, which also
    needs the raw ``rows`` for ``has_stored_row`` alongside the liveness
    bool. Empty ``rows`` (no stored row) is not live.
    """
    return bool(rows) and rows[0][0] in LIVE_STATUS_VALUES


# The remaining pure comparison/accumulation helpers
# (_live_supersedes_cooldown_fallback, _live_supersedes_captured,
# _fallback_worth_retaining, _live_supersedes_fallback, _accrue_fallback,
# _paired_clock, _max_known, _non_regressing_score) moved to
# gamecollect.fallback_merge and are imported above.


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
        backfill_finished_matches: bool = True,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self._pack = pack
        self._db_path = Path(db_path)
        self._source = source
        self._poll_interval = float(poll_interval)
        self._backfill_finished_matches = backfill_finished_matches
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
        # One tracker per in-flight first-sight-finished backfill retry, keyed
        # by provider-native match id — a container SEPARATE from
        # ``self._transitions`` (see ``_first_sight_finished``/Architecture
        # Decisions in the dev plan): ``self._transitions`` membership feeds
        # ``was_live`` and is read by ``_hydrate_vanished_matches``; parking a
        # backfill retry there would misroute a retrying first-sight match
        # onto the live→terminal transition path on its very next poll. Only
        # the give-up SHAPE and its emission cap are shared with
        # ``_TransitionTracker``'s pattern — NOT the cooldown side-effect, with
        # ONE exception: an ordinary backfill give-up landing durably IS
        # permanent (no cooldown needed, since ``has_stored_row`` forecloses
        # re-detection), but when the give-up snapshot's OWN apply never
        # lands within ``_MAX_GIVE_UP_EMISSIONS`` re-emissions,
        # ``_emit_backfill_give_up`` also enters a cooldown (mirroring
        # ``_abandon``) so detection cannot hot-loop every poll — see that
        # method's docstring. Popped ONLY by ``_resolve_backfill_tracker``
        # (consume-side, any non-live durable apply), by emission-cap
        # exhaustion in ``_emit_backfill_give_up``, or by
        # ``_hydrate_vanished_backfill_matches`` reaching its own cap/give-up
        # for an entry that dropped off the slate mid-retry — no OTHER
        # vanished-match purge beyond that dedicated pass (bounded, in-memory
        # only).
        self._backfill: dict[str, _TransitionTracker] = {}
        # Poll-scoped set of match ids whose backfill detail fetch SUCCEEDED
        # this poll (populated in ``_fetch_poll_snapshots``, cleared at the
        # top of every call so it never carries stale ids across polls).
        # ``poll_once`` consults it to tell apart a downstream ``_apply``
        # failure for an ``is_backfill`` match (which must still register a
        # backfill-tracker failure — see ``_register_backfill_apply_failure``)
        # from an ordinary match's ``_apply`` failure (unrelated to backfill
        # retry accounting).
        self._backfill_apply_pending: set[str] = set()
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
                # This _apply raised AFTER seed_match may already have
                # durably written the matches row (seed-before-child-write),
                # so a match this poll's fetch confirmed as ``is_backfill``
                # must still count this as a backfill attempt — otherwise a
                # row now exists with no tracker to keep is_backfill alive,
                # permanently foreclosing retry. Not reached for a
                # non-backfill match (absent from
                # ``self._backfill_apply_pending``).
                if diff.match.match_id in self._backfill_apply_pending:
                    self._register_backfill_apply_failure(diff.match)
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
                # Same rationale as the SequenceError branch above.
                if diff.match.match_id in self._backfill_apply_pending:
                    self._register_backfill_apply_failure(diff.match)
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
            elif diff.match.match_id in self._backfill_apply_pending:
                # seed_match returned None (missing identity) after a
                # successful detail fetch for an is_backfill match — no
                # durable write landed, so nothing was foreclosed, but without
                # registering a failure here nothing ever counts against
                # _BACKFILL_MAX_ATTEMPTS either, and _first_sight_finished
                # would keep re-firing (fetching detail again) every poll
                # forever.
                self._register_backfill_apply_failure(diff.match)

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
        ride in the JSON payload, under ``scorer``/``assist`` for goal-family
        events), so a reconstructed baseline compares equal to an unchanged
        incoming event under the diffing fingerprint.
        """
        rows = self._conn.execute(
            "SELECT seq, type, minute, importance, detail, payload FROM events "
            "WHERE source = ? AND match_id = ? ORDER BY seq",
            (self._source, seeded_id),
        ).fetchall()
        events: list[NormalizedEvent] = []
        for seq, event_type, minute, importance, detail, payload in rows:
            extras = json.loads(payload) if payload else {}
            participant_key = _participant_key(event_type)
            player = extras.get(participant_key)
            if player is None and event_type in _GOAL_FAMILY_EVENT_TYPES:
                # Fallback for rows written before this convention landed
                # (legacy ``payload.player`` on goal/own_goal rows): without
                # this, such a row would silently read back as
                # ``player=None`` on the seq-conflict reconciliation path.
                player = extras.get("player")
            events.append(
                NormalizedEvent(
                    seq=seq,
                    minute=minute,
                    event_type=event_type,
                    importance=importance,
                    team=extras.get("team"),
                    player=player,
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
        # Poll-scoped: cleared unconditionally before this poll populates it,
        # so a stale id from a prior poll (whose _apply already resolved one
        # way or another) can never be misread as "this poll's fetch
        # succeeded" by poll_once's failure-registration checks below.
        self._backfill_apply_pending.clear()
        matches = self._provider.fetch_live_matches()
        slate_ids = {m.match_id for m in matches}
        # Subset of ``slate_ids`` reporting a LIVE status THIS poll. Narrower
        # than ``slate_ids``: it exists only so
        # :meth:`_seed_restart_backfill_trackers` can distinguish "a
        # sibling id is genuinely live right now" (must not seed, would wedge
        # a live row) from "a mapped id merely reappeared on the slate,
        # non-live" (must still seed — see that method's docstring for why
        # its guard is narrower than :meth:`_seed_restart_trackers`'s).
        live_slate_ids = {m.match_id for m in matches if m.status in LIVE_STATUSES}
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
            # Sibling one-shot scan for the OTHER restart gap: a terminal
            # stored row whose backfill retry was tracked only in the
            # process-local
            # ``self._backfill`` dict. Gated on the kill switch — with
            # backfill disabled, ``is_backfill`` can never be True, so
            # seeding a tracker here would be inert bookkeeping only.
            if self._backfill_finished_matches:
                self._seed_restart_backfill_trackers(live_slate_ids)
            self._restart_scan_done = True
        hydrated: list[NormalizedMatch] = []
        # Poll-scoped budget: bounds how many ``is_backfill`` detail fetches
        # THIS poll may issue — see ``_BACKFILL_MAX_FETCHES_PER_POLL``'s
        # docstring for the rationale. A local, NOT an instance attribute: it
        # must not survive past this poll.
        backfill_fetches_this_poll = 0
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
            # Default-initialized ahead of the restart-gap guard below (not
            # derived from was_live) so it is never unbound/stale for a
            # baselined, non-live, never-live match that reaches the skip
            # condition further down (e.g. a repeatedly-polled SCHEDULED
            # fixture never enters the guard).
            has_stored_row = False
            if not was_live and previous is None and match.status not in LIVE_STATUSES:
                # No in-memory baseline (typically the first poll after a
                # restart): the transition may have happened while we were
                # down — the stored row still says live. Cheap: fires only
                # until the match is first baselined or tracker'd. Also
                # surfaces row-existence for the first-sight-finished
                # detection below, from this same lookup (no second call).
                rows = self._stored_liveness_rows(match.match_id)
                has_stored_row = bool(rows)
                was_live = _rows_report_live(rows)
            # ``has_stored_row`` alone cannot stand as "backfill already
            # attempted" — several paths (a partial scoreboard snapshot persisted
            # directly, a downstream ``_apply`` failure after ``seed_match``
            # already durably wrote the row, see ``_register_backfill_apply_failure``)
            # can durably write a ``matches`` row without ever completing full
            # detail hydration, which would permanently foreclose retry if
            # ``has_stored_row`` were the sole gate. ``pending_backfill_retry``
            # is the fix: once a match has an ACTIVE ``self._backfill``
            # tracker — created by ANY failed backfill attempt, whether a
            # ``fetch_match_detail`` exception, a downstream ``_apply``
            # exception, or ``seed_match`` returning ``None`` — it keeps
            # routing through ``is_backfill`` on every subsequent poll
            # regardless of what ``has_stored_row`` reads, because that flag
            # may now mean "a partial/failed attempt already wrote
            # something" rather than "backfill fully completed". Uses
            # ``_is_terminal`` (mirroring ``_first_sight_finished``'s own
            # term) rather than the wider ``status not in LIVE_STATUSES`` so
            # a tracked match that flips to
            # SCHEDULED/UNKNOWN does not force backfill routing either.
            # Gated additionally on ``not was_live`` so a pending-retry match
            # that reappears genuinely LIVE this poll still takes the
            # ordinary live path instead of being force-routed into
            # backfill-failure handling — preserves the
            # ``is_backfill``/``in_transition`` mutual exclusivity invariant
            # by construction.
            #
            # The status conjunct here is ``status not in LIVE_STATUSES`` —
            # NOT ``_is_terminal`` (unlike
            # ``_first_sight_finished``, whose OWN terminal-only conjunct is
            # unrelated and unchanged). This predicate's job is narrower than
            # first-sight detection: "keep an ALREADY-tracked match (an
            # ACTIVE ``self._backfill`` entry already exists) routing through
            # backfill regardless of what its current-poll status momentarily
            # reads." A prior version required ``_is_terminal`` here too; that
            # let a transient SCHEDULED/UNKNOWN provider status regression on
            # a match with a pending retry (the ABC does not guarantee status
            # monotonicity) flip ``pending_backfill_retry`` — and therefore
            # ``is_backfill`` overall — to ``False``. The match then fell into
            # the generic direct-persist skip branch below, baselining
            # ``self._last`` off a raw non-terminal snapshot and letting
            # :meth:`_resolve_backfill_tracker` (popped on EVERY durable
            # apply, unconditionally) silently drop the tracker — permanently
            # foreclosing re-detection via ``_first_sight_finished``'s
            # ``previous is None`` conjunct, even if the match later turned
            # FINISHED again. Widening this conjunct means a SCHEDULED/UNKNOWN
            # reset on a tracked match now still routes through
            # ``is_backfill`` and reaches the fetch below, where the new
            # ``elif is_backfill and ...`` branch (after the merge) handles an
            # ALSO-still-inconclusive detail without a durable persist.
            pending_backfill_retry = (
                self._backfill_finished_matches
                and match.match_id in self._backfill
                and match.status not in LIVE_STATUSES
                and not was_live
            )
            first_sight = _first_sight_finished(
                previous=previous,
                status=match.status,
                was_live=was_live,
                has_stored_row=has_stored_row,
            )
            # An unpersistable backfill give-up drops its
            # tracker AND enters a cooldown (:meth:`_emit_backfill_give_up`),
            # so on the very next poll ``pending_backfill_retry`` reads False
            # (tracker gone) and the ``first_sight``-driven half of
            # ``is_backfill`` below is ALSO gated off by the
            # ``not self._cooling_down`` conjunct — leaving ``is_backfill``
            # False while still cooling. Without this early defer, that
            # combination fell through to the ordinary skip-branch below,
            # which raw-persists a bare scoreboard snapshot (no detail
            # fetch): if identity has since become resolvable, ``seed_match``
            # can succeed, durably seeding a permanent zero-event row that
            # forecloses backfill forever. Placed before the skip-branch,
            # the fetch-budget/in_transition-at-cap checks, and the ordinary
            # fetch/try path so this poll is a pure no-op for the match — no
            # fetch, no persist, no tracker touched — deferred exactly like
            # the per-poll fetch-budget-exhaustion deferral below.
            backfill_cooling_deferred = (
                self._backfill_finished_matches
                and first_sight
                and self._cooling_down(match.match_id)
            )
            if backfill_cooling_deferred:
                continue
            is_backfill = self._backfill_finished_matches and (
                (
                    first_sight
                    # Caller-side gate, NOT folded into the pure predicate —
                    # its own docstring already says it "deliberately excludes
                    # any give-up/at-cap term... evaluated separately". An
                    # unpersistable backfill give-up (its OWN apply never
                    # durably lands) now pops the tracker AND enters a
                    # cooldown (:meth:`_emit_backfill_give_up`) instead of
                    # leaving the match free to re-fire from scratch next
                    # poll — without this gate that would be a full
                    # fetch/give-up cycle hot-looping every poll forever.
                    and not self._cooling_down(match.match_id)
                )
                or pending_backfill_retry
            )
            if match.status not in LIVE_STATUSES and not was_live and not is_backfill:
                hydrated.append(match)
                continue
            in_transition = _live_to_terminal_transition(was_live, match.status)
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
            backfill_tracker = self._backfill.get(match.match_id) if is_backfill else None
            if (
                is_backfill
                and backfill_tracker is not None
                and _backfill_tracker_at_cap(backfill_tracker)
            ):
                # A backfill give-up is already due (typically re-emitted
                # because its apply did not land last poll): skip the fetch,
                # retry the durable write instead of re-fetching only to fail
                # again.
                self._emit_backfill_give_up(hydrated, match.match_id, backfill_tracker, fresh=match)
                continue
            if is_backfill and backfill_fetches_this_poll >= _BACKFILL_MAX_FETCHES_PER_POLL:
                # The per-poll fetch budget is exhausted. One
                # shared budget covers both candidate shapes:
                if backfill_tracker is not None:
                    # Pending-retry candidate: DEFER without touching the
                    # failure count (a deferral, not a failure) — retried
                    # next poll via ``pending_backfill_retry``, budget
                    # permitting again.
                    continue
                # Fresh first-sight candidate the budget can't cover this
                # poll: mark it pending (get-or-create, ZERO failures — not
                # a failed attempt) but do NOT append it to ``hydrated`` this
                # poll. Appending the bare scoreboard snapshot here would
                # durably write it via ``_apply``'s ``seed_match`` and
                # reintroduce exactly the permanent-block bug this branch
                # exists to prevent (a durable row with no
                # tracker, foreclosing retry forever). It becomes eligible
                # again next poll via ``pending_backfill_retry``, budget
                # permitting again.
                self._backfill_tracker(match.match_id)
                continue
            if is_backfill:
                backfill_fetches_this_poll += 1
            try:
                detail = self._provider.fetch_match_detail(match.match_id)
            except (ProviderUnavailableError, ShapeDriftError) as exc:
                if is_backfill:
                    self._log_detail_fetch_failure(match.match_id, exc)
                    backfill_tracker = self._register_backfill_apply_failure(match)
                    if _backfill_tracker_at_cap(backfill_tracker):
                        self._emit_backfill_give_up(
                            hydrated, match.match_id, backfill_tracker, fresh=match
                        )
                    continue
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
            # NOTE: unlike ``tracker`` above, ``backfill_tracker.consecutive_failures``
            # is deliberately NOT reset to 0 here on fetch success. Before
            # findings #2/#3, a successful fetch always led to a durable
            # apply, which pops the tracker entirely via
            # ``_resolve_backfill_tracker`` regardless of this field's value
            # — so resetting it here was a no-op in practice. Now that the
            # downstream ``_apply`` can still legitimately fail after a
            # successful fetch (``_register_backfill_apply_failure``,
            # populated below via ``self._backfill_apply_pending``), zeroing
            # it here would erase that failure's contribution BEFORE
            # poll_once's except handler ever increments it, permanently
            # masking the cap (every poll: reset to 0, then +1 — never
            # reaching ``_BACKFILL_MAX_ATTEMPTS``).
            if is_backfill:
                # The fetch succeeded, but the downstream diff/_apply in
                # poll_once can still fail (an exception _apply itself does
                # not reconcile, or seed_match returning None). Mark this match so poll_once
                # can tell such a failure apart from an ordinary match's
                # _apply failure and still register it against the backfill
                # tracker (see _register_backfill_apply_failure), rather than
                # silently either foreclosing retry (a partial row was
                # already durably seeded) or retrying unboundedly (no row was
                # seeded, no tracker exists to bound it).
                self._backfill_apply_pending.add(match.match_id)
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
            elif (
                is_backfill
                and merged.status not in LIVE_STATUSES
                and not _is_terminal(merged.status)
            ):
                # The fetch succeeded but the detail is ALSO still
                # inconclusive — both
                # the scoreboard and the detail report SCHEDULED/UNKNOWN, the
                # provider itself has not resolved this match yet. Structurally
                # mutually exclusive with the ``in_transition`` branch above
                # (``is_backfill`` and ``in_transition`` are never both true
                # by construction — see the mutual-exclusivity invariant
                # documented throughout this file), so this ``elif`` can never
                # steal the ``in_transition`` branch's cases; the ordering
                # between them is immaterial. Do NOT fall through to the
                # unconditional ``hydrated.append(merged)`` below: persisting
                # a non-terminal snapshot here would durably apply it, and
                # :meth:`_resolve_backfill_tracker` pops the tracker on ANY
                # durable apply (not just terminal ones) — silently
                # abandoning the retry via a different path than the one
                # ``pending_backfill_retry``'s widened conjunct (above) just
                # closed. Instead, treat this as an inconclusive attempt that
                # counts toward the retry cap, exactly like a fetch exception
                # does (:meth:`_register_backfill_apply_failure` is shared
                # with that path).
                #
                # Accrue ``merged`` (scoreboard+detail), not the
                # bare ``match`` — a successful-but-inconclusive detail fetch
                # supplies richer data than the scoreboard alone, and passing
                # ``match`` here silently discarded it from the tracker's
                # fallback.
                backfill_tracker = self._register_backfill_apply_failure(merged)
                if _backfill_tracker_at_cap(backfill_tracker):
                    self._emit_backfill_give_up(
                        hydrated, match.match_id, backfill_tracker, fresh=merged
                    )
                continue
            hydrated.append(merged)
        hydrated.extend(
            self._hydrate_vanished_matches(
                slate_ids,
                remaining_backfill_budget=_BACKFILL_MAX_FETCHES_PER_POLL
                - backfill_fetches_this_poll,
            )
        )
        return hydrated, live_resumptions

    def _hydrate_vanished_matches(
        self, slate_ids: set[str], *, remaining_backfill_budget: int
    ) -> list[NormalizedMatch]:
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

        SECOND, BACKFILL-SPECIFIC PASS: a match with an ACTIVE
        ``self._backfill`` tracker (its
        first-sight-finished detail fetch failed at least once, retry
        pending) that then drops off the slate before its next poll is
        tracked the same way — off-slate and not cooling — but against
        ``self._backfill`` instead of ``self._last``/``self._transitions``,
        via the separate :meth:`_hydrate_vanished_backfill_matches` helper
        (own docstring has the details: base resolution, cap/give-up
        handling, and how it shares THIS poll's remaining
        ``_BACKFILL_MAX_FETCHES_PER_POLL`` budget via
        ``remaining_backfill_budget`` rather than being unbounded). The
        "Residual gap" paragraph above does not apply to it: this pass's ids
        are already-resolved provider ids sitting in ``self._backfill``'s
        keys, not stored ids needing map resolution — that only matters for
        the restart scan (:meth:`_seed_restart_backfill_trackers`). CRITICAL
        invariant, repeatedly documented elsewhere in this file: this second
        pass must NEVER create or touch a ``self._transitions`` entry, and
        the first pass above must NEVER create or touch a ``self._backfill``
        entry — ``is_backfill``/``in_transition`` mutual exclusivity by
        construction.
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
        if self._backfill_finished_matches:
            snapshots.extend(
                self._hydrate_vanished_backfill_matches(
                    slate_ids, remaining_backfill_budget=remaining_backfill_budget
                )
            )
        return snapshots

    def _hydrate_vanished_backfill_matches(
        self, slate_ids: set[str], *, remaining_backfill_budget: int
    ) -> list[NormalizedMatch]:
        """Second vanished-match pass, keyed to ``self._backfill``.

        Mirrors the transitions pass in :meth:`_hydrate_vanished_matches`'s
        shape but uses ONLY the backfill give-up/cap helpers
        (:meth:`_backfill_tracker`, :func:`_backfill_tracker_at_cap`,
        :meth:`_emit_backfill_give_up`) — never :meth:`_tracker`,
        :func:`_tracker_at_cap`, or :meth:`_emit_give_up` — preserving the
        ``is_backfill``/``in_transition`` mutual-exclusivity invariant this
        file repeatedly documents.

        Tracked set: every ``self._backfill`` id off the current slate and
        not cooling (the same give-up hot-loop guard the transitions
        pass already applies via ``_cooling_down``). A cooling id is skipped
        here for the identical reason: a match whose give-up snapshot could
        not be durably applied is re-tracked only after its cooldown gate
        lapses.

        On a fetch exception there is no shared helper equivalent to
        :meth:`_register_backfill_apply_failure` reached here — that helper
        accrues a SAME-POLL scoreboard snapshot into the fallback, and a
        vanished match has none this poll (it is, by definition, off the
        slate); the failure counter is bumped directly instead, mirroring
        exactly how the transitions pass above handles ITS OWN
        fetch-exception branch (no board to accrue there either).

        On a fetch success the tracker's ``consecutive_failures`` is
        deliberately NOT reset to 0 (mirrors the on-slate ``is_backfill``
        fetch-success comment in :meth:`_fetch_poll_snapshots` verbatim): the
        match id is marked in ``self._backfill_apply_pending`` so a
        downstream ``_apply`` failure in ``poll_once`` still registers
        against this tracker rather than resetting the counter here and
        erasing that later failure's contribution before it can count.

        Base resolution deliberately does NOT call :meth:`_transition_base`
        — that helper reads ``self._last`` then falls back to the STORED row
        via :meth:`_stored_match_snapshot`, both transitions-specific
        concepts. A backfill candidate reaching this method got into
        ``self._backfill`` only via ``_first_sight_finished`` (or a
        downstream-apply-failure registration for the same first-sight
        candidate), whose own conjuncts require ``previous is None`` (no
        in-memory baseline) and ``not has_stored_row`` (no stored row) at
        first detection — so the only admissible base is the tracker's own
        accrued ``fallback``; absent that, the fresh ``detail`` stands
        alone. Unlike the transitions pass, a successful non-terminal detail
        is still appended unconditionally (mirroring the on-slate
        ``is_backfill`` branch, which has no terminal-gating for backfill
        candidates either — that gating is an ``in_transition``-only
        concept, and the two are mutually exclusive by construction).

        BOUNDED: this pass shares THIS poll's
        REMAINING ``_BACKFILL_MAX_FETCHES_PER_POLL`` budget with the on-slate
        loop in :meth:`_fetch_poll_snapshots`, threaded in as
        ``remaining_backfill_budget`` (that budget minus
        ``backfill_fetches_this_poll`` at the call site). This matters
        because :meth:`_seed_restart_backfill_trackers` seeds regardless of
        slate presence, only skipping a LIVE sibling — a single restart can
        seed MANY backfill trackers for historical incomplete rows in one
        call, most immediately eligible for this pass in the very same
        poll (off today's slate), so this pass must share the per-poll cap
        rather than run unbounded. Once
        the local fetch count over this method's own loop reaches
        ``remaining_backfill_budget``, every subsequent id in ``sorted``
        order is ``continue``d without a fetch or a failure-count change —
        deferred, matching the main loop's deferral semantics for an
        already-tracked pending-retry candidate at the same cap
        (``_fetch_poll_snapshots``'s ``if is_backfill and
        backfill_fetches_this_poll >= _BACKFILL_MAX_FETCHES_PER_POLL``
        branch) — retried next poll via the ordinary
        ``pending_backfill_retry``/off-slate tracking, budget permitting
        again.
        """
        backfill_tracked = {
            match_id
            for match_id in self._backfill
            if match_id not in slate_ids and not self._cooling_down(match_id)
        }
        snapshots: list[NormalizedMatch] = []
        fetches_this_pass = 0
        for match_id in sorted(backfill_tracked):
            tracker = self._backfill_tracker(match_id)
            if _backfill_tracker_at_cap(tracker):
                # An at-cap give-up costs no fetch — check this BEFORE the
                # budget-exhaustion deferral (mirrors the on-slate loop's own
                # ordering in ``_fetch_poll_snapshots``), so an already-
                # exhausted off-slate tracker can still resolve even when
                # this poll's on-slate loop already spent the whole budget.
                self._emit_backfill_give_up(snapshots, match_id, tracker, fresh=None)
                continue
            if fetches_this_pass >= remaining_backfill_budget:
                # Budget exhausted: defer without touching the failure count
                # (a deferral, not a failure) — retried next poll once this
                # match is (still) off-slate and eligible again.
                continue
            try:
                detail = self._provider.fetch_match_detail(match_id)
            except (ProviderUnavailableError, ShapeDriftError) as exc:
                fetches_this_pass += 1
                self._log_detail_fetch_failure(match_id, exc)
                tracker.consecutive_failures += 1
                if _backfill_tracker_at_cap(tracker):
                    self._emit_backfill_give_up(snapshots, match_id, tracker, fresh=None)
                continue
            fetches_this_pass += 1
            self._backfill_apply_pending.add(match_id)
            base = tracker.fallback
            merged = _merge_over_base(base, detail) if base is not None else detail
            if merged.status not in LIVE_STATUSES and not _is_terminal(merged.status):
                # Mirrors the on-slate loop's inconclusive branch
                # (:meth:`_fetch_poll_snapshots`'s ``elif is_backfill and ...
                # not _is_terminal`` branch): a non-terminal merged snapshot
                # (SCHEDULED/UNKNOWN reset) must NOT be durably appended to
                # ``snapshots`` here — :meth:`_resolve_backfill_tracker` pops
                # the tracker on ANY durable apply, not just terminal ones,
                # so appending would permanently foreclose retry the moment
                # this off-slate candidate happens to read back as
                # inconclusive. Instead, treat this as an inconclusive
                # attempt that counts toward the retry cap, exactly like a
                # fetch exception does just above.
                backfill_tracker = self._register_backfill_apply_failure(merged)
                if _backfill_tracker_at_cap(backfill_tracker):
                    self._emit_backfill_give_up(snapshots, match_id, backfill_tracker, fresh=merged)
                continue
            snapshots.append(merged)
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
        give-up (the R2 carry-forward rule). Get-or-create is only reached OUTSIDE an active
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

    def _backfill_tracker(self, match_id: str) -> _TransitionTracker:
        """Get-or-create the backfill retry tracker for ``match_id``.

        A backfill cooldown entered at give-up exhaustion (see
        ``self._backfill``'s docstring in ``__init__`` and
        :meth:`_emit_backfill_give_up`) lapses (``remaining`` hits 0) before
        its ``_Cooldown`` entry is purged (:meth:`_age_cooldowns` only purges
        an UNSEEN entry after a much longer window) — so a fresh
        get-or-create here can land in that lapsed-but-unpurged window, same
        as :meth:`_tracker`. Mirror :meth:`_tracker`'s seeding so a richer
        partial-detail fallback accrued across prior failed attempts is not
        silently discarded by a bare fresh tracker.
        """
        tracker = self._backfill.get(match_id)
        if tracker is None:
            tracker = self._backfill[match_id] = _TransitionTracker()
            cooldown = self._cooldowns.get(match_id)
            if cooldown is not None and cooldown.fallback is not None:
                tracker.fallback = _accrue_fallback(tracker.fallback, cooldown.fallback)
        return tracker

    def _register_backfill_apply_failure(self, match: NormalizedMatch) -> _TransitionTracker:
        """Get-or-create ``match``'s backfill tracker and count one failure.

        Shared by three call sites:
        a ``fetch_match_detail`` exception for an ``is_backfill`` match (the
        original path), and two downstream-``_apply``-failure cases in
        ``poll_once`` for a match whose fetch this poll SUCCEEDED: an
        exception where ``_apply`` itself does not reconcile, or
        ``seed_match`` returning ``None``. Both of those cases would
        otherwise never create/advance a tracker: if ``seed_match`` already
        durably wrote the row this poll, ``self._backfill`` would stay empty,
        permanently foreclosing ``_first_sight_finished`` next
        poll with no tracker to keep ``is_backfill`` alive via
        ``pending_backfill_retry``; if no row was seeded, the retry would be
        unbounded, since ``_first_sight_finished`` keeps re-firing
        every poll with nothing counting against ``_BACKFILL_MAX_ATTEMPTS``.
        Registering the failure here, keyed by the same tracker/cap machinery
        the fetch-exception path already uses, closes both gaps: the next
        poll's pre-fetch at-cap check (or ``pending_backfill_retry``) takes
        over from here.
        """
        tracker = self._backfill_tracker(match.match_id)
        tracker.fallback = _accrue_fallback(tracker.fallback, match)
        tracker.consecutive_failures += 1
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

        Backfill tracker resolution runs FIRST and unconditionally — before the
        live/non-live branch below and independent of ``state_advanced``:
        ``self._backfill`` membership is never read by the live/cooldown
        machinery above, so popping it here on every durable apply (a
        no-change apply included, since the stored row already durably
        matches) is safe and required. Without this, a backfill-tracked match
        whose merged detail resolves LIVE (a provider correcting a stale
        terminal/scheduled read) would leak its ``self._backfill`` entry
        forever: neither other documented pop path (the non-live consume-side
        resolve in :meth:`_resolve_tracker`, or emission-cap exhaustion in
        :meth:`_emit_backfill_give_up`) would ever fire for it.
        """
        self._resolve_backfill_tracker(match_id, snapshot)
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
            or _live_supersedes_fallback(applied, fallback)
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

    def _emit_backfill_give_up(
        self,
        out: list[NormalizedMatch],
        match_id: str,
        tracker: _TransitionTracker,
        fresh: NormalizedMatch | None,
    ) -> None:
        """Queue a backfill give-up snapshot — shape shared, cooldown bypassed.

        Shares :meth:`_emit_give_up`'s give-up SHAPE (best-known,
        merged-never-raw snapshot via :meth:`_build_give_up_snapshot`,
        ``give_up_pending`` set so a non-landing write re-emits next poll
        instead of losing the captured state) but does NOT call
        :meth:`_abandon` directly: unlike the transition give-up, a backfill
        give-up landing durably IS permanent (``_first_sight_finished``'s
        ``has_stored_row`` term goes ``False`` → permanently foreclosed once
        the row exists), so a LANDED write needs no cooldown. But in either
        branch below where the give-up's OWN apply cannot be relied on to
        land — no snapshot could even be built, or every emission has been
        exhausted without a durable write — the ``self._backfill`` entry is
        dropped AND a cooldown is entered (:meth:`_enter_cooldown`, mirroring
        :meth:`_abandon`'s own call), carrying the best-known state forward:
        without it, ``_first_sight_finished`` would re-fire from scratch on
        the very next poll — a full fetch/give-up cycle hot-looping forever.
        The cooldown bounds that: detection is gated on ``not
        self._cooling_down(...)`` at the call site (see the ``is_backfill``
        computation in :meth:`_fetch_poll_snapshots`), so re-detection is
        loud and bounded, but not truly permanent, once the cooldown lapses.
        This is pop path 2 of 2 (the other is
        :meth:`_resolve_backfill_tracker`, consume-side, on a durable apply
        landing).

        Passes ``force_terminal=False`` to :meth:`_build_give_up_snapshot`:
        the broad ``is_backfill`` detection also covers a first-sight
        SCHEDULED/UNKNOWN match (see ``_first_sight_finished``), so a backfill
        give-up candidate is not guaranteed to be terminal. Force-coercing it
        to FINISHED would fabricate a durable "FINISHED 0-0" row for a match
        that never kicked off — the dev plan's give-up contract is to persist
        the best-known *actual* scoreboard-only snapshot, not a forced close.
        """
        snapshot = self._build_give_up_snapshot(match_id, tracker, fresh, force_terminal=False)
        if snapshot is None:
            log.error(
                "giving up on backfill detail hydration for match %s on "
                "source %s after %d consecutive fetch failures: no baseline, "
                "fallback, or stored row is known: nothing to persist",
                match_id,
                self._source,
                tracker.consecutive_failures,
            )
            self._backfill.pop(match_id, None)
            self._enter_cooldown(match_id, fallback=tracker.fallback)
            return
        tracker.give_up_emissions += 1
        if tracker.give_up_emissions > _MAX_GIVE_UP_EMISSIONS:
            log.error(
                "could not persist backfill terminal state for match %s on "
                "source %s: the give-up snapshot's apply never landed after "
                "%d emissions — abandoning backfill tracker and entering a "
                "cooldown so detection cannot hot-loop every poll",
                match_id,
                self._source,
                _MAX_GIVE_UP_EMISSIONS,
            )
            self._backfill.pop(match_id, None)
            # An unpersistable give-up must not restart the full
            # first-sight-finished cycle on the very next poll (the row never
            # became durable, so ``has_stored_row`` cannot foreclose
            # re-detection). Enter the same capped-exponential cooldown
            # ``_abandon`` uses for the transition tracker, carrying the
            # best-known state forward; the caller-side
            # ``not self._cooling_down(...)`` gate on ``_first_sight_finished``
            # (see ``_fetch_poll_snapshots``) bounds the hot loop until the
            # cooldown lapses.
            self._enter_cooldown(match_id, fallback=tracker.fallback)
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
        self,
        match_id: str,
        tracker: _TransitionTracker,
        fresh: NormalizedMatch | None,
        *,
        force_terminal: bool = True,
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
        return self._nonregress_over_stored(match_id, merged, stored, force_terminal=force_terminal)

    def _nonregress_over_stored(
        self,
        match_id: str,
        snap: NormalizedMatch,
        stored: NormalizedMatch | None = _UNSET,
        *,
        force_terminal: bool = True,
    ) -> NormalizedMatch:
        """Give-up decision table: emit a monotonic, non-regressing close.

        The single emission choke point (shared, per design, between the
        transition and backfill give-up paths — see :meth:`_emit_give_up` /
        :meth:`_emit_backfill_give_up`). Combines the candidate ``snap`` with
        the durably-stored row per the prescribed table:

        G0 TERMINALITY (``force_terminal=True``, the transition give-up
           default — a give-up MUST be terminal there so the non-live consume
           point :meth:`_resolve_tracker` can resolve it): if the candidate's
           status is not terminal it is coerced to FINISHED (the accepted loud
           force-close) AND its minute/display_clock are NULLed — a coerced close
           has no authoritative clock (mirrors the detail-merge doctrine that
           clocks are legitimately null at FT), so it can never ship a stale live
           clock like "80'". A genuinely-terminal candidate keeps its own
           status and clock. **Backfill give-up passes ``force_terminal=False``**:
           a first-sight-finished candidate is not necessarily terminal (the
           broad ``is_backfill`` detection also covers a first-sight SCHEDULED
           match whose detail fetch never lands — see ``_first_sight_finished``),
           and the dev plan's give-up contract is to persist the best-known
           *actual* scoreboard-only snapshot, not to force-close a match that
           never kicked off into a fabricated "FINISHED 0-0" row. With
           ``force_terminal=False`` the candidate's own status/minute/clock are
           always kept (never coerced), matching the existing non-coerced
           branch below.
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
        coerced = force_terminal and not _is_terminal(snap.status)
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
            if coerced or not _is_terminal(snap.status):
                # G2: a stored terminal status is never regressed to
                # non-terminal — whether our own candidate was force-coerced
                # (``coerced``) or is simply non-terminal on its own (e.g. a
                # ``force_terminal=False`` backfill give-up whose candidate is
                # still SCHEDULED/UNKNOWN); a genuinely terminal candidate
                # still wins per G2's other half (this branch is not taken).
                status = stored.status
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
        """CONSUME point: pop the terminal tracker after a DURABLE non-live apply.

        Called only from :meth:`_on_durable_snapshot`'s non-live tail — the
        live-status case never reaches here (it returns earlier). A pending
        give-up logs its ERROR here — persistence is claimed only after the
        write actually landed. A durable non-live apply REMOVES the cooldown
        entry entirely (the terminal resolution is one of the only two
        entry-removal points, alongside the expired-unseen purge): the row has
        resolved terminally, so the epoch/backoff memory is moot and must not
        linger and block later re-tracking.

        Backfill tracker resolution does NOT happen here: it is resolved by
        :meth:`_on_durable_snapshot` itself, unconditionally, before branching
        on live status — see that method's docstring for why it must not be
        gated behind this (non-live-only) method.
        """
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

    def _resolve_backfill_tracker(self, match_id: str, snapshot: NormalizedMatch) -> None:
        """CONSUME point: pop a resolved backfill tracker on any durable apply.

        Pop path 1 of 2 (the other is emission-cap exhaustion in
        :meth:`_emit_backfill_give_up`). Called from
        :meth:`_on_durable_snapshot` FIRST, unconditionally, for EVERY durable
        apply (live or not, ``state_advanced`` or not — including a no-change
        apply, since the stored row already durably matches the snapshot),
        covering an ordinary retry-then-succeed apply (``give_up_pending``
        never set), a give-up apply landing, AND the case of a merged detail
        resolving LIVE for a backfill-tracked match (e.g. a provider
        correcting a stale terminal/scheduled read) — ``self._backfill`` must
        never leak any of these ways. No cooldown involvement: backfill has
        none.
        """
        tracker = self._backfill.pop(match_id, None)
        if tracker is None or not tracker.give_up_pending:
            return
        log.error(
            "giving up on backfill detail hydration for match %s on source %s "
            "after %d consecutive fetch failures: persisted best-known "
            "terminal state (status %s, score %s-%s) — final event list may "
            "be missing",
            match_id,
            self._source,
            tracker.consecutive_failures,
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
    # Narrow shape for the :meth:`_stored_liveness_rows` hot path: no payload
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
        (:meth:`_stored_match_snapshot`, :meth:`_stored_liveness_rows`) read
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

    def _stored_liveness_rows(self, provider_match_id: str) -> list[tuple]:
        """Return the stored liveness-check candidate rows for this match.

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

        Returned rows are ordered best (authority-first, then freshest)
        first — consistent with :meth:`_stored_match_snapshot`, which merges
        from that same row. An empty list means no stored row exists at all,
        which the caller uses as its own signal (e.g. first-sight-finished
        detection) alongside liveness, from this one lookup.
        """
        return self._stored_rows_for_provider(
            provider_match_id, columns=self._STORED_LIVENESS_COLUMNS
        )

    def _stored_status_is_live(self, provider_match_id: str) -> bool:
        """True when the AUTHORITATIVE stored ``matches`` row for this match is live.

        Evaluates ONLY the best (authority-first, then freshest) row's status
        from :meth:`_stored_liveness_rows` — consistent with
        :meth:`_stored_match_snapshot`, which merges from that same row.
        Scanning "is ANY candidate row live" would let an older live stub
        override a fresher non-live canonical row, wrongly classifying a
        restart-gap match as a live→terminal transition when the canonical
        row already recorded the terminal state; freshest-first would
        inversely let a fresher non-live stub mask a still-live canonical row.
        """
        rows = self._stored_liveness_rows(provider_match_id)
        return _rows_report_live(rows)

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

    def _seed_restart_backfill_trackers(self, live_slate_ids: set[str]) -> None:
        """One-time restart scan: seed backfill-retry trackers for incomplete rows.

        The sibling of :meth:`_seed_restart_trackers`, for the OTHER restart
        gap: a first-sight-
        finished match whose ``seed_match`` durably wrote the ``matches`` row
        (seed-before-child-write) while its detail-hydration retry was
        tracked only in the process-local ``self._backfill`` dict. A restart
        loses that tracker — ``has_stored_row`` then reads ``True`` forever
        (the row exists), so ``_first_sight_finished`` never re-fires, and
        ``pending_backfill_retry`` never fires either (no tracker survives
        the restart to keep it alive) — the match is permanently stuck with
        its partial/missing event list, invisible to every hydration path.

        Scans ``matches`` rows under this source with a TERMINAL stored
        status (per :func:`_is_terminal`, PARSED IN PYTHON from each row —
        not a SQL ``IN`` list, so an unrecognized/legacy status string is
        simply skipped rather than accidentally matched) that have ZERO rows
        in ``events`` for that same stored id (a correlated ``LEFT JOIN ...
        IS NULL``, keyed exactly like :meth:`_stored_events` reads events —
        by the row's own stored id, source-scoped).

        Reuses :meth:`_resolve_stored_provider_id` VERBATIM, but its SLATE
        guard is deliberately NARROWER than :meth:`_seed_restart_trackers`'s
        sibling guard — the caller passes
        ``live_slate_ids`` (only ids reporting a LIVE status THIS poll), not
        the full slate. ``_seed_restart_trackers``'s "skip if any mapped id
        is on the slate AT ALL" rationale is valid for the transitions scan:
        a live match on the slate is naturally re-detected via the ordinary
        ``was_live``/``_live_to_terminal_transition`` machinery, which does
        not depend on ``has_stored_row``. It is INVALID here: ``has_stored_row``
        reads ``True`` regardless of slate presence (the row exists either
        way), and with no tracker seeded, ``pending_backfill_retry``'s first
        conjunct (``match.match_id in self._backfill``) is ``False`` too — so
        an incomplete terminal row whose match happens to reappear on the
        current slate, even still reporting the SAME terminal/non-live
        status, would fall straight into the generic direct-persist skip
        branch on this very first restart poll and permanently lose its
        events. Narrowing to "skip
        only if a mapped id is on the slate reporting LIVE right now"
        preserves the genuine multi-provider safety concern
        :meth:`_resolve_stored_provider_id`'s docstring documents (a sibling
        id that is actually live must not get wedged by seeding a tracker
        keyed to a different, stale, non-live id) while still seeding for the
        non-live-reappearance case:

        * a row whose provider id cannot be resolved is skipped (WARNING
          already logged inside :meth:`_resolve_stored_provider_id`);
        * a row ANY of whose mapped provider ids is reporting LIVE on the
          slate THIS poll, or already tracked in ``self._backfill``, is
          skipped — it is either being collected normally under a live
          sibling id or already retrying.

        ACCEPTED FALSE POSITIVE (same tone as the "Residual gap" paragraph in
        :meth:`_hydrate_vanished_matches`'s docstring): a FINISHED match that
        genuinely has zero events (e.g. a scoreless match with no card/goal
        events) is indistinguishable from a partially-failed backfill by
        this heuristic. Restart will harmlessly seed a pending-retry tracker
        for it and, if it ever reappears on the slate, re-attempt one
        redundant ``fetch_match_detail`` — idempotent, since
        ``seed_match``/``append_events`` no-op when there is nothing new to
        write.

        ACCEPTED, KNOWN GAP (do NOT
        "fix" this without a deliberate, separately-considered schema/payload
        change): this heuristic also cannot distinguish a genuinely
        interrupted backfill from a match that already went through
        :meth:`_emit_backfill_give_up`'s bounded exhaustion path and had a
        scoreboard-only (zero-event) terminal snapshot durably persisted as
        its ACCEPTED final state. A later restart will re-seed a tracker for
        that already-abandoned match and repeat one doomed fetch/fail/give-up
        cycle. Properly closing this gap needs a durable marker
        distinguishing "interrupted" from "already gave up" (e.g. a new
        column or a new engine-reserved ``matches.payload`` key) — a
        larger, separately-considered change intentionally left out of scope
        here. What bounds the harm instead: the shared per-poll
        fetch budget (threaded into :meth:`_hydrate_vanished_backfill_matches`
        too) means this redundant retry is now bounded per poll, not an
        unbounded burst — the practical cost is a bounded, occasional,
        wasted retry cycle after a restart, not repeated outage-scale load.
        """
        rows = self._conn.execute(
            "SELECT matches.match_id, matches.status FROM matches "
            "LEFT JOIN events ON events.source = matches.source "
            "AND events.match_id = matches.match_id "
            "WHERE matches.source = ? AND events.match_id IS NULL",
            (self._source,),
        ).fetchall()
        for stored_id, status_value in rows:
            try:
                status = MatchStatus(status_value)
            except ValueError:
                continue
            if not _is_terminal(status):
                continue
            provider_id, mapped_ids = self._resolve_stored_provider_id(stored_id)
            if provider_id is None or mapped_ids & live_slate_ids:
                continue
            if provider_id in self._backfill:
                continue
            log.warning(
                "match %s (stored id %s) on source %s has a terminal stored "
                "row with zero stored events after a restart: seeding "
                "backfill-retry tracking in case its detail hydration was "
                "interrupted before this shutdown",
                provider_id,
                stored_id,
                self._source,
            )
            self._backfill_tracker(provider_id)

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
