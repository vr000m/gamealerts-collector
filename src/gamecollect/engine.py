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

from gamecollect.db.connection import connect
from gamecollect.db.writer import (
    CrossPartitionError,
    PartitionWriter,
    SequenceError,
    TaxonomyError,
    UnseededMatchError,
)
from gamecollect.diffing import MatchDiff, diff_matches
from gamecollect.fixture_io import fixture_stem, write_fixture
from gamecollect.packs.spec import SportPack
from gamecollect.provider import (
    LIVE_STATUS_VALUES,
    LIVE_STATUSES,
    MatchDataProvider,
    MatchStatus,
    NormalizedEvent,
    NormalizedMatch,
    ProviderUnavailableError,
    ShapeDriftError,
)

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


def _merge_detail(scoreboard: NormalizedMatch, detail: NormalizedMatch) -> NormalizedMatch:
    """Merge a detail snapshot over its scoreboard snapshot, field by field.

    Payload keys merge with detail winning; identity fields and scores take
    the detail value unless it is ``None``, in which case the scoreboard value
    fills it — a detail endpoint omitting ``kickoff_utc``/``home_team``/
    ``away_team`` must not wipe stored identity to NULL (or make ``seed_match``
    return ``None`` and drop the poll's events), and a detail omitting
    ``score_home``/``score_away`` must not null a known score (scores are
    cumulative facts; only ``minute``/``display_clock`` are legitimately
    nulled at HT/FT, so those stay detail-wins including ``None``).

    Status is forward-only (see ``_STATUS_RANK``): when the detail's status
    lags BEHIND the scoreboard's, the scoreboard's status wins, and so do its
    ``minute``/``display_clock`` (a lagging detail's clock is stale live state
    by definition) and its non-``None`` scores. The detail's events/payload
    are still taken — that is the whole point of hydrating the transition poll.
    """
    payload = dict(scoreboard.payload)
    payload.update(detail.payload)
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
    """
    payload = dict(base.payload)
    payload.update(snapshot.payload)
    fills = {
        name: getattr(base, name) for name in _MERGE_FILL_FIELDS if getattr(snapshot, name) is None
    }
    return replace(snapshot, payload=payload, **fills)


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
    * ``give_up_pending`` short-circuits further detail fetches: the give-up
      snapshot is re-emitted every poll until its apply lands, and the ERROR
      log stating what was persisted fires only after that durable success.
    * ``last_reported_status`` dedupes the non-terminal WARN: once per detail
      status change, not once per poll.
    """

    fallback: NormalizedMatch | None = None
    consecutive_failures: int = 0
    total_attempts: int = 0
    give_up_pending: bool = False
    last_reported_status: MatchStatus | None = None


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
        self._record_path = Path(record_path) if record_path is not None else None
        if self._record_path is not None:
            # Fail fast on an unusable --record target BEFORE any polling: a
            # bad path discovered only at shutdown would lose the whole
            # recorded session (close() flushes at most once).
            self._validate_record_target(self._record_path)
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
        self._record: dict[str, _RecordedMatch] = {}
        self._record_flush_failed = False
        self._backoff_multiplier = 1

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
        matches = self._fetch_poll_snapshots()
        if self._record_path is not None:
            self._accumulate_record(matches)
        for diff in diff_matches(matches, self._last):
            if not diff.has_changes:
                # Stored state already durably matches this snapshot; a
                # pending transition tracker for a non-live snapshot is
                # therefore resolved (its terminal state IS persisted).
                self._resolve_tracker(diff.match.match_id, diff.match)
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
                # CONSUME point of the transition state machine: only a
                # DURABLE apply of a non-live snapshot pops the tracker. An
                # apply failure above (baseline None / exception) retains it,
                # so the captured terminal state is retried next poll.
                self._resolve_tracker(diff.match.match_id, baseline)

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

    def _fetch_poll_snapshots(self) -> list[NormalizedMatch]:
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
        hydrated: list[NormalizedMatch] = []
        for match in matches:
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
            tracker = self._transitions.get(match.match_id)
            if in_transition and tracker is not None and self._tracker_at_cap(tracker):
                # A give-up is already due (typically re-emitted because its
                # apply did not land last poll): skip the fetch, retry the
                # durable write.
                self._emit_give_up(hydrated, match.match_id, tracker, fresh=match)
                continue
            try:
                detail = self._provider.fetch_match_detail(match.match_id)
            except (ProviderUnavailableError, ShapeDriftError) as exc:
                if isinstance(exc, ShapeDriftError):
                    log.error(
                        "detail shape drift for match %s on source %s: %s",
                        match.match_id,
                        self._source,
                        exc,
                        exc_info=True,
                    )
                else:
                    log.warning(
                        "detail fetch unavailable for match %s on source %s: %s",
                        match.match_id,
                        self._source,
                        exc,
                    )
                if not in_transition:
                    # Live match, per-match isolation: fall back to the
                    # scoreboard snapshot; the others proceed.
                    hydrated.append(match)
                    continue
                tracker = self._tracker(match.match_id)
                tracker.fallback = self._richest_fallback(tracker.fallback, match)
                tracker.consecutive_failures += 1
                tracker.total_attempts += 1
                if self._tracker_at_cap(tracker):
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
            if in_transition and merged.status is not MatchStatus.FINISHED:
                # The transition hydration succeeded but did NOT yield a
                # terminal snapshot: either the detail still says live over a
                # non-live board, or both report a SCHEDULED/UNKNOWN provider
                # reset. Count the attempt (bounded by the total cap) and keep
                # the richest fallback either way; persist live progress, but
                # never a non-live non-terminal reset.
                tracker = self._tracker(match.match_id)
                tracker.fallback = self._richest_fallback(tracker.fallback, match)
                tracker.total_attempts += 1
                self._warn_nonterminal(match.match_id, tracker, merged.status)
                if self._tracker_at_cap(tracker):
                    self._emit_give_up(hydrated, match.match_id, tracker, fresh=merged)
                    continue
                if merged.status in LIVE_STATUSES:
                    hydrated.append(merged)
                continue
            hydrated.append(merged)
        hydrated.extend(self._hydrate_vanished_matches(slate_ids))
        return hydrated

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
        only after the apply succeeds. Known limitation: after a daemon
        restart ``self._last`` and the trackers are empty, so a match that
        vanished across the restart (never reappearing on the slate) is not
        tracked — same as pre-fix.
        """
        tracked = {
            match_id
            for match_id, previous in self._last.items()
            if match_id not in slate_ids and previous.status in LIVE_STATUSES
        }
        # A pending tracker also marks an unresolved transition (restart-gap
        # or mid-retry): never dropped by slate absence alone.
        tracked.update(m for m in self._transitions if m not in slate_ids)
        snapshots: list[NormalizedMatch] = []
        for match_id in sorted(tracked):
            tracker = self._tracker(match_id)
            if self._tracker_at_cap(tracker):
                self._emit_give_up(snapshots, match_id, tracker, fresh=None)
                continue
            try:
                detail = self._provider.fetch_match_detail(match_id)
            except (ProviderUnavailableError, ShapeDriftError) as exc:
                tracker.consecutive_failures += 1
                tracker.total_attempts += 1
                if self._tracker_at_cap(tracker):
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
            if detail.status is MatchStatus.FINISHED:
                # Terminal from the detail itself: persist the merge; the
                # tracker pops only after the apply lands (_resolve_tracker).
                snapshots.append(merged)
                continue
            tracker.total_attempts += 1
            self._warn_nonterminal(match_id, tracker, detail.status)
            if self._tracker_at_cap(tracker):
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
        """Get-or-create the transition tracker for ``match_id``."""
        tracker = self._transitions.get(match_id)
        if tracker is None:
            tracker = self._transitions[match_id] = _TransitionTracker()
        return tracker

    @staticmethod
    def _tracker_at_cap(tracker: _TransitionTracker) -> bool:
        """True when the tracker owes a give-up (pending or a cap reached)."""
        return (
            tracker.give_up_pending
            or tracker.consecutive_failures >= _MAX_TRANSITION_DETAIL_FAILURES
            or tracker.total_attempts >= _MAX_TRANSITION_TOTAL_ATTEMPTS
        )

    @staticmethod
    def _richest_fallback(old: NormalizedMatch | None, new: NormalizedMatch) -> NormalizedMatch:
        """Keep the RICHEST non-live scoreboard snapshot as the fallback.

        A fallback that carries events, known scores, or a terminal status
        must not be overwritten by a sparser one (e.g. a later SCHEDULED
        reset board with NULL scores); ties prefer the fresher snapshot.
        """
        if old is None:
            return new

        def rank(m: NormalizedMatch) -> tuple[bool, bool, bool]:
            return (
                bool(m.events),
                m.score_home is not None or m.score_away is not None,
                m.status is MatchStatus.FINISHED,
            )

        return new if rank(new) >= rank(old) else old

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
        all is known to persist the tracker is dropped with an ERROR (there
        is no write that could ever succeed).
        """
        snapshot = self._build_give_up_snapshot(match_id, tracker, fresh)
        if snapshot is None:
            log.error(
                "giving up on terminal detail hydration for match %s on source %s "
                "after %d consecutive fetch failures / %d total attempts, and no "
                "baseline, fallback, or stored row is known: nothing to persist",
                match_id,
                self._source,
                tracker.consecutive_failures,
                tracker.total_attempts,
            )
            self._transitions.pop(match_id, None)
            return
        tracker.give_up_pending = True
        out.append(snapshot)

    def _build_give_up_snapshot(
        self, match_id: str, tracker: _TransitionTracker, fresh: NormalizedMatch | None
    ) -> NormalizedMatch | None:
        """Best-known terminal snapshot at give-up — merged, never raw.

        Candidate order: a fresh same-poll TERMINAL scoreboard snapshot, else
        the tracker's fallback (its non-live status is accepted as-is: at
        give-up a provider status reset is termination-worthy), else whatever
        fresh non-terminal snapshot this poll produced, else the in-memory
        baseline, else the stored row — the latter three force-closed to
        FINISHED when still live. The candidate is merged over the best base
        (baseline → fallback → stored row) so known identity and scores are
        never NULLed by a sparse snapshot.
        """
        baseline = self._last.get(match_id)
        stored: NormalizedMatch | None = None
        if fresh is not None and fresh.status is MatchStatus.FINISHED:
            snap = fresh
        elif tracker.fallback is not None:
            snap = tracker.fallback
        elif fresh is not None:
            snap = fresh
        elif baseline is not None:
            snap = baseline
        else:
            stored = self._stored_match_snapshot(match_id)
            snap = stored
        if snap is None:
            return None
        if snap.status in LIVE_STATUSES:
            snap = replace(snap, status=MatchStatus.FINISHED)
        base = next(
            (c for c in (baseline, tracker.fallback) if c is not None and c is not snap),
            None,
        )
        if base is None and stored is None:
            base = self._stored_match_snapshot(match_id)
        return _merge_over_base(base, snap) if base is not None else snap

    def _resolve_tracker(self, match_id: str, snapshot: NormalizedMatch) -> None:
        """CONSUME point: pop the tracker after a DURABLE non-live apply.

        Called from ``poll_once`` only once ``_apply`` returned a baseline
        (or the snapshot produced no diff because stored state already
        matches). A pending give-up logs its ERROR here — persistence is
        claimed only after the write actually landed.
        """
        if snapshot.status in LIVE_STATUSES:
            return
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

    def _stored_match_snapshot(self, provider_match_id: str) -> NormalizedMatch | None:
        """Rebuild a merge base from the stored ``matches`` row, if any.

        Used when neither an in-memory baseline nor a captured fallback
        exists (a restart gap): the stored row supplies identity
        (``home_team``/``away_team`` ride in the payload JSON per the
        ``default_seed_match`` convention, when present) and last-known
        scores so a sparse terminal snapshot does not seed identity-less or
        NULL known scores. Looked up under both id forms core knows, like
        :meth:`_stored_status_is_live`.
        """
        row = self._conn.execute(
            "SELECT status, minute, score_home, score_away, display_clock, "
            "kickoff_utc, payload FROM matches WHERE source = ? AND match_id IN (?, ?)",
            (
                self._source,
                provider_match_id,
                f"{self._source}:{provider_match_id}",
            ),
        ).fetchone()
        if row is None:
            return None
        status_value, minute, score_home, score_away, display_clock, kickoff_utc, payload = row
        extras: dict[str, Any] = {}
        if payload:
            try:
                decoded = json.loads(payload)
            except (TypeError, ValueError):
                decoded = None
            if isinstance(decoded, dict):
                extras = decoded
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
        """True when the stored ``matches`` row for this match is still live.

        Consulted only for a non-live slate match with no in-memory diff
        baseline (a restart gap). The row is looked up under this source by
        BOTH id forms core can know about: the provider-native id (the
        ``default_seed_match`` shape) and the source-qualified
        ``f"{source}:{id}"`` unreconciled form (the reader-contract shape
        packs write for unreconciled live matches). A pack that reconciles
        provider ids to canonical ids it alone can map is out of core's
        reach — such matches fall back to no hydration, same as pre-fix.
        """
        rows = self._conn.execute(
            "SELECT status FROM matches WHERE source = ? AND match_id IN (?, ?)",
            (
                self._source,
                provider_match_id,
                f"{self._source}:{provider_match_id}",
            ),
        ).fetchall()
        return any(status in LIVE_STATUS_VALUES for (status,) in rows)

    def stop(self) -> None:
        """Signal the loop to finish its current iteration and shut down."""
        self._stop_event.set()

    def close(self) -> None:
        """Flush any recorded fixture and close the database connection (idempotent).

        The flush is attempted at most once (a failure sets
        ``_record_flush_failed`` so a second ``close()`` does not re-raise) and
        the connection is closed unconditionally in a ``finally`` — a
        ``write_fixture`` failure must not leak the WAL connection, and the CLI's
        redundant second ``close()`` must be a no-op that re-raises nothing.
        ``_record`` is cleared only AFTER a successful flush: a failed flush
        preserves the accumulated session in memory instead of destroying it.
        """
        try:
            if self._record_path is not None and self._record and not self._record_flush_failed:
                try:
                    self._flush_record(self._record)
                except BaseException:
                    self._record_flush_failed = True
                    raise
                self._record = {}
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
    # Recording (--record)
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_record_target(target: Path) -> None:
        """Reject an unusable ``--record`` target at construction time.

        A non-``.json`` path is treated as a directory by ``_flush_record``;
        if it already exists as a regular file, every fixture write would fail
        at shutdown — after a whole session was collected. A ``.json`` path
        that exists as a directory is equally unwritable. The same applies to
        any *existing* component along the target's ancestry (e.g.
        ``out/session.json`` where ``out`` is an existing regular file):
        ``write_fixture``'s ``mkdir(parents=True)`` would raise at ``close()``.
        All fail fast here, before any polling starts.
        """
        if target.suffix == ".json":
            if target.is_dir():
                raise ValueError(
                    f"--record path {target} names a .json fixture file but is an "
                    f"existing directory; pass a file path or a directory without "
                    f"a .json suffix"
                )
            ancestry_root = target.parent
        else:
            if target.exists() and not target.is_dir():
                raise ValueError(
                    f"--record path {target} is an existing file without a .json "
                    f"suffix and would be treated as a directory; pass a .json "
                    f"fixture path or a directory"
                )
            ancestry_root = target
        # Walk to the nearest EXISTING ancestor (components below it do not
        # exist yet and will be created by mkdir(parents=True) at flush time);
        # if that ancestor is not a directory, the flush is doomed.
        for ancestor in (ancestry_root, *ancestry_root.parents):
            if ancestor.exists():
                if not ancestor.is_dir():
                    raise ValueError(
                        f"--record path {target} requires {ancestor} to be a "
                        f"directory, but it is an existing file; fixture writes "
                        f"would fail at shutdown"
                    )
                break

    def _accumulate_record(self, matches: list[NormalizedMatch]) -> None:
        for match in matches:
            recorded = self._record.get(match.match_id)
            if recorded is None:
                self._record[match.match_id] = _RecordedMatch.from_match(match)
            else:
                recorded.update(match)

    def _flush_record(self, records: dict[str, _RecordedMatch]) -> None:
        if not records:
            return
        target = self._record_path
        assert target is not None
        to_json_file = target.suffix == ".json"
        single = len(records) == 1
        for match_id, recorded in records.items():
            if to_json_file:
                # A ``.json`` record path names a single fixture file. With more
                # than one match it cannot be that file for all of them, and
                # ``target / <id>.json`` would create a directory literally named
                # ``*.json``; write unambiguous siblings ``<stem>-<id>.json``
                # next to it instead.
                path = (
                    target
                    if single
                    else target.with_name(f"{target.stem}-{fixture_stem(match_id)}.json")
                )
            else:
                path = target / f"{fixture_stem(match_id)}.json"
            write_fixture(path, recorded.as_match())

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


class _RecordedMatch:
    """Accumulates a match's latest state + union of events for ``--record``."""

    def __init__(self, header: NormalizedMatch) -> None:
        self._header = header
        self._events: dict[int, NormalizedEvent] = {e.seq: e for e in header.events}

    @classmethod
    def from_match(cls, match: NormalizedMatch) -> _RecordedMatch:
        return cls(match)

    def update(self, match: NormalizedMatch) -> None:
        self._header = match
        for event in match.events:
            self._events[event.seq] = event

    def as_match(self) -> NormalizedMatch:
        ordered = [self._events[seq] for seq in sorted(self._events)]
        return replace(self._header, events=ordered)
