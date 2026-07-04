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
from dataclasses import replace
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
    LIVE_STATUSES,
    MatchDataProvider,
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


# NormalizedMatch top-level fields where ``None`` means "not provided" (identity
# and timing/state fields optional on partial snapshots). A detail endpoint that
# omits one must not wipe the scoreboard's value: detail wins where it has a
# value; the scoreboard fills detail's Nones. ``status`` is a non-optional enum
# and ``events``/``payload`` merge separately, so they are not listed.
_MERGE_FILL_FIELDS = (
    "home_team",
    "away_team",
    "kickoff_utc",
    "minute",
    "score_home",
    "score_away",
    "display_clock",
)


def _merge_detail(scoreboard: NormalizedMatch, detail: NormalizedMatch) -> NormalizedMatch:
    """Merge a detail snapshot over its scoreboard snapshot, field by field.

    Payload keys merge with detail winning; top-level fields take the detail
    value unless it is ``None``, in which case the scoreboard value fills it —
    a detail endpoint omitting ``kickoff_utc``/``home_team``/``away_team``
    must not wipe stored identity/timing to NULL (or make ``seed_match``
    return ``None`` and drop the poll's events).
    """
    payload = dict(scoreboard.payload)
    payload.update(detail.payload)
    fills = {
        name: getattr(scoreboard, name)
        for name in _MERGE_FILL_FIELDS
        if getattr(detail, name) is None
    }
    return replace(detail, payload=payload, **fills)


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
          for every insert it performs.

        Other writer rejections stay fatal to the poll for this match (they
        propagate to ``poll_once``'s per-match isolation). Returns the events
        stored AFTER reconciliation, for the caller to baseline from.
        """
        stored_seqs = {event.seq for event in self._stored_events(seeded_id)}
        head = max(stored_seqs) if stored_seqs else None
        for row in rows:
            seq = row.get("seq")
            if head is not None and isinstance(seq, int) and seq <= head:
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
        there would permanently lose stoppage-time events.

        One match's failed detail fetch must not abort the slate: it is logged
        (WARNING for unavailability, ERROR for shape drift) and that match
        falls back to its scoreboard snapshot while the others proceed —
        per-match isolation. Slate-level errors from ``fetch_live_matches``
        still propagate to the loop's backoff.
        """
        matches = self._provider.fetch_live_matches()
        hydrated: list[NormalizedMatch] = []
        for match in matches:
            previous = self._last.get(match.match_id)
            was_live = previous is not None and previous.status in LIVE_STATUSES
            if match.status not in LIVE_STATUSES and not was_live:
                hydrated.append(match)
                continue
            try:
                detail = self._provider.fetch_match_detail(match.match_id)
            except ProviderUnavailableError as exc:
                log.warning(
                    "detail fetch unavailable for match %s on source %s: %s "
                    "(falling back to scoreboard snapshot; other matches proceed)",
                    match.match_id,
                    self._source,
                    exc,
                )
                hydrated.append(match)
                continue
            except ShapeDriftError as exc:
                log.error(
                    "detail shape drift for match %s on source %s: %s "
                    "(falling back to scoreboard snapshot; other matches proceed)",
                    match.match_id,
                    self._source,
                    exc,
                    exc_info=True,
                )
                hydrated.append(match)
                continue
            hydrated.append(_merge_detail(match, detail))
        return hydrated

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
