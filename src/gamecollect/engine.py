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
  raises :class:`~gamecollect.db.writer.SequenceError`; the engine logs it and
  skips that match for the poll, leaving other matches collecting and the
  daemon up.
* **Seed-before-child-write** — the engine never calls ``append_events``
  before the match row exists; it seeds through the pack's ``seed_match`` hook
  (core never imports a pack) and skips child writes when the hook returns
  ``None`` (missing identity).
* **Graceful shutdown** — SIGTERM sets a stop event that interrupts the sleep;
  the loop finishes the current iteration, flushes any ``--record`` fixture,
  and closes the connection.
"""

from __future__ import annotations

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
        self._rng = rng if rng is not None else random.Random()
        self._stop_event = threading.Event()
        self._sleep = sleep if sleep is not None else self._stop_event.wait

        self._conn = connect(self._db_path, side_table_ddl=pack.side_table_ddl)
        self._writer = PartitionWriter(self._conn, source, taxonomy=pack.taxonomy)

        # Last-written provider snapshot per provider-native match_id (the diff
        # baseline) and, when recording, the merged fixture accumulator.
        self._last: dict[str, NormalizedMatch] = {}
        self._record: dict[str, _RecordedMatch] = {}
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
        events into the baseline and losing them forever.
        """
        matches = self._provider.fetch_live_matches()
        if self._record_path is not None:
            self._accumulate_record(matches)
        for diff in diff_matches(matches, self._last):
            if not diff.has_changes:
                continue
            try:
                applied = self._apply(diff)
            except SequenceError as exc:
                # A re-sent seq's fingerprint shifted under the provider's
                # positional list: loud per-match drift, not a daemon fault.
                # Leave self._last unchanged so a corrected list re-syncs.
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
            if applied:
                self._last[diff.match.match_id] = diff.match

    def _apply(self, diff: MatchDiff) -> bool:
        """Seed the match through the pack hook, then append its new events.

        Returns ``True`` when the match was seeded and its events (if any)
        written, ``False`` when ``seed_match`` returned ``None`` and child
        writes were skipped. The caller uses this to decide whether to advance
        the diff baseline — a skipped match must NOT be baselined, or its
        events would be treated as already-written on the next seedable poll.
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
            return False
        if diff.new_events:
            self._writer.append_events(seeded_id, [_event_to_row(e) for e in diff.new_events])
        return True

    def stop(self) -> None:
        """Signal the loop to finish its current iteration and shut down."""
        self._stop_event.set()

    def close(self) -> None:
        """Flush any recorded fixture and close the database connection (idempotent).

        The flush is attempted at most once (``_record`` is cleared before the
        write so a second ``close()`` after a failed flush does not re-raise) and
        the connection is closed unconditionally in a ``finally`` — a
        ``write_fixture`` failure must not leak the WAL connection, and the CLI's
        redundant second ``close()`` must be a no-op that re-raises nothing.
        """
        try:
            if self._record_path is not None and self._record:
                recorded = self._record
                self._record = {}
                self._flush_record(recorded)
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
