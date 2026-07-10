"""``--record`` fixture capture collaborator for :class:`CollectorEngine`.

Encapsulates the ``--record`` feature: validating the target up front, accumulating
the best-known state per match across polls, and flushing captured fixtures to disk
at most once at shutdown.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from gamecollect.fixture_io import fixture_stem, write_fixture
from gamecollect.provider import NormalizedEvent, NormalizedMatch

__all__ = ["RecordWriter", "RecordedMatch"]


class RecordedMatch:
    """Accumulates a match's latest state + union of events for ``--record``."""

    def __init__(self, header: NormalizedMatch) -> None:
        self._header = header
        self._events: dict[int, NormalizedEvent] = {e.seq: e for e in header.events}

    @classmethod
    def from_match(cls, match: NormalizedMatch) -> RecordedMatch:
        return cls(match)

    def update(self, match: NormalizedMatch) -> None:
        self._header = match
        for event in match.events:
            self._events[event.seq] = event

    def as_match(self) -> NormalizedMatch:
        ordered = [self._events[seq] for seq in sorted(self._events)]
        return replace(self._header, events=ordered)


class RecordWriter:
    """Captures observed matches to fixtures for ``--record``.

    Constructed only when a ``--record`` target is configured; validates the
    target up front (fail fast before any polling) and flushes at most once.
    """

    def __init__(self, record_path: Path) -> None:
        # Fail fast on an unusable --record target BEFORE any polling: a bad
        # path discovered only at shutdown would lose the whole recorded
        # session (flush happens at most once).
        self.validate_target(record_path)
        self._record_path = record_path
        self._record: dict[str, RecordedMatch] = {}
        self._record_flush_failed = False

    @staticmethod
    def validate_target(target: Path) -> None:
        """Reject an unusable ``--record`` target at construction time.

        A non-``.json`` path is treated as a directory by :meth:`flush`;
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

    @property
    def pending_count(self) -> int:
        """Number of matches accumulated since the last successful flush."""
        return len(self._record)

    def accumulate(self, matches: list[NormalizedMatch]) -> None:
        for match in matches:
            recorded = self._record.get(match.match_id)
            if recorded is None:
                self._record[match.match_id] = RecordedMatch.from_match(match)
            else:
                recorded.update(match)

    def flush(self) -> None:
        """Write captured fixtures to disk at most once.

        A no-op when nothing was captured or a prior flush already failed
        (``_record_flush_failed`` latch). On a write failure the latch is set
        and the error re-raised; the accumulated session is cleared only AFTER
        a successful flush so a failed flush preserves it in memory.
        """
        if not self._record or self._record_flush_failed:
            return
        try:
            self._write(self._record)
        except BaseException:
            self._record_flush_failed = True
            raise
        self._record = {}

    def _write(self, records: dict[str, RecordedMatch]) -> None:
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
