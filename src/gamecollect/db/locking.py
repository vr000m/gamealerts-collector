"""Advisory locking for the collector's shared SQLite file.

Mirrors gamealerts' locking convention (``gamealerts/locks.py``) so the
collector and gamealerts agree on one primitive when they share a data
directory: ``_live_db_admission.lock`` is acquired SHARED by every live-DB
writer for the duration of its write session — many SHARED holders coexist,
they do not block one another. EXCLUSIVE is reserved for destructive
maintenance (a *future* collector-side reset; no such user exists yet, so
today the lock excludes nothing — see dev plan
`20260707-feature-gameworker-integration.md`, Phase 1 and Review Focus).

Write serialization itself is SQLite WAL + ``busy_timeout`` (connection.py),
not this lock. Readers never acquire it.
"""

from __future__ import annotations

import fcntl
from pathlib import Path
from typing import IO

__all__ = ["AdvisoryLock", "live_db_admission_lock"]

_ADMISSION_LOCK_FILENAME = "_live_db_admission.lock"


class AdvisoryLock:
    """An ``flock``-backed advisory lock file, usable as a context manager.

    ``exclusive=False`` (the default) acquires SHARED — coexists with other
    SHARED holders. ``exclusive=True`` acquires EXCLUSIVE — blocks until no
    other holder (shared or exclusive) remains, and blocks any new SHARED
    acquire while held. ``blocking=False`` makes a contended acquire raise
    ``BlockingIOError`` immediately instead of waiting, per ``flock(2)``.
    """

    def __init__(self, path: str | Path, *, exclusive: bool = False, blocking: bool = True) -> None:
        self._path = Path(path)
        self._exclusive = exclusive
        self._blocking = blocking
        self._fh: IO[str] | None = None

    def __enter__(self) -> AdvisoryLock:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self._path, "a+")
        op = fcntl.LOCK_EX if self._exclusive else fcntl.LOCK_SH
        if not self._blocking:
            op |= fcntl.LOCK_NB
        try:
            fcntl.flock(fh, op)
        except BaseException:
            fh.close()
            raise
        self._fh = fh
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._fh is not None:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None


def live_db_admission_lock(
    lock_dir: str | Path, *, exclusive: bool = False, blocking: bool = True
) -> AdvisoryLock:
    """The collector's shared-file admission lock, scoped to ``lock_dir``.

    ``lock_dir`` is ``<data_dir>/locks`` (:func:`gamecollect.db.paths.lock_dir`).
    Every live-DB writer session — collector daemon or gamealerts' commentary
    writer — holds this SHARED for its duration; a future destructive
    collector-side reset would hold it EXCLUSIVE to fence in-flight writers.
    """
    return AdvisoryLock(
        Path(lock_dir) / _ADMISSION_LOCK_FILENAME, exclusive=exclusive, blocking=blocking
    )
