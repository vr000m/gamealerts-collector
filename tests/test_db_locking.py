"""Phase 1: SHARED/EXCLUSIVE admission-lock semantics + WAL read-through.

Plan contract (docs/dev_plans/20260707-feature-gameworker-integration.md
Phase 1, Testing Notes, Acceptance Criteria):
- ``gamecollect.db.locking`` mirrors gamealerts' ``AdvisoryLock`` /
  ``live_db_admission_lock`` convention (``fcntl.flock``) verbatim: a SHARED
  admission lock many writers hold concurrently for their whole write
  session, EXCLUSIVE reserved for a destructive reset — shared holders
  coexist; the exclusion is solely a reset-fence.
- Two connections to one temp file: a SHARED writer never blocks another
  SHARED writer; an EXCLUSIVE holder blocks a new SHARED acquire and
  vice-versa (the reset-fence direction — the whole point of the lock); a
  reader sees committed rows via WAL without blocking.
"""

from __future__ import annotations

import threading

import pytest
from _phase1_helpers import acquire_admission_lock

from gamecollect.db import locking
from gamecollect.db.connection import connect

ACQUIRE_TIMEOUT = 2.0
NO_BLOCK_DEADLINE = 0.5


@pytest.fixture
def lock_dir(tmp_path):
    d = tmp_path / "locks"
    d.mkdir()
    return d


def _lock_fn():
    fn = getattr(locking, "live_db_admission_lock", None)
    if fn is None:
        pytest.skip("gamecollect.db.locking has no live_db_admission_lock yet")
    return fn


class _Holder:
    """Acquires the admission lock on a background thread and holds it until told to stop.

    fcntl.flock locks are scoped to the open file description, so two
    ``AdvisoryLock``/``live_db_admission_lock`` instances in the same process
    (each doing its own open()) contend exactly as two separate processes
    would — a background thread is sufficient to exercise SHARED/EXCLUSIVE
    contention without spawning a subprocess.
    """

    def __init__(self, lock_dir, *, exclusive):
        self._lock_dir = lock_dir
        self._exclusive = exclusive
        self.acquired = threading.Event()
        self._release = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        cm = acquire_admission_lock(_lock_fn(), self._lock_dir, exclusive=self._exclusive)
        with cm:
            self.acquired.set()
            self._release.wait(ACQUIRE_TIMEOUT)

    def start(self):
        self._thread.start()
        if not self.acquired.wait(ACQUIRE_TIMEOUT):
            raise AssertionError("holder failed to acquire the admission lock in time")

    def stop(self):
        self._release.set()
        self._thread.join(ACQUIRE_TIMEOUT)


def test_shared_writer_does_not_block_another_shared_writer(lock_dir):
    holder = _Holder(lock_dir, exclusive=False)
    holder.start()
    try:
        acquired_second = threading.Event()

        def second():
            cm = acquire_admission_lock(_lock_fn(), lock_dir, exclusive=False)
            with cm:
                acquired_second.set()

        t = threading.Thread(target=second, daemon=True)
        t.start()
        t.join(NO_BLOCK_DEADLINE)
        assert acquired_second.is_set(), "a second SHARED acquire blocked behind the first"
    finally:
        holder.stop()


def test_exclusive_holder_blocks_new_shared_acquire(lock_dir):
    """The reset-fence direction: EXCLUSIVE (destructive reset) fences out new SHARED writers."""
    holder = _Holder(lock_dir, exclusive=True)
    holder.start()
    try:
        acquired_shared = threading.Event()

        def shared_waiter():
            cm = acquire_admission_lock(_lock_fn(), lock_dir, exclusive=False)
            with cm:
                acquired_shared.set()

        t = threading.Thread(target=shared_waiter, daemon=True)
        t.start()
        t.join(NO_BLOCK_DEADLINE)
        assert not acquired_shared.is_set(), (
            "SHARED acquire did not block behind an EXCLUSIVE holder"
        )

        holder.stop()
        t.join(ACQUIRE_TIMEOUT)
        assert acquired_shared.is_set(), (
            "SHARED acquire never succeeded after the EXCLUSIVE holder released"
        )
    finally:
        holder.stop()


def test_shared_holder_blocks_new_exclusive_acquire(lock_dir):
    holder = _Holder(lock_dir, exclusive=False)
    holder.start()
    try:
        acquired_exclusive = threading.Event()

        def exclusive_waiter():
            cm = acquire_admission_lock(_lock_fn(), lock_dir, exclusive=True)
            with cm:
                acquired_exclusive.set()

        t = threading.Thread(target=exclusive_waiter, daemon=True)
        t.start()
        t.join(NO_BLOCK_DEADLINE)
        assert not acquired_exclusive.is_set(), (
            "EXCLUSIVE acquire did not block behind a SHARED holder"
        )

        holder.stop()
        t.join(ACQUIRE_TIMEOUT)
        assert acquired_exclusive.is_set(), (
            "EXCLUSIVE acquire never succeeded after the SHARED holder released"
        )
    finally:
        holder.stop()


def test_two_connections_open_same_temp_file(tmp_path):
    db_path = tmp_path / "locking.db"
    conn_a = connect(db_path)
    conn_b = connect(db_path)
    try:
        version_a = conn_a.execute("SELECT major, minor FROM schema_meta").fetchone()
        version_b = conn_b.execute("SELECT major, minor FROM schema_meta").fetchone()
        assert tuple(version_a) == tuple(version_b)
    finally:
        conn_a.close()
        conn_b.close()


def test_reader_sees_committed_rows_via_wal_without_blocking(tmp_path):
    db_path = tmp_path / "locking.db"
    writer = connect(db_path)
    reader = connect(db_path)
    try:
        writer.execute(
            "INSERT INTO matches (match_id, source) VALUES (?, ?)",
            ("m1", "phase1-lock-test"),
        )
        writer.commit()

        rows = reader.execute("SELECT match_id FROM matches WHERE match_id = ?", ("m1",)).fetchall()
        assert [row["match_id"] for row in rows] == ["m1"]

        # A concurrent WAL reader must complete promptly, not queue behind the writer.
        done = threading.Event()

        def read():
            reader.execute("SELECT 1").fetchone()
            done.set()

        t = threading.Thread(target=read, daemon=True)
        t.start()
        t.join(NO_BLOCK_DEADLINE)
        assert done.is_set(), "reader query did not complete promptly under WAL"
    finally:
        writer.close()
        reader.close()
