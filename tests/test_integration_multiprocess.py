"""Multiprocess counterpart to tests/test_integration_shared_file.py.

That file's admission-lock and end-to-end checks run the "collector" and a
second ("commentary") writer as threads within one Python process — a real
sqlite3.Connection is shared across threads there via check_same_thread=False.
That setup cannot surface cross-process locking issues: flock(2) semantics,
WAL visibility, and busy_timeout retries only really get exercised once two
separate OS processes, each with its own connection, contend for the same
file. This file re-runs the same two scenarios with the writers as separate
processes via multiprocessing, to catch what a same-process thread test can't.
"""

from __future__ import annotations

import multiprocessing
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "football"))
from _football_helpers import KNOCKOUT_FIXTURES, replay_fixture  # noqa: E402

from gamecollect.client import get_vocabulary  # noqa: E402
from gamecollect.db.connection import connect  # noqa: E402
from gamecollect.db.locking import live_db_admission_lock  # noqa: E402
from gamecollect.db.paths import lock_dir as resolve_lock_dir  # noqa: E402
from gamecollect.db.writer import PartitionWriter  # noqa: E402
from gamecollect.engine import _event_to_row  # noqa: E402
from gamecollect_football.pack import (  # noqa: E402
    FOOTBALL_SIDE_TABLE_DDL,
    persist_football_side_tables,
)
from gamecollect_football.readport import FootballReadPort  # noqa: E402

SOURCE = "wc2026"
PACK_NAME = "football-wc2026"
FIXTURE = KNOCKOUT_FIXTURES[2]  # spain_portugal_760506: full lineups, one goal, venue
MATCH_ID = FIXTURE["match_id"]

GAMEALERTS_COMMENTARY_DDL = """
CREATE TABLE IF NOT EXISTS commentary (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id    TEXT NOT NULL,
    text        TEXT NOT NULL,
    spoken      INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);
"""

# Generous vs. the threaded test's 2.0s/0.5s: process start/teardown is
# slower than a thread context switch, especially under CI.
ACQUIRE_TIMEOUT = 10.0
NO_BLOCK_DEADLINE = 3.0


def _seed_collector_facts(conn) -> None:
    """Same recipe as test_integration_shared_file.py::_seed_collector_facts,
    duplicated locally: each worker below runs in its own OS process and must
    import/construct everything itself — nothing but picklable arguments
    (paths, multiprocessing.Event) crosses the process boundary."""
    match = replay_fixture(FIXTURE["summary"], MATCH_ID)
    writer = PartitionWriter(conn, SOURCE)
    status = match.status.value if hasattr(match.status, "value") else str(match.status)
    payload = dict(match.payload)
    payload["home_team"] = match.home_team
    payload["away_team"] = match.away_team
    writer.upsert_match(
        {
            "match_id": MATCH_ID,
            "source": SOURCE,
            "status": status,
            "kickoff_utc": match.kickoff_utc,
            "score_home": match.score_home,
            "score_away": match.score_away,
            "display_clock": match.display_clock,
            "payload": payload,
        }
    )
    if match.events:
        writer.append_events(MATCH_ID, [_event_to_row(ev) for ev in match.events])
    persist_football_side_tables(conn, writer, match, MATCH_ID)
    conn.commit()


def _goal_family_types(vocabulary) -> set[str]:
    return {
        event_type
        for event_type, entry in vocabulary.taxonomy.items()
        if "goal" in entry.display_name.lower()
    }


# Module-level (not nested-closure) so multiprocessing can pickle a reference
# to each target function by qualified name.


def _collector_writer_process(
    db_path: str, lock_dir: str, collector_acquired, release_collector
) -> None:
    """Opens its own connection, seeds facts under the SHARED admission lock,
    signals readiness, then holds the lock until told to release."""
    conn = connect(Path(db_path), side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
    try:
        with live_db_admission_lock(Path(lock_dir), exclusive=False):
            _seed_collector_facts(conn)
            collector_acquired.set()
            release_collector.wait(ACQUIRE_TIMEOUT)
    finally:
        conn.close()


def _commentary_lock_process(lock_dir: str, collector_acquired, commentary_acquired) -> None:
    """Waits for the collector to signal it holds the SHARED lock, then
    acquires SHARED itself — must not block behind the collector's concurrent
    hold, proving SHARED holders coexist across a real process boundary."""
    collector_acquired.wait(ACQUIRE_TIMEOUT)
    with live_db_admission_lock(Path(lock_dir), exclusive=False):
        commentary_acquired.set()


def _commentary_insert_process(db_path: str, lock_dir: str) -> None:
    """Applies gamealerts' own commentary DDL and inserts one row under the
    SHARED admission lock, then closes — the process-boundary counterpart to
    TestEndToEndSharedFile's step 3."""
    conn = connect(Path(db_path), side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
    try:
        with live_db_admission_lock(Path(lock_dir), exclusive=False):
            conn.executescript(GAMEALERTS_COMMENTARY_DDL)
            conn.execute(
                "INSERT INTO commentary (match_id, text, created_at) VALUES (?, ?, ?)",
                (MATCH_ID, "Merino scores in stoppage time!", "2026-07-06T20:50:00+00:00"),
            )
            conn.commit()
    finally:
        conn.close()


@pytest.fixture
def data_dir(tmp_path):
    d = tmp_path / "shared"
    d.mkdir()
    return d


@pytest.fixture
def db_path(data_dir):
    return data_dir / "gamecollect.db"


@pytest.fixture
def mp_context():
    """fork, not the platform default (spawn on macOS/Windows): spawn requires
    this test module to be freshly re-importable by dotted name in the child,
    which is fragile given the sys.path manipulation above for
    _football_helpers. Each worker function still opens its own connect()
    rather than relying on any parent-process state, so fork's copy-on-write
    memory doesn't mask the thing this test checks — cross-process flock +
    WAL + busy_timeout behavior."""
    return multiprocessing.get_context("fork")


class TestConcurrentSharedAdmissionLockAcrossProcesses:
    """Process-boundary counterpart to
    TestConcurrentSharedAdmissionLock in test_integration_shared_file.py: same
    SHARED-lock-coexistence property, but the two writers are real OS
    processes, not threads in one interpreter."""

    def test_collector_and_commentary_writer_hold_shared_lock_concurrently(
        self, data_dir, db_path, mp_context
    ):
        lock_dir = resolve_lock_dir(data_dir)

        collector_acquired = mp_context.Event()
        release_collector = mp_context.Event()
        commentary_acquired = mp_context.Event()

        p1 = mp_context.Process(
            target=_collector_writer_process,
            args=(str(db_path), str(lock_dir), collector_acquired, release_collector),
        )
        p2 = mp_context.Process(
            target=_commentary_lock_process,
            args=(str(lock_dir), collector_acquired, commentary_acquired),
        )
        p1.start()
        p2.start()
        try:
            assert collector_acquired.wait(ACQUIRE_TIMEOUT), (
                "collector writer process never acquired the SHARED admission lock"
            )
            assert commentary_acquired.wait(NO_BLOCK_DEADLINE), (
                "commentary writer process blocked behind the collector's SHARED "
                "hold across a real process boundary — SHARED admission-lock "
                "holders must coexist, not serialize"
            )

            release_collector.set()
            p1.join(ACQUIRE_TIMEOUT)
            p2.join(ACQUIRE_TIMEOUT)
            assert not p1.is_alive(), "collector writer process failed to complete"
            assert not p2.is_alive(), "commentary writer process failed to complete"
            assert p1.exitcode == 0, f"collector writer process failed: exitcode={p1.exitcode}"
            assert p2.exitcode == 0, f"commentary writer process failed: exitcode={p2.exitcode}"
        finally:
            for p in (p1, p2):
                if p.is_alive():
                    p.terminate()
                    p.join(ACQUIRE_TIMEOUT)


class TestEndToEndSharedFileAcrossProcesses:
    """Process-boundary counterpart to TestEndToEndSharedFile: the collector
    seeds facts in one child process, a second child process writes
    commentary, and this (parent) process reads back through a fresh
    connection — verifying no WAL corruption and that both writers' data are
    visible once every process has released the file."""

    def test_full_flow_facts_and_commentary_across_processes_no_corruption(
        self, data_dir, db_path, mp_context
    ):
        lock_dir = resolve_lock_dir(data_dir)

        # 1. Collector writer process: creates + stamps the file, seeds facts
        #    under the SHARED admission lock, then exits.
        collector_acquired = mp_context.Event()
        release_collector = mp_context.Event()
        p1 = mp_context.Process(
            target=_collector_writer_process,
            args=(str(db_path), str(lock_dir), collector_acquired, release_collector),
        )
        p1.start()
        assert collector_acquired.wait(ACQUIRE_TIMEOUT), (
            "collector writer process never acquired the SHARED admission lock"
        )
        release_collector.set()
        p1.join(ACQUIRE_TIMEOUT)
        assert not p1.is_alive(), "collector writer process failed to complete"
        assert p1.exitcode == 0, f"collector writer process failed: exitcode={p1.exitcode}"

        # 2. Second writer process: applies gamealerts' own commentary DDL and
        #    inserts one row under the SHARED admission lock.
        p2 = mp_context.Process(
            target=_commentary_insert_process, args=(str(db_path), str(lock_dir))
        )
        p2.start()
        p2.join(ACQUIRE_TIMEOUT)
        assert not p2.is_alive(), "commentary writer process failed to complete"
        assert p2.exitcode == 0, f"commentary writer process failed: exitcode={p2.exitcode}"

        # 3. Parent process: a fresh connection observes BOTH writers' data,
        #    with no corruption.
        fresh = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            adapter = FootballReadPort(fresh)
            vocabulary = get_vocabulary(fresh, PACK_NAME)
            goal_types = _goal_family_types(vocabulary)

            scorers = [
                e["player"] for e in adapter.events_for_match(MATCH_ID) if e["type"] in goal_types
            ]
            assert scorers == ["Mikel Merino"]

            lineup = adapter.lineup_for_match(MATCH_ID)
            assert {row["participant"] for row in lineup} == {"Portugal", "Spain"}

            venue = adapter.venue_for_match(MATCH_ID)
            assert venue == {"stadium": "AT&T Stadium", "city": "Arlington"}

            commentary_rows = fresh.execute(
                "SELECT match_id, text FROM commentary WHERE match_id = ?", (MATCH_ID,)
            ).fetchall()
            assert [dict(row) for row in commentary_rows] == [
                {"match_id": MATCH_ID, "text": "Merino scores in stoppage time!"}
            ]

            recent = adapter.recent_commentary(MATCH_ID)
            assert len(recent) == 1
            assert recent[0]["text"] == "Merino scores in stoppage time!"
        finally:
            fresh.close()
