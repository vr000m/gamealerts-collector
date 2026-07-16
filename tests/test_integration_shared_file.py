"""Phase 5: end-to-end integration over one shared collector-owned file.

Plan (docs/dev_plans/20260707-feature-gameworker-integration.md, Phase 5 +
Testing Notes "Integration"):

- One shared temp file exercised by a collector writer thread (facts), a
  stub reader (``FootballReadPort`` + the ``vocabulary`` op answering
  "who scored / lineup / venue"), and a second "commentary writer" thread —
  all under the SHARED admission lock (``gamecollect.db.locking``).
- No corruption; both writers' rows are visible; a fresh connection observes
  both via WAL; the two SHARED holders neither deadlock nor block each other
  (the lock is a reset-fence, not a per-write mutex — Review Focus "Lock
  semantics").
- Startup ordering (Architecture Decisions): the collector must ``connect()``
  (create + stamp ``schema_meta`` + create ``matches``) before anything else
  touches the file — gamealerts' scoped ``commentary`` DDL does not stamp
  ``schema_meta``, and the ``commentary -> matches`` FK needs ``matches`` to
  already exist.
- "Who scored" is answered by cross-referencing ``events_for_match`` against
  the ``vocabulary`` op's taxonomy display names (goal-family event types
  identified generically, not by hardcoding ``"goal"``) — Phase 3's Review
  Focus "Read shape genericity" applies to this integration path too.

This module lives at the flat ``tests/`` level (not ``tests/football/``), so
``_football_helpers`` needs the explicit ``sys.path`` insert already
established by ``tests/test_readport_contract.py`` — plain rootdir-relative
resolution does not reach across the ``tests/football/`` boundary.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "football"))
from _football_helpers import KNOCKOUT_FIXTURES, replay_fixture  # noqa: E402

from gamecollect.client import get_vocabulary
from gamecollect.db.connection import connect
from gamecollect.db.locking import live_db_admission_lock
from gamecollect.db.migrations import get_schema_version
from gamecollect.db.paths import lock_dir as resolve_lock_dir
from gamecollect.db.writer import PartitionWriter
from gamecollect.engine import _event_to_row
from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL, persist_football_side_tables
from gamecollect_football.readport import FootballReadPort

SOURCE = "wc2026"
PACK_NAME = "football-wc2026"

# spain_portugal_760506: one clean regulation goal (Mikel Merino, Spain,
# stoppage time), two full committed lineups, and stadium/city data — one
# fixture exercises all three read-surface questions (scorer/lineup/venue).
FIXTURE = KNOCKOUT_FIXTURES[2]
MATCH_ID = FIXTURE["match_id"]

# gamealerts' OWN scoped commentary DDL, applied through its OWN connection
# (never the collector's side_table_ddl seam, never a full schema.sql) —
# same minimal shape as tests/test_consumer_ddl.py's GAMEALERTS_COMMENTARY_DDL.
# This repo has no gamealerts package to import it from.
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

ACQUIRE_TIMEOUT = 2.0
NO_BLOCK_DEADLINE = 0.5


@pytest.fixture
def data_dir(tmp_path):
    d = tmp_path / "shared"
    d.mkdir()
    return d


@pytest.fixture
def db_path(data_dir):
    return data_dir / "gamecollect.db"


def _seed_collector_facts(conn) -> None:
    """Collector writer: seed the knockout fixture's facts (match row,
    events, football side tables), mirroring the engine's real per-poll
    write path (same shape as the seeded_db fixture in
    tests/test_readport_contract.py)."""

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
        # The real engine write path (gamecollect.engine._event_to_row)
        # stamps goal-family player names under the "scorer" payload key,
        # not "player" — gamecollect_football.readport.FootballReadPort
        # reads that same key, so seeding must mirror it exactly rather
        # than a simplified re-derivation.
        writer.append_events(MATCH_ID, [_event_to_row(ev) for ev in match.events])
    persist_football_side_tables(conn, writer, match, MATCH_ID)
    conn.commit()


def _goal_family_types(vocabulary) -> set[str]:
    """Identify goal-family event types generically from the vocabulary op's
    taxonomy display names, rather than hardcoding "goal"/"own_goal" — the
    data-driven design intent Phase 4's vocabulary op exists to serve."""
    return {
        event_type
        for event_type, entry in vocabulary.taxonomy.items()
        if "goal" in entry.display_name.lower()
    }


class TestStartupOrdering:
    """Architecture Decisions: the collector must create + stamp the file
    before anything else opens it — a hard ordering constraint, not left
    implicit."""

    def test_collector_creates_and_stamps_file_before_anything_else(self, db_path):
        assert not db_path.exists()
        conn = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            assert get_schema_version(conn) is not None
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            assert "matches" in tables, (
                "collector connect() must create 'matches' before any commentary "
                "writer's FK-dependent DDL can be applied"
            )
        finally:
            conn.close()


class TestConcurrentSharedAdmissionLock:
    """The admission lock is SHARED for both the collector and a commentary
    writer — they coexist for the whole write session and must not deadlock
    or block one another (Review Focus "Lock semantics" / "Lock hold
    duration")."""

    def test_collector_and_commentary_writer_hold_shared_lock_concurrently(self, data_dir, db_path):
        lock_dir = resolve_lock_dir(data_dir)
        collector_conn = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            collector_acquired = threading.Event()
            release_collector = threading.Event()
            commentary_acquired = threading.Event()

            def collector_writer():
                with live_db_admission_lock(lock_dir, exclusive=False):
                    _seed_collector_facts(collector_conn)
                    collector_acquired.set()
                    release_collector.wait(ACQUIRE_TIMEOUT)

            def commentary_writer():
                collector_acquired.wait(ACQUIRE_TIMEOUT)
                with live_db_admission_lock(lock_dir, exclusive=False):
                    commentary_acquired.set()

            t1 = threading.Thread(target=collector_writer, daemon=True)
            t2 = threading.Thread(target=commentary_writer, daemon=True)
            t1.start()
            t2.start()

            assert collector_acquired.wait(ACQUIRE_TIMEOUT), (
                "collector writer never acquired the SHARED admission lock"
            )
            t2.join(NO_BLOCK_DEADLINE)
            assert commentary_acquired.is_set(), (
                "commentary writer blocked behind the collector's SHARED hold — "
                "SHARED admission-lock holders must coexist, not serialize"
            )

            release_collector.set()
            t1.join(ACQUIRE_TIMEOUT)
            t2.join(ACQUIRE_TIMEOUT)
            assert not t1.is_alive() and not t2.is_alive(), "a writer thread failed to complete"
        finally:
            collector_conn.close()


class TestEndToEndSharedFile:
    """Collector facts + stub reader (readport/vocabulary) + commentary
    writer, all against one shared file: no corruption, WAL reads on a fresh
    connection observe both writers' data."""

    def test_full_flow_facts_readport_vocabulary_and_commentary_no_corruption(
        self, data_dir, db_path
    ):
        lock_dir = resolve_lock_dir(data_dir)

        # 1. Collector connects first (creates + stamps schema_meta +
        #    matches — the startup-ordering constraint), then writes facts
        #    under the SHARED admission lock.
        collector_conn = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            with live_db_admission_lock(lock_dir, exclusive=False):
                _seed_collector_facts(collector_conn)
        finally:
            collector_conn.close()

        # 2. Stub reader: FootballReadPort + the vocabulary op answer three
        #    concrete questions from the seeded fixture, over a separate
        #    connection to the same file.
        reader_conn = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            adapter = FootballReadPort(reader_conn)
            vocabulary = get_vocabulary(reader_conn, PACK_NAME)
            goal_family_types = _goal_family_types(vocabulary)
            assert goal_family_types, (
                "expected the football pack's taxonomy to declare at least one "
                "goal-family event type"
            )

            # Who scored?
            events = adapter.events_for_match(MATCH_ID)
            scorers = [e["player"] for e in events if e["type"] in goal_family_types]
            assert scorers == ["Mikel Merino"], (
                f"expected Mikel Merino to have scored, got {scorers}"
            )

            # Lineup?
            lineup = adapter.lineup_for_match(MATCH_ID)
            assert lineup, "expected a non-empty lineup for the seeded fixture"
            assert {row["participant"] for row in lineup} == {"Portugal", "Spain"}
            assert any(row["player"] == "Mikel Merino" for row in lineup)

            # Venue?
            venue = adapter.venue_for_match(MATCH_ID)
            assert venue == {"stadium": "AT&T Stadium", "city": "Arlington"}
        finally:
            reader_conn.close()

        # 3. Second "commentary writer": under the SHARED admission lock,
        #    apply gamealerts' OWN scoped commentary DDL (never the
        #    collector's side_table_ddl seam) and insert one row.
        commentary_conn = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            with live_db_admission_lock(lock_dir, exclusive=False):
                commentary_conn.executescript(GAMEALERTS_COMMENTARY_DDL)
                commentary_conn.execute(
                    "INSERT INTO commentary (match_id, text, created_at) VALUES (?, ?, ?)",
                    (MATCH_ID, "Merino scores in stoppage time!", "2026-07-06T20:50:00+00:00"),
                )
                commentary_conn.commit()
        finally:
            commentary_conn.close()

        # 4. No corruption; a fresh connection's WAL reads observe BOTH
        #    writers' data.
        fresh = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            fresh_adapter = FootballReadPort(fresh)
            fresh_scorers = [
                e["player"]
                for e in fresh_adapter.events_for_match(MATCH_ID)
                if e["type"] in goal_family_types
            ]
            assert fresh_scorers == ["Mikel Merino"]

            fresh_lineup = fresh_adapter.lineup_for_match(MATCH_ID)
            assert {row["participant"] for row in fresh_lineup} == {"Portugal", "Spain"}

            fresh_venue = fresh_adapter.venue_for_match(MATCH_ID)
            assert fresh_venue == {"stadium": "AT&T Stadium", "city": "Arlington"}

            commentary_rows = fresh.execute(
                "SELECT match_id, text FROM commentary WHERE match_id = ?", (MATCH_ID,)
            ).fetchall()
            assert [dict(row) for row in commentary_rows] == [
                {"match_id": MATCH_ID, "text": "Merino scores in stoppage time!"}
            ]

            recent = fresh_adapter.recent_commentary(MATCH_ID)
            assert len(recent) == 1
            assert recent[0]["text"] == "Merino scores in stoppage time!"
        finally:
            fresh.close()

    def test_collector_write_cycle_creates_no_gamealerts_pollution_tables(self, db_path):
        """Sibling assertion to Phase 2's pollution check, exercised at the
        end-to-end level: the collector's own write cycle never creates
        gamealerts' native table names."""
        conn = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            _seed_collector_facts(conn)
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            for polluted in ("stadiums", "stats", "rosters", "lineups"):
                assert polluted not in tables
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'commentary'"
            ).fetchone()
            assert count == 0, "the collector must never create the commentary table itself"
        finally:
            conn.close()
