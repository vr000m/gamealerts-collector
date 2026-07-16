"""Phase 2: commentary-hosting tolerance.

Plan (docs/dev_plans/20260707-feature-gameworker-integration.md, Phase 2 +
Architecture Decisions "Commentary is gamealerts' ONLY owned table"):

- gamealerts owns exactly one table (``commentary``) in the collector's file
  and applies ONLY its own scoped ``CREATE TABLE IF NOT EXISTS`` DDL (never
  the collector's ``side_table_ddl`` seam, never a full gamealerts
  ``schema.sql``).
- The collector's ``schema_meta`` version gate keys only on the version row
  (``migrations.py:106-116``) and must tolerate the foreign ``commentary``
  table sitting alongside the collector schema.
- The collector never writes commentary, and its own reader/writers must
  ignore the table entirely.
- After a full collector write cycle, the file must contain NONE of
  gamealerts' OWN native pollution table names (``stadiums``/``stats``/
  ``rosters``/``lineups`` — distinct from the collector pack's own
  ``football_stats``/``football_lineups`` side tables) and zero rows in
  ``commentary`` if the table happens to exist.

This test applies the ``commentary`` DDL directly as a raw string (no import
from a gamealerts package — it does not exist in this repo).
"""

from __future__ import annotations

import sqlite3

import pytest

from gamecollect.db import reader
from gamecollect.db.connection import connect
from gamecollect.db.migrations import SCHEMA_MAJOR, get_schema_version
from gamecollect.db.writer import PartitionWriter
from gamecollect.provider import MatchStatus, NormalizedMatch
from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL, persist_football_side_tables

SOURCE = "wc2026"
MATCH_ID = "wc2026:760421"

# gamealerts' OWN scoped commentary DDL (mirrors its documented FK:
# match_id -> matches(match_id), applied through its OWN connection, never
# the collector's side_table_ddl seam). Deliberately minimal/opaque to the
# collector: this repo has no gamealerts package to import it from.
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

# The four native gamealerts tables its full schema.sql would create, whose
# names collide with (or pollute alongside) the collector's own tables. None
# of these must ever appear in the collector's file — the collector's own
# side tables are football_stats/football_lineups, not these bare names.
GAMEALERTS_POLLUTION_TABLES = ("stadiums", "stats", "rosters", "lineups")


def _match(match_id: str = "760421") -> NormalizedMatch:
    return NormalizedMatch(
        match_id=match_id,
        status=MatchStatus.IN_PLAY,
        minute=10,
        score_home=0,
        score_away=0,
        display_clock="10'",
        home_team="Morocco",
        away_team="Haiti",
        kickoff_utc="2026-06-14T04:00:00+00:00",
        payload={"stats": [{"team": "Morocco", "shots": 1}]},
    )


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {r[0] for r in rows}


def _seed_match(conn: sqlite3.Connection, match: NormalizedMatch, match_id: str) -> PartitionWriter:
    """Run a real collector write: upsert the matches row + football side
    tables, mirroring the engine's per-poll write path (persist_side_tables
    following upsert_match), so pollution assertions exercise the actual
    write cycle rather than a synthetic row."""
    writer = PartitionWriter(conn, SOURCE)
    status = match.status.value if hasattr(match.status, "value") else str(match.status)
    writer.upsert_match(
        {
            "match_id": match_id,
            "source": SOURCE,
            "status": status,
            "kickoff_utc": match.kickoff_utc,
            "score_home": match.score_home,
            "score_away": match.score_away,
            "display_clock": match.display_clock,
            "payload": match.payload,
        }
    )
    persist_football_side_tables(conn, writer, match, match_id)
    conn.commit()
    return writer


@pytest.fixture
def collector_db(tmp_path):
    """A collector-created + stamped file (startup-ordering precondition)."""
    path = tmp_path / "shared.db"
    conn = connect(path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
    yield path, conn
    conn.close()


class TestCommentaryDoesNotTripSchemaMetaGate:
    def test_commentary_table_coexists_with_collector_schema(self, collector_db):
        _path, conn = collector_db
        conn.executescript(GAMEALERTS_COMMENTARY_DDL)
        conn.commit()

        assert "commentary" in _table_names(conn)
        assert "matches" in _table_names(conn)

    def test_schema_meta_gate_still_reports_collector_version(self, collector_db):
        _path, conn = collector_db
        version_before = get_schema_version(conn)
        conn.executescript(GAMEALERTS_COMMENTARY_DDL)
        conn.commit()

        assert get_schema_version(conn) == version_before
        assert version_before is not None and version_before[0] == SCHEMA_MAJOR

    def test_connect_reopens_fine_with_commentary_table_present(self, collector_db):
        """connect() must not raise / re-migrate / choke because a foreign
        table exists — the gate keys only on the schema_meta row."""
        path, conn = collector_db
        conn.executescript(GAMEALERTS_COMMENTARY_DDL)
        conn.commit()
        conn.close()

        reopened = connect(path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            assert "commentary" in _table_names(reopened)
            assert get_schema_version(reopened)[0] == SCHEMA_MAJOR
        finally:
            reopened.close()

    def test_open_reader_ignores_commentary_table(self, collector_db):
        """The read-only consumer path (open_reader) must open fine and
        never surface the commentary table through any collector read
        helper."""
        path, conn = collector_db
        conn.executescript(GAMEALERTS_COMMENTARY_DDL)
        conn.commit()
        conn.close()

        ro = reader.open_reader(path)
        try:
            # The collector's reader surface exposes no commentary accessor;
            # its documented helpers must run clean with the table present.
            assert reader.list_matches(ro) == []
            assert reader.get_state(ro, MATCH_ID) is None
            assert not hasattr(reader, "get_commentary")
            assert not hasattr(reader, "recent_commentary")
        finally:
            ro.close()


class TestCollectorWriteCycleLeavesNoPollution:
    def test_full_write_cycle_creates_no_gamealerts_pollution_tables(self, collector_db):
        _path, conn = collector_db
        _seed_match(conn, _match(), MATCH_ID)

        tables = _table_names(conn)
        for polluted in GAMEALERTS_POLLUTION_TABLES:
            assert polluted not in tables, (
                f"collector write cycle must never create gamealerts' own '{polluted}' table"
            )

    def test_collector_never_writes_commentary_rows(self, collector_db):
        _path, conn = collector_db
        conn.executescript(GAMEALERTS_COMMENTARY_DDL)
        conn.commit()

        _seed_match(conn, _match(), MATCH_ID)

        (count,) = conn.execute("SELECT COUNT(*) FROM commentary").fetchone()
        assert count == 0, "the collector must write zero commentary rows"

    def test_commentary_fk_is_satisfiable_against_collector_matches_row(self, collector_db):
        """Sanity check on the documented FK: gamealerts can only insert
        commentary for a match_id the collector has already persisted —
        confirms the startup/write ordering constraint the plan documents,
        not a collector obligation to write commentary itself."""
        _path, conn = collector_db
        conn.executescript(GAMEALERTS_COMMENTARY_DDL)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.commit()

        _seed_match(conn, _match(), MATCH_ID)

        # A commentary insert against an EXISTING match_id succeeds.
        conn.execute(
            "INSERT INTO commentary (match_id, text, created_at) VALUES (?, ?, ?)",
            (MATCH_ID, "Kickoff!", "2026-06-14T04:00:00+00:00"),
        )
        conn.commit()
        (count,) = conn.execute("SELECT COUNT(*) FROM commentary").fetchone()
        assert count == 1

        # A commentary insert against an UNKNOWN match_id violates the FK.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO commentary (match_id, text, created_at) VALUES (?, ?, ?)",
                ("wc2026:no-such-match", "Ghost line", "2026-06-14T04:00:00+00:00"),
            )
            conn.commit()


class TestGetRecentCommentaryToleranceBoundary:
    """get_recent_commentary's docstring promises tolerating an ABSENT
    commentary table (returns [] instead of raising) since gamealerts creates
    it lazily. It does NOT promise tolerating a column-shape mismatch on a
    table that DOES exist -- that is a real bug in the consumer's schema and
    must still raise, so it doesn't silently degrade into a "no commentary"
    read. This asserts both sides of that documented boundary using a direct
    sqlite_master existence check (not exception-message string matching,
    which is fragile across SQLite versions)."""

    def test_absent_table_returns_empty_list(self, collector_db):
        _path, conn = collector_db
        # No commentary DDL applied at all.
        assert reader.get_recent_commentary(conn, MATCH_ID) == []

    def test_existing_table_missing_expected_column_still_raises(self, collector_db):
        _path, conn = collector_db
        # A commentary table that exists but lacks the documented
        # created_at/id columns get_recent_commentary's ORDER BY relies on.
        conn.executescript("CREATE TABLE commentary (match_id TEXT NOT NULL, text TEXT NOT NULL);")
        conn.commit()

        with pytest.raises(sqlite3.OperationalError):
            reader.get_recent_commentary(conn, MATCH_ID)


class TestSideTableHelpersRejectNonIdentifierTable:
    """get_side_table_row/get_side_table_rows/get_team_side_table_rows
    f-string-interpolate a caller-supplied ``table`` name into SQL (currently
    unreachable -- every caller passes a hardcoded literal -- but they are
    exported, generic core API). Defense-in-depth: reject anything that isn't
    shaped like a bare SQL identifier before it reaches the query."""

    @pytest.mark.parametrize("garbage_table", ["foo; DROP TABLE matches", "foo bar", ""])
    def test_get_side_table_row_rejects_garbage_table(self, collector_db, garbage_table):
        _path, conn = collector_db
        with pytest.raises(ValueError):
            reader.get_side_table_row(conn, garbage_table, SOURCE, MATCH_ID)

    @pytest.mark.parametrize("garbage_table", ["foo; DROP TABLE matches", "foo bar", ""])
    def test_get_side_table_rows_rejects_garbage_table(self, collector_db, garbage_table):
        _path, conn = collector_db
        with pytest.raises(ValueError):
            reader.get_side_table_rows(conn, garbage_table, SOURCE, MATCH_ID)

    @pytest.mark.parametrize("garbage_table", ["foo; DROP TABLE matches", "foo bar", ""])
    def test_get_team_side_table_rows_rejects_garbage_table(self, collector_db, garbage_table):
        _path, conn = collector_db
        with pytest.raises(ValueError):
            reader.get_team_side_table_rows(conn, garbage_table, "Australia")


class TestSideTableHelpersRejectGarbageOrderBy:
    """``order_by`` is also f-string-interpolated (currently unreachable --
    every caller passes a hardcoded literal) but, unlike ``table``, cannot use
    a bare-identifier regex: real callers pass multi-column clauses with
    commas and ``IS NULL`` (see ``gamecollect_football.readport``). This
    checks both that garbage is rejected and that the real call sites'
    ``order_by`` values still pass.
    """

    @pytest.mark.parametrize(
        "garbage_order_by",
        [
            "number; DROP TABLE matches",
            "number -- comment",
            "number /* comment */",
            "number); DELETE FROM matches WHERE (1=1",
            "UPDATE matches SET status = 'x'",
        ],
    )
    def test_get_side_table_rows_rejects_garbage_order_by(self, collector_db, garbage_order_by):
        _path, conn = collector_db
        with pytest.raises(ValueError):
            reader.get_side_table_rows(
                conn, "football_lineups", SOURCE, MATCH_ID, order_by=garbage_order_by
            )

    @pytest.mark.parametrize(
        "garbage_order_by",
        [
            "number; DROP TABLE matches",
            "number -- comment",
            "number /* comment */",
        ],
    )
    def test_get_team_side_table_rows_rejects_garbage_order_by(
        self, collector_db, garbage_order_by
    ):
        _path, conn = collector_db
        with pytest.raises(ValueError):
            reader.get_team_side_table_rows(
                conn, "football_roster", "Australia", order_by=garbage_order_by
            )

    @pytest.mark.parametrize(
        "garbage_order_by",
        [
            "number; DROP TABLE matches",
            "number -- comment",
        ],
    )
    def test_get_all_team_side_table_rows_rejects_garbage_order_by(
        self, collector_db, garbage_order_by
    ):
        _path, conn = collector_db
        with pytest.raises(ValueError):
            reader.get_all_team_side_table_rows(conn, "football_roster", order_by=garbage_order_by)

    def test_real_lineups_order_by_passes(self, collector_db):
        """The order_by used by gamecollect_football.readport for lineups
        must not be rejected by the new validation."""
        _path, conn = collector_db
        assert (
            reader.get_side_table_rows(
                conn,
                "football_lineups",
                SOURCE,
                MATCH_ID,
                order_by="team, formation_place IS NULL, formation_place, athlete_id",
            )
            == []
        )

    def test_real_roster_order_by_passes(self, collector_db):
        """The order_by used by gamecollect_football.readport for roster
        must not be rejected by the new validation."""
        _path, conn = collector_db
        assert (
            reader.get_team_side_table_rows(conn, "football_roster", "Australia", order_by="number")
            == []
        )
