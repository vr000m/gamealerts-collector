"""persist_football_side_tables: change-gated writes + malformed-value hardening.

Review findings (2026-07): the hook used to DELETE+reinsert every stats/lineup
row on EVERY applied diff (every clock tick) even when the payload sections
were unchanged — and a non-scalar payload value (ESPN emitting
``{"possession": {"pct": 55}}``) raised ``sqlite3.InterfaceError`` out of the
hook and killed the daemon. These tests pin the fixed behaviour: unchanged
sections perform no writes at all; malformed values are coerced/skipped.
"""

from __future__ import annotations

import pytest

from gamecollect.db.connection import connect
from gamecollect.db.writer import PartitionWriter
from gamecollect.provider import MatchStatus, NormalizedMatch
from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL, persist_football_side_tables

SOURCE = "wc2026"
MATCH_ID = "wc2026:760421"


def _match(payload: dict) -> NormalizedMatch:
    return NormalizedMatch(
        match_id="760421",
        status=MatchStatus.IN_PLAY,
        minute=27,
        score_home=1,
        score_away=0,
        display_clock="27'",
        home_team="Morocco",
        away_team="Haiti",
        kickoff_utc="2026-06-14T04:00:00+00:00",
        payload=payload,
    )


PAYLOAD = {
    "stats": [
        {"team": "Morocco", "possession": 55.0, "shots": 7, "corners": 3},
        {"team": "Haiti", "possession": 45.0, "shots": 2, "corners": 1},
    ],
    "lineups": [
        {
            "team": "Morocco",
            "home_away": "home",
            "formation": "4-3-3",
            "players": [
                {
                    "athlete_id": "a1",
                    "display_name": "Achraf Hakimi",
                    "jersey": "2",
                    "position": "D",
                    "starter": True,
                }
            ],
        }
    ],
}


@pytest.fixture
def db(tmp_path):
    conn = connect(tmp_path / "side.db", side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
    writer = PartitionWriter(conn, SOURCE)
    yield conn, writer
    conn.close()


def _stats_rows(conn):
    return conn.execute(
        "SELECT team, possession, shots, corners FROM football_stats ORDER BY team"
    ).fetchall()


def _lineup_rows(conn):
    return conn.execute(
        "SELECT team, athlete_id, display_name, name_folded, jersey, starter "
        "FROM football_lineups ORDER BY athlete_id"
    ).fetchall()


class TestChangeGatedWrites:
    def test_unchanged_payload_second_call_performs_no_writes(self, db):
        conn, writer = db
        persist_football_side_tables(conn, writer, _match(PAYLOAD), MATCH_ID)
        rows_before = (_stats_rows(conn), _lineup_rows(conn))

        changes_before = conn.total_changes
        # Same sections again (a fresh equal dict, not the same object).
        persist_football_side_tables(
            conn, writer, _match({k: list(v) for k, v in PAYLOAD.items()}), MATCH_ID
        )
        assert conn.total_changes == changes_before, (
            "an unchanged stats/lineups payload must not DELETE+reinsert the rows"
        )
        assert (_stats_rows(conn), _lineup_rows(conn)) == rows_before

    def test_changed_stats_rewrite_and_removed_team_disappears(self, db):
        conn, writer = db
        persist_football_side_tables(conn, writer, _match(PAYLOAD), MATCH_ID)

        changed = dict(PAYLOAD)
        changed["stats"] = [{"team": "Morocco", "possession": 61.0, "shots": 9, "corners": 4}]
        persist_football_side_tables(conn, writer, _match(changed), MATCH_ID)

        assert [tuple(r) for r in _stats_rows(conn)] == [("Morocco", 61.0, 9, 4)], (
            "a changed section must rewrite; DELETE+reinsert removes dropped teams"
        )

    def test_absent_sections_leave_tables_untouched(self, db):
        conn, writer = db
        persist_football_side_tables(conn, writer, _match(PAYLOAD), MATCH_ID)
        persist_football_side_tables(conn, writer, _match({}), MATCH_ID)
        assert len(_stats_rows(conn)) == 2
        assert len(_lineup_rows(conn)) == 1

    def test_duplicate_team_entries_collapse_last_wins(self, db):
        """The old ON CONFLICT clauses were dead code behind the DELETE; the
        projection keys rows by the table PK so an in-payload duplicate still
        collapses last-wins instead of raising on the plain INSERT."""
        conn, writer = db
        payload = {
            "stats": [
                {"team": "Morocco", "shots": 1},
                {"team": "Morocco", "shots": 8},
            ]
        }
        persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)
        assert [tuple(r) for r in _stats_rows(conn)] == [("Morocco", None, 8, None)]


class TestMalformedValueHardening:
    def test_nested_dict_values_are_coerced_not_raised(self, db):
        conn, writer = db
        payload = {
            "stats": [
                {"team": "Morocco", "possession": {"pct": 55}, "shots": "7"},
                {"team": {"name": "Haiti"}, "shots": 2},  # non-scalar team: skipped
                "not-a-dict",
            ],
            "lineups": [
                {
                    "team": "Morocco",
                    "home_away": {"side": "home"},
                    "players": [
                        {"athlete_id": "a1", "display_name": "Hakimi", "jersey": {"n": 2}},
                        {"athlete_id": {"id": 9}, "display_name": "Skipped"},
                        {"athlete_id": "a2", "display_name": {"nm": "x"}},
                    ],
                }
            ],
        }
        persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)  # must not raise

        assert [tuple(r) for r in _stats_rows(conn)] == [("Morocco", None, 7, None)]
        lineup = conn.execute(
            "SELECT display_name, jersey, home_away FROM football_lineups"
        ).fetchall()
        assert [tuple(r) for r in lineup] == [("Hakimi", None, None)]

    def test_numeric_scalars_coerce_to_text_columns(self, db):
        conn, writer = db
        payload = {
            "lineups": [
                {
                    "team": "Morocco",
                    "formation": 433,  # numeric scalar → stored as text
                    "players": [{"athlete_id": 7, "display_name": "Ziyech", "jersey": 7}],
                }
            ]
        }
        persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)
        row = conn.execute("SELECT athlete_id, jersey, formation FROM football_lineups").fetchone()
        assert tuple(row) == ("7", "7", "433")


def test_helper_replace_uses_partition_and_match_scope(db):
    """Partition discipline: rows of another (source, match) are untouched by a
    rewrite for this one."""
    conn, writer = db
    other_writer = PartitionWriter(conn, "eur2028")
    persist_football_side_tables(conn, other_writer, _match(PAYLOAD), "eur2028:1")
    persist_football_side_tables(conn, writer, _match(PAYLOAD), MATCH_ID)

    changed = dict(PAYLOAD)
    changed["stats"] = []
    persist_football_side_tables(conn, writer, _match(changed), MATCH_ID)

    counts = dict(
        conn.execute("SELECT source, COUNT(*) FROM football_stats GROUP BY source").fetchall()
    )
    assert counts.get("eur2028") == 2
    assert counts.get(SOURCE) is None


def test_numeric_athlete_id_and_team_are_coerced_not_dropped(db):
    """Scalar-but-not-str identifiers coerce to TEXT (review: narrowed filters
    silently dropped rows the old hook stored). Integral floats CANONICALIZE
    to their int string (review: ``12345.0`` used to mint the key "12345.0",
    duplicating the player against a later "12345")."""
    conn, writer = db
    payload = {
        "lineups": [
            {
                "team": "Canada",
                "players": [
                    {"athlete_id": 12345.0, "display_name": "F. Point"},
                    {"athlete_id": 67890, "display_name": "I. Nteger"},
                ],
            }
        ],
        "stats": [{"team": 42, "shots": 3}],
    }
    persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)
    ids = {
        row[0]
        for row in conn.execute(
            "SELECT athlete_id FROM football_lineups WHERE match_id = ?", (MATCH_ID,)
        )
    }
    assert ids == {"12345", "67890"}
    teams = {
        row[0]
        for row in conn.execute("SELECT team FROM football_stats WHERE match_id = ?", (MATCH_ID,))
    }
    assert teams == {"42"}


def test_integral_float_and_string_athlete_id_collapse_to_one_row(db):
    """The same athlete spelled as JSON float ``760421.0`` and string
    ``"760421"`` must collapse to ONE primary-key row (last-wins), not two."""
    conn, writer = db
    payload = {
        "lineups": [
            {
                "team": "Canada",
                "players": [
                    {"athlete_id": 760421.0, "display_name": "First Spelling"},
                    {"athlete_id": "760421", "display_name": "Second Spelling"},
                ],
            }
        ]
    }
    persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)
    rows = conn.execute(
        "SELECT athlete_id, display_name FROM football_lineups WHERE match_id = ?", (MATCH_ID,)
    ).fetchall()
    assert [tuple(r) for r in rows] == [("760421", "Second Spelling")], (
        "float and string spellings of one athlete id must collapse to one row, last-wins"
    )
    # A NON-integral float keeps its str() form (no information loss).
    payload2 = {
        "lineups": [{"team": "Canada", "players": [{"athlete_id": 7.5, "display_name": "Odd Id"}]}]
    }
    persist_football_side_tables(conn, writer, _match(payload2), MATCH_ID)
    ids = {
        row[0]
        for row in conn.execute(
            "SELECT athlete_id FROM football_lineups WHERE match_id = ?", (MATCH_ID,)
        )
    }
    assert "7.5" in ids


def test_bool_identifier_values_are_rejected_not_stringified(db):
    """A bool team/athlete_id (untrusted JSON true/false) must be REJECTED —
    ``str(True)`` would mint a literal "True" primary-key row."""
    conn, writer = db
    payload = {
        "stats": [{"team": True, "shots": 3}],
        "lineups": [
            {
                "team": False,
                "players": [{"athlete_id": "a1", "display_name": "Ghost Team"}],
            },
            {
                "team": "Canada",
                "players": [{"athlete_id": True, "display_name": "Ghost Player"}],
            },
        ],
    }
    persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)
    (stats_count,) = conn.execute(
        "SELECT COUNT(*) FROM football_stats WHERE match_id = ?", (MATCH_ID,)
    ).fetchone()
    assert stats_count == 0, "a bool team must not become a 'True' stats row"
    (lineup_count,) = conn.execute(
        "SELECT COUNT(*) FROM football_lineups WHERE match_id = ?", (MATCH_ID,)
    ).fetchone()
    assert lineup_count == 0, "bool team/athlete_id must not mint 'True'/'False' PK rows"


def test_string_float_spelling_collapses_with_float_and_bare_string(db):
    """Review finding: ALL three spellings of one athlete id — float
    ``760421.0``, string ``"760421.0"``, and string ``"760421"`` — must
    collapse to ONE primary-key row (last-wins), not two or three."""
    conn, writer = db
    payload = {
        "lineups": [
            {
                "team": "Canada",
                "players": [
                    {"athlete_id": 760421.0, "display_name": "Float Spelling"},
                    {"athlete_id": "760421.0", "display_name": "String Float Spelling"},
                    {"athlete_id": "760421", "display_name": "Bare String Spelling"},
                ],
            }
        ]
    }
    persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)
    rows = conn.execute(
        "SELECT athlete_id, display_name FROM football_lineups WHERE match_id = ?", (MATCH_ID,)
    ).fetchall()
    assert [tuple(r) for r in rows] == [("760421", "Bare String Spelling")], (
        "float, string-float, and bare-string spellings must collapse to one row, last-wins"
    )


def test_leading_zero_id_string_is_not_reparsed(db):
    """A non-float-looking id string like ``"01"`` is an opaque identifier —
    canonicalization must NOT strip its leading zero."""
    conn, writer = db
    payload = {
        "lineups": [{"team": "Canada", "players": [{"athlete_id": "01", "display_name": "Keeper"}]}]
    }
    persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)
    ids = {
        row[0]
        for row in conn.execute(
            "SELECT athlete_id FROM football_lineups WHERE match_id = ?", (MATCH_ID,)
        )
    }
    assert ids == {"01"}, "a plain digit string must keep its leading zero"


def test_non_finite_float_identifiers_are_rejected(db):
    """NaN/inf floats must not mint literal 'nan'/'inf' PK keys."""
    conn, writer = db
    payload = {
        "stats": [{"team": float("nan"), "shots": 3}],
        "lineups": [
            {
                "team": "Canada",
                "players": [
                    {"athlete_id": float("inf"), "display_name": "Ghost"},
                    {"athlete_id": float("-inf"), "display_name": "Ghost Two"},
                    {"athlete_id": float("nan"), "display_name": "Ghost Three"},
                ],
            }
        ],
    }
    persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)
    (stats_count,) = conn.execute(
        "SELECT COUNT(*) FROM football_stats WHERE match_id = ?", (MATCH_ID,)
    ).fetchone()
    (lineup_count,) = conn.execute(
        "SELECT COUNT(*) FROM football_lineups WHERE match_id = ?", (MATCH_ID,)
    ).fetchone()
    assert stats_count == 0 and lineup_count == 0, (
        "non-finite floats must never become 'nan'/'inf' primary-key rows"
    )


def test_skipped_rows_and_players_log_warnings(db, caplog):
    """Review finding: silently dropping a whole stats row / lineup (invalid
    team key) or a player (invalid athlete_id) hides provider garbage — each
    skip must log a WARNING naming the match and the offending value."""
    import logging

    conn, writer = db
    payload = {
        "stats": [{"team": True, "shots": 3}],
        "lineups": [
            {
                "team": False,
                "players": [{"athlete_id": "a1", "display_name": "Ghost Team"}],
            },
            {
                "team": "Canada",
                "players": [{"athlete_id": True, "display_name": "Ghost Player"}],
            },
        ],
    }
    with caplog.at_level(logging.WARNING, logger="gamecollect_football.pack"):
        persist_football_side_tables(conn, writer, _match(payload), MATCH_ID)

    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("football_stats" in m and MATCH_ID in m and "True" in m for m in messages), (
        "skipping a stats row must WARN with the match id and offending team value"
    )
    assert any("football_lineups" in m and MATCH_ID in m and "False" in m for m in messages), (
        "skipping an entire lineup must WARN with the match id and offending team value"
    )
    assert any("athlete_id" in m and MATCH_ID in m and "True" in m for m in messages), (
        "skipping a player must WARN with the match id and offending athlete_id"
    )
