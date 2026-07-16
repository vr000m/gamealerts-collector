"""Phase 2: schema v1 shape, connection pragmas, fold, reader read-only.

Plan contract (docs/dev_plans/20260702-feature-collector-foundation.md §Phase 2):
- ``connect(path, *, side_table_ddl=())`` sets WAL / busy_timeout=5000 /
  foreign_keys=ON, applies schema v1 when missing, applies pack side-table
  DDL after core schema.
- Schema v1 core tables with pinned primary keys; no ``commentary`` table.
- Entity refs are soft (no FK constraints) in v1.
- ``gamecollect.fold`` is core-owned; write-fold == query-fold; folded
  lookup returns diacritic-named entities.
- ``gamecollect.db.reader`` issues no DML (asserted via sqlite authorizer).
"""

import sqlite3

import pytest
from _phase2_helpers import (
    ENTITY_LOOKUP_CANDIDATES,
    call_reader,
    columns_of,
    core_schema_sql,
    entity_row,
    event_row,
    fold_fn,
    match_row,
    pk_of,
    resolve_attr,
    row_values,
    standing_row,
    table_names,
    writer_method,
)

from gamecollect.db.connection import connect

CORE_TABLES = {"schema_meta", "matches", "events", "entities", "standings", "provider_match_map"}
SRC = "wc2026-espn"


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "phase2.db"


# --- pragmas -----------------------------------------------------------------


def test_pragmas_on_fresh_connection(db_path):
    conn = connect(db_path)
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    finally:
        conn.close()


def test_connect_creates_db_file_and_schema(db_path):
    assert not db_path.exists()
    conn = connect(db_path)
    conn.close()
    assert db_path.exists()
    assert CORE_TABLES <= table_names(db_path)


def test_reconnect_existing_db_is_stable(db_path):
    connect(db_path).close()
    before = core_schema_sql(db_path)
    connect(db_path).close()
    assert core_schema_sql(db_path) == before


# --- schema v1 shape -----------------------------------------------------------


def test_core_tables_present_and_no_commentary(db_path):
    connect(db_path).close()
    names = table_names(db_path)
    assert CORE_TABLES <= names
    assert "commentary" not in names


@pytest.mark.parametrize(
    ("table", "expected_pk"),
    [
        ("matches", ["match_id"]),
        ("events", ["source", "match_id", "seq"]),
        ("entities", ["source", "entity_id"]),
        ("standings", ["source", "group_key", "entity_id"]),
        ("provider_match_map", ["source", "provider", "provider_match_id"]),
    ],
)
def test_primary_keys(db_path, table, expected_pk):
    connect(db_path).close()
    assert pk_of(db_path, table) == expected_pk


def test_pinned_columns_and_not_null(db_path):
    connect(db_path).close()

    matches = columns_of(db_path, "matches")
    assert {"match_id", "source", "status", "kickoff_utc", "payload", "updated_at"} <= set(matches)
    assert matches["source"]["notnull"]

    events = columns_of(db_path, "events")
    assert {"source", "match_id", "seq", "type", "importance", "payload"} <= set(events)
    assert events["type"]["notnull"]

    entities = columns_of(db_path, "entities")
    expected = {"source", "entity_id", "kind", "display_name", "name_folded", "parent_entity"}
    assert expected | {"payload"} <= set(entities)

    standings = columns_of(db_path, "standings")
    assert {"source", "group_key", "entity_id", "payload"} <= set(standings)


def test_entity_refs_are_soft_no_foreign_keys(db_path):
    connect(db_path).close()
    with sqlite3.connect(db_path) as c:
        for table in ("matches", "events", "standings"):
            fks = c.execute(f"PRAGMA foreign_key_list({table})").fetchall()
            assert fks == [], f"schema v1 must not declare FK constraints on {table}: {fks}"


# --- pack side-table DDL --------------------------------------------------------


def test_side_table_ddl_applied_after_core_schema(db_path, tmp_path):
    ddl = (
        "CREATE TABLE IF NOT EXISTS pack_side_stats ("
        "source TEXT NOT NULL, match_id TEXT NOT NULL, payload TEXT, "
        "PRIMARY KEY (source, match_id))",
    )
    conn = connect(db_path, side_table_ddl=ddl)
    conn.close()
    assert "pack_side_stats" in table_names(db_path)

    # Core tables must be identical to a pack-less database.
    plain = tmp_path / "plain.db"
    connect(plain).close()
    assert core_schema_sql(db_path) == core_schema_sql(plain)


def test_side_table_ddl_reopen_is_idempotent(db_path):
    ddl = ("CREATE TABLE IF NOT EXISTS pack_side_stats (id TEXT PRIMARY KEY)",)
    connect(db_path, side_table_ddl=ddl).close()
    conn = connect(db_path, side_table_ddl=ddl)  # second open must not fail
    conn.close()
    assert "pack_side_stats" in table_names(db_path)


# --- fold ----------------------------------------------------------------------


def test_fold_strips_diacritics():
    fold = fold_fn()
    assert fold("Türkiye") == fold("Turkiye")
    assert fold("Côte d'Ivoire") == fold("Cote d'Ivoire")


def test_fold_is_idempotent_and_case_insensitive():
    fold = fold_fn()
    for name in ("Türkiye", "Côte d'Ivoire", "Qatar"):
        assert fold(fold(name)) == fold(name)
        assert fold(name) != ""
    assert fold("TÜRKIYE") == fold("türkiye")


def test_write_fold_equals_query_fold_in_db(db_path):
    """Stored name_folded must equal fold(display_name) — same fold both sides."""
    from gamecollect.db.writer import PartitionWriter

    fold = fold_fn()
    conn = connect(db_path)
    w = PartitionWriter(conn, SRC)
    writer_method(w, "entity")(entity_row("t-tur", "Türkiye"))
    writer_method(w, "entity")(entity_row("t-civ", "Côte d'Ivoire"))
    conn.commit()
    conn.close()

    with sqlite3.connect(db_path) as c:
        rows = dict(c.execute("SELECT display_name, name_folded FROM entities"))
    assert rows["Türkiye"] == fold("Türkiye")
    assert rows["Côte d'Ivoire"] == fold("Côte d'Ivoire")


def test_folded_lookup_returns_diacritic_entities(db_path):
    from gamecollect.db import reader
    from gamecollect.db.writer import PartitionWriter

    conn = connect(db_path)
    w = PartitionWriter(conn, SRC)
    writer_method(w, "entity")(entity_row("t-tur", "Türkiye"))
    writer_method(w, "entity")(entity_row("t-civ", "Côte d'Ivoire"))
    conn.commit()
    conn.close()

    lookup = resolve_attr(reader, ENTITY_LOOKUP_CANDIDATES, "entity folded-name lookup")
    ro = reader.open_reader(db_path)
    try:
        for query, display in (("Turkiye", "Türkiye"), ("Cote d'Ivoire", "Côte d'Ivoire")):
            result = call_reader(lookup, ro, db_path, query)
            rows = result if isinstance(result, list) else [result]
            assert rows, f"folded lookup for {query!r} returned nothing"
            flat = [v for r in rows for v in row_values(r)]
            assert display in flat, f"lookup {query!r} did not return {display!r}: {flat}"
    finally:
        ro.close()


# --- reader is DML-free ----------------------------------------------------------


_DENIED_ACTIONS = {
    sqlite3.SQLITE_INSERT,
    sqlite3.SQLITE_UPDATE,
    sqlite3.SQLITE_DELETE,
    sqlite3.SQLITE_CREATE_TABLE,
    sqlite3.SQLITE_CREATE_INDEX,
    sqlite3.SQLITE_ALTER_TABLE,
    sqlite3.SQLITE_DROP_TABLE,
    sqlite3.SQLITE_DROP_INDEX,
}


def test_reader_issues_no_dml(db_path):
    """Every reader entry point must work on a connection where DML is denied."""
    from gamecollect.db import reader
    from gamecollect.db.writer import PartitionWriter

    conn = connect(db_path)
    w = PartitionWriter(conn, SRC)
    writer_method(w, "match")(match_row("m1"))
    writer_method(w, "event")(event_row("m1", 1, type="goal"))
    writer_method(w, "event")(event_row("m1", 2, type="yellow_card"))
    writer_method(w, "entity")(entity_row("t1", "Qatar"))
    writer_method(w, "standing")(standing_row("A", "t1"))
    conn.commit()
    conn.close()

    ro = reader.open_reader(db_path)
    try:
        # The reader's own connection must be read-only at the sqlite level.
        with pytest.raises(sqlite3.OperationalError, match="(?i)read.?only|attempt to write"):
            ro.execute("INSERT INTO matches (match_id, source) VALUES ('mx', 'x')")

        # And no reader entry point may issue DML/DDL of any kind.
        def deny_dml(action, *_args):
            return sqlite3.SQLITE_DENY if action in _DENIED_ACTIONS else sqlite3.SQLITE_OK

        ro.set_authorizer(deny_dml)
        call_reader(reader.list_matches, ro, db_path)
        call_reader(reader.get_state, ro, db_path, "m1")
        events = call_reader(reader.get_events_since, ro, db_path, "m1", 0)
        assert len(list(events)) == 2
        call_reader(reader.get_standings, ro, db_path, SRC)
    finally:
        ro.close()

    # And the data is untouched.
    with sqlite3.connect(db_path) as c:
        assert c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2
        assert c.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 1


def test_reader_and_writer_agree_on_paths_with_uri_metachars(tmp_path):
    """connect() takes a plain path; open_reader builds a URI. A literal
    '%' or '#' in the path must not make them address different files."""
    from gamecollect.db import reader
    from gamecollect.db.connection import connect

    for weird in ("a%20b", "a#b"):
        d = tmp_path / weird
        d.mkdir()
        db_path = d / "t.db"
        connect(db_path).close()
        ro = reader.open_reader(db_path)
        try:
            assert ro.execute("SELECT major FROM schema_meta").fetchone() is not None
        finally:
            ro.close()


def test_folded_lookup_uses_index_without_source_or_kind(tmp_path):
    """idx_entities_folded must serve a name-only lookup (no full scan)."""
    from gamecollect.db.connection import connect

    conn = connect(tmp_path / "idx.db")
    try:
        plan = " ".join(
            row[3]
            for row in conn.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM entities WHERE name_folded = ?", ("x",)
            )
        )
        assert "idx_entities_folded" in plan, f"name-only lookup not indexed: {plan}"
    finally:
        conn.close()


def test_latest_event_of_type_query_uses_covering_index(tmp_path):
    """idx_events_match_type_seq (schema v1.1) must serve
    get_latest_event_of_type's actual query shape — a per-type subquery
    (``WHERE match_id = ? AND type = ? ORDER BY seq DESC LIMIT 1``) unioned
    across the caller's types, NOT a single ``type IN (...)`` predicate.

    A single ``IN (...)`` form was tried first but confirmed (via this same
    EXPLAIN QUERY PLAN check, against a populated + ANALYZE'd table) to make
    SQLite's planner fall back to the OLD (match_id, seq) index and scan
    every event row for the match — it cannot satisfy ORDER BY seq DESC
    across an IN-list from the composite index without an extra merge step.
    Each per-type subquery, by contrast, seeks the composite index directly
    on an equality (match_id, type) prefix."""
    from gamecollect.db import reader
    from gamecollect.db.connection import connect

    conn = connect(tmp_path / "idx_events.db")
    try:
        types = ("kickoff", "half_time", "full_time")
        subquery = (
            "SELECT * FROM (SELECT * FROM events WHERE match_id = ? AND type = ? "
            "ORDER BY seq DESC LIMIT 1)"
        )
        union = " UNION ALL ".join([subquery] * len(types))
        sql = f"SELECT * FROM ({union}) ORDER BY seq DESC LIMIT 1"
        params = []
        for t in types:
            params.extend(("m1", t))
        plan = " ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params))
        assert plan.count("idx_events_match_type_seq") == len(types), (
            f"expected one per-type index seek: {plan}"
        )
        assert "idx_events_match_seq" not in plan, f"fell back to the old scan-only index: {plan}"

        # Also pin that reader.get_latest_event_of_type itself issues this
        # exact query shape, not some other equivalent-looking form.
        statements: list[str] = []
        conn.set_trace_callback(statements.append)
        try:
            reader.get_latest_event_of_type(conn, "m1", types)
        finally:
            conn.set_trace_callback(None)
        assert any("UNION ALL" in s for s in statements), (
            f"get_latest_event_of_type did not issue the per-type UNION ALL query: {statements}"
        )
    finally:
        conn.close()
