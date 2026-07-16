"""Untyped low-level read API — the consumer-side view of the shared database.

Per DESIGN.md §3 the database is read-only to everyone but collector daemons:
:func:`open_reader` opens the SQLite file in read-only mode (``mode=ro`` URI +
``PRAGMA query_only=ON``), so no code path through this module can issue DML —
attempting a write on a reader connection raises ``sqlite3.OperationalError``.

Consumers refuse a major schema version differing from the library's, same
policy as ``connect()``: a newer major means the file was written by a newer
library whose layout this code does not understand; an older major has a
different table layout too, and only a daemon's ``connect()`` may migrate it.

Reader contract: ``match_id`` is globally unambiguous (writers source-qualify
unreconciled ids), so match-scoped reads take only ``match_id`` — no ``source``
parameter. ``get_events_since(match_id, seq)`` works regardless of which
daemon wrote the rows because ``seq`` is monotonic within its partition.

Rows are returned as plain dicts with ``payload`` left as raw JSON text —
typed views belong to the client library layered on top (interfaces plan).
"""

from __future__ import annotations

import json
import re
import sqlite3
import urllib.parse
from collections.abc import Collection
from pathlib import Path
from typing import Any

# The multi-daemon locking contract depends on reader and writer agreeing on
# the busy timeout, so the constant is defined once, in connection.py.
from gamecollect.db.connection import _BUSY_TIMEOUT_MS
from gamecollect.db.migrations import SCHEMA_MAJOR, SchemaVersionError, get_schema_version
from gamecollect.fold import fold

__all__ = [
    "open_reader",
    "list_matches",
    "get_state",
    "get_stored_payload",
    "get_events_since",
    "get_latest_event_of_type",
    "get_standings",
    "get_entity",
    "find_entities_by_name",
    "get_side_table_row",
    "get_side_table_rows",
    "get_team_side_table_rows",
    "get_recent_commentary",
]


def open_reader(path: str | Path) -> sqlite3.Connection:
    """Open an existing gamecollect database read-only and return a connection.

    Raises :class:`~gamecollect.db.migrations.SchemaVersionError` when the
    file's stamped major differs from this library's (see module docstring),
    and ``sqlite3.OperationalError`` when the file does not exist (read-only
    mode never creates a database).
    """
    db_path = Path(path)
    # Percent-escape the path: a literal '%', '?', or '#' in an unescaped URI
    # is decoded/misparsed by SQLite, making the reader open a DIFFERENT file
    # than connect() (which passes the plain path) would for the same input.
    escaped = urllib.parse.quote(str(db_path))
    conn = sqlite3.connect(f"file:{escaped}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        # Belt and braces on top of mode=ro: the connection itself refuses DML.
        conn.execute("PRAGMA query_only=ON")
        version = get_schema_version(conn)
        if version is None:
            raise SchemaVersionError(
                f"{db_path} has no schema_meta version stamp — not a gamecollect "
                "database (or created by something other than connect())"
            )
        if version[0] != SCHEMA_MAJOR:
            direction = "upgrade the library" if version[0] > SCHEMA_MAJOR else "migrate the file"
            raise SchemaVersionError(
                f"database schema is v{version[0]}.{version[1]} but this library "
                f"supports major v{SCHEMA_MAJOR}: {direction} to read this file"
            )
    except BaseException:
        conn.close()
        raise

    return conn


def _query(conn: sqlite3.Connection, sql: str, params: Any = ()) -> list[dict[str, Any]]:
    """Run a SELECT and return dict rows regardless of the connection's row_factory.

    Reader functions accept any open connection (not only ones from
    :func:`open_reader`), so column names come from ``cursor.description``
    instead of assuming ``sqlite3.Row``.
    """
    cursor = conn.execute(sql, params)
    names = [col[0] for col in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def list_matches(
    conn: sqlite3.Connection,
    *,
    source: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Return matches rows, optionally filtered by source and/or status."""
    clauses: list[str] = []
    params: list[Any] = []
    if source is not None:
        clauses.append("source = ?")
        params.append(source)
    if status is not None:
        clauses.append("status = ?")
        params.append(status)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return _query(conn, f"SELECT * FROM matches{where} ORDER BY kickoff_utc, match_id", params)


def get_state(conn: sqlite3.Connection, match_id: str) -> dict[str, Any] | None:
    """Return the current matches row for ``match_id``, or None if unknown."""
    rows = _query(conn, "SELECT * FROM matches WHERE match_id = ?", (match_id,))
    return rows[0] if rows else None


def decode_payload(raw: Any) -> dict[str, Any]:
    """Decode a stored ``matches.payload`` value to a dict.

    Returns ``{}`` when *raw* is empty, unparseable, or not a JSON object.
    Accepts either the raw JSON text (the DB column shape) or an
    already-decoded value, so callers holding a fetched row and callers
    re-reading by id share one decode-or-empty-dict rule (the drift-free
    single source for :func:`get_stored_payload`, the reconciler's
    candidate-name resolver, and the engine's stored-snapshot reader).
    """
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def get_stored_payload(conn: sqlite3.Connection, match_id: str) -> dict[str, Any]:
    """Decode the stored ``matches.payload`` JSON for ``match_id`` (``{}`` when
    the row is absent or the payload is empty/unparseable/not an object).

    The one deliberate exception to this module's raw-JSON-text rule: write
    paths (core ``default_seed_match``, pack reconcilers) must read-merge-write
    because ``upsert_match`` replaces ``payload`` wholesale, and they all need
    the identical decode-or-empty-dict semantics.
    """
    existing = get_state(conn, match_id)
    return decode_payload(existing["payload"]) if existing is not None else {}


def get_events_since(
    conn: sqlite3.Connection, match_id: str, seq: int = -1
) -> list[dict[str, Any]]:
    """Return events for ``match_id`` with seq strictly greater than ``seq``.

    Ordered by seq; the default returns the full event list.
    """
    return _query(
        conn,
        "SELECT * FROM events WHERE match_id = ? AND seq > ? ORDER BY seq",
        (match_id, seq),
    )


def get_latest_event_of_type(
    conn: sqlite3.Connection, match_id: str, types: Collection[str]
) -> dict[str, Any] | None:
    """Return the most recent event row for ``match_id`` whose ``type`` is in
    ``types`` (ordered by ``seq DESC``, one row), or ``None`` if there is none.

    Targeted alternative to decoding the full event history just to find one
    marker event (e.g. a football pack's last phase-marker event on every
    ``latest_state()`` poll tick) -- a per-poll hot path where
    ``get_events_since`` + full decode-and-scan does needless work every
    call. ``types`` is expected to be a small, caller-controlled set of
    literal type strings (not user input), so it is passed as bound
    parameters (safe) rather than interpolated.

    Issues one bounded per-type subquery (each an ``idx_events_match_type_seq
    (match_id, type, seq)`` index SEEK on ``match_id = ? AND type = ?``,
    touching only that type's own rows) unioned together, rather than a
    single ``type IN (...)`` predicate: SQLite's planner cannot satisfy
    ``ORDER BY seq DESC`` across an IN-list from that composite index (the
    per-type row groups are not globally seq-ordered without a merge step),
    so a single-query ``IN (...)`` plan falls back to the OLD
    ``idx_events_match_seq (match_id, seq)`` full per-match scan this index
    was added to avoid — confirmed via ``EXPLAIN QUERY PLAN`` against a
    populated, ``ANALYZE``'d table. The per-type subquery form lets each seek
    use the composite index directly; the outer query then picks the overall
    max-``seq`` row among the (at most ``len(types)``) candidates.
    """
    types = tuple(types)
    if not types:
        return None
    subquery = (
        "SELECT * FROM (SELECT * FROM events WHERE match_id = ? AND type = ? "
        "ORDER BY seq DESC LIMIT 1)"
    )
    params: list[Any] = []
    for t in types:
        params.extend((match_id, t))
    union = " UNION ALL ".join([subquery] * len(types))
    sql = f"SELECT * FROM ({union}) ORDER BY seq DESC LIMIT 1"
    rows = _query(conn, sql, params)
    return rows[0] if rows else None


def get_standings(
    conn: sqlite3.Connection, source: str, group_key: str | None = None
) -> list[dict[str, Any]]:
    """Return standings rows for ``source``, optionally scoped to one group."""
    if group_key is not None:
        return _query(
            conn,
            "SELECT * FROM standings WHERE source = ? AND group_key = ? ORDER BY rank, entity_id",
            (source, group_key),
        )
    return _query(
        conn,
        "SELECT * FROM standings WHERE source = ? ORDER BY group_key, rank, entity_id",
        (source,),
    )


def get_entity(conn: sqlite3.Connection, source: str, entity_id: str) -> dict[str, Any] | None:
    """Return one entities row by its (source, entity_id) key, or None."""
    rows = _query(
        conn,
        "SELECT * FROM entities WHERE source = ? AND entity_id = ?",
        (source, entity_id),
    )
    return rows[0] if rows else None


def find_entities_by_name(
    conn: sqlite3.Connection,
    name: str,
    *,
    source: str | None = None,
    kind: str | None = None,
) -> list[dict[str, Any]]:
    """Folded-name entity lookup (accent/case-insensitive, indexed equality).

    ``name`` is folded with the same core :func:`gamecollect.fold.fold` the
    writer stamps ``name_folded`` with, so query-fold == write-fold by
    construction — "Turkiye" finds the entity stored as "Türkiye".
    """
    clauses = ["name_folded = ?"]
    params: list[Any] = [fold(name)]
    if source is not None:
        clauses.append("source = ?")
        params.append(source)
    if kind is not None:
        clauses.append("kind = ?")
        params.append(kind)
    return _query(
        conn,
        f"SELECT * FROM entities WHERE {' AND '.join(clauses)} ORDER BY source, entity_id",
        params,
    )


# ---------------------------------------------------------------------------
# Generic side-table reads (table-name-parameterized helpers backing
# pack-owned read adapters, e.g. gamecollect_football's MatchReadPort
# adapter). Table/column names are supplied by the caller — core stays
# sport-agnostic; only the calling pack module knows its own table names.
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _assert_identifier_shaped(table: str) -> None:
    """Cheap defense-in-depth for the ``table`` params below.

    These are exported, generic core helpers that f-string-interpolate a
    caller-supplied table name into SQL (``# noqa: S608``). Every current
    caller passes a hardcoded literal, so this is unreachable today, but the
    functions are public API inviting future misuse. This is NOT a full
    allowlist against ``sqlite_master`` (that would add a query per call) --
    just a shape check that rejects anything that couldn't possibly be a bare
    SQL identifier (e.g. containing ``;``, whitespace, or quotes).
    """
    if not _IDENTIFIER_RE.match(table):
        raise ValueError(f"table must be a bare SQL identifier, got {table!r}")


def get_side_table_row(
    conn: sqlite3.Connection, table: str, source: str, match_id: str
) -> dict[str, Any] | None:
    """Return one ``(source, match_id)``-keyed row from a pack's side table.

    For per-match, single-row side tables (e.g. a venue table). Returns None
    for an unrecorded match rather than raising.
    """
    _assert_identifier_shaped(table)
    rows = _query(
        conn,
        f"SELECT * FROM {table} WHERE source = ? AND match_id = ?",  # noqa: S608
        (source, match_id),
    )
    return rows[0] if rows else None


def get_side_table_rows(
    conn: sqlite3.Connection,
    table: str,
    source: str,
    match_id: str,
    *,
    order_by: str | None = None,
) -> list[dict[str, Any]]:
    """Return all ``(source, match_id)``-keyed rows from a pack's side table.

    For per-match, multi-row side tables (e.g. a lineup table). Empty list
    for an unrecorded match rather than raising.
    """
    _assert_identifier_shaped(table)
    sql = f"SELECT * FROM {table} WHERE source = ? AND match_id = ?"  # noqa: S608
    if order_by:
        sql += f" ORDER BY {order_by}"
    return _query(conn, sql, (source, match_id))


def get_team_side_table_rows(
    conn: sqlite3.Connection, table: str, team: str, *, order_by: str | None = None
) -> list[dict[str, Any]]:
    """Return all ``team``-keyed rows from a pack's side table.

    For team-level (not per-match) side tables (e.g. a static squad roster).
    Empty list when the team has no recorded rows.
    """
    _assert_identifier_shaped(table)
    sql = f"SELECT * FROM {table} WHERE team = ?"  # noqa: S608
    if order_by:
        sql += f" ORDER BY {order_by}"
    return _query(conn, sql, (team,))


def get_all_team_side_table_rows(
    conn: sqlite3.Connection, table: str, *, order_by: str | None = None
) -> list[dict[str, Any]]:
    """Return every row from a pack's team-keyed side table, unfiltered.

    Companion to :func:`get_team_side_table_rows` for callers that need to
    match ``team`` through a fold-based comparison (casefold + diacritic
    strip, not just an exact string) rather than an exact SQL equality — the
    caller fetches the full (small, team-level) table and filters in Python.
    Empty list for an empty table.
    """
    _assert_identifier_shaped(table)
    sql = f"SELECT * FROM {table}"  # noqa: S608
    if order_by:
        sql += f" ORDER BY {order_by}"
    return _query(conn, sql)


def get_recent_commentary(
    conn: sqlite3.Connection, match_id: str, limit: int = 20
) -> list[dict[str, Any]]:
    """Return the most recent ``commentary`` rows for ``match_id``, newest first.

    The collector never writes commentary (DESIGN.md §3) — the table is
    created lazily by the consuming app on its own scoped connection, so a
    collector-first boot / pre-match / replay may have no such table yet.
    Tolerated here: returns ``[]`` when the table is absent, instead of
    letting ``sqlite3.OperationalError`` escape. This checks
    ``sqlite_master`` directly rather than string-matching the exception
    message (fragile across SQLite versions) — a genuine column-shape
    mismatch on a table that DOES exist is a real bug in the consumer's
    schema and is allowed to raise, since that is not the documented
    tolerance here.
    """
    exists = _query(
        conn,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'commentary'",
    )
    if not exists:
        return []
    return _query(
        conn,
        "SELECT * FROM commentary WHERE match_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
        (match_id, limit),
    )
