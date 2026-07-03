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

import sqlite3
import urllib.parse
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
    "get_events_since",
    "get_standings",
    "get_entity",
    "find_entities_by_name",
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
