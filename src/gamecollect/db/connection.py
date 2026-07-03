"""SQLite connection factory for collector daemons (read-write path).

Every connection (pragma pattern ported from the gamealerts ``store/db.py``):

* WAL journal mode — one writer, many readers; no read/write blocking.
* ``busy_timeout=5000`` ms — concurrent writers wait rather than immediately
  raising ``OperationalError("database is locked")``. WAL serializes writers
  file-wide; partition-per-source does NOT grant concurrent writes, so
  multi-daemon safety depends on this timeout absorbing ``SQLITE_BUSY``.
* ``foreign_keys=ON`` — enforced for any FK a pack side table declares (core
  v1 entity refs are soft, with no FK constraints).

Pragmas are contract: they are set HERE, by the core library, never by
individual packs (DESIGN.md §3, first-writer-wins rules live in one place).

Consumers (readers) must use :mod:`gamecollect.db.reader` instead — the
database is read-only to everyone but collector daemons.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path

from gamecollect.db.migrations import SchemaVersionError, ensure_schema

__all__ = ["connect", "SchemaVersionError"]

_BUSY_TIMEOUT_MS = 5000


def connect(path: str | Path, *, side_table_ddl: Iterable[str] = ()) -> sqlite3.Connection:
    """Open (or create) a gamecollect database and return a connection.

    Applies the core schema when missing, runs additive migrations when the
    file is at an older minor, and refuses (raising
    :class:`~gamecollect.db.migrations.SchemaVersionError`) when the file's
    ``schema_meta`` major is newer than this library's.

    ``side_table_ddl`` is the pack-owned additive DDL hook (DESIGN.md §4):
    each entry is an SQL script executed AFTER core schema/migrations, in this
    same connection-setup path. Pack DDL must be additive and idempotent
    (``CREATE TABLE IF NOT EXISTS`` etc.) and must never alter core tables.
    """
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    try:
        # WAL mode — must be set before any DML.
        conn.execute("PRAGMA journal_mode=WAL")
        # Busy timeout — wait up to N ms before raising SQLITE_BUSY.
        conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        # Foreign key enforcement.
        conn.execute("PRAGMA foreign_keys=ON")
        ensure_schema(conn)
        for ddl in side_table_ddl:
            conn.executescript(ddl)
        conn.commit()
    except BaseException:
        conn.close()
        raise

    return conn
