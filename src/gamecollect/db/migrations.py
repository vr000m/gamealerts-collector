"""Schema versioning and ordered additive migrations.

Version policy (docs/DESIGN.md §3, pinned by the foundation plan):

* ``schema_meta.major > SCHEMA_MAJOR``  → refuse to open (raise
  :class:`SchemaVersionError`). A newer major means the file was written by a
  newer library whose schema this code does not understand.
* ``schema_meta.major == SCHEMA_MAJOR`` and ``minor > SCHEMA_MINOR`` → open OK.
  Minor bumps are additive-only, so an older library reads a newer-minor file
  safely (it just ignores columns/tables it does not know).
* ``schema_meta.major == SCHEMA_MAJOR`` and ``minor < SCHEMA_MINOR`` → run the
  ordered additive migrations up to ``(SCHEMA_MAJOR, SCHEMA_MINOR)`` and stamp
  the new version.
* ``schema_meta.major < SCHEMA_MAJOR`` → refuse. Major bumps are by definition
  not additive; there is no in-place upgrade path.

Migrations are keyed by the ``(major, minor)`` they upgrade the schema TO and
must be additive + idempotent (safe to re-run). The pattern to follow is
gamealerts' ``db._migrate_rosters_folded`` — check-then-act with a re-check on
``OperationalError`` so two processes racing the same DDL cannot fail spuriously;
:func:`add_column_if_missing` packages that pattern for future migrations.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path

# Library schema version. Bump MINOR for additive changes (new column/table/
# index) with a matching entry in MIGRATIONS **and the same DDL folded into
# schema.sql** — schema.sql always reflects the LATEST schema; migrations only
# upgrade existing files. The fresh-DB path applies schema.sql and stamps the
# target version WITHOUT running MIGRATIONS, so a migration whose DDL is
# missing from schema.sql would leave fresh files permanently short of it
# (tests/test_migrations.py asserts fresh-vs-migrated schema parity).
# Bump MAJOR only for breaking changes (not in-place upgradable).
SCHEMA_MAJOR = 1
SCHEMA_MINOR = 0

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

Migration = Callable[[sqlite3.Connection], None]

# Ordered additive migrations, keyed by the (major, minor) they produce.
# Empty at v1.0 — the first minor bump adds its entry here.
MIGRATIONS: dict[tuple[int, int], Migration] = {}


class SchemaVersionError(RuntimeError):
    """The on-disk schema version is incompatible with this library."""


def ensure_schema(
    conn: sqlite3.Connection,
    *,
    target: tuple[int, int] | None = None,
    migrations: Mapping[tuple[int, int], Migration] | None = None,
) -> None:
    """Apply/upgrade the core schema on ``conn`` per the version policy.

    ``target`` and ``migrations`` default to the library's own
    ``(SCHEMA_MAJOR, SCHEMA_MINOR)`` and :data:`MIGRATIONS`; they are
    injectable so tests can exercise the version policy without fabricating
    library history.
    """
    target_major, target_minor = target if target is not None else (SCHEMA_MAJOR, SCHEMA_MINOR)
    if migrations is None:
        migrations = MIGRATIONS

    current = get_schema_version(conn)
    if current is None:
        # Fresh (or pre-versioned) file: apply the baseline schema and stamp.
        conn.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
        _stamp_version(conn, target_major, target_minor)
        conn.commit()
        return

    major, minor = current
    if major > target_major:
        raise SchemaVersionError(
            f"database schema is v{major}.{minor} but this library supports "
            f"v{target_major}.{target_minor}: the file was written by a newer "
            f"gamecollect — upgrade the library (or point at a different database)"
        )
    if major < target_major:
        raise SchemaVersionError(
            f"database schema is v{major}.{minor} but this library requires major "
            f"v{target_major}: major bumps are not in-place upgradable — recollect "
            f"into a fresh database"
        )
    if minor >= target_minor:
        # Equal or newer minor within our major: additive-only guarantee makes
        # this safe to open as-is. Never downgrade the stamped version.
        return

    # Older minor: run the ordered additive migrations up to the target.
    for key in sorted(migrations):
        if (major, minor) < key <= (target_major, target_minor):
            migrations[key](conn)
    _stamp_version(conn, target_major, target_minor)
    conn.commit()


def get_schema_version(conn: sqlite3.Connection) -> tuple[int, int] | None:
    """Return the stamped ``(major, minor)``, or ``None`` if unstamped."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_meta'"
    ).fetchone()
    if row is None:
        return None
    meta = conn.execute("SELECT major, minor FROM schema_meta WHERE id = 1").fetchone()
    if meta is None:
        return None
    return (meta[0], meta[1])


def _stamp_version(conn: sqlite3.Connection, major: int, minor: int) -> None:
    conn.execute(
        """
        INSERT INTO schema_meta (id, major, minor, applied_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            major = excluded.major,
            minor = excluded.minor,
            applied_at = excluded.applied_at
        """,
        (major, minor, datetime.now(UTC).isoformat()),
    )


def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> None:
    """Additive, idempotent column back-add for use inside migrations.

    Generalizes gamealerts' ``_migrate_rosters_folded`` pattern: check-then-act,
    and on ``OperationalError`` re-read the live schema and swallow the error
    ONLY if the column is genuinely present now (another process won the race);
    anything else (locked schema, real DDL failure) still propagates.
    """
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column in cols:
        return
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")
    except sqlite3.OperationalError:
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            raise
