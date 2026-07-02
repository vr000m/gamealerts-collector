"""Phase 2: schema-version policy and migration idempotency.

Plan contract (docs/dev_plans/20260702-feature-collector-foundation.md §Phase 2):
- Refuse-on-newer-major: ``connect()`` raises when the DB's schema_meta
  major is greater than the library's major (actionable message).
- Equal-major / newer-minor opens OK (minor is additive-only).
- Equal-major / older-minor triggers additive migration up to the library's
  current minor.
- ``gamecollect.db.migrations`` holds ordered additive migrations keyed by
  (major, minor); running the migration path repeatedly is idempotent.
"""

import pytest
from _phase2_helpers import (
    core_schema_sql,
    current_schema_version,
    force_schema_version,
)

from gamecollect.db.connection import connect


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "migrations.db"


def _fresh_db(db_path):
    connect(db_path).close()
    return current_schema_version(db_path)


def test_migrations_module_importable():
    import gamecollect.db.migrations  # noqa: F401


def test_fresh_db_records_schema_v1(db_path):
    major, minor = _fresh_db(db_path)
    assert major == 1
    assert minor >= 0


def test_reopen_at_current_version_is_fine(db_path):
    version = _fresh_db(db_path)
    conn = connect(db_path)
    conn.close()
    assert current_schema_version(db_path) == version


def test_equal_major_newer_minor_opens_ok(db_path):
    major, minor = _fresh_db(db_path)
    force_schema_version(db_path, major, minor + 3)
    # A DB written by a newer minor of the same major is compatible
    # (minor = additive only) — connect must succeed and not downgrade it.
    conn = connect(db_path)
    conn.close()
    assert current_schema_version(db_path) == (major, minor + 3)


def test_newer_major_refuses_with_actionable_message(db_path):
    major, _minor = _fresh_db(db_path)
    force_schema_version(db_path, major + 1, 0)
    with pytest.raises(Exception, match="(?i)(schema|major|version)"):
        connect(db_path)


def test_equal_major_older_minor_triggers_additive_migration(db_path):
    major, minor = _fresh_db(db_path)
    if minor == 0:
        pytest.skip(
            "library schema is at minor 0; no older-minor database can be "
            "constructed yet (first additive migration lands in a later minor)"
        )
    force_schema_version(db_path, major, minor - 1)
    conn = connect(db_path)
    conn.close()
    # connect() must have migrated the DB back up to the library's version.
    assert current_schema_version(db_path) == (major, minor)


def test_migration_path_is_idempotent(db_path):
    """Repeated connects (the migration entry point) never change the schema."""
    version = _fresh_db(db_path)
    baseline = core_schema_sql(db_path)
    for _ in range(3):
        connect(db_path).close()
        assert core_schema_sql(db_path) == baseline
        assert current_schema_version(db_path) == version


def test_migration_idempotent_from_older_minor(db_path):
    major, minor = _fresh_db(db_path)
    if minor == 0:
        pytest.skip("library schema is at minor 0; no registered migrations to re-run yet")
    baseline = core_schema_sql(db_path)
    force_schema_version(db_path, major, minor - 1)
    connect(db_path).close()  # migrates up
    after_first = core_schema_sql(db_path)
    connect(db_path).close()  # must be a no-op
    assert core_schema_sql(db_path) == after_first == baseline
    assert current_schema_version(db_path) == (major, minor)


def test_reader_refuses_major_mismatch_both_directions(db_path):
    """open_reader applies the same major policy as connect(): != refuses.

    Review fix: the reader previously accepted older-major files, which have a
    different table layout and would silently return wrong-shaped rows.
    """
    from gamecollect.db import reader

    major, _minor = _fresh_db(db_path)
    force_schema_version(db_path, major + 1, 0)
    with pytest.raises(Exception, match="(?i)(schema|major|version)"):
        reader.open_reader(db_path)

    force_schema_version(db_path, major - 1, 0)
    with pytest.raises(Exception, match="(?i)(schema|major|version)"):
        reader.open_reader(db_path)


def test_fresh_db_schema_matches_migrated_schema(db_path, tmp_path):
    """Fresh path (schema.sql + stamp) and migration path must converge.

    Guards the convention that schema.sql always reflects the LATEST schema:
    a MIGRATIONS entry whose DDL is not folded into schema.sql would make a
    fresh DB diverge from a migrated one and this test fail.
    """
    _fresh_db(db_path)
    fresh = core_schema_sql(db_path)

    migrated_path = tmp_path / "migrated.db"
    major, minor = _fresh_db(migrated_path)
    if minor > 0:
        # Rewind the stamp and re-run the migration path over the baseline.
        force_schema_version(migrated_path, major, 0)
        connect(migrated_path).close()
    assert core_schema_sql(migrated_path) == fresh


def test_reader_refuses_unstamped_file(tmp_path):
    """A SQLite file without a schema_meta stamp is not a gamecollect DB."""
    import sqlite3

    from gamecollect.db import reader
    from gamecollect.db.migrations import SchemaVersionError

    foreign = tmp_path / "foreign.db"
    with sqlite3.connect(foreign) as c:
        c.execute("CREATE TABLE unrelated (x)")
    with pytest.raises(SchemaVersionError, match="schema_meta"):
        reader.open_reader(foreign)
