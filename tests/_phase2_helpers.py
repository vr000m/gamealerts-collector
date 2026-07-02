"""Shared helpers for the Phase 2 DB-layer tests.

The plan (docs/dev_plans/20260702-feature-collector-foundation.md, Phase 2)
pins module paths and the ``connect`` / ``PartitionWriter`` constructor
signatures, but NOT the writer/reader method names or row shapes. These tests
therefore assume the following surface, and resolve method names among
candidates so minor naming differences do not spuriously fail:

- ``PartitionWriter`` exposes per-table write methods taking a ``dict`` of
  column values (``source`` optional; stamped by the writer when absent,
  rejected when it differs from the constructor source).
- ``gamecollect.db.reader`` functions take an open connection as the first
  argument; if that raises ``TypeError`` a database path is tried instead.
- ``gamecollect.fold`` exposes a single fold function (candidates below).

If the real implementation diverges, fix the candidates here (one place)
rather than each test.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Any

WRITER_METHOD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "match": ("upsert_match", "write_match", "put_match", "insert_match"),
    "event": ("append_event", "insert_event", "write_event", "add_event", "upsert_event"),
    "entity": ("upsert_entity", "write_entity", "put_entity", "insert_entity"),
    "standing": ("upsert_standing", "write_standing", "upsert_standings", "insert_standing"),
    "provider_map": (
        "map_provider_match",
        "upsert_provider_match",
        "upsert_provider_match_map",
        "record_provider_match",
        "insert_provider_match_map",
    ),
}

FOLD_CANDIDATES = ("fold_name", "fold", "name_fold", "fold_text")

ENTITY_LOOKUP_CANDIDATES = (
    "lookup_entities_by_name",
    "lookup_entity_by_name",
    "find_entities_by_name",
    "get_entities_by_name",
    "entities_by_folded_name",
    "get_entity_by_folded_name",
    "lookup_entities",
    "lookup_entity",
    "find_entity",
)


def resolve_attr(obj: Any, candidates: tuple[str, ...], what: str) -> Callable[..., Any]:
    for name in candidates:
        fn = getattr(obj, name, None)
        if callable(fn):
            return fn
    raise AssertionError(f"{obj!r} exposes no {what}; tried {candidates}")


def writer_method(writer: Any, kind: str) -> Callable[..., Any]:
    return resolve_attr(writer, WRITER_METHOD_CANDIDATES[kind], f"{kind} write method")


def fold_fn():
    from gamecollect import fold as fold_mod

    return resolve_attr(fold_mod, FOLD_CANDIDATES, "fold function")


def call_reader(fn: Callable[..., Any], conn: sqlite3.Connection, path: Any, *args: Any) -> Any:
    """Call a reader function assuming conn-first; fall back to path-first."""
    try:
        return fn(conn, *args)
    except TypeError:
        return fn(str(path), *args)


def row_values(row: Any) -> list[Any]:
    """Flatten a reader result row (tuple / sqlite3.Row / dict / dataclass)."""
    if isinstance(row, dict):
        return list(row.values())
    if isinstance(row, sqlite3.Row):
        return list(row)
    if isinstance(row, (tuple, list)):
        return list(row)
    if hasattr(row, "__dict__"):
        return list(vars(row).values())
    return [row]


# --- minimal row factories (only plan-pinned NOT NULL / PK columns) ---------


def match_row(match_id: str, **extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"match_id": match_id, "status": "IN_PLAY"}
    row.update(extra)
    return row


def event_row(match_id: str, seq: int, type: str = "goal", **extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"match_id": match_id, "seq": seq, "type": type}
    row.update(extra)
    return row


def entity_row(entity_id: str, display_name: str, kind: str = "team", **extra) -> dict[str, Any]:
    row: dict[str, Any] = {"entity_id": entity_id, "display_name": display_name, "kind": kind}
    row.update(extra)
    return row


def standing_row(group_key: str, entity_id: str, **extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"group_key": group_key, "entity_id": entity_id}
    row.update(extra)
    return row


def provider_map_row(provider: str, provider_match_id: str, match_id: str, **extra) -> dict:
    row: dict[str, Any] = {
        "provider": provider,
        "provider_match_id": provider_match_id,
        "match_id": match_id,
    }
    row.update(extra)
    return row


# --- raw-sqlite inspection helpers ------------------------------------------


def table_names(db_path: Any) -> set[str]:
    with sqlite3.connect(db_path) as c:
        return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def pk_of(db_path: Any, table: str) -> list[str]:
    with sqlite3.connect(db_path) as c:
        info = c.execute(f"PRAGMA table_info({table})").fetchall()
    return [name for _, name in sorted((r[5], r[1]) for r in info if r[5] > 0)]


def columns_of(db_path: Any, table: str) -> dict[str, dict[str, Any]]:
    with sqlite3.connect(db_path) as c:
        info = c.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1]: {"type": r[2], "notnull": bool(r[3])} for r in info}


def core_schema_sql(db_path: Any) -> set[str]:
    core = {"schema_meta", "matches", "events", "entities", "standings", "provider_match_map"}
    with sqlite3.connect(db_path) as c:
        rows = c.execute(
            "SELECT name, sql FROM sqlite_master WHERE type IN ('table','index') "
            "AND sql IS NOT NULL"
        ).fetchall()
    return {sql for name, sql in rows if name in core or any(name.startswith(t) for t in core)}


def all_rows(db_path: Any, table: str, order_by: str | None = None) -> list[tuple]:
    q = f"SELECT * FROM {table}"
    if order_by:
        q += f" ORDER BY {order_by}"
    with sqlite3.connect(db_path) as c:
        return c.execute(q).fetchall()


def integrity_ok(db_path: Any) -> bool:
    with sqlite3.connect(db_path) as c:
        return c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def schema_meta_version_columns(db_path: Any) -> tuple[str, str]:
    """Find the (major, minor) column names in schema_meta by name substring."""
    cols = columns_of(db_path, "schema_meta")
    major = next((c for c in cols if "major" in c.lower()), None)
    minor = next((c for c in cols if "minor" in c.lower()), None)
    assert major and minor, f"schema_meta has no recognizable major/minor columns: {list(cols)}"
    return major, minor


def current_schema_version(db_path: Any) -> tuple[int, int]:
    maj_col, min_col = schema_meta_version_columns(db_path)
    with sqlite3.connect(db_path) as c:
        row = c.execute(
            f"SELECT {maj_col}, {min_col} FROM schema_meta "
            f"ORDER BY {maj_col} DESC, {min_col} DESC LIMIT 1"
        ).fetchone()
    assert row is not None, "schema_meta is empty on a freshly created database"
    return int(row[0]), int(row[1])


def force_schema_version(db_path: Any, major: int, minor: int) -> None:
    """Rewrite schema_meta to simulate a DB created by a different library version."""
    maj_col, min_col = schema_meta_version_columns(db_path)
    cols = columns_of(db_path, "schema_meta")
    names = [maj_col, min_col]
    values: list[Any] = [major, minor]
    for name, meta in cols.items():
        if name in (maj_col, min_col):
            continue
        if "applied" in name.lower() or meta["notnull"]:
            names.append(name)
            values.append("2026-01-01T00:00:00Z")
    with sqlite3.connect(db_path) as c:
        c.execute("DELETE FROM schema_meta")
        placeholders = ", ".join("?" for _ in names)
        c.execute(
            f"INSERT INTO schema_meta ({', '.join(names)}) VALUES ({placeholders})",
            values,
        )
        c.commit()
