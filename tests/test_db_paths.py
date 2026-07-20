"""Phase 1: data_dir resolver + collector db-path/lock-dir conventions.

Plan contract (docs/dev_plans/20260707-feature-gameworker-integration.md
Phase 1, Testing Notes, Acceptance Criteria):
- ``resolve_data_dir(explicit=None)`` honors an explicit path, else a
  documented env var, else a default dir.
- A db-path helper resolves the collector-owned file's own filename (NOT
  ``gamealerts.db``) under a data_dir.
- The lock dir is ``<data_dir>/locks/`` (mirrors gamealerts), when exposed as
  a dedicated helper.
- The existing explicit-path form of ``connect()`` keeps working unchanged,
  independent of the data_dir env var.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from _phase1_helpers import (
    DATA_DIR_ENV_CANDIDATES,
    LOCK_DIR_HELPER_CANDIDATES,
    get_db_path_fn,
)

from gamecollect.db import paths
from gamecollect.db.connection import connect


@pytest.fixture(autouse=True)
def _clean_data_dir_env(monkeypatch):
    for var in DATA_DIR_ENV_CANDIDATES:
        monkeypatch.delenv(var, raising=False)


def _resolve_data_dir():
    fn = getattr(paths, "resolve_data_dir", None)
    if fn is None:
        pytest.skip("gamecollect.db.paths has no resolve_data_dir yet")
    return fn


def test_resolve_data_dir_returns_explicit_path_unchanged(tmp_path):
    resolve_data_dir = _resolve_data_dir()
    explicit = tmp_path / "explicit-data"
    result = Path(resolve_data_dir(explicit))
    assert result == explicit


def test_resolve_data_dir_honors_env_var(tmp_path, monkeypatch):
    resolve_data_dir = _resolve_data_dir()
    env_dir = tmp_path / "from-env"

    matched_var = None
    for var in DATA_DIR_ENV_CANDIDATES:
        monkeypatch.setenv(var, str(env_dir))
        result = Path(resolve_data_dir(None))
        monkeypatch.delenv(var, raising=False)
        if result == env_dir:
            matched_var = var
            break

    assert matched_var is not None, (
        "resolve_data_dir(None) did not honor any candidate env var "
        f"{DATA_DIR_ENV_CANDIDATES}; update DATA_DIR_ENV_CANDIDATES in "
        "tests/_phase1_helpers.py to match the real variable name"
    )


def test_resolve_data_dir_default_falls_under_home():
    # The default is resolved from Path.home() at import time (a module-level
    # constant), not re-read per call — so this compares against the real
    # $HOME rather than monkeypatching it, which would have no effect.
    resolve_data_dir = _resolve_data_dir()

    result = Path(resolve_data_dir(None))

    assert result == Path.home() / ".local" / "share" / "gamecollect"


def test_db_path_helper_returns_collector_owned_filename(tmp_path):
    db_path_fn = get_db_path_fn(paths)
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    result = Path(db_path_fn(data_dir))

    assert result.parent == data_dir
    assert result.suffix == ".db"
    assert result.name != "gamealerts.db"


def test_lock_dir_convention_if_exposed(tmp_path):
    data_dir = tmp_path / "data"
    for name in LOCK_DIR_HELPER_CANDIDATES:
        fn = getattr(paths, name, None)
        if callable(fn):
            result = Path(fn(data_dir))
            assert result == data_dir / "locks"
            return
    pytest.skip(
        "paths.py exposes no dedicated lock-dir helper; callers construct "
        "<data_dir>/locks/ directly per the plan's stated convention"
    )


def test_explicit_path_connect_still_works_independent_of_data_dir_env(tmp_path, monkeypatch):
    for var in DATA_DIR_ENV_CANDIDATES:
        monkeypatch.setenv(var, str(tmp_path / "should-not-be-used"))

    explicit_path = tmp_path / "explicit.db"
    conn = connect(explicit_path)
    try:
        assert explicit_path.exists()
        conn.execute("SELECT 1").fetchone()
    finally:
        conn.close()
