"""Shared helpers for the Phase 1 DB-layer tests (locking + paths).

The plan (docs/dev_plans/20260707-feature-gameworker-integration.md, Phase 1)
pins `resolve_data_dir(explicit=None)` explicitly, and pins the locking
convention (mirror gamealerts' `AdvisoryLock` / `live_db_admission_lock`
verbatim), but does NOT pin the db-path helper's name, the data_dir env var
name, or the exact keyword used to select SHARED vs EXCLUSIVE mode. These
tests resolve those among candidates so a minor naming difference does not
spuriously fail — mirrors the existing `tests/_phase2_helpers.py` pattern in
this repo.

If the real implementation diverges from every candidate here, fix the
candidates in this one place rather than each test.
"""

from __future__ import annotations

from typing import Any

DB_PATH_HELPER_CANDIDATES = (
    "resolve_db_path",
    "db_path",
    "collector_db_path",
    "default_db_path",
)

DATA_DIR_ENV_CANDIDATES = (
    "GAMECOLLECT_DATA_DIR",
    "GAMECOLLECT_COLLECTOR_DATA_DIR",
    "COLLECTOR_DATA_DIR",
)

LOCK_DIR_HELPER_CANDIDATES = (
    "resolve_lock_dir",
    "lock_dir",
    "locks_dir",
)


def get_db_path_fn(paths_module):
    for name in DB_PATH_HELPER_CANDIDATES:
        fn = getattr(paths_module, name, None)
        if callable(fn):
            return fn
    raise AttributeError(
        f"gamecollect.db.paths exposes none of {DB_PATH_HELPER_CANDIDATES}; "
        "update DB_PATH_HELPER_CANDIDATES in tests/_phase1_helpers.py"
    )


def acquire_admission_lock(lock_fn, lock_dir, *, exclusive: bool):
    """Call the admission-lock context-manager factory across kwarg-name candidates.

    Returns the (unentered) context manager; callers use it with ``with``.
    """
    kwargs_candidates: tuple[dict[str, Any], ...] = (
        {"exclusive": exclusive},
        {"mode": "exclusive" if exclusive else "shared"},
        {"shared": not exclusive},
    )
    last_exc: TypeError | None = None
    for kwargs in kwargs_candidates:
        try:
            return lock_fn(lock_dir, **kwargs)
        except TypeError as exc:
            last_exc = exc
            continue
    if not exclusive:
        # SHARED may be the no-kwarg default.
        try:
            return lock_fn(lock_dir)
        except TypeError as exc:
            last_exc = exc
    raise AssertionError(
        f"could not call the admission-lock factory with an exclusive={exclusive} "
        f"selector via any candidate kwarg; last error: {last_exc}. Update "
        "acquire_admission_lock in tests/_phase1_helpers.py to match the real signature."
    )
