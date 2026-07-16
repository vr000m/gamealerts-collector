"""data_dir resolution for the collector's shared-file write model.

Both the collector daemon and gamealerts must agree on one directory to
open the same SQLite file (dev plan `20260707-feature-gameworker-integration.md`,
Phase 1). Mirrors gamealerts' own convention (env var, else a default under
the user's home) so the two processes can be pointed at the same place with
one setting.
"""

from __future__ import annotations

import os
from pathlib import Path

__all__ = ["DATA_DIR_ENV_VAR", "DB_FILENAME", "resolve_data_dir", "db_path", "lock_dir"]

DATA_DIR_ENV_VAR = "GAMECOLLECT_DATA_DIR"

# The collector's own filename — deliberately not gamealerts.db, so the two
# processes' default paths never collide even before either side sets the
# shared-data-dir env var.
DB_FILENAME = "gamecollect.db"

_DEFAULT_DATA_DIR = Path.home() / ".local" / "share" / "gamecollect"


def resolve_data_dir(explicit: str | Path | None = None) -> Path:
    """Resolve the collector's data directory.

    Precedence: ``explicit`` argument, then the ``GAMECOLLECT_DATA_DIR`` env
    var, then ``~/.local/share/gamecollect``. Does not create the directory —
    callers that need it to exist (:func:`gamecollect.db.connection.connect`,
    :func:`gamecollect.db.locking.live_db_admission_lock`) create it.
    """
    if explicit is not None:
        return Path(explicit)
    env_value = os.environ.get(DATA_DIR_ENV_VAR)
    if env_value:
        return Path(env_value)
    return _DEFAULT_DATA_DIR


def db_path(data_dir: str | Path) -> Path:
    """The collector-owned database file path within ``data_dir``."""
    return Path(data_dir) / DB_FILENAME


def lock_dir(data_dir: str | Path) -> Path:
    """The lock directory within ``data_dir`` (mirrors gamealerts' ``locks/``)."""
    return Path(data_dir) / "locks"
