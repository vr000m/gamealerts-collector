"""Core DB layer: schema v1, connection factory, partition writer, reader.

- :mod:`gamecollect.db.connection` — read-write connections for collector
  daemons (pragmas, schema apply, migrations, pack side-table DDL).
- :mod:`gamecollect.db.reader` — read-only connections + untyped low-level
  reads for consumers.
- :mod:`gamecollect.db.writer` — :class:`~gamecollect.db.writer.PartitionWriter`,
  the only write path, scoped to one ``source`` partition.
- :mod:`gamecollect.db.migrations` — schema version policy and ordered
  additive migrations.
- :mod:`gamecollect.db.paths` — ``data_dir`` resolution for the collector's
  shared-file write model.
- :mod:`gamecollect.db.locking` — the ``_live_db_admission.lock`` advisory
  lock shared writers hold for their write session.
"""

from gamecollect.db.connection import connect
from gamecollect.db.locking import AdvisoryLock, live_db_admission_lock
from gamecollect.db.migrations import SCHEMA_MAJOR, SCHEMA_MINOR, SchemaVersionError
from gamecollect.db.paths import resolve_data_dir
from gamecollect.db.reader import open_reader
from gamecollect.db.writer import (
    CrossPartitionError,
    PartitionWriter,
    SequenceError,
    TaxonomyError,
    UnseededMatchError,
)

__all__ = [
    "connect",
    "open_reader",
    "PartitionWriter",
    "SchemaVersionError",
    "CrossPartitionError",
    "SequenceError",
    "TaxonomyError",
    "UnseededMatchError",
    "SCHEMA_MAJOR",
    "SCHEMA_MINOR",
    "AdvisoryLock",
    "live_db_admission_lock",
    "resolve_data_dir",
]
