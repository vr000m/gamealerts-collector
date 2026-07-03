"""Core DB layer: schema v1, connection factory, partition writer, reader.

- :mod:`gamecollect.db.connection` — read-write connections for collector
  daemons (pragmas, schema apply, migrations, pack side-table DDL).
- :mod:`gamecollect.db.reader` — read-only connections + untyped low-level
  reads for consumers.
- :mod:`gamecollect.db.writer` — :class:`~gamecollect.db.writer.PartitionWriter`,
  the only write path, scoped to one ``source`` partition.
- :mod:`gamecollect.db.migrations` — schema version policy and ordered
  additive migrations.
"""

from gamecollect.db.connection import connect
from gamecollect.db.migrations import SCHEMA_MAJOR, SCHEMA_MINOR, SchemaVersionError
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
]
