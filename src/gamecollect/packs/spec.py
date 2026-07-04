"""Sport-pack contract: the dataclasses a pack factory must return.

A pack is a Python package registered under the ``gamecollect.packs`` entry
point group; the entry-point value is a zero-arg factory returning a
:class:`SportPack`. The six DESIGN.md §5 contributions map onto the fields
below and are validated by :mod:`gamecollect.packs.registry` at load time.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gamecollect.provider import MatchDataProvider, NormalizedMatch

if TYPE_CHECKING:
    from gamecollect.db.writer import PartitionWriter
    from gamecollect.registry import Operation

__all__ = [
    "EventTypeDecl",
    "SportPack",
    "default_persist_side_tables",
    "default_seed_match",
]

# The seeding hook the engine calls before any child (event/mapping) write.
SeedMatch = Callable[[sqlite3.Connection, "PartitionWriter", NormalizedMatch], "str | None"]
PersistSideTables = Callable[
    [sqlite3.Connection, "PartitionWriter", NormalizedMatch, str],
    None,
]


def default_seed_match(
    conn: sqlite3.Connection, writer: PartitionWriter, match: NormalizedMatch
) -> str | None:
    """Sport-agnostic default :attr:`SportPack.seed_match`.

    Seeds/updates a ``matches`` row under the provider-native ``match.match_id``
    (no reconciliation, no source-qualification — that is pack territory) and
    returns that id so the engine can proceed with child writes. A pack that
    reconciles against a canonical schedule (football) overrides this with its
    own hook; this default keeps the contract usable for packs that emit
    already-canonical ids. ``conn`` is unused here but is part of the hook
    signature so reconciling packs can read existing rows.
    """
    writer.upsert_match(
        {
            "match_id": match.match_id,
            "status": match.status.value if match.status is not None else None,
            "minute": match.minute,
            "score_home": match.score_home,
            "score_away": match.score_away,
            "display_clock": match.display_clock,
            "kickoff_utc": match.kickoff_utc,
        }
    )
    return match.match_id


def default_persist_side_tables(
    conn: sqlite3.Connection,
    writer: PartitionWriter,
    match: NormalizedMatch,
    seeded_match_id: str,
) -> None:
    """Sport-agnostic default :attr:`SportPack.persist_side_tables`.

    Core has no pack-owned side tables, so the default is a no-op. The parameters
    mirror the engine's write context so packs can persist typed side-table rows
    from ``match.payload`` without the core importing pack modules.
    """


@dataclass(frozen=True)
class EventTypeDecl:
    """Declaration of one pack event type: display name + importance default.

    ``importance_default`` uses the small-is-critical integer scale that
    gamealerts established (1=critical … 4=low); adapters may override it
    per event.
    """

    display_name: str
    importance_default: int


@dataclass
class SportPack:
    """Everything one sport contributes to the collector (DESIGN.md §5).

    ``taxonomy`` keys are the legal ``events.type`` strings for this pack;
    :attr:`event_types` is the string-set to pass to
    ``PartitionWriter(conn, source, taxonomy=pack.event_types)`` so undeclared
    types are rejected loudly at write time.

    ``side_table_ddl`` is additive, pack-owned DDL applied by
    ``connect(path, side_table_ddl=pack.side_table_ddl)`` after core
    schema/migrations — packs may own typed side tables but never alter core
    tables.

    ``provider_factory`` must be cheap and side-effect-free at construction
    time (no network I/O, no blocking work): the registry's ``validate_pack``
    instantiates a throwaway provider on every pack load purely to type-check
    it. Defer expensive setup to the provider's fetch methods.
    """

    name: str
    sport: str
    provider_factory: Callable[[], MatchDataProvider]
    taxonomy: dict[str, EventTypeDecl]
    prompt_fragments: dict[str, str]
    preference_schema: dict
    display_metadata: dict
    compaction_boundaries: list[str]
    side_table_ddl: tuple[str, ...] = field(default=())
    operations: tuple[Operation, ...] = field(default=())
    """Pack-contributed read operations merged into the operation registry.

    A pack may register additional read ops over its own side tables (football
    contributes ``squad``/``player-stats`` over ``football_lineups``/
    ``football_stats``). :func:`gamecollect.registry.build_registry` merges
    these onto the four core ops; the CLI and ``tools --json`` manifest are
    generated from the merged set. Each entry is a
    :class:`gamecollect.registry.Operation` whose ``impl`` lives in the pack —
    core never imports pack modules (dependency direction is pack → core).
    """
    seed_match: SeedMatch = field(default=default_seed_match)
    """Seed-before-child-write hook the engine calls per changed match.

    ``seed_match(conn, writer, match) -> str | None`` must ensure a ``matches``
    row exists (and is owned by ``writer.source``) for ``match`` before the
    engine appends its events, then return the **canonical** ``match_id`` the
    engine writes children against. A ``str`` return always names a seeded row
    satisfying the writer's child-write precondition
    (:class:`~gamecollect.db.writer.UnseededMatchError`); ``None`` means the
    match could not be seeded (e.g. missing identity fields) and the engine
    skips child writes for it this poll. Defaults to :func:`default_seed_match`;
    the football pack overrides it with its reconcile-aware seeder.
    """
    persist_side_tables: PersistSideTables = field(default=default_persist_side_tables)
    """Optional pack-owned side-table persistence hook.

    ``persist_side_tables(conn, writer, match, seeded_match_id)`` is called by the
    engine after ``seed_match`` succeeds and after new core events for that poll
    are appended. Packs that normalize sport-specific detail into
    ``NormalizedMatch.payload`` can project it into their own side tables here
    (for example football lineups/stats). The hook must only touch pack-owned
    additive tables and must write under ``writer.source`` plus the canonical
    ``seeded_match_id`` returned by ``seed_match``. The default is a no-op so
    core remains independent of every pack package.
    """

    @property
    def event_types(self) -> frozenset[str]:
        """The declared event ``type`` strings (the writer-taxonomy set)."""
        return frozenset(self.taxonomy)
