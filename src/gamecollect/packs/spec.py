"""Sport-pack contract: the dataclasses a pack factory must return.

A pack is a Python package registered under the ``gamecollect.packs`` entry
point group; the entry-point value is a zero-arg factory returning a
:class:`SportPack`. The six DESIGN.md §5 contributions map onto the fields
below and are validated by :mod:`gamecollect.packs.registry` at load time.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from gamecollect.provider import MatchDataProvider

__all__ = ["EventTypeDecl", "SportPack"]


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

    @property
    def event_types(self) -> frozenset[str]:
        """The declared event ``type`` strings (the writer-taxonomy set)."""
        return frozenset(self.taxonomy)
