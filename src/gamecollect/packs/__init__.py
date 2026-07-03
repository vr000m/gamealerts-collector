"""Sport-pack SDK: the pack contract (:mod:`spec`) and entry-point registry.

Packs register a zero-arg factory under the ``gamecollect.packs`` entry-point
group; :func:`~gamecollect.packs.registry.load_pack` discovers, loads, and
validates it against the six DESIGN.md §5 contributions.
"""

from gamecollect.packs.registry import (
    ENTRY_POINT_GROUP,
    PackError,
    PackNotFoundError,
    PackValidationError,
    load_pack,
    pack_names,
    validate_pack,
)
from gamecollect.packs.spec import EventTypeDecl, SportPack

__all__ = [
    "ENTRY_POINT_GROUP",
    "EventTypeDecl",
    "SportPack",
    "PackError",
    "PackNotFoundError",
    "PackValidationError",
    "load_pack",
    "pack_names",
    "validate_pack",
]
