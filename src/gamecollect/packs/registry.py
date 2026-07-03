"""Sport-pack discovery and validation via ``importlib.metadata`` entry points.

Packs register under the ``gamecollect.packs`` entry-point group; each entry
point's value is a zero-arg factory returning a
:class:`~gamecollect.packs.spec.SportPack`. :func:`load_pack` resolves the
entry point, calls the factory, and validates the pack against all six
DESIGN.md §5 contributions — a pack missing (or supplying a degenerate value
for) any of them is rejected loudly with :class:`PackValidationError`.
"""

from __future__ import annotations

from importlib.metadata import entry_points

from gamecollect.packs.spec import EventTypeDecl, SportPack
from gamecollect.provider import MatchDataProvider

__all__ = [
    "ENTRY_POINT_GROUP",
    "PackError",
    "PackNotFoundError",
    "PackValidationError",
    "pack_names",
    "load_pack",
    "validate_pack",
]

ENTRY_POINT_GROUP = "gamecollect.packs"


class PackError(Exception):
    """Base class for pack discovery/validation failures."""


class PackNotFoundError(PackError):
    """No entry point with the requested name in ``gamecollect.packs``."""


class PackValidationError(PackError):
    """A loaded pack violates the DESIGN.md §5 contract."""


def pack_names() -> tuple[str, ...]:
    """Names of all installed packs (sorted; discovery only, no loading)."""
    return tuple(sorted(ep.name for ep in entry_points(group=ENTRY_POINT_GROUP)))


def load_pack(name: str) -> SportPack:
    """Load and validate the installed pack registered as ``name``.

    Raises :class:`PackNotFoundError` when no such entry point exists and
    :class:`PackValidationError` when the loaded pack is malformed.
    """
    matches = [ep for ep in entry_points(group=ENTRY_POINT_GROUP) if ep.name == name]
    if not matches:
        available = ", ".join(pack_names()) or "(none installed)"
        raise PackNotFoundError(
            f"no pack named {name!r} in entry-point group {ENTRY_POINT_GROUP!r}; "
            f"installed packs: {available}"
        )
    factory = matches[0].load()
    if not callable(factory):
        raise PackValidationError(
            f"pack {name!r} entry point must be a zero-arg factory returning "
            f"SportPack, got non-callable {factory!r}"
        )
    pack = factory()
    return validate_pack(pack, entry_point_name=name)


def validate_pack(pack: object, *, entry_point_name: str | None = None) -> SportPack:
    """Validate all six DESIGN.md §5 contributions; return the pack or raise.

    The six contributions: provider adapter (factory returning a
    :class:`MatchDataProvider`), event taxonomy, prompt fragments, preference
    schema, display metadata, compaction boundaries. Each must be present and
    well-formed — an empty contribution is treated as missing.
    """
    if not isinstance(pack, SportPack):
        raise PackValidationError(
            f"pack factory must return a SportPack, got {type(pack).__name__}"
        )
    name = pack.name

    if not name or not isinstance(name, str):
        raise PackValidationError("pack 'name' must be a non-empty string")
    if entry_point_name is not None and name != entry_point_name:
        raise PackValidationError(
            f"pack name {name!r} does not match its entry-point name {entry_point_name!r}"
        )
    if not pack.sport or not isinstance(pack.sport, str):
        raise PackValidationError(f"pack {name!r}: 'sport' must be a non-empty string")

    # 1. Provider adapter — factory must be callable and return the ABC.
    if not callable(pack.provider_factory):
        raise PackValidationError(f"pack {name!r}: 'provider_factory' is not callable")
    provider = pack.provider_factory()
    if not isinstance(provider, MatchDataProvider):
        raise PackValidationError(
            f"pack {name!r}: provider_factory returned {type(provider).__name__}, "
            "not a MatchDataProvider"
        )

    # 2. Event taxonomy — non-empty, str keys, EventTypeDecl values.
    if not isinstance(pack.taxonomy, dict) or not pack.taxonomy:
        raise PackValidationError(f"pack {name!r}: 'taxonomy' must be a non-empty dict")
    for key, decl in pack.taxonomy.items():
        if not key or not isinstance(key, str):
            raise PackValidationError(
                f"pack {name!r}: taxonomy keys must be non-empty strings, got {key!r}"
            )
        if not isinstance(decl, EventTypeDecl):
            raise PackValidationError(
                f"pack {name!r}: taxonomy[{key!r}] must be an EventTypeDecl, "
                f"got {type(decl).__name__}"
            )
        if not decl.display_name or not isinstance(decl.display_name, str):
            raise PackValidationError(
                f"pack {name!r}: taxonomy[{key!r}].display_name must be a non-empty string"
            )
        if not isinstance(decl.importance_default, int) or isinstance(
            decl.importance_default, bool
        ):
            raise PackValidationError(
                f"pack {name!r}: taxonomy[{key!r}].importance_default must be an int, "
                f"got {decl.importance_default!r}"
            )

    # 3. Prompt fragments — non-empty str -> str mapping.
    if not isinstance(pack.prompt_fragments, dict) or not pack.prompt_fragments:
        raise PackValidationError(f"pack {name!r}: 'prompt_fragments' must be a non-empty dict")
    for key, fragment in pack.prompt_fragments.items():
        if not isinstance(key, str) or not isinstance(fragment, str):
            raise PackValidationError(
                f"pack {name!r}: prompt_fragments entries must map str to str, "
                f"got {key!r} -> {fragment!r}"
            )

    # 4. Preference schema — JSON-schema-shaped dict.
    if not isinstance(pack.preference_schema, dict) or not pack.preference_schema:
        raise PackValidationError(f"pack {name!r}: 'preference_schema' must be a non-empty dict")

    # 5. Display metadata.
    if not isinstance(pack.display_metadata, dict) or not pack.display_metadata:
        raise PackValidationError(f"pack {name!r}: 'display_metadata' must be a non-empty dict")

    # 6. Compaction boundaries — non-empty list of non-empty strings.
    if not isinstance(pack.compaction_boundaries, list) or not pack.compaction_boundaries:
        raise PackValidationError(
            f"pack {name!r}: 'compaction_boundaries' must be a non-empty list"
        )
    for boundary in pack.compaction_boundaries:
        if not boundary or not isinstance(boundary, str):
            raise PackValidationError(
                f"pack {name!r}: compaction boundaries must be non-empty strings, got {boundary!r}"
            )

    # Side tables are optional, but when supplied must be a tuple of SQL strings.
    if not isinstance(pack.side_table_ddl, tuple) or not all(
        isinstance(ddl, str) and ddl.strip() for ddl in pack.side_table_ddl
    ):
        raise PackValidationError(
            f"pack {name!r}: 'side_table_ddl' must be a tuple of non-empty SQL strings"
        )

    return pack
