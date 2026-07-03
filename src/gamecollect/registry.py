"""Operation registry — the single source of truth for library, CLI, manifest.

DESIGN.md §6 and this plan pin one registry from which the client library
surface, the ``gamecollect`` argparse subcommands, and the ``tools --json``
manifest are all derived. A CLI subcommand and its library op cannot drift
because they are the same :class:`Operation`; a new op is automatically
discoverable via ``tools --json``.

Each :class:`Operation` declares:

* ``name`` — the registry key, the CLI subcommand, and the manifest op name
  (one string, so the single-source-of-truth equality test holds by
  construction);
* ``params`` — ordered :class:`ParamSpec` declarations (name/type/required)
  that Phase 3 turns into argparse arguments and the manifest lists verbatim;
* ``summary`` — one-line help (CLI/description; not part of the wire manifest);
* ``output_schema`` — the JSON Schema of the ``--json`` output, which IS the
  public contract (versioned by :data:`CONTRACT_VERSION`, golden-file tested);
* ``impl`` — the callable ``impl(conn, **params) -> dataclass | list | None``;
* ``to_json`` — serializes the typed return to JSON-safe data (defaults to a
  generic dataclass serializer that all core and pack ops share).

Packs contribute additional operations through
:attr:`gamecollect.packs.spec.SportPack.operations`; :func:`build_registry`
merges them onto the core set. Core never imports a pack — the dependency
direction is pack → core only.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

from gamecollect import client

__all__ = [
    "CONTRACT_VERSION",
    "ParamSpec",
    "Operation",
    "Registry",
    "CORE_OPERATIONS",
    "build_registry",
    "default_result_to_json",
]

# Version of the wire contract (manifest + --json output schemas). Bump on any
# breaking change to an output schema; a bump shows up as a golden-file diff in
# review (this plan's JSON-output-schema-stability review focus).
CONTRACT_VERSION = 1

# Legal ParamSpec.type strings — the JSON-Schema-ish scalar kinds an argparse
# argument can carry. Kept deliberately small (the CLI maps each to a parser).
PARAM_TYPES = frozenset({"string", "integer"})


def default_result_to_json(result: Any) -> Any:
    """Serialize a typed operation return to JSON-safe data.

    Core and pack ops all return frozen dataclasses (or lists of them, or
    ``None`` for a not-found single object); :func:`dataclasses.asdict`
    recursively renders each to a plain dict, leaving already-decoded
    ``payload`` objects intact. Ops with a bespoke return can override
    :attr:`Operation.to_json`.
    """
    if result is None:
        return None
    if isinstance(result, list):
        return [asdict(item) if is_dataclass(item) else item for item in result]
    if is_dataclass(result):
        return asdict(result)
    return result


@dataclass(frozen=True)
class ParamSpec:
    """One operation parameter: name, scalar type, and whether it is required.

    ``summary`` feeds CLI ``--help`` (Phase 3); ``default`` is the value the
    CLI uses when an optional argument is absent. The wire manifest carries
    only name/type/required (see :meth:`to_manifest`).
    """

    name: str
    type: str
    required: bool
    summary: str = ""
    default: Any = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ParamSpec.name must be non-empty")
        if self.type not in PARAM_TYPES:
            raise ValueError(f"ParamSpec.type {self.type!r} not in {sorted(PARAM_TYPES)}")

    def to_manifest(self) -> dict[str, Any]:
        """The wire form: name/type/required only (the pinned param contract)."""
        return {"name": self.name, "type": self.type, "required": self.required}


@dataclass(frozen=True)
class Operation:
    """A registered read operation shared by the library, CLI, and manifest."""

    name: str
    summary: str
    params: tuple[ParamSpec, ...]
    output_schema: dict[str, Any]
    impl: Callable[..., Any]
    to_json: Callable[[Any], Any] = default_result_to_json

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Operation.name must be non-empty")
        if not callable(self.impl):
            raise ValueError(f"Operation {self.name!r}: impl must be callable")
        seen: set[str] = set()
        for param in self.params:
            if not isinstance(param, ParamSpec):
                raise ValueError(
                    f"Operation {self.name!r}: params must be ParamSpec, got {type(param).__name__}"
                )
            if param.name in seen:
                raise ValueError(f"Operation {self.name!r}: duplicate param {param.name!r}")
            seen.add(param.name)

    def to_manifest(self) -> dict[str, Any]:
        """The manifest entry: ``{name, params, output_schema}`` from the objects.

        Returns the operation's own ``params``/``output_schema`` (serialized),
        never a hand-written literal — the single-source-of-truth guarantee.
        """
        return {
            "name": self.name,
            "params": [p.to_manifest() for p in self.params],
            "output_schema": self.output_schema,
        }


class Registry:
    """An ordered, name-unique collection of :class:`Operation`."""

    def __init__(self) -> None:
        self._ops: dict[str, Operation] = {}

    def register(self, op: Operation) -> None:
        """Add ``op``; raise on a duplicate name (core/pack collision is loud)."""
        if not isinstance(op, Operation):
            raise TypeError(f"expected Operation, got {type(op).__name__}")
        if op.name in self._ops:
            raise ValueError(f"duplicate operation name {op.name!r}")
        self._ops[op.name] = op

    @property
    def operations(self) -> tuple[Operation, ...]:
        """All operations in registration order (core first, then packs)."""
        return tuple(self._ops.values())

    @property
    def names(self) -> tuple[str, ...]:
        """All operation names in registration order."""
        return tuple(self._ops)

    def get(self, name: str) -> Operation:
        """Return the operation named ``name`` (``KeyError`` if absent)."""
        return self._ops[name]

    def __contains__(self, name: object) -> bool:
        return name in self._ops

    def manifest(self, contract_version: int = CONTRACT_VERSION) -> dict[str, Any]:
        """Emit the ``tools --json`` manifest generated from the operations."""
        return {
            "contract_version": contract_version,
            "operations": [op.to_manifest() for op in self.operations],
        }


# ---------------------------------------------------------------------------
# Core operations — the four sport-agnostic reads over the core tables.
# Output schemas ARE the public contract (golden-file tested in Phase 3).
# ---------------------------------------------------------------------------

# Reusable "T or null" property schemas (payloads/optional columns are nullable).
_STR = {"type": ["string", "null"]}
_INT = {"type": ["integer", "null"]}
_OBJ = {"type": ["object", "array", "null"]}

_MATCH_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "match_id": {"type": "string"},
        "source": {"type": "string"},
        "kickoff_utc": _STR,
        "home_entity": _STR,
        "away_entity": _STR,
        "status": _STR,
        "minute": _INT,
        "period": _INT,
        "score_home": _INT,
        "score_away": _INT,
        "display_clock": _STR,
        "payload": _OBJ,
        "updated_at": _STR,
    },
}

_EVENT_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "source": {"type": "string"},
        "match_id": {"type": "string"},
        "seq": {"type": "integer"},
        "minute": _INT,
        "period": _INT,
        "type": {"type": "string"},
        "importance": _INT,
        "actor_entity": _STR,
        "target_entity": _STR,
        "detail": _STR,
        "payload": _OBJ,
    },
}

_STANDING_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "source": {"type": "string"},
        "group_key": {"type": "string"},
        "entity_id": {"type": "string"},
        "points": _INT,
        "rank": _INT,
        "payload": _OBJ,
    },
}


CORE_OPERATIONS: tuple[Operation, ...] = (
    Operation(
        name="matches",
        summary="List matches, optionally filtered by source and/or status.",
        params=(
            ParamSpec("source", "string", required=False, summary="Restrict to one source."),
            ParamSpec("status", "string", required=False, summary="Restrict to one status."),
        ),
        output_schema={"type": "array", "items": _MATCH_ITEM_SCHEMA},
        impl=client.list_matches,
    ),
    Operation(
        name="state",
        summary="Return the current state of one match by id.",
        params=(
            ParamSpec("match_id", "string", required=True, summary="Globally-unique match id."),
        ),
        output_schema={
            "oneOf": [_MATCH_ITEM_SCHEMA, {"type": "null"}],
        },
        impl=client.get_state,
    ),
    Operation(
        name="events",
        summary="Return match events with seq strictly greater than since_seq.",
        params=(
            ParamSpec("match_id", "string", required=True, summary="Globally-unique match id."),
            ParamSpec(
                "since_seq",
                "integer",
                required=False,
                summary="Return events with seq > this (default -1 = all).",
                default=-1,
            ),
        ),
        output_schema={"type": "array", "items": _EVENT_ITEM_SCHEMA},
        impl=client.get_events_since,
    ),
    Operation(
        name="standings",
        summary="Return standings for a source, optionally scoped to one group.",
        params=(
            ParamSpec("source", "string", required=True, summary="Standings source (tournament)."),
            ParamSpec("group_key", "string", required=False, summary="Restrict to one group."),
        ),
        output_schema={"type": "array", "items": _STANDING_ITEM_SCHEMA},
        impl=client.get_standings,
    ),
)


def build_registry(packs: Iterable[Any] = ()) -> Registry:
    """Build the merged registry: core operations plus each pack's contributions.

    ``packs`` are loaded :class:`~gamecollect.packs.spec.SportPack` objects
    (accepted structurally so this module need not import the pack spec at call
    time). Core ops register first, in declaration order; then each pack's
    ``operations`` in pack order. A name collision (two ops claiming the same
    subcommand) raises loudly via :meth:`Registry.register`.
    """
    registry = Registry()
    for op in CORE_OPERATIONS:
        registry.register(op)
    for pack in packs:
        for op in getattr(pack, "operations", ()):
            registry.register(op)
    return registry


# A ready-made core-only registry for callers that need the sport-agnostic
# surface without loading a pack.
CORE_REGISTRY = build_registry()
