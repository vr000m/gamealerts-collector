"""The ``gamecollect`` console entry point — a thin ``main()`` over the library.

DESIGN.md §6 and the interfaces plan pin one operation registry
(:mod:`gamecollect.registry`) as the single source of truth: the client library
surface, these argparse subcommands, and the ``tools --json`` manifest are all
derived from it. A read subcommand and its library op cannot drift because they
are the same :class:`~gamecollect.registry.Operation`; the manifest is emitted
from the same objects, never hand-written.

Command surface:

* one subcommand per registered read operation (``matches``/``state``/
  ``events``/``standings`` from core, plus ``squad``/``player-stats`` when the
  football pack is installed and contributes them). Each takes the operation's
  declared params (required params as positionals, optional ones as ``--flag``s),
  plus a CLI-level ``--db`` and the ``--json`` contract flag;
* ``collect`` — hand-wired: runs the Phase 1 :class:`CollectorEngine` for one
  source (``--pack``/``--db``/``--source``/``--record``);
* ``tools --json`` — emits the registry manifest for runtime capability
  discovery (the CLI-over-MCP choice).

The registry the CLI builds its subcommands from is the merge of the core ops
with every installed pack's contributed ops (:func:`load_registry`); a pack that
fails to load is skipped with a warning rather than breaking the whole CLI.
Core never imports a pack — the ops carry their own impls (dependency direction
is pack → core).
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import sys
from collections.abc import Callable, Sequence
from typing import Any

from gamecollect import __version__
from gamecollect.db import reader
from gamecollect.engine import CollectorEngine
from gamecollect.packs.registry import PackError, PackNotFoundError, load_pack, pack_names
from gamecollect.registry import Operation, Registry, build_registry

__all__ = ["main", "build_parser", "load_registry", "HAND_WIRED_COMMANDS", "cli_subcommand_names"]

log = logging.getLogger(__name__)

# Subcommands the CLI wires by hand rather than deriving from the registry.
# The single-source-of-truth test subtracts these from the parser's subcommand
# set to compare the remainder against the registry op names.
HAND_WIRED_COMMANDS: tuple[str, ...] = ("collect", "tools")
# Option names a read subcommand's parser already takes: the CLI's own --db
# and --json (added by _add_read_arguments) plus argparse's automatic --help.
# A pack param with one of these names would make sub.add_argument raise
# ArgumentError and kill the whole CLI, so such ops are skipped up front.
RESERVED_READ_OPTION_NAMES: frozenset[str] = frozenset({"db", "json", "help"})


def _copy_pack_with_operations(pack: Any, operations: tuple[Operation, ...]) -> Any:
    """Return a shallow pack copy with filtered operations."""
    filtered = copy.copy(pack)
    filtered.operations = operations
    return filtered


def _validated_pack_for_cli(name: str, pack: Any, accepted: list[Any]) -> Any:
    """Drop pack ops that would break argparse or the merged registry."""
    existing_names = set(build_registry(accepted).names) | set(HAND_WIRED_COMMANDS)
    kept: list[Operation] = []
    for op in getattr(pack, "operations", ()):
        if op.name in existing_names:
            log.warning(
                "skipping operation %r from pack %r for CLI registry: name collides "
                "with an existing or hand-wired command",
                op.name,
                name,
            )
            continue
        reserved_params = sorted(
            param.name for param in op.params if param.name in RESERVED_READ_OPTION_NAMES
        )
        if reserved_params:
            log.warning(
                "skipping operation %r from pack %r for CLI registry: parameter "
                "name(s) %s collide with reserved CLI option names",
                op.name,
                name,
                reserved_params,
            )
            continue
        kept.append(op)
        existing_names.add(op.name)
    if tuple(getattr(pack, "operations", ())) == tuple(kept):
        return pack
    return _copy_pack_with_operations(pack, tuple(kept))


def load_registry(
    *,
    pack_loader: Callable[[str], Any] = load_pack,
    names: Sequence[str] | None = None,
    loaded_packs: dict[str, Any] | None = None,
) -> Registry:
    """Build the merged registry: core ops plus every installed pack's ops.

    ``names`` defaults to all installed pack entry points
    (:func:`~gamecollect.packs.registry.pack_names`); each is loaded and its
    contributed operations merged onto the core set. A pack that fails to load
    or validate is logged and skipped — a broken pack must not take the whole
    CLI (and its core reads) down with it. Duplicate entry-point names (a wheel
    installed alongside an editable checkout can register the same name twice)
    are de-duplicated before loading, and the registry merge is done pack by pack
    so a pack whose op name collides with core or an already-merged pack is
    skipped with a loud warning rather than raising and killing even the core
    reads. ``pack_loader``/``names`` are injectable so tests can pin a
    deterministic registry; ``loaded_packs``, when supplied, is populated with
    the successfully-merged ``{name: pack}`` so callers (``collect``) can reuse
    the already-loaded pack instead of loading it a second time.
    """
    seen: set[str] = set()
    pack_list: list[str] = []
    for name in list(names) if names is not None else list(pack_names()):
        if name in seen:
            # A duplicate entry-point name (e.g. wheel + editable install of the
            # same pack) would otherwise be loaded and merged twice, and the
            # second merge would collide on every op name. Load each name once.
            log.warning("ignoring duplicate pack entry-point name %r", name)
            continue
        seen.add(name)
        pack_list.append(name)

    accepted: list[Any] = []
    for name in pack_list:
        try:
            pack = pack_loader(name)
        except PackError as exc:
            # An expected, structured pack failure (bad spec, dup op, etc.).
            log.warning("skipping pack %r for CLI registry: %s", name, exc)
            continue
        except Exception as exc:  # noqa: BLE001 - broad by design (see below)
            # An installed pack's entry-point load / factory can raise anything
            # (ImportError, a bug in the pack, a version skew). None of that may
            # take the whole CLI — including the sport-agnostic core reads — down
            # with it. Warn loudly and continue with the remaining packs + core.
            log.warning(
                "skipping pack %r for CLI registry (unexpected %s: %s)",
                name,
                type(exc).__name__,
                exc,
            )
            continue
        # Validate this pack's ops merge cleanly against core, hand-wired CLI
        # commands, injected CLI options, and already-accepted packs before
        # accepting it. A single bad contributed op must not abort the whole CLI.
        pack = _validated_pack_for_cli(name, pack, accepted)
        try:
            build_registry([*accepted, pack])
        except ValueError as exc:
            log.warning(
                "skipping pack %r for CLI registry (duplicate operation name: %s)",
                name,
                exc,
            )
            continue
        accepted.append(pack)
        if loaded_packs is not None:
            loaded_packs[name] = pack
    return build_registry(accepted)


def cli_subcommand_names(registry: Registry) -> tuple[str, ...]:
    """The subcommands :func:`build_parser` creates: registry ops + hand-wired.

    The derivation the single-source-of-truth test asserts against — the read
    subcommands are exactly the registry op names, in registration order,
    followed by the hand-wired ``collect``/``tools``.
    """
    return tuple(registry.names) + HAND_WIRED_COMMANDS


def _add_read_arguments(sub: argparse.ArgumentParser, op: Operation) -> None:
    """Wire one read subcommand's arguments from its operation's params.

    Required params become positionals, optional params ``--<name>`` flags
    (``integer`` → ``type=int``). ``--db`` and ``--json`` are CLI infrastructure,
    not operation params, so they are NOT in the manifest — only the registry's
    declared params are.
    """
    for param in op.params:
        argtype = int if param.type == "integer" else str
        help_text = param.summary or None
        if param.required:
            sub.add_argument(param.name, type=argtype, help=help_text)
        else:
            sub.add_argument(f"--{param.name}", type=argtype, default=param.default, help=help_text)
    sub.add_argument("--db", required=True, help="Path to the gamecollect SQLite database.")
    sub.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Emit the result as JSON (the public --json output contract).",
    )


def build_parser(registry: Registry) -> argparse.ArgumentParser:
    """Build the argparse parser: read subcommands from ``registry`` + hand-wired.

    Every :class:`~gamecollect.registry.Operation` in ``registry`` gets a read
    subcommand; ``collect`` and ``tools`` are added by hand. The subcommand set
    is exactly :func:`cli_subcommand_names` by construction.
    """
    parser = argparse.ArgumentParser(
        prog="gamecollect",
        description="Sport-agnostic live-match data collector CLI.",
    )
    parser.add_argument("--version", action="version", version=f"gamecollect {__version__}")
    subs = parser.add_subparsers(dest="command", metavar="<command>", required=True)

    for op in registry.operations:
        sub = subs.add_parser(op.name, help=op.summary, description=op.summary)
        _add_read_arguments(sub, op)

    collect = subs.add_parser(
        "collect",
        help="Run the collector engine daemon for one source.",
        description=(
            "Load a pack, poll its provider, and write diffed match state into the "
            "shared database until SIGTERM."
        ),
    )
    collect.add_argument("--pack", required=True, help="Installed pack name (entry point).")
    collect.add_argument("--db", required=True, help="SQLite database to write into.")
    collect.add_argument("--source", required=True, help="Partition this daemon owns.")
    collect.add_argument(
        "--record",
        default=None,
        help="Write a replayable fixture of the session here (file or directory).",
    )
    collect.add_argument(
        "--no-finished-backfill",
        action="store_true",
        help=(
            "Disable one-time event backfill for a match first seen already "
            "FINISHED (default: enabled)."
        ),
    )

    tools = subs.add_parser(
        "tools",
        help="Emit the operation manifest (runtime capability discovery).",
        description="Emit the {contract_version, operations} manifest from the registry.",
    )
    tools.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Emit the manifest as JSON (the machine-readable form).",
    )

    return parser


def _format_item(item: Any) -> str:
    """Render one result row for the non-JSON text output."""
    if isinstance(item, dict):
        return "  ".join(f"{key}={value}" for key, value in item.items())
    return str(item)


def _render_text(payload: Any) -> None:
    """Human-readable fallback when ``--json`` is not requested."""
    if payload is None:
        print("(no result)")
        return
    if isinstance(payload, list):
        if not payload:
            print("(no rows)")
            return
        for item in payload:
            print(_format_item(item))
        return
    print(_format_item(payload))


def _run_read(op: Operation, args: argparse.Namespace) -> int:
    """Execute a registry read operation and print its result."""
    kwargs = {param.name: getattr(args, param.name) for param in op.params}
    conn = reader.open_reader(args.db)
    try:
        result = op.impl(conn, **kwargs)
    finally:
        conn.close()
    payload = op.to_json(result)
    if args.as_json:
        print(json.dumps(payload, indent=2))
    else:
        _render_text(payload)
    return 0


def _run_tools(registry: Registry, args: argparse.Namespace) -> int:
    """Emit the manifest generated from the registry (never hand-written)."""
    if args.as_json:
        print(json.dumps(registry.manifest(), indent=2))
    else:
        for op in registry.operations:
            params = ", ".join(f"{p.name}:{p.type}{'' if p.required else '?'}" for p in op.params)
            print(f"{op.name}({params}) — {op.summary}")
    return 0


def _default_runner(engine: CollectorEngine) -> None:
    engine.run()


def _run_collect(
    args: argparse.Namespace,
    *,
    provider: Any = None,
    engine_factory: Callable[..., CollectorEngine] = CollectorEngine,
    runner: Callable[[CollectorEngine], None] | None = None,
    pack_loader: Callable[[str], Any] = load_pack,
    loaded_packs: dict[str, Any] | None = None,
) -> int:
    """Run the collector engine for one source.

    Reuses the pack already loaded during registry construction when available
    (``loaded_packs``) so ``main`` does not load and validate every installed
    pack once for the registry and then load the target pack a second time here;
    falls back to ``pack_loader`` when the target was not among the merged packs
    (e.g. a directly-injected registry, or a pack skipped from the registry).
    Constructs a :class:`CollectorEngine`, then hands it to ``runner`` (defaults
    to :func:`_default_runner`, which blocks in ``engine.run()`` until SIGTERM).
    ``provider``/``engine_factory``/``runner``/``pack_loader`` are injection
    seams: the in-phase unit test drives a single poll with a fake provider and a
    one-shot runner instead of the real ESPN provider and blocking loop.
    """
    pack = (loaded_packs or {}).get(args.pack)
    if pack is None:
        try:
            pack = pack_loader(args.pack)
        except PackNotFoundError:
            print(f"error: no pack named {args.pack!r}", file=sys.stderr)
            return 2
        except PackError as exc:
            print(f"error: pack {args.pack!r} failed to load: {exc}", file=sys.stderr)
            return 2
    engine = engine_factory(
        pack,
        args.db,
        args.source,
        provider=provider,
        record_path=args.record,
        backfill_finished_matches=not args.no_finished_backfill,
    )
    try:
        (runner or _default_runner)(engine)
    finally:
        engine.close()
    return 0


def main(
    argv: Sequence[str] | None = None,
    *,
    registry: Registry | None = None,
    provider: Any = None,
    engine_factory: Callable[..., CollectorEngine] = CollectorEngine,
    runner: Callable[[CollectorEngine], None] | None = None,
    pack_loader: Callable[[str], Any] = load_pack,
) -> int:
    """Parse ``argv`` and dispatch to a read op, ``collect``, or ``tools``.

    ``registry`` defaults to the merged core+installed-packs registry
    (:func:`load_registry`). The keyword injection seams (``registry``,
    ``provider``, ``engine_factory``, ``runner``, ``pack_loader``) let the
    in-phase tests pin a deterministic registry and drive ``collect`` against a
    fake provider; the console-script call ``main()`` uses live defaults.
    """
    loaded_packs: dict[str, Any] = {}
    if registry is None:
        registry = load_registry(pack_loader=pack_loader, loaded_packs=loaded_packs)
    parser = build_parser(registry)
    args = parser.parse_args(argv)

    command = args.command
    if command in registry:
        return _run_read(registry.get(command), args)
    if command == "collect":
        return _run_collect(
            args,
            provider=provider,
            engine_factory=engine_factory,
            runner=runner,
            pack_loader=pack_loader,
            loaded_packs=loaded_packs,
        )
    if command == "tools":
        return _run_tools(registry, args)
    parser.error(f"unknown command {command!r}")  # unreachable: argparse constrains choices
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
