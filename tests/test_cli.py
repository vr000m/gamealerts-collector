"""Phase 3: ``gamecollect`` CLI + ``tools --json`` manifest.

Plan contract (docs/dev_plans/20260702-feature-collector-interfaces.md §Phase 3):

- ``cli.py`` builds argparse subcommands from the merged operation registry
  (``matches``/``state``/``events``/``standings`` core, plus ``squad``/
  ``player-stats`` from the football pack), plus hand-wired ``collect`` and
  ``tools``. Every read subcommand takes ``--json``; ``tools --json`` emits the
  ``{contract_version, operations}`` manifest generated from the registry.
- Golden-file tests: ``tests/golden/manifest.json`` plus one sample ``--json``
  output per read operation, checked in. The sample data comes from a synthetic
  ``PartitionWriter``-seeded temp DB (the Phase 2 pattern), NOT Phase 4
  fixtures, so this phase is committable before Phase 4. Goldens are generated
  by running the real CLI code paths (:func:`gamecollect.cli.main`) and checked
  in; these tests re-run the same seed + CLI and byte-compare. The one
  nondeterministic column (``matches.updated_at``, auto-stamped by the writer)
  is pinned at seed time so the outputs are fully deterministic.
- Single-source-of-truth test: derive at runtime and assert
  ``set(registry op names) == set(CLI read-subparser names)
  == set(manifest operation names)``, and that each manifest entry's
  ``params``/``output_schema`` is the registry object's, not a literal.
- ``collect`` is unit-tested in-phase with a fake provider driven through the
  ``main(...)`` injection seams (single poll); the ReplayProvider end-to-end run
  is Phase 4's job.

Regenerating goldens: run with ``UPDATE_GOLDENS=1`` in the environment (e.g.
``UPDATE_GOLDENS=1 uv run pytest tests/test_cli.py -q``) to rewrite the checked-in
golden files from the current CLI output, then review the diff.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from gamecollect import cli
from gamecollect.db.connection import connect
from gamecollect.db.writer import CrossPartitionError, PartitionWriter
from gamecollect.fold import fold
from gamecollect.provider import MatchStatus, NormalizedMatch

PACK_NAME = "football-wc2026"
SRC = "wc2026-test"
MATCH_ID = f"{SRC}:golden"
# Pinned so ``matches.updated_at`` (the one column the writer auto-stamps with
# wall-clock time) is deterministic and the golden bytes are stable.
PINNED_UPDATED_AT = "2026-06-18T20:45:00+00:00"

GOLDEN_DIR = Path(__file__).parent / "golden"


# --------------------------------------------------------------------------- #
# Deterministic synthetic seed (PartitionWriter — the Phase 2 seeding pattern)
# --------------------------------------------------------------------------- #


def _seed_golden_db(db_path: Path) -> None:
    """Seed one source with rows for every read op: core tables + football side.

    A single FINISHED 6-0 match (Canada-Qatar, David hat-trick) with events,
    standings, a lineup, and team stats — enough that ``matches``/``state``/
    ``events``/``standings``/``squad``/``player-stats`` each return a non-empty
    sample. ``updated_at`` is pinned so the golden output is byte-stable.
    """
    pack = cli.load_pack(PACK_NAME)
    conn = connect(db_path, side_table_ddl=pack.side_table_ddl)
    # No taxonomy: this synthetic seed uses plain event-type strings and should
    # not be gated on the pack taxonomy (that is the writer's Phase-1 concern).
    writer = PartitionWriter(conn, SRC)

    writer.upsert_entity({"entity_id": "t-can", "kind": "team", "display_name": "Canada"})
    writer.upsert_entity({"entity_id": "t-qat", "kind": "team", "display_name": "Qatar"})
    writer.upsert_match(
        {
            "match_id": MATCH_ID,
            "kickoff_utc": "2026-06-18T19:00:00+00:00",
            "home_entity": "t-can",
            "away_entity": "t-qat",
            "status": "FINISHED",
            "minute": 90,
            "period": 2,
            "score_home": 6,
            "score_away": 0,
            "display_clock": "90'",
            "payload": {"round": "group", "venue": "BC Place"},
            "updated_at": PINNED_UPDATED_AT,
        }
    )
    # ``detail`` is opaque provider text (e.g. ESPN's raw shortText) for every
    # row here — it is descriptive prose, not a structured field, and must not
    # be parsed for participant identity. ``actor_entity``/``target_entity``
    # are left NULL (unpopulated/reserved, matching the real writer's
    # ``_event_to_row`` — see docs/dev_plans/20260710-...-participants.md);
    # goal-family participant names live in ``payload.scorer``/``.assist``.
    writer.append_events(
        MATCH_ID,
        [
            {
                "match_id": MATCH_ID,
                "seq": 0,
                "type": "goal",
                "minute": 10,
                "period": 1,
                "importance": 3,
                "detail": "Jonathan David scores!",
                "payload": {
                    "team": "Canada",
                    "scorer": "Jonathan David",
                    "assist": "Alphonso Davies",
                },
            },
            {
                "match_id": MATCH_ID,
                "seq": 1,
                "type": "goal",
                "minute": 25,
                "period": 1,
                "importance": 3,
                "detail": "Jonathan David scores again!",
                "payload": {
                    "team": "Canada",
                    "scorer": "Jonathan David",
                    "assist": "Alphonso Davies",
                },
            },
            {
                "match_id": MATCH_ID,
                "seq": 2,
                "type": "yellow_card",
                "minute": 40,
                "period": 1,
                "importance": 1,
                "detail": "Tactical foul",
                "payload": {"team": "Qatar", "player": "Akram Afif"},
            },
        ],
    )
    writer.upsert_standing({"group_key": "A", "entity_id": "t-can", "points": 9, "rank": 1})
    writer.upsert_standing({"group_key": "A", "entity_id": "t-qat", "points": 0, "rank": 4})

    # Football side tables are pack-owned: no PartitionWriter method, so seed
    # them with the same direct INSERT the pack's own writer uses.
    conn.executemany(
        "INSERT INTO football_lineups "
        "(source, match_id, team, athlete_id, display_name, name_folded, "
        "jersey, position, starter, formation_place) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                SRC,
                MATCH_ID,
                "Canada",
                "a1",
                "Jonathan David",
                fold("Jonathan David"),
                "20",
                "F",
                1,
                1,
            ),
            (
                SRC,
                MATCH_ID,
                "Canada",
                "a2",
                "Alphonso Davies",
                fold("Alphonso Davies"),
                "19",
                "D",
                1,
                2,
            ),
            (SRC, MATCH_ID, "Qatar", "a3", "Akram Afif", fold("Akram Afif"), "11", "F", 1, 1),
        ],
    )
    conn.execute(
        "INSERT INTO football_stats "
        "(source, match_id, team, possession, shots, shots_on_target, corners) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (SRC, MATCH_ID, "Canada", 61.5, 15, 9, 7),
    )
    conn.commit()
    conn.close()


def _registry() -> Any:
    """The merged core + football registry the CLI derives its surface from."""
    return cli.load_registry(names=[PACK_NAME])


def _run_cli(capsys, argv: list[str]) -> str:
    """Run the real CLI entry point with a pinned registry; return stdout."""
    rc = cli.main(argv, registry=_registry())
    assert rc == 0, f"CLI exited non-zero for {argv!r}"
    return capsys.readouterr().out


def _assert_golden(name: str, actual: str) -> None:
    """Byte-compare ``actual`` against ``tests/golden/<name>`` (regen on flag)."""
    path = GOLDEN_DIR / name
    if os.environ.get("UPDATE_GOLDENS"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(actual)
    assert path.exists(), (
        f"missing golden {path}; regenerate with UPDATE_GOLDENS=1 uv run pytest tests/test_cli.py"
    )
    assert actual == path.read_text(), (
        f"CLI --json output drifted from golden {name}; if intentional, "
        f"regenerate with UPDATE_GOLDENS=1 and review the diff"
    )


# The read operations, each mapped to the argv that samples its --json output
# and the golden file that output is byte-compared against.
_READ_OP_ARGV: dict[str, list[str]] = {
    "matches": ["matches"],
    "state": ["state", MATCH_ID],
    "events": ["events", MATCH_ID],
    "standings": ["standings", SRC],
    "squad": ["squad", MATCH_ID],
    "player-stats": ["player-stats", MATCH_ID],
}


# --------------------------------------------------------------------------- #
# Golden-file tests: manifest + one --json sample per read operation
# --------------------------------------------------------------------------- #


def test_tools_manifest_matches_golden(capsys):
    out = _run_cli(capsys, ["tools", "--json"])
    _assert_golden("manifest.json", out)


def test_matches_json_matches_golden(tmp_path, capsys):
    db = tmp_path / "cli.db"
    _seed_golden_db(db)
    out = _run_cli(capsys, ["matches", "--db", str(db), "--json"])
    _assert_golden("matches.json", out)


def test_state_json_matches_golden(tmp_path, capsys):
    db = tmp_path / "cli.db"
    _seed_golden_db(db)
    out = _run_cli(capsys, ["state", MATCH_ID, "--db", str(db), "--json"])
    _assert_golden("state.json", out)


def test_events_json_matches_golden(tmp_path, capsys):
    db = tmp_path / "cli.db"
    _seed_golden_db(db)
    out = _run_cli(capsys, ["events", MATCH_ID, "--db", str(db), "--json"])
    _assert_golden("events.json", out)


def test_standings_json_matches_golden(tmp_path, capsys):
    db = tmp_path / "cli.db"
    _seed_golden_db(db)
    out = _run_cli(capsys, ["standings", SRC, "--db", str(db), "--json"])
    _assert_golden("standings.json", out)


def test_squad_json_matches_golden(tmp_path, capsys):
    db = tmp_path / "cli.db"
    _seed_golden_db(db)
    out = _run_cli(capsys, ["squad", MATCH_ID, "--db", str(db), "--json"])
    _assert_golden("squad.json", out)


def test_player_stats_json_matches_golden(tmp_path, capsys):
    db = tmp_path / "cli.db"
    _seed_golden_db(db)
    out = _run_cli(capsys, ["player-stats", MATCH_ID, "--db", str(db), "--json"])
    _assert_golden("player-stats.json", out)


# --------------------------------------------------------------------------- #
# Each golden --json sample conforms to its op's declared output_schema
# --------------------------------------------------------------------------- #
#
# The schema literals in gamecollect.registry / gamecollect_football.operations
# are hand-maintained alongside the dataclasses they describe, so they can drift
# out of sync with the actual --json output undetected. These tests close that
# gap: every golden sample (which IS the real CLI output) is validated against
# the very output_schema object the manifest publishes, so a schema that
# misdescribes its op's output fails here.
#
# No jsonschema dependency (dev deps are pinned to pytest+ruff): the small
# recursive validator below covers exactly the constructs these schemas use —
# `type` (scalar or nullable union), `properties`, `required`, `items`, and
# `oneOf`/`anyOf` branches — plus unknown-key detection.

_JSON_TYPE_CHECKS: dict[str, Callable[[Any], bool]] = {
    "null": lambda v: v is None,
    # bool is a subclass of int in Python; exclude it from integer/number so a
    # stray True never satisfies a numeric schema.
    "boolean": lambda v: isinstance(v, bool),
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
}


def _type_matches(value: Any, type_decl: Any) -> bool:
    """True if ``value`` satisfies a JSON-Schema ``type`` (scalar or list union)."""
    names = [type_decl] if isinstance(type_decl, str) else list(type_decl)
    return any(_JSON_TYPE_CHECKS[name](value) for name in names)


def _schema_errors(value: Any, schema: dict[str, Any], *, path: str = "$") -> list[str]:
    """Return a list of human-readable violations of ``value`` against ``schema``.

    Empty list == valid. Structural, not a full JSON-Schema implementation:
    it handles only the keywords these contract schemas actually use. Unknown
    object keys are flagged unless ``additionalProperties`` is explicitly
    ``True`` — stricter than the JSON-Schema default so a field the dataclass
    emits but the schema forgot to describe (drift) is caught, not ignored.
    """
    for combinator in ("oneOf", "anyOf"):
        if combinator in schema:
            branches = schema[combinator]
            if not any(not _schema_errors(value, b, path=path) for b in branches):
                return [f"{path}: no {combinator} branch matched value {value!r}"]
            return []

    type_decl = schema.get("type")
    if type_decl is not None and not _type_matches(value, type_decl):
        # A wrong type here is the exact drift these tests exist to catch.
        return [f"{path}: expected type {type_decl!r}, got {type(value).__name__} ({value!r})"]

    errors: list[str] = []
    if isinstance(value, dict):
        props: dict[str, Any] = schema.get("properties", {})
        for required in schema.get("required", ()):  # required is optional in these schemas
            if required not in value:
                errors.append(f"{path}: missing required key {required!r}")
        # An object schema that declares no `properties` (e.g. the opaque
        # `payload`) is an anything-goes object — only enforce described-key
        # coverage when the schema actually enumerates its properties.
        described = "properties" in schema
        additional = schema.get("additionalProperties")
        for key, sub in value.items():
            if key in props:
                errors.extend(_schema_errors(sub, props[key], path=f"{path}.{key}"))
            elif described and additional is not True:
                errors.append(f"{path}: key {key!r} not described by schema")
    if isinstance(value, list) and "items" in schema:
        for index, item in enumerate(value):
            errors.extend(_schema_errors(item, schema["items"], path=f"{path}[{index}]"))
    return errors


def test_schema_validator_rejects_wrong_types_and_undescribed_keys():
    """Guard the guard: the validator must actually flag the drift it exists to
    catch (a mistyped field, an extra key), else a green conformance test would
    be meaningless."""
    schema = {
        "type": "object",
        "properties": {"n": {"type": "integer"}, "opt": {"type": ["string", "null"]}},
    }
    assert _schema_errors({"n": 1, "opt": None}, schema) == []
    assert _schema_errors({"n": 1, "opt": "x"}, schema) == []
    assert _schema_errors({"n": "not-an-int", "opt": None}, schema)  # wrong type
    assert _schema_errors({"n": True, "opt": None}, schema)  # bool is not integer
    assert _schema_errors({"n": 1, "surprise": 2}, schema)  # undescribed key
    # oneOf nullable-object (the `state` op's shape).
    nullable = {"oneOf": [schema, {"type": "null"}]}
    assert _schema_errors(None, nullable) == []
    assert _schema_errors({"n": 1}, nullable) == []
    assert _schema_errors({"n": "bad"}, nullable)


def test_every_golden_sample_conforms_to_its_op_output_schema():
    """Each read op's checked-in golden --json sample must validate against the
    exact ``output_schema`` object the registry publishes in the manifest. This
    couples the hand-maintained schema literals to the real serialized output:
    a schema that misdescribes its op (wrong type, forgotten field) fails
    here."""
    registry = _registry()
    for op_name in _READ_OP_ARGV:
        op = registry.get(op_name)
        sample = json.loads((GOLDEN_DIR / f"{op_name}.json").read_text())
        errors = _schema_errors(sample, op.output_schema)
        assert not errors, f"{op_name} golden violates its declared output_schema: {errors}"


# --------------------------------------------------------------------------- #
# tools --json validates as JSON and lists every read op with output schemas
# --------------------------------------------------------------------------- #


def test_tools_json_is_valid_json_listing_every_read_op_with_schemas(capsys):
    registry = _registry()
    out = _run_cli(capsys, ["tools", "--json"])
    manifest = json.loads(out)  # must parse (the Phase-3 validation command)

    op_names = [op["name"] for op in manifest["operations"]]
    # Every registry read op (core + football squad/player-stats) is listed…
    assert set(op_names) == set(registry.names)
    assert {"matches", "state", "events", "standings", "squad", "player-stats"} <= set(op_names)
    # …each carrying a non-empty output schema (the public --json contract).
    for op in manifest["operations"]:
        assert "output_schema" in op, f"manifest op {op['name']} has no output_schema"
        assert op["output_schema"], f"manifest op {op['name']} has an empty output_schema"
        assert isinstance(op["params"], list)


def test_manifest_carries_contract_version(capsys):
    out = _run_cli(capsys, ["tools", "--json"])
    manifest = json.loads(out)
    from gamecollect.registry import CONTRACT_VERSION

    assert manifest["contract_version"] == CONTRACT_VERSION


# --------------------------------------------------------------------------- #
# Single-source-of-truth: registry == CLI subparsers == manifest, by object
# --------------------------------------------------------------------------- #


def _read_subparser_names(parser: argparse.ArgumentParser) -> set[str]:
    """The CLI's read subcommands: all subparsers minus the hand-wired ones."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return set(action.choices) - set(cli.HAND_WIRED_COMMANDS)
    raise AssertionError("parser exposes no subparsers")


def test_registry_cli_manifest_name_sets_are_identical():
    """Derive all three name sets at runtime and assert they are equal — the
    CLI subcommands and the manifest cannot drift from the registry because
    they are generated from it."""
    registry = _registry()
    parser = cli.build_parser(registry)
    manifest = registry.manifest()

    registry_names = set(registry.names)
    cli_names = _read_subparser_names(parser)
    manifest_names = {op["name"] for op in manifest["operations"]}

    assert registry_names == cli_names == manifest_names
    # Sanity: the football pack's contributed ops are in the set.
    assert {"squad", "player-stats"} <= registry_names


def test_manifest_entries_are_the_registry_objects_not_literals():
    """Each manifest entry's params/output_schema must come from the registry
    Operation, not a hand-written duplicate. ``output_schema`` identity (``is``)
    proves the manifest passes the very object through — a literal would be a
    distinct dict."""
    registry = _registry()
    manifest = registry.manifest()
    by_name = {op["name"]: op for op in manifest["operations"]}

    assert set(by_name) == set(registry.names)
    for op in registry.operations:
        entry = by_name[op.name]
        # Same object, not an equal copy: no hand-maintained manifest schema.
        assert entry["output_schema"] is op.output_schema
        # Params are the operation's own ParamSpecs, serialized in order.
        assert entry["params"] == [p.to_manifest() for p in op.params]


def test_cli_subcommand_names_helper_matches_built_parser():
    """``cli_subcommand_names`` is the derivation the ssot test relies on; it
    must equal the parser's actual subcommand set (read ops + hand-wired)."""
    registry = _registry()
    parser = cli.build_parser(registry)
    declared = set(cli.cli_subcommand_names(registry))

    all_subparsers: set[str] = set()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            all_subparsers = set(action.choices)
    assert declared == all_subparsers
    assert set(cli.HAND_WIRED_COMMANDS) <= declared


# --------------------------------------------------------------------------- #
# A broken installed pack is skipped, not fatal — core reads survive it
# --------------------------------------------------------------------------- #


def test_broken_pack_is_skipped_and_core_reads_still_work(tmp_path, capsys, caplog):
    """A pack whose loader raises a non-``PackError`` (ImportError, a bug in the
    pack, version skew) must be skipped with a loud warning — never crash the
    whole CLI. The invariant DESIGN.md pins is that core reads keep working even
    when an installed pack is broken."""
    db = tmp_path / "cli.db"
    _seed_golden_db(db)

    def broken_loader(name: str) -> Any:
        raise ImportError(f"cannot import pack {name!r}")

    with caplog.at_level(logging.WARNING):
        registry = cli.load_registry(pack_loader=broken_loader, names=["busted-pack"])

    # The broken pack contributed nothing; only the sport-agnostic core ops remain.
    assert set(registry.names) == {"matches", "state", "events", "standings", "vocabulary"}
    # It was skipped loudly, naming the offending pack.
    assert any("busted-pack" in rec.getMessage() for rec in caplog.records), (
        "a skipped broken pack must be warned about, not silently dropped"
    )

    # And a core read still runs end to end through the real main() dispatch.
    rc = cli.main(["matches", "--db", str(db), "--json"], registry=registry)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload and payload[0]["match_id"] == MATCH_ID


def test_pack_with_duplicate_op_name_is_skipped_core_survives(tmp_path, caplog):
    """A pack whose contributed op name collides with core (or an earlier pack)
    must be skipped with a loud warning — the unprotected ``build_registry`` used
    to raise ``ValueError`` and kill even the sport-agnostic core reads. Core ops
    survive and a core read still runs end to end."""
    from gamecollect.registry import CORE_OPERATIONS

    class _DupOpPack:
        # Re-contributes the core "matches" op → a duplicate subcommand name.
        operations = (CORE_OPERATIONS[0],)

    with caplog.at_level(logging.WARNING):
        registry = cli.load_registry(pack_loader=lambda name: _DupOpPack(), names=["dup-pack"])

    assert set(registry.names) == {"matches", "state", "events", "standings", "vocabulary"}
    assert any("dup-pack" in rec.getMessage() for rec in caplog.records), (
        "a pack skipped for a duplicate op name must be warned about"
    )

    db = tmp_path / "cli.db"
    _seed_golden_db(db)
    rc = cli.main(["matches", "--db", str(db), "--json"], registry=registry)
    assert rc == 0


def _test_operation(name: str, *param_names: str):
    from gamecollect.registry import Operation, ParamSpec

    return Operation(
        name=name,
        summary=f"Test operation {name}",
        params=tuple(
            ParamSpec(param_name, "string", required=False, summary=f"{param_name} filter")
            for param_name in param_names
        ),
        output_schema={"type": "array"},
        impl=lambda conn, **kwargs: [],
    )


@pytest.mark.parametrize("hand_wired_name", ["collect", "backfill", "tools"])
def test_pack_op_name_collision_with_hand_wired_command_raises(hand_wired_name):
    """A pack op that claims a hand-wired command name (``collect``/``backfill``/
    ``tools``) is a hard error, not a droppable op: the hand-wired parser would
    silently shadow it and it would never dispatch. Surface it loudly rather than
    warn-and-drop."""

    class _CollisionPack:
        operations = (_test_operation(hand_wired_name),)

    with pytest.raises(ValueError, match=f"hand-wired command {hand_wired_name!r}"):
        cli._validated_pack_for_cli("bad-op-pack", _CollisionPack(), [])


def test_pack_op_name_collision_with_hand_wired_command_propagates_through_load_registry():
    """The hard error surfaces through ``load_registry`` too — it is raised before
    the per-pack ``build_registry`` guard, so it is not swallowed as a soft skip."""

    class _CollisionPack:
        operations = (_test_operation("backfill"),)

    with pytest.raises(ValueError, match="hand-wired command 'backfill'"):
        cli.load_registry(pack_loader=lambda name: _CollisionPack(), names=["bad-op-pack"])


def test_pack_op_reserved_cli_option_collision_is_skipped(caplog):
    class _CollisionPack:
        operations = (
            _test_operation("bad-db", "db"),
            _test_operation("bad-json", "json"),
            _test_operation("bad-help", "help"),
            _test_operation("ok-pack-op", "team"),
        )

    with caplog.at_level(logging.WARNING):
        registry = cli.load_registry(
            pack_loader=lambda name: _CollisionPack(), names=["bad-option-pack"]
        )

    assert "bad-db" not in registry.names
    assert "bad-json" not in registry.names
    # argparse auto-adds --help to every subparser; an op with a param named
    # "help" would raise ArgumentError at sub.add_argument and kill the CLI.
    assert "bad-help" not in registry.names
    assert "ok-pack-op" in registry.names
    assert any("bad-db" in rec.getMessage() and "db" in rec.getMessage() for rec in caplog.records)
    assert any(
        "bad-json" in rec.getMessage() and "json" in rec.getMessage() for rec in caplog.records
    )
    assert any(
        "bad-help" in rec.getMessage() and "help" in rec.getMessage() for rec in caplog.records
    )
    cli.build_parser(registry)


def test_duplicate_pack_entry_point_name_loaded_once(caplog):
    """A duplicate entry-point name (wheel + editable install register the same
    name twice) is de-duplicated before loading, so the pack is loaded once and
    its ops are not merged twice (which would itself collide)."""

    class _NoOpPack:
        operations = ()

    loads: list[str] = []

    def counting_loader(name: str) -> Any:
        loads.append(name)
        return _NoOpPack()

    with caplog.at_level(logging.WARNING):
        registry = cli.load_registry(pack_loader=counting_loader, names=[PACK_NAME, PACK_NAME])

    assert loads == [PACK_NAME], f"a duplicate entry-point name must load once, got {loads}"
    assert set(registry.names) == {"matches", "state", "events", "standings", "vocabulary"}


def test_collect_reuses_registry_loaded_pack_without_double_load(tmp_path, monkeypatch):
    """``collect`` must reuse the pack already loaded while building the registry
    rather than loading and validating it a second time. With ``registry`` left to
    default (built inside ``main``), the injected loader is invoked exactly once —
    during registry construction — and ``collect`` picks the same object up."""

    class _NoOpPack:
        operations = ()

    monkeypatch.setattr(cli, "pack_names", lambda: [PACK_NAME])
    sentinel_pack = _NoOpPack()
    calls: list[str] = []

    def counting_loader(name: str) -> Any:
        calls.append(name)
        return sentinel_pack

    built: list[_FakeEngine] = []

    def engine_factory(
        pack, db_path, source, *, provider, record_path, backfill_finished_matches=True
    ):  # noqa: A002
        engine = _FakeEngine(
            pack,
            db_path,
            source,
            provider=provider,
            record_path=record_path,
            backfill_finished_matches=backfill_finished_matches,
        )
        built.append(engine)
        return engine

    rc = cli.main(
        ["collect", "--pack", PACK_NAME, "--db", str(tmp_path / "c.db"), "--source", "s"],
        provider=_FakeProvider(),
        engine_factory=engine_factory,
        runner=lambda engine: engine.poll_once(),
        pack_loader=counting_loader,
    )

    assert rc == 0
    assert calls == [PACK_NAME], f"the pack must be loaded exactly once, got {calls}"
    assert built and built[0].pack is sentinel_pack, "collect must reuse the registry-loaded pack"


# --------------------------------------------------------------------------- #
# collect — unit-tested via the main(...) injection seams (single fake poll)
# --------------------------------------------------------------------------- #


class _FakeProvider:
    """A provider stub whose ``fetch_live_matches`` records each poll."""

    def __init__(self) -> None:
        self.polls = 0

    def fetch_live_matches(self) -> list[Any]:
        self.polls += 1
        return []


class _FakeEngine:
    """Records the args the CLI constructs it with and its lifecycle calls."""

    def __init__(
        self,
        pack: Any,
        db: Any,
        source: Any,
        *,
        provider: Any,
        record_path: Any,
        backfill_finished_matches: bool = True,
    ) -> None:
        self.pack = pack
        self.db = db
        self.source = source
        self.provider = provider
        self.record_path = record_path
        self.backfill_finished_matches = backfill_finished_matches
        self.closed = False
        self.poll_count = 0

    def poll_once(self) -> None:
        self.poll_count += 1
        self.provider.fetch_live_matches()

    def close(self) -> None:
        self.closed = True


def test_collect_wires_pack_provider_engine_and_runs_one_poll(tmp_path):
    """``gamecollect collect`` loads the pack via ``pack_loader``, constructs the
    engine via ``engine_factory`` with the injected provider / db / source /
    record path, runs the (one-shot) runner, and closes the engine — proven
    entirely through the ``main(...)`` seams, no real engine or network."""
    db = tmp_path / "collect.db"
    record = tmp_path / "session.json"
    provider = _FakeProvider()
    sentinel_pack = object()
    loaded: list[str] = []
    built: list[_FakeEngine] = []

    def fake_pack_loader(name: str) -> Any:
        loaded.append(name)
        return sentinel_pack

    def engine_factory(
        pack, db_path, source, *, provider, record_path, backfill_finished_matches=True
    ):  # noqa: A002
        engine = _FakeEngine(
            pack,
            db_path,
            source,
            provider=provider,
            record_path=record_path,
            backfill_finished_matches=backfill_finished_matches,
        )
        built.append(engine)
        return engine

    def one_shot_runner(engine: _FakeEngine) -> None:
        engine.poll_once()

    rc = cli.main(
        [
            "collect",
            "--pack",
            PACK_NAME,
            "--db",
            str(db),
            "--source",
            "test-src",
            "--record",
            str(record),
        ],
        registry=_registry(),
        provider=provider,
        engine_factory=engine_factory,
        runner=one_shot_runner,
        pack_loader=fake_pack_loader,
    )

    assert rc == 0
    assert loaded == [PACK_NAME], "collect must load the named pack via pack_loader"
    assert len(built) == 1, "collect must construct exactly one engine"
    engine = built[0]
    assert engine.pack is sentinel_pack
    assert engine.db == str(db)
    assert engine.source == "test-src"
    assert engine.provider is provider, "the injected provider must reach the engine"
    assert str(engine.record_path) == str(record)
    assert engine.poll_count == 1, "the one-shot runner must poll exactly once"
    assert provider.polls == 1, "the single poll must reach the fake provider"
    assert engine.closed, "collect must close the engine after the runner returns"
    assert engine.backfill_finished_matches is True, (
        "absent --no-finished-backfill, the engine must be constructed with "
        "backfill_finished_matches=True"
    )


def test_collect_no_finished_backfill_flag_constructs_engine_with_backfill_disabled(tmp_path):
    """``gamecollect collect --no-finished-backfill`` must construct the
    engine with ``backfill_finished_matches=False`` — the Phase 1 kill
    switch, shipped alongside the constructor flag it wires into, not
    deferred to a later phase."""
    db = tmp_path / "collect.db"
    provider = _FakeProvider()
    built: list[_FakeEngine] = []

    def engine_factory(
        pack, db_path, source, *, provider, record_path, backfill_finished_matches=True
    ):  # noqa: A002
        engine = _FakeEngine(
            pack,
            db_path,
            source,
            provider=provider,
            record_path=record_path,
            backfill_finished_matches=backfill_finished_matches,
        )
        built.append(engine)
        return engine

    rc = cli.main(
        [
            "collect",
            "--pack",
            PACK_NAME,
            "--db",
            str(db),
            "--source",
            "test-src",
            "--no-finished-backfill",
        ],
        registry=_registry(),
        provider=provider,
        engine_factory=engine_factory,
        runner=lambda engine: engine.poll_once(),
        pack_loader=lambda name: object(),
    )

    assert rc == 0
    assert len(built) == 1
    assert built[0].backfill_finished_matches is False, (
        "--no-finished-backfill must construct the engine with backfill_finished_matches=False"
    )


def test_collect_absent_no_finished_backfill_flag_defaults_engine_backfill_to_true(tmp_path):
    """Absence of ``--no-finished-backfill`` must default the engine's
    ``backfill_finished_matches`` to ``True`` — the feature ships ON by
    default from the moment Phase 1 lands."""
    db = tmp_path / "collect.db"
    provider = _FakeProvider()
    built: list[_FakeEngine] = []

    def engine_factory(
        pack, db_path, source, *, provider, record_path, backfill_finished_matches=True
    ):  # noqa: A002
        engine = _FakeEngine(
            pack,
            db_path,
            source,
            provider=provider,
            record_path=record_path,
            backfill_finished_matches=backfill_finished_matches,
        )
        built.append(engine)
        return engine

    rc = cli.main(
        ["collect", "--pack", PACK_NAME, "--db", str(db), "--source", "test-src"],
        registry=_registry(),
        provider=provider,
        engine_factory=engine_factory,
        runner=lambda engine: engine.poll_once(),
        pack_loader=lambda name: object(),
    )

    assert rc == 0
    assert len(built) == 1
    assert built[0].backfill_finished_matches is True, (
        "absent --no-finished-backfill, the engine must default to backfill_finished_matches=True"
    )


def test_collect_closes_engine_even_when_runner_raises(tmp_path):
    """The ``finally: engine.close()`` contract holds when the runner blows up —
    a crashing collect run must not leak the engine's DB connection."""
    db = tmp_path / "collect.db"
    built: list[_FakeEngine] = []

    def engine_factory(
        pack, db_path, source, *, provider, record_path, backfill_finished_matches=True
    ):  # noqa: A002
        engine = _FakeEngine(
            pack,
            db_path,
            source,
            provider=provider,
            record_path=record_path,
            backfill_finished_matches=backfill_finished_matches,
        )
        built.append(engine)
        return engine

    def exploding_runner(engine: _FakeEngine) -> None:
        raise RuntimeError("boom")

    try:
        cli.main(
            ["collect", "--pack", PACK_NAME, "--db", str(db), "--source", "s"],
            registry=_registry(),
            provider=_FakeProvider(),
            engine_factory=engine_factory,
            runner=exploding_runner,
            pack_loader=lambda name: object(),
        )
    except RuntimeError as exc:
        assert str(exc) == "boom"
    else:  # pragma: no cover - the runner must propagate its error
        raise AssertionError("collect swallowed the runner's exception")

    assert built and built[0].closed, "engine must be closed even when the runner raises"


def test_collect_unknown_pack_exits_nonzero_with_one_line_error(tmp_path, capsys):
    """``collect --pack nonexistent`` must exit non-zero with a clean one-line
    error on stderr — never a raw ``PackNotFoundError`` traceback through the
    ``raise SystemExit(main())`` entry point. Uses the real default
    ``pack_loader`` (``load_pack``) so the real not-found path is exercised."""
    rc = cli.main(
        [
            "collect",
            "--pack",
            "nonexistent",
            "--db",
            str(tmp_path / "c.db"),
            "--source",
            "s",
        ],
        registry=_registry(),
    )

    assert rc != 0, "a missing pack must exit non-zero"
    captured = capsys.readouterr()
    assert captured.out == "", "the error must go to stderr, not stdout"
    assert "Traceback" not in captured.err
    error_lines = [line for line in captured.err.splitlines() if line]
    assert error_lines == ["error: no pack named 'nonexistent'"]


def test_collect_broken_pack_exits_nonzero_with_one_line_error(tmp_path, capsys):
    """A pack that exists but fails to load (any ``PackError``) surfaces as a
    clean one-line stderr error and a non-zero exit, not a traceback."""
    from gamecollect.packs.registry import PackValidationError

    def broken_loader(name: str) -> Any:
        raise PackValidationError("provider factory raised: boom")

    rc = cli.main(
        ["collect", "--pack", PACK_NAME, "--db", str(tmp_path / "c.db"), "--source", "s"],
        registry=_registry(),
        pack_loader=broken_loader,
    )

    assert rc != 0, "a broken pack must exit non-zero"
    captured = capsys.readouterr()
    assert captured.out == "", "the error must go to stderr, not stdout"
    assert "Traceback" not in captured.err
    error_lines = [line for line in captured.err.splitlines() if line]
    assert error_lines == [
        f"error: pack {PACK_NAME!r} failed to load: provider factory raised: boom"
    ]


def test_vocabulary_unknown_pack_exits_nonzero_with_one_line_error(tmp_path, capsys):
    """Round 3 review finding: ``vocabulary`` routes through ``_run_read``,
    whose impl (``client.get_vocabulary``) loads the pack internally and raises
    ``PackNotFoundError`` for an unknown name. Without a guard the CLI printed a
    raw traceback, unlike ``collect``/``backfill`` which surface a clean
    one-line stderr error and exit 2 for the same invalid-pack case. This pins
    that ``vocabulary`` matches that convention. Uses the real default
    ``load_pack`` (get_vocabulary imports it directly) so the real not-found
    path is exercised; a valid seeded DB so ``open_reader`` succeeds and the
    pack load is what fails."""
    db = tmp_path / "vocab.db"
    _seed_golden_db(db)

    rc = cli.main(["vocabulary", "nonexistent", "--db", str(db)], registry=_registry())

    assert rc == 2, "an unknown pack must exit 2 like collect/backfill"
    captured = capsys.readouterr()
    assert captured.out == "", "the error must go to stderr, not stdout"
    assert "Traceback" not in captured.err
    error_lines = [line for line in captured.err.splitlines() if line]
    assert error_lines == ["error: no pack named 'nonexistent'"]


# --------------------------------------------------------------------------- #
# backfill — unit-tested via the main(...) injection seams, mirroring collect
# --------------------------------------------------------------------------- #
#
# Phase 3 contract (docs/dev_plans/20260718-feature-historical-backfill.md
# §Phase 3): ``backfill`` must match ``collect``'s injection-seam pattern
# exactly (provider/engine_factory/pack_loader overrides via cli.main(...))
# so these tests never hit real ESPN or real time. ``_run_backfill`` checks
# the provider's historical-enumeration capability (fetch_schedule AND
# fetch_match_detail) BEFORE constructing the engine, calls
# ``backfill.run_backfill(...)``, prints a BackfillReport summary, always
# closes the engine, and is the sole catcher of CrossPartitionError (a
# run-wide --source misconfiguration), surfacing a clean non-zero exit
# rather than a raw traceback.

BACKFILL_MATCH_ID = f"{SRC}:bf-1"


def _bf_match(match_id: str = BACKFILL_MATCH_ID) -> NormalizedMatch:
    return NormalizedMatch(
        match_id=match_id,
        status=MatchStatus.FINISHED,
        minute=90,
        score_home=1,
        score_away=0,
        display_clock="FT",
        events=[],
        home_team="Canada",
        away_team="Qatar",
        kickoff_utc="2026-01-10T18:00:00Z",
        payload={},
    )


class _FakeBackfillProvider:
    """Historical-enumeration-capable double: every ``fetch_schedule`` chunk
    returns the same scripted match (regardless of the exact day-chunk keys
    ``_run_backfill`` derives from ``chunk_date_range``, so these tests don't
    couple to the overlap-padding implementation detail), and
    ``fetch_match_detail`` returns the scripted detail for a known id."""

    def __init__(self, match: NormalizedMatch | None = None) -> None:
        if match is None:
            match = _bf_match()
        self._match = match
        self.schedule_calls: list[str] = []
        self.detail_calls: list[str] = []

    def fetch_schedule(self, *, dates: str) -> list[NormalizedMatch]:
        self.schedule_calls.append(dates)
        return [self._match] if self._match is not None else []

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        self.detail_calls.append(match_id)
        return self._match if self._match is not None else _bf_match(match_id)


class _ScheduleOnlyProvider:
    """Has ``fetch_schedule`` but lacks ``fetch_match_detail`` entirely."""

    def fetch_schedule(self, *, dates: str) -> list[NormalizedMatch]:
        return []


class _DetailOnlyProvider:
    """Has ``fetch_match_detail`` but lacks ``fetch_schedule`` entirely — the
    ordinary live ``collect`` provider shape, which does not support
    backfill's historical enumeration."""

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        return _bf_match(match_id)


class _FakeBackfillEngine:
    """A CollectorEngine-shaped double: the exact read/write surface
    ``backfill.run_backfill`` uses (``.source``, ``.stored_source``,
    ``.has_events``, ``.apply_one_off_match``), plus ``.close()`` for the
    CLI's lifecycle contract. Accepts (and ignores) whatever extra
    positional/keyword args the real ``engine_factory`` call site passes
    (e.g. a poll interval, ``record_path``) so this test does not couple to
    that exact call shape."""

    def __init__(
        self,
        pack: Any,
        db: Any,
        source: Any,
        *args: Any,
        provider: Any = None,
        apply_raises: BaseException | None = None,
        **kwargs: Any,
    ) -> None:
        self.pack = pack
        self.db = db
        self._source = source
        self.provider = provider
        self._apply_raises = apply_raises
        self.closed = False
        self.apply_calls: list[Any] = []

    @property
    def source(self) -> Any:
        return self._source

    def stored_source(self, match_id: str) -> str | None:
        return None

    def has_events(self, match_id: str) -> bool:
        return False

    def stored_source_and_has_events(self, match_id: str) -> tuple[str | None, bool]:
        return None, False

    def apply_one_off_match(self, scoreboard: NormalizedMatch, detail: NormalizedMatch) -> Any:
        self.apply_calls.append((scoreboard, detail))
        if self._apply_raises is not None:
            raise self._apply_raises
        return scoreboard

    def close(self) -> None:
        self.closed = True


_BACKFILL_ARGV_BASE = [
    "backfill",
    "--pack",
    PACK_NAME,
    "--source",
    SRC,
    "--start-date",
    "2026-01-10",
    "--end-date",
    "2026-01-10",
]


def test_backfill_command_is_hand_wired():
    """``backfill`` must be a hand-wired command (not a registry read op) —
    the sequencing-lens correction requires it land in ``HAND_WIRED_COMMANDS``
    in the same commit as the subparser, or the pack-name collision guard and
    ``test_cli_subcommand_names_helper_matches_built_parser`` misclassify it."""
    assert "backfill" in cli.HAND_WIRED_COMMANDS


def test_backfill_subparser_declares_required_flags():
    """argparse wiring: ``--pack``/``--db``/``--source`` and the date-range
    flags are all required on the ``backfill`` subcommand."""
    registry = _registry()
    parser = cli.build_parser(registry)
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            backfill_parser = action.choices["backfill"]
            break
    else:  # pragma: no cover
        raise AssertionError("parser exposes no subparsers")

    required_dests = {
        action.dest for action in backfill_parser._actions if getattr(action, "required", False)
    }
    assert "pack" in required_dests
    assert "db" in required_dests
    assert "source" in required_dests
    # Plan allows either --start-date/--end-date or a single --dates
    # START:END flag; assert whichever form is wired is present and required.
    has_split_dates = {"start_date", "end_date"} <= required_dests
    has_combined_dates = "dates" in required_dests
    assert has_split_dates or has_combined_dates, (
        f"backfill must require a date-range flag (--start-date/--end-date or "
        f"--dates), got required dests {required_dests!r}"
    )


def test_backfill_help_text_states_cross_source_constraint(capsys):
    """The ``--source`` help text must state the cross-source constraint from
    Requirements: a backfill run's ``--source`` must equal the live daemon's,
    or it will raise ``CrossPartitionError`` (Requirements: "must be
    documented as 'must equal the live daemon's `--source`'")."""
    registry = _registry()
    parser = cli.build_parser(registry)
    with pytest.raises(SystemExit):
        parser.parse_args(["backfill", "--help"])
    out = capsys.readouterr().out.lower()
    assert "source" in out
    assert (
        "live daemon" in out or "same source" in out or "must match" in out or "must equal" in out
    )


def test_backfill_wires_provider_engine_and_closes_engine(tmp_path):
    """``gamecollect backfill`` loads the pack via ``pack_loader``, resolves
    the injected provider, checks its historical-enumeration capability,
    constructs the engine via ``engine_factory``, drives
    ``backfill.run_backfill`` against it (proven by the fake provider/engine
    seeing calls), prints a report, and always closes the engine — entirely
    through the ``main(...)`` seams, no real ESPN or real time."""
    db = tmp_path / "backfill.db"
    provider = _FakeBackfillProvider()
    sentinel_pack = object()
    loaded: list[str] = []
    built: list[_FakeBackfillEngine] = []

    def fake_pack_loader(name: str) -> Any:
        loaded.append(name)
        return sentinel_pack

    def engine_factory(pack, db_path, source, *args: Any, **kwargs: Any) -> _FakeBackfillEngine:
        engine = _FakeBackfillEngine(pack, db_path, source, *args, **kwargs)
        built.append(engine)
        return engine

    rc = cli.main(
        [*_BACKFILL_ARGV_BASE, "--db", str(db)],
        registry=_registry(),
        provider=provider,
        engine_factory=engine_factory,
        pack_loader=fake_pack_loader,
    )

    assert rc == 0, "a clean backfill run must exit zero"
    assert loaded == [PACK_NAME], "backfill must load the named pack via pack_loader"
    assert len(built) == 1, "backfill must construct exactly one engine"
    engine = built[0]
    assert engine.provider is provider, "the injected provider must reach run_backfill"
    assert provider.schedule_calls, "run_backfill must have called fetch_schedule at least once"
    assert engine.apply_calls, "the enumerated terminal match must have been applied"
    assert engine.closed, "backfill must close the engine after the run completes"


def test_backfill_prints_report_summary(tmp_path, capsys):
    """``_run_backfill`` prints a summary derived from the ``BackfillReport``
    counts (not silence) on a successful run."""
    db = tmp_path / "backfill.db"
    provider = _FakeBackfillProvider()

    def engine_factory(pack, db_path, source, *args: Any, **kwargs: Any) -> _FakeBackfillEngine:
        return _FakeBackfillEngine(pack, db_path, source, *args, **kwargs)

    rc = cli.main(
        [*_BACKFILL_ARGV_BASE, "--db", str(db)],
        registry=_registry(),
        provider=provider,
        engine_factory=engine_factory,
        pack_loader=lambda name: object(),
    )

    assert rc == 0
    out = capsys.readouterr().out
    assert out.strip(), "backfill must print a BackfillReport summary, not run silently"


def test_backfill_provider_missing_fetch_schedule_returns_clear_error_before_engine(
    tmp_path, capsys
):
    """A provider lacking ``fetch_schedule`` must fail the capability check
    immediately with a clear, non-traceback error and non-zero exit — and,
    per the reordered capability check (`/review-plan` 2026-07-19,
    architecture lens, Minor), the engine must never be constructed, so an
    unsupported-provider invocation never creates/stamps a DB file it's
    about to fail out of."""
    db = tmp_path / "backfill.db"
    provider = _DetailOnlyProvider()
    built: list[Any] = []

    def engine_factory(pack, db_path, source, *args: Any, **kwargs: Any) -> _FakeBackfillEngine:
        engine = _FakeBackfillEngine(pack, db_path, source, *args, **kwargs)
        built.append(engine)
        return engine

    rc = cli.main(
        [*_BACKFILL_ARGV_BASE, "--db", str(db)],
        registry=_registry(),
        provider=provider,
        engine_factory=engine_factory,
        pack_loader=lambda name: object(),
    )

    assert rc != 0, "a provider lacking fetch_schedule must exit non-zero"
    assert not built, "the engine must not be constructed when the capability check fails"
    assert not db.exists(), "no DB file may be created/stamped when the capability check fails"
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert captured.err.strip(), "the capability-check failure must print a clear error"


def test_backfill_provider_missing_fetch_match_detail_returns_clear_error_before_engine(
    tmp_path, capsys
):
    """A provider with ``fetch_schedule`` but lacking ``fetch_match_detail``
    must ALSO fail the capability check before the engine is constructed —
    the reordered check (`/review-plan` 2026-07-19) covers both methods, not
    just the fetch_schedule-absent case (`/review-plan` 2026-07-19,
    spec-and-testing lens, Minor: this exact gap had no test before)."""
    db = tmp_path / "backfill.db"
    provider = _ScheduleOnlyProvider()
    built: list[Any] = []

    def engine_factory(pack, db_path, source, *args: Any, **kwargs: Any) -> _FakeBackfillEngine:
        engine = _FakeBackfillEngine(pack, db_path, source, *args, **kwargs)
        built.append(engine)
        return engine

    rc = cli.main(
        [*_BACKFILL_ARGV_BASE, "--db", str(db)],
        registry=_registry(),
        provider=provider,
        engine_factory=engine_factory,
        pack_loader=lambda name: object(),
    )

    assert rc != 0, "a provider lacking fetch_match_detail must exit non-zero"
    assert not built, "the engine must not be constructed when the capability check fails"
    assert not db.exists(), "no DB file may be created/stamped when the capability check fails"
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert captured.err.strip(), "the capability-check failure must print a clear error"


def test_backfill_cross_partition_error_caught_with_actionable_message(tmp_path, capsys):
    """``CrossPartitionError`` raised out of ``run_backfill`` (a run-wide
    ``--source`` misconfiguration) must be the ONE exception ``_run_backfill``
    catches specially: non-zero exit, actionable stderr message (naming the
    offending match id and that ``--source`` must match the live daemon's),
    never a raw traceback."""
    db = tmp_path / "backfill.db"
    provider = _FakeBackfillProvider()
    cross_partition_error = CrossPartitionError(
        f"match {BACKFILL_MATCH_ID} is owned by source 'wc2026-live', not {SRC!r}"
    )

    def engine_factory(pack, db_path, source, *args: Any, **kwargs: Any) -> _FakeBackfillEngine:
        return _FakeBackfillEngine(
            pack, db_path, source, *args, apply_raises=cross_partition_error, **kwargs
        )

    rc = cli.main(
        [*_BACKFILL_ARGV_BASE, "--db", str(db)],
        registry=_registry(),
        provider=provider,
        engine_factory=engine_factory,
        pack_loader=lambda name: object(),
    )

    assert rc != 0, "a CrossPartitionError from run_backfill must exit non-zero"
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err, (
        "CrossPartitionError must be caught by _run_backfill, never surface as a raw traceback"
    )
    assert captured.err.strip(), "the CrossPartitionError path must print an actionable message"
    assert BACKFILL_MATCH_ID in captured.err or SRC in captured.err, (
        "the actionable message should name the offending match id and/or source, not just "
        "a generic failure"
    )
