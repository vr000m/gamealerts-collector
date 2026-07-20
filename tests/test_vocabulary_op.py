"""Phase 4: pack-manifest ``vocabulary`` read operation.

Plan contract (docs/dev_plans/20260707-feature-gameworker-integration.md
§Phase 4):

- A new read operation ``vocabulary`` (a core-registered registry
  ``Operation`` — it takes an installed pack name and is not itself a
  database read; see ``src/gamecollect/registry.py``'s ``CORE_OPERATIONS``
  and ``src/gamecollect/client.py``'s ``get_vocabulary``/``Vocabulary``)
  returns the named pack's manifest for an LLM: ``taxonomy`` (event type ->
  ``{display_name, importance_default}``), ``prompt_fragments``,
  ``display_metadata``, and ``compaction_boundaries``.
- It is exposed via the client library, the CLI (a ``pack`` positional plus
  the usual ``--db``/``--json``, matching the existing core-op wiring — see
  ``tests/test_cli.py``), and the ``tools --json`` manifest, which carries
  ``contract_version``.
- A golden file (``tests/golden/vocabulary_football.json``) pins the exact
  football-pack vocabulary output.

These tests target the football pack (``football-wc2026``) as the concrete
pack under test, sourcing expected taxonomy/prompt/display-metadata/
compaction-boundary values from ``gamecollect_football.pack.pack()`` and
``gamecollect_football.taxonomy.TAXONOMY`` directly, so they cannot drift
from the pack's own literals.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from gamecollect import cli
from gamecollect.db.connection import connect
from gamecollect.db.reader import open_reader
from gamecollect.registry import CONTRACT_VERSION
from gamecollect_football.pack import pack as football_pack_factory
from gamecollect_football.taxonomy import TAXONOMY

PACK_NAME = "football-wc2026"
GOLDEN_DIR = Path(__file__).parent / "golden"
GOLDEN_PATH = GOLDEN_DIR / "vocabulary_football.json"


def _registry() -> Any:
    """The merged core + football registry (same pattern as tests/test_cli.py).

    ``vocabulary`` is a CORE operation (registered regardless of which packs
    are merged in) — the pack whose manifest it returns is selected by its
    required ``pack`` param, not by which packs were merged into the
    registry. The football pack is still merged here so this file matches
    the rest of the suite's registry-construction convention.
    """
    return cli.load_registry(names=[PACK_NAME])


def _empty_db(tmp_path: Path) -> Path:
    """A schema-initialized (but otherwise empty) db — vocabulary has no match
    data dependency, so no rows need seeding."""
    db = tmp_path / "vocabulary.db"
    football_pack = football_pack_factory()
    conn = connect(db, side_table_ddl=football_pack.side_table_ddl)
    conn.commit()
    conn.close()
    return db


def _call_vocabulary_op(tmp_path: Path, pack_name: str = PACK_NAME) -> Any:
    """Invoke the registered ``vocabulary`` op's impl exactly like the CLI does
    (``op.impl(conn, **kwargs)`` with the reader's connection), and return the
    JSON-safe payload via the op's own ``to_json``."""
    registry = _registry()
    op = registry.get("vocabulary")
    db = _empty_db(tmp_path)
    conn = open_reader(db)
    try:
        result = op.impl(conn, pack=pack_name)
    finally:
        conn.close()
    return op.to_json(result)


def _run_cli(capsys, argv: list[str]) -> str:
    rc = cli.main(argv, registry=_registry())
    assert rc == 0, f"CLI exited non-zero for {argv!r}"
    return capsys.readouterr().out


# --------------------------------------------------------------------------- #
# Registry discoverability: registered as a core op, with a required `pack`
# param naming which installed pack's manifest to return.
# --------------------------------------------------------------------------- #


def test_vocabulary_op_is_registered():
    registry = _registry()
    assert "vocabulary" in registry.names, "the registry must expose a 'vocabulary' read operation"


def test_vocabulary_op_is_a_core_operation():
    """Unlike squad/player-stats (football-only, match-scoped), vocabulary
    takes a pack name and must be present even with no packs merged in."""
    from gamecollect.registry import CORE_OPERATIONS

    assert "vocabulary" in {op.name for op in CORE_OPERATIONS}


def test_vocabulary_op_requires_a_pack_param():
    registry = _registry()
    op = registry.get("vocabulary")
    required = [param.name for param in op.params if param.required]
    assert required == ["pack"], (
        f"vocabulary should require exactly one 'pack' param, got {op.params!r}"
    )


def test_vocabulary_op_has_a_non_empty_output_schema():
    registry = _registry()
    op = registry.get("vocabulary")
    assert op.output_schema, "vocabulary must publish a non-empty output_schema"


# --------------------------------------------------------------------------- #
# Behavioral: the op's impl returns the football pack's real taxonomy/prompts/
# display metadata/compaction boundaries — sourced from the pack's own
# literals, not hard-coded twice, so this test can't drift from the pack.
# --------------------------------------------------------------------------- #


def test_vocabulary_op_returns_football_taxonomy(tmp_path):
    payload = _call_vocabulary_op(tmp_path)
    assert isinstance(payload, dict)
    assert payload["pack"] == PACK_NAME
    taxonomy = payload["taxonomy"]
    assert set(taxonomy) == set(TAXONOMY), (
        "vocabulary taxonomy keys must equal the football pack's declared event types"
    )
    for event_type, decl in TAXONOMY.items():
        entry = taxonomy[event_type]
        assert entry["display_name"] == decl.display_name
        assert entry["importance_default"] == decl.importance_default


def test_vocabulary_op_returns_football_prompt_fragments_and_display_metadata(tmp_path):
    football_pack = football_pack_factory()
    payload = _call_vocabulary_op(tmp_path)
    assert payload["prompt_fragments"] == football_pack.prompt_fragments
    assert payload["display_metadata"] == football_pack.display_metadata


def test_vocabulary_op_returns_football_compaction_boundaries(tmp_path):
    football_pack = football_pack_factory()
    payload = _call_vocabulary_op(tmp_path)
    assert list(payload["compaction_boundaries"]) == list(football_pack.compaction_boundaries)
    assert list(payload["compaction_boundaries"]) == ["kickoff", "half_time", "full_time"]


def test_vocabulary_op_unknown_pack_raises_pack_error(tmp_path):
    from gamecollect.packs.registry import PackError

    registry = _registry()
    op = registry.get("vocabulary")
    db = _empty_db(tmp_path)
    conn = open_reader(db)
    try:
        try:
            op.impl(conn, pack="not-a-real-pack")
        except PackError:
            pass
        else:
            raise AssertionError("expected PackError for an unknown pack name")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Golden file: pins the exact football-pack vocabulary output
# --------------------------------------------------------------------------- #


def test_vocabulary_op_matches_golden(tmp_path):
    payload = _call_vocabulary_op(tmp_path)
    assert GOLDEN_PATH.exists(), f"missing golden {GOLDEN_PATH}"
    golden = json.loads(GOLDEN_PATH.read_text())
    # Structural (key/value) comparison rather than a byte-exact dump compare:
    # the golden was hand-authored from the pack's own literals, so it is
    # robust to dict-ordering/whitespace choices the implementation makes.
    assert payload["pack"] == golden["pack"]
    assert payload["taxonomy"] == golden["taxonomy"]
    assert payload["prompt_fragments"] == golden["prompt_fragments"]
    assert payload["display_metadata"] == golden["display_metadata"]
    assert list(payload["compaction_boundaries"]) == golden["compaction_boundaries"]


def test_golden_file_taxonomy_matches_live_pack_literals():
    """Guard against the golden itself drifting from the pack's real taxonomy
    (independent of the op's implementation) — pins the golden to
    source-of-truth literals."""
    golden = json.loads(GOLDEN_PATH.read_text())
    assert golden["pack"] == PACK_NAME
    assert set(golden["taxonomy"]) == set(TAXONOMY)
    for event_type, decl in TAXONOMY.items():
        entry = golden["taxonomy"][event_type]
        assert entry["display_name"] == decl.display_name
        assert entry["importance_default"] == decl.importance_default
    football_pack = football_pack_factory()
    assert golden["prompt_fragments"] == football_pack.prompt_fragments
    assert golden["display_metadata"] == football_pack.display_metadata
    assert golden["compaction_boundaries"] == list(football_pack.compaction_boundaries)


# --------------------------------------------------------------------------- #
# Each golden --json sample conforms to the op's declared output_schema
# --------------------------------------------------------------------------- #


def test_vocabulary_golden_conforms_to_output_schema():
    from test_cli import _schema_errors

    registry = _registry()
    op = registry.get("vocabulary")
    golden = json.loads(GOLDEN_PATH.read_text())
    errors = _schema_errors(golden, op.output_schema)
    assert not errors, f"vocabulary golden violates its declared output_schema: {errors}"


# --------------------------------------------------------------------------- #
# contract_version: present on the manifest that wraps the vocabulary op
# --------------------------------------------------------------------------- #


def test_manifest_carries_contract_version_alongside_vocabulary(capsys):
    out = _run_cli(capsys, ["tools", "--json"])
    manifest = json.loads(out)
    assert manifest["contract_version"] == CONTRACT_VERSION
    op_names = {op["name"] for op in manifest["operations"]}
    assert "vocabulary" in op_names


# --------------------------------------------------------------------------- #
# CLI + tools --json manifest exposure (matching the existing core-op wiring)
# --------------------------------------------------------------------------- #


def test_vocabulary_is_a_cli_subcommand():
    registry = _registry()
    parser = cli.build_parser(registry)
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            assert "vocabulary" in action.choices
            return
    raise AssertionError("parser exposes no subparsers")


def test_vocabulary_json_via_cli(tmp_path, capsys):
    db = _empty_db(tmp_path)
    out = _run_cli(capsys, ["vocabulary", PACK_NAME, "--db", str(db), "--json"])
    payload = json.loads(out)
    assert payload["pack"] == PACK_NAME
    assert set(payload["taxonomy"]) == set(TAXONOMY)
    assert "prompt_fragments" in payload
    assert "display_metadata" in payload
    assert list(payload["compaction_boundaries"]) == ["kickoff", "half_time", "full_time"]


def test_vocabulary_discoverable_via_cli_subcommand_names():
    registry = _registry()
    assert "vocabulary" in cli.cli_subcommand_names(registry)
