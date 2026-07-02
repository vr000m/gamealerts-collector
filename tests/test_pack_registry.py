"""Phase 3: pack registry — entry-point discovery, six-field validation, taxonomy wiring.

Plan contract (docs/dev_plans/20260702-feature-collector-foundation.md §Phase 3):
- ``gamecollect.packs.registry.load_pack(name)`` discovers packs via
  ``importlib.metadata.entry_points(group="gamecollect.packs")``; an in-repo
  pack named ``test-fixture`` is registered via a REAL entry point (no mocks).
- The registry validates all six DESIGN.md §5 contributions — provider factory
  returning a ``MatchDataProvider``, non-empty taxonomy, ``prompt_fragments``,
  ``preference_schema``, ``display_metadata``, ``compaction_boundaries`` — and
  rejects loudly (raises) when any is missing. Rejection is exercised at the
  validation-function level with in-test invalid ``SportPack`` objects
  (arranging a bad entry point through importlib is out of scope per the plan).
- Unknown pack name -> clear error from ``load_pack``.
- Taxonomy wiring: the pack's taxonomy key-set feeds
  ``PartitionWriter(conn, source, taxonomy=...)`` (Phase 2 signature); a
  declared event type writes fine, an undeclared one is rejected loudly.

The plan pins ``load_pack`` but not the validation function's name, so it is
resolved among candidates via ``_phase2_helpers.resolve_attr``.
"""

import dataclasses
import importlib.metadata

import pytest
from _phase2_helpers import event_row, match_row, resolve_attr, writer_method

from gamecollect.db.connection import connect
from gamecollect.db.writer import PartitionWriter
from gamecollect.packs import registry
from gamecollect.packs.registry import load_pack
from gamecollect.packs.spec import SportPack
from gamecollect.provider import MatchDataProvider

FIXTURE_PACK = "test-fixture"

VALIDATE_CANDIDATES = (
    "validate_pack",
    "validate_sport_pack",
    "validate",
    "check_pack",
    "_validate_pack",
)


def validate_fn():
    return resolve_attr(registry, VALIDATE_CANDIDATES, "pack validation function")


@pytest.fixture
def pack() -> SportPack:
    return load_pack(FIXTURE_PACK)


# --- discovery through importlib.metadata ------------------------------------------


def test_fixture_pack_registered_via_real_entry_point():
    eps = importlib.metadata.entry_points(group="gamecollect.packs")
    names = {ep.name for ep in eps}
    assert FIXTURE_PACK in names, (
        f"'{FIXTURE_PACK}' not registered under gamecollect.packs; found {sorted(names)}"
    )


def test_load_pack_returns_sport_pack(pack):
    assert isinstance(pack, SportPack)
    assert pack.name == FIXTURE_PACK


def test_load_pack_unknown_name_raises_clear_error():
    with pytest.raises(Exception) as excinfo:
        load_pack("no-such-pack")
    assert "no-such-pack" in str(excinfo.value), (
        "error for an unknown pack should name the pack it failed to find"
    )


# --- the six DESIGN.md §5 contributions on the loaded pack --------------------------


def test_pack_exposes_all_six_contributions(pack):
    assert isinstance(pack.sport, str) and pack.sport
    assert callable(pack.provider_factory)
    assert isinstance(pack.taxonomy, dict) and pack.taxonomy
    assert all(isinstance(k, str) for k in pack.taxonomy)
    assert isinstance(pack.prompt_fragments, dict)
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in pack.prompt_fragments.items())
    assert isinstance(pack.preference_schema, dict)
    assert isinstance(pack.display_metadata, dict)
    assert isinstance(pack.compaction_boundaries, list)
    assert isinstance(pack.side_table_ddl, tuple)


def test_provider_factory_returns_match_data_provider(pack):
    provider = pack.provider_factory()
    assert isinstance(provider, MatchDataProvider)


def test_taxonomy_values_carry_display_name_and_importance_default(pack):
    for key, decl in pack.taxonomy.items():
        attrs = [a for a in dir(decl) if not a.startswith("_")]
        assert any("display" in a or a in ("name", "label") for a in attrs), (
            f"taxonomy[{key!r}] declares no display name; attrs: {attrs}"
        )
        assert any("importance" in a for a in attrs), (
            f"taxonomy[{key!r}] declares no importance default; attrs: {attrs}"
        )


# --- validation rejects a pack missing any of the six --------------------------------


def test_valid_pack_passes_validation(pack):
    validate_fn()(pack)  # must not raise


def _non_provider_factory():
    return object()


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("provider_factory", _non_provider_factory),  # returns a non-MatchDataProvider
        ("provider_factory", None),  # missing entirely
        ("taxonomy", {}),  # empty taxonomy
        ("taxonomy", None),
        ("prompt_fragments", None),
        ("preference_schema", None),
        ("display_metadata", None),
        ("compaction_boundaries", None),
    ],
)
def test_validation_rejects_pack_missing_a_contribution(pack, field, bad_value):
    broken = dataclasses.replace(pack, **{field: bad_value})
    # "Rejects loudly": any raise with a non-empty message counts; the plan
    # does not pin the exception type.
    with pytest.raises(Exception, match=r"(?s)."):
        validate_fn()(broken)


# --- taxonomy wiring into PartitionWriter -------------------------------------------


def test_pack_taxonomy_gates_event_types_at_write_time(tmp_path, pack):
    conn = connect(tmp_path / "taxonomy.db", side_table_ddl=pack.side_table_ddl)
    try:
        writer = PartitionWriter(conn, "test-fixture-src", taxonomy=set(pack.taxonomy))
        writer_method(writer, "match")(match_row("m1"))

        declared = next(iter(pack.taxonomy))
        writer_method(writer, "event")(event_row("m1", 1, type=declared))
        conn.commit()

        # Loud rejection of an undeclared type; exception type unpinned.
        with pytest.raises(Exception, match=r"(?s)."):
            writer_method(writer, "event")(event_row("m1", 2, type="not-a-declared-type"))
    finally:
        conn.close()

    import sqlite3

    with sqlite3.connect(tmp_path / "taxonomy.db") as c:
        types = [r[0] for r in c.execute("SELECT type FROM events ORDER BY seq")]
    assert types == [declared], "only the declared event type may reach the events table"


def test_writer_without_taxonomy_stays_sport_agnostic(tmp_path):
    # Phase 2 contract: validation is skipped when taxonomy=None.
    conn = connect(tmp_path / "agnostic.db")
    try:
        writer = PartitionWriter(conn, "src-a")
        writer_method(writer, "match")(match_row("m1"))
        writer_method(writer, "event")(event_row("m1", 1, type="anything-goes"))
        conn.commit()
    finally:
        conn.close()
