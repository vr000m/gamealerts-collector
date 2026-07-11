"""Phase 4: football taxonomy parity, loud write-time rejection, core-table
immutability under pack side tables.

Taxonomy parity oracle: the pack's declared event-type key set must equal the
gamealerts ``data/provider.py`` ``EventType`` enum VALUE set exactly — no
silently dropped or renamed types (plan §Review Focus). The expected set is
hardcoded in ``_football_helpers.GAMEALERTS_EVENT_TYPE_VALUES``; gamealerts is never
imported at test time.
"""

from __future__ import annotations

import sqlite3

import pytest
from _football_helpers import (
    GAMEALERTS_EVENT_TYPE_VALUES,
    GAMEALERTS_IMPORTANCE_DEFAULTS,
    PACK_NAME,
)

from gamecollect.db.connection import connect
from gamecollect.db.writer import PartitionWriter, TaxonomyError

CORE_TABLES = (
    "matches",
    "events",
    "entities",
    "standings",
    "provider_match_map",
    "schema_meta",
)


# ===========================================================================
# Pack loads through the SDK registry (real entry point, no mocks).
# ===========================================================================


class TestPackLoads:
    def test_pack_loads_via_registry_entry_point(self, football_pack):
        from gamecollect.packs.spec import SportPack

        assert isinstance(football_pack, SportPack)
        assert football_pack.name == PACK_NAME
        assert football_pack.sport == "football"

    def test_pack_is_discoverable_by_name(self):
        from gamecollect.packs.registry import pack_names

        assert PACK_NAME in pack_names()

    def test_pack_supplies_side_table_ddl(self, football_pack):
        """Phase 4 exercises the "packs may own typed side tables" contract:
        the football pack must actually supply DDL (stats, lineups)."""
        assert football_pack.side_table_ddl, "football pack declares no side-table DDL"


# ===========================================================================
# Taxonomy parity with gamealerts EventType.
# ===========================================================================


class TestTaxonomyParity:
    def test_declared_key_set_equals_gamealerts_event_type_values(self, football_pack):
        declared = set(football_pack.taxonomy)
        missing = GAMEALERTS_EVENT_TYPE_VALUES - declared
        extra = declared - GAMEALERTS_EVENT_TYPE_VALUES
        assert declared == GAMEALERTS_EVENT_TYPE_VALUES, (
            f"taxonomy drifted from gamealerts EventType values: "
            f"missing={sorted(missing)} extra={sorted(extra)}"
        )

    def test_event_types_property_matches_taxonomy_keys(self, football_pack):
        assert football_pack.event_types == frozenset(football_pack.taxonomy)

    @pytest.mark.parametrize(
        "event_type,expected_importance", sorted(GAMEALERTS_IMPORTANCE_DEFAULTS.items())
    )
    def test_importance_defaults_match_gamealerts(
        self, football_pack, event_type, expected_importance
    ):
        """EVENT_IMPORTANCE parity: goal/own_goal/penalty critical (1),
        red/full_time high (2), yellow/half_time/kickoff medium (3), sub low (4)."""
        decl = football_pack.taxonomy[event_type]
        assert decl.importance_default == expected_importance

    def test_every_declared_type_has_a_display_name(self, football_pack):
        for key, decl in football_pack.taxonomy.items():
            assert isinstance(decl.display_name, str) and decl.display_name.strip(), (
                f"taxonomy[{key!r}] has an empty display name"
            )

    def test_goal_family_event_types_are_declared_by_the_pack(self, football_pack):
        """``gamecollect.engine._GOAL_FAMILY_EVENT_TYPES`` gates the
        ``player``->``scorer`` payload-key rewrite by hardcoded slug, with no
        shared constant tying it to the football pack's own taxonomy (a
        deliberate, documented boundary erosion — see that constant's
        docstring). If a future taxonomy slug rename or removal drifts out of
        sync with the engine's literal, this must fail loudly rather than
        silently mis-key goal-family payloads."""
        from gamecollect.engine import _GOAL_FAMILY_EVENT_TYPES

        # NOTE: this check is football-pack-specific — it only compares
        # against `football_pack.taxonomy`. Football is currently the only
        # sport pack in the repo, so there is nothing to genuinely
        # generalize against yet. If/when a second sport pack lands, it
        # will need its own analogous parity test; this one won't catch
        # drift for that pack.
        assert _GOAL_FAMILY_EVENT_TYPES <= set(football_pack.taxonomy), (
            f"engine._GOAL_FAMILY_EVENT_TYPES {sorted(_GOAL_FAMILY_EVENT_TYPES)} "
            f"drifted from the football pack's declared taxonomy keys "
            f"{sorted(football_pack.taxonomy)}"
        )


# ===========================================================================
# Loud rejection with the REAL pack: undeclared type raises at write time.
# ===========================================================================


class TestLoudRejectionWithRealPack:
    def test_undeclared_event_type_raises_at_write_time(self, tmp_path, football_pack):
        conn = connect(tmp_path / "reject.db", side_table_ddl=football_pack.side_table_ddl)
        try:
            writer = PartitionWriter(conn, "wc2026-test", taxonomy=football_pack.event_types)
            writer.upsert_match({"match_id": "m1", "status": "IN_PLAY"})
            # A declared football type writes fine…
            assert writer.append_events("m1", [{"match_id": "m1", "seq": 0, "type": "goal"}]) == 1
            # …an undeclared one is rejected loudly, before touching the DB.
            with pytest.raises(TaxonomyError):
                writer.append_events("m1", [{"match_id": "m1", "seq": 1, "type": "touchdown"}])
            types = [r[0] for r in conn.execute("SELECT type FROM events ORDER BY seq")]
            assert types == ["goal"], "the rejected event must not reach the events table"
        finally:
            conn.close()

    def test_rejection_names_the_offending_type(self, tmp_path, football_pack):
        conn = connect(tmp_path / "reject2.db", side_table_ddl=football_pack.side_table_ddl)
        try:
            writer = PartitionWriter(conn, "wc2026-test", taxonomy=football_pack.event_types)
            writer.upsert_match({"match_id": "m1", "status": "IN_PLAY"})
            with pytest.raises(TaxonomyError, match="touchdown"):
                writer.append_events("m1", [{"match_id": "m1", "seq": 0, "type": "touchdown"}])
        finally:
            conn.close()


# ===========================================================================
# Core-table immutability: pack side tables never alter core schema.
# ===========================================================================


def _core_schema_sql(db_path) -> dict[str, str]:
    with sqlite3.connect(db_path) as c:
        rows = c.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND sql IS NOT NULL"
        ).fetchall()
    return {name: sql for name, sql in rows if name in CORE_TABLES}


def _all_table_names(db_path) -> set[str]:
    with sqlite3.connect(db_path) as c:
        return {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }


class TestCoreTableImmutability:
    def test_core_schema_identical_with_and_without_the_pack(self, tmp_path, football_pack):
        bare = tmp_path / "bare.db"
        packed = tmp_path / "packed.db"
        connect(bare).close()
        connect(packed, side_table_ddl=football_pack.side_table_ddl).close()

        bare_core = _core_schema_sql(bare)
        packed_core = _core_schema_sql(packed)
        assert set(bare_core) == set(CORE_TABLES), (
            f"core tables missing from bare DB: {set(CORE_TABLES) - set(bare_core)}"
        )
        assert packed_core == bare_core, "pack side-table registration altered core-table SQL"

    def test_side_tables_exist_only_in_the_pack_enabled_db(self, tmp_path, football_pack):
        bare = tmp_path / "bare.db"
        packed = tmp_path / "packed.db"
        connect(bare).close()
        connect(packed, side_table_ddl=football_pack.side_table_ddl).close()

        side_tables = _all_table_names(packed) - _all_table_names(bare)
        assert side_tables, "pack DDL created no side tables in the pack-enabled DB"
        assert _all_table_names(bare) == set(CORE_TABLES), (
            f"unexpected non-core tables in the bare DB: "
            f"{_all_table_names(bare) - set(CORE_TABLES)}"
        )
        assert not (side_tables & set(CORE_TABLES))

    def test_side_table_ddl_is_idempotent(self, tmp_path, football_pack):
        """connect() re-applies pack DDL on every open — it must be additive
        and idempotent (CREATE TABLE IF NOT EXISTS posture)."""
        db = tmp_path / "reopen.db"
        connect(db, side_table_ddl=football_pack.side_table_ddl).close()
        before = _all_table_names(db)
        connect(db, side_table_ddl=football_pack.side_table_ddl).close()
        assert _all_table_names(db) == before
