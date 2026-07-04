"""Phase 4: ported team/player reconciliation (gamealerts ``data/reconcile.py``).

Ports the alias/fold assertions from gamealerts ``tests/test_phase3_reconcile.py``
(TestCanonicalTeamName et al.) and exercises ``resolve_canonical_match_id`` /
``register_unreconciled_match`` against a temp DB through ``gamecollect.db``
(the plan rewrites the DAO calls onto ``writer.py``/``reader.py``).

Key ported invariant, tightened per the plan: a newly created unreconciled
canonical id is SOURCE-qualified (not merely provider-qualified) — two sources
collecting the same provider-native id must yield two distinct ``match_id``s
so the match_id-global reader contract holds.

Ported signatures (read from the landed port):
``resolve_canonical_match_id(conn, writer, match, provider)`` and
``register_unreconciled_match(conn, writer, match)`` — the writer carries the
source, so no provider argument is needed to qualify the new id; ``conn`` is
read for the payload read-merge-write. Missing identity fields return ``None``
(nothing seeded, so the id would be unusable for child writes).
"""

from __future__ import annotations

import pytest

from gamecollect.db.connection import connect
from gamecollect.db.reader import get_state
from gamecollect.db.writer import PartitionWriter
from gamecollect.fold import fold
from gamecollect.provider import MatchStatus, NormalizedMatch
from gamecollect_football.reconcile import (
    TEAM_ALIASES,
    canonical_display_name,
    canonical_player_name,
    canonical_team_name,
    register_unreconciled_match,
    resolve_canonical_match_id,
    seed_or_reconcile_match,
)

PROVIDER = "espn"
SOURCE_A = "wc2026"
SOURCE_B = "eur2028"


def _match(
    match_id: str = "760421",
    home: str | None = "Australia",
    away: str | None = "Türkiye",
    kickoff: str | None = "2026-06-14T04:00:00+00:00",
) -> NormalizedMatch:
    return NormalizedMatch(
        match_id=match_id,
        status=MatchStatus.IN_PLAY,
        minute=27,
        score_home=1,
        score_away=0,
        display_clock="27'",
        home_team=home,
        away_team=away,
        kickoff_utc=kickoff,
    )


@pytest.fixture
def db(tmp_path):
    conn = connect(tmp_path / "reconcile.db")
    writer = PartitionWriter(conn, SOURCE_A)
    yield conn, writer
    conn.close()


# ===========================================================================
# 1. TEAM_ALIASES / canonical_team_name (ported from gamealerts).
# ===========================================================================


class TestCanonicalTeamName:
    def test_turkiye_aliases_to_turkey(self):
        assert canonical_team_name("Türkiye") == canonical_team_name("Turkey")

    def test_cote_divoire_aliases_to_ivory_coast(self):
        assert canonical_team_name("Côte d'Ivoire") == canonical_team_name("Ivory Coast")

    def test_alias_table_has_required_entries(self):
        assert TEAM_ALIASES["Türkiye"] == "Turkey"
        assert TEAM_ALIASES["Côte d'Ivoire"] == "Ivory Coast"
        assert TEAM_ALIASES["Korea Republic"] == "South Korea"
        assert TEAM_ALIASES["IR Iran"] == "Iran"
        assert TEAM_ALIASES["United States"] == "USA"

    def test_distinct_teams_do_not_collide(self):
        assert canonical_team_name("Turkey") != canonical_team_name("Australia")
        assert canonical_team_name("Mexico") != canonical_team_name("South Africa")

    def test_key_is_case_and_whitespace_insensitive(self):
        assert canonical_team_name("  Turkey ") == canonical_team_name("turkey")


# ===========================================================================
# 2. canonical_player_name — accent-insensitive fold, no alias table.
# ===========================================================================


class TestCanonicalPlayerName:
    def test_accented_name_folds_to_deaccented_key(self):
        assert canonical_player_name("Maxime Crépeau") == canonical_player_name("Maxime Crepeau")

    def test_case_and_trim_insensitive(self):
        assert canonical_player_name("  DEREK Cornelius ") == canonical_player_name(
            "derek cornelius"
        )

    def test_distinct_players_do_not_collide(self):
        assert canonical_player_name("Cyle Larin") != canonical_player_name("Jonathan David")

    def test_player_fold_agrees_with_core_fold(self):
        """Plan: pack reconciliation calls into the core fold, so write-fold ==
        query-fold by construction — the pack's player key must equal what the
        core ``gamecollect.fold`` produces for the same name."""
        for name in ("Maxime Crépeau", "Türkiye", "  Derek Cornelius "):
            assert canonical_player_name(name) == fold(name)


# ===========================================================================
# 3. canonical_display_name — alias only, human-readable case preserved.
# ===========================================================================


class TestCanonicalDisplayName:
    def test_alias_yields_canonical_display_name(self):
        assert canonical_display_name("Türkiye") == "Turkey"
        assert canonical_display_name("Côte d'Ivoire") == "Ivory Coast"

    def test_non_aliased_name_passes_through_unchanged(self):
        assert canonical_display_name("Brazil") == "Brazil"

    def test_display_name_is_not_casefolded(self):
        """Unlike canonical_team_name, the display fold keeps proper case —
        it yields ``Turkey``, never the comparison key ``turkey``."""
        assert canonical_display_name("Türkiye") == "Turkey"
        assert canonical_display_name("Turkey") != "turkey"


# ===========================================================================
# 4. register_unreconciled_match — source-qualified ids on a temp DB.
# ===========================================================================


class TestRegisterUnreconciledMatch:
    def test_returns_a_source_qualified_id(self, db):
        conn, writer = db
        match = _match()
        new_id = register_unreconciled_match(conn, writer, match)
        assert isinstance(new_id, str) and new_id
        assert match.match_id in new_id, "qualified id should embed the provider-native id"
        assert new_id != match.match_id, (
            "unreconciled id must be qualified, not the bare provider-native id"
        )

    def test_registered_row_exists_and_is_source_stamped(self, db):
        conn, writer = db
        new_id = register_unreconciled_match(conn, writer, _match())
        row = get_state(conn, new_id)
        assert row is not None, f"no matches row written under {new_id!r}"
        assert row["source"] == SOURCE_A
        assert row["kickoff_utc"] == "2026-06-14T04:00:00+00:00"

    def test_registration_is_idempotent(self, db):
        conn, writer = db
        match = _match()
        first = register_unreconciled_match(conn, writer, match)
        second = register_unreconciled_match(conn, writer, match)
        assert first == second
        (count,) = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
        assert count == 1

    def test_missing_identity_fields_write_no_row_and_return_none(self, db):
        """Without home/away/kickoff the row cannot satisfy its identity
        contract — write nothing and return None (review fix: a returned str
        must always name a seeded row usable for child-table writes)."""
        conn, writer = db
        match = _match(home=None, away=None, kickoff=None)
        returned = register_unreconciled_match(conn, writer, match)
        assert returned is None
        (count,) = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
        assert count == 0

    def test_reregistration_merges_payload_instead_of_erasing(self, db):
        """Review fix: upsert_match replaces payload wholesale, so re-running
        register_unreconciled_match must read-merge-write — keys added to the
        strip row by other write paths survive the next poll's registration."""
        import json

        conn, writer = db
        match = _match()
        qualified = register_unreconciled_match(conn, writer, match)
        row = get_state(conn, qualified)
        enriched = json.loads(row["payload"])
        enriched["stats"] = [{"team": "Australia", "possession": 61.0}]
        writer.upsert_match({"match_id": qualified, "payload": enriched})

        register_unreconciled_match(conn, writer, match)
        payload = json.loads(get_state(conn, qualified)["payload"])
        assert payload["stats"] == [{"team": "Australia", "possession": 61.0}]
        assert payload["home_team"] == "Australia"
        assert payload["away_team"] == "Turkey"

    def test_match_payload_extras_survive_and_canonical_names_win(self, db):
        """Review fix: the snapshot's own payload (adapter sport extras such as
        round_name/venue/HT scores) must be merged into the stored payload —
        while the canonical DISPLAY team names still win over any raw
        provider-supplied home_team/away_team payload keys."""
        import json
        from dataclasses import replace

        conn, writer = db
        match = replace(
            _match(),
            payload={
                "round_name": "Group B",
                "venue": "Estadio Azteca",
                "away_team": "Türkiye",  # raw provider name; canonical must win
            },
        )
        qualified = register_unreconciled_match(conn, writer, match)
        payload = json.loads(get_state(conn, qualified)["payload"])
        assert payload["round_name"] == "Group B"
        assert payload["venue"] == "Estadio Azteca"
        assert payload["home_team"] == "Australia"
        assert payload["away_team"] == "Turkey", (
            "canonical display name must win over the raw provider payload key"
        )

    def test_sparser_repoll_does_not_clobber_richer_stored_payload(self, db):
        """Review fix (preserve-richer merge): a post-FT scoreboard snapshot
        whose payload carries None/empty values for keys the stored strip row
        already holds rich values for (detail-derived stats/venue) must NOT
        overwrite them; missing-from-stored keys still land."""
        import json
        from dataclasses import replace

        conn, writer = db
        rich = replace(
            _match(),
            payload={"venue": "Estadio Azteca", "stats": [{"team": "Australia", "shots": 9}]},
        )
        qualified = register_unreconciled_match(conn, writer, rich)

        sparse = replace(
            _match(),
            payload={"venue": None, "stats": [], "attendance": 83000, "referee": ""},
        )
        register_unreconciled_match(conn, writer, sparse)

        payload = json.loads(get_state(conn, qualified)["payload"])
        assert payload["venue"] == "Estadio Azteca", "None must not clobber a stored value"
        assert payload["stats"] == [{"team": "Australia", "shots": 9}], (
            "an empty list must not clobber stored rich data"
        )
        assert payload["attendance"] == 83000, "new non-empty keys still land"
        assert payload["referee"] == "", "an empty value may fill a MISSING key"

    def test_fresher_non_empty_value_still_replaces_stored_non_empty(self, db):
        import json
        from dataclasses import replace

        conn, writer = db
        first = replace(_match(), payload={"venue": "Estadio Azteca"})
        qualified = register_unreconciled_match(conn, writer, first)

        second = replace(_match(), payload={"venue": "Azteca (renamed)"})
        register_unreconciled_match(conn, writer, second)

        payload = json.loads(get_state(conn, qualified)["payload"])
        assert payload["venue"] == "Azteca (renamed)", "non-empty over non-empty: fresher wins"

    def test_two_sources_same_provider_id_do_not_collide(self, tmp_path):
        """THE source-qualification invariant (plan §Phase 2/4): two sources
        collecting the same provider-native id must land on two DISTINCT
        canonical match_ids, each stamped with its own source."""
        conn = connect(tmp_path / "collide.db")
        try:
            writer_a = PartitionWriter(conn, SOURCE_A)
            writer_b = PartitionWriter(conn, SOURCE_B)
            id_a = register_unreconciled_match(conn, writer_a, _match())
            id_b = register_unreconciled_match(conn, writer_b, _match())
            assert id_a != id_b, (
                f"two sources produced the SAME unreconciled id {id_a!r}; "
                "ids must be source-qualified"
            )
            assert get_state(conn, id_a)["source"] == SOURCE_A
            assert get_state(conn, id_b)["source"] == SOURCE_B
        finally:
            conn.close()


# ===========================================================================
# 5. resolve_canonical_match_id — precedence against a temp DB.
# ===========================================================================


class TestResolveCanonicalMatchId:
    def test_unknown_match_resolves_to_none(self, db):
        conn, writer = db
        assert resolve_canonical_match_id(conn, writer, _match(), PROVIDER) is None

    def test_cached_provider_map_wins(self, db):
        conn, writer = db
        match = _match()
        writer.upsert_match({"match_id": "wc2026_md03_m04", "status": "SCHEDULED"})
        writer.map_provider_match(PROVIDER, match.match_id, "wc2026_md03_m04")
        assert resolve_canonical_match_id(conn, writer, match, PROVIDER) == "wc2026_md03_m04"

    def test_direct_seed_backcompat_returns_provider_id(self, db):
        """A row seeded directly under the provider-native id IS the canonical
        row (gamealerts precedence step (b))."""
        conn, writer = db
        match = _match()
        writer.upsert_match({"match_id": match.match_id, "status": "SCHEDULED"})
        assert resolve_canonical_match_id(conn, writer, match, PROVIDER) == match.match_id

    def test_direct_seed_hit_is_cached_in_the_provider_map(self, db):
        """Review fix: (b) now persists the bind (docstring contract: (b)/(c)
        cache, (d) does not), so the next poll resolves via the (a) cache."""
        conn, writer = db
        match = _match()
        writer.upsert_match({"match_id": match.match_id, "status": "SCHEDULED"})
        resolve_canonical_match_id(conn, writer, match, PROVIDER)
        (count,) = conn.execute(
            "SELECT COUNT(*) FROM provider_match_map "
            "WHERE provider = ? AND provider_match_id = ? AND match_id = ?",
            (PROVIDER, match.match_id, match.match_id),
        ).fetchone()
        assert count == 1

    def test_register_then_resolve_round_trips(self, db):
        """After register_unreconciled_match, re-resolving the same provider
        match must land on the registered source-qualified id — the collector's
        poll loop depends on this round trip."""
        conn, writer = db
        match = _match()
        registered = register_unreconciled_match(conn, writer, match)
        assert resolve_canonical_match_id(conn, writer, match, PROVIDER) == registered

    def test_resolution_is_scoped_away_from_other_provider(self, db):
        """A cached mapping under one provider must not answer for another."""
        conn, writer = db
        match = _match()
        writer.upsert_match({"match_id": "wc2026_md03_m04", "status": "SCHEDULED"})
        writer.map_provider_match("footballdata", match.match_id, "wc2026_md03_m04")
        assert resolve_canonical_match_id(conn, writer, match, PROVIDER) != "wc2026_md03_m04"


class TestResolveCrossSourceIsolation:
    def test_direct_seed_owned_by_other_source_is_not_bound(self, db):
        """Review fix: step (b) must be source-scoped. A bare provider-native
        id seeded by a DIFFERENT source is not this source's canonical row —
        binding to it would append events under another partition's match."""
        conn, writer = db
        match = _match(home=None, away=None, kickoff=None)  # force (b)/(d) path only

        other = PartitionWriter(conn, SOURCE_B)
        other.upsert_match({"match_id": match.match_id, "status": "SCHEDULED"})

        assert resolve_canonical_match_id(conn, writer, match, PROVIDER) is None


# ===========================================================================
# 6. seed_or_reconcile_match — the pack's reconcile-first seed hook.
# ===========================================================================


class TestSeedOrReconcileMatch:
    """Review fix: pack.seed_match must reconcile BEFORE seeding a strip row,
    so a live match whose canonical schedule row exists gets its events under
    the canonical id instead of a duplicate source-qualified strip."""

    CANONICAL = "wc2026_md03_m04"

    def _seed_schedule_row(self, writer):
        writer.upsert_match(
            {
                "match_id": self.CANONICAL,
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "payload": {
                    "home_team": "Australia",
                    "away_team": "Turkey",
                    "round_name": "Group B",
                },
            }
        )

    def test_resolves_to_canonical_row_when_schedule_row_exists(self, db):
        import json

        conn, writer = db
        self._seed_schedule_row(writer)

        seeded = seed_or_reconcile_match(conn, writer, _match(), PROVIDER)
        assert seeded == self.CANONICAL

        row = get_state(conn, self.CANONICAL)
        assert row["status"] == MatchStatus.IN_PLAY.value
        assert row["minute"] == 27
        assert row["score_home"] == 1
        payload = json.loads(row["payload"])
        assert payload["home_team"] == "Australia"
        assert payload["away_team"] == "Turkey", "schedule-seeded names must be preserved"
        assert payload["round_name"] == "Group B"
        # No duplicate source-qualified strip row was created.
        assert get_state(conn, f"{SOURCE_A}:760421") is None

    def test_resolved_upsert_merges_snapshot_payload_extras(self, db):
        import json
        from dataclasses import replace

        conn, writer = db
        self._seed_schedule_row(writer)
        match = replace(_match(), payload={"venue": "Estadio Azteca", "away_team": "Türkiye"})

        seeded = seed_or_reconcile_match(conn, writer, match, PROVIDER)
        payload = json.loads(get_state(conn, seeded)["payload"])
        assert payload["venue"] == "Estadio Azteca"
        assert payload["away_team"] == "Turkey", (
            "canonical schedule name must win over the raw provider payload key"
        )

    def test_falls_back_to_unreconciled_strip_row(self, db):
        conn, writer = db
        seeded = seed_or_reconcile_match(conn, writer, _match(), PROVIDER)
        assert seeded == f"{SOURCE_A}:760421"
        assert get_state(conn, seeded) is not None

    def test_missing_identity_and_no_resolution_returns_none(self, db):
        conn, writer = db
        match = _match(home=None, away=None, kickoff=None)
        assert seed_or_reconcile_match(conn, writer, match, PROVIDER) is None
        (count,) = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
        assert count == 0

    def test_missing_identity_but_cached_resolution_still_seeds(self, db):
        """A cache hit resolves without identity fields — the canonical row
        already carries them, so live state must still land (str return)."""
        conn, writer = db
        self._seed_schedule_row(writer)
        writer.map_provider_match(PROVIDER, "760421", self.CANONICAL)

        match = _match(home=None, away=None, kickoff=None)
        seeded = seed_or_reconcile_match(conn, writer, match, PROVIDER)
        assert seeded == self.CANONICAL
        assert get_state(conn, self.CANONICAL)["status"] == MatchStatus.IN_PLAY.value

    def test_repolls_stay_on_the_canonical_row(self, db):
        """Poll twice: the second poll must resolve via the cached bind and
        keep writing the same canonical row (no strip-row drift)."""
        conn, writer = db
        self._seed_schedule_row(writer)

        first = seed_or_reconcile_match(conn, writer, _match(), PROVIDER)
        second = seed_or_reconcile_match(conn, writer, _match(), PROVIDER)
        assert first == second == self.CANONICAL
        (count,) = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
        assert count == 1


# ===========================================================================
# 7. Late canonical reconciliation — stub adoption (review finding D).
# ===========================================================================


class TestStubAdoption:
    """If early polls collected on a source-qualified stub (schedule row absent)
    and the canonical schedule row appears LATER, the stub's events/mappings/
    side-table rows must migrate onto the canonical id in one transaction and
    the stub row must be deleted — otherwise readers of the canonical match
    silently miss the early history. If BOTH rows carry events, nothing is
    migrated (never interleave two timelines): ERROR logged, collection stays
    on the stub."""

    CANONICAL = "wc2026_md03_m04"
    STUB = f"{SOURCE_A}:760421"

    def _seed_schedule_row(self, writer):
        writer.upsert_match(
            {
                "match_id": self.CANONICAL,
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "payload": {"home_team": "Australia", "away_team": "Turkey"},
            }
        )

    def test_stub_events_and_mappings_migrate_when_canonical_appears(self, db):
        conn, writer = db
        # Poll 1: no schedule row yet — a stub is registered and events accrue.
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        writer.append_events(
            self.STUB,
            [
                {"seq": 0, "type": "kickoff", "minute": 0},
                {"seq": 1, "type": "goal", "minute": 12},
            ],
        )
        # The canonical schedule row lands late; the next poll must adopt.
        self._seed_schedule_row(writer)
        seeded = seed_or_reconcile_match(conn, writer, _match(), PROVIDER)
        assert seeded == self.CANONICAL

        seqs = [
            r[0]
            for r in conn.execute(
                "SELECT seq FROM events WHERE source = ? AND match_id = ? ORDER BY seq",
                (SOURCE_A, self.CANONICAL),
            )
        ]
        assert seqs == [0, 1], "stub events must migrate to the canonical id, seqs preserved"
        assert get_state(conn, self.STUB) is None, "the orphaned stub row must be deleted"
        (stranded,) = conn.execute(
            "SELECT COUNT(*) FROM events WHERE match_id = ?", (self.STUB,)
        ).fetchone()
        assert stranded == 0
        mapped = conn.execute(
            "SELECT match_id FROM provider_match_map WHERE source = ? AND provider = ? "
            "AND provider_match_id = ?",
            (SOURCE_A, PROVIDER, "760421"),
        ).fetchone()
        assert mapped is not None and mapped[0] == self.CANONICAL

    def test_both_have_events_no_migration_error_logged_stub_keeps_collecting(self, db, caplog):
        import logging

        conn, writer = db
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        writer.append_events(self.STUB, [{"seq": 0, "type": "goal", "minute": 12}])

        self._seed_schedule_row(writer)
        # The canonical row somehow carries its own events too.
        writer.append_events(self.CANONICAL, [{"seq": 0, "type": "kickoff", "minute": 0}])

        with caplog.at_level(logging.ERROR):
            seeded = seed_or_reconcile_match(conn, writer, _match(), PROVIDER)
        assert seeded == self.STUB, "on a timeline conflict, collection stays on the stub"
        assert any("refusing to interleave" in r.getMessage() for r in caplog.records), (
            "the conflict must be logged loudly at ERROR"
        )
        # Neither timeline was touched.
        canonical_types = [
            r[0]
            for r in conn.execute(
                "SELECT type FROM events WHERE match_id = ? ORDER BY seq", (self.CANONICAL,)
            )
        ]
        stub_types = [
            r[0]
            for r in conn.execute(
                "SELECT type FROM events WHERE match_id = ? ORDER BY seq", (self.STUB,)
            )
        ]
        assert canonical_types == ["kickoff"]
        assert stub_types == ["goal"]
        assert get_state(conn, self.STUB) is not None, "the stub row must survive the conflict"

    def test_football_side_table_rows_follow_the_stub(self, tmp_path):
        from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL

        conn = connect(tmp_path / "adopt.db", side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            writer = PartitionWriter(conn, SOURCE_A)
            assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
            with conn:
                conn.execute(
                    "INSERT INTO football_stats (source, match_id, team, shots) "
                    "VALUES (?, ?, ?, ?)",
                    (SOURCE_A, self.STUB, "Australia", 5),
                )
                # A foreign partition's row keyed to the same stub-looking id
                # must NOT be touched (partition discipline).
                conn.execute(
                    "INSERT INTO football_stats (source, match_id, team, shots) "
                    "VALUES (?, ?, ?, ?)",
                    (SOURCE_B, self.STUB, "Australia", 9),
                )
            self._seed_schedule_row(writer)
            assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.CANONICAL

            rows = conn.execute(
                "SELECT source, match_id, shots FROM football_stats ORDER BY source"
            ).fetchall()
            assert [tuple(r) for r in rows] == [
                (SOURCE_B, self.STUB, 9),
                (SOURCE_A, self.CANONICAL, 5),
            ] or [tuple(r) for r in rows] == [
                (SOURCE_A, self.CANONICAL, 5),
                (SOURCE_B, self.STUB, 9),
            ]
        finally:
            conn.close()

    def test_stub_with_no_events_still_migrates_and_dedupes_rows(self, db):
        """A stub with no events (state-only strip row) is trivially adopted:
        mappings move, the stub matches row is deleted, one row remains."""
        conn, writer = db
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        self._seed_schedule_row(writer)
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.CANONICAL
        assert get_state(conn, self.STUB) is None
        (count,) = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
        assert count == 1


# ===========================================================================
# 8. Review-finding fixes: preserve-richer on every poll, adoption payload
#    merge, split-brain map repoint, freshest-wins side-table migration.
# ===========================================================================


class TestPreserveRicherOnRepoll:
    """Review finding 1: seed_or_reconcile_match's payload merge must use the
    SAME preserve-richer policy as register_unreconciled_match — on both the
    canonical-row path and the stub re-poll path (resolver path (d)), a sparse
    post-FT snapshot must not clobber richer stored values on poll 2+."""

    CANONICAL = "wc2026_md03_m04"
    STUB = f"{SOURCE_A}:760421"

    def _seed_schedule_row(self, writer):
        writer.upsert_match(
            {
                "match_id": self.CANONICAL,
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "payload": {"home_team": "Australia", "away_team": "Turkey"},
            }
        )

    def test_sparse_repoll_does_not_clobber_richer_canonical_payload(self, db):
        import json
        from dataclasses import replace

        conn, writer = db
        self._seed_schedule_row(writer)
        rich = replace(
            _match(),
            payload={"venue": "Estadio Azteca", "stats": [{"team": "Australia", "shots": 9}]},
        )
        assert seed_or_reconcile_match(conn, writer, rich, PROVIDER) == self.CANONICAL

        sparse = replace(
            _match(),
            payload={"venue": None, "stats": [], "attendance": 83000},
        )
        assert seed_or_reconcile_match(conn, writer, sparse, PROVIDER) == self.CANONICAL

        payload = json.loads(get_state(conn, self.CANONICAL)["payload"])
        assert payload["venue"] == "Estadio Azteca", "None must not clobber a stored value"
        assert payload["stats"] == [{"team": "Australia", "shots": 9}], (
            "an empty list must not clobber stored rich data"
        )
        assert payload["attendance"] == 83000, "new non-empty keys still land"

    def test_sparse_repoll_does_not_clobber_richer_stub_payload(self, db):
        """Poll 2+ on an unreconciled match resolves via path (d) to the stub
        id and flows through seed_or_reconcile_match's upsert branch — NOT
        register_unreconciled_match — so the richer guard must hold there."""
        import json
        from dataclasses import replace

        conn, writer = db
        rich = replace(
            _match(),
            payload={"venue": "Estadio Azteca", "stats": [{"team": "Australia", "shots": 9}]},
        )
        assert seed_or_reconcile_match(conn, writer, rich, PROVIDER) == self.STUB

        sparse = replace(_match(), payload={"venue": None, "stats": []})
        assert seed_or_reconcile_match(conn, writer, sparse, PROVIDER) == self.STUB

        payload = json.loads(get_state(conn, self.STUB)["payload"])
        assert payload["venue"] == "Estadio Azteca"
        assert payload["stats"] == [{"team": "Australia", "shots": 9}]


class TestAdoptionPreservesStubPayload:
    """Review finding 2: adoption must merge the stub's accumulated payload
    into the canonical row before deleting the stub — HT scores/venue etc.
    collected on the stub must survive even when adoption fires on a sparse
    poll."""

    CANONICAL = "wc2026_md03_m04"
    STUB = f"{SOURCE_A}:760421"

    def test_adoption_on_sparse_poll_keeps_stub_accumulated_keys(self, db):
        import json
        from dataclasses import replace

        conn, writer = db
        rich = replace(
            _match(),
            payload={
                "venue": "Estadio Azteca",
                "ht_score_home": 1,
                "ht_score_away": 0,
                "round_name": "Group B (provider)",
            },
        )
        assert seed_or_reconcile_match(conn, writer, rich, PROVIDER) == self.STUB

        writer.upsert_match(
            {
                "match_id": self.CANONICAL,
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "payload": {
                    "home_team": "Australia",
                    "away_team": "Turkey",
                    "round_name": "Group B",
                },
            }
        )
        sparse = replace(_match(), payload={})
        assert seed_or_reconcile_match(conn, writer, sparse, PROVIDER) == self.CANONICAL
        assert get_state(conn, self.STUB) is None

        payload = json.loads(get_state(conn, self.CANONICAL)["payload"])
        assert payload["venue"] == "Estadio Azteca", "stub-collected venue must survive adoption"
        assert payload["ht_score_home"] == 1
        assert payload["ht_score_away"] == 0
        assert payload["round_name"] == "Group B", "canonical schedule-owned keys win"
        assert payload["home_team"] == "Australia"
        assert payload["away_team"] == "Turkey", "canonical team names win over stub names"


class TestAdoptionRefusalClearsMap:
    """Review finding: on the both-have-events adoption refusal the resolver's
    path-(c) cache entry must be DELETED, not repointed at the stub. A cached
    source-qualified stub id violates the resolver's own never-cache-qualified
    rule and makes path (a) return the stub forever — adoption never
    re-attempted (contradicting the documented per-attempt ERROR) and healing
    (the operator clearing the canonical row's conflicting events) never
    picked up. With NO entry, every poll re-resolves via (c), re-attempts
    adoption, re-logs the ERROR while the conflict persists, and adoption
    succeeds automatically once it becomes safe."""

    CANONICAL = "wc2026_md03_m04"
    STUB = f"{SOURCE_A}:760421"

    def _mapped_id(self, conn):
        row = conn.execute(
            "SELECT match_id FROM provider_match_map WHERE source = ? AND provider = ? "
            "AND provider_match_id = ?",
            (SOURCE_A, PROVIDER, "760421"),
        ).fetchone()
        return row[0] if row else None

    def _setup_conflict(self, conn, writer):
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        writer.append_events(self.STUB, [{"seq": 0, "type": "goal", "minute": 12}])
        writer.upsert_match(
            {
                "match_id": self.CANONICAL,
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "payload": {"home_team": "Australia", "away_team": "Turkey"},
            }
        )
        writer.append_events(self.CANONICAL, [{"seq": 0, "type": "kickoff", "minute": 0}])

    def test_refusal_leaves_no_map_entry_and_relogs_error_every_poll(self, db, caplog):
        import logging

        conn, writer = db
        self._setup_conflict(conn, writer)

        with caplog.at_level(logging.ERROR):
            assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        assert any("refusing to interleave" in r.getMessage() for r in caplog.records)
        assert self._mapped_id(conn) is None, (
            "a refusal must leave NO provider_match_map entry — a cached stub id "
            "would freeze adoption forever, and a canonical entry points readers "
            "at the frozen timeline"
        )

        # Every subsequent poll re-attempts adoption and re-logs the ERROR
        # while the conflict persists; the map stays empty.
        caplog.clear()
        with caplog.at_level(logging.ERROR):
            assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        assert any("refusing to interleave" in r.getMessage() for r in caplog.records), (
            "the adoption ERROR must be re-logged on EVERY poll, not once"
        )
        assert self._mapped_id(conn) is None

        # The stub keeps collecting the live timeline meanwhile.
        writer.append_events(self.STUB, [{"seq": 1, "type": "goal", "minute": 44}])
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM events WHERE source = ? AND match_id = ?",
            (SOURCE_A, self.STUB),
        ).fetchone()
        assert n == 2

    def test_clearing_canonical_events_heals_adoption_on_next_poll(self, db):
        conn, writer = db
        self._setup_conflict(conn, writer)
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        assert self._mapped_id(conn) is None

        # Operator repairs the split history: the canonical row's conflicting
        # events are cleared. The very next poll must adopt automatically.
        with conn:
            conn.execute(
                "DELETE FROM events WHERE source = ? AND match_id = ?",
                (SOURCE_A, self.CANONICAL),
            )
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.CANONICAL

        seqs = [
            r[0]
            for r in conn.execute(
                "SELECT seq FROM events WHERE source = ? AND match_id = ? ORDER BY seq",
                (SOURCE_A, self.CANONICAL),
            )
        ]
        assert seqs == [0], "the stub's timeline must migrate on the healing poll"
        assert get_state(conn, self.STUB) is None, "the stub row must be adopted away"
        assert self._mapped_id(conn) == self.CANONICAL, (
            "after healing, the map must point at the canonical row"
        )


class TestSideTableCollisionFreshestWins:
    """Review finding 4: on a side-table PK collision during adoption (e.g. a
    fixture-imported row already under the canonical id), the stub's row was
    written by live collection and must REPLACE the stale canonical row — not
    be silently dropped."""

    CANONICAL = "wc2026_md03_m04"
    STUB = f"{SOURCE_A}:760421"

    def test_stub_row_replaces_colliding_canonical_row(self, tmp_path):
        from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL

        conn = connect(tmp_path / "collide-adopt.db", side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            writer = PartitionWriter(conn, SOURCE_A)
            assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
            writer.upsert_match(
                {
                    "match_id": self.CANONICAL,
                    "status": "SCHEDULED",
                    "kickoff_utc": "2026-06-14T04:00:00+00:00",
                    "payload": {"home_team": "Australia", "away_team": "Turkey"},
                }
            )
            with conn:
                # Fresher live-collected row on the stub...
                conn.execute(
                    "INSERT INTO football_stats (source, match_id, team, shots) "
                    "VALUES (?, ?, ?, ?)",
                    (SOURCE_A, self.STUB, "Australia", 9),
                )
                # ...colliding with a stale fixture-imported canonical row
                # (same PK remainder: source + team).
                conn.execute(
                    "INSERT INTO football_stats (source, match_id, team, shots) "
                    "VALUES (?, ?, ?, ?)",
                    (SOURCE_A, self.CANONICAL, "Australia", 1),
                )
                # A non-colliding stub row must migrate too.
                conn.execute(
                    "INSERT INTO football_stats (source, match_id, team, shots) "
                    "VALUES (?, ?, ?, ?)",
                    (SOURCE_A, self.STUB, "Turkey", 4),
                )

            assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.CANONICAL

            rows = conn.execute(
                "SELECT source, match_id, team, shots FROM football_stats ORDER BY team"
            ).fetchall()
            assert [tuple(r) for r in rows] == [
                (SOURCE_A, self.CANONICAL, "Australia", 9),
                (SOURCE_A, self.CANONICAL, "Turkey", 4),
            ], "stub's fresher row must replace the stale canonical row; none dropped"
            (stranded,) = conn.execute(
                "SELECT COUNT(*) FROM football_stats WHERE match_id = ?", (self.STUB,)
            ).fetchone()
            assert stranded == 0
        finally:
            conn.close()


class TestStaleStubMapEntryHeals:
    """Review finding: databases written by the previous repoint-on-refusal
    revision hold provider→stub map entries. Resolver path (a) must treat a
    cached source-qualified stub id as stale — purge it (INFO) and fall
    through to normal resolution, so adoption is re-attempted instead of the
    stub being returned forever."""

    CANONICAL = "wc2026_md03_m04"
    STUB = f"{SOURCE_A}:760421"

    def _seed_schedule_row(self, writer):
        writer.upsert_match(
            {
                "match_id": self.CANONICAL,
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "payload": {"home_team": "Australia", "away_team": "Turkey"},
            }
        )

    def _plant_stale_entry(self, conn):
        # Simulate the old code's repoint: a map entry at the stub id, the
        # shape the resolver's own rules say must never be cached.
        with conn:
            conn.execute(
                "INSERT INTO provider_match_map (source, provider, provider_match_id, match_id) "
                "VALUES (?, ?, ?, ?)",
                (SOURCE_A, PROVIDER, "760421", self.STUB),
            )

    def test_stale_stub_entry_is_purged_and_adoption_reattempted(self, db, caplog):
        import logging

        conn, writer = db
        # Old-world state: stub row with events, canonical schedule row
        # present, and a stale provider→stub map entry.
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        writer.append_events(self.STUB, [{"seq": 0, "type": "goal", "minute": 12}])
        self._seed_schedule_row(writer)
        self._plant_stale_entry(conn)

        with caplog.at_level(logging.INFO, logger="gamecollect_football.reconcile"):
            seeded = seed_or_reconcile_match(conn, writer, _match(), PROVIDER)
        assert seeded == self.CANONICAL, (
            "a stale stub map entry must not pin resolution to the stub forever"
        )
        assert any("stale" in r.getMessage() for r in caplog.records), (
            "purging the stale entry must be logged at INFO"
        )
        assert get_state(conn, self.STUB) is None, "the stub must be adopted away"
        row = conn.execute(
            "SELECT match_id FROM provider_match_map WHERE source = ? AND provider = ? "
            "AND provider_match_id = ?",
            (SOURCE_A, PROVIDER, "760421"),
        ).fetchone()
        assert row is not None and row[0] == self.CANONICAL

    def test_stale_entry_purged_via_public_resolver_too(self, db):
        """resolve_canonical_match_id (the public wrapper) applies the same
        stale-stub guard: the entry is purged and (c) re-binds canonically."""
        conn, writer = db
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        self._seed_schedule_row(writer)
        self._plant_stale_entry(conn)

        assert resolve_canonical_match_id(conn, writer, _match(), PROVIDER) == self.CANONICAL
        row = conn.execute(
            "SELECT match_id FROM provider_match_map WHERE source = ? AND provider = ? "
            "AND provider_match_id = ?",
            (SOURCE_A, PROVIDER, "760421"),
        ).fetchone()
        assert row is not None and row[0] == self.CANONICAL


class TestNoMapChurnDuringRefusal:
    """Review finding: while a both-have-events refusal conflict persists, the
    old cache-then-delete order performed an INSERT + DELETE on
    provider_match_map every poll. With deferred caching the map must see NO
    writes at all during a conflicted poll."""

    CANONICAL = "wc2026_md03_m04"
    STUB = f"{SOURCE_A}:760421"

    def _setup_conflict(self, conn, writer):
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        writer.append_events(self.STUB, [{"seq": 0, "type": "goal", "minute": 12}])
        writer.upsert_match(
            {
                "match_id": self.CANONICAL,
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "payload": {"home_team": "Australia", "away_team": "Turkey"},
            }
        )
        writer.append_events(self.CANONICAL, [{"seq": 0, "type": "kickoff", "minute": 0}])

    def test_conflicted_polls_perform_no_provider_map_writes(self, db):
        conn, writer = db
        self._setup_conflict(conn, writer)
        # First conflicted poll flushes any one-time work.
        assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB

        statements: list[str] = []
        conn.set_trace_callback(statements.append)
        try:
            assert seed_or_reconcile_match(conn, writer, _match(), PROVIDER) == self.STUB
        finally:
            conn.set_trace_callback(None)

        map_writes = [
            s
            for s in statements
            if "provider_match_map" in s
            and any(op in s.upper() for op in ("INSERT", "DELETE", "UPDATE"))
        ]
        assert map_writes == [], (
            "a persisting refusal conflict must not churn provider_match_map "
            f"every poll; saw: {map_writes}"
        )
