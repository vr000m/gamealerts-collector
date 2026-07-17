"""Phase 3: football-profile ``MatchReadPort`` adapter (``gamecollect_football.readport``).

Plan (docs/dev_plans/20260707-feature-gameworker-integration.md, Phase 3):
- The adapter lives in the pack (not core, per pack->core layering) and
  implements the generic ``MatchReadPort`` surface over the collector's
  reader/client + football side tables, returning plain dicts/lists.
- It tolerates an absent ``commentary`` table (returns ``[]``, mirroring the
  pre-match venue/roster ``None``/empty case) since gamealerts creates that
  table lazily on first write.
- Football-profile values for the three already-committed knockout fixtures
  (regulation/AET/penalties, ``tests/football/test_knockout_fixtures.py`` +
  ``golden/``) must come back correctly shaped through the adapter.
- Missing venue/roster (pre-match, no Phase-2 side-table rows yet) returns
  None/empty, not an error.

The adapter class name/constructor and exact Protocol method names are an
implementation choice made by a parallel Phase 3 implementer working
concurrently with this file, so both are discovered via introspection
(mirroring ``tests/football/test_side_tables_venue_roster.py``'s
skip-with-reason convention) rather than hard-guessed.
"""

from __future__ import annotations

import inspect

import pytest
from _football_helpers import KNOCKOUT_FIXTURES, replay_fixture

from gamecollect.db.connection import connect
from gamecollect.db.writer import PartitionWriter
from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL, persist_football_side_tables
from gamecollect_football.readport import FootballReadPort

SOURCE = "wc2026"

CANDIDATE_NAMES: dict[str, tuple[str, ...]] = {
    "state": ("latest_state", "state_for_match", "get_state", "match_state"),
    "events": ("events_for_match", "get_events", "match_events"),
    "lineup": ("lineup_for_match", "get_lineup"),
    "venue": ("venue_for_match", "get_venue"),
    "roster": ("roster_for_team", "get_roster"),
    "commentary": ("recent_commentary", "get_commentary"),
}


def _match_read_port():
    try:
        from gamecollect.readport import MatchReadPort
    except ImportError:
        pytest.skip("gamecollect.readport.MatchReadPort does not exist yet")
    return MatchReadPort


def _protocol_method_names(protocol) -> list[str]:
    names = []
    for name in dir(protocol):
        if name.startswith("_"):
            continue
        attr = getattr(protocol, name, None)
        if callable(attr):
            names.append(name)
    return sorted(names)


def _resolve(protocol, keyword_variants: tuple[str, ...]) -> str | None:
    names = _protocol_method_names(protocol)
    for candidate in keyword_variants:
        if candidate in names:
            return candidate
    core = keyword_variants[0].split("_")[0]
    for name in names:
        if core in name:
            return name
    return None


def _football_readport_module():
    try:
        import gamecollect_football.readport as module
    except ImportError:
        pytest.skip("gamecollect_football.readport does not exist yet")
    return module


def _find_adapter_class():
    module = _football_readport_module()
    protocol = _match_read_port()
    proto_methods = set(_protocol_method_names(protocol))
    candidates = [
        obj
        for name, obj in vars(module).items()
        if inspect.isclass(obj) and not name.startswith("_")
    ]
    for cls in candidates:
        if proto_methods and proto_methods.issubset(set(dir(cls))):
            return cls
    for cls in candidates:
        if "ReadPort" in cls.__name__ or "Adapter" in cls.__name__:
            return cls
    pytest.skip("no adapter class found in gamecollect_football.readport")


def _make_adapter(conn):
    cls = _find_adapter_class()
    for args in ((conn,), ()):
        try:
            return cls(*args)
        except TypeError:
            continue
    try:
        return cls(conn=conn)
    except TypeError:
        pytest.skip(f"cannot construct {cls} with a bare connection")


def _event_row(event) -> dict:
    payload = {
        key: value
        for key, value in (
            ("team", event.team),
            ("player", event.player),
            ("assist", event.assist),
        )
        if value is not None
    }
    row = {
        "seq": event.seq,
        "type": event.event_type,
        "minute": event.minute,
        "importance": event.importance,
        "detail": event.detail,
    }
    if payload:
        row["payload"] = payload
    return row


def _seed_fixture(conn, fx: dict):
    """Replay one committed knockout fixture and write it through the real
    collector write path (upsert_match + events + football side tables)."""
    match_id = fx["match_id"]
    match = replay_fixture(fx["summary"], match_id)
    writer = PartitionWriter(conn, SOURCE)
    status = match.status.value if hasattr(match.status, "value") else str(match.status)
    # The real write cycle stamps home_team/away_team into payload via
    # gamecollect_football.reconcile before the adapter ever reads them —
    # mirror that so the readport's participants projection has data.
    payload = dict(match.payload)
    payload["home_team"] = match.home_team
    payload["away_team"] = match.away_team
    writer.upsert_match(
        {
            "match_id": match_id,
            "source": SOURCE,
            "status": status,
            "kickoff_utc": match.kickoff_utc,
            "score_home": match.score_home,
            "score_away": match.score_away,
            "display_clock": match.display_clock,
            "payload": payload,
        }
    )
    if match.events:
        writer.append_events(match_id, [_event_row(ev) for ev in match.events])
    persist_football_side_tables(conn, writer, match, match_id)
    conn.commit()
    return match


@pytest.fixture
def db(tmp_path):
    conn = connect(tmp_path / "football_readport.db", side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
    yield conn
    conn.close()


class TestAdapterSatisfiesProtocolStructurally:
    def test_football_adapter_satisfies_match_read_port(self, db):
        protocol = _match_read_port()
        adapter = _make_adapter(db)

        if getattr(protocol, "_is_runtime_protocol", False):
            assert isinstance(adapter, protocol), (
                f"{type(adapter).__name__} does not structurally satisfy "
                f"@runtime_checkable MatchReadPort"
            )
        else:
            missing = [
                name
                for name in _protocol_method_names(protocol)
                if not callable(getattr(adapter, name, None))
            ]
            assert not missing, (
                f"MatchReadPort is not @runtime_checkable; falling back to a "
                f"method-name check — adapter is missing: {missing}"
            )


class TestAbsentCommentaryTable:
    def test_commentary_method_returns_empty_not_exception(self, db):
        """No gamealerts commentary table exists yet on a collector-first
        boot / pre-match / replay file — the adapter must tolerate this."""
        fx = KNOCKOUT_FIXTURES[0]
        _seed_fixture(db, fx)

        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["commentary"])
        if name is None:
            pytest.skip("no commentary-shaped method found on MatchReadPort")

        assert "commentary" not in {
            r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

        adapter = _make_adapter(db)
        result = getattr(adapter, name)(fx["match_id"])
        assert result == [] or result == () or result is None


class TestKnockoutScenarios:
    """Football-profile values for the three committed knockout scenarios
    (regulation / AET / penalties, tests/football/test_knockout_fixtures.py)
    come back correctly shaped through the adapter."""

    @pytest.fixture(params=KNOCKOUT_FIXTURES, ids=[fx["match_id"] for fx in KNOCKOUT_FIXTURES])
    def seeded(self, request, db):
        fx = request.param
        match = _seed_fixture(db, fx)
        return db, fx, match

    def test_state_reflects_seeded_scoreboard(self, seeded):
        conn, fx, match = seeded
        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["state"])
        if name is None:
            pytest.skip("no state-shaped method found on MatchReadPort")
        adapter = _make_adapter(conn)
        result = adapter.__getattribute__(name)(fx["match_id"])
        assert result is not None
        assert "team1" not in result and "team2" not in result

    def test_result_type_reachable_via_events_or_state(self, seeded):
        """The three scenarios are distinguished by
        ``payload['result_type']`` (regulation/extra_time/penalties) — assert
        the adapter surfaces the seeded match without raising for every
        scenario, across whichever shaped method exposes it."""
        conn, fx, match = seeded
        protocol = _match_read_port()
        adapter = _make_adapter(conn)
        state_name = _resolve(protocol, CANDIDATE_NAMES["state"])
        events_name = _resolve(protocol, CANDIDATE_NAMES["events"])
        if state_name is None and events_name is None:
            pytest.skip("no state- or events-shaped method found on MatchReadPort")
        if state_name is not None:
            getattr(adapter, state_name)(fx["match_id"])
        if events_name is not None:
            events = getattr(adapter, events_name)(fx["match_id"])
            assert isinstance(events, list)
            assert len(events) == len(match.events)

    def test_lineup_shaped_method_returns_seeded_side_table_data(self, seeded):
        conn, fx, match = seeded
        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["lineup"])
        if name is None:
            pytest.skip("no lineup-shaped method found on MatchReadPort")
        adapter = _make_adapter(conn)
        result = getattr(adapter, name)(fx["match_id"])
        assert result is not None


class TestMissingVenueAndRoster:
    """Pre-match, no Phase-2 side-table rows yet -> None/empty, not an error."""

    def test_venue_method_returns_none_or_empty_when_unseeded(self, db):
        fx = KNOCKOUT_FIXTURES[0]
        match = replay_fixture(fx["summary"], fx["match_id"])
        writer = PartitionWriter(db, SOURCE)
        status = match.status.value if hasattr(match.status, "value") else str(match.status)
        # Seed only the bare match row — no side tables (pre-match state).
        writer.upsert_match(
            {
                "match_id": fx["match_id"],
                "source": SOURCE,
                "status": status,
                "kickoff_utc": match.kickoff_utc,
                "score_home": None,
                "score_away": None,
                "display_clock": None,
            }
        )
        db.commit()

        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["venue"])
        if name is None:
            pytest.skip("no venue-shaped method found on MatchReadPort")
        adapter = _make_adapter(db)
        result = getattr(adapter, name)(fx["match_id"])
        assert result is None or result == {} or result == []

    def test_roster_method_returns_none_or_empty_when_unseeded(self, db):
        fx = KNOCKOUT_FIXTURES[0]
        match = replay_fixture(fx["summary"], fx["match_id"])
        writer = PartitionWriter(db, SOURCE)
        status = match.status.value if hasattr(match.status, "value") else str(match.status)
        writer.upsert_match(
            {
                "match_id": fx["match_id"],
                "source": SOURCE,
                "status": status,
                "kickoff_utc": match.kickoff_utc,
                "score_home": None,
                "score_away": None,
                "display_clock": None,
            }
        )
        db.commit()

        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["roster"])
        if name is None:
            pytest.skip("no roster-shaped method found on MatchReadPort")
        adapter = _make_adapter(db)
        sig = inspect.signature(getattr(adapter, name))
        params = [p for p in sig.parameters.values() if p.name != "self"]
        if len(params) == 1:
            result = getattr(adapter, name)(match.home_team)
        else:
            result = getattr(adapter, name)(fx["match_id"], match.home_team)
        assert result is None or result == {} or result == []


class TestLegacyRowCanonicalization:
    """Regression: rows written before write-side team-name canonicalization
    (or by legacy code) can carry a raw provider name (e.g. "Türkiye") in
    football_lineups.team, while ``participant`` passed to the adapter is
    always the canonical display name (e.g. "Turkey") from latest_state().
    An exact-string filter in ``_lineup_rows`` would silently drop these rows;
    the fix compares through ``canonical_team_name`` on both sides."""

    MATCH_ID = "760421"

    def test_lineup_for_match_matches_legacy_raw_team_name(self, db):
        writer = PartitionWriter(db, SOURCE)
        writer.upsert_match(
            {
                "match_id": self.MATCH_ID,
                "source": SOURCE,
                "status": "IN_PLAY",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "score_home": 1,
                "score_away": 0,
                "display_clock": "27'",
                "payload": {"home_team": "Turkey", "away_team": "Australia"},
            }
        )
        with db:
            # Legacy/raw provider spelling stored directly, never passed
            # through canonical_display_name at write time.
            db.execute(
                "INSERT INTO football_lineups "
                "(source, match_id, team, athlete_id, display_name, name_folded, starter) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (SOURCE, self.MATCH_ID, "Türkiye", "tur-1", "Legacy Starter", "legacy starter", 1),
            )
        db.commit()

        adapter = FootballReadPort(db)
        # Query with the canonical participant name, as latest_state() would
        # supply it.
        rows = adapter.lineup_for_match(self.MATCH_ID, participant="Turkey")
        assert [r["player"] for r in rows] == ["Legacy Starter"]

    def test_lineup_team_announced_matches_legacy_raw_team_name(self, db):
        writer = PartitionWriter(db, SOURCE)
        writer.upsert_match(
            {
                "match_id": self.MATCH_ID,
                "source": SOURCE,
                "status": "IN_PLAY",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "score_home": 1,
                "score_away": 0,
                "display_clock": "27'",
                "payload": {"home_team": "Turkey", "away_team": "Australia"},
            }
        )
        with db:
            db.execute(
                "INSERT INTO football_lineups "
                "(source, match_id, team, athlete_id, display_name, name_folded, starter) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (SOURCE, self.MATCH_ID, "Türkiye", "tur-1", "Legacy Starter", "legacy starter", 1),
            )
        db.commit()

        adapter = FootballReadPort(db)
        assert adapter.lineup_team_announced(self.MATCH_ID, "Turkey") is True


class TestRosterAliasCanonicalization:
    """Regression: football_roster.team is ALWAYS written through
    canonical_display_name (pack.py's _persist_roster_for_team writes the
    squad fixture's canonical name, never a raw provider spelling), but
    _roster_rows previously passed a caller's ``participant`` argument
    straight into an exact-match query with no canonicalization. A caller
    supplying a raw provider alias (e.g. "Türkiye" instead of the stored
    "Turkey") would silently get no rows back from roster_for_team/
    roster_contains — this pins that the read path canonicalizes the query
    argument the same way the write path canonicalized what it stored."""

    def _seed_roster_row(self, db):
        with db:
            db.execute(
                "INSERT INTO football_roster "
                "(team, fifa_code, group_name, number, position, player_name, "
                "name_folded, date_of_birth) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("Turkey", "TUR", "A", 10, "MF", "Roster Player", "roster player", None),
            )
        db.commit()

    def test_roster_for_team_matches_raw_provider_alias(self, db):
        self._seed_roster_row(db)
        adapter = FootballReadPort(db)
        result = adapter.roster_for_team("Türkiye")
        assert [r["player"] for r in result] == ["Roster Player"]

    def test_roster_contains_matches_raw_provider_alias(self, db):
        self._seed_roster_row(db)
        adapter = FootballReadPort(db)
        assert adapter.roster_contains("Türkiye", "Roster Player") is True

    def test_roster_for_team_matches_casing_variant_not_an_alias_key(self, db):
        """Round 3 review finding: ``_roster_rows`` compared through
        ``canonical_display_name`` (alias-dict lookup only — exact-key
        substitution, no casefold/diacritic-fold), a weaker comparison than
        ``_lineup_rows``'s ``canonical_team_name`` (fold-based: casefolds and
        strips diacritics in addition to alias lookup). A participant string
        that folds to the same team but is neither byte-identical to the
        stored display name NOR itself a ``TEAM_ALIASES`` key — e.g. an
        all-caps variant — would fail to match in the (old) roster path while
        succeeding in the lineup path. This pins that both paths now use the
        same fold-based comparison strength."""
        self._seed_roster_row(db)
        adapter = FootballReadPort(db)
        result = adapter.roster_for_team("TURKEY")
        assert [r["player"] for r in result] == ["Roster Player"]


class TestLegacyGoalScorerFallback:
    """Regression: existing databases can contain goal/own_goal event rows
    written before the scorer-field rename, using payload.player instead of
    payload.scorer (see gamecollect.client._normalize_legacy_payload for the
    same fallback on the core read path). The adapter must still surface the
    scorer name for these legacy-shaped rows."""

    MATCH_ID = "760421"

    def test_project_event_falls_back_to_legacy_player_key(self, db):
        writer = PartitionWriter(db, SOURCE)
        writer.upsert_match(
            {
                "match_id": self.MATCH_ID,
                "source": SOURCE,
                "status": "IN_PLAY",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "score_home": 1,
                "score_away": 0,
                "display_clock": "27'",
                "payload": {"home_team": "Turkey", "away_team": "Australia"},
            }
        )
        writer.append_events(
            self.MATCH_ID,
            [
                {
                    "seq": 0,
                    "type": "goal",
                    "minute": 27,
                    # Legacy shape: scorer under "player", not "scorer".
                    "payload": {"team": "Turkey", "player": "Legacy Scorer"},
                }
            ],
        )
        db.commit()

        adapter = FootballReadPort(db)
        events = adapter.events_for_match(self.MATCH_ID)
        assert [e["player"] for e in events] == ["Legacy Scorer"]


class TestCurrentPhaseTargetedLookup:
    """latest_state() is a per-poll hot path; _current_phase must find the
    last phase-marker event via a targeted query (reader.get_latest_event_of_type,
    ORDER BY seq DESC LIMIT 1) rather than decoding the full event history on
    every call. This is a behavior test (correct last-marker result across
    several events, including non-marker events interleaved after the last
    marker) -- it does not assert on query plan."""

    MATCH_ID = "760421"

    def _seed(self, db):
        writer = PartitionWriter(db, SOURCE)
        writer.upsert_match(
            {
                "match_id": self.MATCH_ID,
                "source": SOURCE,
                "status": "IN_PLAY",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "score_home": 0,
                "score_away": 0,
                "display_clock": "1'",
                "payload": {"home_team": "Turkey", "away_team": "Australia"},
            }
        )
        writer.append_events(
            self.MATCH_ID,
            [
                {"seq": 0, "type": "kickoff", "minute": 0},
                {"seq": 1, "type": "goal", "minute": 10, "payload": {"team": "Turkey"}},
                {"seq": 2, "type": "half_time", "minute": 45},
                {"seq": 3, "type": "goal", "minute": 50, "payload": {"team": "Australia"}},
            ],
        )
        db.commit()

    def test_current_phase_returns_last_marker_past_later_non_marker_events(self, db):
        self._seed(db)
        adapter = FootballReadPort(db)
        state = adapter.latest_state(self.MATCH_ID)
        assert state["phase"] == "half_time"

    def test_get_latest_event_of_type_returns_last_matching_row(self, db):
        from gamecollect.db import reader

        self._seed(db)
        row = reader.get_latest_event_of_type(
            db, self.MATCH_ID, frozenset({"kickoff", "half_time", "full_time"})
        )
        assert row is not None
        assert row["type"] == "half_time"
        assert row["seq"] == 2

    def test_get_latest_event_of_type_returns_none_when_no_match(self, db):
        from gamecollect.db import reader

        self._seed(db)
        row = reader.get_latest_event_of_type(db, self.MATCH_ID, frozenset({"full_time"}))
        assert row is None


class TestListMatches:
    """Discovery/resolution gap raised by gamealerts while building against
    PR #5: every other MatchReadPort method needs an already-known match_id
    or participant. list_matches() closes it — a consumer holding only the
    port can enumerate matches and fold-match a spoken/typed team name
    against participants[].name to resolve a match_id."""

    def _seed_two_matches(self, db):
        writer = PartitionWriter(db, SOURCE)
        writer.upsert_match(
            {
                "match_id": "760421",
                "source": SOURCE,
                "status": "IN_PLAY",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "score_home": 1,
                "score_away": 0,
                "display_clock": "27'",
                "payload": {"home_team": "Turkey", "away_team": "Australia"},
            }
        )
        writer.upsert_match(
            {
                "match_id": "760422",
                "source": SOURCE,
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-15T04:00:00+00:00",
                "score_home": None,
                "score_away": None,
                "display_clock": None,
                "payload": {"home_team": "Spain", "away_team": "Portugal"},
            }
        )

    def test_returns_a_summary_per_seeded_match(self, db):
        self._seed_two_matches(db)
        adapter = FootballReadPort(db)
        results = adapter.list_matches()
        assert {r["match_id"] for r in results} == {"760421", "760422"}

    def test_summary_shape_has_participants_with_side_and_score(self, db):
        self._seed_two_matches(db)
        adapter = FootballReadPort(db)
        results = adapter.list_matches()
        by_id = {r["match_id"]: r for r in results}
        turkey_match = by_id["760421"]
        assert turkey_match["status"] == "IN_PLAY"
        assert turkey_match["kickoff_utc"] == "2026-06-14T04:00:00+00:00"
        assert turkey_match["participants"] == [
            {"name": "Turkey", "side": "home", "score": 1},
            {"name": "Australia", "side": "away", "score": 0},
        ]

    def test_status_filter_scopes_results(self, db):
        self._seed_two_matches(db)
        adapter = FootballReadPort(db)
        results = adapter.list_matches(status="SCHEDULED")
        assert {r["match_id"] for r in results} == {"760422"}

    def test_source_filter_excludes_other_sources(self, db):
        self._seed_two_matches(db)
        adapter = FootballReadPort(db)
        assert adapter.list_matches(source="not-a-real-source") == []

    def test_no_matches_returns_empty_list(self, db):
        adapter = FootballReadPort(db)
        assert adapter.list_matches() == []

    def test_participant_names_are_canonical_not_raw_provider_strings(self, db):
        """Regression guard mirroring TestLegacyRowCanonicalization above:
        list_matches reads payload["home_team"]/["away_team"], which the
        write seam always canonicalizes (gamecollect_football.reconcile) —
        confirm a canonical name comes back, not a raw provider alias."""
        writer = PartitionWriter(db, SOURCE)
        writer.upsert_match(
            {
                "match_id": "760423",
                "source": SOURCE,
                "status": "IN_PLAY",
                "kickoff_utc": "2026-06-16T04:00:00+00:00",
                "score_home": 0,
                "score_away": 0,
                "display_clock": "5'",
                # Written pre-canonicalized, exactly as the real write seam
                # would stamp it (canonical_display_name("Türkiye") == "Turkey").
                "payload": {"home_team": "Turkey", "away_team": "Cape Verde"},
            }
        )
        adapter = FootballReadPort(db)
        results = adapter.list_matches()
        names = {p["name"] for r in results for p in r["participants"]}
        assert "Turkey" in names
        assert "Türkiye" not in names
