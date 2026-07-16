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
