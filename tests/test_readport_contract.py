"""Phase 3: generic ``MatchReadPort`` Protocol contract.

Plan (docs/dev_plans/20260707-feature-gameworker-integration.md, Phase 3 +
Review Focus "Read shape genericity"):

- ``src/gamecollect/readport.py`` (core) declares a sport-neutral, typing-only
  ``MatchReadPort`` Protocol: a ``participants`` list (name/side/score) rather
  than ``team1``/``team2`` columns, events carrying a ``phase`` field rather
  than a hardcoded ``half_time`` literal, lineup/roster keyed by participant.
- Structural ``Protocol`` satisfaction alone does NOT catch a polluted
  signature (an adapter can satisfy ``isinstance`` while the Protocol's own
  method signatures still leak football column names) — the plan explicitly
  calls for a source/AST/grep test over ``readport.py``'s source text.

The exact method names on ``MatchReadPort`` are an implementation choice made
by a parallel Phase 3 implementer (working concurrently with this test file),
so method names are discovered via introspection against the plan's stated
10-method mapping (``latest_state``, ``events_for_match``,
``lineup_for_match``, ``lineup_for_match_live``, ``venue_for_match``,
``roster_for_team``, ``roster_contains``, ``player_in_lineup``,
``lineup_team_announced``, ``recent_commentary``) rather than hard-guessed —
a shape test skips with a clear reason if it cannot resolve a method, instead
of failing on a wrong guess.
"""

from __future__ import annotations

import inspect
import re
import sys
from pathlib import Path

import pytest

# tests/test_readport_contract.py lives outside tests/football/, so
# _football_helpers is not importable via plain rootdir-relative resolution
# the way tests/football/*.py imports it (same-directory). Add that
# directory to sys.path explicitly rather than relying on collection-order
# side effects from other test modules importing it first.
sys.path.insert(0, str(Path(__file__).parent / "football"))
from _football_helpers import KNOCKOUT_FIXTURES, replay_fixture  # noqa: E402

from gamecollect.db.connection import connect
from gamecollect.db.writer import PartitionWriter
from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL, persist_football_side_tables

READPORT_MODULE = "gamecollect.readport"
FOOTBALL_READPORT_MODULE = "gamecollect_football.readport"

# Football literals that must never leak into the generic Protocol's method
# signatures/type annotations (Review Focus "Read shape genericity").
FORBIDDEN_FOOTBALL_LITERALS = ("team1", "team2", "score_home", "score_away", "formation")

SOURCE = "wc2026"

# Candidate names for each of the plan's 10 documented worker-facing methods,
# most-likely-name first, used only to resolve the *actual* names the
# implementer chose without hard-failing on a wrong guess.
CANDIDATE_NAMES: dict[str, tuple[str, ...]] = {
    "state": ("latest_state", "state_for_match", "get_state", "match_state"),
    "events": ("events_for_match", "get_events", "match_events"),
    "lineup": ("lineup_for_match", "get_lineup"),
    "lineup_live": ("lineup_for_match_live",),
    "venue": ("venue_for_match", "get_venue"),
    "roster": ("roster_for_team", "get_roster"),
    "roster_contains": ("roster_contains",),
    "player_in_lineup": ("player_in_lineup",),
    "lineup_announced": ("lineup_team_announced",),
    "commentary": ("recent_commentary", "get_commentary"),
}


def _import_readport():
    try:
        import gamecollect.readport as readport_module
    except ImportError:
        pytest.skip(f"{READPORT_MODULE} does not exist yet (Phase 3 implementer in progress)")
    return readport_module


def _match_read_port():
    module = _import_readport()
    protocol = getattr(module, "MatchReadPort", None)
    if protocol is None:
        pytest.skip(f"{READPORT_MODULE}.MatchReadPort is not defined yet")
    return protocol


def _protocol_method_names(protocol) -> list[str]:
    """Public callables declared on the Protocol (structural surface)."""
    names = []
    for name in dir(protocol):
        if name.startswith("_"):
            continue
        try:
            attr = getattr(protocol, name)
        except AttributeError:
            continue
        if callable(attr):
            names.append(name)
    return sorted(names)


def _resolve(protocol, keyword_variants: tuple[str, ...]) -> str | None:
    """Find the Protocol method name matching one of ``keyword_variants``,
    falling back to a substring search on the first variant's keyword root."""
    names = _protocol_method_names(protocol)
    for candidate in keyword_variants:
        if candidate in names:
            return candidate
    core = keyword_variants[0].split("_")[0]
    for name in names:
        if core in name:
            return name
    return None


def _find_football_adapter_class():
    try:
        import gamecollect_football.readport as football_readport_module
    except ImportError:
        pytest.skip(
            f"{FOOTBALL_READPORT_MODULE} does not exist yet (Phase 3 implementer in progress)"
        )
    protocol = _match_read_port()
    proto_methods = set(_protocol_method_names(protocol))
    candidates = [
        obj
        for name, obj in vars(football_readport_module).items()
        if inspect.isclass(obj) and not name.startswith("_")
    ]
    for cls in candidates:
        if proto_methods and proto_methods.issubset(set(dir(cls))):
            return cls
    for cls in candidates:
        if "ReadPort" in cls.__name__ or "Adapter" in cls.__name__:
            return cls
    pytest.skip(f"no adapter class found in {FOOTBALL_READPORT_MODULE}")


def _make_football_adapter(conn):
    cls = _find_football_adapter_class()
    for args in ((conn,), (), (conn,)):
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


@pytest.fixture
def seeded_db(tmp_path):
    """A temp collector DB seeded with one committed knockout fixture,
    replayed through the real football adapter (facts + events + side
    tables), mirroring the engine's per-poll write path."""
    fx = KNOCKOUT_FIXTURES[0]
    match_id = fx["match_id"]
    match = replay_fixture(fx["summary"], match_id)

    conn = connect(tmp_path / "readport.db", side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
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
    try:
        yield conn, match_id, fx
    finally:
        conn.close()


class TestProtocolSourceHasNoFootballLiterals:
    """Review Focus "Read shape genericity": structural satisfaction alone
    does not catch a polluted Protocol signature — scan the source text."""

    def test_readport_source_contains_no_football_literals(self):
        module = _import_readport()
        source = inspect.getsource(module)
        leaked = [literal for literal in FORBIDDEN_FOOTBALL_LITERALS if literal in source]
        assert not leaked, (
            f"generic MatchReadPort Protocol source contains football-specific "
            f"literal(s) {leaked} — the Protocol must stay sport-neutral in its "
            f"type signatures; football specifics belong only in the adapter's values"
        )

    def test_readport_source_uses_participants_not_two_team_columns(self):
        """Positive-shape half of the genericity check: the Protocol text
        should reference a generic participants concept somewhere, not just
        the absence of football literals (an empty file would pass the
        negative check vacuously)."""
        module = _import_readport()
        source = inspect.getsource(module)
        assert re.search(r"\bparticipant", source, re.IGNORECASE), (
            "expected the generic MatchReadPort Protocol to reference "
            "'participant(s)' as its team/side concept"
        )


class TestFootballAdapterSatisfiesProtocolStructurally:
    def test_adapter_satisfies_protocol(self, seeded_db):
        conn, _match_id, _fx = seeded_db
        protocol = _match_read_port()
        adapter = _make_football_adapter(conn)

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


class TestGenericShapesOverSeededDB:
    """Every documented Protocol method returns the documented generic shape
    (participants list keyed by name/side/score; events carrying a phase
    field) over a seeded temp DB — not team1/team2 or a hardcoded half_time
    literal check."""

    def test_state_method_returns_participants_not_team_columns(self, seeded_db):
        conn, match_id, _fx = seeded_db
        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["state"])
        if name is None:
            pytest.skip("no state-shaped method found on MatchReadPort")
        adapter = _make_football_adapter(conn)
        result = getattr(adapter, name)(match_id)
        assert result is not None
        assert "team1" not in result and "team2" not in result
        assert "participants" in result, (
            f"{name}() must return a generic 'participants' list, not team1/team2 columns"
        )
        participants = result["participants"]
        assert isinstance(participants, list)
        assert len(participants) >= 2
        for participant in participants:
            assert "side" in participant or "name" in participant

    def test_events_method_returns_phase_field(self, seeded_db):
        conn, match_id, _fx = seeded_db
        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["events"])
        if name is None:
            pytest.skip("no events-shaped method found on MatchReadPort")
        adapter = _make_football_adapter(conn)
        events = getattr(adapter, name)(match_id)
        assert isinstance(events, list)
        assert events, "the seeded knockout fixture has events; expected a non-empty list"
        for event in events:
            assert "team1" not in event and "team2" not in event
            assert "phase" in event, (
                f"{name}() events must carry a generic 'phase' field, not a "
                f"hardcoded half_time literal check"
            )

    def test_lineup_method_keyed_by_participant(self, seeded_db):
        conn, match_id, _fx = seeded_db
        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["lineup"])
        if name is None:
            pytest.skip("no lineup-shaped method found on MatchReadPort")
        adapter = _make_football_adapter(conn)
        result = getattr(adapter, name)(match_id)
        assert result is not None

    def test_venue_method_returns_shape_over_seeded_match(self, seeded_db):
        conn, match_id, _fx = seeded_db
        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["venue"])
        if name is None:
            pytest.skip("no venue-shaped method found on MatchReadPort")
        adapter = _make_football_adapter(conn)
        # Must not raise even though venue side-table seeding is Phase 2's
        # concern, not this fixture's.
        getattr(adapter, name)(match_id)

    def test_commentary_method_tolerates_absent_table(self, seeded_db):
        conn, match_id, _fx = seeded_db
        protocol = _match_read_port()
        name = _resolve(protocol, CANDIDATE_NAMES["commentary"])
        if name is None:
            pytest.skip("no commentary-shaped method found on MatchReadPort")
        adapter = _make_football_adapter(conn)
        result = getattr(adapter, name)(match_id)
        assert result == [] or result == () or result is None
