"""Phase 4: ``gamecollect.replay.ReplayProvider`` + the replay-driven engine path.

Plan contract (docs/dev_plans/20260702-feature-collector-interfaces.md §Phase 4):

- ``ReplayProvider(fixture, speed=1.0)`` implements the provider ABC; wall-clock
  pacing scaled by ``speed``; ``speed=inf`` / ``step()`` mode for deterministic
  tests; terminal state = FINISHED. In step mode the replay path supplies
  ``updated_at`` deterministically (the writer auto-stamps wall clock otherwise).
- Replay is invisible to the engine: the engine polls a ReplayProvider exactly
  as it polls a live one, so these tests drive the REAL ``CollectorEngine`` with
  a ReplayProvider injected and assert the resulting DB via the client library.
- Determinism: the same fixture replayed twice into two temp DBs yields identical
  canonical ordered row dumps of ``matches``/``events``/``entities``/``standings``
  (``updated_at`` excluded — the football seed path does not supply it, so the
  writer stamps wall clock; the plan sanctions excluding it), NOT raw ``.db``
  bytes.
- End-to-end: replay the canada-qatar seed fixture through the engine and assert
  the ground truth (6 events, 6-0, Jonathan David x3 goals) via the client.
- Zero-event (scheduled) fixture synthesized via ``write_fixture`` replays and
  terminates cleanly.
- Record -> replay round trip: a fixture replayed through an engine running
  ``--record`` produces a recording that ``read_fixture`` accepts and that equals
  the original match + events.

Parallel-implementation note (mirrors ``tests/_phase2_helpers.py`` /
``tests/test_engine.py``): the implementer is building ``src/gamecollect/replay.py``
in parallel with these tests. The plan pins ``ReplayProvider(fixture, speed=...)``,
``step()`` mode and the "FINISHED = terminal" semantics, but NOT whether the
constructor takes a :class:`~gamecollect.fixture_io.Fixture` or a path, nor
whether a poll auto-advances the replay clock or requires an explicit
``step()``. Both are resolved TOLERANTLY in one place below, so a minor surface
difference is a one-line fix here rather than a rewrite of every test. Until the
module lands these tests error at call — a timing artifact, not a test bug.

The engine is driven via its single-poll method (resolved among candidates)
rather than its threaded ``run()`` loop, so no real clock, signals or sleeps are
involved: at ``speed=inf`` every event is immediately available and a poll writes
it, and a step-based provider is advanced once per tick.
"""

from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path

import pytest

from gamecollect import client
from gamecollect.db.reader import open_reader
from gamecollect.fixture_io import read_fixture, write_fixture
from gamecollect.fold import fold
from gamecollect.packs.registry import load_pack
from gamecollect.provider import MatchDataProvider, MatchStatus, NormalizedEvent, NormalizedMatch

PACK_NAME = "football-wc2026"


# --------------------------------------------------------------------------- #
# Domain-object factories (taxonomy-valid types so the football writer accepts)
# --------------------------------------------------------------------------- #


def _synth_match(
    match_id: str,
    *,
    n_goals: int = 3,
    david_idx: tuple[int, ...] = (0,),
    status: MatchStatus = MatchStatus.FINISHED,
    home: str = "Canada",
    away: str = "Qatar",
) -> NormalizedMatch:
    """A FINISHED football match with ``n_goals`` 'goal' events, some by David.

    Identity fields (home/away/kickoff) are present so the football pack's
    reconcile-aware ``seed_match`` seeds a strip row rather than returning None.
    Event ``type`` is a real taxonomy key so the writer does not reject it.
    """
    events = [
        NormalizedEvent(
            seq=i,
            minute=10 + i * 5,
            event_type="goal",
            importance=1,
            team=home,
            player="Jonathan David" if i in david_idx else f"Player {i}",
            assist=None,
            detail=f"Goal {i}",
        )
        for i in range(n_goals)
    ]
    return NormalizedMatch(
        match_id=match_id,
        status=status,
        minute=90 if n_goals else None,
        score_home=n_goals,
        score_away=0,
        display_clock="FT" if status is MatchStatus.FINISHED else None,
        events=events,
        home_team=home,
        away_team=away,
        kickoff_utc="2026-06-18T18:00:00Z",
    )


# --------------------------------------------------------------------------- #
# Tolerant ReplayProvider construction + engine driving
# --------------------------------------------------------------------------- #


def _make_replay(fixture, path):
    """Construct ``ReplayProvider`` tolerantly (see module docstring).

    Prefers the plan-pinned ``ReplayProvider(fixture, speed=inf)``; falls back to
    a ``step=True`` flavour, a bare construction, and finally a path argument in
    case the constructor reads the file itself.
    """
    from gamecollect.replay import ReplayProvider

    attempts = [
        ((fixture,), {"speed": math.inf}),
        ((fixture,), {"step": True}),
        ((fixture,), {}),
        ((str(path),), {"speed": math.inf}),
        ((str(path),), {}),
    ]
    last: Exception | None = None
    for args, kwargs in attempts:
        try:
            return ReplayProvider(*args, **kwargs)
        except TypeError as exc:
            last = exc
            continue
    raise AssertionError(
        f"could not construct ReplayProvider from a Fixture or path; last error: {last}. "
        f"Update the candidates in tests/test_replay.py::_make_replay."
    )


_POLL_CANDIDATES = ("poll_once", "poll", "tick")


def _resolve_poll(engine):
    for name in _POLL_CANDIDATES:
        fn = getattr(engine, name, None)
        if callable(fn):
            return fn
    raise AssertionError(
        f"CollectorEngine exposes no single-poll method among {_POLL_CANDIDATES}; "
        f"attrs: {sorted(a for a in dir(engine) if not a.startswith('__'))}"
    )


def _drive(engine, provider, expected_events: int, *, margin: int = 6) -> None:
    """Advance the replay to completion, one poll (and one step, if any) per tick.

    At ``speed=inf`` the first poll already sees every event; a step-based
    provider reveals one per tick. Either way ``expected_events + margin`` ticks
    exhaust the fixture, and re-polling an already-written snapshot is a diff
    no-op (idempotent), so an over-count of ticks is harmless.
    """
    poll = _resolve_poll(engine)
    step = getattr(provider, "step", None)
    for _ in range(expected_events + margin):
        if callable(step):
            try:
                step()
            except StopIteration:
                pass
        try:
            poll()
        except StopIteration:
            break


def _run_engine_replay(
    fixture, path, db_path, source: str, *, record_path=None, expected: int | None = None
):
    """Replay ``fixture`` through the real engine (football pack) into ``db_path``.

    Returns nothing; the DB is populated (and, when ``record_path`` is set, a
    fixture recording is flushed on ``close``). ``expected`` overrides the
    event-count used to size the drive loop (needed for a step-based provider).
    """
    from gamecollect.engine import CollectorEngine

    pack = load_pack(PACK_NAME)
    provider = _make_replay(fixture, path)
    assert isinstance(provider, MatchDataProvider), (
        "ReplayProvider must implement the provider ABC so replay is invisible to the engine"
    )
    engine = CollectorEngine(
        pack,
        str(db_path),
        source,
        0.001,
        provider=provider,
        record_path=str(record_path) if record_path is not None else None,
    )
    exp = expected if expected is not None else len(fixture.match.events)
    _drive(engine, provider, exp)
    engine.close()
    return provider


# --------------------------------------------------------------------------- #
# Canonical row dump (logical DB content, never raw .db bytes)
# --------------------------------------------------------------------------- #

_DUMP_TABLES = ("matches", "events", "entities", "standings")


def _canonical_dump(db_path) -> dict[str, list[dict]]:
    """Ordered row dump of the core tables, ``updated_at`` excluded.

    Rows are ordered by every retained column so insert order never registers as
    a difference; ``updated_at`` (wall-clock-stamped by the writer when the seed
    path omits it) is dropped, per the plan's determinism contract.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        out: dict[str, list[dict]] = {}
        for table in _DUMP_TABLES:
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            keep = [c for c in cols if c != "updated_at"]
            order = ", ".join(f'"{c}"' for c in keep)
            rows = conn.execute(f'SELECT {order} FROM "{table}" ORDER BY {order}').fetchall()
            out[table] = [dict(zip(keep, row, strict=True)) for row in rows]
    finally:
        conn.close()
    return out


def _seeded_match_id(conn: sqlite3.Connection, source: str) -> str:
    matches = client.list_matches(conn, source=source)
    assert matches, f"replay seeded no match under source {source!r}"
    return matches[0].match_id


def _event_player(event) -> str | None:
    """The scorer name the engine parked in the event payload (or actor ref).

    Goal-family events (``goal``, ``own_goal``) land under ``payload.scorer``
    since Phase 1 of docs/dev_plans/20260710-feature-goal-event-participants.md;
    every other event type still uses ``payload.player``.
    """
    payload = getattr(event, "payload", None)
    if isinstance(payload, dict) and payload.get("scorer"):
        return payload["scorer"]
    if isinstance(payload, dict) and payload.get("player"):
        return payload["player"]
    return getattr(event, "actor_entity", None)


def _is_david_goal(event) -> bool:
    if getattr(event, "type", None) != "goal":
        return False
    player = _event_player(event)
    return bool(player) and "david" in fold(player)


# --------------------------------------------------------------------------- #
# Seed-fixture discovery (importer output; may be absent mid-parallel)
# --------------------------------------------------------------------------- #


def _seed_fixtures_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures" / "football"


def _find_seed_fixture(*substrings: str) -> Path | None:
    directory = _seed_fixtures_dir()
    if not directory.is_dir():
        return None
    for path in sorted(directory.glob("*.json")):
        low = path.name.lower()
        if all(sub in low for sub in substrings):
            return path
    return None


# --------------------------------------------------------------------------- #
# Provider contract
# --------------------------------------------------------------------------- #


def test_replay_provider_implements_provider_abc(tmp_path):
    fx = tmp_path / "provider.json"
    write_fixture(str(fx), _synth_match("prov-1", n_goals=2))
    provider = _make_replay(read_fixture(str(fx)), fx)
    assert isinstance(provider, MatchDataProvider), (
        "ReplayProvider must subclass/register as MatchDataProvider so the engine "
        "cannot tell replay from live"
    )
    # The ABC requires these two methods; a partial implementation would have
    # raised TypeError at construction, but assert the surface explicitly.
    assert callable(getattr(provider, "fetch_live_matches", None))
    assert callable(getattr(provider, "fetch_match_detail", None))


# --------------------------------------------------------------------------- #
# Determinism: same fixture twice -> identical logical DB content
# --------------------------------------------------------------------------- #


def test_replay_is_deterministic_across_two_runs(tmp_path):
    fx = tmp_path / "determinism.json"
    write_fixture(str(fx), _synth_match("det-1", n_goals=4, david_idx=(0, 2)))

    db1 = tmp_path / "run1.db"
    db2 = tmp_path / "run2.db"
    # A fresh Fixture per run (the provider may consume/advance its copy); the
    # SAME source both runs, so the seed writes identical source-qualified ids.
    _run_engine_replay(read_fixture(str(fx)), fx, db1, "detsrc")
    _run_engine_replay(read_fixture(str(fx)), fx, db2, "detsrc")

    dump1 = _canonical_dump(db1)
    dump2 = _canonical_dump(db2)
    # The replay actually wrote content (guards against two trivially-equal empty
    # DBs passing the determinism assertion).
    assert len(dump1["events"]) == 4, f"replay did not write all events: {dump1['events']}"
    assert dump1 == dump2, "two replays of the same fixture diverged in logical DB content"


def test_replay_determinism_compares_logical_rows_not_file_bytes(tmp_path):
    """Two runs are byte-different at the .db level (SQLite page/WAL churn) yet
    logically identical — the reason the contract compares row dumps, not bytes."""
    fx = tmp_path / "bytes.json"
    write_fixture(str(fx), _synth_match("bytes-1", n_goals=3))
    db1 = tmp_path / "b1.db"
    db2 = tmp_path / "b2.db"
    _run_engine_replay(read_fixture(str(fx)), fx, db1, "bytesrc")
    _run_engine_replay(read_fixture(str(fx)), fx, db2, "bytesrc")
    assert _canonical_dump(db1) == _canonical_dump(db2)


# --------------------------------------------------------------------------- #
# End-to-end: canada-qatar ground truth via the client library
# --------------------------------------------------------------------------- #


def test_end_to_end_canada_qatar_ground_truth(tmp_path):
    fx_path = _find_seed_fixture("canada", "qatar")
    if fx_path is None:
        pytest.skip("canada-qatar seed fixture not imported yet (mid-parallel build)")

    fixture = read_fixture(str(fx_path))
    db = tmp_path / "e2e.db"
    _run_engine_replay(fixture, fx_path, db, "wc2026")

    conn = open_reader(db)
    try:
        match_id = _seeded_match_id(conn, "wc2026")

        state = client.get_state(conn, match_id)
        assert state is not None, "canada-qatar match not found after replay"
        assert state.status == MatchStatus.FINISHED.value, f"expected FINISHED, got {state.status}"
        assert state.score_home == 6 and state.score_away == 0, (
            f"expected 6-0, got {state.score_home}-{state.score_away}"
        )

        events = client.get_events_since(conn, match_id, -1)
        assert len(events) == 6, f"expected 6 events, got {len(events)}"

        david_goals = [e for e in events if _is_david_goal(e)]
        assert len(david_goals) == 3, (
            f"expected a Jonathan David hat-trick (3 goals); got {len(david_goals)} "
            f"from players {[_event_player(e) for e in events]}"
        )
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Zero-event (scheduled) fixture -> replay terminates cleanly
# --------------------------------------------------------------------------- #


def test_zero_event_scheduled_fixture_replays_and_terminates_cleanly(tmp_path):
    fx = tmp_path / "scheduled.json"
    scheduled = _synth_match("upcoming", n_goals=0, status=MatchStatus.SCHEDULED)
    write_fixture(str(fx), scheduled)
    db = tmp_path / "scheduled.db"

    # The drive loop returning (no hang, no exception) IS the "terminates
    # cleanly" assertion; a bounded margin caps a provider that never signals a
    # terminal state for a match that is never live.
    _run_engine_replay(read_fixture(str(fx)), fx, db, "schedsrc", expected=0)

    # No events were fabricated for a match that never went live / had none.
    conn = open_reader(db)
    try:
        for match in client.list_matches(conn):
            assert client.get_events_since(conn, match.match_id, -1) == [], (
                "a zero-event scheduled fixture must not produce events"
            )
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Record -> replay round trip equals the original fixture
# --------------------------------------------------------------------------- #


def _projection(fixture) -> dict:
    """Comparable projection of a fixture's match header + ordered events.

    Excludes the optional entities/standings snapshots: a live ``--record``
    session (and the engine's recorder) only sees ``NormalizedMatch`` state, so
    it records those empty regardless of what the original carried — the round
    trip is defined over the facts the engine actually observes.
    """
    match = fixture.match
    events = sorted(match.events, key=lambda e: e.seq)
    return {
        "match_id": match.match_id,
        "status": match.status.value,
        "minute": match.minute,
        "score_home": match.score_home,
        "score_away": match.score_away,
        "events": [
            (e.seq, e.event_type, e.minute, e.importance, e.team, e.player, e.assist, e.detail)
            for e in events
        ],
    }


def test_record_then_replay_round_trip_equals_original(tmp_path):
    # Original fixture: pure match + events, no entities/standings, so the round
    # trip is a strong equality over exactly what the engine records.
    original = _synth_match("rt-match", n_goals=4, david_idx=(1, 3))
    orig_path = tmp_path / "original.json"
    write_fixture(str(orig_path), original)
    original_fixture = read_fixture(str(orig_path))

    # Replay the original through an engine running --record; the recorder
    # accumulates the provider snapshots and flushes a fixture on close.
    db = tmp_path / "roundtrip.db"
    record_path = tmp_path / "recorded.json"
    _run_engine_replay(original_fixture, orig_path, db, "rtsrc", record_path=record_path)

    assert record_path.is_file(), (
        f"--record did not write a fixture at {record_path} (single match => single file)"
    )
    recorded_fixture = read_fixture(str(record_path))

    # The recording is a valid, versioned fixture read_fixture accepts (above),
    # and its match + ordered events equal the original — the record path is the
    # symmetric inverse of the replay path.
    assert _projection(recorded_fixture) == _projection(original_fixture), (
        "record->replay round trip diverged from the original fixture"
    )


def test_recorded_fixture_is_valid_json_and_versioned(tmp_path):
    original = _synth_match("rt-json", n_goals=2)
    orig_path = tmp_path / "orig.json"
    write_fixture(str(orig_path), original)

    db = tmp_path / "rt.db"
    record_path = tmp_path / "rec.json"
    _run_engine_replay(
        read_fixture(str(orig_path)), orig_path, db, "rtjson", record_path=record_path
    )

    assert record_path.is_file()
    data = json.loads(record_path.read_text())
    assert data.get("format_version"), "recorded fixture carries no format_version"
    assert data["match"]["match_id"] == "rt-json"


# --------------------------------------------------------------------------- #
# Paced mode: exhausted reflects fetched events, never the wall clock
# --------------------------------------------------------------------------- #


class _FakeClock:
    """A controllable monotonic clock: returns ``t`` and counts every read."""

    def __init__(self) -> None:
        self.t = 0.0
        self.calls = 0

    def __call__(self) -> float:
        self.calls += 1
        return self.t


def test_paced_reading_exhausted_before_poll_does_not_start_clock(tmp_path):
    """In paced mode, merely reading ``exhausted`` before the first poll must not
    lazily start the pacing clock (the origin instant is the FIRST fetch, not a
    property read) and must report False for a non-empty fixture."""
    from gamecollect.replay import ReplayProvider

    fx = tmp_path / "paced.json"
    write_fixture(str(fx), _synth_match("paced-1", n_goals=3))
    clock = _FakeClock()
    provider = ReplayProvider(read_fixture(str(fx)), speed=1.0, monotonic=clock)

    assert provider.exhausted is False
    assert clock.calls == 0, "reading exhausted must not consult the pacing clock"
    assert getattr(provider, "_start", None) is None, (
        "reading exhausted must not start (lazily set) the pacing origin"
    )


def test_paced_exhausted_tracks_reveals_not_wallclock(tmp_path):
    """``exhausted`` must reflect the events a fetch actually revealed, not the
    wall clock. If it read the clock it could report done the instant enough time
    elapsed — letting the documented fetch-then-check-exhausted loop stop before
    the tail events were ever revealed. Advance the clock past every event with no
    intervening fetch and prove exhausted stays False (and reads no clock), then
    that the consumer loop still receives every event."""
    from gamecollect.replay import ReplayProvider

    fx = tmp_path / "paced.json"
    # Events at minutes 10/15/20 -> game-time offsets 600/900/1200s.
    write_fixture(str(fx), _synth_match("paced-2", n_goals=3))
    clock = _FakeClock()
    provider = ReplayProvider(read_fixture(str(fx)), speed=1.0, monotonic=clock)

    # First fetch fixes the pacing origin at t=0 and reveals nothing yet.
    assert provider.fetch_live_matches()[0].events == []
    # Jump wall time past every event WITHOUT another fetch.
    clock.t = 5000.0
    calls_before = clock.calls
    assert provider.exhausted is False, "exhausted must reflect fetched events, not wall time"
    assert clock.calls == calls_before, "exhausted must not consult the pacing clock"

    # The exhausted-then-stop consumer pattern still receives all tail events.
    revealed: list = []
    for _ in range(5):
        revealed = provider.fetch_live_matches()[0].events
        if provider.exhausted:
            break
    assert len(revealed) == 3, "every tail event must be revealed before exhausted stops the loop"
