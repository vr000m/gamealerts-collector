"""Phase 1: ``gamecollect.engine.CollectorEngine`` — the collection daemon.

Plan contract (docs/dev_plans/20260702-feature-collector-interfaces.md §Phase 1):

- ``CollectorEngine(pack, db_path, source, poll_interval)`` opens the DB with
  ``connect(db_path, side_table_ddl=pack.side_table_ddl)`` and constructs
  ``PartitionWriter(conn, source, taxonomy=pack.taxonomy)``; it obtains its
  provider from ``pack.provider_factory()``.
- Poll loop: jittered interval (uniform ±20% of ``poll_interval``); exponential
  backoff on provider errors (base = poll interval, doubling, capped at 8× base,
  reset on first success); graceful shutdown; no prose, no IPC.
- Provider-error posture: ``ProviderUnavailableError`` → backoff + retry, loop
  survives; ``ShapeDriftError`` → loud ERROR log + backoff + retry, loop
  survives; both non-fatal.
- Seeding via the NEW SportPack hook ``pack.seed_match(conn, writer, match) ->
  str | None``: a ``str`` return names the seeded row and the engine appends
  child rows under it; ``None`` means identity fields were missing and the
  engine skips child writes for that match this poll (no ``UnseededMatchError``).
- Re-poll drift: a re-sent seq whose full fingerprint differs raises
  ``SequenceError`` at the writer; the poll loop treats it as per-match provider
  drift (log + skip/re-sync), other matches keep collecting, the daemon stays up.

Surface the plan pins the *constructor* and the *seed hook* but NOT the loop
entry point or the shutdown handle. Following the repo convention established in
``tests/_phase2_helpers.py`` (resolve a method among candidate names so minor
naming differences do not spuriously fail), the loop entry point and the stop
handle are resolved among candidates here. The engine is driven by a scripted
fake provider against a temp DB — no network, no real clock.

The loop is exercised in the *main thread* (so a SIGTERM/SIGINT handler the
engine may install does not raise "signal only works in main thread"); a
``setitimer`` watchdog aborts a run that fails to stop, and prior signal
handlers are restored afterwards. The scripted provider requests shutdown once
its script is exhausted, so ``run()`` returns on its own.
"""

from __future__ import annotations

import json
import logging
import signal
import sqlite3
import threading

import pytest

from gamecollect.db import reader
from gamecollect.packs.spec import EventTypeDecl, SportPack, default_seed_match
from gamecollect.provider import (
    MatchDataProvider,
    MatchStatus,
    NormalizedEvent,
    NormalizedMatch,
    ProviderUnavailableError,
    ShapeDriftError,
)

SOURCE = "test-src"

# Small taxonomy covering every event type these tests emit; passed to the
# writer via pack.taxonomy so writes are not rejected as undeclared types.
TAXONOMY: dict[str, EventTypeDecl] = {
    "goal": EventTypeDecl(display_name="Goal", importance_default=1),
    "yellow": EventTypeDecl(display_name="Yellow card", importance_default=3),
    "sub": EventTypeDecl(display_name="Substitution", importance_default=4),
    "red": EventTypeDecl(display_name="Red card", importance_default=2),
    "half_time": EventTypeDecl(display_name="Half time", importance_default=3),
}

# A trivial pack-owned side table, so the test can prove the engine wired
# ``connect(db_path, side_table_ddl=pack.side_table_ddl)``.
FAKE_SIDE_TABLE_DDL: tuple[str, ...] = (
    "CREATE TABLE IF NOT EXISTS fake_side "
    "(source TEXT, match_id TEXT, PRIMARY KEY (source, match_id));",
)

_seed_log = logging.getLogger("tests.engine.seed")


# --------------------------------------------------------------------------- #
# Domain-object factories
# --------------------------------------------------------------------------- #


def ev(
    seq: int,
    event_type: str = "goal",
    *,
    minute: int | None = 10,
    importance: int = 1,
    team: str | None = "Canada",
    player: str | None = "Jonathan David",
    assist: str | None = None,
    detail: str | None = "Goal",
) -> NormalizedEvent:
    return NormalizedEvent(
        seq=seq,
        minute=minute,
        event_type=event_type,
        importance=importance,
        team=team,
        player=player,
        assist=assist,
        detail=detail,
    )


def nm(
    match_id: str,
    events: tuple[NormalizedEvent, ...] = (),
    *,
    status: MatchStatus = MatchStatus.IN_PLAY,
    minute: int | None = 10,
    score_home: int | None = 1,
    score_away: int | None = 0,
    display_clock: str | None = "10'",
    home_team: str | None = "Canada",
    away_team: str | None = "Qatar",
    kickoff_utc: str | None = "2026-06-18T18:00:00Z",
) -> NormalizedMatch:
    return NormalizedMatch(
        match_id=match_id,
        status=status,
        minute=minute,
        score_home=score_home,
        score_away=score_away,
        display_clock=display_clock,
        events=list(events),
        home_team=home_team,
        away_team=away_team,
        kickoff_utc=kickoff_utc,
    )


def qualified(match_id: str, source: str = SOURCE) -> str:
    """The source-qualified id the seed hook writes (mirrors the football pack's
    ``register_unreconciled_match``: ``f"{writer.source}:{match_id}"``)."""
    return f"{source}:{match_id}"


# --------------------------------------------------------------------------- #
# Fake provider + fake pack
# --------------------------------------------------------------------------- #


class ScriptedProvider(MatchDataProvider):
    """Replays a fixed script of poll responses.

    ``script`` is a list where each item is either a ``list[NormalizedMatch]``
    (the snapshot returned by that poll) or an ``Exception`` instance (raised by
    that poll). Once the script is exhausted the provider returns ``[]`` and
    fires ``on_exhausted`` exactly once, so the driving loop can shut down after
    every scripted response has been consumed.
    """

    def __init__(self, script: list) -> None:
        self._script = list(script)
        self._i = 0
        self.calls = 0
        self.on_exhausted = None
        self._exhausted_fired = False
        self._by_id: dict[str, NormalizedMatch] = {}

    def fetch_live_matches(self) -> list[NormalizedMatch]:
        self.calls += 1
        if self._i < len(self._script):
            resp = self._script[self._i]
            self._i += 1
            if isinstance(resp, BaseException):
                raise resp
            self._by_id = {m.match_id: m for m in resp}
            return list(resp)
        if not self._exhausted_fired:
            self._exhausted_fired = True
            if self.on_exhausted is not None:
                self.on_exhausted()
        return []

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        # Engines that fetch detail per live match still see the events the
        # snapshot carried; engines that use the snapshot directly do too.
        return self._by_id.get(match_id) or nm(match_id, ())


class DetailProvider(MatchDataProvider):
    """Scoreboard snapshot plus separate per-match detail snapshots."""

    def __init__(
        self,
        live_matches: list[NormalizedMatch],
        details: dict[str, NormalizedMatch],
    ) -> None:
        self._live_matches = live_matches
        self._details = details
        self.detail_calls: list[str] = []

    def fetch_live_matches(self) -> list[NormalizedMatch]:
        return list(self._live_matches)

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        self.detail_calls.append(match_id)
        return self._details[match_id]


class ScriptedDetailProvider(MatchDataProvider):
    """Scripted scoreboard polls, each with its own per-match detail map.

    ``polls`` is a list of ``(live_matches, details)`` pairs consumed one per
    ``fetch_live_matches`` call; an exhausted script returns ``[]``. A detail
    value that is an exception instance is RAISED by ``fetch_match_detail``
    (per-match detail failure), otherwise returned as the detail snapshot.
    """

    def __init__(self, polls: list[tuple[list[NormalizedMatch], dict]]) -> None:
        self._polls = list(polls)
        self._i = 0
        self._details: dict = {}
        self.detail_calls: list[str] = []

    def fetch_live_matches(self) -> list[NormalizedMatch]:
        if self._i < len(self._polls):
            live, details = self._polls[self._i]
            self._i += 1
            self._details = details
            return list(live)
        return []

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        self.detail_calls.append(match_id)
        value = self._details[match_id]
        if isinstance(value, BaseException):
            raise value
        return value


def _seed_match(conn: sqlite3.Connection, writer, match: NormalizedMatch) -> str | None:
    """Fake ``SportPack.seed_match`` hook.

    Mirrors the football pack's ``register_unreconciled_match`` contract: a
    ``str`` return (the source-qualified id) always names a seeded row; ``None``
    means the identity fields were missing and the engine must skip child writes
    for this match this poll.
    """
    if not match.home_team or not match.away_team or not match.kickoff_utc:
        _seed_log.warning("match %s missing identity fields; skipping seed", match.match_id)
        return None
    qid = qualified(match.match_id, writer.source)
    writer.upsert_match(
        {
            "match_id": qid,
            "kickoff_utc": match.kickoff_utc,
            "status": match.status.value,
            "minute": match.minute,
            "score_home": match.score_home,
            "score_away": match.score_away,
        }
    )
    return qid


def make_pack(provider: ScriptedProvider, *, seed_match=_seed_match, taxonomy=None):
    """Build a SportPack wired to ``provider`` with the new ``seed_match`` hook.

    ``seed_match`` is passed as a constructor argument when the SportPack dataclass
    accepts it (the Phase 1 contract amendment) and set as an attribute otherwise,
    so the test does not assume how the implementer exposed the field.
    """
    base = dict(
        name="football-test",
        sport="football",
        provider_factory=lambda: provider,
        taxonomy=taxonomy if taxonomy is not None else TAXONOMY,
        prompt_fragments={"x": "y"},
        preference_schema={"type": "object"},
        display_metadata={"sport_display": "Football"},
        compaction_boundaries=["kickoff"],
        side_table_ddl=FAKE_SIDE_TABLE_DDL,
    )
    try:
        pack = SportPack(**base, seed_match=seed_match)
    except TypeError:
        pack = SportPack(**base)
        pack.seed_match = seed_match
    return pack


# --------------------------------------------------------------------------- #
# Engine construction + loop driving
# --------------------------------------------------------------------------- #


def _engine(pack, db_path, poll_interval: float = 0.01):
    from gamecollect.engine import CollectorEngine

    return CollectorEngine(pack, str(db_path), SOURCE, poll_interval)


_RUN_CANDIDATES = ("run", "start", "serve", "run_forever", "loop", "poll_loop", "main")
_STOP_METHOD_CANDIDATES = (
    "stop",
    "shutdown",
    "request_stop",
    "request_shutdown",
    "cancel",
    "terminate",
    "close",
)
_STOP_EVENT_CANDIDATES = (
    "_stop",
    "_stop_event",
    "_shutdown",
    "_shutdown_event",
    "stop_event",
    "_should_stop",
)


def _resolve_run(engine):
    for name in _RUN_CANDIDATES:
        fn = getattr(engine, name, None)
        if callable(fn):
            return fn
    raise AssertionError(
        f"CollectorEngine exposes no loop entry point among {_RUN_CANDIDATES}; "
        f"attrs: {sorted(a for a in dir(engine) if not a.startswith('__'))}"
    )


def _resolve_stop(engine):
    for name in _STOP_METHOD_CANDIDATES:
        fn = getattr(engine, name, None)
        if callable(fn):
            return fn
    for name in _STOP_EVENT_CANDIDATES:
        obj = getattr(engine, name, None)
        if isinstance(obj, threading.Event):
            return obj.set
    raise AssertionError(
        f"CollectorEngine exposes no stop()/shutdown() method or stop Event "
        f"among {_STOP_METHOD_CANDIDATES + _STOP_EVENT_CANDIDATES}"
    )


class _EngineTimeout(Exception):
    pass


def run_engine(engine, provider: ScriptedProvider, *, timeout: float = 5.0) -> None:
    """Run the engine loop in the main thread until the provider script is
    exhausted, with a ``setitimer`` watchdog so a loop that never stops fails the
    test instead of hanging."""
    run = _resolve_run(engine)
    provider.on_exhausted = _resolve_stop(engine)

    saved = {}
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM):
        try:
            saved[signum] = signal.getsignal(signum)
        except (ValueError, OSError):
            pass

    def _fire(signum, frame):
        raise _EngineTimeout

    signal.signal(signal.SIGALRM, _fire)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        run()
    except _EngineTimeout:
        raise AssertionError(
            f"engine loop did not return within {timeout}s of shutdown request"
        ) from None
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for signum, handler in saved.items():
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError):
                pass


def read_db(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def event_seqs(db_path, match_id: str) -> list[int]:
    conn = read_db(db_path)
    try:
        return [r["seq"] for r in reader.get_events_since(conn, match_id)]
    finally:
        conn.close()


def event_types(db_path, match_id: str) -> list[str]:
    conn = read_db(db_path)
    try:
        return [r["type"] for r in reader.get_events_since(conn, match_id)]
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Happy path + idempotency
# --------------------------------------------------------------------------- #


def test_single_poll_seeds_then_appends_events(tmp_path):
    db = tmp_path / "engine.db"
    match = nm("m1", (ev(0, "goal"), ev(1, "yellow", detail="Booking")))
    provider = ScriptedProvider([[match]])
    run_engine(_engine(make_pack(provider), db), provider)

    qid = qualified("m1")
    # Events landing at all proves the seed ran first: append under an unseeded
    # id raises UnseededMatchError, which would have wedged/logged an error.
    assert event_seqs(db, qid) == [0, 1]
    assert event_types(db, qid) == ["goal", "yellow"]

    state = reader.get_state(read_db(db), qid)
    assert state is not None
    assert state["source"] == SOURCE


def test_in_progress_scoreboard_snapshot_fetches_detail_events_before_write(tmp_path):
    db = tmp_path / "engine.db"
    scoreboard = nm("m1", (), status=MatchStatus.IN_PLAY, score_home=1, score_away=0)
    detail = nm(
        "m1",
        (
            ev(0, "goal", minute=12, player="Jonathan David", detail="David scores"),
            ev(1, "yellow", minute=30, player="Booked Player", detail="Booking"),
        ),
        status=MatchStatus.IN_PLAY,
        score_home=1,
        score_away=0,
    )
    provider = DetailProvider([scoreboard], {"m1": detail})
    from gamecollect.engine import CollectorEngine

    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
    finally:
        engine.close()

    assert provider.detail_calls == ["m1"]
    assert event_types(db, qualified("m1")) == ["goal", "yellow"]


def test_collect_with_football_pack_persists_squad_and_player_stats_side_tables(tmp_path):
    from gamecollect.db.reader import open_reader
    from gamecollect.engine import CollectorEngine
    from gamecollect_football.operations import get_player_stats, get_squad
    from gamecollect_football.pack import pack as football_pack

    db = tmp_path / "football.db"
    scoreboard = nm("760999", (), status=MatchStatus.IN_PLAY, score_home=1, score_away=0)
    detail = nm(
        "760999",
        (ev(0, "goal", minute=12, team="Canada", player="Jonathan David"),),
        status=MatchStatus.IN_PLAY,
        score_home=1,
        score_away=0,
    )
    detail.payload = {
        "stats": [
            {
                "team": "Canada",
                "possession": 62.5,
                "shots": 8,
                "shots_on_target": 4,
                "corners": 3,
                "fouls": 5,
                "yellow_cards": 1,
                "red_cards": 0,
                "offsides": 2,
            }
        ],
        "lineups": [
            {
                "team": "Canada",
                "home_away": "home",
                "formation": "4-3-3",
                "players": [
                    {
                        "athlete_id": "ath-20",
                        "display_name": "Jonathan David",
                        "jersey": "20",
                        "position": "F",
                        "starter": True,
                        "subbed_in": False,
                        "subbed_out": False,
                        "formation_place": 9,
                    }
                ],
            }
        ],
    }
    provider = DetailProvider([scoreboard], {"760999": detail})

    engine = CollectorEngine(football_pack(), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
    finally:
        engine.close()

    seeded_id = qualified("760999")
    conn = open_reader(db)
    try:
        squad = get_squad(conn, seeded_id)
        stats = get_player_stats(conn, seeded_id)
    finally:
        conn.close()

    assert [member.display_name for member in squad] == ["Jonathan David"]
    assert squad[0].name_folded == "jonathan david"
    assert [(row.team, row.shots, row.shots_on_target) for row in stats] == [("Canada", 8, 4)]


def test_engine_creates_pack_side_tables_via_connect(tmp_path):
    db = tmp_path / "engine.db"
    provider = ScriptedProvider([[nm("m1", (ev(0),))]])
    run_engine(_engine(make_pack(provider), db), provider)

    conn = read_db(db)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()
    assert "fake_side" in tables, "engine did not pass pack.side_table_ddl to connect()"


def test_idempotent_repoll_does_not_duplicate_events(tmp_path):
    db = tmp_path / "engine.db"
    match = nm("m1", (ev(0, "goal"), ev(1, "yellow")))
    # The identical snapshot is delivered on three consecutive polls.
    provider = ScriptedProvider([[match], [match], [match]])
    run_engine(_engine(make_pack(provider), db), provider)

    assert event_seqs(db, qualified("m1")) == [0, 1]


def test_repoll_appends_only_new_events(tmp_path):
    db = tmp_path / "engine.db"
    poll1 = nm("m1", (ev(0, "goal"), ev(1, "yellow")))
    poll2 = nm("m1", (ev(0, "goal"), ev(1, "yellow"), ev(2, "goal", minute=30)))
    provider = ScriptedProvider([[poll1], [poll2]])
    run_engine(_engine(make_pack(provider), db), provider)

    assert event_seqs(db, qualified("m1")) == [0, 1, 2]


# --------------------------------------------------------------------------- #
# Seeding via the pack hook
# --------------------------------------------------------------------------- #


def test_seed_returns_none_skips_child_writes_without_error(tmp_path, caplog):
    db = tmp_path / "engine.db"
    # No home/away/kickoff → seed_match returns None → engine must skip child
    # writes and NOT raise UnseededMatchError.
    match = nm(
        "m1",
        (ev(0, "goal"),),
        home_team=None,
        away_team=None,
        kickoff_utc=None,
    )
    provider = ScriptedProvider([[match]])
    with caplog.at_level(logging.DEBUG):
        run_engine(_engine(make_pack(provider), db), provider)

    # Neither the qualified nor the bare id has events, and nothing crashed.
    assert event_seqs(db, qualified("m1")) == []
    assert event_seqs(db, "m1") == []
    assert not any(
        "UnseededMatchError" in (r.getMessage() + (r.exc_text or "")) for r in caplog.records
    ), "engine leaked an UnseededMatchError instead of skipping the None-seeded match"


def test_seedable_match_seed_precedes_append(tmp_path):
    db = tmp_path / "engine.db"
    match = nm("m1", (ev(0, "goal"), ev(1, "goal", minute=20)))
    provider = ScriptedProvider([[match]])
    run_engine(_engine(make_pack(provider), db), provider)

    qid = qualified("m1")
    assert reader.get_state(read_db(db), qid) is not None
    assert event_seqs(db, qid) == [0, 1]


def test_none_seeded_match_does_not_block_other_matches(tmp_path):
    db = tmp_path / "engine.db"
    good = nm("good", (ev(0, "goal"),))
    bad = nm("bad", (ev(0, "goal"),), home_team=None, away_team=None, kickoff_utc=None)
    provider = ScriptedProvider([[bad, good]])
    run_engine(_engine(make_pack(provider), db), provider)

    assert event_seqs(db, qualified("good")) == [0]
    assert event_seqs(db, qualified("bad")) == []


def test_unseeded_poll_does_not_baseline_skipped_events(tmp_path):
    db = tmp_path / "engine.db"
    # Poll 1: identity missing → seed_match returns None, both events skipped.
    poll1 = nm(
        "m1",
        (ev(0, "goal"), ev(1, "yellow", detail="Booking")),
        home_team=None,
        away_team=None,
        kickoff_utc=None,
    )
    # Poll 2: identity now present and a third event has appeared. Because the
    # skipped poll must NOT have advanced the diff baseline, this poll re-diffs
    # the FULL list and every event — including the two previously skipped —
    # must land. (With the pre-fix baseline bug, seq 0/1 were baked in as
    # already-written and silently lost, leaving only seq 2.)
    poll2 = nm("m1", (ev(0, "goal"), ev(1, "yellow", detail="Booking"), ev(2, "goal", minute=30)))
    provider = ScriptedProvider([[poll1], [poll2]])
    run_engine(_engine(make_pack(provider), db), provider)

    assert event_seqs(db, qualified("m1")) == [0, 1, 2]


def test_side_table_hook_failure_survives_logs_and_retries_next_poll(tmp_path, caplog):
    """A pack persist_side_tables hook raising (e.g. sqlite3.InterfaceError on
    a malformed payload value) must NOT kill the daemon: the failure is logged
    at ERROR, the match's diff baseline is NOT advanced, and the next poll
    re-diffs the full match — idempotent core appends no-op while the
    side-table write gets a natural retry and heals."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    match = nm("m1", (ev(0, "goal"),))
    provider = ScriptedProvider([[match], [match]])
    pack = make_pack(provider)

    calls: list[str] = []

    def flaky_persist(conn, writer, m, seeded_id):
        calls.append(seeded_id)
        if len(calls) == 1:
            raise sqlite3.InterfaceError("Error binding parameter 4: type 'dict'")
        with conn:  # commit like the real pack hooks do
            conn.execute(
                "INSERT OR REPLACE INTO fake_side (source, match_id) VALUES (?, ?)",
                (writer.source, seeded_id),
            )

    pack.persist_side_tables = flaky_persist

    engine = CollectorEngine(pack, str(db), SOURCE, 0.01, provider=provider)
    try:
        with caplog.at_level(logging.DEBUG):
            engine.poll_once()  # hook raises — daemon must survive
            assert engine._last == {}, (
                "a failed side-table write must not advance the diff baseline"
            )
            engine.poll_once()  # identical snapshot: re-diffs, retries, heals
    finally:
        engine.close()

    assert calls == [qualified("m1"), qualified("m1")], (
        "the hook must be retried on the next poll after a failure"
    )
    assert any(
        r.levelno == logging.ERROR and "persist_side_tables failed" in r.getMessage()
        for r in caplog.records
    )
    # Core events landed on poll 1 and were not duplicated by the retry.
    assert event_seqs(db, qualified("m1")) == [0]
    # The retry healed the side table.
    conn = read_db(db)
    try:
        rows = conn.execute("SELECT source, match_id FROM fake_side").fetchall()
    finally:
        conn.close()
    assert [tuple(r) for r in rows] == [(SOURCE, qualified("m1"))]


def test_malformed_nested_payload_value_does_not_crash_football_hook(tmp_path):
    """The ESPN shape drift that motivated the fix: a stats value arriving as a
    nested dict ({"possession": {"pct": 55}}) must be coerced/skipped by the
    football hook, not raise sqlite3.InterfaceError out of the poll."""
    from gamecollect.engine import CollectorEngine
    from gamecollect_football.pack import pack as football_pack

    db = tmp_path / "football.db"
    scoreboard = nm("760421", (), status=MatchStatus.IN_PLAY)
    detail = nm("760421", (ev(0, "goal", team="Morocco"),), status=MatchStatus.IN_PLAY)
    detail.payload = {
        "stats": [
            {"team": "Morocco", "possession": {"pct": 55}, "shots": "7", "corners": [3]},
            {"team": {"name": "Haiti"}},  # non-scalar team key: row skipped
        ],
        "lineups": [
            {
                "team": "Morocco",
                "home_away": {"side": "home"},  # non-scalar → None
                "players": [
                    {"athlete_id": "a1", "display_name": "Achraf Hakimi", "jersey": {"n": 2}},
                    {"athlete_id": {"id": 9}, "display_name": "Skipped"},  # skipped
                ],
            }
        ],
    }
    provider = DetailProvider([scoreboard], {"760421": detail})

    engine = CollectorEngine(football_pack(), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()  # must not raise
    finally:
        engine.close()

    conn = read_db(db)
    try:
        stats = conn.execute(
            "SELECT team, possession, shots, corners FROM football_stats ORDER BY team"
        ).fetchall()
        lineup = conn.execute(
            "SELECT display_name, jersey, home_away FROM football_lineups"
        ).fetchall()
    finally:
        conn.close()
    assert [tuple(r) for r in stats] == [("Morocco", None, 7, None)]
    assert [tuple(r) for r in lineup] == [("Achraf Hakimi", None, None)]
    assert event_seqs(db, qualified("760421")) == [0]


def test_late_canonical_schedule_row_adopts_stub_events(tmp_path):
    """Review finding D, end-to-end through the engine: events collected on a
    source-qualified stub while the schedule row was absent must migrate onto
    the canonical id once it appears — new events keep appending after the
    migrated head, the stub matches row is gone, nothing is stranded."""
    from gamecollect.db.writer import PartitionWriter
    from gamecollect.engine import CollectorEngine
    from gamecollect_football.pack import pack as football_pack

    db = tmp_path / "football.db"
    poll1 = nm("760421", (ev(0, "goal"),), minute=10)
    poll2 = nm("760421", (ev(0, "goal"), ev(1, "yellow", detail="Booking")), minute=30)
    provider = ScriptedProvider([[poll1], [poll2]])

    engine = CollectorEngine(football_pack(), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()  # schedule row absent → stub collects
        assert event_seqs(db, qualified("760421")) == [0]

        # The canonical schedule row lands between polls.
        PartitionWriter(engine._conn, SOURCE).upsert_match(
            {
                "match_id": "wc2026_md05_m01",
                "status": "SCHEDULED",
                "kickoff_utc": "2026-06-18T18:00:00Z",
                "payload": {"home_team": "Canada", "away_team": "Qatar"},
            }
        )
        engine.poll_once()  # adopts the stub, appends the new event
    finally:
        engine.close()

    assert event_seqs(db, "wc2026_md05_m01") == [0, 1], (
        "stub history plus the new event must all live under the canonical id"
    )
    conn = read_db(db)
    try:
        stub = reader.get_state(conn, qualified("760421"))
        (stranded,) = conn.execute(
            "SELECT COUNT(*) FROM events WHERE match_id = ?", (qualified("760421"),)
        ).fetchone()
    finally:
        conn.close()
    assert stub is None, "the orphaned stub matches row must be deleted"
    assert stranded == 0


def test_writer_taxonomy_error_skips_match_but_others_keep_collecting(tmp_path, caplog):
    db = tmp_path / "engine.db"
    # "bad" carries an undeclared event type → the writer raises TaxonomyError
    # from append_events. That must be isolated per-match (loud ERROR + skip),
    # not crash the daemon; "good" keeps collecting in the same poll.
    bad = nm("bad", (ev(0, "offside"),))
    good = nm("good", (ev(0, "goal"),))
    provider = ScriptedProvider([[bad, good]])
    with caplog.at_level(logging.DEBUG):
        run_engine(_engine(make_pack(provider), db), provider)

    assert event_seqs(db, qualified("good")) == [0]
    assert event_seqs(db, qualified("bad")) == []
    assert any(r.levelno >= logging.ERROR for r in caplog.records), (
        "a writer rejection must be logged loudly at ERROR level"
    )


# --------------------------------------------------------------------------- #
# Provider-error posture: loop must survive
# --------------------------------------------------------------------------- #


def test_provider_unavailable_is_non_fatal_loop_survives(tmp_path):
    db = tmp_path / "engine.db"
    match = nm("m1", (ev(0, "goal"),))
    # Poll 1 raises; the loop must back off and retry, then poll 2 succeeds.
    provider = ScriptedProvider([ProviderUnavailableError("upstream 503"), [match]])
    run_engine(_engine(make_pack(provider), db, poll_interval=0.01), provider)

    # The later successful poll landed → the loop survived the error.
    assert event_seqs(db, qualified("m1")) == [0]


def test_shape_drift_is_non_fatal_and_logged_loudly(tmp_path, caplog):
    db = tmp_path / "engine.db"
    match = nm("m1", (ev(0, "goal"),))
    provider = ScriptedProvider([ShapeDriftError("unexpected ESPN payload shape"), [match]])
    with caplog.at_level(logging.DEBUG):
        run_engine(_engine(make_pack(provider), db, poll_interval=0.01), provider)

    # Loop survived and the recovery poll landed.
    assert event_seqs(db, qualified("m1")) == [0]
    # Loud log: an ERROR-level record was emitted for the shape drift.
    assert any(r.levelno >= logging.ERROR for r in caplog.records), (
        "ShapeDriftError must be logged loudly at ERROR level"
    )


def test_provider_error_does_not_lose_already_collected_data(tmp_path):
    db = tmp_path / "engine.db"
    poll1 = nm("m1", (ev(0, "goal"),))
    poll3 = nm("m1", (ev(0, "goal"), ev(1, "yellow")))
    provider = ScriptedProvider([[poll1], ProviderUnavailableError("blip"), [poll3]])
    run_engine(_engine(make_pack(provider), db, poll_interval=0.01), provider)

    assert event_seqs(db, qualified("m1")) == [0, 1]


# --------------------------------------------------------------------------- #
# Sequence drift: per-match skip, other matches unaffected, daemon stays up
# --------------------------------------------------------------------------- #


def test_sequence_drift_skips_match_but_others_keep_collecting(tmp_path, caplog):
    db = tmp_path / "engine.db"
    # Poll 1: both matches seeded with one goal each.
    p1_a = nm("A", (ev(0, "goal", detail="Opener"),))
    p1_b = nm("B", (ev(0, "goal", detail="Opener"),))
    # Poll 2: match A's event list shifts positionally — a yellow is inserted at
    # the front, so seq 0's stored fingerprint (goal) no longer matches the
    # re-sent seq 0 (yellow). The writer raises SequenceError; the engine must
    # treat it as per-match drift. Match B meanwhile gains a clean new event.
    p2_a = nm(
        "A",
        (ev(0, "yellow", detail="Booking"), ev(1, "goal", detail="Opener"), ev(2, "sub")),
    )
    p2_b = nm("B", (ev(0, "goal", detail="Opener"), ev(1, "goal", minute=40, detail="Second")))
    # Poll 3: the provider re-sends the same SHIFTED snapshot for A. Because the
    # baseline is rebuilt from the stored rows (not the incoming snapshot), the
    # mismatch at seq 0 must re-diff and re-log — persistent shift corruption
    # stays loud instead of being silently baselined away.
    provider = ScriptedProvider([[p1_a, p1_b], [p2_a, p2_b], [p2_a, p2_b]])

    with caplog.at_level(logging.DEBUG):
        run_engine(_engine(make_pack(provider), db), provider)

    # Match B was unaffected and kept collecting.
    assert event_seqs(db, qualified("B")) == [0, 1]
    # Match A's original seq 0 is intact — drift was skipped, not silently
    # applied over the stored stream, and NO event was ever inserted at or
    # below the stored head (no duplicate seq-0 row, strictly monotonic tail).
    assert event_types(db, qualified("A"))[0] == "goal"
    assert event_seqs(db, qualified("A")) == sorted(set(event_seqs(db, qualified("A"))))
    # The drift was logged loudly (WARNING or higher) — and on EVERY poll the
    # shifted snapshot persisted, not just the first.
    drift_logs = [
        r
        for r in caplog.records
        if r.levelno >= logging.WARNING and "drift" in r.getMessage() and "seq 0" in r.getMessage()
    ]
    assert len(drift_logs) >= 2, "persistent shift drift must be re-logged on every poll"


def test_inplace_correction_skips_conflicted_seq_but_new_events_keep_landing(tmp_path, caplog):
    """An in-place provider correction (same seq, changed fingerprint — e.g. a
    fixed scorer name) must not wedge the match forever. Pre-fix, the batched
    ``append_events`` raised ``SequenceError`` before inserting anything, the
    baseline never advanced, and the identical failure repeated every poll —
    genuinely NEW later events were never written. Post-fix the engine
    reconciles against the stored rows: the conflicted seq is skipped loudly
    (stored row kept — append-only), the new seqs beyond the stored head land,
    and the baseline is rebuilt from the STORED rows, so the still-mutated seq
    keeps re-logging on later polls (loud-but-alive) while collection
    continues. A sibling match in the same poll is unaffected."""
    db = tmp_path / "engine.db"
    # Poll 1: match A has seqs 0-1; match B has seq 0.
    p1_a = nm("A", (ev(0, "goal", detail="Opener"), ev(1, "goal", minute=20, detail="Second")))
    p1_b = nm("B", (ev(0, "goal", detail="Opener"),))
    # Poll 2: A re-sends seq 1 with a CORRECTED scorer name (mutated
    # fingerprint) plus genuinely new seqs 2 and 3; B gains a clean new event.
    p2_a = nm(
        "A",
        (
            ev(0, "goal", detail="Opener"),
            ev(1, "goal", minute=20, player="Corrected Name", detail="Second"),
            ev(2, "yellow", minute=55, detail="Booking"),
            ev(3, "sub", minute=60, detail="Substitution"),
        ),
    )
    p2_b = nm("B", (ev(0, "goal", detail="Opener"), ev(1, "goal", minute=40, detail="Second")))
    # Poll 3: A gains one more event on top of the (still mutated) seq 1 — the
    # match must still be collecting, and the persistent seq-1 conflict must
    # re-log (the baseline is rebuilt from stored rows, never the raw snapshot).
    p3_a = nm("A", (*p2_a.events, ev(4, "red", minute=70, detail="Sent off")))
    provider = ScriptedProvider([[p1_a, p1_b], [p2_a, p2_b], [p3_a, p2_b]])

    with caplog.at_level(logging.DEBUG):
        run_engine(_engine(make_pack(provider), db), provider)

    qid_a = qualified("A")
    # New events (2, 3) landed despite the seq-1 conflict, and poll 3's seq 4
    # landed too — the match kept collecting instead of wedging.
    assert event_seqs(db, qid_a) == [0, 1, 2, 3, 4]
    # The stored seq 1 keeps its ORIGINAL fingerprint — append-only discipline;
    # the correction was skipped, not applied over the stored row.
    conn = read_db(db)
    try:
        players = {
            r["seq"]: json.loads(r["payload"])["player"]
            for r in conn.execute(
                "SELECT seq, payload FROM events WHERE match_id = ? ORDER BY seq", (qid_a,)
            )
        }
    finally:
        conn.close()
    assert players[1] == "Jonathan David", "stored event must NOT be mutated by the correction"
    # The skip was loud: an ERROR record naming the conflicted seq — on BOTH
    # polls where the mutated seq 1 persisted (baseline from stored rows means
    # unresolved drift keeps re-surfacing rather than being baselined away).
    drift_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "drift" in r.getMessage() and "seq 1" in r.getMessage()
    ]
    assert len(drift_errors) >= 2, (
        "the conflicted seq must be logged at ERROR level on every poll it persists"
    )
    # The sibling match kept collecting in the same polls.
    assert event_seqs(db, qualified("B")) == [0, 1]


def test_retroactive_insert_below_head_is_never_written_and_stays_loud(tmp_path, caplog):
    """A retroactively-inserted event (new seq BELOW the stored head, e.g. a
    VAR-restored goal) must never be written — inserting under the head would
    violate the writer's monotonic invariant and could interleave two
    timelines — but it must NOT be silently baselined away either: it is
    logged at ERROR on every poll it persists (loud-but-alive), while
    genuinely new events beyond the head keep landing."""
    db = tmp_path / "engine.db"
    # Poll 1: seqs 0, 1, 3 stored (head = 3; the gap at 2 is legal).
    p1 = nm("m1", (ev(0, "goal"), ev(1, "yellow", detail="Booking"), ev(3, "sub", minute=46)))
    # Poll 2: the provider retroactively inserts seq 2 below the head AND adds
    # a genuinely new seq 4 beyond it.
    p2 = nm(
        "m1",
        (
            ev(0, "goal"),
            ev(1, "yellow", detail="Booking"),
            ev(2, "goal", minute=25, detail="VAR-restored"),
            ev(3, "sub", minute=46),
            ev(4, "red", minute=70, detail="Sent off"),
        ),
    )
    # Poll 3: the same snapshot again — the retro insert persists.
    provider = ScriptedProvider([[p1], [p2], [p2]])
    with caplog.at_level(logging.DEBUG):
        run_engine(_engine(make_pack(provider), db), provider)

    # Seq 4 landed (no wedge); seq 2 was never written (no insert below head).
    assert event_seqs(db, qualified("m1")) == [0, 1, 3, 4]
    # The retro insert was logged at ERROR on BOTH polls it persisted — it was
    # not baked into the baseline and swallowed after the first poll.
    retro_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR
        and "seq 2" in r.getMessage()
        and "retroactive" in r.getMessage()
    ]
    assert len(retro_errors) >= 2, (
        "a retroactively-inserted seq must be re-logged at ERROR on every poll it persists"
    )


# --------------------------------------------------------------------------- #
# Detail hydration: live→final transition, per-match failure isolation, merge
# --------------------------------------------------------------------------- #


def test_live_to_finished_transition_hydrates_final_detail(tmp_path):
    """When a match transitions live→FINISHED between polls, the transition
    poll's scoreboard snapshot carries ``events=[]`` (the documented ESPN
    shape). The engine must still hydrate it — the previous baseline was live —
    or stoppage-time events are permanently lost. Once the FINAL baseline is
    stored, later final polls need no detail fetch."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=90, score_home=2)
    ft_detail = nm(
        "m1",
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,
        minute=90,
        score_home=2,
    )
    provider = ScriptedDetailProvider(
        [
            ([live_board], {"m1": live_detail}),
            ([ft_board], {"m1": ft_detail}),
            # Already-final poll: previous baseline is FINISHED → no hydration.
            ([ft_board], {"m1": ft_detail}),
        ]
    )
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
        engine.poll_once()
        engine.poll_once()
    finally:
        engine.close()

    # The stoppage-time event from the transition poll's detail landed.
    assert event_seqs(db, qualified("m1")) == [0, 1]
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value
    # Hydrated on the live poll and the transition poll; NOT on the
    # already-final third poll.
    assert provider.detail_calls == ["m1", "m1"]


@pytest.mark.parametrize(
    ("error", "min_level"),
    [
        (ProviderUnavailableError("summary endpoint 503"), logging.WARNING),
        (ShapeDriftError("unexpected summary payload shape"), logging.ERROR),
    ],
    ids=["unavailable", "shape-drift"],
)
def test_one_match_detail_failure_does_not_stop_siblings(tmp_path, caplog, error, min_level):
    """One match's failing ``fetch_match_detail`` must not abort the slate:
    the failed match falls back to its scoreboard snapshot (logged — WARNING
    for unavailability, ERROR for shape drift) and every other match keeps
    collecting in the same poll."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    # A's scoreboard snapshot carries one event, so the fallback write is
    # observable; B hydrates normally with two detail events.
    a_board = nm("A", (ev(0, "goal"),), status=MatchStatus.IN_PLAY)
    b_board = nm("B", (), status=MatchStatus.IN_PLAY)
    b_detail = nm("B", (ev(0, "goal"), ev(1, "yellow", detail="Booking")))
    provider = ScriptedDetailProvider([([a_board, b_board], {"A": error, "B": b_detail})])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            engine.poll_once()
        finally:
            engine.close()

    # The sibling was hydrated and written — the slate was not aborted.
    assert event_seqs(db, qualified("B")) == [0, 1]
    # The failed match fell back to its scoreboard snapshot instead of dying.
    assert event_seqs(db, qualified("A")) == [0]
    failure_logs = [r for r in caplog.records if r.levelno >= min_level and "A" in r.getMessage()]
    assert failure_logs, "a failed detail fetch must be logged at the documented level"


def test_detail_none_identity_fields_fall_back_to_scoreboard_but_state_is_not_backfilled(
    tmp_path,
):
    """A detail endpoint omitting IDENTITY fields (``None`` home/away/kickoff)
    must not wipe the scoreboard's values — ``seed_match`` would return
    ``None`` (events dropped) or stored kickoff would NULL out every poll.
    Scores backfill too: they are cumulative facts a detail ``None`` must
    never null (review finding: detail None scores wiped known scoreboard
    scores outside the lagging branch). CLOCK fields are the opposite: at
    HT/FT a detail may legitimately null ``minute``/``display_clock``, so the
    scoreboard must NOT backfill them — detail wins for those, including
    ``None`` — or stored state keeps showing a stale live clock."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    board = nm("m1", (), status=MatchStatus.IN_PLAY)  # full identity + state
    detail = nm(
        "m1",
        (ev(0, "goal"),),
        status=MatchStatus.IN_PLAY,
        minute=None,
        score_home=None,
        score_away=None,
        display_clock=None,
        home_team=None,
        away_team=None,
        kickoff_utc=None,
    )
    provider = ScriptedDetailProvider([([board], {"m1": detail})])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
    finally:
        engine.close()

    # Identity survived the merge → the match seeded and its events landed.
    assert event_seqs(db, qualified("m1")) == [0]
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None, "identity-less detail must not prevent seeding"
    assert state["kickoff_utc"] == "2026-06-18T18:00:00Z", (
        "detail None must not wipe scoreboard kickoff_utc"
    )
    # Clock fields take the detail's None (no stale-clock backfill)...
    assert state["minute"] is None, "scoreboard minute must not backfill a detail None"
    # ...but scores are cumulative facts: the scoreboard fills detail Nones.
    assert state["score_home"] == 1 and state["score_away"] == 0, (
        "a detail None score must not null the scoreboard's known score"
    )


def test_detail_none_scores_keep_scoreboard_scores_in_every_merge_branch(tmp_path):
    """Scores are cumulative facts, never legitimately cleared mid/post-match:
    a detail snapshot with ``None`` scores must not null the scoreboard's
    known scores on the equal-rank LIVE poll nor on the FINISHED transition
    poll (review finding: the restore only ran in the lagging-detail branch).
    Minute/display_clock stay detail-wins — legitimately nulled at HT/FT."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44, score_home=1, score_away=0)
    live_detail = nm(
        "m1",
        (ev(0, "goal", minute=12),),
        status=MatchStatus.IN_PLAY,  # equal rank: no lagging-branch restore
        minute=45,
        score_home=None,
        score_away=None,
    )
    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=None, score_home=2, score_away=0)
    ft_detail = nm(
        "m1",
        (ev(0, "goal", minute=12), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,  # equal rank on the transition poll too
        minute=None,
        score_home=None,
        score_away=None,
    )
    provider = ScriptedDetailProvider(
        [
            ([live_board], {"m1": live_detail}),
            ([ft_board], {"m1": ft_detail}),
        ]
    )
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
        state = reader.get_state(read_db(db), qualified("m1"))
        assert state["score_home"] == 1 and state["score_away"] == 0, (
            "equal-rank live detail None scores must not null the scoreboard's scores"
        )
        assert state["minute"] == 45, "minute stays detail-wins"
        engine.poll_once()
    finally:
        engine.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state["status"] == MatchStatus.FINISHED.value
    assert state["score_home"] == 2 and state["score_away"] == 0, (
        "FINISHED-transition detail None scores must not null the final scores"
    )
    assert state["minute"] is None, "detail None minute still wins at FT"
    assert event_seqs(db, qualified("m1")) == [0, 1]


@pytest.mark.parametrize("use_qualified_seed", [True, False], ids=["qualified-id", "bare-id"])
def test_restart_hydrates_final_when_stored_row_was_live(tmp_path, use_qualified_seed):
    """A daemon restart between the last live poll and the final poll empties
    the in-memory baseline, but the stored ``matches`` row still says live —
    the live→final transition is detectable from storage. The fresh engine
    must consult it and hydrate the transition poll's sparse final scoreboard
    snapshot, or stoppage-time events are permanently lost. Covered for both
    stored-id forms core knows: the source-qualified unreconciled id and the
    bare provider id (``default_seed_match``)."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    seed = _seed_match if use_qualified_seed else default_seed_match
    sid = qualified("m1") if use_qualified_seed else "m1"

    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    p1 = ScriptedDetailProvider([([live_board], {"m1": live_detail})])
    e1 = CollectorEngine(make_pack(p1, seed_match=seed), str(db), SOURCE, 0.01, provider=p1)
    try:
        e1.poll_once()
    finally:
        e1.close()
    state = reader.get_state(read_db(db), sid)
    assert state is not None and state["status"] == MatchStatus.IN_PLAY.value

    # Restart: a FRESH engine (empty self._last) sees the FT slate directly.
    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=90, score_home=2)
    ft_detail = nm(
        "m1",
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,
        minute=90,
        score_home=2,
    )
    p2 = ScriptedDetailProvider([([ft_board], {"m1": ft_detail})])
    e2 = CollectorEngine(make_pack(p2, seed_match=seed), str(db), SOURCE, 0.01, provider=p2)
    try:
        e2.poll_once()
    finally:
        e2.close()

    assert p2.detail_calls == ["m1"], "restart must not skip live→final detail hydration"
    # The stoppage-time event landed and the row closed out.
    assert event_seqs(db, sid) == [0, 1]
    state = reader.get_state(read_db(db), sid)
    assert state is not None and state["status"] == MatchStatus.FINISHED.value


def test_transient_detail_failure_on_transition_poll_is_retried_next_poll(tmp_path, caplog):
    """A detail fetch that fails once on the exact live→final poll must not
    baseline the sparse final scoreboard snapshot (``events=[]``) — that would
    make every later poll see previous=FINISHED and never hydrate again,
    silently losing the stoppage-time events. The engine skips the match that
    poll (live baseline kept, stored row untouched) and retries the final
    hydration on the next poll."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=90, score_home=2)
    ft_detail = nm(
        "m1",
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,
        minute=90,
        score_home=2,
    )
    provider = ScriptedDetailProvider(
        [
            ([live_board], {"m1": live_detail}),
            # The transition poll's detail fetch fails transiently.
            ([ft_board], {"m1": ProviderUnavailableError("summary 503 at the whistle")}),
            ([ft_board], {"m1": ft_detail}),
        ]
    )
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            engine.poll_once()
            engine.poll_once()
            # The failed transition poll must NOT have written the sparse
            # FINAL snapshot: the stored row still says live, so the
            # hydration window is still open.
            state = reader.get_state(read_db(db), qualified("m1"))
            assert state is not None and state["status"] == MatchStatus.IN_PLAY.value, (
                "a failed transition-detail poll must not baseline the sparse final snapshot"
            )
            engine.poll_once()
        finally:
            engine.close()

    # The retry hydrated: stoppage event landed, row closed out.
    assert provider.detail_calls == ["m1", "m1", "m1"]
    assert event_seqs(db, qualified("m1")) == [0, 1]
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value
    retry_logs = [
        r
        for r in caplog.records
        if "retried next poll" in r.getMessage() and "m1" in r.getMessage()
    ]
    assert retry_logs, "the transition skip-and-retry must be logged"


def test_transition_detail_failures_cap_then_accept_scoreboard_with_error(tmp_path, caplog):
    """A detail endpoint that permanently fails post-FT (e.g. a 404ing summary)
    must not wedge the match on the slate forever: after
    ``_MAX_TRANSITION_DETAIL_FAILURES`` consecutive transition-poll failures
    the engine gives up loudly (ERROR) and accepts the scoreboard snapshot, so
    the row still closes out as FINISHED."""
    from gamecollect.engine import _MAX_TRANSITION_DETAIL_FAILURES, CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=90, score_home=2)
    failing_polls = [
        ([ft_board], {"m1": ProviderUnavailableError(f"summary 404 #{i}")})
        for i in range(_MAX_TRANSITION_DETAIL_FAILURES)
    ]
    provider = ScriptedDetailProvider([([live_board], {"m1": live_detail}), *failing_polls])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            for _ in range(1 + _MAX_TRANSITION_DETAIL_FAILURES):
                engine.poll_once()
        finally:
            engine.close()

    # Capped out: the scoreboard snapshot was accepted — the row closed out
    # FINISHED with only the events collected while live.
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "after the retry cap the scoreboard FINAL must be accepted, not wedged"
    )
    assert event_seqs(db, qualified("m1")) == [0]
    give_up_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "giving up" in r.getMessage() and "m1" in r.getMessage()
    ]
    assert give_up_errors, "the capped give-up must be logged at ERROR"


def test_live_match_vanishing_from_slate_gets_terminal_detail_hydration(tmp_path):
    """A live match that simply vanishes from ``fetch_live_matches`` (never
    showing a final status on the slate) must still be closed out: the engine
    explicitly fetches its detail and processes it through the normal
    diff/apply path, persisting the final status and the final-only events."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    ft_detail = nm(
        "m1",
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,
        minute=None,
        score_home=2,
    )
    provider = ScriptedDetailProvider(
        [
            ([live_board], {"m1": live_detail}),
            # The match drops off the slate entirely; detail is still served.
            ([], {"m1": ft_detail}),
        ]
    )
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
        engine.poll_once()
    finally:
        engine.close()

    assert provider.detail_calls == ["m1", "m1"], "the vanished match must be detail-fetched"
    assert event_seqs(db, qualified("m1")) == [0, 1], "final-only events must land"
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value
    assert state["score_home"] == 2


def test_vanished_match_with_still_live_detail_stays_tracked_until_final(tmp_path, caplog):
    """A scoreboard may drop a match EARLY while its detail endpoint still says
    live: the match must stay on the tracked set and keep being re-fetched
    until a terminal detail arrives."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    still_live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=88)
    ft_detail = nm(
        "m1",
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,
        minute=None,
        score_home=2,
    )
    provider = ScriptedDetailProvider(
        [
            ([live_board], {"m1": live_detail}),
            ([], {"m1": still_live_detail}),  # dropped early, still live
            ([], {"m1": ft_detail}),
        ]
    )
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            engine.poll_once()
            engine.poll_once()
            engine.poll_once()
        finally:
            engine.close()

    assert provider.detail_calls == ["m1", "m1", "m1"], (
        "a still-live vanished match must stay tracked and be re-fetched"
    )
    assert event_seqs(db, qualified("m1")) == [0, 1]
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value


def test_vanished_match_detail_failures_cap_gives_up_loudly_persisting_best_known(tmp_path, caplog):
    """A vanished live match whose detail fetch fails the full retry cap must
    not be left IN_PLAY silently: the engine gives up with an ERROR and
    persists the best-known terminal state (the live baseline force-closed to
    FINISHED when no final snapshot was ever seen)."""
    from gamecollect.engine import _MAX_TRANSITION_DETAIL_FAILURES, CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    failing_polls = [
        ([], {"m1": ProviderUnavailableError(f"summary 404 #{i}")})
        for i in range(_MAX_TRANSITION_DETAIL_FAILURES)
    ]
    provider = ScriptedDetailProvider([([live_board], {"m1": live_detail}), *failing_polls])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            for _ in range(1 + _MAX_TRANSITION_DETAIL_FAILURES):
                engine.poll_once()
        finally:
            engine.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "at the failure cap the best-known terminal state must be persisted, "
        "not a row left IN_PLAY forever"
    )
    assert event_seqs(db, qualified("m1")) == [0], "events collected while live survive"
    give_up_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "giving up" in r.getMessage() and "m1" in r.getMessage()
    ]
    assert give_up_errors, "the vanished-match give-up must be logged at ERROR"


def test_mid_transition_retry_slate_drop_still_ends_finished(tmp_path, caplog):
    """The earlier confirmed variant: a live→final transition poll whose detail
    fails is skipped (live baseline kept, counter=1); if the match then drops
    off the slate before the retries land, the retry state must NOT be erased
    by slate cleanup — the vanished-match path keeps counting, and at the cap
    the sparse final scoreboard snapshot captured on the transition poll is
    persisted, so the row ends FINISHED instead of IN_PLAY forever."""
    from gamecollect.engine import _MAX_TRANSITION_DETAIL_FAILURES, CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=None, score_home=2)
    polls = [
        ([live_board], {"m1": live_detail}),
        # Transition poll: FT on the slate, detail fails → skip + counter=1.
        ([ft_board], {"m1": ProviderUnavailableError("summary 503 at the whistle")}),
    ]
    # The match then vanishes; detail keeps failing until the cap.
    polls += [
        ([], {"m1": ProviderUnavailableError(f"summary 404 #{i}")})
        for i in range(_MAX_TRANSITION_DETAIL_FAILURES - 1)
    ]
    provider = ScriptedDetailProvider(polls)
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            for _ in range(len(polls)):
                engine.poll_once()
        finally:
            engine.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "the sparse final snapshot captured mid-retry must be persisted at the cap"
    )
    assert state["score_home"] == 2, "the captured final scoreboard score must land"
    assert event_seqs(db, qualified("m1")) == [0]
    give_up_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "giving up" in r.getMessage() and "m1" in r.getMessage()
    ]
    assert give_up_errors, "the capped give-up must be logged at ERROR"


def test_lagging_detail_live_status_does_not_regress_scoreboard_final(tmp_path):
    """On the transition poll a cached/lagging detail endpoint may still say
    IN_PLAY while the scoreboard already says FINISHED. Status is forward-only
    in the merge: the detail's events are taken (the point of hydrating), but
    its stale live status/minute must not overwrite the scoreboard's FINAL —
    if the match then drops off the slate, the DB would stay in-progress
    forever."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    ft_board = nm(
        "m1", (), status=MatchStatus.FINISHED, minute=None, display_clock="FT", score_home=2
    )
    lagging_detail = nm(
        "m1",
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.IN_PLAY,  # cached: still live
        minute=88,
        display_clock="88'",
        score_home=1,
    )
    provider = ScriptedDetailProvider(
        [
            ([live_board], {"m1": live_detail}),
            ([ft_board], {"m1": lagging_detail}),
        ]
    )
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
        engine.poll_once()
    finally:
        engine.close()

    # Detail's events landed...
    assert event_seqs(db, qualified("m1")) == [0, 1]
    # ...but the scoreboard's FINAL and clock progression won the merge.
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None
    assert state["status"] == MatchStatus.FINISHED.value, (
        "a lagging detail's live status must never regress the scoreboard's FINAL"
    )
    assert state["minute"] is None, "the lagging detail's stale live minute must not persist"
    assert state["score_home"] == 2, "the scoreboard's final score wins over the lagging detail"


def test_restart_gap_sparse_terminal_detail_merges_over_fallback_base(tmp_path):
    """Round-5 finding 1: on a restart gap (stored row live, no in-memory
    baseline) whose transition-poll detail fails, the captured scoreboard
    fallback must serve as the MERGE BASE when a later (vanished) terminal
    detail arrives sparse — never popped before the base is computed. A raw
    sparse detail would seed identity-less (events dropped), fall out of
    tracking, and leave the row in-play forever."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    p1 = ScriptedDetailProvider([([live_board], {"m1": live_detail})])
    e1 = CollectorEngine(make_pack(p1), str(db), SOURCE, 0.01, provider=p1)
    try:
        e1.poll_once()
    finally:
        e1.close()

    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=None, score_home=2)
    sparse_ft_detail = nm(
        "m1",
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,
        minute=None,
        score_home=None,
        score_away=None,
        home_team=None,
        away_team=None,
        kickoff_utc=None,
    )
    p2 = ScriptedDetailProvider(
        [
            # Restart gap: fresh engine, FT board on the slate, detail fails →
            # tracker created (stored row is live), FT board kept as fallback.
            ([ft_board], {"m1": ProviderUnavailableError("summary 503 at the whistle")}),
            # The match then vanishes and the terminal detail arrives SPARSE.
            ([], {"m1": sparse_ft_detail}),
        ]
    )
    e2 = CollectorEngine(make_pack(p2), str(db), SOURCE, 0.01, provider=p2)
    try:
        e2.poll_once()
        e2.poll_once()
    finally:
        e2.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "a sparse terminal detail on a restart gap must merge over the fallback "
        "base and close the row out, not seed identity-less and stay in-play"
    )
    assert state["score_home"] == 2, "the fallback board's score must fill the detail's None"
    assert state["kickoff_utc"] == "2026-06-18T18:00:00Z", "identity must survive the merge"
    assert event_seqs(db, qualified("m1")) == [0, 1], "the stoppage-time event must land"


def test_provider_status_reset_keeps_fallback_and_terminates_at_attempts_cap(tmp_path, caplog):
    """Round-5 finding 2: a non-terminal non-live detail (SCHEDULED/UNKNOWN —
    a postponed/abandoned provider reset) must NOT discard the captured
    terminal fallback nor loop silently forever: the fallback is kept, the
    WARN fires once per state change (not every poll), and at the
    total-attempts cap the merged fallback is persisted so the row terminates."""
    from gamecollect.engine import _MAX_TRANSITION_TOTAL_ATTEMPTS, CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=None, score_home=2)
    reset_detail = nm(
        "m1",
        (),
        status=MatchStatus.SCHEDULED,
        minute=None,
        score_home=None,
        score_away=None,
        display_clock=None,
    )
    polls = [
        ([live_board], {"m1": live_detail}),
        # Transition poll: detail fails → FT board captured as fallback (ta=1).
        ([ft_board], {"m1": ProviderUnavailableError("summary 503 at the whistle")}),
    ]
    # The match vanishes; the detail endpoint keeps returning a status RESET.
    polls += [([], {"m1": reset_detail}) for _ in range(_MAX_TRANSITION_TOTAL_ATTEMPTS - 1)]
    provider = ScriptedDetailProvider(polls)
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            for _ in range(len(polls)):
                engine.poll_once()
        finally:
            engine.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "at the attempts cap the captured terminal fallback must be persisted — "
        "a status reset must not leave the row live-tracked forever"
    )
    assert state["score_home"] == 2, "the fallback's final score must not be discarded"
    reset_warns = [r for r in caplog.records if "status reset" in r.getMessage()]
    assert len(reset_warns) == 1, "the reset WARN must fire once per state change, not per poll"
    give_up_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "giving up" in r.getMessage() and "m1" in r.getMessage()
    ]
    assert give_up_errors, "the capped give-up must be logged at ERROR"


def test_give_up_merges_sparse_fallback_over_live_baseline_never_raw(tmp_path, caplog):
    """Round-5 finding 3: the give-up path must run the sparse terminal
    snapshot through the merge over the live baseline — persisting it RAW
    would NULL known scores, or (identity-less) fail to seed at all while the
    ERROR log claims persistence."""
    from gamecollect.engine import _MAX_TRANSITION_DETAIL_FAILURES, CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    sparse_ft_board = nm(
        "m1",
        (),
        status=MatchStatus.FINISHED,
        minute=None,
        score_home=None,
        score_away=None,
        display_clock="FT",
        home_team=None,
        away_team=None,
        kickoff_utc=None,
    )
    failing_polls = [
        ([sparse_ft_board], {"m1": ProviderUnavailableError(f"summary 404 #{i}")})
        for i in range(_MAX_TRANSITION_DETAIL_FAILURES)
    ]
    provider = ScriptedDetailProvider([([live_board], {"m1": live_detail}), *failing_polls])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            for _ in range(1 + _MAX_TRANSITION_DETAIL_FAILURES):
                engine.poll_once()
        finally:
            engine.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "an identity-less give-up snapshot must merge the baseline's identity "
        "and seed — not be dropped while the row stays in-play"
    )
    assert state["score_home"] == 1 and state["score_away"] == 0, (
        "the sparse board's None scores must not NULL the baseline's known scores"
    )
    assert state["kickoff_utc"] == "2026-06-18T18:00:00Z"
    give_up_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "giving up" in r.getMessage() and "m1" in r.getMessage()
    ]
    assert give_up_errors, "the give-up must be logged at ERROR after the write landed"


def test_give_up_snapshot_retried_when_apply_is_not_durable(tmp_path, caplog):
    """Round-5 finding 4: the tracker (and its captured fallback) must survive
    a give-up whose apply did not complete durably — the SAME best-known
    snapshot is re-emitted next poll, instead of the old force-close with a
    stale live score after the fallback was popped pre-durability."""
    from gamecollect.engine import _MAX_TRANSITION_DETAIL_FAILURES, CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=None, score_home=2)
    polls = [
        ([live_board], {"m1": live_detail}),
        # Transition poll: FT board captured as fallback (score 2), cf=1.
        ([ft_board], {"m1": ProviderUnavailableError("summary 503 at the whistle")}),
    ]
    # Vanishes; detail keeps failing up to the cap, then keeps failing after.
    polls += [
        ([], {"m1": ProviderUnavailableError(f"summary 404 #{i}")})
        for i in range(_MAX_TRANSITION_DETAIL_FAILURES + 2)
    ]
    provider = ScriptedDetailProvider(polls)
    pack = make_pack(provider)
    original_persist = pack.persist_side_tables
    outage = {"pending": True}

    def flaky_persist(conn, writer, match, seeded_id):
        if match.status is MatchStatus.FINISHED and outage["pending"]:
            outage["pending"] = False
            raise RuntimeError("transient side-table outage")
        return original_persist(conn, writer, match, seeded_id)

    pack.persist_side_tables = flaky_persist
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            for _ in range(len(polls)):
                engine.poll_once()
        finally:
            engine.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value
    assert state["score_home"] == 2, (
        "the retried give-up must re-persist the captured fallback score — not a "
        "stale live-baseline force-close after the fallback was lost"
    )
    # The give-up short-circuits further fetches: 1 live hydration + the
    # failures up to the cap; nothing after (old code restarted the counter
    # and kept fetching).
    assert provider.detail_calls == ["m1"] * (1 + _MAX_TRANSITION_DETAIL_FAILURES)
    give_up_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR
        and "persisted best-known" in r.getMessage()
        and "m1" in r.getMessage()
    ]
    assert len(give_up_errors) == 1, (
        "persistence must be claimed exactly once, only after the durable apply"
    )


def test_still_live_detail_not_force_closed_by_terminal_fallback_base(tmp_path, caplog):
    """Round-5 finding 5: a still-live detail merged over a TERMINAL fallback
    base (restart gap: the fallback is the only base) must NOT be force-closed
    by the base's FINISHED status — the WARN promises the match stays tracked,
    and only the detail itself (or the fallback at give-up) may terminate it."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    p1 = ScriptedDetailProvider([([live_board], {"m1": live_detail})])
    e1 = CollectorEngine(make_pack(p1), str(db), SOURCE, 0.01, provider=p1)
    try:
        e1.poll_once()
    finally:
        e1.close()

    ft_board = nm("m1", (), status=MatchStatus.FINISHED, minute=None, score_home=2)
    still_live_detail = nm(
        "m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=88, score_home=2
    )
    ft_detail = nm(
        "m1",
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,
        minute=None,
        score_home=2,
    )
    p2 = ScriptedDetailProvider(
        [
            # Restart gap: transition poll fails → terminal FT board becomes
            # the fallback (and the only merge base — self._last is empty).
            ([ft_board], {"m1": ProviderUnavailableError("summary 503 at the whistle")}),
            ([], {"m1": still_live_detail}),  # dropped early, still live
            ([], {"m1": ft_detail}),
        ]
    )
    e2 = CollectorEngine(make_pack(p2), str(db), SOURCE, 0.01, provider=p2)
    with caplog.at_level(logging.DEBUG):
        try:
            e2.poll_once()
            e2.poll_once()
            state = reader.get_state(read_db(db), qualified("m1"))
            assert state is not None and state["status"] == MatchStatus.IN_PLAY.value, (
                "a still-live detail must not be force-closed by its terminal fallback merge base"
            )
            assert state["minute"] == 88, "the live detail's clock must persist"
            e2.poll_once()
        finally:
            e2.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value
    assert event_seqs(db, qualified("m1")) == [0, 1]
    live_warns = [r for r in caplog.records if "still says live" in r.getMessage()]
    assert live_warns, "the still-live tracking WARN must fire"


def test_still_live_vanished_loop_capped_force_closes_with_error(tmp_path, caplog):
    """Round-5 finding 6: a vanished match whose detail NEVER turns terminal
    must not be re-fetched and WARNed about every poll forever — the
    total-attempts cap force-closes the best-known live state loudly, the WARN
    fires once (not per poll), and tracking stops after the durable close."""
    from gamecollect.engine import _MAX_TRANSITION_TOTAL_ATTEMPTS, CollectorEngine

    db = tmp_path / "engine.db"
    live_board = nm("m1", (), status=MatchStatus.IN_PLAY, minute=44)
    live_detail = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=44)
    polls: list[tuple[list, dict]] = [([live_board], {"m1": live_detail})]
    for i in range(_MAX_TRANSITION_TOTAL_ATTEMPTS):
        polls.append(
            (
                [],
                {
                    "m1": nm(
                        "m1",
                        (ev(0, "goal", minute=44),),
                        status=MatchStatus.IN_PLAY,
                        minute=80 + i,
                        score_home=3,
                    )
                },
            )
        )
    polls.append(([], {}))  # one poll past the cap: nothing may be fetched
    provider = ScriptedDetailProvider(polls)
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    with caplog.at_level(logging.DEBUG):
        try:
            for _ in range(len(polls)):
                engine.poll_once()
        finally:
            engine.close()

    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "the never-terminal loop must be capped and force-closed, not left live"
    )
    assert state["score_home"] == 3, "the freshest live detail's score is the best-known state"
    assert provider.detail_calls == ["m1"] * (1 + _MAX_TRANSITION_TOTAL_ATTEMPTS), (
        "after the capped close the match must not be re-fetched"
    )
    live_warns = [r for r in caplog.records if "still says live" in r.getMessage()]
    assert len(live_warns) == 1, "the still-live WARN must fire once per state change"
    give_up_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "giving up" in r.getMessage() and "m1" in r.getMessage()
    ]
    assert len(give_up_errors) == 1


def test_duplicate_seq_within_one_drift_snapshot_is_warned_not_retro_error(tmp_path, caplog):
    """A snapshot carrying the same beyond-head seq TWICE fails batched
    ``append_events`` (not strictly increasing) and reconciles per-row: the
    first copy lands, and the second must be classified as a
    duplicate-in-snapshot (WARN) — not as a 'retroactive insertion below the
    stored head' ERROR, which it structurally is not."""
    db = tmp_path / "engine.db"
    p1 = nm("m1", (ev(0, "goal"), ev(1, "yellow", detail="Booking")))
    new_event = ev(2, "goal", minute=50, detail="Second goal")
    p2 = nm("m1", (ev(0, "goal"), ev(1, "yellow", detail="Booking"), new_event, new_event))
    provider = ScriptedProvider([[p1], [p2]])
    with caplog.at_level(logging.DEBUG):
        run_engine(_engine(make_pack(provider), db), provider)

    # The new seq landed exactly once; nothing else was disturbed.
    assert event_seqs(db, qualified("m1")) == [0, 1, 2]
    dup_warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "duplicate" in r.getMessage()
        and "seq 2" in r.getMessage()
    ]
    assert dup_warnings, "an in-snapshot duplicate must be logged as a WARN duplicate"
    retro_errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR
        and "retroactive" in r.getMessage()
        and "seq 2" in r.getMessage()
    ]
    assert not retro_errors, (
        "an in-snapshot duplicate must NOT be misclassified as a retroactive insertion"
    )


# --------------------------------------------------------------------------- #
# Clean shutdown
# --------------------------------------------------------------------------- #


def test_clean_shutdown_returns_and_persists_collected_data(tmp_path):
    db = tmp_path / "engine.db"
    match = nm("m1", (ev(0, "goal"), ev(1, "yellow")))
    provider = ScriptedProvider([[match]])
    engine = _engine(make_pack(provider), db)
    # run_engine asserts the loop returns within the watchdog window (a hung
    # loop raises); reaching here means shutdown was graceful.
    run_engine(engine, provider)
    assert event_seqs(db, qualified("m1")) == [0, 1]


# --------------------------------------------------------------------------- #
# Jitter + backoff timing (observed via the sleep primitive when observable)
# --------------------------------------------------------------------------- #


def capture_delays(monkeypatch) -> list[float]:
    """Record every requested sleep/wait duration without actually sleeping.

    Patches both ``time.sleep`` and ``threading.Event.wait`` so the timing is
    captured regardless of which primitive the engine uses for its interval. The
    numeric tests skip when nothing is captured, rather than assume the seam.
    """
    import time as _time

    delays: list[float] = []

    def _sleep(secs):
        if secs is not None:
            delays.append(float(secs))

    monkeypatch.setattr(_time, "sleep", _sleep)

    def _wait(self, timeout=None):
        if timeout is not None:
            delays.append(float(timeout))
        return self.is_set()

    monkeypatch.setattr(threading.Event, "wait", _wait, raising=False)
    return delays


def test_success_interval_is_jittered_within_20_percent(tmp_path, monkeypatch):
    db = tmp_path / "engine.db"
    interval = 0.5
    snapshots = [[nm(f"m{i}", (ev(0, "goal"),))] for i in range(8)]
    provider = ScriptedProvider(snapshots)
    delays = capture_delays(monkeypatch)
    run_engine(_engine(make_pack(provider), db, poll_interval=interval), provider)

    if not delays:
        pytest.skip("engine interval not observable via time.sleep / Event.wait")
    # Every scheduled interval on the success path sits within ±20% of base.
    lo, hi = interval * 0.8, interval * 1.2
    in_band = [d for d in delays if lo <= d <= hi]
    assert in_band, f"no interval within ±20% of {interval}s among {delays}"
    for d in in_band:
        assert lo <= d <= hi
    # Jitter is applied (not a constant) — continuous uniform never repeats.
    assert len(set(in_band)) > 1, f"interval is not jittered (constant): {in_band}"


def test_backoff_doubles_and_caps_then_resets(tmp_path, monkeypatch):
    db = tmp_path / "engine.db"
    interval = 0.5
    match = nm("m1", (ev(0, "goal"),))
    # Five consecutive failures then a success: backoff should climb base → 2× →
    # 4× → 8× (capped) and reset to ~base once the success poll lands.
    script = [ProviderUnavailableError(f"fail{i}") for i in range(5)] + [[match]]
    provider = ScriptedProvider(script)
    delays = capture_delays(monkeypatch)
    run_engine(_engine(make_pack(provider), db, poll_interval=interval), provider)

    if not delays:
        pytest.skip("engine backoff not observable via time.sleep / Event.wait")

    cap = 8 * interval
    # Nothing exceeds the 8× cap (allow ±20% jitter headroom).
    assert max(delays) <= cap * 1.2 + 1e-9, f"backoff exceeded 8× cap: {delays}"
    # It climbed well past the base interval toward the cap.
    assert max(delays) >= interval * 3, f"backoff never grew under repeated failure: {delays}"
    # The first observed delay is on the order of the base interval, not the cap.
    assert delays[0] <= interval * 2, f"first backoff delay too large: {delays}"
    # After the failure run, a delay returns to ~base (reset on first success).
    tail_reset = [d for d in delays if d <= interval * 1.2]
    assert tail_reset, f"backoff never reset toward base after success: {delays}"


# --------------------------------------------------------------------------- #
# Construction contract
# --------------------------------------------------------------------------- #


def test_engine_constructor_positional_signature(tmp_path):
    # CollectorEngine(pack, db_path, source, poll_interval) — plan-pinned order.
    from gamecollect.engine import CollectorEngine

    provider = ScriptedProvider([])
    engine = CollectorEngine(make_pack(provider), str(tmp_path / "x.db"), SOURCE, 0.01)
    assert engine is not None


# --------------------------------------------------------------------------- #
# --record output path (single file vs siblings)
# --------------------------------------------------------------------------- #


def test_record_multi_match_json_path_writes_siblings(tmp_path):
    from gamecollect.engine import CollectorEngine
    from gamecollect.fixture_io import fixture_stem

    db = tmp_path / "engine.db"
    # A ``.json`` record path with more than one match cannot be that single
    # file for all of them; the engine must write unambiguous sibling files
    # ``<stem>-<fixture_stem(match_id)>.json`` rather than a directory literally
    # named ``session.json``. The per-match stem is the collision-proof
    # ``fixture_stem`` (sanitized id + short hash), not the raw id.
    record = tmp_path / "session.json"
    m1 = nm("m1", (ev(0, "goal"),))
    m2 = nm("m2", (ev(0, "goal"),))
    provider = ScriptedProvider([[m1, m2]])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, record_path=str(record))
    run_engine(engine, provider)

    # No directory masquerading as the ``.json`` path.
    assert not record.is_dir()
    sib1 = tmp_path / f"session-{fixture_stem('m1')}.json"
    sib2 = tmp_path / f"session-{fixture_stem('m2')}.json"
    assert sib1.is_file(), f"expected sibling fixture {sib1.name}"
    assert sib2.is_file(), f"expected sibling fixture {sib2.name}"
    # Each sibling is a valid fixture for its own match.
    assert json.loads(sib1.read_text())["match"]["match_id"] == "m1"
    assert json.loads(sib2.read_text())["match"]["match_id"] == "m2"


# --------------------------------------------------------------------------- #
# display_clock survives the engine write path
# --------------------------------------------------------------------------- #


def test_engine_persists_display_clock_to_db(tmp_path):
    """A provider-supplied ``display_clock`` must round-trip to the ``matches``
    row. The writer accepts the column and the read contract exposes it, but the
    seed/upsert dict on the engine path historically omitted it, so the value was
    silently dropped. Drive one poll through the default seed hook and assert the
    stored row carries the clock."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "clock.db"
    match = nm("m-clock", (ev(0, "goal"),), display_clock="45'+2")
    provider = ScriptedProvider([[match]])
    # default_seed_match seeds under the bare provider id "m-clock".
    pack = make_pack(provider, seed_match=default_seed_match)
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01)
    run_engine(engine, provider)

    conn = read_db(db)
    try:
        row = reader.get_state(conn, "m-clock")
    finally:
        conn.close()
    assert row is not None, "the match row was not seeded"
    assert row["display_clock"] == "45'+2", "provider display_clock must reach the DB"


# --------------------------------------------------------------------------- #
# default_seed_match persists identity + payload (no baseline-and-drop)
# --------------------------------------------------------------------------- #


def test_default_seed_persists_identity_and_payload_corrections(tmp_path):
    """Review fix: the diff state key counts home_team/away_team/payload and the
    engine baselines after a successful apply, so default_seed_match must persist
    them — otherwise an identity/payload correction is detected, "applied"
    without those fields, baselined, and dropped forever. Drive two polls: the
    seed poll and a correction poll changing home_team + a payload key; both
    must land in matches.payload."""
    from dataclasses import replace

    from gamecollect.engine import CollectorEngine

    db = tmp_path / "identity.db"
    poll1 = replace(
        nm("m-id", (ev(0, "goal"),)),
        payload={"round_name": "Group A", "venue": "BMO Field"},
    )
    # The correction: provider fixes the home team name and the venue.
    poll2 = replace(
        nm("m-id", (ev(0, "goal"),), home_team="Canada MNT"),
        payload={"round_name": "Group A", "venue": "BC Place"},
    )
    provider = ScriptedProvider([[poll1], [poll2]])
    pack = make_pack(provider, seed_match=default_seed_match)
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01)
    run_engine(engine, provider)

    conn = read_db(db)
    try:
        row = reader.get_state(conn, "m-id")
    finally:
        conn.close()
    assert row is not None, "the match row was not seeded"
    payload = json.loads(row["payload"])
    assert payload["home_team"] == "Canada MNT", (
        "a later identity correction must land, not be baselined-and-dropped"
    )
    assert payload["away_team"] == "Qatar"
    assert payload["venue"] == "BC Place", "a later payload correction must land"
    assert payload["round_name"] == "Group A"


# --------------------------------------------------------------------------- #
# close(): flush-once, always close the connection, idempotent
# --------------------------------------------------------------------------- #


def test_close_flush_failure_closes_conn_and_is_idempotent(tmp_path):
    """A failing ``--record`` flush must not leak the WAL connection nor re-raise
    on a second ``close()``. The record path points through a regular file (so
    ``write_fixture``'s ``mkdir`` raises): the first ``close()`` surfaces the
    error once but still closes the connection; the second ``close()`` is a
    no-op (a failed flush is not re-attempted)."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "close.db"
    # A regular file where write_fixture needs a parent directory forces mkdir
    # (and thus the flush) to raise. It is created AFTER construction —
    # construction-time validation now rejects an existing-file ancestor, so
    # the blocker must appear between poll and close to exercise the flush
    # failure path.
    blocker = tmp_path / "blocker"
    record = blocker / "session.json"

    match = nm("m1", (ev(0, "goal"),))
    provider = ScriptedProvider([[match]])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, record_path=str(record))
    # One poll populates the record accumulator without run_engine's own close().
    engine.poll_once()
    blocker.write_text("x")

    with pytest.raises(OSError):
        engine.close()
    assert engine._conn is None, "connection must be closed even when the flush fails"
    # The recorded session survives the failed flush — it is NOT destroyed.
    assert engine._record, "a failed flush must preserve the record buffer, not clear it"

    # A second close() must not re-run the failing flush.
    engine.close()
    assert engine._record, "the preserved record buffer must survive the idempotent re-close"


def test_record_existing_non_json_file_fails_fast_at_construction(tmp_path):
    """A ``--record`` path without a ``.json`` suffix is treated as a directory
    by the flush writer. If it already exists as a regular FILE, every fixture
    write would fail — at shutdown, after the whole session was collected. The
    engine must reject it at construction time, before any polling starts."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    blocker = tmp_path / "already-a-file"
    blocker.write_text("not a directory")

    provider = ScriptedProvider([])
    with pytest.raises(ValueError, match="record"):
        CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, record_path=str(blocker))
    # Fail-fast must not have created the database either (no polling started).
    assert not db.exists()


def test_record_json_path_that_is_a_directory_fails_fast_at_construction(tmp_path):
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    dir_named_json = tmp_path / "session.json"
    dir_named_json.mkdir()

    provider = ScriptedProvider([])
    with pytest.raises(ValueError, match="record"):
        CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, record_path=str(dir_named_json))


def test_record_json_path_with_existing_file_parent_fails_fast_at_construction(tmp_path):
    """``--record out/session.json`` where ``out`` is an existing regular FILE:
    the suffix is ``.json`` and the target is not a directory, but the flush's
    ``mkdir(parents=True)`` would raise at close() — after a whole session was
    collected. Construction must reject the existing-file ancestor."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    out = tmp_path / "out"
    out.write_text("not a directory")

    provider = ScriptedProvider([])
    with pytest.raises(ValueError, match="record"):
        CollectorEngine(
            make_pack(provider), str(db), SOURCE, 0.01, record_path=str(out / "session.json")
        )
    assert not db.exists()


def test_record_deep_path_with_file_mid_ancestry_fails_fast_at_construction(tmp_path):
    """A regular file anywhere along the not-yet-existing ancestry (here
    ``a`` under a deep ``a/b/c/session.json`` target) is caught too — the
    nearest EXISTING ancestor must be a directory."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    mid = tmp_path / "a"
    mid.write_text("file, not dir")

    provider = ScriptedProvider([])
    with pytest.raises(ValueError, match="record"):
        CollectorEngine(
            make_pack(provider),
            str(db),
            SOURCE,
            0.01,
            record_path=str(mid / "b" / "c" / "session.json"),
        )
    assert not db.exists()


def test_record_nested_not_yet_existing_dirs_allowed(tmp_path):
    """A nested target whose ancestry does not exist yet is fine — the flush
    creates it with ``mkdir(parents=True)``. Validation must not reject it."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    record = tmp_path / "new" / "dirs" / "session.json"

    match = nm("m1", (ev(0, "goal"),))
    provider = ScriptedProvider([[match]])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, record_path=str(record))
    engine.poll_once()
    engine.close()
    assert record.is_file()


# --------------------------------------------------------------------------- #
# One-time restart scan: matches live at shutdown that never reappear
# --------------------------------------------------------------------------- #


def _prepare_restart_db(db_path, stored_id: str, *, map_provider_id: str | None = None) -> None:
    """Simulate a prior session's leftovers: a live ``matches`` row (and
    optionally its ``provider_match_map`` entry) under this source."""
    from gamecollect.db.connection import connect

    conn = connect(str(db_path), side_table_ddl=FAKE_SIDE_TABLE_DDL)
    try:
        with conn:
            conn.execute(
                "INSERT INTO matches (match_id, source, kickoff_utc, status, minute, "
                "score_home, score_away, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    stored_id,
                    SOURCE,
                    "2026-06-18T18:00:00Z",
                    MatchStatus.IN_PLAY.value,
                    88,
                    1,
                    0,
                    json.dumps({"home_team": "Canada", "away_team": "Qatar"}),
                ),
            )
            if map_provider_id is not None:
                conn.execute(
                    "INSERT INTO provider_match_map "
                    "(source, provider, provider_match_id, match_id) VALUES (?, ?, ?, ?)",
                    (SOURCE, "espn", map_provider_id, stored_id),
                )
    finally:
        conn.close()


def _reconciling_seed(conn: sqlite3.Connection, writer, match: NormalizedMatch) -> str | None:
    """Minimal reconciling seed hook (NOT default_seed_match): map-resolved
    canonical id when one exists, else the source-qualified stub."""
    row = conn.execute(
        "SELECT match_id FROM provider_match_map WHERE source = ? AND provider_match_id = ?",
        (writer.source, match.match_id),
    ).fetchone()
    target = row[0] if row is not None else qualified(match.match_id, writer.source)
    writer.upsert_match(
        {
            "match_id": target,
            "kickoff_utc": match.kickoff_utc,
            "status": match.status.value,
            "minute": match.minute,
            "score_home": match.score_home,
            "score_away": match.score_away,
        }
    )
    return target


def _restart_ft_detail(match_id: str = "m1") -> NormalizedMatch:
    return nm(
        match_id,
        (ev(0, "goal", minute=44), ev(1, "goal", minute=95, detail="Stoppage winner")),
        status=MatchStatus.FINISHED,
        minute=None,
        score_home=2,
        score_away=0,
    )


def test_restart_scan_hydrates_qualified_stub_row_vanished_across_restart(tmp_path):
    """Codex adversarial finding: a source-qualified stub row left live at
    shutdown whose match NEVER reappears on the slate must be picked up by the
    one-time restart scan, tracker'd under the provider id, and terminally
    hydrated — not left in-play forever."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    _prepare_restart_db(db, qualified("m1"))

    provider = ScriptedDetailProvider([([], {"m1": _restart_ft_detail()})])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
    finally:
        engine.close()

    assert provider.detail_calls == ["m1"], "the scan must fetch detail by PROVIDER id"
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "a stub row live across a restart and gone from the slate must be closed out"
    )
    assert state["score_home"] == 2
    assert event_seqs(db, qualified("m1")) == [0, 1], "final-only events must land"


def test_restart_scan_hydrates_bare_provider_id_row(tmp_path):
    """Same restart gap for a default_seed_match pack: the stored id IS the
    provider id, so the scan resolves it to itself and hydrates."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    _prepare_restart_db(db, "m1")

    provider = ScriptedDetailProvider([([], {"m1": _restart_ft_detail()})])
    pack = make_pack(provider, seed_match=default_seed_match)
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
    finally:
        engine.close()

    assert provider.detail_calls == ["m1"]
    state = reader.get_state(read_db(db), "m1")
    assert state is not None and state["status"] == MatchStatus.FINISHED.value
    assert event_seqs(db, "m1") == [0, 1]


def test_restart_scan_resolves_canonical_row_via_provider_match_map(tmp_path):
    """A canonical (reconciled) stored id is not a provider id: the scan must
    resolve the provider id through provider_match_map, fetch detail under it,
    and the seed hook lands the terminal state back on the canonical row."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    _prepare_restart_db(db, "canada-vs-qatar-2026-06-18", map_provider_id="m1")

    provider = ScriptedDetailProvider([([], {"m1": _restart_ft_detail()})])
    pack = make_pack(provider, seed_match=_reconciling_seed)
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
    finally:
        engine.close()

    assert provider.detail_calls == ["m1"], "detail must be fetched by the MAPPED provider id"
    state = reader.get_state(read_db(db), "canada-vs-qatar-2026-06-18")
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "the canonical row must be closed via the map-resolved provider id"
    )
    assert event_seqs(db, "canada-vs-qatar-2026-06-18") == [0, 1]


def test_restart_scan_skips_canonical_row_whose_provider_id_is_on_slate(tmp_path):
    """Still-live protection: a canonical live row whose mapped provider id IS
    on the current slate is being collected normally — the scan must NOT seed
    a tracker (which would wrongly treat it as vanished)."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    _prepare_restart_db(db, "canada-vs-qatar-2026-06-18", map_provider_id="m1")

    live = nm("m1", (ev(0, "goal", minute=44),), status=MatchStatus.IN_PLAY, minute=89)
    provider = ScriptedDetailProvider([([live], {"m1": live})])
    pack = make_pack(provider, seed_match=_reconciling_seed)
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
        assert engine._transitions == {}, (
            "a stored-live row whose provider id is on the slate must not be tracker'd"
        )
    finally:
        engine.close()

    state = reader.get_state(read_db(db), "canada-vs-qatar-2026-06-18")
    assert state is not None and state["status"] == MatchStatus.IN_PLAY.value, (
        "normal live collection must continue on the canonical row, never a forced close"
    )


def test_restart_scan_canonical_row_without_map_entry_warns_and_does_not_seed(tmp_path, caplog):
    """A canonical row with NO provider_match_map entry cannot be resolved to a
    provider id: seeding a tracker would detail-fetch a slug, fail to the cap,
    and force-close a possibly-live match. The scan must WARN and leave it."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    _prepare_restart_db(db, "canada-vs-qatar-2026-06-18")

    provider = ScriptedDetailProvider([([], {})])
    pack = make_pack(provider, seed_match=_reconciling_seed)
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01, provider=provider)
    try:
        with caplog.at_level(logging.WARNING, logger="gamecollect.engine"):
            engine.poll_once()
        assert engine._transitions == {}, "an unresolvable stored id must not be tracker'd"
    finally:
        engine.close()

    assert provider.detail_calls == [], "no detail fetch may be attempted on a non-provider id"
    assert any(
        "no provider_match_map entry" in rec.message and rec.levelno == logging.WARNING
        for rec in caplog.records
    ), "the unresolvable live row must be WARNed about"
    state = reader.get_state(read_db(db), "canada-vs-qatar-2026-06-18")
    assert state is not None and state["status"] == MatchStatus.IN_PLAY.value, (
        "the row must be left as-is (status quo beats a wrong forced close)"
    )


def test_restart_scan_runs_exactly_once(tmp_path):
    """The restart scan is a one-shot: the second poll must not re-scan the
    matches table for live leftovers (asserted via the sqlite statement trace)."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    _prepare_restart_db(db, qualified("m1"))

    provider = ScriptedDetailProvider([([], {"m1": _restart_ft_detail()}), ([], {}), ([], {})])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    scan_sql = "SELECT match_id FROM matches"
    try:
        statements: list[str] = []
        engine._conn.set_trace_callback(statements.append)
        engine.poll_once()
        first_poll_scans = [s for s in statements if scan_sql in s]
        assert len(first_poll_scans) == 1, "the first poll must scan stored live rows exactly once"
        statements.clear()
        engine.poll_once()
        engine.poll_once()
        assert [s for s in statements if scan_sql in s] == [], (
            "subsequent polls must never re-run the restart scan"
        )
    finally:
        engine._conn.set_trace_callback(None)
        engine.close()


# --------------------------------------------------------------------------- #
# 2026-07 review findings: map-resolved give-up base, flicker budget reset,
# multi-provider map, bounded give-up re-emission, warn consistency,
# deterministic stored-row choice
# --------------------------------------------------------------------------- #


def test_restart_seeded_canonical_row_give_up_persists_via_map_lookup(tmp_path, caplog):
    """Finding 1: a restart-seeded RECONCILED match whose detail keeps failing
    must, at give-up, resolve its canonical stored row through
    provider_match_map and close it — not log "nothing to persist" and leave
    the row IN_PLAY forever (the exact wedge the restart scan targets)."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    _prepare_restart_db(db, "canada-vs-qatar-2026-06-18", map_provider_id="m1")

    fail = ProviderUnavailableError("detail endpoint down")
    provider = ScriptedDetailProvider([([], {"m1": fail})] * 3)
    pack = make_pack(provider, seed_match=_reconciling_seed)
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01, provider=provider)
    try:
        with caplog.at_level(logging.WARNING, logger="gamecollect.engine"):
            for _ in range(3):
                engine.poll_once()
        assert engine._transitions == {}, "the give-up must consume the tracker durably"
    finally:
        engine.close()

    assert not any("nothing to persist" in rec.message for rec in caplog.records), (
        "the canonical row IS reachable via provider_match_map — never 'nothing to persist'"
    )
    assert any(
        "persisted best-known terminal state" in rec.message and rec.levelno == logging.ERROR
        for rec in caplog.records
    )
    state = reader.get_state(read_db(db), "canada-vs-qatar-2026-06-18")
    assert state is not None and state["status"] == MatchStatus.FINISHED.value, (
        "the canonical row must leave IN_PLAY via the map-resolved stored base"
    )
    assert state["score_home"] == 1 and state["score_away"] == 0, (
        "stored scores must survive the force-close"
    )


def test_slate_flicker_then_live_resumption_pops_tracker_for_fresh_ft_budget(tmp_path):
    """Finding 2: slate flicker earlier in the match must not permanently
    consume the retry budget of the genuine FT transition — a healthy LIVE
    resumption on the slate pops the tracker entirely."""
    from gamecollect.engine import _MAX_TRANSITION_TOTAL_ATTEMPTS, CollectorEngine

    db = tmp_path / "engine.db"
    live1 = nm("m1", (ev(0),), status=MatchStatus.IN_PLAY, minute=10)
    flicker = nm("m1", status=MatchStatus.SCHEDULED, minute=None, score_home=None, score_away=None)
    live2 = nm("m1", (ev(0),), status=MatchStatus.IN_PLAY, minute=55, display_clock="55'")
    final = nm(
        "m1",
        (ev(0), ev(1, "goal", minute=90)),
        status=MatchStatus.FINISHED,
        minute=None,
        score_home=2,
        score_away=0,
    )
    provider = ScriptedDetailProvider(
        [
            ([live1], {"m1": live1}),
            ([flicker], {"m1": flicker}),  # flicker: non-terminal transition attempt
            ([live2], {"m1": live2}),  # healthy live resumption
            ([final], {"m1": final}),  # the genuine FT transition
        ]
    )
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        engine.poll_once()
        engine.poll_once()
        assert "m1" in engine._transitions, "the flicker poll must create a tracker"
        # Simulate a long flicker history: the budget is fully consumed, so
        # WITHOUT the fix the FT poll's at-cap pre-check would give up
        # without a single detail fetch.
        engine._transitions["m1"].total_attempts = _MAX_TRANSITION_TOTAL_ATTEMPTS
        engine.poll_once()  # live resumption
        assert engine._transitions == {}, (
            "a healthy live resumption on the slate must pop the stale tracker"
        )
        engine.poll_once()  # genuine FT transition: fresh budget, detail fetched
        assert engine._transitions == {}
    finally:
        engine.close()

    assert provider.detail_calls[-1] == "m1", "the FT poll must actually fetch detail"
    state = reader.get_state(read_db(db), qualified("m1"))
    assert state is not None and state["status"] == MatchStatus.FINISHED.value
    assert state["score_home"] == 2
    assert event_seqs(db, qualified("m1")) == [0, 1], (
        "the genuine transition's final events must land (no premature give-up)"
    )


def test_multi_provider_map_rows_warn_and_hydrate_without_wedge(tmp_path, caplog):
    """Finding 3: >1 DISTINCT provider ids mapped to one stored row (a
    multi-provider source) must WARN naming them and proceed with the first —
    graceful loud degradation, never a wedge."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    _prepare_restart_db(db, "canada-vs-qatar-2026-06-18", map_provider_id="m1")
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO provider_match_map (source, provider, provider_match_id, match_id) "
            "VALUES (?, ?, ?, ?)",
            (SOURCE, "zprov", "x9", "canada-vs-qatar-2026-06-18"),
        )
        conn.commit()
    finally:
        conn.close()

    provider = ScriptedDetailProvider([([], {"m1": _restart_ft_detail()})])
    pack = make_pack(provider, seed_match=_reconciling_seed)
    engine = CollectorEngine(pack, str(db), SOURCE, 0.01, provider=provider)
    try:
        with caplog.at_level(logging.WARNING, logger="gamecollect.engine"):
            engine.poll_once()
    finally:
        engine.close()

    warns = [r for r in caplog.records if "multiple provider ids" in r.message]
    assert len(warns) == 1 and "espn:m1" in warns[0].getMessage(), (
        "the ambiguity must be WARNed about, naming the candidate ids"
    )
    assert "zprov:x9" in warns[0].getMessage()
    assert provider.detail_calls == ["m1"], (
        "the first id in (provider, provider_match_id) order must be used"
    )
    state = reader.get_state(read_db(db), "canada-vs-qatar-2026-06-18")
    assert state is not None and state["status"] == MatchStatus.FINISHED.value


def test_unpersistable_give_up_bounded_reemits_then_final_error_and_pop(tmp_path, caplog):
    """Finding 4: a give-up snapshot that can NEVER persist (missing identity,
    seed_match returns None every poll) must re-emit a bounded number of times,
    then log ONE final ERROR and drop the tracker — not loop forever."""
    from gamecollect.engine import (
        _MAX_GIVE_UP_EMISSIONS,
        _MAX_TRANSITION_DETAIL_FAILURES,
        CollectorEngine,
        _TransitionTracker,
    )

    db = tmp_path / "engine.db"
    provider = ScriptedDetailProvider([([], {})] * 5)
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    engine._restart_scan_done = True
    identityless = nm(
        "m1",
        status=MatchStatus.FINISHED,
        home_team=None,
        away_team=None,
        kickoff_utc=None,
    )
    engine._transitions["m1"] = _TransitionTracker(
        fallback=identityless,
        consecutive_failures=_MAX_TRANSITION_DETAIL_FAILURES,  # already at cap
    )
    try:
        with caplog.at_level(logging.INFO, logger="gamecollect.engine"):
            for _ in range(5):
                engine.poll_once()
        assert engine._transitions == {}, "the exhausted tracker must be abandoned"
    finally:
        engine.close()

    unpersisted = [r for r in caplog.records if "seed_match returned None" in r.message]
    assert len(unpersisted) == _MAX_GIVE_UP_EMISSIONS, (
        f"exactly {_MAX_GIVE_UP_EMISSIONS} re-emission applies must be attempted"
    )
    finals = [
        r
        for r in caplog.records
        if "could not persist terminal state" in r.message and r.levelno == logging.ERROR
    ]
    assert len(finals) == 1, "exactly ONE final abandonment ERROR"
    assert "abandoning tracker" in finals[0].getMessage()


def test_nonterminal_warn_dedupes_consistently_across_slate_and_vanished_paths(tmp_path, caplog):
    """Finding 5: both hydration paths must feed the SAME source (merged
    status) into _warn_nonterminal — one dedupe key, so an unchanged
    non-terminal status warns once across an on-slate poll and a subsequent
    vanished poll."""
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    live = nm("m1", (ev(0),), status=MatchStatus.IN_PLAY, minute=80)
    reset = nm("m1", status=MatchStatus.SCHEDULED, minute=None, score_home=None, score_away=None)
    provider = ScriptedDetailProvider(
        [
            ([live], {"m1": live}),
            ([reset], {"m1": reset}),  # on-slate non-terminal reset: WARN once
            ([], {"m1": reset}),  # vanished, same merged status: deduped
        ]
    )
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        with caplog.at_level(logging.WARNING, logger="gamecollect.engine"):
            for _ in range(3):
                engine.poll_once()
    finally:
        engine.close()

    warns = [r for r in caplog.records if "non-terminal status" in r.message]
    assert len(warns) == 1, "the same non-terminal status across both paths must WARN exactly once"
    assert MatchStatus.SCHEDULED.value in warns[0].getMessage()


def test_stored_snapshot_prefers_freshest_when_bare_and_qualified_rows_exist(tmp_path):
    """Finding 6: with BOTH the bare and the source-qualified row stored, the
    merge base must deterministically be the freshest (updated_at DESC), not
    whichever row sqlite happens to return first."""
    from gamecollect.db.connection import connect
    from gamecollect.engine import CollectorEngine

    db = tmp_path / "engine.db"
    conn = connect(str(db), side_table_ddl=FAKE_SIDE_TABLE_DDL)
    try:
        with conn:
            for match_id, score_home, updated_at in (
                # Older row inserted FIRST: without ORDER BY, rowid order
                # would (wrongly) pick it.
                ("m1", 1, "2026-06-18T19:00:00Z"),
                (qualified("m1"), 2, "2026-06-18T20:00:00Z"),
            ):
                conn.execute(
                    "INSERT INTO matches (match_id, source, kickoff_utc, status, minute, "
                    "score_home, score_away, payload, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        match_id,
                        SOURCE,
                        "2026-06-18T18:00:00Z",
                        MatchStatus.IN_PLAY.value,
                        88,
                        score_home,
                        0,
                        json.dumps({"home_team": "Canada", "away_team": "Qatar"}),
                        updated_at,
                    ),
                )
    finally:
        conn.close()

    provider = ScriptedDetailProvider([])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, provider=provider)
    try:
        snap = engine._stored_match_snapshot("m1")
    finally:
        engine.close()
    assert snap is not None
    assert snap.score_home == 2, "the freshest (updated_at) row must win deterministically"
