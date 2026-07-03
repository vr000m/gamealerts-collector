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
from gamecollect.packs.spec import EventTypeDecl, SportPack
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
    provider = ScriptedProvider([[p1_a, p1_b], [p2_a, p2_b]])

    with caplog.at_level(logging.DEBUG):
        run_engine(_engine(make_pack(provider), db), provider)

    # Match B was unaffected and kept collecting.
    assert event_seqs(db, qualified("B")) == [0, 1]
    # Match A's original seq 0 is intact — drift was skipped, not silently
    # applied over the stored stream.
    assert event_types(db, qualified("A"))[0] == "goal"
    # The drift was logged loudly (WARNING or higher).
    assert any(r.levelno >= logging.WARNING for r in caplog.records), (
        "SequenceError drift must be logged loudly"
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

    db = tmp_path / "engine.db"
    # A ``.json`` record path with more than one match cannot be that single
    # file for all of them; the engine must write unambiguous sibling files
    # ``<stem>-<match_id>.json`` rather than a directory literally named
    # ``session.json``.
    record = tmp_path / "session.json"
    m1 = nm("m1", (ev(0, "goal"),))
    m2 = nm("m2", (ev(0, "goal"),))
    provider = ScriptedProvider([[m1, m2]])
    engine = CollectorEngine(make_pack(provider), str(db), SOURCE, 0.01, record_path=str(record))
    run_engine(engine, provider)

    # No directory masquerading as the ``.json`` path.
    assert not record.is_dir()
    sib1 = tmp_path / "session-m1.json"
    sib2 = tmp_path / "session-m2.json"
    assert sib1.is_file(), f"expected sibling fixture {sib1.name}"
    assert sib2.is_file(), f"expected sibling fixture {sib2.name}"
    # Each sibling is a valid fixture for its own match.
    assert json.loads(sib1.read_text())["match"]["match_id"] == "m1"
    assert json.loads(sib2.read_text())["match"]["match_id"] == "m2"
