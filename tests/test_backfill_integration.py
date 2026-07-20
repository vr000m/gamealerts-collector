"""Phase 4: end-to-end/idempotency/cross-source ``backfill`` tests.

Plan contract (docs/dev_plans/20260718-feature-historical-backfill.md §Phase
4): prove — don't assert in prose — idempotent re-run, cross-source
rejection, and a real replay-fixture ingest through the full backfill path.

Unlike ``tests/test_backfill.py`` (Phase 2's unit tests, which drive
``run_backfill``/``enumerate_finished_matches`` against ``FakeEngine``/
``FakeScheduleProvider`` doubles, plus a few real-``CollectorEngine``
``apply_one_off_match`` tests already proving the merge-path and
direct-cross-partition invariants), this module drives ``run_backfill`` (and,
for the readiness-stamp test, ``_run_backfill``) against a REAL
``CollectorEngine`` over a REAL temp SQLite file — the only way to prove the
full stack (writer partition constraints, schema stamping, side-table
persistence, ``MatchReadPort``) actually holds end-to-end, not just that the
unit-level contracts are individually satisfied.

Not duplicated here (already covered by ``tests/test_backfill.py`` against a
real engine): the merge-path test
(``test_apply_one_off_match_merges_scoreboard_identity_when_detail_omits_it``)
and the direct cross-partition test
(``test_apply_one_off_match_second_source_raises_cross_partition_error``).
"""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from gamecollect import cli, diffing
from gamecollect.db.migrations import get_schema_version
from gamecollect.db.writer import CrossPartitionError
from gamecollect.engine import CollectorEngine
from gamecollect.packs.spec import EventTypeDecl, SportPack
from gamecollect.provider import MatchStatus, NormalizedEvent, NormalizedMatch

backfill = pytest.importorskip(
    "gamecollect.backfill",
    reason="src/gamecollect/backfill.py is written by the Phase 2 implementer subagent",
)

sys.path.insert(0, str(Path(__file__).parent / "football"))
from _football_helpers import KNOCKOUT_FIXTURES, replay_fixture  # noqa: E402

SOURCE = "test-src"

TAXONOMY: dict[str, EventTypeDecl] = {
    "goal": EventTypeDecl(display_name="Goal", importance_default=1),
    "yellow": EventTypeDecl(display_name="Yellow card", importance_default=3),
}


# --------------------------------------------------------------------------- #
# Domain-object factories — mirrors tests/test_backfill.py's nm()/ev() conventions.
# --------------------------------------------------------------------------- #


def ev(seq: int, event_type: str = "goal") -> NormalizedEvent:
    return NormalizedEvent(
        seq=seq,
        minute=10,
        event_type=event_type,
        importance=1,
        team="Canada",
        player="Jonathan David",
        assist=None,
        detail="Goal",
    )


def nm(
    match_id: str,
    events: tuple[NormalizedEvent, ...] = (),
    *,
    status: MatchStatus = MatchStatus.FINISHED,
    minute: int | None = 90,
    score_home: int | None = 1,
    score_away: int | None = 0,
    display_clock: str | None = "FT",
    home_team: str | None = "Canada",
    away_team: str | None = "Qatar",
    kickoff_utc: str | None = "2026-06-18T18:00:00Z",
    payload: dict | None = None,
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
        payload=payload or {},
    )


class NullProvider:
    """Minimal MatchDataProvider-shaped stub; the poll loop is never driven
    in these tests, so only construction needs to succeed."""

    def fetch_live_matches(self):
        return []

    def fetch_match_detail(self, match_id):
        return nm(match_id)


def make_pack(provider, *, side_table_ddl: tuple[str, ...] = ()) -> SportPack:
    """Generic (non-football) pack using ``default_seed_match`` — the raw,
    non-source-qualified ``match_id`` — mirroring tests/test_backfill.py's
    ``make_pack`` so cross-partition collisions stay directly observable."""
    return SportPack(
        name="backfill-integration-test",
        sport="football",
        provider_factory=lambda: provider,
        taxonomy=TAXONOMY,
        prompt_fragments={"x": "y"},
        preference_schema={"type": "object"},
        display_metadata={"sport_display": "Football"},
        compaction_boundaries=["kickoff"],
        side_table_ddl=side_table_ddl,
    )


def make_football_pack(provider) -> SportPack:
    """The REAL football side-table wiring (``FOOTBALL_SIDE_TABLE_DDL`` +
    ``persist_football_side_tables``), but the sport-agnostic
    ``default_seed_match`` — deliberately NOT the full ``gamecollect_football
    .pack.pack()`` factory, whose ``seed_or_reconcile_match`` hook resolves
    against a canonical WC2026 schedule this test has no need to depend on.
    This is exactly the same wiring ``tests/test_integration_shared_file.py``
    exercises via a raw ``PartitionWriter``, but reached through the real
    ``run_backfill``/``apply_one_off_match``/``_apply`` write path instead."""
    from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL, persist_football_side_tables
    from gamecollect_football.taxonomy import TAXONOMY as FOOTBALL_TAXONOMY

    return SportPack(
        name="backfill-integration-football-test",
        sport="football",
        provider_factory=lambda: provider,
        taxonomy=FOOTBALL_TAXONOMY,
        prompt_fragments={"x": "y"},
        preference_schema={"type": "object"},
        display_metadata={"sport_display": "Football"},
        compaction_boundaries=["kickoff", "half_time", "full_time"],
        side_table_ddl=FOOTBALL_SIDE_TABLE_DDL,
        persist_side_tables=persist_football_side_tables,
    )


def make_engine(
    tmp_path, db_name: str = "backfill.db", *, source: str = SOURCE, pack: SportPack | None = None
) -> CollectorEngine:
    db = tmp_path / db_name
    provider = NullProvider()
    return CollectorEngine(pack or make_pack(provider), str(db), source, 30.0, provider=provider)


class FakeScheduleProvider:
    """``fetch_schedule(dates=chunk)`` returns the scripted slate for that
    chunk; ``fetch_match_detail`` returns the scripted per-match value —
    mirrors tests/test_backfill.py's double of the same name."""

    def __init__(
        self,
        schedule: dict[str, list[NormalizedMatch]],
        *,
        details: dict[str, NormalizedMatch] | None = None,
    ) -> None:
        self._schedule = schedule
        self._details = details or {}
        self.schedule_calls: list[str] = []
        self.detail_calls: list[str] = []

    def fetch_schedule(self, *, dates: str) -> list[NormalizedMatch]:
        self.schedule_calls.append(dates)
        return list(self._schedule.get(dates, []))

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        self.detail_calls.append(match_id)
        return self._details.get(match_id, nm(match_id))


def _matches_row_count(db_path: Path, match_id: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        (count,) = conn.execute(
            "SELECT COUNT(*) FROM matches WHERE match_id = ?", (match_id,)
        ).fetchone()
        return count
    finally:
        conn.close()


def _events_for(db_path: Path, match_id: str) -> list[tuple[int, str]]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT seq, type FROM events WHERE match_id = ? ORDER BY seq", (match_id,)
        ).fetchall()
        return [(r["seq"], r["type"]) for r in rows]
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 1. Replay-fixture ingest test — real CollectorEngine, real DB, real
#    football side tables, MatchReadPort/events_for_match assertions.
# --------------------------------------------------------------------------- #


def test_run_backfill_replay_fixture_end_to_end_populates_readport(tmp_path):
    from gamecollect.db.connection import connect
    from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL
    from gamecollect_football.readport import FootballReadPort

    # spain_portugal_760506: one clean regulation goal (Mikel Merino, Spain,
    # stoppage time) — the same fixture tests/test_integration_shared_file.py
    # uses as its scorer/lineup/venue template.
    fx = KNOCKOUT_FIXTURES[2]
    match_id = fx["match_id"]
    detail = replay_fixture(fx["summary"], match_id)
    assert detail.status == MatchStatus.FINISHED
    # The enumerating scoreboard snapshot: same identity, no events (the
    # scoreboard/schedule endpoint never carries the event list — that's
    # what fetch_match_detail is for).
    scoreboard = replace(detail, events=[])

    provider = FakeScheduleProvider(schedule={"20260701": [scoreboard]}, details={match_id: detail})
    db = tmp_path / "replay.db"
    pack = make_football_pack(provider)
    engine = CollectorEngine(pack, str(db), SOURCE, provider=provider)
    try:
        report = backfill.run_backfill(
            engine, provider, ["20260701"], source=SOURCE, request_delay=0.0
        )
        assert report.applied == 1
        assert report.failed == {}
    finally:
        engine.close()

    # matches/events/football_* side tables populated: query directly, mirroring
    # tests/test_integration_shared_file.py's own scorer/lineup/venue template.
    conn = connect(db, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
    try:
        assert _matches_row_count(db, match_id) == 1
        events = _events_for(db, match_id)
        assert events, "expected at least one stored event for the backfilled match"

        readport = FootballReadPort(conn)
        # pack.taxonomy is the real gamecollect_football.taxonomy.TAXONOMY
        # (make_football_pack wires it in directly) — identify goal-family
        # event types generically from its display names, same intent as
        # tests/test_integration_shared_file.py's _goal_family_types, without
        # requiring this synthetic (non-entry-point-registered) pack to be
        # loadable via gamecollect.client.get_vocabulary/load_pack.
        goal_family_types = {
            event_type
            for event_type, decl in pack.taxonomy.items()
            if "goal" in decl.display_name.lower()
        }
        assert goal_family_types

        matches = readport.list_matches()
        assert [m["match_id"] for m in matches] == [match_id]
        participants = {p["name"] for p in matches[0]["participants"]}
        assert participants == {fx["home"], fx["away"]}

        match_events = readport.events_for_match(match_id)
        scorers = [e["player"] for e in match_events if e["type"] in goal_family_types]
        assert scorers == ["Mikel Merino"]
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 2. Re-run idempotency — second run: zero applies, zero new fetch_match_detail
#    calls, already_stored_skipped == enumerated.
# --------------------------------------------------------------------------- #


def test_run_backfill_rerun_is_idempotent_no_new_detail_fetches(tmp_path):
    match_id = "m1"
    provider = FakeScheduleProvider(
        {"20260601": [nm(match_id)]},
        details={match_id: nm(match_id, (ev(0),))},
    )
    engine = make_engine(tmp_path)
    try:
        report1 = backfill.run_backfill(
            engine, provider, ["20260601"], source=SOURCE, request_delay=0.0
        )
        assert report1.applied == 1
        assert provider.detail_calls == [match_id]

        report2 = backfill.run_backfill(
            engine, provider, ["20260601"], source=SOURCE, request_delay=0.0
        )
        assert report2.applied == 0
        assert report2.already_stored_skipped == report2.enumerated == 1
        # No new detail fetch on the second run — the skip check short-circuits
        # before any HTTP-issuing call.
        assert provider.detail_calls == [match_id]
    finally:
        engine.close()


# --------------------------------------------------------------------------- #
# 3. Different-source existing-row test, driven through run_backfill against
#    TWO real engines sharing one DB file — proves the skip check does NOT
#    silently skip a foreign-source row, and that it surfaces as a real
#    CrossPartitionError from the writer (not a scripted double).
# --------------------------------------------------------------------------- #


def test_run_backfill_different_source_existing_row_raises_cross_partition_error(tmp_path):
    db = tmp_path / "shared.db"
    match_id = "m1"

    engine_x = CollectorEngine(
        make_pack(NullProvider()), str(db), "source-x", provider=NullProvider()
    )
    try:
        engine_x.apply_one_off_match(nm(match_id), nm(match_id, (ev(0),)))
    finally:
        engine_x.close()
    assert _matches_row_count(db, match_id) == 1

    provider = FakeScheduleProvider(
        {"20260601": [nm(match_id)]}, details={match_id: nm(match_id, (ev(0),))}
    )
    engine_y = CollectorEngine(make_pack(provider), str(db), "source-y", provider=provider)
    try:
        with pytest.raises(CrossPartitionError):
            backfill.run_backfill(
                engine_y, provider, ["20260601"], source="source-y", request_delay=0.0
            )
        # Not silently skipped: the detail fetch and apply attempt both
        # actually happened before the writer's guard fired.
        assert provider.detail_calls == [match_id]
    finally:
        engine_y.close()


# --------------------------------------------------------------------------- #
# 4. Event-less stored-row test — real engine, real DB: a same-source row
#    with a matches row but zero events is retried, not skipped.
# --------------------------------------------------------------------------- #


def test_run_backfill_same_source_eventless_row_is_hydrated_with_events(tmp_path):
    match_id = "m1"
    db = tmp_path / "eventless.db"
    provider_seed = NullProvider()
    pack = make_pack(provider_seed)
    engine = CollectorEngine(pack, str(db), SOURCE, provider=provider_seed)
    try:
        # Seed a same-source matches row with ZERO events — simulating the
        # live daemon's bounded-retry-then-give-up path.
        engine.apply_one_off_match(nm(match_id, ()), nm(match_id, ()))
        assert _matches_row_count(db, match_id) == 1
        assert _events_for(db, match_id) == []

        provider = FakeScheduleProvider(
            {"20260601": [nm(match_id)]}, details={match_id: nm(match_id, (ev(0), ev(1, "yellow")))}
        )
        # Re-point the engine's own provider reference is unnecessary — the
        # engine only reads/writes via run_backfill's explicit provider arg.
        report = backfill.run_backfill(
            engine, provider, ["20260601"], source=SOURCE, request_delay=0.0
        )
        assert report.already_stored_skipped == 0
        assert report.applied == 1
        assert provider.detail_calls == [match_id]
        assert _events_for(db, match_id) == [(0, "goal"), (1, "yellow")]
    finally:
        engine.close()


# --------------------------------------------------------------------------- #
# 5. Backfill/live concurrent-overlap no-duplicate test.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("backfill_first", [True, False])
def test_backfill_and_live_apply_overlap_produce_one_matches_row(tmp_path, backfill_first):
    match_id = "m1"
    db_name = f"overlap-{backfill_first}.db"
    engine = make_engine(tmp_path, db_name)
    try:
        scoreboard = nm(match_id)
        detail = nm(match_id, (ev(0),))

        def apply_via_backfill_path():
            # The backfill entry point: merge scoreboard+detail, diff against
            # None, apply.
            result = engine.apply_one_off_match(scoreboard, detail)
            assert result is not None

        def apply_via_live_path():
            # The live-daemon entry point: poll_once ultimately builds a diff
            # via diff_match(snapshot, last) and calls self._apply(diff) —
            # exercised directly here (as the plan names:
            # "CollectorEngine.poll_once/_apply") rather than driving the
            # full poll loop's transition/cooldown machinery, which is
            # orthogonal to the idempotency invariant under test.
            diff = diffing.diff_match(detail, last=None)
            result = engine._apply(diff)
            assert result is not None

        if backfill_first:
            apply_via_backfill_path()
            apply_via_live_path()
        else:
            apply_via_live_path()
            apply_via_backfill_path()

        db_path = tmp_path / db_name
        assert _matches_row_count(db_path, match_id) == 1
    finally:
        engine.close()


# --------------------------------------------------------------------------- #
# 6. Backfill-only readiness-stamp test — through _run_backfill (Phase 3's
#    CLI handler), zero-match enumeration, verified via a fresh read-only
#    connection.
# --------------------------------------------------------------------------- #


def test_run_backfill_cli_stamps_schema_on_zero_match_enumeration(tmp_path):
    db = tmp_path / "readiness.db"
    assert not db.exists()

    provider = FakeScheduleProvider(schedule={})  # every chunk enumerates zero matches
    pack = make_pack(provider)
    args = SimpleNamespace(
        pack="backfill-integration-test",
        db=str(db),
        source=SOURCE,
        start_date="2026-01-10",
        end_date="2026-01-10",
    )

    rc = cli._run_backfill(
        args,
        provider=provider,
        engine_factory=CollectorEngine,
        pack_loader=lambda name: pack,
    )

    assert rc == 0
    assert db.exists()
    # Never drove any match apply — this proves the stamp comes from engine
    # CONSTRUCTION, not from a write (a zero-match run has no writes to hide
    # behind).
    assert provider.detail_calls == []

    # Fresh, separate, READ-ONLY connection — mirroring gamealerts' reported
    # read-only open and TestStartupOrdering in test_integration_shared_file.py.
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        assert get_schema_version(conn) is not None
        tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert "matches" in tables
    finally:
        conn.close()
