"""Phase 2: ``gamecollect.backfill`` enumeration module + ``CollectorEngine.apply_one_off_match``.

Plan contract (docs/dev_plans/20260718-feature-historical-backfill.md §Phase 2):

- ``chunk_date_range(start, end, *, overlap_days=1) -> Iterator[str]`` is a pure
  function yielding single-day ``YYYYMMDD`` chunk strings only, padding
  ``overlap_days`` on each side of ``[start, end]`` by default.
- ``enumerate_finished_matches(provider, date_chunks) -> Iterator[NormalizedMatch]``
  calls ``provider.fetch_schedule(dates=chunk)`` per chunk and yields matches
  whose status is terminal via ``gamecollect.fallback_merge.is_terminal``. It
  raises a clear, typed error (not a bare ``AttributeError``) when the provider
  lacks ``fetch_schedule``.
- ``run_backfill(engine, provider, date_chunks, *, source, request_delay=0.5)
  -> BackfillReport`` asserts ``source == engine.source`` at entry, wraps each
  chunk's ``fetch_schedule`` call so a chunk-level failure lands in
  ``BackfillReport.failed_chunks`` and enumeration continues, skips a match only
  when ``engine.stored_source(match_id) == source`` AND
  ``engine.has_events(match_id)`` are both true, otherwise fetches detail and
  calls ``engine.apply_one_off_match(scoreboard, detail)``. A ``None`` return
  from ``apply_one_off_match`` counts as ``failed``. ``CrossPartitionError`` is
  the one exception ``run_backfill`` does NOT swallow into ``failed`` — it
  propagates uncaught.
- ``CollectorEngine.apply_one_off_match(scoreboard, detail)`` is a thin wrapper:
  ``merged = _merge_detail(scoreboard, detail)``, ``diff = diff_match(merged,
  None)``, ``return self._apply(diff)`` — no other engine in-memory state
  (``_transitions``, ``_backfill``, ``_backfill_apply_pending``) is read or
  written by the one-shot path.

Design intent under test: enumeration and one-shot apply are plain,
independently testable functions built on the EXISTING write path — no new
persistence logic, no engine in-memory state touched. Fake providers/engine
doubles are used for ``run_backfill``/``enumerate_finished_matches``; a REAL
``CollectorEngine`` against a temp DB is used to prove ``apply_one_off_match``'s
in-memory-state isolation and its use of the existing write path.
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from gamecollect import diffing
from gamecollect.db.writer import CrossPartitionError
from gamecollect.engine import CollectorEngine
from gamecollect.packs.spec import EventTypeDecl, SportPack
from gamecollect.provider import MatchStatus, NormalizedEvent, NormalizedMatch

backfill = pytest.importorskip(
    "gamecollect.backfill",
    reason="src/gamecollect/backfill.py is written by the Phase 2 implementer subagent",
)

SOURCE = "test-src"

TAXONOMY: dict[str, EventTypeDecl] = {
    "goal": EventTypeDecl(display_name="Goal", importance_default=1),
    "yellow": EventTypeDecl(display_name="Yellow card", importance_default=3),
}

FAKE_SIDE_TABLE_DDL: tuple[str, ...] = ()


# --------------------------------------------------------------------------- #
# Domain-object factories (mirrors tests/test_engine.py's conventions)
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


def make_pack(provider):
    # Uses the sport-agnostic ``default_seed_match`` (packs/spec.py): seeds
    # under the RAW provider-native ``match_id`` (no source-qualification —
    # that is pack territory, e.g. the football pack's own reconciler). This
    # is deliberate here: a cross-partition collision (two different-source
    # writers touching the same literal ``matches.match_id`` row) is only
    # observable when the id is NOT source-qualified; see
    # test_writer_partition.py's own cross-partition tests for the same
    # raw-match_id convention.
    return SportPack(
        name="backfill-test",
        sport="football",
        provider_factory=lambda: provider,
        taxonomy=TAXONOMY,
        prompt_fragments={"x": "y"},
        preference_schema={"type": "object"},
        display_metadata={"sport_display": "Football"},
        compaction_boundaries=["kickoff"],
        side_table_ddl=FAKE_SIDE_TABLE_DDL,
    )


class NullProvider:
    """A minimal MatchDataProvider-shaped stub; the poll loop is never driven
    in these tests, so only construction needs to succeed."""

    def fetch_live_matches(self):
        return []

    def fetch_match_detail(self, match_id):
        return nm(match_id)


def make_engine(tmp_path, db_name: str = "backfill.db", *, source: str = SOURCE) -> CollectorEngine:
    db = tmp_path / db_name
    return CollectorEngine(
        make_pack(NullProvider()), str(db), source, 30.0, provider=NullProvider()
    )


# --------------------------------------------------------------------------- #
# Fake providers / engine double for enumerate_finished_matches + run_backfill
# --------------------------------------------------------------------------- #


class FakeScheduleProvider:
    """``fetch_schedule(dates=chunk)`` returns the scripted slate for that chunk;
    ``fetch_match_detail`` returns (or raises) a scripted per-match value."""

    def __init__(
        self,
        schedule: dict[str, list[NormalizedMatch]],
        *,
        details: dict[str, NormalizedMatch | BaseException] | None = None,
        schedule_raises: dict[str, BaseException] | None = None,
    ) -> None:
        self._schedule = schedule
        self._details = details or {}
        self._schedule_raises = schedule_raises or {}
        self.schedule_calls: list[str] = []
        self.detail_calls: list[str] = []

    def fetch_schedule(self, *, dates: str) -> list[NormalizedMatch]:
        self.schedule_calls.append(dates)
        if dates in self._schedule_raises:
            raise self._schedule_raises[dates]
        return list(self._schedule.get(dates, []))

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        self.detail_calls.append(match_id)
        value = self._details.get(match_id)
        if isinstance(value, BaseException):
            raise value
        return value if value is not None else nm(match_id)


class NoScheduleProvider:
    """A provider lacking fetch_schedule entirely (only fetch_match_detail)."""

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        return nm(match_id)


class FakeEngine:
    """A CollectorEngine-shaped double exposing exactly the read surface
    ``run_backfill`` is documented to use: ``.source``, ``.stored_source``,
    ``.has_events``, ``.apply_one_off_match``."""

    def __init__(
        self,
        source: str,
        *,
        stored: dict[str, tuple[str, bool]] | None = None,
        apply_results: dict[str, NormalizedMatch | None] | None = None,
        apply_raises: dict[str, BaseException] | None = None,
        stored_raises: dict[str, BaseException] | None = None,
    ) -> None:
        self._source = source
        self._stored = stored or {}
        self._apply_results = apply_results or {}
        self._apply_raises = apply_raises or {}
        self._stored_raises = stored_raises or {}
        self.apply_calls: list[tuple[NormalizedMatch, NormalizedMatch]] = []

    @property
    def source(self) -> str:
        return self._source

    def stored_source(self, match_id: str) -> str | None:
        if match_id in self._stored_raises:
            raise self._stored_raises[match_id]
        entry = self._stored.get(match_id)
        return entry[0] if entry else None

    def has_events(self, match_id: str) -> bool:
        entry = self._stored.get(match_id)
        return entry[1] if entry else False

    def stored_source_and_has_events(self, match_id: str) -> tuple[str | None, bool]:
        # Route through the two single-answer methods so stored_raises still
        # raises from the skip-check exactly as before the combined call.
        return self.stored_source(match_id), self.has_events(match_id)

    def apply_one_off_match(
        self, scoreboard: NormalizedMatch, detail: NormalizedMatch
    ) -> NormalizedMatch | None:
        match_id = scoreboard.match_id
        self.apply_calls.append((scoreboard, detail))
        if match_id in self._apply_raises:
            raise self._apply_raises[match_id]
        return self._apply_results.get(match_id, scoreboard)


# --------------------------------------------------------------------------- #
# chunk_date_range
# --------------------------------------------------------------------------- #


def test_chunk_date_range_single_day_default_overlap_pads_both_sides():
    chunks = list(backfill.chunk_date_range(date(2026, 1, 10), date(2026, 1, 10)))
    assert chunks == ["20260109", "20260110", "20260111"]


def test_chunk_date_range_single_day_no_overlap_is_one_chunk():
    chunks = list(backfill.chunk_date_range(date(2026, 1, 10), date(2026, 1, 10), overlap_days=0))
    assert chunks == ["20260110"]


def test_chunk_date_range_multi_day_no_overlap_yields_each_calendar_day():
    chunks = list(backfill.chunk_date_range(date(2026, 1, 10), date(2026, 1, 13), overlap_days=0))
    assert chunks == ["20260110", "20260111", "20260112", "20260113"]


def test_chunk_date_range_multi_day_exact_overlap_boundary():
    chunks = list(backfill.chunk_date_range(date(2026, 1, 10), date(2026, 1, 12), overlap_days=2))
    assert chunks == [
        "20260108",
        "20260109",
        "20260110",
        "20260111",
        "20260112",
        "20260113",
        "20260114",
    ]


def test_chunk_date_range_start_after_end_is_empty():
    chunks = list(backfill.chunk_date_range(date(2026, 1, 13), date(2026, 1, 10)))
    assert chunks == []


def test_chunk_date_range_yields_single_day_strings_only():
    # No hyphenated-range chunks — every yielded chunk is exactly 8 digits.
    chunks = list(backfill.chunk_date_range(date(2026, 1, 1), date(2026, 1, 3), overlap_days=0))
    assert all(len(c) == 8 and c.isdigit() for c in chunks)


# --------------------------------------------------------------------------- #
# enumerate_finished_matches
# --------------------------------------------------------------------------- #


def test_enumerate_finished_matches_yields_only_terminal_matches():
    provider = FakeScheduleProvider(
        {
            "20260601": [
                nm("m1", status=MatchStatus.FINISHED),
                nm("m2", status=MatchStatus.IN_PLAY),
            ],
            "20260602": [
                nm("m3", status=MatchStatus.SCHEDULED),
                nm("m4", status=MatchStatus.FINISHED),
            ],
        }
    )
    matches = list(backfill.enumerate_finished_matches(provider, ["20260601", "20260602"]))
    assert [m.match_id for m in matches] == ["m1", "m4"]


def test_enumerate_finished_matches_calls_fetch_schedule_per_chunk():
    provider = FakeScheduleProvider({"20260601": [], "20260602": []})
    list(backfill.enumerate_finished_matches(provider, ["20260601", "20260602"]))
    assert provider.schedule_calls == ["20260601", "20260602"]


def test_enumerate_finished_matches_raises_typed_error_not_attribute_error_when_unsupported():
    # The plan does not pin an exact exception class here, only that it must
    # NOT be a bare AttributeError (an unhelpful "'X' object has no attribute
    # 'fetch_schedule'") — so this intentionally asserts the broad exception
    # type, then narrows on what it must not be.
    with pytest.raises(Exception) as exc_info:  # noqa: B017
        list(backfill.enumerate_finished_matches(NoScheduleProvider(), ["20260601"]))
    assert not isinstance(exc_info.value, AttributeError)


# --------------------------------------------------------------------------- #
# run_backfill — skip / apply / failed accounting against a fake engine double
# --------------------------------------------------------------------------- #


def test_run_backfill_counts_skip_apply_and_failed():
    provider = FakeScheduleProvider(
        {
            "20260601": [
                nm("already_stored", status=MatchStatus.FINISHED),
                nm("to_apply", status=MatchStatus.FINISHED),
                nm("apply_fails", status=MatchStatus.FINISHED),
            ]
        },
        details={"apply_fails": RuntimeError("boom")},
    )
    engine = FakeEngine(
        SOURCE,
        stored={"already_stored": (SOURCE, True)},
    )
    report = backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)
    assert report.enumerated == 3
    assert report.already_stored_skipped == 1
    assert report.applied == 1
    # ``failed`` collects failed match ids + exceptions (not a bare count —
    # BackfillReport.failed is keyed by match id per the plan's "failed match
    # ids + exceptions collected" contract).
    assert set(report.failed) == {"apply_fails"}
    assert isinstance(report.failed["apply_fails"], Exception)
    # Both non-skipped matches reached detail-fetch; only the one whose
    # fetch_match_detail succeeded reached apply_one_off_match.
    assert provider.detail_calls == ["to_apply", "apply_fails"]
    assert [m.match_id for m, _ in engine.apply_calls] == ["to_apply"]


def test_run_backfill_same_source_eventless_stored_row_is_not_skipped():
    provider = FakeScheduleProvider({"20260601": [nm("m1", status=MatchStatus.FINISHED)]})
    engine = FakeEngine(SOURCE, stored={"m1": (SOURCE, False)})
    report = backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)
    assert report.already_stored_skipped == 0
    assert report.applied == 1
    assert provider.detail_calls == ["m1"]


def test_run_backfill_different_source_stored_row_is_not_silently_skipped():
    # engine.stored_source differs from the run's source: the skip predicate
    # requires an EXACT source match, so this must not be counted as skipped.
    provider = FakeScheduleProvider({"20260601": [nm("m1", status=MatchStatus.FINISHED)]})
    engine = FakeEngine(SOURCE, stored={"m1": ("other-src", True)})
    report = backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)
    assert report.already_stored_skipped == 0
    # Not silently skipped: detail was fetched and apply was attempted.
    assert provider.detail_calls == ["m1"]
    assert len(engine.apply_calls) == 1


def test_run_backfill_detail_fetch_failure_for_one_match_does_not_stop_enumeration():
    provider = FakeScheduleProvider(
        {
            "20260601": [
                nm("bad", status=MatchStatus.FINISHED),
                nm("good", status=MatchStatus.FINISHED),
            ]
        },
        details={"bad": ConnectionError("network blip")},
    )
    engine = FakeEngine(SOURCE)
    report = backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)
    assert report.enumerated == 2
    assert set(report.failed) == {"bad"}
    assert report.applied == 1
    assert provider.detail_calls == ["bad", "good"]


def test_run_backfill_skip_check_sqlite_lock_is_recorded_as_failure_and_continues():
    # backfill runs against the same --db a live ``collect`` daemon may be
    # polling; a transient ``sqlite3.OperationalError`` from the skip-check
    # reads must be recorded per-match, not propagated out of run_backfill.
    provider = FakeScheduleProvider(
        {
            "20260601": [
                nm("locked", status=MatchStatus.FINISHED),
                nm("good", status=MatchStatus.FINISHED),
            ]
        }
    )
    engine = FakeEngine(
        SOURCE,
        stored_raises={"locked": sqlite3.OperationalError("database is locked")},
    )
    report = backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)
    assert report.enumerated == 2
    assert set(report.failed) == {"locked"}
    assert isinstance(report.failed["locked"], sqlite3.OperationalError)
    # The locked match never reached detail-fetch; the remaining match still ran.
    assert provider.detail_calls == ["good"]
    assert report.applied == 1


def test_run_backfill_apply_one_off_match_returning_none_counts_as_failed():
    provider = FakeScheduleProvider({"20260601": [nm("m1", status=MatchStatus.FINISHED)]})
    engine = FakeEngine(SOURCE, apply_results={"m1": None})
    report = backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)
    assert report.applied == 0
    assert set(report.failed) == {"m1"}


def test_run_backfill_fetch_schedule_failure_on_one_chunk_records_failed_chunk_and_continues():
    provider = FakeScheduleProvider(
        {"20260602": [nm("m1", status=MatchStatus.FINISHED)]},
        schedule_raises={"20260601": RuntimeError("upstream 500")},
    )
    engine = FakeEngine(SOURCE)
    report = backfill.run_backfill(
        engine, provider, ["20260601", "20260602"], source=SOURCE, request_delay=0.0
    )
    assert len(report.failed_chunks) == 1
    assert report.failed_chunks[0][0] == "20260601"
    assert isinstance(report.failed_chunks[0][1], RuntimeError)
    # The second, non-failing chunk was still enumerated and applied.
    assert report.enumerated == 1
    assert report.applied == 1


class FailFirstThenSucceedEngine:
    """Engine double whose ``apply_one_off_match`` fails the FIRST time it sees
    a given match_id and succeeds on every later encounter.

    Models a match re-enumerated across two adjacent PADDED date chunks (the
    overlap ``chunk_date_range`` deliberately produces): the same match can be
    applied in chunk N and again in chunk N+1. Its skip-check reports nothing
    durably stored until an apply has succeeded, so the second encounter is
    correctly NOT skipped and the retry actually runs.
    """

    def __init__(self, source: str) -> None:
        self._source = source
        self._seen: set[str] = set()

    @property
    def source(self) -> str:
        return self._source

    def stored_source_and_has_events(self, match_id: str) -> tuple[str | None, bool]:
        # The first (failed) apply leaves no durable row, so the skip-check
        # keeps reporting "not stored" and the next chunk retries the match.
        return None, False

    def apply_one_off_match(
        self, scoreboard: NormalizedMatch, detail: NormalizedMatch
    ) -> NormalizedMatch | None:
        match_id = scoreboard.match_id
        if match_id not in self._seen:
            self._seen.add(match_id)
            raise RuntimeError("transient apply failure on first encounter")
        return scoreboard


def test_run_backfill_match_that_fails_then_succeeds_is_not_reported_as_both():
    """Regression: a match re-enumerated across two adjacent padded chunks that
    FAILS in the first chunk and SUCCEEDS in the second must end up counted as
    applied only — never simultaneously in ``report.failed`` AND
    ``report.applied``. The stale ``failed[match_id]`` entry from the first
    encounter must be cleared when the retry succeeds.
    """
    provider = FakeScheduleProvider(
        {
            "20260601": [nm("m1", status=MatchStatus.FINISHED)],
            "20260602": [nm("m1", status=MatchStatus.FINISHED)],
        }
    )
    engine = FailFirstThenSucceedEngine(SOURCE)
    report = backfill.run_backfill(
        engine, provider, ["20260601", "20260602"], source=SOURCE, request_delay=0.0
    )
    # The match ultimately succeeded: it must be counted as applied and must
    # NOT linger in ``failed`` from its first-chunk failure.
    assert report.applied == 1
    assert "m1" not in report.failed
    assert report.failed == {}


def test_run_backfill_source_mismatch_raises_before_any_enumeration_or_fetch():
    provider = FakeScheduleProvider({"20260601": [nm("m1", status=MatchStatus.FINISHED)]})
    engine = FakeEngine("engine-owned-source")
    with pytest.raises(ValueError):
        backfill.run_backfill(
            engine, provider, ["20260601"], source="mismatched-source", request_delay=0.0
        )
    assert provider.schedule_calls == []
    assert provider.detail_calls == []
    assert engine.apply_calls == []


def test_run_backfill_cross_partition_error_propagates_uncaught_not_swallowed_into_failed():
    provider = FakeScheduleProvider({"20260601": [nm("m1", status=MatchStatus.FINISHED)]})
    engine = FakeEngine(
        SOURCE, apply_raises={"m1": CrossPartitionError("m1 owned by another source")}
    )
    with pytest.raises(CrossPartitionError):
        backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)


def test_run_backfill_side_table_failure_is_retried_not_skipped(tmp_path):
    """Finding 1 regression: a ``persist_side_tables`` failure must not strand
    side-table data with no retry path.

    ``apply_one_off_match`` writes events LAST (``side_tables_first``), so a
    first-run side-table failure leaves NO events stored — the skip-check's
    ``has_events()`` proxy then correctly reports "not fully stored" and a
    SECOND ``run_backfill`` retries the match (re-running the side-table
    persist to success) instead of silently skipping it as already-stored and
    leaving its stats/lineups/venue lost forever.
    """

    side_ddl = (
        "CREATE TABLE IF NOT EXISTS bf_side ("
        "source TEXT NOT NULL, match_id TEXT NOT NULL, val TEXT, "
        "PRIMARY KEY (source, match_id));",
    )
    persist_calls = {"n": 0}

    def flaky_persist(conn, writer, match: NormalizedMatch, seeded_id: str) -> None:
        persist_calls["n"] += 1
        if persist_calls["n"] == 1:
            raise RuntimeError("transient side-table write failure")
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO bf_side (source, match_id, val) VALUES (?, ?, ?)",
                (writer.source, seeded_id, "persisted"),
            )

    pack = SportPack(
        name="backfill-sidetable-test",
        sport="football",
        provider_factory=NullProvider,
        taxonomy=TAXONOMY,
        prompt_fragments={"x": "y"},
        preference_schema={"type": "object"},
        display_metadata={"sport_display": "Football"},
        compaction_boundaries=["kickoff"],
        side_table_ddl=side_ddl,
        persist_side_tables=flaky_persist,
    )
    finished = nm("m1", (ev(0, "goal"),), status=MatchStatus.FINISHED)
    provider = FakeScheduleProvider({"20260601": [finished]}, details={"m1": finished})
    engine = CollectorEngine(pack, str(tmp_path / "bf.db"), SOURCE, 30.0, provider=NullProvider())
    try:
        # Run 1: side-table persist fails → events NOT stored (side_tables_first),
        # match recorded as failed, side table empty.
        r1 = backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)
        assert set(r1.failed) == {"m1"}
        assert engine.has_events("m1") is False, (
            "events must not land when the side-table persist fails, so the skip-check "
            "proxy correctly reports the match as not fully stored"
        )
        assert engine._conn.execute("SELECT COUNT(*) FROM bf_side").fetchone()[0] == 0

        # Run 2: the eventless/side-tableless row must NOT be skipped; the retry
        # persists both the side table AND the events.
        r2 = backfill.run_backfill(engine, provider, ["20260601"], source=SOURCE, request_delay=0.0)
        assert r2.already_stored_skipped == 0, "a match with stranded side tables must be retried"
        assert r2.applied == 1
        assert engine.has_events("m1") is True
        assert engine._conn.execute("SELECT val FROM bf_side").fetchone()[0] == "persisted"
    finally:
        engine.close()


# --------------------------------------------------------------------------- #
# CollectorEngine.apply_one_off_match — real engine, existing write path only
# --------------------------------------------------------------------------- #


def test_apply_one_off_match_writes_through_existing_path(tmp_path):
    engine = make_engine(tmp_path)
    scoreboard = nm("m1", status=MatchStatus.IN_PLAY, minute=45)
    detail = nm("m1", (ev(0, "goal"), ev(1, "yellow")), status=MatchStatus.FINISHED)

    result = engine.apply_one_off_match(scoreboard, detail)

    assert result is not None
    conn = sqlite3.connect(str(tmp_path / "backfill.db"))
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT seq, type FROM events WHERE match_id = ? ORDER BY seq",
            ("m1",),
        ).fetchall()
    finally:
        conn.close()
    assert [(r["seq"], r["type"]) for r in rows] == [(0, "goal"), (1, "yellow")]


def test_apply_one_off_match_leaves_poll_loop_only_containers_untouched(tmp_path):
    engine = make_engine(tmp_path)
    scoreboard = nm("m1", status=MatchStatus.IN_PLAY, minute=45)
    detail = nm("m1", (ev(0, "goal"),), status=MatchStatus.FINISHED)

    engine.apply_one_off_match(scoreboard, detail)

    assert engine._transitions == {}
    assert engine._backfill == {}
    assert engine._backfill_apply_pending == set()


def test_apply_one_off_match_diffs_against_last_none_not_a_running_diff(tmp_path, monkeypatch):
    engine = make_engine(tmp_path)
    scoreboard = nm("m1", status=MatchStatus.IN_PLAY, minute=45)
    detail = nm("m1", (ev(0, "goal"),), status=MatchStatus.FINISHED)

    calls: list[tuple] = []
    real_diff_match = diffing.diff_match

    def spy_diff_match(current, last):
        calls.append((current, last))
        return real_diff_match(current, last)

    monkeypatch.setattr("gamecollect.engine.diff_match", spy_diff_match)

    engine.apply_one_off_match(scoreboard, detail)

    assert len(calls) == 1
    _current, last = calls[0]
    assert last is None


def test_apply_one_off_match_merges_scoreboard_identity_when_detail_omits_it(tmp_path):
    # Proves apply_one_off_match actually runs _merge_detail (not a raw detail
    # write): a detail response omitting home_team must not lose the
    # scoreboard's identity value — default_seed_match persists home_team into
    # matches.payload, so that JSON column is where the merge is observable.
    from gamecollect.db import reader

    engine = make_engine(tmp_path)
    scoreboard = nm("m1", status=MatchStatus.IN_PLAY, minute=45, home_team="Canada")
    detail = nm("m1", (ev(0, "goal"),), status=MatchStatus.FINISHED, home_team=None)

    engine.apply_one_off_match(scoreboard, detail)

    conn = sqlite3.connect(str(tmp_path / "backfill.db"))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT payload FROM matches WHERE match_id = ?", ("m1",)).fetchone()
        payload = reader.decode_payload(row["payload"])
    finally:
        conn.close()
    assert payload.get("home_team") == "Canada"


def test_apply_one_off_match_second_source_raises_cross_partition_error(tmp_path):
    db = tmp_path / "shared.db"
    engine_a = CollectorEngine(
        make_pack(NullProvider()), str(db), "source-a", 30.0, provider=NullProvider()
    )
    scoreboard_a = nm("m1", status=MatchStatus.IN_PLAY, minute=45)
    detail_a = nm("m1", (ev(0, "goal"),), status=MatchStatus.FINISHED)
    engine_a.apply_one_off_match(scoreboard_a, detail_a)

    engine_b = CollectorEngine(
        make_pack(NullProvider()), str(db), "source-b", 30.0, provider=NullProvider()
    )
    scoreboard_b = nm("m1", status=MatchStatus.IN_PLAY, minute=45)
    detail_b = nm("m1", (ev(0, "goal"),), status=MatchStatus.FINISHED)

    with pytest.raises(CrossPartitionError):
        engine_b.apply_one_off_match(scoreboard_b, detail_b)
