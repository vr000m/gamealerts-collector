"""Phase 4: ``scripts/import_gamealerts_fixtures.py`` — gamealerts DB -> fixtures.

Plan contract (docs/dev_plans/20260702-feature-collector-interfaces.md §Phase 4):

- The importer reads a gamealerts-schema SQLite file (BOTH known shapes: with
  and without ``rosters.player_name_folded`` and a ``lineups`` table), maps its
  events onto the collector fixture shape via Phase 1's ``fixture_io``, and
  re-asserts the ground-truth snapshot (event counts, scores, status) against the
  source DB, failing loudly on mismatch.
- The event-field mapping is pinned: gamealerts
  ``(match_id, seq, minute, type, detail, team, player, assist)`` -> the
  collector event shape. ``type`` maps to the football taxonomy key, ``importance``
  is derived from the pack taxonomy, minute/detail/team/player/assist are
  preserved; every distinct gamealerts ``type`` must have a taxonomy entry.
- ``--check <dir>`` validates existing fixtures parse and replay deterministically.
- CI never touches ``~/.local/share/gamealerts/``.

Two seams are exercised, matching how the importer is actually built:

* The **CLI** does the one-time live-DB seed import (``--real-db``/``--fictional-db``,
  not CI-safe) and ``--check`` (CI-safe) — so the ``--check`` path is driven via
  subprocess over the checked-in ``fixtures/football``.
* The **schema-tolerant read** (``import_match`` / ``read_match``) is the seam the
  two mini-DB shapes exercise: they are built from the checked-in
  ``tests/assets/build_mini_dbs.py`` helper into a temp dir and imported by id,
  so no live DB is touched and both shapes prove the with/without
  ``player_name_folded`` + ``lineups`` tolerance.

Mid-parallel the script/helper may be absent; tests skip (not fail) then.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from gamecollect.fold import fold
from gamecollect_football.taxonomy import TAXONOMY

REPO_ROOT = Path(__file__).resolve().parents[1]
IMPORTER = REPO_ROOT / "scripts" / "import_gamealerts_fixtures.py"
BUILDER = REPO_ROOT / "tests" / "assets" / "build_mini_dbs.py"
SEED_FIXTURES = REPO_ROOT / "fixtures" / "football"


# --------------------------------------------------------------------------- #
# Ground truth pinned by tests/assets/build_mini_dbs.py (kept in sync there).
# --------------------------------------------------------------------------- #

# Newer shape (rosters.player_name_folded + lineups): match ``mini:new1``,
# Alpha 1 - Beta 1 FINISHED, 4 events (kickoff, goal, goal, half_time), 2 lineups.
NEW_MATCH_ID = "mini:new1"
NEW_TEAMS = {"Alpha", "Beta"}
NEW_EVENTS = [
    (0, "kickoff", None, None),
    (1, "goal", 12, "Álvaro Núñez"),
    (2, "goal", 40, "Bob"),
    (3, "half_time", 45, None),
]

# Older shape (no folded column, no lineups): match ``mini:old1``,
# Gamma 2 - Delta 0 FINISHED, 3 events (goal, goal, full_time).
OLD_MATCH_ID = "mini:old1"
OLD_TEAMS = {"Gamma", "Delta"}
OLD_EVENTS = [
    (0, "goal", 22, "Grace"),
    (1, "goal", 55, "Grace"),
    (2, "full_time", 90, None),
]


# --------------------------------------------------------------------------- #
# Load the importer script and the mini-DB builder as importable modules
# --------------------------------------------------------------------------- #


def _load_module(path: Path, name: str):
    if not path.is_file():
        pytest.skip(f"{name} not present yet: {path} (mid-parallel build)")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Register before exec: the importer defines frozen dataclasses, and
    # dataclasses resolves ``cls.__module__`` via ``sys.modules`` during class
    # creation — an unregistered module makes that lookup None and crashes.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _importer():
    return _load_module(IMPORTER, "import_gamealerts_fixtures")


def _builder():
    return _load_module(BUILDER, "gamealerts_mini_builder")


def _build_new(tmp_path: Path) -> Path:
    return Path(_builder().build_mini_new(tmp_path / "gamealerts_mini_new.db"))


def _build_old(tmp_path: Path) -> Path:
    return Path(_builder().build_mini_old(tmp_path / "gamealerts_mini_old.db"))


def _import(db: Path, match_id: str, out_path: Path):
    """Import one match via the schema-tolerant ``import_match`` seam.

    Returns the written :class:`~gamecollect.fixture_io.Fixture` (the importer
    re-reads it before returning).
    """
    return _importer().import_match(str(db), match_id, str(out_path))


def _run_cli(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(IMPORTER), *args],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )


# --------------------------------------------------------------------------- #
# Assertions over an imported fixture
# --------------------------------------------------------------------------- #


def _event_tuples(fixture) -> list[tuple]:
    return sorted((e.seq, e.event_type, e.minute, e.player) for e in fixture.match.events)


def _assert_reasserted_ground_truth(fixture, *, teams: set[str], events: list[tuple]) -> None:
    match = fixture.match
    assert {match.home_team, match.away_team} == teams, (
        f"team identity drifted: {(match.home_team, match.away_team)} != {teams}"
    )
    assert match.status.value == "FINISHED", f"status not re-asserted FINISHED: {match.status}"
    assert _event_tuples(fixture) == sorted(events), (
        f"event stream drifted from ground truth: {_event_tuples(fixture)}"
    )
    # Score/goal-count re-assertion, orientation-independent: the number of
    # 'goal' events equals the total goals in the scoreline.
    goals = sum(1 for e in match.events if e.event_type == "goal")
    total_score = (match.score_home or 0) + (match.score_away or 0)
    assert goals == total_score, (
        f"goal-event count {goals} != scoreline total {total_score} "
        f"({match.score_home}-{match.score_away})"
    )


# --------------------------------------------------------------------------- #
# Import against BOTH synthetic gamealerts schema shapes
# --------------------------------------------------------------------------- #


def test_import_new_shape_db_with_folded_and_lineups(tmp_path):
    db = _build_new(tmp_path)
    fixture = _import(db, NEW_MATCH_ID, tmp_path / "new.json")
    _assert_reasserted_ground_truth(fixture, teams=NEW_TEAMS, events=NEW_EVENTS)
    # The newer shape carries a per-match lineups table -> the squad snapshot
    # lands in the fixture's entities slot.
    assert fixture.entities, "newer-shape import dropped the lineups squad snapshot"
    folded = {row.get("name_folded") for row in fixture.entities}
    assert fold("Álvaro Núñez") in folded, f"lineup fold missing from squad snapshot: {folded}"


def test_import_old_shape_db_without_folded_or_lineups(tmp_path):
    # Older fictional-DB shape: no rosters.player_name_folded, no lineups table.
    # The importer must tolerate the missing column/table and still emit the match
    # (with an empty squad snapshot).
    db = _build_old(tmp_path)
    fixture = _import(db, OLD_MATCH_ID, tmp_path / "old.json")
    _assert_reasserted_ground_truth(fixture, teams=OLD_TEAMS, events=OLD_EVENTS)
    assert fixture.entities == [], "older shape has no lineups; squad snapshot must be empty"


def test_importer_maps_pinned_event_fields(tmp_path):
    """The pinned gamealerts -> collector event mapping: seq order, ``type`` (as a
    taxonomy key), ``minute`` and the scorer (gamealerts ``player``) survive, and
    ``importance`` is derived from the pack taxonomy."""
    db = _build_new(tmp_path)
    fixture = _import(db, NEW_MATCH_ID, tmp_path / "map.json")

    events = sorted(fixture.match.events, key=lambda e: e.seq)
    assert [e.seq for e in events] == [0, 1, 2, 3], "seq order not preserved"
    assert [e.event_type for e in events] == ["kickoff", "goal", "goal", "half_time"], (
        "gamealerts type strings not mapped onto taxonomy keys"
    )
    assert [e.minute for e in events] == [None, 12, 40, 45], "minutes not carried through mapping"

    # Every produced type is a declared taxonomy key (the importer's type-vs-
    # taxonomy check) and importance is the taxonomy default for that type.
    for event in events:
        assert event.event_type in TAXONOMY, f"undeclared event type leaked: {event.event_type}"
        assert event.importance == TAXONOMY[event.event_type].importance_default, (
            f"importance for {event.event_type} not derived from the taxonomy: {event.importance}"
        )

    # The scorer name round-trips (folded-compared, tolerant of diacritics).
    scorers = {fold(e.player) for e in events if e.player}
    assert "nunez" in " ".join(scorers), f"scorer 'Álvaro Núñez' lost in mapping: {scorers}"


def test_undeclared_event_type_fails_the_import(tmp_path):
    """An event ``type`` with no taxonomy entry must fail the import loudly, not
    ship a fixture the writer would later reject (the type-vs-taxonomy guard)."""
    db = _build_old(tmp_path)
    # Inject an event whose type is not in the football taxonomy.
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO events (match_id, seq, minute, type, detail, team, player, assist) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (OLD_MATCH_ID, 3, 80, "var_review", "not a taxonomy type", "Gamma", "Grace", None),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(Exception) as excinfo:
        _import(db, OLD_MATCH_ID, tmp_path / "bad.json")
    assert "var_review" in str(excinfo.value) or "taxonomy" in str(excinfo.value).lower(), (
        f"undeclared-type failure did not name the offending type: {excinfo.value}"
    )


# --------------------------------------------------------------------------- #
# --check validation over the checked-in seed fixtures (CLI seam)
# --------------------------------------------------------------------------- #


def _require_importer_script() -> None:
    if not IMPORTER.is_file():
        pytest.skip(f"importer script not present yet: {IMPORTER} (mid-parallel build)")


def test_check_validates_seed_fixtures():
    _require_importer_script()
    if not SEED_FIXTURES.is_dir() or not any(SEED_FIXTURES.glob("*.json")):
        pytest.skip("seed fixtures not imported yet (mid-parallel build)")

    result = _run_cli(["--check", str(SEED_FIXTURES)])
    assert result.returncode == 0, (
        f"--check failed on {SEED_FIXTURES}: rc={result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


def test_check_reports_a_malformed_fixture(tmp_path):
    """``--check`` must fail loudly on a fixture that does not parse, not pass it
    silently — the guard the checked-in fixtures rely on."""
    _require_importer_script()
    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    (bad_dir / "broken.json").write_text("{ not valid fixture json ")

    result = _run_cli(["--check", str(bad_dir)])
    assert result.returncode != 0, (
        "--check passed a malformed fixture; it must fail loudly.\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


# --------------------------------------------------------------------------- #
# CI hermeticity: the importer test path never touches the live user DBs
# --------------------------------------------------------------------------- #


def test_mini_dbs_build_only_under_tmp(tmp_path):
    """A guard on the test itself: the synthetic DBs live under tmp_path, never
    under ~/.local/share/gamealerts (the live DBs are for the one-time seed only)."""
    new_db = _build_new(tmp_path)
    old_db = _build_old(tmp_path)
    for db in (new_db, old_db):
        assert db.is_file()
        assert str(db).startswith(str(tmp_path))
    local_share = Path.home() / ".local" / "share" / "gamealerts"
    assert not str(new_db).startswith(str(local_share))


def test_checked_in_mini_dbs_match_builder_ground_truth(tmp_path):
    """Drift guard for the committed binary assets: the checked-in mini DBs must
    import through the schema-tolerant seam and yield the same pinned ground
    truth as freshly built copies, so the binaries cannot silently diverge from
    ``build_mini_dbs.py``."""
    checked_in_new = REPO_ROOT / "tests" / "assets" / "gamealerts_mini_new.db"
    checked_in_old = REPO_ROOT / "tests" / "assets" / "gamealerts_mini_old.db"
    assert checked_in_new.is_file() and checked_in_old.is_file()

    fixture_new = _import(checked_in_new, NEW_MATCH_ID, tmp_path / "new.json")
    _assert_reasserted_ground_truth(fixture_new, teams=NEW_TEAMS, events=NEW_EVENTS)
    fixture_old = _import(checked_in_old, OLD_MATCH_ID, tmp_path / "old.json")
    _assert_reasserted_ground_truth(fixture_old, teams=OLD_TEAMS, events=OLD_EVENTS)

    # The committed binaries and fresh builder output import identically.
    rebuilt_new = _import(_build_new(tmp_path), NEW_MATCH_ID, tmp_path / "new2.json")
    rebuilt_old = _import(_build_old(tmp_path), OLD_MATCH_ID, tmp_path / "old2.json")
    assert _event_tuples(fixture_new) == _event_tuples(rebuilt_new)
    assert _event_tuples(fixture_old) == _event_tuples(rebuilt_old)
