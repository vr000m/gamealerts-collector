#!/usr/bin/env python3
"""Smoke-test the GameWorker read contract end-to-end, outside pytest.

Seeds a temporary collector-owned SQLite file from the same checked-in football
knockout fixture and write path used by tests/test_integration_shared_file.py,
then drives every gamecollect.readport.MatchReadPort method plus the
vocabulary op against it, printing PASS/FAIL per check. Intended for
gamealerts developers to sanity-check the read contract against a real
seeded database without needing the collector's pytest/test-fixture tooling
wired up.

Usage::

    uv run python scripts/smoke_gameworker_contract.py
    uv run python scripts/smoke_gameworker_contract.py --keep-db

Exit code is 0 if every check passes, 1 otherwise (including an unhandled
seeding error).
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tests" / "football"))

from _football_helpers import KNOCKOUT_FIXTURES, replay_fixture  # noqa: E402

from gamecollect.client import get_vocabulary  # noqa: E402
from gamecollect.db.connection import connect  # noqa: E402
from gamecollect.db.writer import PartitionWriter  # noqa: E402
from gamecollect.engine import _event_to_row  # noqa: E402
from gamecollect_football.pack import (  # noqa: E402
    FOOTBALL_SIDE_TABLE_DDL,
    persist_football_side_tables,
)
from gamecollect_football.readport import FootballReadPort  # noqa: E402

SOURCE = "wc2026"
PACK_NAME = "football-wc2026"
FIXTURE = KNOCKOUT_FIXTURES[2]  # spain_portugal_760506: full lineups, one goal, venue
MATCH_ID = FIXTURE["match_id"]
EXPECTED_SCORER = "Mikel Merino"
EXPECTED_PARTICIPANTS = {"Portugal", "Spain"}
EXPECTED_VENUE = {"stadium": "AT&T Stadium", "city": "Arlington"}

Check = Callable[[], tuple[bool, str]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="don't delete the temp DB directory on exit (path is printed either way)",
    )
    return parser


def _seed(conn: Any) -> None:
    """Replay the fixture through the real ESPN adapter, then write it via the
    real collector write path (PartitionWriter + persist_football_side_tables) —
    the same recipe tests/test_integration_shared_file.py uses to mirror what
    the engine writes per poll."""
    match = replay_fixture(FIXTURE["summary"], MATCH_ID)
    writer = PartitionWriter(conn, SOURCE)
    status = match.status.value if hasattr(match.status, "value") else str(match.status)
    payload = dict(match.payload)
    payload["home_team"] = match.home_team
    payload["away_team"] = match.away_team
    writer.upsert_match(
        {
            "match_id": MATCH_ID,
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
        writer.append_events(MATCH_ID, [_event_to_row(ev) for ev in match.events])
    persist_football_side_tables(conn, writer, match, MATCH_ID)
    conn.commit()


def _goal_family_types(vocabulary: Any) -> set[str]:
    return {
        event_type
        for event_type, entry in vocabulary.taxonomy.items()
        if "goal" in entry.display_name.lower()
    }


def _run_check(name: str, fn: Check) -> tuple[str, bool, str]:
    try:
        ok, detail = fn()
    except Exception as exc:
        return name, False, f"raised {exc!r}"
    return name, ok, detail


def build_checks(adapter: FootballReadPort, conn: Any) -> list[tuple[str, Check]]:
    def vocabulary_check() -> tuple[bool, str]:
        vocabulary = get_vocabulary(conn, PACK_NAME)
        goal_types = _goal_family_types(vocabulary)
        return bool(goal_types), f"goal_types={sorted(goal_types)!r}"

    def events_check() -> tuple[bool, str]:
        goal_types = _goal_family_types(get_vocabulary(conn, PACK_NAME))
        events = adapter.events_for_match(MATCH_ID)
        scorers = [e["player"] for e in events if e["type"] in goal_types]
        return scorers == [EXPECTED_SCORER], f"scorers={scorers!r}"

    def lineup_check() -> tuple[bool, str]:
        lineup = adapter.lineup_for_match(MATCH_ID)
        participants = {row["participant"] for row in lineup}
        has_scorer = any(row["player"] == EXPECTED_SCORER for row in lineup)
        ok = bool(lineup) and participants == EXPECTED_PARTICIPANTS and has_scorer
        return ok, f"participants={participants!r} has_scorer={has_scorer}"

    def venue_check() -> tuple[bool, str]:
        venue = adapter.venue_for_match(MATCH_ID)
        return venue == EXPECTED_VENUE, f"venue={venue!r}"

    def list_matches_check() -> tuple[bool, str]:
        results = adapter.list_matches()
        by_id = {r["match_id"]: r for r in results}
        found = MATCH_ID in by_id
        participants = {p["name"] for p in by_id.get(MATCH_ID, {}).get("participants", [])}
        ok = found and participants == EXPECTED_PARTICIPANTS
        return ok, f"found={found} participants={participants!r}"

    def list_matches_status_filter_check() -> tuple[bool, str]:
        status = adapter.list_matches()[0]["status"] if adapter.list_matches() else None
        filtered = adapter.list_matches(status=status) if status else []
        ok = bool(filtered) and all(r["status"] == status for r in filtered)
        return ok, f"status={status!r} count={len(filtered)}"

    def latest_state_check() -> tuple[bool, str]:
        state = adapter.latest_state(MATCH_ID)
        keys = sorted(state) if isinstance(state, dict) else state
        return isinstance(state, dict), f"keys={keys!r}"

    def events_since_check() -> tuple[bool, str]:
        events = adapter.events_for_match(MATCH_ID, since_seq=-1)
        count = len(events) if isinstance(events, list) else events
        return isinstance(events, list), f"count={count!r}"

    def live_lineup_check() -> tuple[bool, str]:
        live = adapter.lineup_for_match_live(MATCH_ID)
        count = len(live) if isinstance(live, list) else live
        return isinstance(live, list), f"count={count!r}"

    def roster_check() -> tuple[bool, str]:
        roster = adapter.roster_for_team("Spain")
        count = len(roster) if isinstance(roster, list) else roster
        return isinstance(roster, list), f"count={count!r}"

    def roster_contains_check() -> tuple[bool, str]:
        result = adapter.roster_contains("Spain", EXPECTED_SCORER)
        return isinstance(result, bool), f"result={result!r}"

    def player_in_lineup_check() -> tuple[bool, str]:
        player = adapter.player_in_lineup(MATCH_ID, EXPECTED_SCORER)
        return player is not None, f"player={player!r}"

    def lineup_announced_check() -> tuple[bool, str]:
        announced = adapter.lineup_team_announced(MATCH_ID, "Spain")
        return isinstance(announced, bool), f"result={announced!r}"

    def commentary_check() -> tuple[bool, str]:
        commentary = adapter.recent_commentary(MATCH_ID)
        count = len(commentary) if isinstance(commentary, list) else commentary
        return isinstance(commentary, list), f"count={count!r}"

    return [
        ("vocabulary: taxonomy declares goal-family event type(s)", vocabulary_check),
        ("events_for_match: scorer matches fixture", events_check),
        ("lineup_for_match: both teams announced, includes scorer", lineup_check),
        ("venue_for_match: matches fixture", venue_check),
        ("list_matches: finds the fixture with canonical participants", list_matches_check),
        ("list_matches(status=...): filter scopes results", list_matches_status_filter_check),
        ("latest_state: returns a dict", latest_state_check),
        ("events_for_match(since_seq=-1): returns a list", events_since_check),
        ("lineup_for_match_live: returns a list", live_lineup_check),
        ("roster_for_team: returns a list", roster_check),
        ("roster_contains: returns a bool", roster_contains_check),
        ("player_in_lineup: finds the fixture's scorer", player_in_lineup_check),
        ("lineup_team_announced: returns a bool", lineup_announced_check),
        ("recent_commentary: returns a list", commentary_check),
    ]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    tmp_dir = Path(tempfile.mkdtemp(prefix="gameworker-smoke-"))
    db_path = tmp_dir / "gamecollect.db"
    print(f"seeding {db_path} from fixture {FIXTURE['summary']!r} ({FIXTURE['scenario']})")

    try:
        conn = connect(db_path, side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
        try:
            _seed(conn)
            adapter = FootballReadPort(conn)
            checks = build_checks(adapter, conn)
            results = [_run_check(name, fn) for name, fn in checks]
        finally:
            conn.close()
    except Exception as exc:
        print(f"error: seeding failed: {exc}", file=sys.stderr)
        if not args.keep_db:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return 1

    failed = 0
    for name, ok, detail in results:
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {name} - {detail}")
        if not ok:
            failed += 1

    print(f"\n{len(results) - failed}/{len(results)} checks passed")

    if args.keep_db:
        print(f"db kept at: {db_path}")
    else:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
