"""Phase 2: durable venue + squad-roster side tables.

Plan (docs/dev_plans/20260707-feature-gameworker-integration.md, Phase 2):
- ESPN scoreboard/summary venue data (``payload["stadium"]``/``payload["city"]``,
  already carried on ``NormalizedMatch.payload`` per espn.py:918-943) must land
  durably in a football-owned venue side table, not just the transient payload.
- Team-level squad rosters sourced from the static
  ``src/gamecollect_football/fixtures/worldcup.squads.json`` fixture must land
  in a football-owned roster side table, distinct from the existing per-match
  ``football_lineups`` table.

Table/loader names are an implementation choice made by a parallel phase-2
implementer, so these tests discover the actual table name from
``FOOTBALL_SIDE_TABLE_DDL`` (documented naming convention: a table whose name
contains "venue"/"roster") and the loader callable from the pack module's
public surface, rather than hard-coding a guess. A test skips with a clear
reason if its hook cannot be located, instead of failing on a wrong guess.
"""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path

import pytest

import gamecollect_football
from gamecollect.db.connection import connect
from gamecollect.db.writer import PartitionWriter
from gamecollect.provider import MatchStatus, NormalizedMatch
from gamecollect_football import pack as pack_module
from gamecollect_football.pack import FOOTBALL_SIDE_TABLE_DDL, persist_football_side_tables

SOURCE = "wc2026"
MATCH_ID = "wc2026:760421"


def _find_table_name(keyword: str) -> str | None:
    """Search the pack's side-table DDL for a ``football_*<keyword>*`` table."""
    pattern = re.compile(rf"CREATE TABLE IF NOT EXISTS\s+(football_\w*{keyword}\w*)", re.IGNORECASE)
    for ddl in FOOTBALL_SIDE_TABLE_DDL:
        m = pattern.search(ddl)
        if m:
            return m.group(1)
    return None


def _find_roster_loader():
    """Locate the callable that wires ``worldcup.squads.json`` into the DB.

    Looks for a public, non-dunder callable on the pack module whose name
    mentions "squad" or "roster" and is not the pre-existing per-match
    ``get_squad`` operation (which reads ``football_lineups``, not the new
    team-level roster table).
    """
    candidates = []
    for name, obj in vars(pack_module).items():
        if name.startswith("_") or not callable(obj):
            continue
        if name in {"persist_football_side_tables", "pack"}:
            continue
        if "squad" in name.lower() or "roster" in name.lower():
            candidates.append(obj)
    return candidates[0] if candidates else None


def _match(payload: dict) -> NormalizedMatch:
    return NormalizedMatch(
        match_id="760421",
        status=MatchStatus.IN_PLAY,
        minute=27,
        score_home=1,
        score_away=0,
        display_clock="27'",
        home_team="Morocco",
        away_team="Haiti",
        kickoff_utc="2026-06-14T04:00:00+00:00",
        payload=payload,
    )


VENUE_PAYLOAD = {
    "stadium": "NRG Stadium",
    "city": "Houston",
}


@pytest.fixture
def db(tmp_path):
    conn = connect(tmp_path / "side.db", side_table_ddl=FOOTBALL_SIDE_TABLE_DDL)
    writer = PartitionWriter(conn, SOURCE)
    yield conn, writer
    conn.close()


class TestVenueSideTable:
    def test_venue_table_exists_in_side_table_ddl(self):
        table = _find_table_name("venue")
        assert table is not None, (
            "no football_*venue* table found in FOOTBALL_SIDE_TABLE_DDL — "
            "Phase 2 must add a venue side table following the "
            "football_stats/football_lineups pattern (pack.py:38-76)"
        )

    def test_summary_poll_venue_lands_durably(self, db):
        table = _find_table_name("venue")
        if table is None:
            pytest.skip("no football_*venue* side table wired yet")
        conn, writer = db
        persist_football_side_tables(conn, writer, _match(VENUE_PAYLOAD), MATCH_ID)

        rows = conn.execute(
            f"SELECT * FROM {table} WHERE match_id = ?",
            (MATCH_ID,),  # noqa: S608
        ).fetchall()
        assert rows, (
            f"expected a durable row in {table} for {MATCH_ID} after a summary/"
            "scoreboard poll carrying payload['stadium']/payload['city']"
        )
        row = dict(rows[0])
        values = {str(v) for v in row.values() if v is not None}
        assert "NRG Stadium" in values, f"{table} row does not carry the stadium name: {row}"
        assert "Houston" in values, f"{table} row does not carry the city: {row}"

    def test_venue_persists_across_second_poll_with_same_payload(self, db):
        table = _find_table_name("venue")
        if table is None:
            pytest.skip("no football_*venue* side table wired yet")
        conn, writer = db
        persist_football_side_tables(conn, writer, _match(VENUE_PAYLOAD), MATCH_ID)
        persist_football_side_tables(conn, writer, _match(VENUE_PAYLOAD), MATCH_ID)

        (count,) = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE match_id = ?",
            (MATCH_ID,),  # noqa: S608
        ).fetchone()
        assert count == 1, f"a repeated identical venue payload must not duplicate rows in {table}"

    def test_missing_venue_payload_leaves_table_untouched(self, db):
        table = _find_table_name("venue")
        if table is None:
            pytest.skip("no football_*venue* side table wired yet")
        conn, writer = db
        persist_football_side_tables(
            conn, writer, _match({"stadium": None, "city": None}), MATCH_ID
        )
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE match_id = ?",
            (MATCH_ID,),  # noqa: S608
        ).fetchall()
        assert rows == [], "an absent stadium/city must not insert a placeholder venue row"

    def test_malformed_stadium_value_does_not_wipe_a_stored_venue_row(self, db):
        """Regression: the guard before calling _replace_if_changed used to
        check the RAW stadium/city values (`is not None`), not the projected
        _venue_rows() result. _venue_rows runs each value through _to_text,
        which coerces a non-scalar (e.g. a dict) to None. A malformed-but-
        non-None stadium value used to pass the old raw-value guard while
        _venue_rows produced an EMPTY dict, and _replace_if_changed(desired={})
        against a non-empty `current` performs an unconditional DELETE --
        wiping a previously-good stored venue row over a single bad poll."""
        table = _find_table_name("venue")
        if table is None:
            pytest.skip("no football_*venue* side table wired yet")
        conn, writer = db
        # First poll: a good venue payload lands durably.
        persist_football_side_tables(conn, writer, _match(VENUE_PAYLOAD), MATCH_ID)
        (count_before,) = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE match_id = ?",  # noqa: S608
            (MATCH_ID,),
        ).fetchone()
        assert count_before == 1

        # Second poll: a malformed, non-None stadium value (a dict, not a
        # scalar) -- must not wipe the already-stored good row.
        malformed_payload = {"stadium": {"unexpected": "shape"}, "city": {"also": "bad"}}
        persist_football_side_tables(conn, writer, _match(malformed_payload), MATCH_ID)

        rows = conn.execute(
            f"SELECT * FROM {table} WHERE match_id = ?",  # noqa: S608
            (MATCH_ID,),
        ).fetchall()
        assert rows, (
            f"a malformed stadium/city payload must not wipe the previously-stored "
            f"venue row in {table}"
        )
        row = dict(rows[0])
        values = {str(v) for v in row.values() if v is not None}
        assert "NRG Stadium" in values, f"stored venue row was wiped: {row}"


class TestRosterSideTable:
    def test_roster_falls_back_to_stored_schedule_team_names(self, db):
        """Regression: when the CURRENT poll's snapshot carries neither
        home_team nor away_team (e.g. a summary response whose
        header.competitions was empty/malformed, so the ESPN adapter never
        set NormalizedMatch.home_team/away_team even though the same
        response's rosters/lineups are otherwise intact), the roster persist
        calls must not silently no-op if the canonical schedule row already
        has team names on its stored payload."""
        roster_table = _find_table_name("roster")
        if roster_table is None:
            pytest.skip("no football_*roster* side table wired yet")
        conn, writer = db
        # Seed the canonical schedule row's stored payload with team names
        # (mirrors what reconcile.py's seed path writes for a resolved
        # canonical row -- schedule-owned home_team/away_team).
        writer.upsert_match(
            {
                "match_id": MATCH_ID,
                "source": SOURCE,
                "status": "IN_PLAY",
                "kickoff_utc": "2026-06-14T04:00:00+00:00",
                "score_home": 1,
                "score_away": 0,
                "display_clock": "27'",
                "payload": {"home_team": "Morocco", "away_team": "Haiti"},
            }
        )
        conn.commit()

        # This poll's NormalizedMatch carries NO home_team/away_team.
        match = NormalizedMatch(
            match_id="760421",
            status=MatchStatus.IN_PLAY,
            minute=27,
            score_home=1,
            score_away=0,
            display_clock="27'",
            home_team=None,
            away_team=None,
            kickoff_utc="2026-06-14T04:00:00+00:00",
            payload={},
        )
        persist_football_side_tables(conn, writer, match, MATCH_ID)

        (count,) = conn.execute(
            f"SELECT COUNT(*) FROM {roster_table} WHERE team IN (?, ?)",  # noqa: S608
            ("Morocco", "Haiti"),
        ).fetchone()
        assert count > 0, (
            "expected roster rows for Morocco/Haiti derived from the canonical "
            "schedule row's stored payload when the current snapshot carries no "
            "home_team/away_team"
        )

    def test_roster_table_exists_in_side_table_ddl(self):
        table = _find_table_name("roster")
        assert table is not None, (
            "no football_*roster* table found in FOOTBALL_SIDE_TABLE_DDL — "
            "Phase 2 must add a team-level roster side table distinct from "
            "football_lineups"
        )

    def test_squad_fixture_is_present_and_shaped_as_documented(self):
        """Sanity-check the plan's documented fixture shape independent of any
        loader — guards against the loader test below skipping silently
        because the fixture itself moved/changed shape."""
        fixture_path = (
            Path(gamecollect_football.__file__).parent / "fixtures" / "worldcup.squads.json"
        )
        assert fixture_path.exists(), f"expected fixture at {fixture_path}"
        squads = json.loads(fixture_path.read_text())
        assert isinstance(squads, list) and squads
        team = squads[0]
        assert {"name", "fifa_code", "group", "players"} <= team.keys()
        assert isinstance(team["players"], list) and team["players"]

    def test_roster_loader_wires_fixture_into_roster_table(self, db):
        table = _find_table_name("roster")
        if table is None:
            pytest.skip("no football_*roster* side table wired yet")
        loader = _find_roster_loader()
        if loader is None:
            pytest.skip(
                "no roster/squad-fixture loader callable found on gamecollect_football.pack "
                "(expected e.g. load_squad_fixtures(conn, writer) or persist_football_rosters)"
            )
        conn, writer = db

        sig = inspect.signature(loader)
        try:
            if len(sig.parameters) >= 2:
                loader(conn, writer)
            else:
                loader(conn)
        except TypeError as exc:
            pytest.skip(f"discovered loader {loader.__name__} has an unexpected signature: {exc}")

        (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608
        assert count > 0, (
            f"{loader.__name__} ran but {table} is still empty — the static "
            "worldcup.squads.json fixture must be wired into the roster table"
        )

    def test_roster_rows_distinct_from_lineups_table(self, db):
        """The team-level roster table must be its own table, not a rename or
        alias of the existing per-match football_lineups table."""
        roster_table = _find_table_name("roster")
        if roster_table is None:
            pytest.skip("no football_*roster* side table wired yet")
        assert roster_table != "football_lineups", (
            "roster table must be distinct from the existing per-match football_lineups table"
        )
