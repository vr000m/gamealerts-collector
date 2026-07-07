"""Regression coverage for real WC2026 knockout captures.

The group-stage qatar_canada fixture cannot exercise the knockout paths that
this branch's fixes touch. These three real R32/R16 captures do:

  * ``760489`` Germany 1-1 Paraguay — decided on penalties (shootout capture)
  * ``760500`` Argentina 3-2 Cape Verde — won in extra time, no shootout
  * ``760506`` Portugal 0-1 Spain — regulation win on a stoppage-time goal

Each is replayed through the adapter and pinned against a self-captured golden
(see ``golden/capture_knockout_goldens.py``); the full snapshot catches any
normalization drift, and the targeted asserts document the scenario invariants —
including the lineup ``home_away`` orientation the reconcile fix corrected.
"""

from __future__ import annotations

import json

import pytest
from _football_helpers import (
    KNOCKOUT_FIXTURES,
    KNOCKOUT_GOLDEN_DIR,
    replay_fixture,
    serialize_knockout,
)

_IDS = [fx["match_id"] for fx in KNOCKOUT_FIXTURES]


@pytest.fixture(params=KNOCKOUT_FIXTURES, ids=_IDS)
def knockout(request):
    fx = request.param
    match = replay_fixture(fx["summary"], fx["match_id"])
    return fx, match


class TestGoldenParity:
    def test_snapshot_equals_golden(self, knockout):
        """Full normalized snapshot must equal the frozen golden byte-for-byte."""
        fx, match = knockout
        rendered = (
            json.dumps(serialize_knockout(match), sort_keys=True, indent=2, ensure_ascii=False)
            + "\n"
        )
        assert rendered == (KNOCKOUT_GOLDEN_DIR / fx["golden"]).read_text()


class TestFixtureSanity:
    def test_teams_and_finished(self, knockout):
        fx, match = knockout
        assert match.home_team == fx["home"]
        assert match.away_team == fx["away"]
        assert match.status.value == "FINISHED"

    def test_lineup_orientation_matches_canonical_home_away(self, knockout):
        """The reconcile-fix invariant: a lineup tagged ``home``/``away`` must
        name the same team the match header calls home/away — never the
        provider's opposite orientation."""
        _fx, match = knockout
        by_side = {ln["home_away"]: ln["team"] for ln in match.payload["lineups"]}
        assert by_side.get("home") == match.home_team
        assert by_side.get("away") == match.away_team


class TestScenarioInvariants:
    """The whole point of adding these fixtures: exercise outcomes qatar_canada
    can't. Assert each scenario's distinguishing shape explicitly."""

    def test_penalty_shootout_captured(self):
        # 760489: 1-1, Paraguay (away) win the shootout 4-3.
        match = _replay("760489")
        p = match.payload
        assert (match.score_home, match.score_away) == (1, 1)
        assert (p["score_pen_home"], p["score_pen_away"]) == (3, 4)
        assert p["pen_winner_side"] == "away"

    def test_extra_time_win_has_no_shootout(self):
        # 760500: Argentina win 3-2 in extra time — no penalties taken.
        match = _replay("760500")
        p = match.payload
        assert (match.score_home, match.score_away) == (3, 2)
        assert p["score_pen_home"] is None
        assert p["score_pen_away"] is None
        assert p["pen_winner_side"] is None

    def test_regulation_win_has_no_shootout(self):
        # 760506: Spain win 0-1 in regulation (stoppage-time goal).
        match = _replay("760506")
        p = match.payload
        assert (match.score_home, match.score_away) == (0, 1)
        assert p["score_pen_home"] is None
        assert p["pen_winner_side"] is None


def _replay(match_id: str):
    fx = next(f for f in KNOCKOUT_FIXTURES if f["match_id"] == match_id)
    return replay_fixture(fx["summary"], fx["match_id"])
