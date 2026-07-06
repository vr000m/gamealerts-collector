"""ESPN adapter parity for the two knockout-match fixes ported from gamealerts
PR #30 (vr000m/gamealerts):

1. Extra-time / penalty-shootout status ``state`` fallback — an in-progress
   knockout match whose ``status.type.description`` is unmapped ("End of Extra
   Time", "Penalty Shootout", …) must resolve to IN_PLAY via ``status.type.state``
   instead of dropping to UNKNOWN (which would fall out of LIVE_STATUSES and
   self-reap the per-match collector).
2. Penalty-shootout score capture — ``competitor.shootoutScore`` +
   ``competitor.winner`` are read at both the scoreboard and summary normalizers,
   gated on a numeric pen score on BOTH sides, and carried in
   ``NormalizedMatch.payload`` (``score_pen_home``/``score_pen_away``/
   ``pen_winner_side``) alongside the HT scores — core columns stay
   sport-agnostic (DESIGN.md §5).
"""

from __future__ import annotations

import logging

from gamecollect.provider import MatchStatus
from gamecollect_football.espn import (
    ESPNAdapter,
    _extract_shootout,
    _pen_score,
    _status_from_comp,
    normalize_status,
)


def _comp(description=None, state=None):
    """Build a minimal competition dict carrying only a status.type."""
    type_: dict = {}
    if description is not None:
        type_["description"] = description
    if state is not None:
        type_["state"] = state
    return {"status": {"type": type_}}


# ---------------------------------------------------------------------------
# _status_from_comp: description wins, state fallback, UNKNOWN floor
# ---------------------------------------------------------------------------


class TestStatusFromComp:
    def test_mapped_description_wins_over_state(self):
        # "Half Time" → PAUSED must keep its granularity even though state="in"
        # would coarsen it to IN_PLAY.
        assert _status_from_comp(_comp("Half Time", "in")) is MatchStatus.PAUSED

    def test_extra_time_description_falls_back_to_in_play(self):
        # The exact live bug: an unmapped ET/shootout description must not drop
        # to UNKNOWN while the match is in progress.
        for desc in (
            "End of Extra Time",
            "First Half Extra Time",
            "Second Half Extra Time",
            "Penalty Shootout",
        ):
            assert normalize_status(desc) is MatchStatus.UNKNOWN  # unmapped by table
            assert _status_from_comp(_comp(desc, "in")) is MatchStatus.IN_PLAY

    def test_state_pre_and_post_fallback(self):
        assert _status_from_comp(_comp("Weird Pre Wording", "pre")) is MatchStatus.SCHEDULED
        assert _status_from_comp(_comp("Weird Post Wording", "post")) is MatchStatus.FINISHED

    def test_no_description_uses_state(self):
        assert _status_from_comp(_comp(None, "in")) is MatchStatus.IN_PLAY

    def test_both_unknown_stays_unknown(self):
        assert _status_from_comp(_comp("Nonsense", "nonsense")) is MatchStatus.UNKNOWN
        assert _status_from_comp(_comp(None, None)) is MatchStatus.UNKNOWN

    def test_missing_status_block_is_unknown(self):
        assert _status_from_comp({}) is MatchStatus.UNKNOWN
        assert _status_from_comp({"status": None}) is MatchStatus.UNKNOWN
        assert _status_from_comp({"status": {"type": None}}) is MatchStatus.UNKNOWN


# ---------------------------------------------------------------------------
# _pen_score: float (summary) / int (scoreboard) / absent / malformed
# ---------------------------------------------------------------------------


class TestPenScore:
    def test_int_scoreboard(self):
        assert _pen_score({"shootoutScore": 4}) == 4

    def test_float_summary(self):
        assert _pen_score({"shootoutScore": 4.0}) == 4

    def test_absent_is_none(self):
        assert _pen_score({}) is None
        assert _pen_score({"shootoutScore": None}) is None

    def test_bool_and_non_numeric_rejected(self):
        # bool is an int subclass — must not read True as 1.
        assert _pen_score({"shootoutScore": True}) is None
        assert _pen_score({"shootoutScore": "4"}) is None

    def test_non_integral_float_rejected_not_truncated(self):
        # int(2.9) == 2 would silently persist a wrong tally — treat as absent.
        assert _pen_score({"shootoutScore": 2.9}) is None
        assert _pen_score({"shootoutScore": 2.5}) is None

    def test_negative_rejected(self):
        assert _pen_score({"shootoutScore": -1}) is None
        assert _pen_score({"shootoutScore": -1.0}) is None

    def test_non_finite_rejected_not_raised(self):
        # int(nan) raises ValueError and int(inf) raises OverflowError — neither
        # is mapped to ShapeDriftError by the fetch wrappers, so they must be
        # rejected here rather than escaping the provider seam and crashing.
        assert _pen_score({"shootoutScore": float("nan")}) is None
        assert _pen_score({"shootoutScore": float("inf")}) is None
        assert _pen_score({"shootoutScore": float("-inf")}) is None


# ---------------------------------------------------------------------------
# _extract_shootout: both-sides gate, winner derivation, disagreement logging
# ---------------------------------------------------------------------------


def _competitors(home_pen=None, away_pen=None, home_win=False, away_win=False):
    home: dict = {"homeAway": "home", "winner": home_win}
    away: dict = {"homeAway": "away", "winner": away_win}
    if home_pen is not None:
        home["shootoutScore"] = home_pen
    if away_pen is not None:
        away["shootoutScore"] = away_pen
    return [home, away]


class TestExtractShootout:
    def test_full_shootout_away_winner(self):
        res = _extract_shootout(_competitors(2, 4, away_win=True), "760499")
        assert res == (2, 4, "away")

    def test_full_shootout_home_winner(self):
        res = _extract_shootout(_competitors(5, 3, home_win=True), "m1")
        assert res == (5, 3, "home")

    def test_one_sided_pen_score_gates_to_all_none(self):
        # A malformed single-sided shootoutScore must never emit half a result.
        assert _extract_shootout(_competitors(4, None, away_win=True), "m1") == (None, None, None)

    def test_no_shootout_is_all_none(self):
        assert _extract_shootout(_competitors(None, None, home_win=True), "m1") == (
            None,
            None,
            None,
        )

    def test_zero_winners_flagged_leaves_side_none(self, caplog):
        with caplog.at_level(logging.WARNING):
            res = _extract_shootout(_competitors(4, 2), "m1")
        assert res == (4, 2, None)
        assert "winner=True" in caplog.text

    def test_two_winners_flagged_leaves_side_none(self, caplog):
        with caplog.at_level(logging.WARNING):
            res = _extract_shootout(_competitors(4, 2, home_win=True, away_win=True), "m1")
        assert res == (4, 2, None)
        assert "winner=True" in caplog.text

    def test_winner_disagrees_with_higher_score_stores_espn_winner_and_logs(self, caplog):
        # ESPN says home won but away has the higher pen tally: store ESPN's
        # winner (home), log the disagreement.
        with caplog.at_level(logging.WARNING):
            res = _extract_shootout(_competitors(2, 4, home_win=True), "m1")
        assert res == (2, 4, "home")
        assert "disagrees with higher pen score" in caplog.text

    def test_equal_pen_scores_no_disagreement_log(self, caplog):
        # A tie is inconclusive, not a disagreement — no warning.
        with caplog.at_level(logging.WARNING):
            res = _extract_shootout(_competitors(3, 3, home_win=True), "m1")
        assert res == (3, 3, "home")
        assert "disagrees" not in caplog.text


# ---------------------------------------------------------------------------
# End-to-end through the adapter's public paths (synthetic fixtures)
# ---------------------------------------------------------------------------


def _scoreboard_shootout():
    return {
        "events": [
            {
                "id": "760499",
                "date": "2026-07-04T18:00Z",
                "season": {"slug": "round-of-16"},
                "competitions": [
                    {
                        "date": "2026-07-04T18:00Z",
                        "status": {
                            "displayClock": "120'",
                            "type": {"description": "Penalty Shootout", "state": "in"},
                        },
                        "venue": {
                            "fullName": "NRG Stadium",
                            "address": {"city": "Houston, Texas"},
                        },
                        "competitors": [
                            {
                                "homeAway": "home",
                                "score": "1",
                                "shootoutScore": 2,
                                "winner": False,
                                "team": {"displayName": "Australia"},
                            },
                            {
                                "homeAway": "away",
                                "score": "1",
                                "shootoutScore": 4,
                                "winner": True,
                                "team": {"displayName": "Egypt"},
                            },
                        ],
                    }
                ],
            }
        ]
    }


def _summary_shootout():
    return {
        "header": {
            "competitions": [
                {
                    "date": "2026-07-04T18:00Z",
                    "status": {
                        "displayClock": "120'",
                        "type": {"description": "Full Time", "state": "post"},
                    },
                    "competitors": [
                        {
                            "homeAway": "home",
                            "score": "1",
                            "winner": False,
                            "shootoutScore": 2.0,
                            "team": {"displayName": "Australia"},
                            "linescores": [{"displayValue": "0"}],
                        },
                        {
                            "homeAway": "away",
                            "score": "1",
                            "winner": True,
                            "shootoutScore": 4.0,
                            "team": {"displayName": "Egypt"},
                            "linescores": [{"displayValue": "1"}],
                        },
                    ],
                }
            ]
        },
        "boxscore": {
            "teams": [
                {
                    "team": {"displayName": "Australia"},
                    "statistics": [{"name": "possessionPct", "displayValue": "50.0"}],
                }
            ]
        },
        "keyEvents": [],
        "rosters": [],
        "commentary": [],
    }


class TestAdapterEndToEnd:
    def test_scoreboard_live_shootout_stays_live_with_pens(self):
        adapter = ESPNAdapter(http_get=lambda url, params=None: _scoreboard_shootout())
        (match,) = adapter.fetch_live_matches()
        # State fallback keeps the shootout live rather than dropping to UNKNOWN.
        assert match.status is MatchStatus.IN_PLAY
        assert match.payload["score_pen_home"] == 2
        assert match.payload["score_pen_away"] == 4
        assert match.payload["pen_winner_side"] == "away"

    def test_summary_finished_shootout_carries_pens(self):
        adapter = ESPNAdapter(http_get=lambda url, params=None: _summary_shootout())
        match = adapter.fetch_match_detail("760499")
        assert match.status is MatchStatus.FINISHED
        assert match.payload["score_pen_home"] == 2  # 2.0 float → int
        assert match.payload["score_pen_away"] == 4
        assert match.payload["pen_winner_side"] == "away"

    def test_malformed_pen_score_degrades_without_crashing_the_seam(self):
        # A non-finite shootoutScore must not escape as ValueError/OverflowError
        # through the fetch wrapper; the both-sides gate suppresses the shootout.
        data = _summary_shootout()
        data["header"]["competitions"][0]["competitors"][0]["shootoutScore"] = float("nan")
        adapter = ESPNAdapter(http_get=lambda url, params=None: data)
        match = adapter.fetch_match_detail("760499")  # must not raise
        assert match.payload["score_pen_home"] is None
        assert match.payload["score_pen_away"] is None
        assert match.payload["pen_winner_side"] is None

    def test_non_shootout_summary_emits_none_pen_keys(self):
        data = _summary_shootout()
        for c in data["header"]["competitions"][0]["competitors"]:
            c.pop("shootoutScore")
            c["winner"] = False
        adapter = ESPNAdapter(http_get=lambda url, params=None: data)
        match = adapter.fetch_match_detail("760499")
        assert match.payload["score_pen_home"] is None
        assert match.payload["score_pen_away"] is None
        assert match.payload["pen_winner_side"] is None
