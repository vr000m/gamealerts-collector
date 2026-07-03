"""Ported from gamealerts ``tests/test_espn_event_mapping.py`` (mandatory per plan).

Replays the committed capture ``qatar_canada_760440.summary.json`` (Canada 6-0
Qatar, ESPN event 760440) through the PORTED ``gamecollect_football`` adapter
and pins the normalized event oracle. Adapted for the Phase 3 generalization:
``event_type`` is a plain string (gamealerts ``EventType.GOAL`` -> ``"goal"``),
``importance`` an int.

Oracle (unchanged from gamealerts):
  * 6 goal-family events = 5 GOAL + 1 OWN_GOAL.
  * 3 RED / 3 YELLOW (each includes one from the synthetic ``yellow-red-card``
    second-yellow split), 10 SUB, 1 KICKOFF, 1 HALF_TIME — 24 events total.
  * The ``yellow-red-card`` slug expands to TWO events (the 0-2 expansion's
    upper bound); an unknown slug expands to ZERO events (the lower bound).
  * NEGATIVE: the 28 ``start-delay``/``end-delay`` slugs fabricate nothing.

The gamealerts-only end-to-end reach section (Summarizer/commentary) is NOT
ported — commentary/prose belongs to consuming apps (DESIGN.md §3); its write
path here is covered by tests/football/test_card_counts.py via PartitionWriter.
"""

from __future__ import annotations

import collections

from _football_helpers import load_raw_summary, replay


def _event_counts(match) -> collections.Counter:
    return collections.Counter(ev.event_type for ev in match.events)


# ===========================================================================
# Sanity: the fixture is the game we think it is.
# ===========================================================================


class TestFixtureSanity:
    def test_fixture_is_canada_six_nil_qatar(self):
        match = replay()
        assert {match.home_team, match.away_team} == {"Canada", "Qatar"}
        scores = {match.home_team: match.score_home, match.away_team: match.score_away}
        assert scores["Canada"] == 6
        assert scores["Qatar"] == 0


# ===========================================================================
# POSITIVE oracle — exact normalized counts (event_type as plain strings).
# ===========================================================================


class TestEventMappingOracle:
    def test_goal_family_total_is_six(self):
        counts = _event_counts(replay())
        goal_family = counts["goal"] + counts["own_goal"]
        assert goal_family == 6, (
            f"expected 6 goal-family events (5 goal + 1 own_goal); got "
            f"goal={counts['goal']} own_goal={counts['own_goal']}"
        )

    def test_five_goals(self):
        """goal (x2) + goal---volley (x2) + goal---free-kick (x1) -> 5 'goal'."""
        assert _event_counts(replay())["goal"] == 5

    def test_one_own_goal(self):
        assert _event_counts(replay())["own_goal"] == 1

    def test_three_red_cards(self):
        # 2 real red-card slugs (Qatar) + 1 from the synthetic yellow-red-card split.
        assert _event_counts(replay())["red"] == 3

    def test_three_yellow_cards(self):
        # 2 real yellows (Canada 9', Qatar 62') + 1 from the second-yellow split.
        assert _event_counts(replay())["yellow"] == 3

    def test_ten_substitutions(self):
        assert _event_counts(replay())["sub"] == 10

    def test_halftime_slug_maps_to_half_time(self):
        """ESPN sends ``halftime`` (one word); the map must carry it, yielding
        exactly one 'half_time' event."""
        assert _event_counts(replay())["half_time"] == 1

    def test_full_event_count_matches_oracle(self):
        counts = _event_counts(replay())
        expected = {
            "goal": 5,
            "own_goal": 1,
            "red": 3,
            "yellow": 3,
            "sub": 10,
            "kickoff": 1,
            "half_time": 1,
        }
        assert dict(counts) == expected, f"event family breakdown drifted: {dict(counts)}"
        assert sum(counts.values()) == 24


# ===========================================================================
# The 0–2-event expansion of _normalize_key_event, pinned at both bounds.
# ===========================================================================


def _key_event(slug: str, minute: str = "78'") -> dict:
    return {
        "type": {"type": slug},
        "clock": {"displayValue": minute},
        "team": {"displayName": "Canada"},
        "participants": [{"athlete": {"displayName": "Derek Cornelius"}}],
        "text": f"synthetic {slug} event",
    }


class TestZeroToTwoEventExpansion:
    """Explicit unit coverage of ``_normalize_key_event``'s 0-, 1- and 2-event
    outputs (the plan's Review Focus: the expansion must survive the port)."""

    def test_second_yellow_slug_expands_to_two_events(self):
        from gamecollect_football.espn import _normalize_key_event

        out = _normalize_key_event(_key_event("yellow-red-card"))
        assert len(out) == 2, f"yellow-red-card must expand to TWO events, got {out}"
        assert [ev.event_type for ev in out] == ["yellow", "red"]
        assert all(ev.team == "Canada" for ev in out)
        assert all(ev.minute == 78 for ev in out)

    def test_unknown_slug_expands_to_zero_events(self):
        from gamecollect_football.espn import _normalize_key_event

        assert _normalize_key_event(_key_event("some-future-espn-slug")) == []

    def test_delay_slugs_expand_to_zero_events(self):
        from gamecollect_football.espn import _normalize_key_event

        assert _normalize_key_event(_key_event("start-delay")) == []
        assert _normalize_key_event(_key_event("end-delay")) == []

    def test_ordinary_slug_expands_to_one_event(self):
        from gamecollect_football.espn import _normalize_key_event

        out = _normalize_key_event(_key_event("yellow-card"))
        assert len(out) == 1
        assert out[0].event_type == "yellow"


# ===========================================================================
# Second-yellow → sending-off through the full fixture replay.
# ===========================================================================


class TestSecondYellowSplit:
    _SLUG = "yellow-red-card"

    def test_fixture_carries_exactly_one_yellow_red_card_slug(self):
        raw = load_raw_summary()
        kev = [
            p for p in raw.get("keyEvents", []) if (p.get("type") or {}).get("type") == self._SLUG
        ]
        assert len(kev) == 1, f"expected exactly one {self._SLUG} keyEvent, got {len(kev)}"
        assert kev[0].get("_test_data") is True, "synthetic event must be marked _test_data"

    def test_slug_splits_into_one_yellow_and_one_red(self):
        match = replay()
        cornelius_cards = [
            ev
            for ev in match.events
            if ev.event_type in ("yellow", "red")
            and ev.minute == 78
            and "cornelius" in (ev.player or "").lower()
        ]
        types = collections.Counter(ev.event_type for ev in cornelius_cards)
        assert types["yellow"] == 1, f"missing the second yellow at 78': {cornelius_cards}"
        assert types["red"] == 1, f"missing the red at 78': {cornelius_cards}"
        assert {ev.team for ev in cornelius_cards} == {"Canada"}

    def test_second_yellow_detail_is_tagged(self):
        match = replay()
        second_yellows = [
            ev
            for ev in match.events
            if ev.event_type == "yellow"
            and ev.minute == 78
            and "second yellow" in (ev.detail or "").lower()
        ]
        assert len(second_yellows) == 1, (
            f"expected one detail-tagged second yellow at 78'; got {second_yellows}"
        )

    def test_seq_values_are_contiguous_and_unique_after_expansion(self):
        """``seq`` is assigned AFTER the split (provider-derived, per the plan's
        seq contract), so the 24 events carry exactly 0..23."""
        match = replay()
        seqs = [ev.seq for ev in match.events]
        assert len(seqs) == 24
        assert seqs == list(range(24)), f"seq not contiguous 0..23 after expansion: {seqs}"

    def test_split_yellow_and_red_get_distinct_consecutive_seqs(self):
        match = replay()
        yellow = next(
            ev
            for ev in match.events
            if ev.event_type == "yellow"
            and ev.minute == 78
            and "second yellow" in (ev.detail or "").lower()
        )
        red = next(
            ev
            for ev in match.events
            if ev.event_type == "red"
            and ev.minute == 78
            and "cornelius" in (ev.player or "").lower()
        )
        assert yellow.seq != red.seq, "split yellow and red must not share a seq"
        assert red.seq == yellow.seq + 1, "split red must immediately follow the yellow"


# ===========================================================================
# NEGATIVE oracle — no fabricated cooling/hydration/break events.
# ===========================================================================


class TestNoFabricatedBreakEvents:
    def test_delay_slugs_are_present_in_the_raw_capture(self):
        raw = load_raw_summary()
        slugs = collections.Counter(
            (p.get("type") or {}).get("type") for p in raw.get("keyEvents", [])
        )
        assert slugs["start-delay"] == 14
        assert slugs["end-delay"] == 14

    def test_no_break_type_in_the_pack_taxonomy(self, football_pack):
        """There is deliberately NO cooling/hydration/break/delay key in the
        pack taxonomy — the delay slugs cannot map to one."""
        forbidden = {"cooling", "cooling_break", "hydration", "break", "delay"}
        declared = {key.lower() for key in football_pack.taxonomy}
        assert not (declared & forbidden), (
            f"a break/cooling event type was introduced ({declared & forbidden}); the "
            f"'no fabricated events' constraint must be re-audited"
        )

    def test_normalized_event_count_excludes_all_delay_slugs(self):
        match = replay()
        assert len(match.events) == 24
        produced_types = {ev.event_type for ev in match.events}
        allowed = {"goal", "own_goal", "red", "yellow", "sub", "kickoff", "half_time"}
        assert produced_types <= allowed, f"unexpected event types: {produced_types - allowed}"


# ===========================================================================
# Goal-family classifier precedence (ported unit coverage).
# ===========================================================================


class TestClassifyGoalSlugPrecedence:
    """Direct unit coverage of ``_classify_goal_slug``, independent of the
    fixture. The ported classifier feeds ``NormalizedEvent.event_type`` so it
    yields the taxonomy STRINGS (not enums)."""

    @staticmethod
    def _classify(slug: str):
        from gamecollect_football.espn import _classify_goal_slug

        result = _classify_goal_slug(slug)
        # Tolerate an enum-shaped return by comparing its .value.
        return getattr(result, "value", result)

    def test_own_goal_checked_first(self):
        assert self._classify("own-goal") == "own_goal"
        assert self._classify("own---header") == "own_goal"

    def test_own_goal_subtype_is_not_dropped(self):
        assert self._classify("own-goal---header") == "own_goal"
        assert self._classify("own-goal---volley") == "own_goal"

    def test_converted_penalty_counts_as_goal(self):
        assert self._classify("goal---penalty") == "goal"
        assert self._classify("penalty-goal") == "goal"

    def test_goal_subtypes_map_to_goal(self):
        assert self._classify("goal") == "goal"
        assert self._classify("goal---volley") == "goal"
        assert self._classify("goal---free-kick") == "goal"

    def test_goalkeeper_save_is_not_a_goal(self):
        assert self._classify("goalkeeper-save") is None

    def test_non_goal_family_returns_none(self):
        for slug in ("red-card", "yellow-card", "substitution", "penalty", "halftime"):
            assert self._classify(slug) is None


# ===========================================================================
# Code-review fixes: malformed participants entries and interval status.
# ===========================================================================


class TestMalformedParticipantsEntries:
    def test_null_participant_entry_does_not_abort_the_event(self):
        """ESPN emits explicit nulls on malformed events; a null participants
        ENTRY must degrade to player=None, not AttributeError the whole
        summary parse (review fix — the rosters loop already had this guard)."""
        from gamecollect_football.espn import _normalize_key_event

        raw = _key_event("yellow-card")
        raw["participants"] = [None]
        out = _normalize_key_event(raw)
        assert len(out) == 1
        assert out[0].player is None

    def test_null_assist_entry_degrades_to_none(self):
        from gamecollect_football.espn import _normalize_key_event

        raw = _key_event("goal")
        raw["participants"] = [{"athlete": {"displayName": "Jonathan David"}}, None]
        out = _normalize_key_event(raw)
        assert len(out) == 1
        assert out[0].player == "Jonathan David"
        assert out[0].assist is None


class TestIntervalStatus:
    def test_both_halftime_spellings_normalize_to_paused(self):
        """Review fix: the ported 'Half Time' literal was never live-verified
        and ESPN's event slug is the one-word 'halftime' — both spellings must
        keep the match on the live slate through the interval."""
        from gamecollect.provider import MatchStatus
        from gamecollect_football.espn import normalize_status

        assert normalize_status("Half Time") is MatchStatus.PAUSED
        assert normalize_status("Halftime") is MatchStatus.PAUSED
