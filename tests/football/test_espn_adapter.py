"""Phase 4: ported ESPN adapter parity against the frozen gamealerts golden.

The acceptance bar (plan §Requirements / §Acceptance Criteria): same fixture
in -> normalized output equal to ``golden/qatar_canada_760440.normalized.json``,
captured once from gamealerts' adapter (see golden/capture_golden.py for
provenance; gamealerts is never imported here). Serialization rules are shared
via ``_football_helpers.serialize_match``: enums -> ``.value``, so gamealerts'
``EventType.GOAL``/``EventImportance.CRITICAL`` compare equal to the ported
plain-string ``"goal"`` / int ``1`` (the Phase 3 generalization).
"""

from __future__ import annotations

import json

from _football_helpers import FIXTURE_PATH, MATCH_ID, serialize_match


class TestFixtureIsCheckedIn:
    def test_fixture_ships_inside_the_pack(self):
        """The pack carries its own copy of the ESPN summary fixture — tests
        must not reach into the gamealerts repo."""
        assert FIXTURE_PATH.is_file(), (
            f"missing pack fixture {FIXTURE_PATH}; the Phase 4 implementer copies it "
            "from gamealerts data/fixtures/"
        )

    def test_fixture_matches_the_golden_provenance(self):
        """Sanity: the checked-in raw fixture is the same capture the golden was
        made from — Canada/Qatar, event 760440, with the one synthetic
        ``yellow-red-card`` keyEvent flagged ``_test_data``."""
        raw = json.loads(FIXTURE_PATH.read_text())
        slugs = [(p.get("type") or {}).get("type") for p in raw.get("keyEvents", [])]
        second_yellows = [
            p
            for p in raw.get("keyEvents", [])
            if (p.get("type") or {}).get("type") == "yellow-red-card"
        ]
        assert slugs, "fixture carries no keyEvents"
        assert len(second_yellows) == 1
        assert second_yellows[0].get("_test_data") is True


class TestAdapterParityWithGolden:
    def test_events_equal_golden_exactly(self, golden, replayed_match):
        """The core parity assertion: every normalized event — seq, minute,
        type, importance, team, player, assist, detail — is byte-identical to
        the frozen gamealerts capture."""
        assert serialize_match(replayed_match)["events"] == golden["events"]

    def test_match_header_equals_golden(self, golden, replayed_match):
        """Match-level fields (id, status, score, identity, kickoff) match the
        frozen capture; sport extras (round/group/HT scores/venue) are NOT part
        of this comparison — they moved to ``NormalizedMatch.payload``."""
        assert serialize_match(replayed_match)["match"] == golden["match"]

    def test_full_serialized_document_round_trips_the_golden_file(self, replayed_match):
        """Belt and braces: re-serializing with the exact capture-script dump
        settings reproduces the golden file byte-for-byte (guards against the
        two serializers drifting apart silently)."""
        from _football_helpers import GOLDEN_PATH

        rendered = (
            json.dumps(
                serialize_match(replayed_match), sort_keys=True, indent=2, ensure_ascii=False
            )
            + "\n"
        )
        assert rendered == GOLDEN_PATH.read_text()


class TestGeneralizedShape:
    """The Phase 3 generalization: event_type is a plain string, importance a
    plain int, and sport extras travel in ``NormalizedMatch.payload``."""

    def test_event_types_are_plain_strings(self, replayed_match):
        for ev in replayed_match.events:
            assert type(ev.event_type) is str, (
                f"event_type must be a plain str post-generalization, got "
                f"{type(ev.event_type).__name__} for seq {ev.seq}"
            )

    def test_importance_is_plain_int(self, replayed_match):
        for ev in replayed_match.events:
            assert type(ev.importance) is int and not isinstance(ev.importance, bool)

    def test_match_carries_a_payload_dict(self, replayed_match):
        # Exact payload keys (round/group/city/stadium/HT scores) are the
        # implementer's mapping; the contract pinned here is only that the
        # generalized NormalizedMatch exposes the dict.
        assert isinstance(replayed_match.payload, dict)

    def test_ht_scores_are_parsed_from_linescores(self, replayed_match):
        """Code-review fix: the docstring-promised HT scores were initialized
        to None and never assigned. The fixture's first-period linescores are
        3-0, so the payload must carry them."""
        assert replayed_match.payload["score_ht_home"] == 3
        assert replayed_match.payload["score_ht_away"] == 0

    def test_match_is_core_normalized_match(self, replayed_match):
        """The pack normalizes into the CORE dataclasses (SDK consumed from the
        outside), not a private clone."""
        from gamecollect.provider import MatchStatus, NormalizedEvent, NormalizedMatch

        assert isinstance(replayed_match, NormalizedMatch)
        assert isinstance(replayed_match.status, MatchStatus)
        assert all(isinstance(ev, NormalizedEvent) for ev in replayed_match.events)


class TestTournamentParameter:
    """Phase 4 pins: tournament (``fifa.world``) becomes a constructor
    parameter with the WC2026 default."""

    def test_default_tournament_targets_fifa_world(self):
        from gamecollect_football.espn import ESPNAdapter

        seen: list[str] = []

        def spy(url: str, params=None) -> dict:
            seen.append(url)
            return replay_raw()

        def replay_raw() -> dict:
            return json.loads(FIXTURE_PATH.read_text())

        ESPNAdapter(http_get=spy).fetch_match_detail(MATCH_ID)
        assert seen and "fifa.world" in seen[0], f"default endpoint not fifa.world: {seen}"

    def test_tournament_is_configurable(self):
        from gamecollect_football.espn import ESPNAdapter

        seen: list[str] = []

        def spy(url: str, params=None) -> dict:
            seen.append(url)
            return json.loads(FIXTURE_PATH.read_text())

        # Constructor parameter name is pinned loosely: try the obvious spellings.
        adapter = None
        for kw in ("tournament", "league", "league_slug", "tournament_slug"):
            try:
                adapter = ESPNAdapter(http_get=spy, **{kw: "uefa.euro"})
                break
            except TypeError:
                continue
        assert adapter is not None, (
            "ESPNAdapter accepts no tournament constructor parameter "
            "(plan: tournament becomes a constructor parameter with WC2026 default)"
        )
        adapter.fetch_match_detail(MATCH_ID)
        assert seen and "uefa.euro" in seen[0], f"custom tournament not in URL: {seen}"


class TestResponseSizeGuard:
    def test_max_response_bytes_guard_survives_the_port(self):
        """The ``_MAX_RESPONSE_BYTES`` cap on untrusted responses is ported
        (16 MB in gamealerts)."""
        import gamecollect_football.espn as espn_mod

        cap = getattr(espn_mod, "_MAX_RESPONSE_BYTES", None) or getattr(
            espn_mod, "MAX_RESPONSE_BYTES", None
        )
        assert isinstance(cap, int) and cap > 0, "response-size guard missing from the port"


def test_malformed_later_scoreboard_event_raises_shape_drift():
    """The shape assertion samples events[0]; a malformed LATER event must
    still surface as ShapeDriftError, not raw KeyError (provider seam)."""
    import copy

    import pytest

    from gamecollect.provider import ShapeDriftError
    from gamecollect_football.espn import ESPNAdapter

    good_event = {
        "id": "1",
        "competitions": [{"status": {"type": {"description": "In Progress"}}, "competitors": []}],
    }
    data = {"events": [copy.deepcopy(good_event), {"id": "2"}]}
    adapter = ESPNAdapter(http_get=lambda url, params=None: data)
    with pytest.raises(ShapeDriftError):
        adapter.fetch_live_matches()


def test_scoreboard_event_missing_id_raises_shape_drift():
    """Codex adversarial fix: a scoreboard event with a missing, null, or blank
    id must fail closed at the provider seam. Otherwise it normalizes to an empty
    match_id, collapsing malformed events into one collision-prone identity.

    The event must otherwise satisfy assert_espn_scoreboard_shape (displayClock +
    competitors[0].score) so the shape check doesn't fire first and mask whether
    the id check itself works — a bare status.type-only event is shape-invalid on
    its own and would raise "displayClock missing" before ever reaching the id
    validation in _normalize_scoreboard_event, making the test pass for the wrong
    reason."""
    import copy

    import pytest

    from gamecollect.provider import ShapeDriftError
    from gamecollect_football.espn import ESPNAdapter

    good_event = {
        "id": "1",
        "competitions": [
            {
                "status": {"type": {"description": "In Progress"}, "displayClock": "12'"},
                "competitors": [
                    {"score": "1", "homeAway": "home"},
                    {"score": "0", "homeAway": "away"},
                ],
            }
        ],
    }
    for bad_id in ({}, {"id": None}, {"id": ""}, {"id": "   "}):
        event = copy.deepcopy(good_event)
        event.pop("id")
        event.update(bad_id)
        data = {"events": [event]}
        adapter = ESPNAdapter(http_get=lambda url, params=None, _d=data: _d)
        with pytest.raises(ShapeDriftError, match="missing or blank id"):
            adapter.fetch_live_matches()


def test_scoreboard_event_boolean_id_raises_shape_drift():
    """Same shape-validity note as test_scoreboard_event_missing_id_raises_shape_drift
    above: displayClock + competitors[0].score must be present so the id check —
    not the shape assertion — is what raises."""
    import pytest

    from gamecollect.provider import ShapeDriftError
    from gamecollect_football.espn import ESPNAdapter

    for bad_id in (False, True):
        data = {
            "events": [
                {
                    "id": bad_id,
                    "competitions": [
                        {
                            "status": {
                                "type": {"description": "In Progress"},
                                "displayClock": "12'",
                            },
                            "competitors": [
                                {"score": "1", "homeAway": "home"},
                                {"score": "0", "homeAway": "away"},
                            ],
                        }
                    ],
                }
            ]
        }
        adapter = ESPNAdapter(http_get=lambda url, params=None, _d=data: _d)
        with pytest.raises(ShapeDriftError, match="missing or blank id"):
            adapter.fetch_live_matches()


def test_null_athlete_in_key_event_does_not_crash():
    """ESPN emits present-but-null nested values; normalization must not
    raise raw AttributeError (it previously escaped the ProviderError seam)."""
    from gamecollect_football.espn import _normalize_key_event

    raw = {
        "type": {"type": "goal"},
        "clock": {"displayValue": "12'"},
        "team": None,
        "participants": [{"athlete": None}],
        "text": "Goal!",
    }
    events = _normalize_key_event(raw)
    assert len(events) == 1
    assert events[0].player is None and events[0].team is None


class TestProviderSeamNonDictResponses:
    """Codex adversarial fix: valid JSON with the wrong top-level type (e.g. a
    bare list) must surface as ShapeDriftError, not a raw TypeError escaping
    the ProviderError seam the poll loop degrades on."""

    def test_scoreboard_non_dict_response_raises_shape_drift(self):
        import pytest

        from gamecollect.provider import ShapeDriftError
        from gamecollect_football.espn import ESPNAdapter

        adapter = ESPNAdapter(http_get=lambda url, params=None: [])
        with pytest.raises(ShapeDriftError):
            adapter.fetch_live_matches()

    def test_summary_non_dict_response_raises_shape_drift(self):
        import pytest

        from gamecollect.provider import ShapeDriftError
        from gamecollect_football.espn import ESPNAdapter

        adapter = ESPNAdapter(http_get=lambda url, params=None: [])
        with pytest.raises(ShapeDriftError):
            adapter.fetch_match_detail("760440")
