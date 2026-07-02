"""Minimal in-repo test-fixture sport pack (dev environment only).

Registered under the ``gamecollect.packs`` entry-point group as
``test-fixture`` so registry tests exercise real ``importlib.metadata``
discovery — no mocks. It supplies the smallest well-formed value for each of
the six DESIGN.md §5 contributions, including a canned in-memory provider.
"""

from __future__ import annotations

from gamecollect.packs.spec import EventTypeDecl, SportPack
from gamecollect.provider import MatchDataProvider, MatchStatus, NormalizedEvent, NormalizedMatch

__all__ = ["FixtureProvider", "pack"]


class FixtureProvider(MatchDataProvider):
    """Canned provider returning one deterministic in-play match."""

    def fetch_live_matches(self) -> list[NormalizedMatch]:
        return [self.fetch_match_detail("fixture:m1")]

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        return NormalizedMatch(
            match_id=match_id,
            status=MatchStatus.IN_PLAY,
            minute=7,
            score_home=1,
            score_away=0,
            display_clock="7'",
            events=[
                NormalizedEvent(
                    seq=0,
                    minute=1,
                    event_type="start",
                    importance=3,
                    team=None,
                    player=None,
                    assist=None,
                    detail="fixture start",
                ),
                NormalizedEvent(
                    seq=1,
                    minute=6,
                    event_type="score",
                    importance=1,
                    team="Home XI",
                    player="Fixture Player",
                    assist=None,
                    detail="fixture score",
                ),
            ],
            home_team="Home XI",
            away_team="Away XI",
            kickoff_utc="2026-07-02T18:00:00+00:00",
            payload={"round": "fixture"},
        )


def pack() -> SportPack:
    """Zero-arg entry-point factory returning the test-fixture SportPack."""
    return SportPack(
        name="test-fixture",
        sport="fixture",
        provider_factory=FixtureProvider,
        taxonomy={
            "start": EventTypeDecl(display_name="Start", importance_default=3),
            "score": EventTypeDecl(display_name="Score", importance_default=1),
            "end": EventTypeDecl(display_name="End", importance_default=2),
        },
        prompt_fragments={"summary": "A synthetic fixture sport used only in tests."},
        preference_schema={
            "type": "object",
            "properties": {"followed_teams": {"type": "array", "items": {"type": "string"}}},
        },
        display_metadata={"teams": {"HOME": "Home XI", "AWAY": "Away XI"}},
        compaction_boundaries=["interval"],
        side_table_ddl=(),
    )
