"""Provider seam: domain model + abstract base class for live-data providers.

Ported out of gamealerts ``data/provider.py`` and generalized for the core +
pack split (DESIGN.md §5):

* ``NormalizedEvent.event_type`` is a plain string — event taxonomies are
  pack-declared data, validated at write time by
  :class:`~gamecollect.db.writer.PartitionWriter`, not a core enum.
* :class:`MatchStatus` stays a core enum: match lifecycle is sport-agnostic.
* Football-specific ``NormalizedStats``/``NormalizedLineup*`` dataclasses are
  NOT ported here — they belong to the football pack (side-table shaped).
* :class:`NormalizedMatch` gains ``payload`` for sport extras (round, venue,
  half-time scores, …) which land in the ``matches.payload`` JSON column.

Pack adapters (ESPN, replay, …) normalize raw API responses into these
dataclasses before anything touches the store; errors cross the seam only via
the :class:`ProviderError` hierarchy.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum

__all__ = [
    "MatchStatus",
    "LIVE_STATUSES",
    "LIVE_STATUS_VALUES",
    "NormalizedEvent",
    "NormalizedMatch",
    "ProviderError",
    "ShapeDriftError",
    "ProviderUnavailableError",
    "MatchDataProvider",
]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class MatchStatus(Enum):
    SCHEDULED = "SCHEDULED"
    IN_PLAY = "IN_PLAY"
    PAUSED = "PAUSED"
    FINISHED = "FINISHED"
    UNKNOWN = "UNKNOWN"


# Statuses that count as "live" (in-progress) across the whole system; every
# consumer derives its notion of "live" from this single frozenset.
LIVE_STATUSES: frozenset[MatchStatus] = frozenset({MatchStatus.IN_PLAY, MatchStatus.PAUSED})

# The same set as the persisted store strings (the store keeps each status
# enum's ``.value``). Derived here once so row filters compare against an
# identical set without re-deriving it at each site.
LIVE_STATUS_VALUES: frozenset[str] = frozenset(s.value for s in LIVE_STATUSES)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class NormalizedEvent:
    """One provider-normalized in-match event.

    ``seq`` is provider-derived (e.g. the ESPN keyEvents index after any
    0–2-event expansion) — the writer never allocates it. ``event_type`` is a
    pack-declared taxonomy string; ``importance`` follows the pack's
    :class:`~gamecollect.packs.spec.EventTypeDecl` default unless the adapter
    overrides it per event.
    """

    seq: int
    minute: int | None
    event_type: str
    importance: int
    team: str | None
    player: str | None
    assist: str | None
    detail: str | None


@dataclass
class NormalizedMatch:
    """One provider-normalized match snapshot (state + events).

    Sport extras (round, group, venue, half-time scores, …) travel in
    ``payload`` and land in the ``matches.payload`` JSON column — core fields
    stay sport-agnostic. Match-identity fields (``home_team``, ``away_team``,
    ``kickoff_utc``) are optional so partial scoreboard snapshots stay valid;
    reconciliation against the canonical row is pack territory.
    """

    match_id: str
    status: MatchStatus
    minute: int | None
    score_home: int | None
    score_away: int | None
    display_clock: str | None
    events: list[NormalizedEvent] = field(default_factory=list)
    home_team: str | None = None
    away_team: str | None = None
    kickoff_utc: str | None = None  # ISO-8601 UTC instant
    payload: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ProviderError(Exception):
    pass


class ShapeDriftError(ProviderError):
    pass


class ProviderUnavailableError(ProviderError):
    pass


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------


class MatchDataProvider(ABC):
    """Abstract provider for live match data."""

    @abstractmethod
    def fetch_live_matches(self) -> list[NormalizedMatch]:
        """Return all currently live/in-play matches."""

    @abstractmethod
    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        """Return full detail (events + payload extras) for a single match."""
