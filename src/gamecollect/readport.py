"""Generic, sport-agnostic read Protocol for the collector's shared store.

Any consuming app (the gamealerts GameWorker being the first) answers live/
historical match questions by depending on :class:`MatchReadPort` — a
structural :class:`typing.Protocol`, not a base class — and injecting a
pack-owned adapter (e.g. ``gamecollect_football.readport.FootballReadPort``)
that satisfies it over the collector's own sport-agnostic schema. Core never
imports a pack (dependency direction is pack -> core only), so this module is
typing-only: no implementation, no football (or any other sport's) column
names in any method signature.

Return shapes generalize the football-specific worker methods this Protocol
replaces:

- A ``participants`` list (``{"name", "side", "score"}``), not a pair of
  hardcoded per-side columns — any number of participants, not fixed at two.
- An event/state ``phase`` field (adapter-populated — e.g. the football
  adapter uses its own phase-marker event type strings), not a single
  hardcoded phase-name literal baked into the type signature.
- Lineup/roster entries keyed by ``participant`` (a display name), not by a
  hardcoded per-side column.

Mapping from the gamealerts GameWorker's current 10 football-coupled method
names (docs/dev_plans/20260707-feature-gameworker-integration.md, Context) to
this Protocol — 1:1, so an injected adapter is a drop-in replacement for the
worker's concrete football DAO on every fact-read call site:

| Worker method            | Protocol method           |
|---------------------------|---------------------------|
| ``latest_state``          | :meth:`latest_state`          |
| ``events_for_match``      | :meth:`events_for_match`      |
| ``lineup_for_match``      | :meth:`lineup_for_match`      |
| ``lineup_for_match_live`` | :meth:`lineup_for_match_live` |
| ``venue_for_match``       | :meth:`venue_for_match`       |
| ``roster_for_team``       | :meth:`roster_for_team`       |
| ``roster_contains``       | :meth:`roster_contains`       |
| ``player_in_lineup``      | :meth:`player_in_lineup`      |
| ``lineup_team_announced`` | :meth:`lineup_team_announced` |
| ``recent_commentary``     | :meth:`recent_commentary`     |

:meth:`list_matches` has no worker-DAO counterpart above — it closes a gap
found while gamealerts built against this Protocol: every other method
requires an already-known ``match_id``/``participant``, so a consumer holding
only the port (not the full ``gamecollect.client`` module) had no way to
discover matches or resolve a spoken/typed team name to a ``match_id``.
``gamecollect.client.list_matches`` already existed but returns a typed
``MatchState`` dataclass with soft entity refs, not this Protocol's plain-dict
``participants`` shape — so it is not a drop-in substitute for port consumers.

Every method returns plain dicts/lists (or ``bool``/``None``) — the shapes a
``MatchContext``/Q&A tool consumes directly, not typed dataclasses.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

__all__ = ["MatchReadPort"]


@runtime_checkable
class MatchReadPort(Protocol):
    """Sport-neutral read surface over one collector-owned match store."""

    def list_matches(
        self, *, source: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        """Return match summaries, optionally filtered by ``source``/``status``.

        Shape per entry: ``{"match_id", "source", "status", "kickoff_utc",
        "participants": [{"name", "side", "score"}, ...]}`` — the same
        ``participants`` shape as :meth:`latest_state`, with canonical
        display names (the write seam canonicalizes provider names before
        persisting — see ``gamecollect_football.reconcile`` — so these are
        already fold-match-ready, not raw provider strings). This is the
        sanctioned discovery/resolution path: a caller resolving a spoken or
        typed team name to a ``match_id`` should fold-match against
        ``participants[].name`` here rather than reaching past the port for
        ``gamecollect.client.list_matches``.

        Deliberately lighter than :meth:`latest_state`: no ``phase``,
        ``extra``, ``display_clock``, ``minute``, or ``period`` — those
        require a targeted per-match query this method does not pay for
        across a whole result set. Call :meth:`latest_state` for one match's
        full detail once resolved.
        """
        ...

    def latest_state(self, match_id: str) -> dict[str, Any] | None:
        """Return the current state snapshot for ``match_id``, or None if unknown.

        Shape: ``{"match_id", "status", "minute", "period", "display_clock",
        "kickoff_utc", "phase", "participants": [{"name", "side", "score"},
        ...], "extra": {...}}``. ``extra`` carries adapter/sport-specific
        additions (e.g. a knockout ``result_type``) that do not belong in the
        generic shape.

        Note on ``phase`` here vs. on an event (see ``events_for_match``):
        an adapter MAY fall back to a lowercased ``status`` string when no
        phase-marker event has fired yet (e.g. pre-kickoff) — that fallback
        value is not itself a phase-marker type, even though it shares the
        ``phase`` field name with the event shape's stricter meaning.
        """
        ...

    def events_for_match(self, match_id: str, since_seq: int = -1) -> list[dict[str, Any]]:
        """Return events for ``match_id`` with ``seq`` strictly greater than
        ``since_seq``, ordered by ``seq`` (the default returns the full list).

        Shape per event: ``{"seq", "minute", "period", "type", "phase",
        "team", "player", "assist", "detail", "importance"}``. ``phase`` is
        non-None only for phase-marker event types (adapter-defined).
        """
        ...

    def lineup_for_match(
        self, match_id: str, participant: str | None = None
    ) -> list[dict[str, Any]]:
        """Return the recorded lineup for ``match_id``, optionally scoped to
        one ``participant``.

        Shape per entry: ``{"participant", "player", "player_id", "position",
        "jersey", "starter", "subbed_in", "subbed_out", "extra": {...}}``.
        """
        ...

    def lineup_for_match_live(
        self, match_id: str, participant: str | None = None
    ) -> list[dict[str, Any]]:
        """Same contract and return shape as :meth:`lineup_for_match`.

        A distinct method for callers that want an always-current read; this
        Protocol applies no caching of its own — an adapter or caller-side
        cache, if any, is layered on top, not part of this contract.
        """
        ...

    def venue_for_match(self, match_id: str) -> dict[str, Any] | None:
        """Return ``{"stadium", "city"}`` for ``match_id``, or None if not
        yet recorded (e.g. pre-match)."""
        ...

    def roster_for_team(self, participant: str) -> list[dict[str, Any]]:
        """Return the team-level squad roster for ``participant``.

        Shape per entry: ``{"player", "player_id", "position", "extra":
        {...}}``. Empty list when the participant has no recorded roster.
        """
        ...

    def roster_contains(self, participant: str, player_name: str) -> bool:
        """True if ``player_name`` is on ``participant``'s roster (adapters
        fold-match names — accent/case-insensitive)."""
        ...

    def player_in_lineup(self, match_id: str, player_name: str) -> dict[str, Any] | None:
        """Return ``player_name``'s lineup entry (see :meth:`lineup_for_match`
        for shape) for ``match_id``, or None if the player is not in the
        recorded lineup."""
        ...

    def lineup_team_announced(self, match_id: str, participant: str) -> bool:
        """True once a starting lineup has been recorded for ``participant``
        in ``match_id``."""
        ...

    def recent_commentary(self, match_id: str, limit: int = 20) -> list[dict[str, Any]]:
        """Return the most recent commentary entries for ``match_id``, newest
        first.

        Shape per entry: ``{"text", "spoken", "created_at"}``. Empty list
        when no commentary has been written yet, or the commentary table
        does not exist yet (the collector never writes it — DESIGN.md §3).
        """
        ...
