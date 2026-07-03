"""Poll-to-poll diffing: what changed since the last write, so writes stay minimal.

The engine holds the last-written provider snapshot per match in memory and
compares each fresh provider snapshot against it. Only matches whose state
changed get an ``upsert_match`` (via the pack seed hook), and only events whose
content differs from the last snapshot are handed to ``append_events`` — a
full-list re-poll that re-sends unchanged events produces an empty
``new_events`` and no write.

"Content differs", not "seq is new", is deliberate: a re-sent seq whose
fingerprint mutated (the provider's positional event list shifted under it) MUST
reach the writer so its :class:`~gamecollect.db.writer.SequenceError` drift
guard fires. Filtering purely by max-seq-seen would hide that shift. Genuinely
unchanged re-sends are filtered; new and mutated seqs pass through.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from gamecollect.provider import NormalizedEvent, NormalizedMatch

__all__ = ["MatchDiff", "diff_match", "diff_matches"]


@dataclass(frozen=True)
class MatchDiff:
    """The write-relevant delta for one match between two polls.

    ``match`` is the fresh provider snapshot. ``state_changed`` is True when any
    core state field (status/minute/score/clock/identity/payload) differs from
    the last snapshot (or there was no last snapshot). ``new_events`` are the
    events whose content is new or mutated relative to the last snapshot,
    ordered by seq.
    """

    match: NormalizedMatch
    state_changed: bool
    new_events: list[NormalizedEvent]

    @property
    def has_changes(self) -> bool:
        """True when this poll produced anything worth writing."""
        return self.state_changed or bool(self.new_events)


def _state_key(match: NormalizedMatch) -> tuple:
    """Comparable projection of a match's core state fields.

    ``payload`` is compared as canonical JSON so key order never registers as a
    change. Events are excluded — they diff separately by seq.
    """
    return (
        match.status,
        match.minute,
        match.score_home,
        match.score_away,
        match.display_clock,
        match.home_team,
        match.away_team,
        match.kickoff_utc,
        json.dumps(match.payload, sort_keys=True, ensure_ascii=False),
    )


def _event_key(event: NormalizedEvent) -> tuple:
    """Full content fingerprint of an event (every field except ``seq``)."""
    return (
        event.minute,
        event.event_type,
        event.importance,
        event.team,
        event.player,
        event.assist,
        event.detail,
    )


def diff_match(current: NormalizedMatch, last: NormalizedMatch | None) -> MatchDiff:
    """Diff one fresh snapshot against the last-written one (``None`` if first seen)."""
    if last is None:
        return MatchDiff(
            match=current,
            state_changed=True,
            new_events=sorted(current.events, key=lambda e: e.seq),
        )
    state_changed = _state_key(current) != _state_key(last)
    last_events = {event.seq: _event_key(event) for event in last.events}
    new_events = [
        event for event in current.events if last_events.get(event.seq) != _event_key(event)
    ]
    new_events.sort(key=lambda e: e.seq)
    return MatchDiff(match=current, state_changed=state_changed, new_events=new_events)


def diff_matches(
    current: list[NormalizedMatch], last: dict[str, NormalizedMatch]
) -> list[MatchDiff]:
    """Diff a poll's worth of snapshots against the per-match last-written state.

    ``last`` is keyed by provider-native ``match_id`` (the key the engine holds
    its in-memory state under). Returns one :class:`MatchDiff` per current
    match, in the provider's order; callers filter on :attr:`MatchDiff.has_changes`.
    """
    return [diff_match(match, last.get(match.match_id)) for match in current]
