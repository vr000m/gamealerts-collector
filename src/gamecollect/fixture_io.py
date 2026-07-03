"""Fixture (de)serialization: the on-disk JSON form of a recorded match.

Fixtures are checked into git as JSON (DESIGN.md §7 record/replay): portable,
diffable, and immune to SQLite page-layout churn. One fixture is one match — a
versioned header plus its ordered events (original minutes/importances/actors
preserved), an optional entities snapshot (rosters, for ``get_squad``), and an
optional standings snapshot. Events carry the full
:class:`~gamecollect.provider.NormalizedEvent` shape so a
:class:`ReplayProvider` (Phase 4) can reconstruct provider snapshots exactly;
the events table is a lossier projection of this.

``--record`` (the engine) and the gamealerts importer (Phase 4) both write via
:func:`write_fixture`; :class:`ReplayProvider` and ``--check`` read via
:func:`read_fixture`. The ``format_version`` field guards against silently
misreading a future layout.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from gamecollect.provider import MatchStatus, NormalizedEvent, NormalizedMatch

__all__ = [
    "FORMAT_VERSION",
    "Fixture",
    "FixtureFormatError",
    "write_fixture",
    "read_fixture",
    "fixture_stem",
]

# Bump when the on-disk layout changes incompatibly; read_fixture refuses any
# other version rather than guessing at an unknown shape.
FORMAT_VERSION = 1


class FixtureFormatError(ValueError):
    """A fixture file is malformed or carries an unsupported ``format_version``."""


def fixture_stem(match_id: str) -> str:
    """A collision-proof filename stem for a fixture named after ``match_id``.

    Sanitizing a match_id for the filesystem is lossy: ``espn:760464`` and
    ``espn_760464`` both fold to ``espn_760464``, so two distinct matches would
    silently overwrite one shared file. To keep the stem both filesystem-safe
    and injective, a short stable hash of the *original* id is appended — the
    sanitized part stays human-readable, the hash makes the whole stem unique
    per source id. The hash is derived from the raw id (not the sanitized form),
    so ids that sanitize alike still get distinct stems.
    """
    sanitized = "".join(c if c.isalnum() or c in "-_." else "_" for c in match_id)
    digest = hashlib.sha1(match_id.encode("utf-8")).hexdigest()[:8]
    return f"{sanitized}-{digest}"


@dataclass
class Fixture:
    """An in-memory fixture: the match (with events) plus optional snapshots.

    ``entities``/``standings`` are raw row dicts (nullable — a live ``--record``
    session only sees ``NormalizedMatch`` state, so it records them empty; the
    importer fills them from the source DB).
    """

    format_version: int
    match: NormalizedMatch
    entities: list[dict[str, Any]] = field(default_factory=list)
    standings: list[dict[str, Any]] = field(default_factory=list)


def _match_header(match: NormalizedMatch) -> dict[str, Any]:
    return {
        "match_id": match.match_id,
        "status": match.status.value,
        "minute": match.minute,
        "score_home": match.score_home,
        "score_away": match.score_away,
        "display_clock": match.display_clock,
        "home_team": match.home_team,
        "away_team": match.away_team,
        "kickoff_utc": match.kickoff_utc,
        "payload": match.payload,
    }


def _event_json(event: NormalizedEvent) -> dict[str, Any]:
    return {
        "seq": event.seq,
        "minute": event.minute,
        "event_type": event.event_type,
        "importance": event.importance,
        "team": event.team,
        "player": event.player,
        "assist": event.assist,
        "detail": event.detail,
    }


def write_fixture(
    path: str | Path,
    match: NormalizedMatch,
    *,
    entities: list[dict[str, Any]] | None = None,
    standings: list[dict[str, Any]] | None = None,
) -> None:
    """Serialize ``match`` (header + its events) and snapshots to a JSON file.

    Events are written in seq order regardless of the match's list order so a
    recorded fixture is stable and diffable. Parent directories are created.
    """
    data = {
        "format_version": FORMAT_VERSION,
        "match": _match_header(match),
        "events": [_event_json(event) for event in sorted(match.events, key=lambda e: e.seq)],
        "entities": list(entities) if entities else [],
        "standings": list(standings) if standings else [],
    }
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _require(data: dict[str, Any], key: str) -> Any:
    if key not in data:
        raise FixtureFormatError(f"fixture is missing required key {key!r}")
    return data[key]


def read_fixture(path: str | Path) -> Fixture:
    """Parse a fixture JSON file back into a :class:`Fixture`.

    Raises :class:`FixtureFormatError` on a malformed file, an unknown
    ``format_version``, or an unrecognized :class:`MatchStatus` value.
    """
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise FixtureFormatError(f"cannot read fixture {path!r}: {exc}") from exc
    if not isinstance(data, dict):
        raise FixtureFormatError(f"fixture {path!r} is not a JSON object")

    version = _require(data, "format_version")
    if version != FORMAT_VERSION:
        raise FixtureFormatError(
            f"fixture {path!r} is format_version {version!r}; this library reads {FORMAT_VERSION}"
        )

    header = _require(data, "match")
    if not isinstance(header, dict):
        raise FixtureFormatError(f"fixture {path!r} 'match' is not a JSON object")
    try:
        status = MatchStatus(header["status"])
    except (KeyError, ValueError) as exc:
        raise FixtureFormatError(
            f"fixture {path!r} has a missing/unknown match status: {exc}"
        ) from exc

    raw_events = data.get("events", [])
    if not isinstance(raw_events, list):
        raise FixtureFormatError(f"fixture {path!r} 'events' is not a JSON array")
    events = []
    for index, event in enumerate(raw_events):
        if not isinstance(event, dict):
            raise FixtureFormatError(f"fixture {path!r} event {index} is not a JSON object")
        try:
            events.append(
                NormalizedEvent(
                    seq=event["seq"],
                    minute=event.get("minute"),
                    event_type=event["event_type"],
                    importance=event["importance"],
                    team=event.get("team"),
                    player=event.get("player"),
                    assist=event.get("assist"),
                    detail=event.get("detail"),
                )
            )
        except KeyError as exc:
            raise FixtureFormatError(
                f"fixture {path!r} event {index} is missing required key {exc}"
            ) from exc
    events.sort(key=lambda e: e.seq)

    try:
        match_id = header["match_id"]
    except KeyError as exc:
        raise FixtureFormatError(f"fixture {path!r} 'match' is missing required key {exc}") from exc
    match = NormalizedMatch(
        match_id=match_id,
        status=status,
        minute=header.get("minute"),
        score_home=header.get("score_home"),
        score_away=header.get("score_away"),
        display_clock=header.get("display_clock"),
        events=events,
        home_team=header.get("home_team"),
        away_team=header.get("away_team"),
        kickoff_utc=header.get("kickoff_utc"),
        payload=header.get("payload") or {},
    )
    return Fixture(
        format_version=version,
        match=match,
        entities=list(data.get("entities") or []),
        standings=list(data.get("standings") or []),
    )
