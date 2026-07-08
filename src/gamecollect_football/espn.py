"""ESPN live-data adapter (football).

Ported out of gamealerts ``data/espn.py`` for behavior parity, generalized for
the core + pack split:

* ``NormalizedEvent.event_type`` is a taxonomy string (see
  :mod:`gamecollect_football.taxonomy`), not a core enum.
* Football-specific ``NormalizedStats``/``NormalizedLineup*`` dataclasses live
  HERE now (side-table shaped); the core ``NormalizedMatch`` carries their
  JSON-serializable forms in ``payload`` along with schedule metadata
  (round/city/stadium/HT scores) and commentary text.
* The tournament slug (``fifa.world``) is a constructor parameter with the
  WC2026 default. **Named assumption** (plan): ESPN per-tournament
  endpoint shape/slug uniformity is unverified beyond ``fifa.world`` — a
  probe/record of another tournament is required before treating a second
  tournament as config-only.

Uses ESPN's undocumented public API (no key required). HTTP is injected via
``http_get`` for offline testing.
"""

from __future__ import annotations

import http.client
import json
import logging
import math
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass, field

from gamecollect.provider import (
    MatchDataProvider,
    MatchStatus,
    NormalizedEvent,
    NormalizedMatch,
    ProviderUnavailableError,
    ShapeDriftError,
)
from gamecollect_football.taxonomy import EVENT_IMPORTANCE

logger = logging.getLogger(__name__)

__all__ = [
    "ESPNAdapter",
    "NormalizedStats",
    "NormalizedLineup",
    "NormalizedLineupPlayer",
    "normalize_possession",
    "normalize_status",
    "assert_espn_scoreboard_shape",
    "assert_espn_summary_shape",
]


# ---------------------------------------------------------------------------
# Football-specific normalized shapes (side-table shaped; core stays generic)
# ---------------------------------------------------------------------------


@dataclass
class NormalizedStats:
    team: str
    possession: float | None
    shots: int | None
    shots_on_target: int | None
    corners: int | None
    fouls: int | None
    yellow_cards: int | None
    red_cards: int | None
    offsides: int | None


@dataclass
class NormalizedLineupPlayer:
    athlete_id: str
    display_name: str
    jersey: str | None
    position: str | None
    starter: bool
    subbed_in: bool
    subbed_out: bool
    formation_place: int | None


@dataclass
class NormalizedLineup:
    team: str
    home_away: str | None
    formation: str | None
    players: list[NormalizedLineupPlayer] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Normalization helpers (ported out of gamealerts data/provider.py)
# ---------------------------------------------------------------------------


def normalize_possession(raw: object) -> float | None:
    """
    Convert raw possession values to a float.

    ESPN: "28.4" (string, no percent sign); other providers send ints or
    "46%" strings. Returns None if raw is None.
    """
    if raw is None:
        return None
    if isinstance(raw, int | float):
        return float(raw)
    # string path
    s = str(raw).strip()
    if s.endswith("%"):
        s = s[:-1]
    return float(s)


# Status normalization look-up tables (ported intact out of gamealerts so
# status-string behavior is bit-identical; only the ESPN table is exercised
# by this adapter).
_API_FOOTBALL_STATUS: dict[str, MatchStatus] = {
    "1H": MatchStatus.IN_PLAY,
    "2H": MatchStatus.IN_PLAY,
    "HT": MatchStatus.PAUSED,
    "FT": MatchStatus.FINISHED,
    "PEN": MatchStatus.FINISHED,
    "NS": MatchStatus.SCHEDULED,
}

_FOOTBALL_DATA_STATUS: dict[str, MatchStatus] = {
    "IN_PLAY": MatchStatus.IN_PLAY,
    "LIVE": MatchStatus.IN_PLAY,  # some payloads surface in-progress as LIVE
    "PAUSED": MatchStatus.PAUSED,
    "FINISHED": MatchStatus.FINISHED,
    "SCHEDULED": MatchStatus.SCHEDULED,
    "TIMED": MatchStatus.SCHEDULED,  # scheduled with a confirmed kickoff time
}

_ESPN_DESCRIPTION_STATUS: dict[str, MatchStatus] = {
    "Full Time": MatchStatus.FINISHED,
    # "Half Time" is the literal ported out of gamealerts, but unlike "First
    # Half"/"Second Half" it was never verified against a live interval
    # payload, and ESPN's STATUS_HALFTIME event slug is the one-word
    # "halftime" — cover both spellings so the interval never normalizes to
    # UNKNOWN and drops the match from the live slate.
    "Half Time": MatchStatus.PAUSED,
    "Halftime": MatchStatus.PAUSED,
    "In Progress": MatchStatus.IN_PLAY,
    # ESPN serves live group matches as "First Half"/"Second Half"
    # (STATUS_FIRST_HALF/STATUS_SECOND_HALF), not "In Progress" (verified via
    # the live fifa.world scoreboard probe, 2026-06-21). Without these a real
    # in-play match normalizes to UNKNOWN instead of IN_PLAY.
    "First Half": MatchStatus.IN_PLAY,
    "Second Half": MatchStatus.IN_PLAY,
    "Scheduled": MatchStatus.SCHEDULED,
}


def normalize_status(raw: str | None) -> MatchStatus:
    """
    Normalize a provider status string to MatchStatus.

    Handles API-Football short codes, football-data long strings, and ESPN
    description strings; None / unrecognized → UNKNOWN (never raises).
    """
    if raw is None:
        return MatchStatus.UNKNOWN

    # Try each table in order
    if raw in _API_FOOTBALL_STATUS:
        return _API_FOOTBALL_STATUS[raw]
    if raw in _FOOTBALL_DATA_STATUS:
        return _FOOTBALL_DATA_STATUS[raw]
    if raw in _ESPN_DESCRIPTION_STATUS:
        return _ESPN_DESCRIPTION_STATUS[raw]

    return MatchStatus.UNKNOWN


# ESPN's ``status.type.state`` is the authoritative coarse lifecycle enum
# ("pre" → not started, "in" → in progress, "post" → finished). The granular
# ``status.type.description`` has MANY in-progress variants we do not enumerate —
# "End of Extra Time", "First/Second Half Extra Time", "Penalty Shootout", … — so a
# knockout match in extra time or a shootout normalizes to UNKNOWN via the description
# table alone, which drops it out of LIVE_STATUSES: a live consumer stops showing it live
# AND a per-match collector treats it as "no longer live" and self-reaps mid-match. We
# therefore fall back to ``state`` whenever the description does not map, so an
# in-progress match is never misclassified as UNKNOWN regardless of ESPN's wording.
_ESPN_STATE_STATUS: dict[str, MatchStatus] = {
    "pre": MatchStatus.SCHEDULED,
    "in": MatchStatus.IN_PLAY,
    "post": MatchStatus.FINISHED,
}


def _status_from_comp(comp: dict) -> MatchStatus:
    """Resolve a competition's status: the granular ``description`` table first, then
    ESPN's coarse ``state`` enum as a catch-all so extra-time / penalty-shootout states
    (unmapped descriptions) resolve to IN_PLAY/FINISHED instead of UNKNOWN.

    A mapped description still wins over ``state`` (e.g. ``Half Time`` → PAUSED keeps its
    granularity); only the description-fall-through case consults ``state``.
    """
    type_ = (comp.get("status") or {}).get("type") or {}
    status = normalize_status(type_.get("description"))
    if status is MatchStatus.UNKNOWN:
        status = _ESPN_STATE_STATUS.get(type_.get("state"), MatchStatus.UNKNOWN)
    return status


def _result_type_from_comp(comp: dict) -> str | None:
    """How a finished match was decided: ``"regulation"``, ``"extra_time"``, or
    ``"penalties"`` — ``None`` while the match is unfinished.

    :func:`_status_from_comp` deliberately collapses all three finishes to
    ``FINISHED`` (a new MatchStatus value would break ``LIVE_STATUSES`` and every
    consumer switching on status), so this preserves the knockout distinction in
    payload for downstream consumers alongside ``pen_winner_side``. ESPN encodes
    it in ``status.type.description`` — "Full Time" / "… After Extra Time" /
    "… After Penalties" (substring-matched to tolerate the "Final Score - "
    prefix). A penalty shootout implies extra time was played first.
    """
    type_ = (comp.get("status") or {}).get("type") or {}
    desc = type_.get("description") or ""
    if "Penalt" in desc:
        return "penalties"
    if "Extra Time" in desc:
        return "extra_time"
    if type_.get("state") == "post":
        return "regulation"
    return None


def _pen_score(competitor: dict) -> int | None:
    """Read a competitor's penalty-shootout score.

    ESPN exposes it as ``competitor.shootoutScore``: a float in the summary
    endpoint (``2.0``/``4.0``), an int in the scoreboard endpoint (``2``/``4``),
    and ABSENT on non-shootout matches (never ``0.0``). Accept both int and
    float, guard None/bool/non-numeric.

    A pen tally is a non-negative WHOLE number, so numeric drift that is not one
    is treated as absent (``None``) rather than cast blindly — the same posture
    the sibling ``int(competitor["score"])`` parse takes for a malformed score:
    ``int(2.9)`` would silently persist a truncated ``2``, and ``int(nan)`` /
    ``int(inf)`` raise ``ValueError``/``OverflowError`` that the fetch wrappers do
    NOT map to ``ShapeDriftError`` (they catch only KeyError/IndexError/TypeError/
    AttributeError), so an unguarded cast could escape the provider seam and crash
    the poll. Returning None here lets the both-sides presence gate suppress the
    half/garbage shootout instead.
    """
    raw = competitor.get("shootoutScore")
    if raw is None or isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    if isinstance(raw, float) and (not math.isfinite(raw) or not raw.is_integer()):
        return None
    if raw < 0:
        return None
    return int(raw)


def _extract_shootout(
    competitors: list[dict], match_id: str
) -> tuple[int | None, int | None, str | None]:
    """Extract home/away pen scores + winning side from a ``competitors[]`` list.

    Shared by both the summary and scoreboard normalizers so the two paths stay
    identical (avoid drift). Returns ``(score_pen_home, score_pen_away,
    pen_winner_side)`` in ESPN home/away terms (``pen_winner_side`` ∈
    {"home", "away", None}) — the rest of the normalized match is home/away
    oriented too; a consumer resolves the side to an entity the same way it does
    for ``score_home``/``score_away``.

    ``competitor.winner`` also marks the regulation/ET winner, so the winning
    side is derived from ``winner`` ONLY when BOTH competitors carry a numeric
    ``shootoutScore`` — that gate is the correctness boundary. When zero or two
    sides carry ``winner=True`` in a shootout, ``pen_winner_side`` is left None
    and logged. A disagreement between ``winner`` and the higher pen score is
    logged but ESPN's ``winner`` value is stored.
    """
    score_pen_home: int | None = None
    score_pen_away: int | None = None
    home_winner = False
    away_winner = False
    for competitor in competitors:
        side = competitor.get("homeAway")
        pen = _pen_score(competitor)
        winner = bool(competitor.get("winner"))
        if side == "home":
            score_pen_home = pen
            home_winner = winner
        elif side == "away":
            score_pen_away = pen
            away_winner = winner

    # Gate: only a real shootout carries a numeric pen score on BOTH sides. A
    # one-sided (malformed) shootoutScore yields all None — no half a pens
    # string ever reaches the store or wire.
    if score_pen_home is None or score_pen_away is None:
        return None, None, None

    winner_sides = [s for s, w in (("home", home_winner), ("away", away_winner)) if w]
    if len(winner_sides) != 1:
        logger.warning(
            "ESPN shootout %s has %d competitors flagged winner=True (expected 1); "
            "leaving pen_winner_side=None",
            match_id,
            len(winner_sides),
        )
        return score_pen_home, score_pen_away, None

    pen_winner_side = winner_sides[0]

    # Sanity-check: winner should agree with the higher pen score. Store ESPN's
    # value regardless, but log a disagreement (ties are inconclusive, not
    # disagreements).
    if score_pen_home != score_pen_away:
        higher_side = "home" if score_pen_home > score_pen_away else "away"
        if higher_side != pen_winner_side:
            logger.warning(
                "ESPN shootout %s: winner=%s disagrees with higher pen score "
                "(home=%d away=%d); storing ESPN winner",
                match_id,
                pen_winner_side,
                score_pen_home,
                score_pen_away,
            )

    return score_pen_home, score_pen_away, pen_winner_side


# ---------------------------------------------------------------------------
# Shape assertions (ported out of gamealerts data/provider.py)
# ---------------------------------------------------------------------------


def assert_espn_scoreboard_shape(data: dict) -> None:
    """
    Assert that the ESPN scoreboard JSON has the expected shape.

    Raises ShapeDriftError if required fields are missing.
    """
    if not isinstance(data, dict):
        raise ShapeDriftError(
            f"ESPN scoreboard response must be a JSON object, got {type(data).__name__}"
        )
    try:
        events = data["events"]
    except KeyError as exc:
        raise ShapeDriftError("ESPN scoreboard missing 'events' key") from exc

    if not isinstance(events, list):
        raise ShapeDriftError("ESPN scoreboard 'events' must be a list")

    if not events:
        # Empty events list is valid (no live matches) — nothing more to check.
        return

    # Validate first event's competition structure.
    try:
        comp = events[0]["competitions"][0]
    except (KeyError, IndexError) as exc:
        raise ShapeDriftError("ESPN scoreboard event missing competitions[0]") from exc

    # status.type
    try:
        _status_type = comp["status"]["type"]
    except KeyError as exc:
        raise ShapeDriftError("ESPN scoreboard competitions[0].status.type missing") from exc

    # status.displayClock
    try:
        _display_clock = comp["status"]["displayClock"]
    except KeyError as exc:
        raise ShapeDriftError(
            "ESPN scoreboard competitions[0].status.displayClock missing"
        ) from exc

    # competitors[0].score
    try:
        _score = comp["competitors"][0]["score"]
    except (KeyError, IndexError) as exc:
        raise ShapeDriftError(
            "ESPN scoreboard competitions[0].competitors[0].score missing"
        ) from exc


def assert_espn_summary_shape(data: dict) -> None:
    """
    Assert that the ESPN summary JSON has the expected shape.

    Raises ShapeDriftError if required fields are missing.
    """
    if not isinstance(data, dict):
        raise ShapeDriftError(
            f"ESPN summary response must be a JSON object, got {type(data).__name__}"
        )
    # boxscore.teams with statistics list
    try:
        teams = data["boxscore"]["teams"]
    except KeyError as exc:
        raise ShapeDriftError("ESPN summary missing boxscore.teams") from exc

    if not isinstance(teams, list):
        raise ShapeDriftError("ESPN summary boxscore.teams must be a list")

    # Verify at least one team has a possessionPct stat.
    found_possession = False
    for team in teams:
        for stat in team.get("statistics", []):
            if stat.get("name") == "possessionPct":
                found_possession = True
                break
        if found_possession:
            break
    if not found_possession:
        raise ShapeDriftError("ESPN summary boxscore.teams statistics missing possessionPct entry")

    # keyEvents
    if "keyEvents" not in data:
        raise ShapeDriftError("ESPN summary missing 'keyEvents' key")
    if not isinstance(data["keyEvents"], list):
        raise ShapeDriftError("ESPN summary 'keyEvents' must be a list")

    # rosters
    if "rosters" not in data:
        raise ShapeDriftError("ESPN summary missing 'rosters' key")
    if not isinstance(data["rosters"], list):
        raise ShapeDriftError("ESPN summary 'rosters' must be a list")

    # commentary
    if "commentary" not in data:
        raise ShapeDriftError("ESPN summary missing 'commentary' key")
    if not isinstance(data["commentary"], list):
        raise ShapeDriftError("ESPN summary 'commentary' must be a list")


# ---------------------------------------------------------------------------
# ESPN slug mapping (ported out of gamealerts data/espn.py)
# ---------------------------------------------------------------------------

# ESPN type.type → taxonomy string mapping for NON-goal families (exact match).
#
# The goal family is intentionally NOT in this table — ESPN sends goal subtypes
# (``goal---volley``, ``goal---free-kick``, ``own-goal`` …) that an exact table
# would silently drop. Goal-family slugs are resolved by ``_classify_goal_slug``
# below so a new subtype maps automatically instead of vanishing.
#
# ``penalty`` here is reserved for an AWARDED/MISSED penalty that is NOT a goal;
# a converted penalty arrives as ``goal---penalty`` / ``penalty-goal`` and is
# classified as GOAL by the goal-family rule (see ``_classify_goal_slug``).
#
# ESPN slug audit (event 760440 fixture): ``halftime`` is the real slug (NOT the
# previous ``half-time``); ``kickoff`` maps. ``start-2nd-half``, ``end-regular-time``,
# ``start-delay`` and ``end-delay`` are intentionally left unmapped (ignored): the
# first two are clock-phase markers we do not announce, and the delay events carry
# mixed free-text reasons (injury/VAR/cooling) so synthesising a break from them
# would fabricate events. ``half-time``/``full-time`` keys are kept as harmless
# aliases in case ESPN ever sends the hyphenated form.
_ESPN_EVENT_TYPE_MAP: dict[str, str] = {
    "yellow-card": "yellow",
    "red-card": "red",
    "substitution": "sub",
    "kickoff": "kickoff",
    "halftime": "half_time",
    "half-time": "half_time",
    "full-time": "full_time",
    "penalty": "penalty",
}

# ESPN slug for a second-bookable-offence sending-off (second yellow → red). A
# single ``yellow-red-card`` slug is NOT a 1:1 type-map entry: it must surface
# BOTH the second YELLOW (so the carded side shows 🟨2) AND the resulting RED.
# The split is performed in ``_normalize_summary`` so each emitted event gets its
# own ``seq`` after expansion (see ``_SECOND_YELLOW_SLUG`` usage there).
_SECOND_YELLOW_SLUG = "yellow-red-card"

# Marker stamped on the synthetic second-yellow YELLOW event's ``detail`` so the
# split is greppable downstream (commentary, tests) and not mistaken for a fresh
# first booking.
_SECOND_YELLOW_DETAIL = "Second yellow card"


def _classify_goal_slug(slug: str) -> str | None:
    """
    Resolve a goal-family ESPN slug to a taxonomy string, in strict precedence order.

    Returns None when ``slug`` is not a goal-family slug (caller then falls back
    to the exact-match table). Precedence matters because goal/own_goal/penalty
    are all CRITICAL in the importance defaults:

      1. own-goal (``own-goal``, ``own-goal---*``, ``own---*``) → ``own_goal`` — checked FIRST.
      2. converted penalty (``goal---penalty`` / ``penalty-goal``) → ``goal`` — a
         converted penalty IS a goal and MUST count toward the goal tally; the
         ``penalty`` slug (handled by the exact table) stays reserved for an
         awarded/missed penalty that is NOT a goal.
      3. any remaining goal subtype (``goal``, ``goal---*``) → ``goal``.
    """
    if slug == "own-goal" or slug.startswith("own-goal") or slug.startswith("own---"):
        # ``own-goal``, ``own-goal---header`` … and the ``own---*`` variant. The
        # ``own-goal`` prefix catches any own-goal subtype so it is never silently
        # dropped (it would otherwise miss the goal branch too) — the exact
        # "dropped goal call-out" failure mode this rule prevents.
        return "own_goal"
    if slug == "penalty-goal" or slug == "goal" or slug.startswith("goal---"):
        # ``goal``, ``goal---volley``, ``goal---free-kick``, ``goal---penalty`` …
        # Match the goal-event token boundary (bare ``goal`` or the ``goal---``
        # subtype separator) rather than a bare ``startswith("goal")``, which would
        # wrongly swallow the non-goal ESPN slug family ``goalkeeper-save`` and
        # fabricate a goal call-out — the exact failure mode this rule prevents.
        return "goal"
    return None


# Stat name → NormalizedStats field
_STAT_NAME_MAP: dict[str, str] = {
    "foulsCommitted": "fouls",
    "yellowCards": "yellow_cards",
    "redCards": "red_cards",
    "possessionPct": "possession",
    "totalShots": "shots",
    "shotsOnTarget": "shots_on_target",
    "cornerKicks": "corners",
    "offsides": "offsides",
}


_MAX_RESPONSE_BYTES = 16 * 1024 * 1024  # cap untrusted responses (largest fixture ~0.4 MB)


def _default_http_get(url: str, params: dict | None = None) -> dict:
    """Fetch a URL and return the parsed JSON dict. Uses stdlib urllib."""
    if params:
        from urllib.parse import urlencode

        url = f"{url}?{urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "gamecollect/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read(_MAX_RESPONSE_BYTES + 1)
    except (OSError, http.client.HTTPException) as exc:
        # Normalize EVERY transport-layer failure to ProviderError so callers that
        # degrade on it never see a raw exception escape and crash the poll loop.
        # urlopen raises HTTPError/URLError (both OSError subclasses) on 4xx/5xx and
        # transport failure — but in CPython 3.12 urllib only wraps the h.request()
        # call in URLError; a socket timeout, ssl.SSLError, or ConnectionReset raised
        # during h.getresponse() (or resp.read()) escapes as a RAW OSError, and a
        # protocol error (IncompleteRead, BadStatusLine) as http.client.HTTPException.
        # Catching the OSError + HTTPException families closes that gap (the
        # read-timeout mid-poll crash). OSError already covers URLError/HTTPError, so
        # this is strictly broader than a urllib.error.URLError-only clause.
        raise ProviderUnavailableError(f"ESPN request failed: {exc}") from exc
    if len(data) > _MAX_RESPONSE_BYTES:
        raise ProviderUnavailableError(
            f"ESPN response exceeds {_MAX_RESPONSE_BYTES} bytes; refusing to parse"
        )
    raw = data.decode("utf-8")
    # Detect HTML responses (ESPN sometimes returns HTML on errors)
    stripped = raw.lstrip()
    if stripped.startswith("<"):
        raise ProviderUnavailableError(f"ESPN returned HTML, not JSON: {raw[:200]}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProviderUnavailableError(f"ESPN returned non-JSON response: {raw[:200]}") from exc


def _parse_minute(display_value: str | None) -> int | None:
    """
    Parse a clock display value like "27'" or "90'+6'" into an integer minute.

    Returns None if the string is empty or None.
    """
    if not display_value:
        return None
    # Strip trailing "'" and take only the base minute (before "+")
    base = display_value.replace("'", "").split("+")[0].strip()
    try:
        return int(base)
    except ValueError:
        return None


def _parse_formation_place(raw: object) -> int | None:
    """Parse ESPN's ``formationPlace`` (a string like "1") into an int, or None."""
    if raw is None:
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def _normalize_roster(raw: dict) -> NormalizedLineup | None:
    """
    Convert a single ESPN ``rosters[]`` team block to a NormalizedLineup.

    Reads ``team.displayName``, ``homeAway``, ``formation``, and maps each
    ``roster[]`` entry. Player entries missing ``athlete.displayName`` or
    ``athlete.id`` are skipped. Returns None when the team name is missing.
    """
    team_name = (raw.get("team") or {}).get("displayName")
    if not team_name:
        return None

    players: list[NormalizedLineupPlayer] = []
    # `or []` + isinstance guard: a null "roster" value or a non-dict entry must
    # skip cleanly, not abort the whole summary parse (same posture as the
    # commentary loop's isinstance check).
    for entry in raw.get("roster") or []:
        if not isinstance(entry, dict):
            continue
        athlete = entry.get("athlete") or {}
        athlete_id = athlete.get("id")
        display_name = athlete.get("displayName")
        if athlete_id is None or not display_name:
            continue
        jersey = entry.get("jersey")
        position = (entry.get("position") or {}).get("abbreviation")
        players.append(
            NormalizedLineupPlayer(
                athlete_id=str(athlete_id),
                display_name=display_name,
                jersey=str(jersey) if jersey is not None else None,
                position=position,
                starter=bool(entry.get("starter")),
                subbed_in=bool(entry.get("subbedIn")),
                subbed_out=bool(entry.get("subbedOut")),
                formation_place=_parse_formation_place(entry.get("formationPlace")),
            )
        )

    return NormalizedLineup(
        team=team_name,
        home_away=raw.get("homeAway"),
        formation=raw.get("formation"),
        players=players,
    )


def _normalize_key_event(raw: dict) -> list[NormalizedEvent]:
    """
    Convert a single ESPN keyEvent dict to zero, one, or two NormalizedEvents.

    Returns an empty list for unknown/unsupported event types (skip silently).
    A ``yellow-red-card`` slug expands to TWO events — a YELLOW then a RED — so
    a second booking shows 🟨2 🟥1 for that side (a 1:1 type-map entry cannot
    yield the extra yellow). All other slugs yield exactly one event.

    ``seq`` is assigned by the caller AFTER expansion (see ``_normalize_summary``)
    so the split YELLOW/RED rows get consecutive unique ``(match_id, seq)`` values
    and later events shift instead of colliding under ``INSERT OR IGNORE``. The
    events returned here carry a placeholder ``seq`` of -1.
    """
    # ``or {}`` guards present-but-null values: ESPN emits "athlete": null (etc.)
    # on malformed events, and a bare .get(..., {}) default does not fire for
    # an explicit null — the chained .get would raise AttributeError.
    type_type = (raw.get("type") or {}).get("type", "")

    clock_display = (raw.get("clock") or {}).get("displayValue") or None
    minute = _parse_minute(clock_display)

    team = (raw.get("team") or {}).get("displayName")

    # participants: first is player, second (if present) is assist. Entries
    # can be explicit nulls (like "athlete" above) — a malformed entry must
    # not abort the whole summary parse, mirroring the rosters loop's guard.
    participants = raw.get("participants") or []
    player = None
    assist = None
    if participants and isinstance(participants[0], dict):
        player = (participants[0].get("athlete") or {}).get("displayName")
    if len(participants) >= 2 and isinstance(participants[1], dict):
        assist = (participants[1].get("athlete") or {}).get("displayName")

    detail = raw.get("text") or raw.get("shortText")

    # Second-yellow → sending-off: emit a YELLOW (the second booking) AND a RED.
    # The YELLOW's detail is tagged so it is recognisably the second card; the
    # RED keeps the source text. This is the only slug that expands to two events.
    if type_type == _SECOND_YELLOW_SLUG:
        yellow = NormalizedEvent(
            seq=-1,
            minute=minute,
            event_type="yellow",
            importance=EVENT_IMPORTANCE["yellow"],
            team=team,
            player=player,
            assist=assist,
            detail=_SECOND_YELLOW_DETAIL,
        )
        red = NormalizedEvent(
            seq=-1,
            minute=minute,
            event_type="red",
            importance=EVENT_IMPORTANCE["red"],
            team=team,
            player=player,
            assist=assist,
            detail=detail,
        )
        return [yellow, red]

    # Goal family is resolved by precedence rule (own-goal first, converted
    # penalty as GOAL, any goal* as GOAL); non-goal slugs use the exact table.
    event_type = _classify_goal_slug(type_type) or _ESPN_EVENT_TYPE_MAP.get(type_type)
    if event_type is None:
        return []  # unknown event type — skip

    return [
        NormalizedEvent(
            seq=-1,
            minute=minute,
            event_type=event_type,
            importance=EVENT_IMPORTANCE[event_type],
            team=team,
            player=player,
            assist=assist,
            detail=detail,
        )
    ]


def _normalize_team_stats(team_block: dict, match_id: str) -> NormalizedStats | None:
    """
    Convert an ESPN boxscore team block to NormalizedStats.

    Returns None if team name is missing.
    """
    team_name = team_block.get("team", {}).get("displayName")
    if not team_name:
        return None

    raw_stats: dict[str, object] = {}
    for stat in team_block.get("statistics", []):
        name = stat.get("name")
        if name in _STAT_NAME_MAP:
            field_name = _STAT_NAME_MAP[name]
            raw_stats[field_name] = stat.get("displayValue")

    # Parse possession separately (float)
    possession_raw = raw_stats.get("possession")
    possession: float | None = None
    if possession_raw is not None:
        try:
            possession = normalize_possession(possession_raw)
        except (ValueError, TypeError):
            possession = None

    def _int_or_none(val: object) -> int | None:
        if val is None:
            return None
        try:
            return int(str(val))
        except (ValueError, TypeError):
            return None

    return NormalizedStats(
        team=team_name,
        possession=possession,
        shots=_int_or_none(raw_stats.get("shots")),
        shots_on_target=_int_or_none(raw_stats.get("shots_on_target")),
        corners=_int_or_none(raw_stats.get("corners")),
        fouls=_int_or_none(raw_stats.get("fouls")),
        yellow_cards=_int_or_none(raw_stats.get("yellow_cards")),
        red_cards=_int_or_none(raw_stats.get("red_cards")),
        offsides=_int_or_none(raw_stats.get("offsides")),
    )


class ESPNAdapter(MatchDataProvider):
    """
    Live-data adapter for ESPN's public soccer API.

    No API key required. HTTP is injectable for testing; the tournament slug
    is a constructor parameter (WC2026 ``fifa.world`` default).
    """

    DEFAULT_TOURNAMENT = "fifa.world"
    _BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer"

    def __init__(
        self,
        http_get: Callable | None = None,
        *,
        tournament: str = DEFAULT_TOURNAMENT,
    ) -> None:
        self._http_get = http_get if http_get is not None else _default_http_get
        self.tournament = tournament
        self.SCOREBOARD_URL = f"{self._BASE_URL}/{tournament}/scoreboard"
        self.SUMMARY_URL = f"{self._BASE_URL}/{tournament}/summary"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_live_matches(self) -> list[NormalizedMatch]:
        """
        Fetch the ESPN scoreboard and return NormalizedMatch objects for all events.

        The contracted ``MatchDataProvider`` entry point for the current slate; it is
        the no-date case of :func:`fetch_schedule`, to which it delegates so the
        scoreboard fetch + shape-validate + normalize body lives in one place.

        Raises ProviderUnavailableError or ShapeDriftError on failure.
        """
        return self._fetch_scoreboard()

    def fetch_schedule(self, *, dates: str | None = None) -> list[NormalizedMatch]:
        """
        Fetch the ESPN scoreboard for a date selector and return NormalizedMatch
        objects for ALL events, regardless of status (SCHEDULED/IN_PLAY/FINISHED).

        ESPN-only (not on the MatchDataProvider ABC): only ESPN exposes a
        scoreboard-by-date endpoint. ``dates`` is a ``YYYYMMDD`` (or range) selector
        passed through to the scoreboard URL via the existing ``params`` seam; with
        no ``dates`` it returns ESPN's notion of the current day's slate. Unlike a
        live-only consumer, this normalizes every event with no live filter so a
        schedule loader sees scheduled and finished fixtures too.

        Raises ProviderUnavailableError or ShapeDriftError on failure.
        """
        return self._fetch_scoreboard(params={"dates": dates} if dates else None)

    def _fetch_scoreboard(self, params: dict[str, str] | None = None) -> list[NormalizedMatch]:
        """
        Issue one scoreboard GET, validate its shape, and normalize every event.

        Shared by ``fetch_live_matches`` (no params -> current slate) and
        ``fetch_schedule`` (``dates`` selector). Both normalize the full event list
        with no live filter; the caller decides what to do with the statuses.
        """
        data = self._http_get(self.SCOREBOARD_URL, params=params)
        # The shape assertion samples events[0] only, and its own key probes
        # can hit TypeError/IndexError on structurally wrong (but valid) JSON;
        # normalization of LATER events can still hit missing keys. Map all of
        # those to ShapeDriftError so errors cross the provider seam only via
        # the ProviderError hierarchy.
        try:
            assert_espn_scoreboard_shape(data)
            return [self._normalize_scoreboard_event(ev) for ev in data.get("events", [])]
        except (KeyError, IndexError, TypeError, AttributeError) as exc:
            raise ShapeDriftError(f"malformed scoreboard event: {exc!r}") from exc

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        """
        Fetch the ESPN summary for one match and return a full NormalizedMatch.

        Raises ProviderUnavailableError or ShapeDriftError on failure.
        """
        data = self._http_get(self.SUMMARY_URL, params={"event": match_id})
        try:
            assert_espn_summary_shape(data)
            return self._normalize_summary(match_id, data)
        except (KeyError, IndexError, TypeError, AttributeError) as exc:
            raise ShapeDriftError(f"malformed summary for match {match_id!r}: {exc!r}") from exc

    # ------------------------------------------------------------------
    # Private normalization
    # ------------------------------------------------------------------

    def _normalize_scoreboard_event(self, ev: dict) -> NormalizedMatch:
        """Normalize one ESPN scoreboard event into a NormalizedMatch (score only)."""
        match_id = str(ev.get("id", ""))
        comp = ev["competitions"][0]

        status = _status_from_comp(comp)

        display_clock = comp["status"].get("displayClock") or None

        minute = _parse_minute(display_clock)

        # Determine home/away competitors (score + team name for reconciliation)
        score_home: int | None = None
        score_away: int | None = None
        home_team: str | None = None
        away_team: str | None = None
        for competitor in comp.get("competitors", []):
            try:
                score_val = int(competitor["score"])
            except (KeyError, ValueError, TypeError):
                score_val = None
            team_name = (competitor.get("team") or {}).get("displayName")
            if competitor.get("homeAway") == "home":
                score_home = score_val
                home_team = team_name
            elif competitor.get("homeAway") == "away":
                score_away = score_val
                away_team = team_name

        # Penalty-shootout result (knockout matches). Reads competitor.shootoutScore
        # + competitor.winner, gated on shootoutScore present on both sides; all None
        # for a non-shootout match. Carried in payload alongside the HT scores — core
        # NormalizedMatch columns stay sport-agnostic (DESIGN.md §5).
        score_pen_home, score_pen_away, pen_winner_side = _extract_shootout(
            comp.get("competitors", []), match_id
        )

        kickoff_utc = ev.get("date") or comp.get("date") or None

        # Schedule-metadata the scoreboard event already carries (no extra request):
        # venue full name, host city, and the tournament phase (season.slug). The
        # GROUP letter is NOT in the scoreboard payload, so group_name is left unset
        # (it would need the summary/standings endpoint — a heavier follow-up).
        # These land in NormalizedMatch.payload — schedule metadata lives in the
        # matches.payload JSON column, not core columns (plan pin).
        venue = comp.get("venue") or {}
        stadium = venue.get("fullName") or None
        # ESPN city is "Houston, Texas" / "Kansas City, Missouri"; keep the city name
        # (before the first comma) to match the seeded-schedule city convention.
        raw_city = (venue.get("address") or {}).get("city")
        city = raw_city.split(",")[0].strip() if raw_city else None
        # season.slug is e.g. "group-stage" / "round-of-32" → "Group Stage".
        slug = (ev.get("season") or {}).get("slug")
        round_name = slug.replace("-", " ").title() if slug else None

        return NormalizedMatch(
            match_id=match_id,
            status=status,
            minute=minute,
            score_home=score_home,
            score_away=score_away,
            display_clock=display_clock,
            events=[],
            home_team=home_team,
            away_team=away_team,
            kickoff_utc=kickoff_utc,
            payload={
                "score_ht_home": None,
                "score_ht_away": None,
                "stadium": stadium,
                "city": city,
                "round_name": round_name,
                "group_name": None,
                "score_pen_home": score_pen_home,
                "score_pen_away": score_pen_away,
                "pen_winner_side": pen_winner_side,
                "result_type": _result_type_from_comp(comp),
            },
        )

    def _normalize_summary(self, match_id: str, data: dict) -> NormalizedMatch:
        """Normalize ESPN summary JSON into a NormalizedMatch with events + payload extras."""
        # Derive status from header if available, else UNKNOWN
        status = MatchStatus.UNKNOWN
        display_clock: str | None = None
        minute: int | None = None
        score_home: int | None = None
        score_away: int | None = None
        score_ht_home: int | None = None
        score_ht_away: int | None = None

        # Match-identity fields for reconciliation
        home_team: str | None = None
        away_team: str | None = None
        kickoff_utc: str | None = None

        # Penalty-shootout result (knockout matches). Defaults None; set from the
        # first competition's competitors[] below.
        score_pen_home: int | None = None
        score_pen_away: int | None = None
        pen_winner_side: str | None = None
        # regulation/extra_time/penalties, derived from the first competition.
        # Defaulted here so a headerless / empty-competitions summary normalizes
        # to None rather than reading an unbound loop variable below.
        result_type: str | None = None

        # Try to get status from header competitions
        header = data.get("header", {})
        for comp in header.get("competitions", []):
            status = _status_from_comp(comp)
            display_clock = comp.get("status", {}).get("displayClock") or None
            minute = _parse_minute(display_clock)
            kickoff_utc = comp.get("date") or None
            for competitor in comp.get("competitors", []):
                try:
                    score_val = int(competitor.get("score", ""))
                except (ValueError, TypeError):
                    score_val = None
                team_name = (competitor.get("team") or {}).get("displayName")
                # linescores carries per-period scores; the first entry is the
                # first-half score (present once the half ends).
                linescores = competitor.get("linescores") or []
                ht_val: int | None = None
                if linescores and isinstance(linescores[0], dict):
                    try:
                        ht_val = int(linescores[0].get("displayValue", ""))
                    except (ValueError, TypeError):
                        ht_val = None
                if competitor.get("homeAway") == "home":
                    score_home = score_val
                    home_team = team_name
                    score_ht_home = ht_val
                elif competitor.get("homeAway") == "away":
                    score_away = score_val
                    away_team = team_name
                    score_ht_away = ht_val
            # Reads competitor.shootoutScore + competitor.winner, gated on
            # shootoutScore present on both sides; all None for a non-shootout match.
            score_pen_home, score_pen_away, pen_winner_side = _extract_shootout(
                comp.get("competitors", []), match_id
            )
            result_type = _result_type_from_comp(comp)
            break  # Only need first competition

        # keyEvents → NormalizedEvent list. A keyEvent may expand to two events
        # (the ``yellow-red-card`` second-yellow → YELLOW + RED split), so ``seq``
        # is assigned sequentially AFTER expansion rather than from the source
        # keyEvents index — otherwise the two split rows would share a seq and one
        # would be dropped by ``(match_id, seq)`` INSERT OR IGNORE, and later
        # events would collide on the source index.
        events: list[NormalizedEvent] = []
        for raw_ev in data.get("keyEvents", []):
            for norm_ev in _normalize_key_event(raw_ev):
                norm_ev.seq = len(events)
                events.append(norm_ev)

        # boxscore.teams → NormalizedStats list (payload-carried; the football
        # side tables are the persisted home for these, written by the engine).
        stats: list[NormalizedStats] = []
        for team_block in data.get("boxscore", {}).get("teams", []):
            ns = _normalize_team_stats(team_block, match_id)
            if ns is not None:
                stats.append(ns)

        # commentary[] → in-memory list of human-readable text. Each ESPN
        # commentary[] entry is an object carrying a "text" field; we carry only
        # the text. Prose belongs to consuming apps — there is no commentary
        # table in this schema; the list travels in payload for parity.
        commentary: list[str] = []
        for raw_c in data.get("commentary", []):
            text = (raw_c.get("text") or "").strip() if isinstance(raw_c, dict) else ""
            if text:
                commentary.append(text)

        # rosters[] → per-match lineups (starting XI / bench / formation).
        # Empty list when the summary carries no `rosters` block; team blocks
        # missing a display name are skipped.
        lineups: list[NormalizedLineup] = []
        # `or []` (not a .get default): a present-but-null "rosters" key must not
        # TypeError the whole summary parse (score/events/commentary would be lost).
        for roster_block in data.get("rosters") or []:
            lineup = _normalize_roster(roster_block)
            if lineup is not None:
                lineups.append(lineup)

        return NormalizedMatch(
            match_id=match_id,
            status=status,
            minute=minute,
            score_home=score_home,
            score_away=score_away,
            display_clock=display_clock,
            events=events,
            home_team=home_team,
            away_team=away_team,
            kickoff_utc=kickoff_utc,
            # Football extras travel in payload (JSON-serializable): HT scores,
            # boxscore stats, lineups, and commentary text.
            payload={
                "score_ht_home": score_ht_home,
                "score_ht_away": score_ht_away,
                "stats": [asdict(s) for s in stats],
                "lineups": [asdict(lu) for lu in lineups],
                "commentary": commentary,
                "score_pen_home": score_pen_home,
                "score_pen_away": score_pen_away,
                "pen_winner_side": pen_winner_side,
                "result_type": result_type,
            },
        )
