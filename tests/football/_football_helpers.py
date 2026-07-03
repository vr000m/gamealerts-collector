"""Shared helpers for the Phase 4 football-pack tests (see conftest.py).

Plan contract (docs/dev_plans/20260702-feature-collector-foundation.md, Phase 4):
- The pack ships as ``gamecollect_football`` (espn.py, reconcile.py, taxonomy.py,
  pack.py, fixtures/) and registers the ``football-wc2026`` entry point.
- Adapter parity is asserted against the FROZEN golden
  ``golden/qatar_canada_760440.normalized.json``, captured ONCE from
  gamealerts' adapter by ``golden/capture_golden.py`` (see its docstring for
  provenance). gamealerts is NEVER imported by these tests.
- ``serialize_match`` below MUST stay in sync with ``_serialize`` in
  ``golden/capture_golden.py`` — same fields, same enum->value rule — so the
  golden comparison is byte-for-byte meaningful.

These tests may fail at collection until the parallel implementer lands
``src/gamecollect_football`` and the entry point is installed (``uv sync``);
that is a timing artifact, not a test bug.
"""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
# Checked in by the Phase 4 implementer (copied from gamealerts data/fixtures/).
FIXTURE_PATH = (
    REPO_ROOT / "src" / "gamecollect_football" / "fixtures" / "qatar_canada_760440.summary.json"
)
GOLDEN_PATH = Path(__file__).resolve().parent / "golden" / "qatar_canada_760440.normalized.json"

MATCH_ID = "760440"
PACK_NAME = "football-wc2026"

# gamealerts data/provider.py EventType value set, hardcoded (the parity
# oracle must not import gamealerts).
GAMEALERTS_EVENT_TYPE_VALUES = frozenset(
    {
        "goal",
        "own_goal",
        "penalty",
        "yellow",
        "red",
        "sub",
        "kickoff",
        "half_time",
        "full_time",
    }
)

# gamealerts EVENT_IMPORTANCE defaults (EventImportance.value per EventType.value).
GAMEALERTS_IMPORTANCE_DEFAULTS = {
    "goal": 1,
    "own_goal": 1,
    "penalty": 1,
    "red": 2,
    "full_time": 2,
    "yellow": 3,
    "half_time": 3,
    "kickoff": 3,
    "sub": 4,
}


def _plain(value):
    """Enum -> its .value; plain str/int pass through (post-generalization)."""
    return getattr(value, "value", value)


def serialize_match(match) -> dict:
    """Serialize a normalized match exactly like golden/capture_golden.py."""
    return {
        "match": {
            "match_id": match.match_id,
            "status": _plain(match.status),
            "minute": match.minute,
            "score_home": match.score_home,
            "score_away": match.score_away,
            "display_clock": match.display_clock,
            "home_team": match.home_team,
            "away_team": match.away_team,
            "kickoff_utc": match.kickoff_utc,
        },
        "events": [
            {
                "seq": ev.seq,
                "minute": ev.minute,
                "event_type": _plain(ev.event_type),
                "importance": _plain(ev.importance),
                "team": ev.team,
                "player": ev.player,
                "assist": ev.assist,
                "detail": ev.detail,
            }
            for ev in match.events
        ],
    }


def load_raw_summary() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def replay():
    """Feed the checked-in fixture through the PORTED adapter (no network)."""
    from gamecollect_football.espn import ESPNAdapter

    raw = load_raw_summary()
    return ESPNAdapter(http_get=lambda url, params=None: raw).fetch_match_detail(MATCH_ID)
