"""One-time golden capture script — NOT a test; never imported by the test suite.

Provenance
----------
The frozen golden file ``qatar_canada_760440.normalized.json`` in this
directory was captured ONCE from the gamealerts repo's ESPN adapter by
running this script inside the gamealerts backend environment:

    cd /Users/vr000m/Code/vr000m/gamealerts/backend
    uv run python <this repo>/tests/football/golden/capture_golden.py

It replays the committed fixture
``backend/gamealerts/data/fixtures/qatar_canada_760440.summary.json``
(Canada 6-0 Qatar, ESPN event 760440, 2026-06-18; augmented with one
synthetic ``yellow-red-card`` keyEvent marked ``_test_data``) through
gamealerts' real ``ESPNAdapter.fetch_match_detail`` normalizer and
serializes the normalized match header + events deterministically
(enums -> ``.value``, ``sort_keys=True``, ``indent=2``).

The parity tests in ``tests/football/`` assert the ported
``gamecollect_football`` adapter reproduces this output exactly using the
same serialization rules (see ``tests/football/conftest.py`` —
``serialize_match`` there MUST stay in sync with ``_serialize`` below).
gamealerts is imported ONLY here, never at test time.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

GAMEALERTS_FIXTURE = Path(
    "/Users/vr000m/Code/vr000m/gamealerts/backend/gamealerts/data/fixtures/"
    "qatar_canada_760440.summary.json"
)
GOLDEN_PATH = Path(__file__).resolve().parent / "qatar_canada_760440.normalized.json"


def _plain(value):
    """Enum -> its .value; everything else passes through unchanged."""
    return getattr(value, "value", value)


def _serialize(match) -> dict:
    """Keep in sync with tests/football/conftest.py::serialize_match."""
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


def main() -> int:
    from gamealerts.data.espn import ESPNAdapter  # gamealerts env only

    raw = json.loads(GAMEALERTS_FIXTURE.read_text())
    match = ESPNAdapter(http_get=lambda url, params=None: raw).fetch_match_detail("760440")
    GOLDEN_PATH.write_text(
        json.dumps(_serialize(match), sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    )
    print(f"wrote {GOLDEN_PATH} ({len(match.events)} events)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
