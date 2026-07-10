"""Golden capture for the knockout-stage fixtures — NOT a test.

Regenerates the ``*.normalized.json`` goldens for the real WC2026 R32/R16
captures (penalty shootout, extra-time win, stoppage-time regulation win) by
replaying each committed ``.summary.json`` through the CURRENT
``gamecollect_football`` adapter and serializing with
``_football_helpers.serialize_knockout``.

Unlike ``capture_golden.py`` (which captured the qatar_canada golden ONCE from
the external gamealerts adapter for port parity), these goldens are self-captured
from this repo's own adapter — the adapter is the source of truth and the golden
is a regression guard. Run after an intentional, reviewed adapter change:

    cd <repo root>
    PYTHONPATH=src uv run python tests/football/golden/capture_knockout_goldens.py

Each raw summary is a real ESPN ``.../fifa.world/summary?event=<id>`` capture.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Make the sibling test helper importable when run as a standalone script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _football_helpers import (  # noqa: E402  (path insert must precede import)
    KNOCKOUT_FIXTURES,
    KNOCKOUT_GOLDEN_DIR,
    replay_fixture,
    serialize_knockout,
)


def main() -> int:
    for fx in KNOCKOUT_FIXTURES:
        match = replay_fixture(fx["summary"], fx["match_id"])
        golden_path = KNOCKOUT_GOLDEN_DIR / fx["golden"]
        golden_path.write_text(
            json.dumps(serialize_knockout(match), sort_keys=True, indent=2, ensure_ascii=False)
            + "\n"
        )
        print(f"wrote {golden_path.name} ({len(match.events)} events, {fx['scenario']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
