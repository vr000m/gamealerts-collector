"""Session fixtures for the Phase 4 football-pack tests.

Shared constants/helpers live in ``_football_helpers.py`` (mirroring the
``_phase2_helpers`` convention); this conftest only wires them into fixtures.
"""

from __future__ import annotations

import json

import pytest
from _football_helpers import GOLDEN_PATH, PACK_NAME, replay


@pytest.fixture(scope="session")
def golden() -> dict:
    return json.loads(GOLDEN_PATH.read_text())


@pytest.fixture(scope="session")
def replayed_match():
    return replay()


@pytest.fixture(scope="session")
def football_pack():
    from gamecollect.packs.registry import load_pack

    return load_pack(PACK_NAME)
