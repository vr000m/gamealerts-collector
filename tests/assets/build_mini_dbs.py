"""Builder for the checked-in synthetic gamealerts-schema mini databases.

CI must never touch ``~/.local/share/gamealerts`` (the live seed DBs are used
only for the one-time fixture import). The importer's two-schema-shape tolerance
is instead exercised against two tiny, deterministic SQLite files checked in
under ``tests/assets/``:

* ``gamealerts_mini_new.db`` — the **newer** gamealerts shape: ``rosters`` has a
  ``player_name_folded`` column and a per-match ``lineups`` table exists.
* ``gamealerts_mini_old.db`` — the **older** shape (the fictional DB's shape):
  ``rosters`` has NO ``player_name_folded`` column and there is NO ``lineups``
  table. The importer must tolerate both.

Both files carry the gamealerts ``matches``/``events`` schema exactly (see the
live DBs). Contents are fixed so a test can assert exact ground truth:

``gamealerts_mini_new.db`` — match ``mini:new1`` (Alpha 1, Beta 1, FINISHED):
    4 events, seqs 0-3: kickoff, goal (Alpha / "Álvaro Núñez"),
    goal (Beta / "Bob"), half_time. Distinct types: kickoff, goal, half_time.
    ``lineups`` has 2 rows (one Alpha, one Beta); ``rosters`` carries
    ``player_name_folded``.

``gamealerts_mini_old.db`` — match ``mini:old1`` (Gamma 2, Delta 0, FINISHED):
    3 events, seqs 0-2: goal (Gamma), goal (Gamma), full_time. Distinct types:
    goal, full_time. No ``lineups`` table; ``rosters`` has no folded column.

All event ``type`` values are present in the football taxonomy, so an importer
type-vs-taxonomy check passes for both.

Run ``python tests/assets/build_mini_dbs.py`` to (re)generate the two .db files
next to this script. The ``build_mini_new``/``build_mini_old`` functions are
importable so a test can also build a throwaway copy into a temp dir.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from gamecollect.fold import fold

ASSETS_DIR = Path(__file__).resolve().parent
MINI_NEW_DB = ASSETS_DIR / "gamealerts_mini_new.db"
MINI_OLD_DB = ASSETS_DIR / "gamealerts_mini_old.db"

# gamealerts core schema (ported verbatim from the live DBs). Identical across
# both shapes for matches/events; the shapes differ only in rosters/lineups.
_MATCHES_DDL = """
CREATE TABLE matches (
    match_id    TEXT PRIMARY KEY,
    round       TEXT,
    match_date  TEXT,
    kickoff_utc TEXT NOT NULL,
    team1       TEXT NOT NULL,
    team2       TEXT NOT NULL,
    group_name  TEXT,
    city        TEXT,
    stadium     TEXT,
    status      TEXT,
    minute      INTEGER,
    score_home  INTEGER,
    score_away  INTEGER,
    score_ht_home INTEGER,
    score_ht_away INTEGER,
    updated_at  TEXT
);
"""

_EVENTS_DDL = """
CREATE TABLE events (
    match_id    TEXT NOT NULL REFERENCES matches(match_id),
    seq         INTEGER NOT NULL,
    minute      INTEGER,
    type        TEXT NOT NULL,
    detail      TEXT,
    team        TEXT,
    player      TEXT,
    assist      TEXT,
    PRIMARY KEY (match_id, seq)
);
"""

# Newer shape: rosters carries player_name_folded; a lineups table exists.
_ROSTERS_DDL_NEW = """
CREATE TABLE rosters (
    team            TEXT NOT NULL,
    player_name     TEXT NOT NULL,
    number          INTEGER,
    position        TEXT,
    date_of_birth   TEXT,
    player_name_folded TEXT,
    PRIMARY KEY (team, player_name)
);
"""

_LINEUPS_DDL_NEW = """
CREATE TABLE lineups (
    match_id            TEXT NOT NULL REFERENCES matches(match_id),
    athlete_id          TEXT NOT NULL,
    team                TEXT,
    home_away           TEXT,
    display_name        TEXT NOT NULL,
    display_name_folded TEXT NOT NULL,
    jersey              TEXT,
    position            TEXT,
    starter             INTEGER NOT NULL DEFAULT 0,
    subbed_in           INTEGER NOT NULL DEFAULT 0,
    subbed_out          INTEGER NOT NULL DEFAULT 0,
    formation_place     INTEGER,
    formation           TEXT,
    PRIMARY KEY (match_id, athlete_id)
);
"""

# Older shape (the fictional DB): rosters WITHOUT player_name_folded, no lineups.
_ROSTERS_DDL_OLD = """
CREATE TABLE rosters (
    team            TEXT NOT NULL,
    player_name     TEXT NOT NULL,
    number          INTEGER,
    position        TEXT,
    date_of_birth   TEXT,
    PRIMARY KEY (team, player_name)
);
"""


def _connect_fresh(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    return sqlite3.connect(str(path))


def build_mini_new(path: Path | str = MINI_NEW_DB) -> Path:
    """Write the newer-shape mini DB (rosters folded column + lineups table)."""
    path = Path(path)
    conn = _connect_fresh(path)
    try:
        conn.executescript(_MATCHES_DDL + _EVENTS_DDL + _ROSTERS_DDL_NEW + _LINEUPS_DDL_NEW)
        conn.execute(
            "INSERT INTO matches (match_id, round, match_date, kickoff_utc, team1, team2, "
            "group_name, city, stadium, status, minute, score_home, score_away, "
            "score_ht_home, score_ht_away, updated_at) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "mini:new1",
                "Group Stage",
                "2026-06-15",
                "2026-06-15T18:00:00+00:00",
                "Alpha",
                "Beta",
                "Group A",
                "Testville",
                "Test Arena",
                "FINISHED",
                90,
                1,
                1,
                1,
                1,
                "2026-06-15T20:00:00+00:00",
            ),
        )
        events = [
            ("mini:new1", 0, None, "kickoff", None, None, None, None),
            ("mini:new1", 1, 12, "goal", "Goal! Alpha 1, Beta 0.", "Alpha", "Álvaro Núñez", None),
            ("mini:new1", 2, 40, "goal", "Goal! Alpha 1, Beta 1.", "Beta", "Bob", "Chris"),
            ("mini:new1", 3, 45, "half_time", None, None, None, None),
        ]
        conn.executemany(
            "INSERT INTO events (match_id, seq, minute, type, detail, team, player, assist) "
            "VALUES (?,?,?,?,?,?,?,?)",
            events,
        )
        conn.executemany(
            "INSERT INTO rosters (team, player_name, number, position, date_of_birth, "
            "player_name_folded) VALUES (?,?,?,?,?,?)",
            [
                ("Alpha", "Álvaro Núñez", 9, "F", "1998-01-01", fold("Álvaro Núñez")),
                ("Beta", "Bob", 10, "M", "1997-02-02", fold("Bob")),
            ],
        )
        conn.executemany(
            "INSERT INTO lineups (match_id, athlete_id, team, home_away, display_name, "
            "display_name_folded, jersey, position, starter, subbed_in, subbed_out, "
            "formation_place, formation) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    "mini:new1",
                    "a1",
                    "Alpha",
                    "home",
                    "Álvaro Núñez",
                    fold("Álvaro Núñez"),
                    "9",
                    "F",
                    1,
                    0,
                    0,
                    11,
                    "4-3-3",
                ),
                (
                    "mini:new1",
                    "b1",
                    "Beta",
                    "away",
                    "Bob",
                    fold("Bob"),
                    "10",
                    "M",
                    1,
                    0,
                    0,
                    8,
                    "4-4-2",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return path


def build_mini_old(path: Path | str = MINI_OLD_DB) -> Path:
    """Write the older-shape mini DB (no rosters folded column, no lineups table)."""
    path = Path(path)
    conn = _connect_fresh(path)
    try:
        conn.executescript(_MATCHES_DDL + _EVENTS_DDL + _ROSTERS_DDL_OLD)
        conn.execute(
            "INSERT INTO matches (match_id, round, match_date, kickoff_utc, team1, team2, "
            "group_name, city, stadium, status, minute, score_home, score_away, "
            "score_ht_home, score_ht_away, updated_at) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "mini:old1",
                "Group Stage",
                "2026-06-16",
                "2026-06-16T18:00:00+00:00",
                "Gamma",
                "Delta",
                "Group B",
                "Oldtown",
                "Legacy Field",
                "FINISHED",
                90,
                2,
                0,
                1,
                0,
                "2026-06-16T20:00:00+00:00",
            ),
        )
        events = [
            ("mini:old1", 0, 22, "goal", "Goal! Gamma 1, Delta 0.", "Gamma", "Grace", None),
            ("mini:old1", 1, 55, "goal", "Goal! Gamma 2, Delta 0.", "Gamma", "Grace", "Heidi"),
            ("mini:old1", 2, 90, "full_time", None, None, None, None),
        ]
        conn.executemany(
            "INSERT INTO events (match_id, seq, minute, type, detail, team, player, assist) "
            "VALUES (?,?,?,?,?,?,?,?)",
            events,
        )
        conn.executemany(
            "INSERT INTO rosters (team, player_name, number, position, date_of_birth) "
            "VALUES (?,?,?,?,?)",
            [
                ("Gamma", "Grace", 7, "F", "1996-03-03"),
                ("Delta", "Heidi", 4, "D", "1995-04-04"),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return path


def main() -> None:
    new_path = build_mini_new()
    old_path = build_mini_old()
    print(f"wrote {new_path}")
    print(f"wrote {old_path}")


if __name__ == "__main__":
    main()
