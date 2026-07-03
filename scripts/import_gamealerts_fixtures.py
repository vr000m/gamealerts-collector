#!/usr/bin/env python3
"""Import gamealerts-schema SQLite matches into replayable fixture JSON.

Bridges the live gamealerts databases to the checked-in seed fixtures
(DESIGN.md §7): reads named matches from a gamealerts-schema SQLite file, maps
each to the collector's :class:`~gamecollect.provider.NormalizedMatch` shape,
and writes a fixture via :mod:`gamecollect.fixture_io` (the same module the
engine's ``--record`` and :class:`~gamecollect.replay.ReplayProvider` use).

Two known gamealerts schema shapes are tolerated (the fictional DB is older):

* newer — ``rosters`` has ``player_name_folded`` and a per-match ``lineups``
  table exists (the squad snapshot is imported from it);
* older — neither is present (the squad snapshot is left empty).

Event-field mapping (gamealerts → collector). The gamealerts events schema is
external to this repo, so this map is the contract. A gamealerts event row
``(match_id, seq, minute, type, detail, team, player, assist)`` becomes a
:class:`~gamecollect.provider.NormalizedEvent`:

* ``type`` → the football taxonomy key (gamealerts already keys events by the
  taxonomy strings ``goal``/``own_goal``/``penalty``/``yellow``/``red``/``sub``/
  ``kickoff``/``half_time``/``full_time``); ``importance`` is taken from the
  pack taxonomy's default for that type.
* ``minute``/``detail``/``team``/``player``/``assist`` are preserved verbatim.

The fixture is the faithful **source** record; the collector's derived event
columns are computed downstream from these preserved fields, not baked in here:
``period`` from ``minute`` (half 1 for ≤45(+stoppage), half 2 above; NULL when
``minute`` is missing), ``actor_entity`` from ``player`` (a source-qualified
fold of the name), ``target_entity`` from ``assist``, entity ``parent`` from
``team``. The engine's writer lands ``team``/``player``/``assist`` in the
event ``payload`` when it collects a replayed fixture.

Guards (the live DBs are mutable, with active WAL files):

* Import re-asserts a pinned ground-truth snapshot (event count, score, status)
  for each seed match and fails loudly on any drift from the recorded truth.
* Every distinct gamealerts ``type`` in an imported match must have a taxonomy
  entry — an undeclared type fails the import rather than producing a fixture
  the writer would later reject.

Usage::

    # One-time seed import of all four fixtures from the default live DBs:
    python scripts/import_gamealerts_fixtures.py --out fixtures/football

    # Validate checked-in fixtures parse and replay deterministically:
    python scripts/import_gamealerts_fixtures.py --check fixtures/football

The live DBs are opened READ-ONLY (``mode=ro`` URI); this script never writes to
them. CI validates via ``--check`` only — it never touches ``~/.local/share``.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from gamecollect.fixture_io import (
    Fixture,
    FixtureFormatError,
    fixture_stem,
    read_fixture,
    write_fixture,
)
from gamecollect.fold import fold
from gamecollect.provider import MatchStatus, NormalizedEvent, NormalizedMatch
from gamecollect.replay import ReplayProvider
from gamecollect_football.taxonomy import EVENT_IMPORTANCE, TAXONOMY

# Default live seed-DB locations (used only for the one-time import; never in CI).
DEFAULT_REAL_DB = Path.home() / ".local/share/gamealerts/gamealerts.db"
DEFAULT_FICTIONAL_DB = Path.home() / ".local/share/gamealerts/gamealerts-fictional.db"


class ImportError_(Exception):
    """An import invariant failed (ground-truth drift, undeclared type, ...)."""


@dataclass(frozen=True)
class SeedMatch:
    """One seed fixture: its source DB, gamealerts match id, and ground truth."""

    fixture_name: str
    db: str  # "real" | "fictional"
    match_id: str
    score_home: int
    score_away: int
    status: str
    event_count: int


# The four seed fixtures, with ground truth pinned as of the plan's Context
# (snapshot 2026-07-02, re-verified 2026-07-03 against the live DBs). This
# corrects DESIGN.md §7's ambiguity about which DB holds which match: the three
# real matches live in gamealerts.db; canada-qatar lives in the fictional DB.
SEED_MATCHES: tuple[SeedMatch, ...] = (
    SeedMatch("morocco-haiti-espn760464", "real", "espn:760464", 4, 2, "FINISHED", 21),
    SeedMatch("nz-belgium-espn760477", "real", "espn:760477", 1, 5, "FINISHED", 20),
    SeedMatch("turkey-usa-espn760470", "real", "espn:760470", 3, 2, "FINISHED", 18),
    SeedMatch(
        "canada-qatar-fictional", "fictional", "2026-06-18_canada_vs_qatar_9", 6, 0, "FINISHED", 6
    ),
)


# ---------------------------------------------------------------------------
# Read-only DB access
# ---------------------------------------------------------------------------


def open_readonly(path: str | Path) -> sqlite3.Connection:
    """Open a gamealerts SQLite file READ-ONLY (never mutate the live seed DBs)."""
    escaped = urllib.parse.quote(str(Path(path)))
    conn = sqlite3.connect(f"file:{escaped}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(r["name"] == column for r in conn.execute(f"PRAGMA table_info({table})"))


def _pick(row: sqlite3.Row, *names: str, default: Any = None) -> Any:
    """First present column among ``names`` (schema-name tolerance), else default.

    The gamealerts ``matches``/``lineups`` column names are not pinned by the plan
    (only the ``events`` tuple is), so the live DBs and the synthetic test DBs use
    different names for the same field (``team1`` vs ``home_team``, ``score_home``
    vs ``home_score``). Reading through this helper lets one importer serve both.
    """
    keys = set(row.keys())
    for name in names:
        if name in keys:
            return row[name]
    return default


# ---------------------------------------------------------------------------
# Mapping: gamealerts row -> NormalizedMatch + squad snapshot
# ---------------------------------------------------------------------------


def verify_event_types(conn: sqlite3.Connection, match_id: str) -> None:
    """Fail unless every distinct gamealerts ``type`` for the match is declared.

    An undeclared type would be rejected by the writer's taxonomy check at
    replay time; surfacing it here fails the import loudly instead of shipping a
    fixture that cannot be replayed.
    """
    rows = conn.execute(
        "SELECT DISTINCT type FROM events WHERE match_id = ?", (match_id,)
    ).fetchall()
    undeclared = sorted(r["type"] for r in rows if r["type"] not in TAXONOMY)
    if undeclared:
        raise ImportError_(
            f"match {match_id!r} has gamealerts event type(s) {undeclared} with no "
            f"football taxonomy entry (declared: {sorted(TAXONOMY)})"
        )


def read_match(
    conn: sqlite3.Connection, match_id: str
) -> tuple[NormalizedMatch, list[dict[str, Any]]]:
    """Read one gamealerts match into a NormalizedMatch plus its squad snapshot.

    Returns ``(match, squad)`` where ``squad`` is the per-match lineup rows
    (football-side-table shaped) when the source DB carries a ``lineups`` table,
    else an empty list (older schema shape). Raises :class:`ImportError_` when
    the match id is unknown.
    """
    header = conn.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
    if header is None:
        raise ImportError_(f"match {match_id!r} not found in source database")

    verify_event_types(conn, match_id)

    event_rows = conn.execute(
        "SELECT seq, minute, type, detail, team, player, assist FROM events "
        "WHERE match_id = ? ORDER BY seq",
        (match_id,),
    ).fetchall()
    events = [
        NormalizedEvent(
            seq=row["seq"],
            minute=row["minute"],
            event_type=row["type"],
            importance=EVENT_IMPORTANCE[row["type"]],
            team=row["team"],
            player=row["player"],
            assist=row["assist"],
            detail=row["detail"],
        )
        for row in event_rows
    ]

    raw_status = _pick(header, "status")
    try:
        status = MatchStatus(raw_status)
    except ValueError as exc:
        raise ImportError_(f"match {match_id!r} has unrecognized status {raw_status!r}") from exc

    # Schedule metadata + half-time scores travel in payload (matches.payload
    # JSON column convention), mirroring the ESPN adapter's payload shape. All
    # optional — absent columns (the synthetic test schema carries none of these)
    # resolve to None.
    payload = {
        "round_name": _pick(header, "round", "round_name"),
        "group_name": _pick(header, "group_name"),
        "city": _pick(header, "city"),
        "stadium": _pick(header, "stadium"),
        "score_ht_home": _pick(header, "score_ht_home"),
        "score_ht_away": _pick(header, "score_ht_away"),
    }

    match = NormalizedMatch(
        match_id=_pick(header, "match_id"),
        status=status,
        minute=_pick(header, "minute"),
        score_home=_pick(header, "score_home", "home_score"),
        score_away=_pick(header, "score_away", "away_score"),
        display_clock=None,  # gamealerts matches carries no display clock
        events=events,
        home_team=_pick(header, "team1", "home_team"),
        away_team=_pick(header, "team2", "away_team"),
        kickoff_utc=_pick(header, "kickoff_utc", "kickoff"),
        payload=payload,
    )
    squad = _read_squad(conn, match_id)
    return match, squad


def _read_squad(conn: sqlite3.Connection, match_id: str) -> list[dict[str, Any]]:
    """Per-match squad snapshot from the ``lineups`` table, or [] if absent.

    Tolerates the older schema shape (no ``lineups`` table). Rows are shaped to
    the football ``football_lineups`` side table so a consumer/eval harness can
    load them directly; ``name_folded`` is the core fold of the display name.
    """
    if not _table_exists(conn, "lineups"):
        return []
    rows = conn.execute(
        "SELECT * FROM lineups WHERE match_id = ? ORDER BY team", (match_id,)
    ).fetchall()
    squad: list[dict[str, Any]] = []
    for row in rows:
        # Tolerant column reads: the live ``lineups`` uses ``display_name`` +
        # ``athlete_id``; the synthetic test schema uses ``player`` and no
        # athlete id. The entities snapshot is informational (not replayed by the
        # sport-agnostic engine), so a best-effort projection is enough.
        display_name = _pick(row, "display_name", "player", default="")
        folded = _pick(row, "display_name_folded", "name_folded") or fold(display_name)
        squad.append(
            {
                "match_id": match_id,
                "team": _pick(row, "team"),
                "athlete_id": _pick(row, "athlete_id"),
                "display_name": display_name,
                "name_folded": folded,
                "jersey": _pick(row, "jersey"),
                "position": _pick(row, "position"),
                "starter": _pick(row, "starter", default=0),
                "subbed_in": _pick(row, "subbed_in", default=0),
                "subbed_out": _pick(row, "subbed_out", default=0),
                "formation_place": _pick(row, "formation_place"),
                "home_away": _pick(row, "home_away"),
                "formation": _pick(row, "formation"),
            }
        )
    return squad


def assert_ground_truth(match: NormalizedMatch, seed: SeedMatch) -> None:
    """Re-assert the pinned ground truth against the freshly-read live match.

    The seed DBs are mutable; a drift from the recorded counts/scores/status
    means the snapshot this repo pins is stale — fail loudly rather than ship a
    fixture that no longer matches the documented ground truth.
    """
    actual = (len(match.events), match.score_home, match.score_away, match.status.value)
    expected = (seed.event_count, seed.score_home, seed.score_away, seed.status)
    if actual != expected:
        raise ImportError_(
            f"ground-truth drift for {seed.fixture_name} ({seed.match_id!r}): "
            f"expected (events, home, away, status)={expected}, got {actual}. "
            f"The live DB changed since the pinned snapshot; re-verify before importing."
        )


# ---------------------------------------------------------------------------
# Import + check
# ---------------------------------------------------------------------------


def import_match(
    db_path: str | Path,
    match_id: str,
    out_path: str | Path,
    *,
    seed: SeedMatch | None = None,
) -> Fixture:
    """Read a match from ``db_path`` (read-only) and write its fixture JSON.

    When ``seed`` is given its ground truth is re-asserted before the fixture is
    written; the squad snapshot (if any) is carried in the fixture's ``entities``
    slot. Returns the written :class:`~gamecollect.fixture_io.Fixture`.
    """
    conn = open_readonly(db_path)
    try:
        match, squad = read_match(conn, match_id)
    finally:
        conn.close()
    if seed is not None:
        assert_ground_truth(match, seed)
    write_fixture(out_path, match, entities=squad)
    return read_fixture(out_path)


def import_all_matches(db_path: str | Path, out_dir: str | Path) -> list[Path]:
    """Import EVERY match in ``db_path`` (read-only) into ``out_dir``.

    The generic path for an arbitrary gamealerts-schema DB (no pinned ground
    truth): one fixture per ``matches`` row, named by the sanitized match id.
    Used to import a whole DB (e.g. the synthetic test DBs); the pinned seed
    import with ground-truth re-assertion is :func:`import_seed_matches`.
    """
    out_dir = Path(out_dir)
    conn = open_readonly(db_path)
    try:
        match_ids = [
            r["match_id"] for r in conn.execute("SELECT match_id FROM matches ORDER BY match_id")
        ]
    finally:
        conn.close()
    if not match_ids:
        raise ImportError_(f"no matches found in {db_path}")
    written: list[Path] = []
    for match_id in match_ids:
        out_path = out_dir / f"{fixture_stem(match_id)}.json"
        import_match(db_path, match_id, out_path)
        written.append(out_path)
        print(f"imported {match_id} -> {out_path}")
    return written


def import_seed_matches(
    out_dir: str | Path,
    *,
    real_db: str | Path = DEFAULT_REAL_DB,
    fictional_db: str | Path = DEFAULT_FICTIONAL_DB,
) -> list[Path]:
    """Import all four seed fixtures from the live DBs into ``out_dir``."""
    out_dir = Path(out_dir)
    db_for = {"real": Path(real_db), "fictional": Path(fictional_db)}
    written: list[Path] = []
    for seed in SEED_MATCHES:
        db_path = db_for[seed.db]
        if not db_path.exists():
            raise ImportError_(f"seed DB for {seed.fixture_name} not found: {db_path}")
        out_path = out_dir / f"{seed.fixture_name}.json"
        import_match(db_path, seed.match_id, out_path, seed=seed)
        written.append(out_path)
        print(
            f"imported {seed.fixture_name}: {seed.score_home}-{seed.score_away} "
            f"{seed.status}, {seed.event_count} events -> {out_path}"
        )
    return written


def check_fixture(path: str | Path) -> None:
    """Validate one fixture parses and replays deterministically.

    Replays the fixture twice through :class:`ReplayProvider` (step mode) and
    asserts the two revealed event streams are identical, and that a full replay
    reproduces the fixture's own recorded events. Raises on any mismatch.
    """
    fixture = read_fixture(path)
    expected = [
        (e.seq, e.minute, e.event_type, e.importance, e.team, e.player, e.assist, e.detail)
        for e in sorted(fixture.match.events, key=lambda e: e.seq)
    ]
    streams = []
    for _ in range(2):
        provider = ReplayProvider(fixture, speed=float("inf"))
        seen: list[tuple[Any, ...]] = []
        # Poll one event past the end so exhaustion is observed deterministically.
        for _ in range(len(expected) + 1):
            snapshot = provider.fetch_live_matches()[0]
            seen = [
                (e.seq, e.minute, e.event_type, e.importance, e.team, e.player, e.assist, e.detail)
                for e in snapshot.events
            ]
        streams.append(seen)
        if not provider.exhausted:
            raise ImportError_(f"fixture {path}: replay did not exhaust its event stream")
    if streams[0] != streams[1]:
        raise ImportError_(f"fixture {path}: two replays produced different event streams")
    if streams[0] != expected:
        raise ImportError_(
            f"fixture {path}: replayed events do not match the recorded fixture events"
        )


def check_fixtures(target: str | Path) -> list[Path]:
    """Check every fixture under ``target`` (a dir of ``*.json`` or a single file)."""
    target = Path(target)
    paths = sorted(target.glob("*.json")) if target.is_dir() else [target]
    if not paths:
        raise ImportError_(f"no fixture JSON found at {target}")
    for path in paths:
        check_fixture(path)
        print(f"ok: {path}")
    return paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "target",
        nargs="?",
        help="With --check: the fixtures dir/file to validate. Otherwise a source "
        "gamealerts DB to import EVERY match from into --out. Omit (with neither "
        "--check nor a DB) to run the pinned four-fixture seed import from the "
        "default live DBs.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate existing fixtures parse and replay deterministically (no DB access).",
    )
    parser.add_argument(
        "--out",
        default="fixtures/football",
        help="Output dir for imported fixtures (default: fixtures/football).",
    )
    parser.add_argument("--real-db", default=str(DEFAULT_REAL_DB), help="Path to the real seed DB.")
    parser.add_argument(
        "--fictional-db",
        default=str(DEFAULT_FICTIONAL_DB),
        help="Path to the fictional seed DB.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.check:
            # --check target defaults to --out when no positional is given.
            check_fixtures(args.target or args.out)
        elif args.target:
            # A positional (non-check) target names a source DB: import all of it.
            import_all_matches(args.target, args.out)
        else:
            # No DB, no --check: the one-time pinned seed import from live DBs.
            import_seed_matches(
                args.out,
                real_db=args.real_db,
                fictional_db=args.fictional_db,
            )
    except (ImportError_, FixtureFormatError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
