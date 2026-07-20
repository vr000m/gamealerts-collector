"""The football/FIFA WC2026 SportPack factory (entry point ``football-wc2026``).

Supplies all six DESIGN.md §5 contributions plus the pack-owned additive side
tables for football stats and lineups. The DDL is applied by the core via
``connect(path, side_table_ddl=pack.side_table_ddl)`` AFTER core
schema/migrations — additive and idempotent only; core tables are never
altered by a pack.
"""

from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
from decimal import Decimal
from functools import lru_cache
from pathlib import Path

from gamecollect.db import reader
from gamecollect.db.writer import PartitionWriter
from gamecollect.fold import fold
from gamecollect.packs.spec import SportPack
from gamecollect.provider import NormalizedMatch
from gamecollect_football.espn import ESPNAdapter
from gamecollect_football.operations import FOOTBALL_OPERATIONS
from gamecollect_football.reconcile import (
    FOOTBALL_SIDE_TABLE_DDL,
    canonical_display_name,
    canonical_team_name,
    seed_or_reconcile_match,
)
from gamecollect_football.taxonomy import TAXONOMY

__all__ = ["FOOTBALL_SIDE_TABLE_DDL", "pack", "persist_football_side_tables"]

log = logging.getLogger(__name__)

# FOOTBALL_SIDE_TABLE_DDL (typed homes for the football-specific shapes the
# adapter carries in NormalizedMatch.payload: boxscore stats and lineups) is
# defined in reconcile.py, tagged there per-table as match-keyed or
# team-keyed. That tag is the single source of truth reconcile.py's stub
# adoption uses to decide which side tables must follow a stub row — see
# reconcile._FOOTBALL_SIDE_TABLE_SPECS / _MIGRATABLE_SIDE_TABLES. Re-exported
# here (via __all__ above) since this is the pack's public entry point and
# existing callers import it from here.


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_text(value: object) -> str | None:
    """Coerce a scalar to TEXT; a non-scalar (dict/list) becomes ``None``.

    Provider payloads are untrusted JSON — binding a nested dict/list into a
    TEXT column raises ``sqlite3.InterfaceError``, which must never crash the
    hook (finding: side-table failures killed the daemon)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    return None


# A plain decimal float spelling: digits, one dot, digits (optional sign) —
# including dot-terminal ("123.") and dot-leading (".5") forms, each requiring
# at least one digit on the other side of the dot so a lone "." never matches.
# Deliberately EXCLUDES exponent forms — see _to_key_text.
_PLAIN_DECIMAL_RE = re.compile(r"^[+-]?([0-9]+\.[0-9]*|\.[0-9]+)$")


def _to_key_text(value: object) -> str | None:
    """Coerce an identifier scalar (PK component) to its canonical TEXT key.

    Bools are rejected outright (checked BEFORE the numeric coercion — ``bool``
    is an ``int`` subclass, and ``str(True)`` would mint a bogus ``"True"``
    team/athlete key); non-finite floats are rejected too (a literal ``"nan"``
    or ``"inf"`` PK key is garbage). An integral float canonicalizes to its
    int string, and so does an integral PLAIN-decimal string — a JSON payload
    spelling the same athlete as ``760421.0``, ``"760421.0"``, ``"760421."``,
    and ``"760421"`` must collapse to ONE primary-key row, not duplicate the
    player. Only strings matching ``^[+-]?([0-9]+\\.[0-9]*|\\.[0-9]+)$`` are
    reparsed — this covers dot-terminal (``"123."``) and dot-leading
    (``".5"``) spellings alongside the two-sided form. A plain digit string
    like ``"01"`` (no dot at all) is an opaque id and must keep its leading
    zero, and strings carrying an exponent marker (``"1E2"``,
    ``"1e999999999"``) pass through VERBATIM — they are opaque ids too;
    reparsing them would collide ``"1E2"`` with a genuine ``"100"`` (silent
    wrong-player attribution) and a huge exponent would materialize an
    astronomical digit string (poll-loop hang / MemoryError). Floats keep the
    ``is_integer()`` path — a finite float cannot mint a pathological digit
    string. Whitespace is never stripped: matching is anchored (``^``/``$``)
    against the raw string, so a padded spelling like ``" 760421 "`` fails the
    regex and passes through :func:`_to_key_text` verbatim (it is treated as
    an opaque id, not reparsed). Anything else follows :func:`_to_text`."""
    if isinstance(value, bool):
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        if value.is_integer():
            return str(int(value))
        return _to_text(value)
    if isinstance(value, str):
        # Canonicalize plain decimal float SPELLINGS only. Decimal parses the
        # string exactly (no float precision loss), so the int string it
        # yields represents the same number the payload carried; the regex
        # guarantees a finite, exponent-free parse.
        if _PLAIN_DECIMAL_RE.match(value):
            dec = Decimal(value)
            if dec == dec.to_integral_value():
                return str(int(dec))
        return value
    return _to_text(value)


# Skip-WARNING dedupe: the projection runs on EVERY persist (rows are
# projected BEFORE the write-if-changed compare, so that gate does NOT bound
# the logging) and a static bad row would otherwise re-WARN for the lifetime
# of the match. Keyed (source, match_id, kind, repr(value)) so each offending
# row warns exactly once; bounded — when full the set resets (a rare re-WARN
# beats unbounded growth in a long-lived daemon). The lineup-athlete call site
# folds the TEAM into ``kind`` (``f"lineup-athlete:{team}"``) — two different
# teams in the same match each fielding a player with the same invalid
# athlete_id value are two distinct offending rows, and must each warn once,
# not share one dedupe slot.
_WARNED_SKIPS: set[tuple[str, str, str, str]] = set()
_WARNED_SKIPS_MAX = 4096


def _warn_skip_once(
    source: str, match_id: str, kind: str, value: object, msg: str, *args: object
) -> None:
    key = (source, match_id, kind, repr(value))
    if key in _WARNED_SKIPS:
        return
    if len(_WARNED_SKIPS) >= _WARNED_SKIPS_MAX:
        _WARNED_SKIPS.clear()
    _WARNED_SKIPS.add(key)
    log.warning(msg, *args)


_STATS_COLUMNS = (
    "source, match_id, team, possession, shots, shots_on_target, "
    "corners, fouls, yellow_cards, red_cards, offsides"
)
_LINEUP_COLUMNS = (
    "source, match_id, team, athlete_id, display_name, name_folded, "
    "jersey, position, starter, subbed_in, subbed_out, "
    "formation_place, home_away, formation"
)
_VENUE_COLUMNS = "source, match_id, stadium, city"
_ROSTER_COLUMN_NAMES: tuple[str, ...] = (
    "team",
    "fifa_code",
    "group_name",
    "number",
    "position",
    "player_name",
    "name_folded",
    "date_of_birth",
)
_ROSTER_COLUMNS = ", ".join(_ROSTER_COLUMN_NAMES)


def _stats_rows(
    source: str, seeded_match_id: str, stats: list
) -> dict[tuple[str, str, str], tuple]:
    """Project the payload ``stats`` section onto football_stats row tuples.

    Keyed by the table's PRIMARY KEY so a duplicate team entry within one
    payload collapses last-wins (the old ON CONFLICT semantics). Non-scalar
    values are coerced to ``None`` rather than passed to sqlite; the ``team``
    PK component goes through :func:`_to_key_text` (bool rejected, integral
    float canonicalized) so numeric spellings cannot mint duplicate keys.
    ``team`` is then run through :func:`canonical_display_name`, mirroring
    :func:`_lineup_rows`, so ``football_stats.team`` agrees with
    ``football_roster.team``/``football_lineups.team`` and
    ``payload["home_team"]``/``["away_team"]`` on alias-mapped teams — a
    mismatch here silently breaks ``get_player_stats(team=...)`` for a caller
    passing the canonical name."""
    rows: dict[tuple[str, str, str], tuple] = {}
    for row in stats:
        if not isinstance(row, dict):
            continue
        team = _to_key_text(row.get("team"))
        if team:
            team = canonical_display_name(team)
        if not team:
            _warn_skip_once(
                source,
                seeded_match_id,
                "stats-team",
                row.get("team"),
                "football_stats: skipping stats row for match %s — invalid team key %r",
                seeded_match_id,
                row.get("team"),
            )
            continue
        rows[(source, seeded_match_id, team)] = (
            source,
            seeded_match_id,
            team,
            _to_float(row.get("possession")),
            _to_int(row.get("shots")),
            _to_int(row.get("shots_on_target")),
            _to_int(row.get("corners")),
            _to_int(row.get("fouls")),
            _to_int(row.get("yellow_cards")),
            _to_int(row.get("red_cards")),
            _to_int(row.get("offsides")),
        )
    return rows


def _lineup_rows(
    source: str, seeded_match_id: str, lineups: list
) -> dict[tuple[str, str, str, str], tuple]:
    """Project the payload ``lineups`` section onto football_lineups row tuples.

    Keyed by the table's PRIMARY KEY (last-wins on duplicates, mirroring the
    old ON CONFLICT semantics); non-scalar values coerce to ``None``. The
    ``team``/``athlete_id`` PK components go through :func:`_to_key_text`
    (bool rejected, integral float canonicalized) so ``760421.0`` and
    ``"760421"`` collapse to one player row instead of duplicating it.
    ``team`` is then run through :func:`canonical_display_name` so it agrees
    with ``football_roster.team`` and ``payload["home_team"]``/``["away_team"]``
    on alias-mapped teams (``Türkiye`` -> ``Turkey``, etc.) — a mismatch here
    silently breaks any participant-keyed lookup filtering by team name."""
    rows: dict[tuple[str, str, str, str], tuple] = {}
    for lineup in lineups:
        if not isinstance(lineup, dict):
            continue
        team = _to_key_text(lineup.get("team"))
        # Canonicalize alias-mapped team names (Türkiye -> Turkey, etc.) so
        # football_lineups.team agrees with football_roster.team
        # (_roster_rows_for_team) and payload["home_team"]/["away_team"]
        # (reconcile.canonical_display_name) — all three must use the same
        # team-name vocabulary or FootballReadPort's participant-keyed
        # lookups (lineup_for_match, lineup_team_announced) silently miss.
        if team:
            team = canonical_display_name(team)
        if not team:
            _warn_skip_once(
                source,
                seeded_match_id,
                "lineup-team",
                lineup.get("team"),
                "football_lineups: skipping entire lineup for match %s — invalid team key %r",
                seeded_match_id,
                lineup.get("team"),
            )
            continue
        players = lineup.get("players") or []
        if not isinstance(players, list):
            continue
        for player in players:
            if not isinstance(player, dict):
                continue
            athlete_id = _to_key_text(player.get("athlete_id"))
            display_name = _to_text(player.get("display_name"))
            if not athlete_id:
                _warn_skip_once(
                    source,
                    seeded_match_id,
                    f"lineup-athlete:{team}",
                    player.get("athlete_id"),
                    "football_lineups: skipping player for match %s (team %s) — "
                    "invalid athlete_id %r",
                    seeded_match_id,
                    team,
                    player.get("athlete_id"),
                )
                continue
            if not display_name:
                continue
            key = (source, seeded_match_id, team, athlete_id)
            rows[key] = (
                source,
                seeded_match_id,
                team,
                athlete_id,
                display_name,
                fold(display_name),
                _to_text(player.get("jersey")),
                _to_text(player.get("position")),
                int(bool(player.get("starter"))),
                int(bool(player.get("subbed_in"))),
                int(bool(player.get("subbed_out"))),
                _to_int(player.get("formation_place")),
                _to_text(lineup.get("home_away")),
                _to_text(lineup.get("formation")),
            )
    return rows


def _venue_rows(
    source: str, seeded_match_id: str, stadium: object, city: object
) -> dict[tuple[str, str], tuple]:
    """Project a payload's ``stadium``/``city`` onto a single football_venue row.

    Returns an empty dict when both are missing — the caller only invokes
    :func:`_replace_if_changed` when there is something to persist, so a poll
    that carries no venue data at all leaves any already-stored row alone."""
    stadium_text = _to_text(stadium)
    city_text = _to_text(city)
    if stadium_text is None and city_text is None:
        return {}
    return {(source, seeded_match_id): (source, seeded_match_id, stadium_text, city_text)}


@lru_cache(maxsize=1)
def _load_squads_fixture() -> tuple[dict, ...]:
    """Load the static WC2026 squads fixture (team-level rosters).

    There is no ESPN team-squad fetch — the ESPN ``rosters[]`` block is
    per-match and already becomes ``football_lineups``. This is the only
    source for team-level squad data. Cached (the fixture never changes at
    runtime) and never raises: a missing/malformed fixture must not crash the
    per-poll side-table hook, mirroring the payload-coercion posture above."""
    fixture_path = Path(__file__).parent / "fixtures" / "worldcup.squads.json"
    try:
        with fixture_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        log.warning("football_roster: could not load squads fixture %s", fixture_path)
        return ()
    if not isinstance(data, list):
        return ()
    return tuple(squad for squad in data if isinstance(squad, dict) and squad.get("name"))


@lru_cache(maxsize=1)
def _squads_by_canonical_name() -> dict[str, dict]:
    """Index the squads fixture by :func:`canonical_team_name` for team lookup."""
    return {canonical_team_name(squad["name"]): squad for squad in _load_squads_fixture()}


_WARNED_ROSTER_TEAMS: set[str] = set()


def _warn_roster_unmatched_once(team_name: str) -> None:
    if team_name in _WARNED_ROSTER_TEAMS:
        return
    if len(_WARNED_ROSTER_TEAMS) >= 128:
        _WARNED_ROSTER_TEAMS.clear()
    _WARNED_ROSTER_TEAMS.add(team_name)
    log.warning("football_roster: no squad fixture entry for team %r", team_name)


def _roster_rows_for_team(team_name: str) -> tuple[str | None, dict[tuple[str, int], tuple]]:
    """Project the squad fixture's players for ``team_name`` onto row tuples.

    ``team_name`` is a live provider team name (e.g. ESPN ``displayName``);
    matched against the fixture via :func:`canonical_team_name` so alias
    spellings (``Türkiye``/``Turkey``) still resolve. Returns ``(None, {})``
    when the team has no fixture entry (warns once) or has no valid players;
    otherwise returns ``(team, rows)`` where ``team`` is the canonical
    display name stamped on every projected row's first PK column — the
    caller persisting these rows should use this returned name rather than
    re-deriving it from the rows dict."""
    squad = _squads_by_canonical_name().get(canonical_team_name(team_name))
    if squad is None:
        _warn_roster_unmatched_once(team_name)
        return None, {}
    team = canonical_display_name(squad["name"])
    fifa_code = _to_text(squad.get("fifa_code"))
    group_name = _to_text(squad.get("group"))
    rows: dict[tuple[str, int], tuple] = {}
    for player in squad.get("players") or []:
        if not isinstance(player, dict):
            continue
        number = _to_int(player.get("number"))
        name = _to_text(player.get("name"))
        if number is None or not name:
            continue
        rows[(team, number)] = (
            team,
            fifa_code,
            group_name,
            number,
            _to_text(player.get("pos")),
            name,
            fold(name),
            _to_text(player.get("date_of_birth")),
        )
    return team, rows


def _persist_roster_for_team(conn: sqlite3.Connection, team_name: str | None) -> None:
    """Idempotently persist ``team_name``'s squad rows (static data, insert-once).

    Unlike stats/lineups/venue, roster rows never change poll-to-poll — an
    ``INSERT OR IGNORE`` on the ``(team, number)`` PRIMARY KEY is sufficient
    and cheaper than a diff-and-replace. A cheap existence check (rather than
    an in-memory cache, which would need per-database-file scoping to avoid
    one connection's cache wrongly suppressing a write to a different,
    unrelated database opened later in the same process) skips the
    executemany write entirely once a team's rows are already on disk, so a
    live match's per-poll calls do real write work only once. The lookup
    itself (``_roster_rows_for_team``) stays cheap regardless — it is backed
    by ``@lru_cache``'d fixture data."""
    if not team_name:
        return
    stored_team, rows = _roster_rows_for_team(team_name)
    if not rows:
        return
    # stored_team is the same canonical name _roster_rows_for_team already
    # stamped onto every row's first PK column — using its return value
    # directly (rather than reaching back into the rows dict) means this
    # existence check can never disagree with what the insert below writes,
    # and stays correct if the column order in the row tuples ever changes.
    already_persisted = conn.execute(
        "SELECT 1 FROM football_roster WHERE team = ? LIMIT 1", (stored_team,)
    ).fetchone()
    if already_persisted is not None:
        return
    placeholders = ", ".join("?" for _ in _ROSTER_COLUMN_NAMES)
    conn.executemany(
        f"INSERT OR IGNORE INTO football_roster ({_ROSTER_COLUMNS}) VALUES ({placeholders})",  # noqa: S608
        list(rows.values()),
    )


def _replace_if_changed(
    conn: sqlite3.Connection,
    table: str,
    columns: str,
    source: str,
    seeded_match_id: str,
    desired: dict[tuple, tuple],
) -> None:
    """DELETE+reinsert the match's rows only when they actually changed.

    The engine calls the side-table hook on EVERY applied diff (every clock
    tick), but stats/lineups change far less often — a stateless
    query-and-compare against the currently-stored rows skips the rewrite
    when the incoming section is identical, instead of churning the WAL with
    a full DELETE + reinsert per poll. DELETE+reinsert stays the write
    mechanism because it is the correct semantics for removals."""
    current = {
        tuple(row)  # normalize: the engine's connection uses sqlite3.Row
        for row in conn.execute(
            f"SELECT {columns} FROM {table} WHERE source = ? AND match_id = ?",  # noqa: S608
            (source, seeded_match_id),
        )
    }
    if current == set(desired.values()):
        return
    conn.execute(
        f"DELETE FROM {table} WHERE source = ? AND match_id = ?",  # noqa: S608
        (source, seeded_match_id),
    )
    if desired:
        placeholders = ", ".join("?" for _ in columns.split(","))
        conn.executemany(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",  # noqa: S608
            list(desired.values()),
        )


def persist_football_side_tables(
    conn: sqlite3.Connection,
    writer: PartitionWriter,
    match: NormalizedMatch,
    seeded_match_id: str,
) -> None:
    """Persist football detail payload extras into football-owned side tables.

    Idempotent and cheap under re-polls: each section is projected onto row
    tuples first (non-scalar payload values coerced to ``None``/skipped — a
    provider glitch must not raise out of the hook), compared against the
    stored rows, and rewritten (DELETE + reinsert, correct for removals) only
    when it actually changed."""
    payload = dict(match.payload or {})
    stored_payload = reader.get_stored_payload(conn, seeded_match_id)
    for key in ("stats", "lineups", "stadium", "city"):
        if key in stored_payload:
            payload[key] = stored_payload[key]
    stats = payload.get("stats")
    lineups = payload.get("lineups")
    stadium = payload.get("stadium")
    city = payload.get("city")

    with conn:
        if isinstance(stats, list):
            _replace_if_changed(
                conn,
                "football_stats",
                _STATS_COLUMNS,
                writer.source,
                seeded_match_id,
                _stats_rows(writer.source, seeded_match_id, stats),
            )
        if isinstance(lineups, list):
            _replace_if_changed(
                conn,
                "football_lineups",
                _LINEUP_COLUMNS,
                writer.source,
                seeded_match_id,
                _lineup_rows(writer.source, seeded_match_id, lineups),
            )
        # Compute the projected rows first and gate on the RESULT being
        # non-empty (matching the isinstance(stats, list)/isinstance(lineups,
        # list) guard pattern above), not on the raw stadium/city values.
        # _venue_rows runs each value through _to_text, which coerces a
        # malformed/non-scalar value (e.g. a dict) to None -- if the guard
        # instead checked the raw values, a malformed-but-non-None stadium/
        # city would pass the guard while _venue_rows yields an EMPTY dict,
        # and _replace_if_changed(desired={}) against a non-empty `current`
        # performs an unconditional DELETE, wiping a previously-stored good
        # venue row over a single bad poll.
        venue_rows = _venue_rows(writer.source, seeded_match_id, stadium, city)
        if venue_rows:
            _replace_if_changed(
                conn,
                "football_venue",
                _VENUE_COLUMNS,
                writer.source,
                seeded_match_id,
                venue_rows,
            )
        # Fall back to the canonical schedule row's stored team names when
        # this poll's snapshot carries neither (e.g. a summary response whose
        # header.competitions is empty/malformed, so the ESPN adapter never
        # set NormalizedMatch.home_team/away_team, even though the SAME
        # response's rosters/lineups are otherwise intact). stored_payload is
        # already fetched above; per reconcile.py's seed_or_reconcile_match
        # docstring, a resolved canonical row's payload home_team/away_team
        # are schedule-owned and never overwritten with provider values, so
        # this is a safe, already-verified fallback source rather than a
        # guess at an unverified payload shape.
        _persist_roster_for_team(conn, match.home_team or stored_payload.get("home_team"))
        _persist_roster_for_team(conn, match.away_team or stored_payload.get("away_team"))


def pack() -> SportPack:
    """Zero-arg entry-point factory returning the football/WC2026 SportPack."""
    return SportPack(
        name="football-wc2026",
        sport="football",
        # ESPNAdapter() defaults to stdlib urllib http_get and the fifa.world
        # tournament slug — a zero-arg construction, as the registry requires.
        provider_factory=ESPNAdapter,
        taxonomy=TAXONOMY,
        prompt_fragments={
            "sport_context": (
                "Association football (soccer): two teams of eleven, two 45-minute "
                "halves plus stoppage time. Goals are the only score; a match may "
                "end in a draw in the group stage."
            ),
            "event_call": (
                "Call goals immediately with scorer and new score. Mention own goals "
                "and converted penalties explicitly. Cards matter: a second yellow "
                "means a red and the team plays a player short."
            ),
        },
        preference_schema={
            "type": "object",
            "properties": {
                "followed_teams": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Canonical team display names to follow.",
                },
                "followed_players": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Player display names to follow (fold-matched).",
                },
            },
        },
        display_metadata={
            "sport_display": "Football",
            "tournament_display": "FIFA World Cup 2026",
            "provider": "espn",
            "tournament_slug": ESPNAdapter.DEFAULT_TOURNAMENT,
        },
        # Natural summarization/compaction boundaries in a football match: the
        # phase-marker event types.
        compaction_boundaries=["kickoff", "half_time", "full_time"],
        side_table_ddl=FOOTBALL_SIDE_TABLE_DDL,
        # Pack-contributed read ops (squad/player-stats) over the side tables
        # above; merged onto the core registry at load. Core never imports this
        # pack — the ops carry their own impls (dependency direction pack → core).
        operations=FOOTBALL_OPERATIONS,
        # Reconcile-aware seed hook (amended from the plan's direct
        # register_unreconciled_match wiring per code review): first resolves
        # the provider match against the canonical schedule via
        # resolve_canonical_match_id and upserts live state onto the canonical
        # row when it resolves — so events land there instead of on a duplicate
        # source-qualified strip — falling back to register_unreconciled_match
        # otherwise. Returns None only when nothing resolved AND identity
        # fields are missing; the engine then skips child writes for it. Its
        # (conn, writer, match) -> str | None signature (provider defaulted)
        # is the SportPack.seed_match contract, so it wires in directly.
        seed_match=seed_or_reconcile_match,
        persist_side_tables=persist_football_side_tables,
    )
