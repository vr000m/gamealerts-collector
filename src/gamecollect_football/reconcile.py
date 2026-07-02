"""Provider ↔ canonical match-id reconciliation (football pack).

Ported out of gamealerts ``data/reconcile.py``. A schedule loader seeds
``matches.match_id`` under stable canonical ids, but live adapters emit
provider-native ids (ESPN event ``760421``). This module bridges the two
namespaces so a collector can write live rows against a real schedule.

Reconciliation keys on the **UTC kickoff instant + normalized team-set**, never
the date string: ESPN scoreboard event ``760421`` is Australia v "Türkiye" at
``2026-06-14T04:00Z`` while OpenFootball seeds Australia v "Turkey" dated
``2026-06-13`` (local, 21:00 UTC-7) whose computed kickoff is exactly
``2026-06-14T04:00:00+00:00``. Same instant, different date string, aliased team
name — so a plain (date, teams) match is unsafe and a date comparison wrong.

DAO calls are rewritten against :mod:`gamecollect.db.writer` /
:mod:`gamecollect.db.reader`; name folding delegates to the core-owned
:func:`gamecollect.fold.fold`. Newly-created unreconciled ids are
**source-qualified** (``f"{writer.source}:{provider_id}"`` — a deliberate
change vs gamealerts' provider-qualified form) so ``matches.match_id`` stays
globally unambiguous across partitions in the shared file.

Candidate matches carry their team display names in the ``matches.payload``
JSON (keys ``home_team``/``away_team`` — the schedule-metadata-in-payload plan
pin); when a payload lacks them, the soft ``home_entity``/``away_entity`` refs
are resolved against ``entities.display_name`` as a fallback.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from gamecollect.db import reader
from gamecollect.db.writer import PartitionWriter
from gamecollect.fold import fold
from gamecollect.provider import NormalizedMatch

__all__ = [
    "TEAM_ALIASES",
    "canonical_display_name",
    "canonical_team_name",
    "canonical_player_name",
    "resolve_canonical_match_id",
    "register_unreconciled_match",
]

log = logging.getLogger(__name__)

# Default ±window (minutes) for the kickoff-instant reconciliation.
_KICKOFF_WINDOW_MINUTES = 180.0

# ---------------------------------------------------------------------------
# Team-name aliasing
# ---------------------------------------------------------------------------

# Provider name variants → OpenFootball canonical names. The map is the safety
# net for divergence a plain NFKD fold cannot bridge: NFKD of "Türkiye" is
# "Turkiye", which is still != "Turkey".
TEAM_ALIASES: dict[str, str] = {
    "Türkiye": "Turkey",
    "Côte d'Ivoire": "Ivory Coast",
    "Korea Republic": "South Korea",
    "IR Iran": "Iran",
    "United States": "USA",
}


def canonical_display_name(name: str) -> str:
    """
    Fold a provider name to its canonical DISPLAY name (proper case preserved).

    Applies ``TEAM_ALIASES`` only (e.g. ``Türkiye`` → ``Turkey``,
    ``Côte d'Ivoire`` → ``Ivory Coast``) and otherwise returns the name
    unchanged. Unlike :func:`canonical_team_name`, this does NOT casefold or
    strip diacritics — it yields the human-readable name a schedule seed and a
    display consumer use, so an unreconciled row is written as ``Turkey`` (NOT
    the comparison key ``turkey``). Use this at the write seam; use
    :func:`canonical_team_name` for equality comparisons.
    """
    return TEAM_ALIASES.get(name, name)


def canonical_team_name(name: str) -> str:
    """
    Reduce a team name to a comparison key.

    Applies ``TEAM_ALIASES`` first (exact match on the raw provider name), then
    delegates to the core-owned :func:`gamecollect.fold.fold` (NFKD-fold, strip
    diacritics, casefold, trim). Two names are "the same team" iff their keys
    are equal.
    """
    return fold(TEAM_ALIASES.get(name, name))


def canonical_player_name(name: str) -> str:
    """
    Reduce a player display name to a comparison key.

    The same accent-insensitive mechanism as :func:`canonical_team_name` (both
    delegate to the core :func:`gamecollect.fold.fold`) but with NO alias map
    (players have no ``TEAM_ALIASES``-style table). So an accented ESPN
    ``displayName`` ("Maxime Crépeau") folds to the same key as a de-accented
    stored squad name ("Maxime Crepeau"); the comparison is a no-op unless BOTH
    sides are folded, so callers must fold the stored side too.

    Explicitly OUT of scope: alias expansion and suffix/punctuation equivalence
    (``Jr.`` ⇔ ``Junior``, initials, hyphen variants) — this is NFKD +
    combining-strip + casefold + trim only.
    """
    return fold(name)


# ---------------------------------------------------------------------------
# Canonical-id resolution
# ---------------------------------------------------------------------------


def resolve_canonical_match_id(
    conn: sqlite3.Connection,
    writer: PartitionWriter,
    match: NormalizedMatch,
    provider: str,
) -> str | None:
    """
    Resolve a provider-native match to its canonical ``matches.match_id``.

    ``conn`` is the daemon's read-write connection (reads go through the reader
    helpers on it); ``writer`` is the source-scoped :class:`PartitionWriter`
    used to persist a reconciled mapping.

    Precedence (ported out of gamealerts):
      (a) Cache hit — ``provider_match_map`` already maps
          (source, provider, match.match_id) → canonical id. Avoids re-resolving.
      (b) Direct-seed back-compat — if no cache entry but ``match.match_id`` is
          itself an existing ``matches.match_id`` row, the seeded row IS the
          canonical row under the provider id. Return it.
      (c) Instant-window reconciliation — fetch seeded rows whose kickoff is
          within ±180 min of ``match.kickoff_utc`` and pick the row whose
          {canonical(home), canonical(away)} set equals the match's team-set.
          Tie-break by nearest kickoff. On a hit, persist the mapping and return
          the canonical id. Runs BEFORE (d) so a real content-derived slug always
          wins over a source-qualified strip row that may co-exist for the match.
      (d) Source-qualified seed (last resort) — a live-schedule sync may seed the
          WHOLE schedule under ``f"{source}:{id}"`` ids, and
          :func:`register_unreconciled_match` writes under the same form. The
          adapter emits the BARE id, so (b) misses and (c) finds nothing
          (``:``-qualified rows are excluded from the candidate set by design);
          check the qualified row and return it when present. Deliberately NOT
          cached via the provider map (unlike (b)/(c)): the cache is consulted
          FIRST by (a), so a cached qualified bind would be STICKY — a
          ``<source>:<id>`` strip row can exist transiently before a
          content-slug schedule row lands, and a cached (a) hit would then
          permanently shadow the content slug that (c) should bind once it
          appears. On no hit, return ``None`` (caller skips).
    """
    provider_id = match.match_id

    # (a) cache hit
    cached = _lookup_provider_match(conn, writer.source, provider, provider_id)
    if cached is not None:
        return cached

    # (b) direct-seed back-compat: the provider id is itself a seeded canonical
    # row — but only when the row belongs to THIS source. In a shared
    # multi-source DB a bare provider-native id could match a row seeded by a
    # different source; binding to it would append this source's events under
    # another partition's match. Fall through to (c)/(d) instead.
    seeded = reader.get_state(conn, provider_id)
    if seeded is not None and seeded["source"] == writer.source:
        return provider_id

    # (c) reconcile by (kickoff instant, team-set). Runs BEFORE the qualified
    # fallback (d) so a real content-derived slug always wins over a
    # ``<source>:<id>`` strip row that may co-exist for the same match.
    # Without identity fields or on a parse error there is no kickoff candidate —
    # fall through to (d) rather than returning.
    if match.kickoff_utc and match.home_team and match.away_team:
        try:
            candidates = _matches_in_kickoff_window(
                conn, writer.source, match.kickoff_utc, _KICKOFF_WINDOW_MINUTES
            )
        except (ValueError, TypeError):
            candidates = []
        canonical_id = _reconcile_by_kickoff(match, candidates)
        if canonical_id is not None:
            writer.map_provider_match(provider, provider_id, canonical_id)
            return canonical_id

    # (d) source-qualified seed (last resort): an existing row under
    # f"{source}:{provider_id}" IS the canonical row for this provider event;
    # bind to it. Checked AFTER (c) so a content-derived slug still wins when
    # both exist. Mirrors register_unreconciled_match's qualified id.
    qualified_id = f"{writer.source}:{provider_id}"
    if reader.get_state(conn, qualified_id) is not None:
        return qualified_id

    return None


def _lookup_provider_match(
    conn: sqlite3.Connection, source: str, provider: str, provider_match_id: str
) -> str | None:
    """Return the cached canonical match_id for a provider id in this source, or None.

    ``provider_match_map`` is keyed ``(source, provider, provider_match_id)``
    in this schema (vs gamealerts' ``(provider, provider_match_id)``) so two
    sources collecting the same provider-native id never collide.
    """
    row = conn.execute(
        "SELECT match_id FROM provider_match_map "
        "WHERE source = ? AND provider = ? AND provider_match_id = ?",
        (source, provider, provider_match_id),
    ).fetchone()
    return row[0] if row is not None else None


def _matches_in_kickoff_window(
    conn: sqlite3.Connection, source: str, kickoff_iso: str, minutes: float
) -> list[dict[str, Any]]:
    """
    Return seeded match rows (this source) whose ``kickoff_utc`` falls within
    ±*minutes* of *kickoff_iso* (an ISO-8601 UTC instant).

    Comparison is on the parsed UTC instant, never the date string. Rows with
    an unparseable kickoff are skipped. Each result carries a parsed
    ``kickoff_dt`` key plus ``home_team``/``away_team`` display names resolved
    from ``payload`` JSON (falling back to the ``home_entity``/``away_entity``
    soft refs against ``entities.display_name``).

    Source-qualified rows (``match_id`` containing ``:``, seeded by
    :func:`register_unreconciled_match`) are EXCLUDED from the candidate set:
    they are not canonical targets, so binding a provider id to one would be
    sticky and would block a later bind to a real seeded slug.
    """
    target = _parse_instant(kickoff_iso)
    if target is None:
        raise ValueError(f"unparseable kickoff instant: {kickoff_iso!r}")
    delta = timedelta(minutes=minutes)
    lo = target - delta
    hi = target + delta

    result: list[dict[str, Any]] = []
    for row in reader.list_matches(conn, source=source):
        match_id = row.get("match_id")
        # Skip source-qualified strip rows (e.g. "wc2026:760421"); they are
        # never canonical reconciliation targets.
        if match_id is not None and ":" in match_id:
            continue
        kdt = _parse_instant(row.get("kickoff_utc"))
        if kdt is None:
            continue  # missing/unparseable kickoff can't be placed in the window
        if lo <= kdt <= hi:
            row["kickoff_dt"] = kdt
            home_team, away_team = _candidate_team_names(conn, row)
            row["home_team"] = home_team
            row["away_team"] = away_team
            result.append(row)
    return result


def _candidate_team_names(
    conn: sqlite3.Connection, row: dict[str, Any]
) -> tuple[str | None, str | None]:
    """Resolve a candidate matches row's team display names.

    Prefers the ``home_team``/``away_team`` keys in ``payload`` JSON (the
    schedule-metadata-in-payload convention); falls back to the
    ``home_entity``/``away_entity`` soft refs via ``entities.display_name``.
    """
    payload: dict[str, Any] = {}
    raw_payload = row.get("payload")
    if raw_payload:
        try:
            decoded = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
            if isinstance(decoded, dict):
                payload = decoded
        except (ValueError, TypeError):
            payload = {}

    home = payload.get("home_team")
    away = payload.get("away_team")
    source = row.get("source")

    if not home and row.get("home_entity") and source:
        entity = reader.get_entity(conn, source, row["home_entity"])
        home = entity.get("display_name") if entity else None
    if not away and row.get("away_entity") and source:
        entity = reader.get_entity(conn, source, row["away_entity"])
        away = entity.get("display_name") if entity else None

    return home, away


def _reconcile_by_kickoff(match: NormalizedMatch, candidates: list[dict]) -> str | None:
    """Pick the seeded candidate whose team-set matches *match*, nearest kickoff first.

    Returns the canonical ``match_id`` of the best candidate, or ``None`` if none of
    *candidates* has a matching {canonical(home), canonical(away)} set. Pure helper
    for step (c) of :func:`resolve_canonical_match_id` — the caller persists the bind.
    """
    target_set = {
        canonical_team_name(match.home_team),
        canonical_team_name(match.away_team),
    }

    target_instant = _parse_instant(match.kickoff_utc)

    best: dict | None = None
    best_delta: float | None = None
    for cand in candidates:
        home = cand.get("home_team")
        away = cand.get("away_team")
        if not home or not away:
            continue
        cand_set = {canonical_team_name(home), canonical_team_name(away)}
        if cand_set != target_set:
            continue
        # Tie-break: nearest kickoff instant.
        kdt = cand.get("kickoff_dt")
        delta = (
            abs((kdt - target_instant).total_seconds())
            if kdt is not None and target_instant is not None
            else 0.0
        )
        if best_delta is None or delta < best_delta:
            best = cand
            best_delta = delta

    if best is None:
        return None
    return best["match_id"]


def register_unreconciled_match(writer: PartitionWriter, match: NormalizedMatch) -> str:
    """
    Seed/update a minimal ``matches`` row for a live scoreboard match that has no
    seeded canonical counterpart, so an all-live consumer can render it.

    The row is written under a **source-qualified** id
    ``f"{writer.source}:{match_id}"`` (globally unambiguous across partitions —
    the plan's match_id contract) using the scoreboard's own team names
    (canonical DISPLAY-folded, carried in ``payload`` JSON per the
    schedule-metadata-in-payload pin), ``kickoff_utc``, and the live strip
    fields (status/minute/score). ``updated_at`` is stamped by the writer.
    Idempotent on the qualified id.

    If the identity fields (``home_team``/``away_team``/``kickoff_utc``) are
    missing, the row cannot be meaningfully seeded — log a warning and return
    the qualified id WITHOUT writing (ported behavior: gamealerts' NOT NULL
    team columns are payload keys here, but the skip contract is preserved).
    """
    qualified_id = f"{writer.source}:{match.match_id}"

    if not match.home_team or not match.away_team or not match.kickoff_utc:
        log.warning(
            "Unreconciled match %s (source %s) is missing identity fields "
            "(home_team/away_team/kickoff_utc); cannot seed a strip row. Skipping.",
            match.match_id,
            writer.source,
        )
        return qualified_id

    writer.upsert_match(
        {
            "match_id": qualified_id,
            "kickoff_utc": match.kickoff_utc,
            # Fold provider names to the canonical DISPLAY name at the write seam
            # (the single authority): the unreconciled row carries `Turkey`, not
            # raw `Türkiye`, so display consumers compare canonical-to-canonical
            # with no opposite-direction name map.
            "payload": {
                "home_team": canonical_display_name(match.home_team),
                "away_team": canonical_display_name(match.away_team),
            },
            "status": match.status.value,
            "minute": match.minute,
            "score_home": match.score_home,
            "score_away": match.score_away,
        }
    )
    return qualified_id


def _parse_instant(kickoff_iso: str | None) -> datetime | None:
    """Parse a kickoff ISO string to a UTC instant for window/tie-breaking.

    Tolerates the "Z" and "+00:00" forms; a naive value is assumed UTC.
    Returns ``None`` on an unparseable value.
    """
    if not kickoff_iso:
        return None

    s = kickoff_iso
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt
