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
from gamecollect.provider import NormalizedMatch, is_empty_payload_value

__all__ = [
    "TEAM_ALIASES",
    "canonical_display_name",
    "canonical_team_name",
    "canonical_player_name",
    "resolve_canonical_match_id",
    "register_unreconciled_match",
    "seed_or_reconcile_match",
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
          (:func:`seed_or_reconcile_match` uses the internal deferred-cache
          variant instead, persisting the mapping only after stub adoption
          succeeds — no map churn while an adoption-refusal conflict persists.)
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
    canonical_id, cache_pending = _resolve_canonical_match_id_deferred(
        conn, writer, match, provider
    )
    if canonical_id is not None and cache_pending:
        writer.map_provider_match(provider, match.match_id, canonical_id)
    return canonical_id


def _resolve_canonical_match_id_deferred(
    conn: sqlite3.Connection,
    writer: PartitionWriter,
    match: NormalizedMatch,
    provider: str,
) -> tuple[str | None, bool]:
    """Resolution engine behind :func:`resolve_canonical_match_id`.

    Same precedence (a)-(d), but a (b)/(c) hit does NOT persist the provider
    map entry itself — it returns ``(canonical_id, cache_pending=True)`` and
    the caller decides when to write the mapping. This lets
    :func:`seed_or_reconcile_match` persist the entry only AFTER stub adoption
    succeeds: under the old cache-then-refuse order, every poll on a
    both-have-events conflict did an INSERT (here) followed by a DELETE (in
    the refusal path) — two write transactions of pure map churn per poll.
    ``cache_pending`` is ``False`` for (a) hits (the entry already exists),
    (d) hits (qualified ids are never cached), and ``None``.

    Stale-map self-heal (path (a)): databases written by a previous revision
    (which repointed the mapping at the stub on adoption refusal) can hold a
    cached ``f"{source}:{provider_id}"`` stub id — exactly the shape
    :func:`register_unreconciled_match` mints and the resolver's own rules
    say must never be cached. Left in place, (a) would return the stub
    forever and adoption would never be re-attempted. Such an entry is
    deleted (INFO logged) and resolution falls through to (b)-(d), which
    re-attempts adoption via the normal path.
    """
    provider_id = match.match_id

    # (a) cache hit
    cached = _lookup_provider_match(conn, writer.source, provider, provider_id)
    if cached is not None:
        if cached == f"{writer.source}:{provider_id}":
            # Stale stub entry from the previous repoint-on-refusal behavior:
            # purge it and fall through so adoption can be re-attempted.
            log.info(
                "purging stale provider_match_map entry %s -> %s (source %s, provider %s): "
                "source-qualified stub ids are never cached; re-resolving",
                provider_id,
                cached,
                writer.source,
                provider,
            )
            with conn:
                conn.execute(
                    "DELETE FROM provider_match_map "
                    "WHERE source = ? AND provider = ? AND provider_match_id = ?",
                    (writer.source, provider, provider_id),
                )
        else:
            return cached, False

    # (b) direct-seed back-compat: the provider id is itself a seeded canonical
    # row — but only when the row belongs to THIS source. In a shared
    # multi-source DB a bare provider-native id could match a row seeded by a
    # different source; binding to it would append this source's events under
    # another partition's match. Fall through to (c)/(d) instead.
    seeded = reader.get_state(conn, provider_id)
    if seeded is not None and seeded["source"] == writer.source:
        # Cache-pending bind (docstring contract: (b)/(c) cache, (d) does not)
        # so subsequent polls resolve via (a) without re-running get_state.
        # Safe: the row exists and is source-checked, satisfying
        # map_provider_match's seeded-ownership precondition, and a
        # provider_id → provider_id bind is stable (unlike (d)'s transient
        # qualified row).
        return provider_id, True

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
            return canonical_id, True

    # (d) source-qualified seed (last resort): an existing row under
    # f"{source}:{provider_id}" IS the canonical row for this provider event;
    # bind to it. Checked AFTER (c) so a content-derived slug still wins when
    # both exist. Mirrors register_unreconciled_match's qualified id.
    qualified_id = f"{writer.source}:{provider_id}"
    if reader.get_state(conn, qualified_id) is not None:
        return qualified_id, False

    return None, False


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


def register_unreconciled_match(
    conn: sqlite3.Connection, writer: PartitionWriter, match: NormalizedMatch
) -> str | None:
    """
    Seed/update a minimal ``matches`` row for a live scoreboard match that has no
    seeded canonical counterpart, so an all-live consumer can render it.

    The row is written under a **source-qualified** id
    ``f"{writer.source}:{match_id}"`` (globally unambiguous across partitions —
    the plan's match_id contract) using the scoreboard's own team names
    (canonical DISPLAY-folded, carried in ``payload`` JSON per the
    schedule-metadata-in-payload pin), ``kickoff_utc``, and the live strip
    fields (status/minute/score). ``updated_at`` is stamped by the writer.
    Idempotent on the qualified id. The writer replaces ``payload`` wholesale,
    so the existing row's payload is read (via ``conn``) and merged first — a
    re-registration must not erase keys another write path added.

    If the identity fields (``home_team``/``away_team``/``kickoff_utc``) are
    missing, the row cannot be meaningfully seeded — log a warning and return
    ``None`` WITHOUT writing. A ``str`` return therefore always names a seeded
    row that satisfies the writer's child-write precondition (``append_events``
    et al. raise :class:`~gamecollect.db.writer.UnseededMatchError` on unseeded
    ids); callers must skip event/mapping writes on ``None``.
    """
    qualified_id = f"{writer.source}:{match.match_id}"

    if not match.home_team or not match.away_team or not match.kickoff_utc:
        log.warning(
            "Unreconciled match %s (source %s) is missing identity fields "
            "(home_team/away_team/kickoff_utc); cannot seed a strip row. Skipping.",
            match.match_id,
            writer.source,
        )
        return None

    # Read-merge-write: upsert_match replaces payload wholesale, and this
    # function re-runs on every poll while the match stays unreconciled — a
    # bare {home_team, away_team} write would erase richer keys (stats,
    # lineups, schedule metadata) another path put on the row.
    payload = reader.get_stored_payload(conn, qualified_id)

    # Merge the snapshot's sport extras (round_name, venue, HT scores, lineups,
    # stats — the adapter pins them to matches.payload; the engine merges
    # scoreboard+detail payloads before seeding). Preserve-richer semantics:
    # this re-runs every poll, and a sparser post-FT scoreboard snapshot must
    # not clobber richer detail-derived stored values with None/empty — an
    # incoming value only lands when it is non-empty (fresher data wins) or
    # the key is missing entirely. Merged BEFORE the canonical team names
    # below so canonical names still win over any raw provider
    # home_team/away_team keys a payload might carry.
    _merge_preserving_richer(payload, match.payload)

    # Fold provider names to the canonical DISPLAY name at the write seam
    # (the single authority): the unreconciled row carries `Turkey`, not
    # raw `Türkiye`, so display consumers compare canonical-to-canonical
    # with no opposite-direction name map.
    payload["home_team"] = canonical_display_name(match.home_team)
    payload["away_team"] = canonical_display_name(match.away_team)

    writer.upsert_match(
        {
            "match_id": qualified_id,
            "kickoff_utc": match.kickoff_utc,
            "payload": payload,
            "status": match.status.value,
            "minute": match.minute,
            "score_home": match.score_home,
            "score_away": match.score_away,
            "display_clock": match.display_clock,
        }
    )
    return qualified_id


def _merge_preserving_richer(stored: dict[str, Any], incoming: dict[str, Any]) -> None:
    """Merge *incoming* payload keys into *stored*, never losing richer data.

    A non-empty incoming value always wins (fresher data replaces older,
    non-empty-over-non-empty included). An EMPTY incoming value (None/""/
    []/{}) only lands when the key is missing from *stored* — it never
    replaces a stored non-empty value, so a sparse post-FT scoreboard
    snapshot cannot clobber detail-derived stats/lineups the strip row
    already carries. Emptiness is :func:`gamecollect.provider.is_empty_payload_value`,
    shared with core's in-memory :func:`~gamecollect.engine._merge_over_base`
    so the rule can't drift between the two merge paths."""
    for key, value in incoming.items():
        if key not in stored or not is_empty_payload_value(value):
            stored[key] = value


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


# Pack-owned side tables keyed by match_id that must follow a stub row when it
# is adopted onto a late-appearing canonical schedule row.
_MIGRATABLE_SIDE_TABLES = ("football_stats", "football_lineups")


def _adopt_stub_rows(
    conn: sqlite3.Connection, source: str, stub_id: str, canonical_id: str
) -> bool:
    """Migrate a source-qualified stub's child rows onto the canonical id.

    When early polls collected on a ``<source>:<provider_id>`` stub (schedule
    row absent) and the canonical schedule row appears later, the stub's
    events, provider-id mappings, and football side-table rows would
    otherwise be stranded — readers of the canonical match would silently
    miss the early history. Migrates everything in ONE transaction (seqs
    preserved), merges the stub's accumulated ``payload`` into the canonical
    row (preserve-richer; canonical schedule-owned keys and team names win —
    the adoption may fire on a sparse poll, so HT scores/venue/round_name the
    stub collected must not be lost), and deletes the stub ``matches`` row.

    Side-table PK collisions (e.g. fixture-imported lineups already under the
    canonical id) resolve freshest-wins: the stub's rows were written by live
    collection, so ``UPDATE OR REPLACE`` moves them onto the canonical id,
    displacing any colliding canonical row rather than silently dropping the
    stub's fresher data.

    Safety policy on seq collisions: migration only proceeds when the
    canonical row has NO events of its own (the common case — it was just
    seeded from the schedule). If BOTH rows carry events, interleaving two
    timelines could corrupt both, so nothing is migrated, an ERROR is logged
    on every adoption attempt, and the caller keeps collecting on the stub
    (and must DELETE the ``provider_match_map`` entry — never cache the
    qualified stub id — so the next poll re-resolves and re-attempts
    adoption; see :func:`seed_or_reconcile_match`). Adoption therefore heals
    automatically once the operator clears the canonical row's conflicting
    events. Partition discipline: only rows under *source* are touched.

    Returns ``True`` when the stub is gone (migrated or never existed),
    ``False`` on the both-have-events conflict.
    """
    stub_exists = (
        conn.execute(
            "SELECT 1 FROM matches WHERE source = ? AND match_id = ?", (source, stub_id)
        ).fetchone()
        is not None
    )
    if not stub_exists:
        return True
    (stub_events,) = conn.execute(
        "SELECT COUNT(*) FROM events WHERE source = ? AND match_id = ?", (source, stub_id)
    ).fetchone()
    (canonical_events,) = conn.execute(
        "SELECT COUNT(*) FROM events WHERE source = ? AND match_id = ?", (source, canonical_id)
    ).fetchone()
    if stub_events and canonical_events:
        log.error(
            "stub %s and canonical %s (source %s) BOTH carry events (%d stub, %d canonical); "
            "refusing to interleave two timelines — keeping collection on the stub; "
            "this is SPLIT HISTORY needing manual repair (logged on every adoption attempt)",
            stub_id,
            canonical_id,
            source,
            stub_events,
            canonical_events,
        )
        return False
    # Merge the stub's accumulated payload into the canonical row before the
    # stub is deleted: stub payload as base, canonical payload merged on top
    # with preserve-richer — canonical non-empty values (schedule-owned keys,
    # canonical team names) win, while stub-collected keys the canonical row
    # lacks (or holds empty) survive the adoption.
    merged_payload = dict(reader.get_stored_payload(conn, stub_id))
    _merge_preserving_richer(merged_payload, reader.get_stored_payload(conn, canonical_id))
    with conn:
        conn.execute(
            "UPDATE events SET match_id = ? WHERE source = ? AND match_id = ?",
            (canonical_id, source, stub_id),
        )
        conn.execute(
            "UPDATE provider_match_map SET match_id = ? WHERE source = ? AND match_id = ?",
            (canonical_id, source, stub_id),
        )
        for table in _MIGRATABLE_SIDE_TABLES:
            if not _table_exists(conn, table):
                continue
            # Freshest-wins on PK collision: the stub's rows came from live
            # collection, so they replace any canonical-keyed row with the
            # same PK (e.g. fixture-imported lineups). UPDATE OR REPLACE
            # moves every stub row (deleting the displaced canonical row on
            # conflict), so no stub-keyed rows remain and none are dropped.
            conn.execute(
                f"UPDATE OR REPLACE {table} SET match_id = ? "  # noqa: S608
                f"WHERE source = ? AND match_id = ?",
                (canonical_id, source, stub_id),
            )
        conn.execute(
            "UPDATE matches SET payload = ? WHERE source = ? AND match_id = ?",
            (json.dumps(merged_payload, ensure_ascii=False, sort_keys=True), source, canonical_id),
        )
        conn.execute("DELETE FROM matches WHERE source = ? AND match_id = ?", (source, stub_id))
    log.info(
        "adopted stub %s onto canonical %s (source %s): %d event(s) migrated, stub row deleted",
        stub_id,
        canonical_id,
        source,
        stub_events,
    )
    return True


def seed_or_reconcile_match(
    conn: sqlite3.Connection,
    writer: PartitionWriter,
    match: NormalizedMatch,
    provider: str = "espn",
) -> str | None:
    """
    Reconcile-first seed hook: the football pack's :attr:`SportPack.seed_match`.

    First tries :func:`resolve_canonical_match_id`; when the provider match
    resolves to an existing seeded row (canonical schedule slug, direct-seed
    back-compat, or a prior source-qualified strip row), the live state is
    upserted onto THAT row and its id returned — so the engine's child writes
    (events, side tables) land on the canonical row instead of accumulating a
    duplicate source-qualified strip. Only when nothing resolves does it fall
    back to :func:`register_unreconciled_match`.

    Return contract (the :attr:`SportPack.seed_match` pin): a ``str`` always
    names a seeded row satisfying the writer's child-write precondition;
    ``None`` means identity fields were missing AND nothing resolved, so the
    engine skips child writes this poll. A cache/direct-seed resolution can
    succeed without identity fields — the canonical row already carries them.

    ``kickoff_utc`` and payload ``home_team``/``away_team`` on a resolved
    canonical row are schedule-owned and are NOT overwritten with provider
    values; the snapshot's other payload extras are merged in with the SAME
    preserve-richer policy as :func:`register_unreconciled_match` (stored ←
    incoming richer-wins ← canonical names last) — this path re-runs every
    poll for both canonical rows and stub re-polls (resolver path (d)), so a
    sparse post-FT snapshot must not clobber richer stored values here either.
    """
    canonical_id, cache_pending = _resolve_canonical_match_id_deferred(
        conn, writer, match, provider
    )
    if canonical_id is None:
        return register_unreconciled_match(conn, writer, match)

    # Late canonical reconciliation: if early polls collected on a
    # source-qualified stub (schedule row absent then) and the canonical row
    # appeared later, adopt the stub's events/mappings/side-table rows onto
    # the canonical id so the early history is not stranded. On the unsafe
    # both-have-events conflict, _adopt_stub_rows logs ERROR and we keep
    # collecting on the stub (never corrupt two timelines).
    stub_id = f"{writer.source}:{match.match_id}"
    if canonical_id != stub_id and not _adopt_stub_rows(conn, writer.source, stub_id, canonical_id):
        # Split-brain guard: collection is staying on the stub, so NO
        # provider_match_map entry may exist — a canonical entry would point
        # map-following readers at a frozen timeline while live history lands
        # on the stub, and a stub entry would make resolver path (a) return
        # the stub forever (adoption never re-attempted, the documented
        # per-attempt ERROR fired only once, healing never picked up).
        # Resolution above was cache-DEFERRED, so nothing was written this
        # poll and there is normally nothing to delete — no INSERT+DELETE
        # churn while the conflict persists. Keep a defensive delete for an
        # entry left behind by an older revision or an external writer. The
        # stub id is returned UNCACHED: the next poll re-resolves via (c)
        # (one indexed lookup + kickoff-window scan), re-attempts adoption,
        # re-logs the ERROR while the conflict persists, and adoption
        # succeeds automatically once it is safe.
        if _lookup_provider_match(conn, writer.source, provider, match.match_id) is not None:
            with conn:
                conn.execute(
                    "DELETE FROM provider_match_map "
                    "WHERE source = ? AND provider = ? AND provider_match_id = ?",
                    (writer.source, provider, match.match_id),
                )
        return register_unreconciled_match(conn, writer, match)

    # Adoption succeeded (or there was no stub to adopt): NOW persist the
    # deferred (b)/(c) mapping so subsequent polls resolve via the (a) cache.
    # Writing only on the success path is what keeps a persisting refusal
    # conflict free of per-poll map churn.
    if cache_pending:
        writer.map_provider_match(provider, match.match_id, canonical_id)

    payload = reader.get_stored_payload(conn, canonical_id)
    schedule_home = payload.get("home_team")
    schedule_away = payload.get("away_team")
    # One merge policy everywhere: preserve-richer, same as
    # register_unreconciled_match. A plain dict.update here would let a
    # sparse snapshot clobber richer stored values on every poll after the
    # first (the register-side guard only protects the registration path).
    _merge_preserving_richer(payload, match.payload)
    # Schedule-seeded canonical names win over any provider-supplied payload
    # keys; fill from the (display-folded) snapshot names only when the
    # resolved row lacks them (e.g. a direct-seed row without payload names).
    if schedule_home:
        payload["home_team"] = schedule_home
    elif match.home_team:
        payload["home_team"] = canonical_display_name(match.home_team)
    if schedule_away:
        payload["away_team"] = schedule_away
    elif match.away_team:
        payload["away_team"] = canonical_display_name(match.away_team)

    writer.upsert_match(
        {
            "match_id": canonical_id,
            "payload": payload,
            "status": match.status.value,
            "minute": match.minute,
            "score_home": match.score_home,
            "score_away": match.score_away,
            "display_clock": match.display_clock,
        }
    )
    return canonical_id


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
