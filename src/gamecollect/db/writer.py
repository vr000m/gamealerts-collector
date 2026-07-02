"""Partition-scoped write API — the ONLY write path into the shared database.

Partition-per-writer (DESIGN.md §3) is enforced structurally, not by
convention: every insert/upsert stamps the constructor ``source``, and any row
that carries an explicit ``source`` differing from the writer's raises
:class:`CrossPartitionError` before touching the database. A
:class:`PartitionWriter` cannot be talked into writing another partition's
rows.

``seq`` is provider-derived (the normalizer supplies it, e.g. from the ESPN
keyEvents index after any 0–2-event expansion) — the writer does NOT allocate
it. It enforces monotonic-per-``(source, match_id)`` as an invariant and is
idempotent via ``INSERT OR IGNORE`` on ``(source, match_id, seq)``, preserving
gamealerts' re-poll semantics: re-polling the full event list re-sends already
stored seqs, which are ignored.

Event ``type`` is pack-declared string data: when a ``taxonomy`` is supplied,
an undeclared type raises :class:`TaxonomyError` at write time (loud, explicit
— shape drift surfaces at the writer, mirroring gamealerts'
``ShapeDriftError`` philosophy). Validation is skipped when ``taxonomy`` is
``None`` so the core writer stays sport-agnostic.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Collection, Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from gamecollect.fold import fold

__all__ = [
    "PartitionWriter",
    "CrossPartitionError",
    "TaxonomyError",
    "SequenceError",
]


class CrossPartitionError(ValueError):
    """A write targeted a row owned by (or stamped for) a different source."""


class TaxonomyError(ValueError):
    """An event ``type`` is not declared by the active pack's taxonomy."""


class SequenceError(ValueError):
    """Event seqs violate monotonic-per-(source, match) ordering."""


# Column allow-lists per table: the writer only accepts known core columns, so
# a typo'd key fails loudly instead of being dropped silently.
_MATCH_COLUMNS = (
    "kickoff_utc",
    "home_entity",
    "away_entity",
    "status",
    "minute",
    "period",
    "score_home",
    "score_away",
    "display_clock",
    "payload",
    "updated_at",
)
_EVENT_COLUMNS = (
    "minute",
    "period",
    "importance",
    "actor_entity",
    "target_entity",
    "detail",
    "payload",
)
_ENTITY_COLUMNS = ("kind", "display_name", "name_folded", "parent_entity", "payload")
_STANDING_COLUMNS = ("points", "rank", "payload")

# Key columns per table: only the keys a given write method actually consumes
# are exempt from the unknown-key rejection, so a cross-table key (e.g. ``seq``
# in a match row) fails loudly instead of being dropped.
_MATCH_KEYS = frozenset({"match_id", "source"})
_EVENT_KEYS = frozenset({"source", "match_id", "seq", "type"})
_ENTITY_KEYS = frozenset({"source", "entity_id"})
_STANDING_KEYS = frozenset({"source", "group_key", "entity_id"})


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


def _encode_payload(value: Any) -> Any:
    """JSON-encode dict/list payloads; pass strings/None through untouched."""
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


class PartitionWriter:
    """Scopes every write on ``conn`` to the declared ``source`` partition.

    Parameters
    ----------
    conn:
        A read-write connection from :func:`gamecollect.db.connection.connect`.
    source:
        The partition this writer owns (tournament/provider instance id).
        Stamped onto every row this writer touches.
    taxonomy:
        Opaque set-of-strings data declaring the legal event ``type`` values
        (wired from the active pack). ``None`` skips validation.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        source: str,
        taxonomy: Collection[str] | None = None,
    ) -> None:
        if not source:
            raise ValueError("source must be a non-empty string")
        self._conn = conn
        self._source = source
        self._taxonomy = frozenset(taxonomy) if taxonomy is not None else None

    @property
    def source(self) -> str:
        """The partition this writer is scoped to (immutable)."""
        return self._source

    # ------------------------------------------------------------------
    # Matches
    # ------------------------------------------------------------------

    def upsert_match(self, match: Mapping[str, Any]) -> None:
        """Insert or update one ``matches`` row (keyed by ``match_id``).

        Only the keys present in ``match`` are updated on conflict, so a live
        update carrying score/minute does not wipe schedule fields written
        earlier. ``updated_at`` is stamped automatically when not supplied.
        """
        self._check_source(match)
        match_id = self._require(match, "match_id")
        provided = self._pick(match, _MATCH_COLUMNS, _MATCH_KEYS)
        provided.setdefault("updated_at", _utcnow())

        self._assert_match_owned(match_id)

        columns = ["match_id", "source", *provided]
        placeholders = ", ".join("?" for _ in columns)
        updates = ", ".join(f"{col} = excluded.{col}" for col in provided)
        with self._transaction():
            # The WHERE guard on the conflict clause closes the TOCTOU window
            # left by _assert_match_owned's SELECT: if another source's row
            # landed between the check and this statement, the update is a
            # no-op (rowcount 0) instead of silently overwriting foreign data.
            cursor = self._conn.execute(
                f"INSERT INTO matches ({', '.join(columns)}) VALUES ({placeholders}) "
                f"ON CONFLICT (match_id) DO UPDATE SET {updates} "
                f"WHERE matches.source = excluded.source",
                (match_id, self._source, *provided.values()),
            )
            if cursor.rowcount == 0:
                raise CrossPartitionError(
                    f"match {match_id!r} was created by another source concurrently; "
                    f"this writer is scoped to {self._source!r}"
                )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def append_event(self, event: Mapping[str, Any]) -> int:
        """Append one event row (``match_id`` carried in the mapping).

        Convenience over :meth:`append_events`; returns 1 when the row was
        new, 0 when the ``(source, match_id, seq)`` key already existed.
        """
        match_id = self._require(event, "match_id")
        return self.append_events(match_id, [event])

    def append_events(self, match_id: str, events: Iterable[Mapping[str, Any]]) -> int:
        """Append provider-normalized events for one match; return rows inserted.

        Each event mapping must carry an integer ``seq`` (provider-derived)
        and a ``type`` string. Idempotent via ``INSERT OR IGNORE`` on
        ``(source, match_id, seq)``; a NEW row landing at-or-below the
        partition's existing max seq is out-of-order shape drift and raises
        :class:`SequenceError` (re-sent already-stored seqs are ignored, so
        full-list re-polls stay cheap and legal).
        """
        batch = list(events)
        self._assert_match_owned(match_id)
        for event in batch:
            self._check_source(event)
            seq = self._require(event, "seq")
            if not isinstance(seq, int) or isinstance(seq, bool):
                raise SequenceError(f"event seq must be an int, got {seq!r}")
            event_type = self._require(event, "type")
            if self._taxonomy is not None and event_type not in self._taxonomy:
                raise TaxonomyError(
                    f"event type {event_type!r} is not declared by the active "
                    f"pack taxonomy (match {match_id!r}, seq {seq})"
                )
        seqs = [event["seq"] for event in batch]
        if sorted(set(seqs)) != seqs:
            raise SequenceError(
                f"event batch for match {match_id!r} is not strictly increasing by seq: {seqs}"
            )

        row = self._conn.execute(
            "SELECT MAX(seq) FROM events WHERE source = ? AND match_id = ?",
            (self._source, match_id),
        ).fetchone()
        max_seq = row[0] if row and row[0] is not None else None

        inserted = 0
        with self._transaction():
            for event in batch:
                extras = self._pick(event, _EVENT_COLUMNS, _EVENT_KEYS)
                columns = ["source", "match_id", "seq", "type", *extras]
                placeholders = ", ".join("?" for _ in columns)
                cursor = self._conn.execute(
                    f"INSERT OR IGNORE INTO events ({', '.join(columns)}) VALUES ({placeholders})",
                    (self._source, match_id, event["seq"], event["type"], *extras.values()),
                )
                if cursor.rowcount == 1:
                    if max_seq is not None and event["seq"] <= max_seq:
                        raise SequenceError(
                            f"new event seq {event['seq']} for match {match_id!r} is not "
                            f"monotonic (existing max seq {max_seq} in source {self._source!r})"
                        )
                    inserted += 1
        return inserted

    # ------------------------------------------------------------------
    # Entities
    # ------------------------------------------------------------------

    def upsert_entity(self, entity: Mapping[str, Any]) -> None:
        """Insert or update one ``entities`` row (keyed by (source, entity_id)).

        ``name_folded`` is stamped with the core fold of ``display_name``
        unless explicitly supplied (write-fold == query-fold by construction).
        """
        self._check_source(entity)
        entity_id = self._require(entity, "entity_id")
        provided = self._pick(entity, _ENTITY_COLUMNS, _ENTITY_KEYS)
        kind = provided.get("kind")
        display_name = provided.get("display_name")
        if not kind or not display_name:
            raise ValueError("entity rows require non-empty 'kind' and 'display_name'")
        # name_folded is always the core fold of display_name — accepting a
        # caller-supplied value would break write-fold == query-fold.
        folded = fold(display_name)
        supplied = provided.get("name_folded")
        if supplied is not None and supplied != folded:
            raise ValueError(
                f"supplied name_folded {supplied!r} differs from the core fold "
                f"{folded!r} of {display_name!r}; omit name_folded and let the "
                f"writer stamp it"
            )
        provided["name_folded"] = folded

        columns = ["source", "entity_id", *provided]
        placeholders = ", ".join("?" for _ in columns)
        updates = ", ".join(f"{col} = excluded.{col}" for col in provided)
        with self._transaction():
            self._conn.execute(
                f"INSERT INTO entities ({', '.join(columns)}) VALUES ({placeholders}) "
                f"ON CONFLICT (source, entity_id) DO UPDATE SET {updates}",
                (self._source, entity_id, *provided.values()),
            )

    def upsert_entities(self, entities: Iterable[Mapping[str, Any]]) -> None:
        """Upsert a batch of entity rows (see :meth:`upsert_entity`)."""
        for entity in entities:
            self.upsert_entity(entity)

    # ------------------------------------------------------------------
    # Standings
    # ------------------------------------------------------------------

    def upsert_standing(self, standing: Mapping[str, Any]) -> None:
        """Insert or update one ``standings`` row (keyed by (source, group_key, entity_id))."""
        self._check_source(standing)
        group_key = self._require(standing, "group_key")
        entity_id = self._require(standing, "entity_id")
        provided = self._pick(standing, _STANDING_COLUMNS, _STANDING_KEYS)

        columns = ["source", "group_key", "entity_id", *provided]
        placeholders = ", ".join("?" for _ in columns)
        updates = (
            ", ".join(f"{col} = excluded.{col}" for col in provided) or "entity_id = entity_id"
        )
        with self._transaction():
            self._conn.execute(
                f"INSERT INTO standings ({', '.join(columns)}) VALUES ({placeholders}) "
                f"ON CONFLICT (source, group_key, entity_id) DO UPDATE SET {updates}",
                (self._source, group_key, entity_id, *provided.values()),
            )

    # ------------------------------------------------------------------
    # Provider match map
    # ------------------------------------------------------------------

    def map_provider_match(self, provider: str, provider_match_id: str, match_id: str) -> None:
        """Record the provider-native → canonical match-id mapping for this source."""
        self._assert_match_owned(match_id)
        with self._transaction():
            self._conn.execute(
                "INSERT INTO provider_match_map (source, provider, provider_match_id, match_id) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT (source, provider, provider_match_id) "
                "DO UPDATE SET match_id = excluded.match_id",
                (self._source, provider, provider_match_id, match_id),
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _assert_match_owned(self, match_id: str) -> None:
        """Raise when ``match_id`` exists under a different source.

        Child-table writes (events, provider map) reference matches by their
        globally-unique ``match_id``; without this check a writer scoped to
        source B could build a shadow partition under source A's match and
        break the match_id-global reader contract. An absent match row is
        allowed — an event may legally land before its match is seeded.
        """
        owner = self._conn.execute(
            "SELECT source FROM matches WHERE match_id = ?", (match_id,)
        ).fetchone()
        if owner is not None and owner[0] != self._source:
            raise CrossPartitionError(
                f"match {match_id!r} belongs to source {owner[0]!r}; "
                f"this writer is scoped to {self._source!r}"
            )

    def _check_source(self, row: Mapping[str, Any]) -> None:
        declared = row.get("source")
        if declared is not None and declared != self._source:
            raise CrossPartitionError(
                f"row declares source {declared!r} but this writer is scoped to {self._source!r}"
            )

    @staticmethod
    def _require(row: Mapping[str, Any], key: str) -> Any:
        value = row.get(key)
        if value is None:
            raise ValueError(f"row is missing required key {key!r}")
        return value

    def _pick(
        self, row: Mapping[str, Any], allowed: tuple[str, ...], key_columns: frozenset[str]
    ) -> dict[str, Any]:
        """Extract known columns, rejecting unknown keys loudly."""
        unknown = set(row) - set(allowed) - key_columns
        if unknown:
            raise ValueError(f"unknown column(s) {sorted(unknown)}; core columns are {allowed}")
        picked = {col: row[col] for col in allowed if col in row}
        if "payload" in picked:
            picked["payload"] = _encode_payload(picked["payload"])
        return picked

    def _transaction(self) -> _Transaction:
        return _Transaction(self._conn)


class _Transaction:
    """Commit-on-success / rollback-on-error scope around writer statements."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
