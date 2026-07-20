"""One-shot historical-match enumeration and apply for the ``backfill`` CLI.

Pure, independently testable building blocks — date-range chunking,
terminal-status scoreboard enumeration, and a one-shot apply loop — built on
top of :class:`~gamecollect.engine.CollectorEngine`'s existing write path via
:meth:`CollectorEngine.apply_one_off_match`. No new persistence logic is
introduced here and no poll-loop-only engine in-memory state
(``_last``/``_transitions``/``_backfill``/``_backfill_apply_pending``/
cooldowns) is read or written by anything in this module — see the
historical-backfill dev plan's Requirements and Integration Seams.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from gamecollect.db.writer import CrossPartitionError
from gamecollect.engine import CollectorEngine
from gamecollect.fallback_merge import is_terminal as _is_terminal
from gamecollect.provider import NormalizedMatch

log = logging.getLogger(__name__)

__all__ = [
    "BackfillReport",
    "MissingScheduleSupportError",
    "chunk_date_range",
    "enumerate_finished_matches",
    "run_backfill",
]


class MissingScheduleSupportError(TypeError):
    """Raised when a provider lacks the historical-enumeration methods.

    Deliberately a typed error rather than a bare ``AttributeError`` so
    callers (and the CLI) can distinguish "this provider doesn't support
    backfill" from an unrelated attribute bug.
    """


def chunk_date_range(start: date, end: date, *, overlap_days: int = 1) -> Iterator[str]:
    """Yield single-day ``YYYYMMDD`` chunk strings covering ``[start, end]``.

    Pads ``overlap_days`` on each side of the requested window by default:
    ESPN's scoreboard rollover is slate-based, not calendar-midnight-based,
    so a match's containing day key is not provably its calendar kickoff
    date. Because the writer's core operations are idempotent, re-enumerating
    a match under an adjacent day's padded chunk is a safe no-op re-apply,
    not a duplicate, making the padding a conservative default rather than a
    bare non-overlapping calendar walk (see the dev plan's Requirements).

    Every yielded chunk is exactly one day — there is no hyphenated
    multi-day range form; only single-day ``dates=YYYYMMDD`` queries have
    confirmed provider behavior. Yields nothing when ``start > end``.
    """
    if start > end:
        return
    day = start - timedelta(days=overlap_days)
    last_day = end + timedelta(days=overlap_days)
    while day <= last_day:
        yield day.strftime("%Y%m%d")
        day += timedelta(days=1)


def _fetch_terminal_matches(provider: Any, chunk: str) -> list[NormalizedMatch]:
    """Fetch one date chunk's scoreboard and filter to terminal-status matches."""
    if not hasattr(provider, "fetch_schedule"):
        raise MissingScheduleSupportError(
            f"provider {provider!r} has no fetch_schedule method; historical "
            "backfill requires a provider that supports schedule enumeration"
        )
    matches = provider.fetch_schedule(dates=chunk)
    return [match for match in matches if _is_terminal(match.status)]


def enumerate_finished_matches(
    provider: Any, date_chunks: Iterable[str]
) -> Iterator[NormalizedMatch]:
    """Yield terminal-status matches across ``date_chunks`` via ``provider.fetch_schedule``.

    Pure enumeration only: no writes, no skip/dedup logic (that lives in
    :func:`run_backfill`). Raises :class:`MissingScheduleSupportError` — not
    a bare ``AttributeError`` — if ``provider`` lacks ``fetch_schedule``.
    """
    for chunk in date_chunks:
        yield from _fetch_terminal_matches(provider, chunk)


@dataclass(frozen=True)
class BackfillReport:
    """Summary counts for one :func:`run_backfill` invocation."""

    enumerated: int = 0
    already_stored_skipped: int = 0
    applied: int = 0
    failed: dict[str, Exception] = field(default_factory=dict)
    failed_chunks: list[tuple[str, Exception]] = field(default_factory=list)


def run_backfill(
    engine: CollectorEngine,
    provider: Any,
    date_chunks: Iterable[str],
    *,
    source: str,
    request_delay: float = 0.5,
) -> BackfillReport:
    """Enumerate ``date_chunks`` and apply each not-yet-stored terminal match once.

    ``source`` must equal ``engine.source`` — checked immediately at entry,
    before any enumeration or fetch, so a caller-supplied value that has
    drifted from the engine it is driving fails loudly rather than silently
    reading one partition while writing another.

    For each date chunk, ``provider.fetch_schedule(dates=chunk)`` is wrapped
    in a try/except: a chunk-level failure is recorded in
    :attr:`BackfillReport.failed_chunks` and enumeration continues with the
    next chunk rather than aborting the whole run.

    For each enumerated terminal match: skipped (counted in
    ``already_stored_skipped``) only when the stored ``source`` equals this
    run's ``source`` AND events landed — resolved in a single
    ``engine.stored_source_and_has_events(match_id)`` call (one stored-id
    resolution instead of two). A same-source stored row with zero events is
    retried, not skipped. ``has_events`` is a RELIABLE "already fully stored"
    proxy because :meth:`CollectorEngine.apply_one_off_match` writes events
    LAST (``side_tables_first``): a match whose side-table persist failed has
    no stored events either, so it is retried on a rerun rather than skipped
    with its stats/lineups/venue stranded. Otherwise sleeps
    ``request_delay`` seconds (rate-limit mitigation), fetches full detail
    via ``provider.fetch_match_detail(match_id)``, and applies via
    ``engine.apply_one_off_match(scoreboard, detail)``. The whole per-match
    body — skip check included — runs inside one try/except, so a transient
    failure from the skip-check reads (e.g. a ``sqlite3.OperationalError``
    "database is locked" while a live ``collect`` daemon holds the same
    ``--db``) is recorded in :attr:`BackfillReport.failed` like any other
    per-match failure rather than aborting the run. An ordinary fetch/apply
    failure (including an ``apply_one_off_match`` ``None`` return, meaning
    nothing was durably written) is recorded the same way, keyed by match
    id; one bad match never aborts the run.

    ``CrossPartitionError`` is the one exception NOT swallowed into
    ``failed``: it signals a run-wide ``--source`` misconfiguration (every
    subsequent match would raise the same error), so it propagates
    uncaught — the caller (the CLI's ``_run_backfill``) is the single place
    that catches it and stops the run with an actionable message.
    """
    if source != engine.source:
        raise ValueError(
            f"backfill source {source!r} does not match the engine's source "
            f"{engine.source!r}; refusing to write to a different partition "
            "than the one being read for the skip check"
        )

    enumerated = 0
    already_stored_skipped = 0
    applied = 0
    failed: dict[str, Exception] = {}
    failed_chunks: list[tuple[str, Exception]] = []

    for chunk in date_chunks:
        try:
            matches = _fetch_terminal_matches(provider, chunk)
        except MissingScheduleSupportError:
            # A run-wide configuration problem, not a transient per-chunk
            # one — every subsequent chunk would fail identically.
            raise
        except Exception as exc:
            log.warning("fetch_schedule failed for chunk %s: %s", chunk, exc)
            failed_chunks.append((chunk, exc))
            continue

        for match in matches:
            enumerated += 1
            match_id = match.match_id

            try:
                stored_source, has_events = engine.stored_source_and_has_events(match_id)
                if stored_source == source and has_events:
                    already_stored_skipped += 1
                    continue
                if request_delay:
                    time.sleep(request_delay)
                detail = provider.fetch_match_detail(match_id)
                applied_match = engine.apply_one_off_match(match, detail)
            except CrossPartitionError:
                raise
            except Exception as exc:
                log.warning("backfill apply failed for match %s: %s", match_id, exc)
                failed[match_id] = exc
                continue

            if applied_match is None:
                failed[match_id] = RuntimeError(
                    f"apply_one_off_match returned None for match {match_id!r} "
                    "(no durable write landed)"
                )
            else:
                applied += 1

    return BackfillReport(
        enumerated=enumerated,
        already_stored_skipped=already_stored_skipped,
        applied=applied,
        failed=failed,
        failed_chunks=failed_chunks,
    )
