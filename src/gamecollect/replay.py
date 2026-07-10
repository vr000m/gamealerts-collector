"""ReplayProvider: a recorded fixture, replayed through the provider ABC.

``ReplayProvider`` is a first-class :class:`~gamecollect.provider.MatchDataProvider`
(DESIGN.md §7), not test scaffolding: it is the eval/hardening harness any
consumer uses to replay a recorded match and assert behaviour against ground
truth. The engine polls a replay provider exactly as it polls a live one — the
loop cannot tell them apart, which is what makes replay a real end-to-end
harness.

A fixture (see :mod:`gamecollect.fixture_io`) records a match's **final** state
plus its full, ordered event stream. The provider streams that event stream out
over successive polls: each poll reveals more of the recorded events, so a
consumer sees the match unfold rather than arriving whole. Two pacing modes:

* **Paced** (finite ``speed``) — an event recorded at game-minute *m* is
  revealed once ``m*60 / speed`` seconds of wall-clock have elapsed since the
  first poll. ``speed=60`` replays a 90-minute match in ~90 seconds. Wall-clock
  based, so NOT deterministic across runs.
* **Step** (``speed=math.inf``) — each :meth:`fetch_live_matches` reveals
  exactly one more recorded event, with no clock. Deterministic: N polls stream
  the first N events. This is the mode the determinism/e2e tests use — drive the
  replay by polling and watch :attr:`exhausted` for completion.

Design note (sport-agnostic core): a fixture stores only the recorded *final*
match state (one header) — it does not snapshot intermediate scores/status, and
the core cannot recompute a running score (that "a goal increments the score" is
sport knowledge the football pack owns, not core). So every snapshot carries the
fixture's recorded header state verbatim while the **event list grows**; the
terminal (fully-revealed) snapshot equals the fixture header exactly, which is
what makes a record→replay round trip reproduce the original fixture. Consumers
that need a running score derive it from the revealed event stream.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from gamecollect.fixture_io import Fixture, read_fixture
from gamecollect.provider import (
    MatchDataProvider,
    NormalizedMatch,
    ProviderUnavailableError,
)

__all__ = ["ReplayProvider"]


class ReplayProvider(MatchDataProvider):
    """Replay one recorded fixture through the :class:`MatchDataProvider` ABC.

    Parameters
    ----------
    fixture:
        A :class:`~gamecollect.fixture_io.Fixture` (one match), or a path to a
        fixture JSON file (read via :func:`~gamecollect.fixture_io.read_fixture`).
    speed:
        Wall-clock acceleration for paced replay (must be positive).
        ``math.inf`` selects **step mode** — one recorded event revealed per
        :meth:`fetch_live_matches`, fully deterministic (no wall clock).
    monotonic:
        Injectable monotonic clock ``() -> float`` for paced mode (defaults to
        :func:`time.monotonic`); unused in step mode. Tests that exercise paced
        pacing inject a controllable clock.

    Usage (step mode, engine-driven — the e2e/determinism pattern)::

        provider = ReplayProvider(fixture, speed=math.inf)
        engine = CollectorEngine(
            pack, db, source, provider=provider,
            sleep=lambda _delay: provider.exhausted and engine.stop(),
        )
        engine.run()   # polls until every recorded event has been streamed

    Each engine poll calls :meth:`fetch_live_matches` once, which reveals the
    next recorded event; :attr:`exhausted` turns ``True`` after the last event
    is revealed. A zero-event (scheduled) fixture is :attr:`exhausted` from the
    first poll, so the loop terminates cleanly with only the seeded match row.
    """

    def __init__(
        self,
        fixture: Fixture | str | Path,
        speed: float = 1.0,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if isinstance(fixture, Fixture):
            self._fixture = fixture
        else:
            self._fixture = read_fixture(fixture)
        if not (speed > 0):
            raise ValueError(f"speed must be positive, got {speed!r}")

        self._match = self._fixture.match
        # Events streamed in seq order — a revealed prefix keeps the writer's
        # monotonic-seq invariant (a non-prefix reveal could send a low seq
        # after a higher one and trip SequenceError).
        self._events = sorted(self._match.events, key=lambda e: e.seq)
        self._speed = float(speed)
        self._step_mode = math.isinf(self._speed)
        self._monotonic = monotonic

        # Step mode: number of events revealed so far (advanced by each fetch).
        self._revealed = 0
        # Paced mode: monotonic instant of the first poll (lazily set).
        self._start: float | None = None

    # ------------------------------------------------------------------
    # Provider ABC
    # ------------------------------------------------------------------

    def fetch_live_matches(self) -> list[NormalizedMatch]:
        """Reveal the next recorded events and return the current snapshot.

        Step mode advances the reveal cursor by exactly one event per call;
        paced mode advances it to every event whose recorded time has elapsed.
        Always returns a single-element list (a fixture is one match), so the
        engine collects it exactly as it would a live slate.
        """
        if self._step_mode:
            if self._revealed < len(self._events):
                self._revealed += 1
        else:
            self._revealed = self._paced_count()
        return [self._snapshot()]

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        """Return the current snapshot for ``match_id`` (the replayed match).

        Raises :class:`~gamecollect.provider.ProviderUnavailableError` for any
        other id — a replay provider hosts exactly the one recorded match.
        """
        if match_id != self._match.match_id:
            raise ProviderUnavailableError(
                f"replay provider hosts match {self._match.match_id!r}, not {match_id!r}"
            )
        return self._snapshot()

    # ------------------------------------------------------------------
    # Replay state
    # ------------------------------------------------------------------

    @property
    def exhausted(self) -> bool:
        """True once every recorded event has been *revealed by a fetch call*.

        Drives the engine-driven replay loop's stop condition. Reflects
        ``self._revealed`` — the count the last :meth:`fetch_live_matches`
        returned — in BOTH modes, so it never runs ahead of the events a
        consumer has actually received. A wall-clock reading (paced
        ``_paced_count``) would report exhaustion the instant enough time
        elapsed, letting the stop-then-check loop truncate the tail events that
        no fetch has yet revealed; it would also lazily start the pacing clock as
        a side effect of merely reading the property. Tying it to ``_revealed``
        avoids both. A zero-event fixture is exhausted from the outset (no fetch
        needed).
        """
        return self._revealed >= len(self._events)

    def _paced_count(self) -> int:
        """Number of leading events whose recorded time has elapsed (paced mode)."""
        if self._start is None:
            self._start = self._monotonic()
        elapsed = self._monotonic() - self._start
        budget = elapsed * self._speed  # seconds of game time revealed so far
        count = 0
        for event in self._events:  # seq order — reveal a prefix
            offset_seconds = (event.minute or 0) * 60.0
            if offset_seconds <= budget:
                count += 1
            else:
                break
        return count

    def _snapshot(self) -> NormalizedMatch:
        """Fixture header state (verbatim) plus the revealed event prefix."""
        return replace(
            self._match,
            events=list(self._events[: self._revealed]),
            payload=dict(self._match.payload),
        )
