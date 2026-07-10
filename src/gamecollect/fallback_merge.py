"""Pure match-merge / comparison helpers used by :class:`~gamecollect.engine.CollectorEngine`.

State-free functions over :class:`~gamecollect.provider.NormalizedMatch` snapshots —
no engine, tracker, or poll-loop state. Split out of ``engine.py`` (which remained
large after the ``--record`` extraction) because this cluster has zero coupling to
engine internals and, like :mod:`gamecollect.record`, is a clean standalone
collaborator.
"""

from __future__ import annotations

from dataclasses import replace

from gamecollect.provider import (
    MatchStatus,
    NormalizedMatch,
    merge_payload_preserving_richer,
)

__all__ = [
    "is_terminal",
    "live_supersedes_captured",
    "live_supersedes_cooldown_fallback",
    "live_supersedes_fallback",
    "fallback_worth_retaining",
    "accrue_fallback",
    "paired_clock",
    "max_known",
    "non_regressing_score",
]

_TERMINAL_STATUSES: frozenset[MatchStatus] = frozenset({MatchStatus.FINISHED})


def is_terminal(status: MatchStatus) -> bool:
    """True when ``status`` is a terminal (end-of-match) lifecycle state."""
    return status in _TERMINAL_STATUSES


def live_supersedes_cooldown_fallback(applied: NormalizedMatch, fallback: NormalizedMatch) -> bool:
    """True when a durable live apply proves a cooldown's terminal fallback stale.

    Thin wrapper over the shared :func:`live_supersedes_captured` policy — see
    that method for the single documented supersession rule (Z2 unified the two
    near-duplicate twins that had already silently diverged once). Intentionally
    behavior-identical to :func:`live_supersedes_fallback` (named separately
    only to document cooldown-fallback vs resumption-fallback call-site intent)
    — do not "fix" one in isolation; edit :func:`live_supersedes_captured`.
    """
    return live_supersedes_captured(applied, fallback)


def live_supersedes_captured(applied: NormalizedMatch, fallback: NormalizedMatch) -> bool:
    """SHARED policy: does a durable live ``applied`` state prove a captured
    terminal ``fallback`` stale? (Z2 — one helper for both supersession sites.)

    Both the cooldown fallback (:func:`live_supersedes_cooldown_fallback`) and
    the resumption-tracker fallback (:func:`live_supersedes_fallback`) are judged
    against a DURABLE applied live state carrying a real minute (not a NULL slate
    board), so they share ONE rule. Supersession holds under EITHER condition:

    1. STRICT score advance with NO regression: the live board advanced STRICTLY
       PAST the fallback's known score on at least one side, and regressed on NO
       side — the match is genuinely still live and the pre-recovery terminal
       fallback stale. A regression on ANY side (e.g. a swapped-score board 2-1
       vs a fallback 1-2) proves nothing and does NOT supersede (Z2 — the tracker
       twin previously superseded on a strict advance even when the OTHER side
       regressed, silently dropping a genuine fallback). A NULL applied score on a
       side the fallback knows also proves nothing (conservative — keep).
    2. EQUAL scores at a STRICTLY LATER minute (Y3): every side's scores are known
       and equal AND the applied live minute is strictly greater than the
       fallback's (both known). Equal scores ALONE are not proof — a lagging
       scoreboard still showing the genuine final 2-1 must not drop a real
       FINISHED 2-1 fallback (round 13: ``>=`` wrongly dropped it). But equal
       scores at a demonstrably later minute ARE proof: a false pre-recovery
       terminal (FINISHED 1-0 min 55) on a match that plays on and never scores
       again would otherwise live forever (round 14: strict ``>`` wrongly kept it).
       Null minutes are conservative — keep the fallback.

    ACCEPTED JUDGMENT CALL (residual): the equal-score minute rule may drop a
    GENUINE final when a provider encodes stoppage time as ``minute > 90`` on a
    lagging live board — e.g. a fallback FINISHED at minute 90 vs a live board at
    minute 93 at the SAME score reads as "play continued" and supersedes. The loss
    is BOUNDED: the scores stay correct (via G1 / the stored terminal row), only
    the terminal's events / clock richness is lost. This residual is accepted
    because the dual — keeping FALSE terminals alive at the final score — was
    judged worse in round 14.
    """
    strictly_advanced = False
    both_sides_known = True
    for field in ("score_home", "score_away"):
        live = getattr(applied, field)
        captured = getattr(fallback, field)
        if captured is None:
            both_sides_known = False
            continue
        # A regression on ANY known side, or a NULL applied score on a side the
        # fallback knows, proves nothing → not stale (Z2: no-regression required).
        if live is None or live < captured:
            return False
        if live > captured:
            strictly_advanced = True
    if strictly_advanced:
        return True
    # Y3: no strict advance and no regression → every known side is EQUAL. Equal
    # scores alone are not proof of liveness (a lagging board at the genuine
    # final), but equal scores at a STRICTLY LATER minute are: play demonstrably
    # continued past the fallback's capture. Null minutes are conservative — keep.
    return (
        both_sides_known
        and applied.minute is not None
        and fallback.minute is not None
        and applied.minute > fallback.minute
    )


def fallback_worth_retaining(fallback: NormalizedMatch) -> bool:
    """True when a fallback carries terminal richness worth keeping.

    A fallback with events, a known score, or a terminal status is a
    best-known terminal state that must survive a bogus live flicker; a
    sparse reset board (no events, NULL scores, non-terminal) carries
    nothing a fresh transition would not rebuild, so its tracker is dropped.
    """
    return bool(
        fallback.events
        or fallback.score_home is not None
        or fallback.score_away is not None
        or is_terminal(fallback.status)
    )


def live_supersedes_fallback(applied: NormalizedMatch, fallback: NormalizedMatch) -> bool:
    """True when a live board proves the captured resumption fallback is stale.

    Thin wrapper over the shared :func:`live_supersedes_captured` policy (Z2).
    The resumption-tracker fallback and the cooldown fallback are judged against
    the same durable applied live state, so they now use ONE rule — including the
    no-regression requirement that keeps a swapped-score board (2-1 vs a fallback
    1-2) from wrongly superseding a genuine fallback on a one-sided advance.
    Intentionally behavior-identical to :func:`live_supersedes_cooldown_fallback`
    (named separately only to document call-site intent) — do not "fix" one in
    isolation; edit :func:`live_supersedes_captured`.
    """
    return live_supersedes_captured(applied, fallback)


def accrue_fallback(old: NormalizedMatch | None, new: NormalizedMatch) -> NormalizedMatch:
    """Field-wise ACCUMULATOR of the best-known fallback state (M1).

    The fallback is no longer "one chosen snapshot" but the accumulated
    best-known state across every non-live observation seen for the match —
    replacing the old ``_richest_fallback`` SELECTION comparator, which
    picked one whole snapshot and so was inherently lossy (a fresh FINISHED
    board carrying a NULL score would win outright over a 2-0 incumbent and
    drop the score; a bare higher-score board would win and drop the
    incumbent's captured events). Each field is merged independently:

    * SCORES: per side the MAX of the known values; a ``None`` NEVER replaces
      a known value (a FINISHED None-0 board over a 2-0 incumbent yields 2-0).
    * REGRESSION VETO (X3, terminality-classed per Y1): when the NEWER side
      (``new``) regresses a known incumbent score on either side (both values
      known, ``new < old``) AND both sides share a terminality class (both live
      or both terminal), the new side is a bogus/reset board — it is
      untrustworthy for every field, so the incumbent keeps status, clock,
      events, payload, and IDENTITY (Y2) precedence and the new side contributes
      ONLY where the incumbent has nothing. The per-side score max still holds
      (it already keeps the higher incumbent).
    * TERMINALITY PREFERENCE PRECEDES THE VETO ACROSS CLASSES (Y1): the veto
      fires ONLY within one terminality class. When the sides differ in class —
      a transient inflated live incumbent (a LIVE 3-0 glitch) versus a genuinely
      FINISHED 2-0 provider final — the terminal side is the provider's final
      view and wins status, clock, events, payload, and identity REGARDLESS of
      the score regression, so its authoritative final events are never dropped.
      Judgment call / residual: the per-side score max still keeps the inflated
      3 (an inflated transient score is indistinguishable from a real one under
      the scores-are-cumulative doctrine), so the final reads FINISHED 3-0.
    * STATUS: prefer the genuinely-terminal side's status; if neither or both
      are terminal the newer side's status wins (unless vetoed above).
    * CLOCK (X1 + Y4 per-field fill): ``minute``/``display_clock`` are sourced
      from status-ELIGIBLE sides only — the status winner, or the OTHER side
      only when it shares the winning status (or, when the winner is terminal,
      is also terminal). A live side's mid-match clock never fills an accumulated
      terminal (an INELIGIBLE side contributes nothing). Y4 relaxes the old
      all-or-nothing PAIR rule to a PER-FIELD fill AMONG ELIGIBLE SIDES: a null
      winner field is filled independently from the eligible other side, so a
      winner's minute 90 / null display_clock merges with an eligible side's
      "90'+3" to 90/"90'+3" (same-status mixing is fine). If no eligible side has
      a clock BOTH stay null (doctrine: a null clock at FT is legitimate —
      emission-time G3 fills from a stored terminal row).
    * EVENTS (X4): a NEWER side that is genuinely terminal with a non-empty
      list is the provider's FINAL authoritative view — its list wins outright,
      even if shorter (a corrected terminal list drops rescinded events).
      Otherwise keep the non-empty list, and the LONGER when both are non-empty
      (events accumulate). The regression veto (X3) overrides this entirely.
    * IDENTITY (home_team/away_team/kickoff_utc): fill ``None``s from either
      side (the newer value wins when both are present) — EXCEPT under the veto
      (Y2), where the incumbent's non-None identity wins and the new side fills
      only where the incumbent is None. ACCEPTED JUDGMENT CALL: under a same-class
      score-regression veto a legitimate identity CORRECTION riding the vetoed
      board is DEFERRED, not lost — it heals on the next non-regressed observation,
      where newer non-None identity wins normally.
    * PAYLOAD: preserve-richer merge (:func:`is_empty_payload_value`) — the
      newer value wins unless empty over a non-empty incumbent; under the
      regression veto the precedence flips so the incumbent's non-empty values
      win and the new side only fills keys the incumbent lacks.
    """
    if old is None:
        return new
    score_home = max_known(old.score_home, new.score_home)
    score_away = max_known(old.score_away, new.score_away)
    # X3: the newer side regresses a known incumbent score on either side.
    regressed = any(
        (o := getattr(old, field)) is not None and (n := getattr(new, field)) is not None and n < o
        for field in ("score_home", "score_away")
    )
    # Y1: the regression veto only fires BETWEEN SIDES OF THE SAME TERMINALITY
    # CLASS. Across classes (one live, one terminal) the genuinely-terminal side
    # is the provider's final view and wins status/clock/events/payload/identity
    # regardless of a score regression; per-side max still keeps the higher
    # score, so an inflated transient score itself persists (see Y1 in docstring).
    vetoed = regressed and (is_terminal(new.status) == is_terminal(old.status))
    if vetoed:
        winner, other = old, new
    elif is_terminal(new.status) and not is_terminal(old.status):
        winner, other = new, old
    elif is_terminal(old.status) and not is_terminal(new.status):
        winner, other = old, new
    else:
        # Both terminal or neither terminal (and not vetoed): the newer side wins.
        winner, other = new, old
    status = winner.status
    # Z3: a vetoed side (the bogus reset ``new``, now ``other``) is untrustworthy
    # for EVERY field — it contributes no clock, so the trusted incumbent's null
    # minute is never filled from the reset board's mid-match clock.
    minute, display_clock = paired_clock(winner, other, other_vetoed=vetoed)
    # Z1: whenever the winner is AUTHORITATIVE — either the same-class regression
    # veto (X3) fired, or the sides differ in terminality class so the genuinely
    # terminal side was chosen as winner — that winner wins events, payload, AND
    # identity precedence SYMMETRICALLY (whichever side is newer is irrelevant; the
    # authoritative side wins), and the other side contributes ONLY the fields the
    # winner lacks. This closes the round-14 leak where a score-regressed bogus
    # LIVE board still fed junk events/identity/payload to a terminal incumbent
    # across classes (only status/clock were routed to the terminal side before).
    winner_authoritative = vetoed or (is_terminal(winner.status) != is_terminal(other.status))
    if winner_authoritative:
        # The authoritative winner keeps its events; the other side contributes
        # only when the winner has none. (X4's "newer terminal non-empty list wins
        # outright" is the same principle — the terminal side is the winner here.)
        events = winner.events if winner.events else other.events
    elif new.events and is_terminal(new.status):
        events = new.events  # X4: newer terminal list is authoritative
    elif not new.events:
        events = old.events
    elif not old.events:
        events = new.events
    else:
        events = new.events if len(new.events) >= len(old.events) else old.events
    if winner_authoritative:
        # Authoritative-winner payload precedence — winner's non-empty values win,
        # the other side only fills keys the winner lacks (or where it is empty).
        payload = merge_payload_preserving_richer(other.payload, winner.payload)
    else:
        payload = merge_payload_preserving_richer(old.payload, new.payload)
    if winner_authoritative:
        # Identity fields also keep the authoritative winner's values, filling
        # only where the winner has None.
        home_team = winner.home_team if winner.home_team is not None else other.home_team
        away_team = winner.away_team if winner.away_team is not None else other.away_team
        kickoff_utc = winner.kickoff_utc if winner.kickoff_utc is not None else other.kickoff_utc
    else:
        home_team = new.home_team if new.home_team is not None else old.home_team
        away_team = new.away_team if new.away_team is not None else old.away_team
        kickoff_utc = new.kickoff_utc if new.kickoff_utc is not None else old.kickoff_utc
    return replace(
        new,
        status=status,
        minute=minute,
        score_home=score_home,
        score_away=score_away,
        display_clock=display_clock,
        events=events,
        home_team=home_team,
        away_team=away_team,
        kickoff_utc=kickoff_utc,
        payload=payload,
    )


def paired_clock(
    winner: NormalizedMatch, other: NormalizedMatch, *, other_vetoed: bool = False
) -> tuple[int | None, str | None]:
    """X1 + Y4 + Z3/Z4: minute/display_clock filled PER-FIELD among ELIGIBLE sides.

    The clock is sourced from status-eligible sides only, never from an
    ineligible one. The ``winner`` (which defines the winning status) supplies
    every field it knows; each field the winner leaves null is then filled
    INDEPENDENTLY (Y4) from the ``other`` side, but ONLY when ``other`` is
    eligible. Eligibility now has two gates:

    * STATUS (X1): ``other`` shares the winning status, or (when the winner is
      terminal) is itself terminal. A live side's mid-match clock can never leak
      into an accumulated terminal.
    * NOT VETOED (Z3): when ``other`` is the score-regression-vetoed reset board
      (``other_vetoed``), it is untrustworthy for EVERY field and contributes NO
      clock at all — its minute 3 never fills the trusted incumbent's null minute
      (which would otherwise pair minute 3 with the incumbent's display_clock).

    Even among eligible sides, a field is filled only when it does NOT CONTRADICT
    the winner's own clock (Z4 — no self-contradictory mixed-time pairs):

    * display_clock is filled from ``other`` only when ``other.minute`` does not
      contradict the winner's minute (``other.minute`` is None, or the winner has
      no minute, or they are equal). So winner (90, None) + other (None, "90'+3")
      → (90, "90'+3") [Y4], but winner (90, None) + other (87, "87'") → (90, None)
      [Z4: 87 ≠ 90 contradicts, so no fill].
    * minute is filled from ``other`` only when ``other.display_clock`` does not
      contradict the winner's display_clock (``other.display_clock`` is None, or
      the winner has none, or they are equal).

    The winner's own known field is never overwritten; guards compare against the
    winner's ORIGINAL clock so the two fills cannot contaminate each other.
    """
    minute = winner.minute
    display_clock = winner.display_clock
    if minute is not None and display_clock is not None:
        return minute, display_clock
    if other_vetoed:  # Z3: a vetoed reset board contributes no clock, period.
        return minute, display_clock
    other_eligible = other.status == winner.status or (
        is_terminal(winner.status) and is_terminal(other.status)
    )
    if not other_eligible:
        return minute, display_clock
    # Y4 + Z4: fill each null field independently from the eligible other side, but
    # ONLY when the borrowed field does not contradict the winner's own clock.
    if minute is None and other.minute is not None:
        if (
            winner.display_clock is None
            or other.display_clock is None
            or other.display_clock == winner.display_clock
        ):
            minute = other.minute
    if display_clock is None and other.display_clock is not None:
        if winner.minute is None or other.minute is None or other.minute == winner.minute:
            display_clock = other.display_clock
    return minute, display_clock


def max_known(a: int | None, b: int | None) -> int | None:
    """Per-side score max where ``None`` (unknown) never replaces a known value."""
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def non_regressing_score(candidate: int | None, stored: int | None) -> int | None:
    """Per-side max with ``None``-fill (G1): never below the stored value."""
    if stored is None:
        return candidate
    if candidate is None:
        return stored
    return max(candidate, stored)
