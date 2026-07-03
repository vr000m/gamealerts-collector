"""Football event taxonomy — the gamealerts ``EventType`` value set, as data.

Ported out of gamealerts ``data/provider.py``: the nine ``EventType`` enum
values become the pack-declared ``events.type`` strings, and the
``EVENT_IMPORTANCE`` mapping becomes each declaration's
``importance_default`` on the small-is-critical integer scale gamealerts
established (1=critical, 2=high, 3=medium, 4=low).

Nothing is dropped or renamed relative to the source enum — the taxonomy
parity test asserts this set equals gamealerts' ``EventType`` value set.
"""

from __future__ import annotations

from gamecollect.packs.spec import EventTypeDecl

__all__ = ["TAXONOMY", "EVENT_IMPORTANCE"]

# Importance scale (gamealerts EventImportance): 1=CRITICAL, 2=HIGH,
# 3=MEDIUM, 4=LOW. CRITICAL: goal, own_goal, penalty (converted);
# HIGH: red, full_time; MEDIUM: yellow, half_time, kickoff; LOW: sub.
TAXONOMY: dict[str, EventTypeDecl] = {
    "goal": EventTypeDecl(display_name="Goal", importance_default=1),
    "own_goal": EventTypeDecl(display_name="Own goal", importance_default=1),
    "penalty": EventTypeDecl(display_name="Penalty", importance_default=1),
    "yellow": EventTypeDecl(display_name="Yellow card", importance_default=3),
    "red": EventTypeDecl(display_name="Red card", importance_default=2),
    "sub": EventTypeDecl(display_name="Substitution", importance_default=4),
    "kickoff": EventTypeDecl(display_name="Kickoff", importance_default=3),
    "half_time": EventTypeDecl(display_name="Half time", importance_default=3),
    "full_time": EventTypeDecl(display_name="Full time", importance_default=2),
}

# Convenience view used by the ESPN adapter when stamping per-event
# importance — identical to gamealerts' EVENT_IMPORTANCE, keyed by the
# taxonomy string instead of the enum member.
EVENT_IMPORTANCE: dict[str, int] = {
    event_type: decl.importance_default for event_type, decl in TAXONOMY.items()
}
