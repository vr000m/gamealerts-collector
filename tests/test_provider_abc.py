"""Phase 3: ``gamecollect.provider`` — ABC, Normalized* dataclasses, errors.

Plan contract (docs/dev_plans/20260702-feature-collector-foundation.md §Phase 3):
- ``MatchDataProvider`` ABC with abstract ``fetch_live_matches() ->
  list[NormalizedMatch]`` and ``fetch_match_detail(id)``; it cannot be
  instantiated directly, and a minimal concrete subclass works.
- ``Normalized*`` dataclasses generalized from gamealerts: the event-type
  field is a plain ``str`` (pack-validated at write time, not a core enum),
  ``MatchStatus`` stays a core enum, ``NormalizedMatch`` gains
  ``payload: dict`` for sport extras.
- Error hierarchy ``ProviderError`` / ``ShapeDriftError`` /
  ``ProviderUnavailableError`` ported unchanged (the latter two subclass
  ``ProviderError``).

The plan pins these names but not every field of the generalized dataclasses,
so construction goes through :func:`make_instance`, which fills only the
required (default-less) fields with type-appropriate dummies.
"""

import dataclasses
import enum
from typing import Any

import pytest

from gamecollect.provider import (
    MatchDataProvider,
    MatchStatus,
    NormalizedEvent,
    NormalizedMatch,
    ProviderError,
    ProviderUnavailableError,
    ShapeDriftError,
)

# --- construction helpers ----------------------------------------------------------


def _dummy_for(annotation: Any) -> Any:
    a = str(annotation)
    if "MatchStatus" in a:
        return next(iter(MatchStatus))
    if "list" in a or "List" in a:
        return []
    if "dict" in a or "Dict" in a:
        return {}
    if "tuple" in a or "Tuple" in a:
        return ()
    if "bool" in a:
        return False
    if "int" in a:
        return 0
    if "float" in a:
        return 0.0
    return "x"


def make_instance(cls: type, **overrides: Any) -> Any:
    """Build a dataclass instance filling only required fields with dummies."""
    kwargs: dict[str, Any] = {}
    for f in dataclasses.fields(cls):
        if f.name in overrides:
            kwargs[f.name] = overrides.pop(f.name)
        elif f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
            kwargs[f.name] = _dummy_for(f.type)
    kwargs.update(overrides)
    return cls(**kwargs)


def event_type_field_name() -> str:
    """The plan pins the event-type field as ``event_type`` or ``type``."""
    names = {f.name for f in dataclasses.fields(NormalizedEvent)}
    for candidate in ("event_type", "type"):
        if candidate in names:
            return candidate
    raise AssertionError(f"NormalizedEvent has no event_type/type field; fields: {sorted(names)}")


# --- ABC ---------------------------------------------------------------------------


def test_abc_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        MatchDataProvider()


def test_abc_declares_the_two_fetch_methods_abstract():
    abstract = set(MatchDataProvider.__abstractmethods__)
    assert {"fetch_live_matches", "fetch_match_detail"} <= abstract


def test_partial_subclass_still_abstract():
    class OnlyLive(MatchDataProvider):
        def fetch_live_matches(self):
            return []

    with pytest.raises(TypeError):
        OnlyLive()


def test_minimal_concrete_subclass_works():
    match = make_instance(NormalizedMatch, match_id="m1")

    class Fake(MatchDataProvider):
        def fetch_live_matches(self):
            return [match]

        def fetch_match_detail(self, match_id):
            assert match_id == "m1"
            return match

    p = Fake()
    live = p.fetch_live_matches()
    assert isinstance(live, list)
    assert live == [match]
    assert p.fetch_match_detail("m1") is match


# --- Normalized* dataclasses -------------------------------------------------------


def test_normalized_match_and_event_are_dataclasses():
    assert dataclasses.is_dataclass(NormalizedMatch)
    assert dataclasses.is_dataclass(NormalizedEvent)


def test_event_type_is_plain_str_not_enum():
    name = event_type_field_name()
    ev = make_instance(NormalizedEvent, **{name: "goal"})
    value = getattr(ev, name)
    assert isinstance(value, str)
    assert not isinstance(value, enum.Enum), "event type must be a plain pack-declared string"
    assert value == "goal"


def test_normalized_match_has_payload_dict():
    names = {f.name for f in dataclasses.fields(NormalizedMatch)}
    assert "payload" in names, f"NormalizedMatch lacks payload field; fields: {sorted(names)}"
    m = make_instance(NormalizedMatch, match_id="m1", payload={"round": "group A"})
    assert m.payload == {"round": "group A"}


def test_football_specific_dataclasses_not_in_core():
    # NormalizedStats / NormalizedLineup* move to the football pack (Phase 4);
    # the core provider module must stay sport-agnostic.
    import gamecollect.provider as provider_mod

    for football_only in ("NormalizedStats", "NormalizedLineup", "NormalizedLineupPlayer"):
        assert not hasattr(provider_mod, football_only), (
            f"{football_only} is football-specific and belongs in the pack, not core"
        )


# --- MatchStatus core enum ---------------------------------------------------------


def test_match_status_is_core_enum_with_ported_members():
    assert issubclass(MatchStatus, enum.Enum)
    values = {m.value for m in MatchStatus}
    # Parity with gamealerts data/provider.py MatchStatus.
    assert {"SCHEDULED", "IN_PLAY", "PAUSED", "FINISHED", "UNKNOWN"} <= values


# --- error hierarchy ---------------------------------------------------------------


def test_error_hierarchy():
    assert issubclass(ProviderError, Exception)
    assert issubclass(ShapeDriftError, ProviderError)
    assert issubclass(ProviderUnavailableError, ProviderError)


def test_subclass_errors_catchable_as_provider_error():
    with pytest.raises(ProviderError):
        raise ShapeDriftError("unexpected ESPN shape")
    with pytest.raises(ProviderError):
        raise ProviderUnavailableError("ESPN 5xx")
