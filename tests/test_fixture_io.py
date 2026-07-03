"""Phase 1: ``gamecollect.fixture_io`` — versioned fixture JSON round-trip.

Plan contract (docs/dev_plans/20260702-feature-collector-interfaces.md §Phase 1):

- Fixture JSON format: match header + ordered events with original
  timestamps/minutes + entities snapshot + standings snapshot (nullable, for
  sources that have them) + a **format version field**.
- ``write_fixture`` / ``read_fixture`` round-trip; Phase 4's replay/importer
  consume this module unchanged.
- Edge case: a zero-event (scheduled) fixture synthesized via ``write_fixture``
  round-trips and parses cleanly.

The plan pins the module name, the two function names, the versioned shape and
the "facts only" content, but NOT the exact call signature or the returned
object's type (dict vs dataclass vs ``NormalizedMatch``). These tests therefore:

- Build inputs from the core domain objects the engine already holds
  (``NormalizedMatch`` / ``NormalizedEvent``), since ``--record`` writes fixtures
  from provider snapshots.
- Call ``write_fixture(path, match, ...)`` adapting to whichever optional
  ``entities`` / ``standings`` parameters the signature exposes.
- Assert *invariants* (match id, event count, seq order, minutes, version field)
  via tolerant extraction rather than a pinned object shape, so a reasonable
  implementation is not forced into one representation.
"""

from __future__ import annotations

import inspect
import json

import pytest

from gamecollect.provider import MatchStatus, NormalizedEvent, NormalizedMatch

_MISSING = object()


# --------------------------------------------------------------------------- #
# Domain-object factories
# --------------------------------------------------------------------------- #


def ev(seq: int, event_type: str = "goal", *, minute: int | None = 10) -> NormalizedEvent:
    return NormalizedEvent(
        seq=seq,
        minute=minute,
        event_type=event_type,
        importance=1,
        team="Canada",
        player="Jonathan David",
        assist=None,
        detail=f"{event_type} at {minute}",
    )


def nm(
    match_id: str,
    events: tuple[NormalizedEvent, ...] = (),
    *,
    status: MatchStatus = MatchStatus.FINISHED,
) -> NormalizedMatch:
    return NormalizedMatch(
        match_id=match_id,
        status=status,
        minute=90 if events else None,
        score_home=len([e for e in events if e.event_type == "goal"]),
        score_away=0,
        display_clock="FT" if events else None,
        events=list(events),
        home_team="Canada",
        away_team="Qatar",
        kickoff_utc="2026-06-18T18:00:00Z",
    )


# --------------------------------------------------------------------------- #
# Adapters over the (name-pinned, signature-unpinned) module surface
# --------------------------------------------------------------------------- #


def _fixture_io():
    import gamecollect.fixture_io as fio

    assert hasattr(fio, "write_fixture"), "fixture_io must expose write_fixture"
    assert hasattr(fio, "read_fixture"), "fixture_io must expose read_fixture"
    return fio


def _write(fio, path, match, *, entities=_MISSING, standings=_MISSING):
    """Call ``write_fixture(path, match, ...)`` passing entities/standings only
    when the signature accepts them."""
    params = inspect.signature(fio.write_fixture).parameters
    kwargs = {}
    if entities is not _MISSING and "entities" in params:
        kwargs["entities"] = entities
    if standings is not _MISSING and "standings" in params:
        kwargs["standings"] = standings
    try:
        return fio.write_fixture(str(path), match, **kwargs)
    except TypeError as exc:  # pragma: no cover - surfaces a real contract mismatch
        raise AssertionError(
            f"write_fixture(str(path), match, **{kwargs}) did not match the "
            f"implementation signature {inspect.signature(fio.write_fixture)}: {exc}"
        ) from exc


def _read(fio, path):
    return fio.read_fixture(str(path))


def _get(obj, *names):
    for n in names:
        if isinstance(obj, dict):
            if n in obj:
                return obj[n]
        elif hasattr(obj, n):
            return getattr(obj, n)
    return _MISSING


def _match_id_of(fixture):
    header = _get(fixture, "match", "header", "match_header")
    if header is not _MISSING:
        mid = _get(header, "match_id", "id")
        if mid is not _MISSING:
            return mid
    return _get(fixture, "match_id", "id")


def _events_of(fixture):
    events = _get(fixture, "events")
    if events is _MISSING:
        header = _get(fixture, "match", "header", "match_header")
        if header is not _MISSING:
            events = _get(header, "events")
    assert events is not _MISSING and events is not None, (
        f"read_fixture result exposes no events: {fixture!r}"
    )
    return list(events)


def _event_seq(event):
    seq = _get(event, "seq")
    assert seq is not _MISSING, f"fixture event exposes no seq: {event!r}"
    return seq


def _event_minute(event):
    minute = _get(event, "minute")
    return None if minute is _MISSING else minute


def _find_version_keys(data) -> list[tuple[str, object]]:
    found: list[tuple[str, object]] = []

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(k, str) and "version" in k.lower():
                    found.append((k, v))
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return found


# --------------------------------------------------------------------------- #
# Round-trip
# --------------------------------------------------------------------------- #


def test_write_read_round_trip_preserves_match_and_events(tmp_path):
    fio = _fixture_io()
    path = tmp_path / "fixture.json"
    match = nm("canada-qatar", (ev(0, "goal", minute=13), ev(1, "goal", minute=44)))

    _write(fio, path, match)
    result = _read(fio, path)

    assert _match_id_of(result) == "canada-qatar"
    events = _events_of(result)
    assert len(events) == 2
    assert [_event_seq(e) for e in events] == [0, 1]


def test_fixture_json_carries_a_format_version_field(tmp_path):
    fio = _fixture_io()
    path = tmp_path / "fixture.json"
    _write(fio, path, nm("m1", (ev(0, "goal"),)))

    data = json.loads(path.read_text())
    versions = _find_version_keys(data)
    assert versions, f"fixture JSON carries no *version* field: {sorted(_top_keys(data))}"
    assert all(v is not None and v != "" for _, v in versions), (
        f"fixture version field is empty: {versions}"
    )


def _top_keys(data):
    return data.keys() if isinstance(data, dict) else ()


def test_events_preserve_order_and_original_minutes(tmp_path):
    fio = _fixture_io()
    path = tmp_path / "fixture.json"
    match = nm(
        "m1",
        (
            ev(0, "goal", minute=13),
            ev(1, "yellow", minute=27),
            ev(2, "goal", minute=61),
        ),
    )

    _write(fio, path, match)
    events = _events_of(_read(fio, path))

    assert [_event_seq(e) for e in events] == [0, 1, 2]
    assert [_event_minute(e) for e in events] == [13, 27, 61]


def test_zero_event_scheduled_fixture_round_trips(tmp_path):
    fio = _fixture_io()
    path = tmp_path / "scheduled.json"
    scheduled = nm("upcoming", (), status=MatchStatus.SCHEDULED)

    _write(fio, path, scheduled)
    result = _read(fio, path)

    assert _match_id_of(result) == "upcoming"
    assert _events_of(result) == [] or len(_events_of(result)) == 0
    # A zero-event fixture is still a valid, versioned document.
    assert _find_version_keys(json.loads(path.read_text()))


def test_fixture_file_is_valid_json(tmp_path):
    fio = _fixture_io()
    path = tmp_path / "fixture.json"
    _write(fio, path, nm("m1", (ev(0, "goal"),)))
    # Must parse as JSON (portable, diffable, no binary blobs).
    json.loads(path.read_text())


# --------------------------------------------------------------------------- #
# Optional snapshots (entities / nullable standings)
# --------------------------------------------------------------------------- #


def test_standings_snapshot_is_nullable(tmp_path):
    fio = _fixture_io()
    if "standings" not in inspect.signature(fio.write_fixture).parameters:
        pytest.skip("write_fixture does not expose a standings parameter")
    path = tmp_path / "no_standings.json"
    _write(fio, path, nm("m1", (ev(0, "goal"),)), standings=None)
    # A source without standings still produces a readable fixture.
    result = _read(fio, path)
    assert _match_id_of(result) == "m1"


# --------------------------------------------------------------------------- #
# Malformed fixtures raise FixtureFormatError (never a raw KeyError/TypeError)
# --------------------------------------------------------------------------- #


def _valid_fixture_dict() -> dict:
    """A minimal well-formed fixture document to mutate into malformed shapes."""
    return {
        "format_version": 1,
        "match": {"match_id": "m1", "status": "IN_PLAY"},
        "events": [{"seq": 0, "event_type": "goal", "importance": 1}],
        "entities": [],
        "standings": [],
    }


@pytest.mark.parametrize(
    "mutate, needle",
    [
        (lambda d: d["events"][0].pop("seq"), "seq"),
        (lambda d: d["events"][0].pop("event_type"), "event_type"),
        (lambda d: d["events"][0].pop("importance"), "importance"),
        (lambda d: d["match"].pop("match_id"), "match_id"),
        (lambda d: d.__setitem__("events", {"not": "a list"}), "events"),
        (lambda d: d.__setitem__("events", ["not-a-dict"]), "event"),
        (lambda d: d.__setitem__("match", "not-a-dict"), "match"),
    ],
)
def test_malformed_fixture_raises_fixture_format_error(tmp_path, mutate, needle):
    """A missing event key, missing ``match_id``, or non-dict ``events``/``match``
    entry must raise the documented ``FixtureFormatError`` (with a useful
    message) rather than leaking a raw ``KeyError``/``TypeError`` — the read
    contract callers catch is ``FixtureFormatError`` alone."""
    fio = _fixture_io()
    assert hasattr(fio, "FixtureFormatError"), "fixture_io must expose FixtureFormatError"
    data = _valid_fixture_dict()
    mutate(data)
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(fio.FixtureFormatError) as excinfo:
        _read(fio, path)
    assert needle in str(excinfo.value), (
        f"error message {str(excinfo.value)!r} should mention {needle!r}"
    )
