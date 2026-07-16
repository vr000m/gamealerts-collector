"""Phase 2: client library — core read ops, pack-contributed ops, WAL reads.

Plan contract (docs/dev_plans/20260702-feature-collector-interfaces.md §Phase 2
and Requirements):

- ``gamecollect.client`` exposes the four CORE read functions ``list_matches``,
  ``get_state``, ``get_events_since``, ``get_standings`` over ``db/reader.py``,
  with typed returns and read-only connections.
- ``get_standings(source, group_key)`` takes ``source`` EXPLICITLY (standings
  have no ``match_id`` to resolve it from). Match-scoped ops (``get_state``,
  ``get_events_since``, and the football pack's ``get_squad`` /
  ``get_player_stats``) resolve ``source`` internally via the ``matches`` row —
  the caller never passes ``source``.
- ``get_events_since(match_id, since_seq)`` returns events with seq strictly
  greater than ``since_seq`` in monotonic order; a ``since_seq`` beyond the
  partition head returns an empty list, not an error.
- The football pack contributes ``get_squad`` / ``get_player_stats`` (reading
  the pack-owned ``football_lineups`` / ``football_stats`` side tables). Core
  ``client.py`` never imports the pack; the ops are reached via the registry
  (or the pack module) once the pack is loaded in-test.
- Reads are safe while a daemon writes (WAL): a reader on a separate read-only
  connection sees exactly the committed batch, never uncommitted rows, and
  never errors or blocks.

Parallel-implementation note (mirrors ``tests/_football_helpers.py``): the
implementer is building ``src/gamecollect/client.py`` and
``src/gamecollect/registry.py`` in parallel, and the football pack's op
contribution, at the same time as these tests. The plan pins the four core
op NAMES and the ``get_standings(source, ...)`` vs match-scoped split, but NOT
the exact call surface (does an op take a path or an open connection? how is a
pack-contributed op reached through the registry? what extra args does
``get_player_stats`` accept?). Those are resolved TOLERANTLY below, in one
place, so a minor surface difference is a one-line fix here rather than a
rewrite of every test. Until the implementer lands the modules these tests may
fail at collection/call — a timing artifact, not a test bug.
"""

from __future__ import annotations

import importlib
import inspect
import sqlite3
from collections.abc import Callable, Mapping
from typing import Any

from _phase2_helpers import entity_row, event_row, match_row, standing_row, writer_method

from gamecollect.db.connection import connect
from gamecollect.db.reader import open_reader
from gamecollect.db.writer import PartitionWriter
from gamecollect.fold import fold

SRC = "wc2026-test"
SRC2 = "euro2028-test"
PACK_NAME = "football-wc2026"

# ---------------------------------------------------------------------------
# Tolerant client-op resolution
#
# Fix the candidate names / handle-kind heuristics HERE (one place) if the
# implementer's surface diverges, rather than in each test.
# ---------------------------------------------------------------------------

# Names a first positional parameter is likely to carry, keyed by what it wants.
_CONN_PARAM_NAMES = {"conn", "connection", "con", "cx", "cnx", "db_conn", "reader"}
_PATH_PARAM_NAMES = {
    "path",
    "db_path",
    "dbpath",
    "db",
    "database",
    "file",
    "filename",
    "db_file",
}


def _client_fn(name: str) -> Callable[..., Any]:
    """Return ``gamecollect.client.<name>`` (imported lazily), asserting it exists."""
    client = importlib.import_module("gamecollect.client")
    fn = getattr(client, name, None)
    assert callable(fn), f"gamecollect.client exposes no callable {name!r}"
    return fn


def _handle_kind(fn: Callable[..., Any]) -> str:
    """Guess whether ``fn``'s first positional arg is a path, a conn, or unknown."""
    try:
        params = [
            p
            for p in inspect.signature(fn).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
    except (TypeError, ValueError):
        return "unknown"
    if not params:
        return "unknown"
    first = params[0]
    lname = first.name.lower()
    if lname in _CONN_PARAM_NAMES:
        return "conn"
    if lname in _PATH_PARAM_NAMES:
        return "path"
    ann = str(first.annotation).lower()
    if "connection" in ann:
        return "conn"
    if "path" in ann or first.annotation is str:
        return "path"
    return "unknown"


def _invoke(fn: Callable[..., Any], path: Any, args: tuple = (), kwargs: dict | None = None) -> Any:
    """Call ``fn`` passing ``path`` as the DB handle in whichever form it wants.

    A path-taking client opens its own read-only connection; a conn-taking
    client is handed a fresh :func:`open_reader` connection (closed after). A
    fresh handle per call means each read sees the latest committed WAL
    snapshot — exactly the property the read-while-write test relies on.
    """
    kwargs = kwargs or {}
    kind = _handle_kind(fn)
    if kind == "path":
        return fn(str(path), *args, **kwargs)
    if kind == "conn":
        conn = open_reader(path)
        try:
            return fn(conn, *args, **kwargs)
        finally:
            conn.close()
    # Unknown: try a path first, then a connection.
    try:
        return fn(str(path), *args, **kwargs)
    except (TypeError, AttributeError, sqlite3.Error):
        conn = open_reader(path)
        try:
            return fn(conn, *args, **kwargs)
        finally:
            conn.close()


def _field(row: Any, *names: str) -> Any:
    """Read a field from a typed (dataclass) or dict-shaped result row.

    Accepts several candidate names and returns the first that resolves, so a
    dataclass whose attribute is ``match_id`` and a dict keyed ``match_id`` both
    work without pinning the return type.
    """
    for name in names:
        if isinstance(row, Mapping):
            if name in row:
                return row[name]
        elif hasattr(row, name):
            return getattr(row, name)
    return None


def _nonempty(result: Any) -> bool:
    if result is None:
        return False
    try:
        return len(result) > 0
    except TypeError:
        return bool(result)


# ---------------------------------------------------------------------------
# Tolerant pack-contributed-op resolution
# ---------------------------------------------------------------------------


def _load_football_pack():
    from gamecollect.packs.registry import load_pack

    return load_pack(PACK_NAME)


def _impl_of(op: Any) -> Callable[..., Any] | None:
    """Extract the callable implementation from a registry-entry object.

    A registry entry is plausibly a dataclass ``(name, params, output_schema,
    impl)``; the impl may hang off any of these attrs, or the entry may itself
    be callable.
    """
    for attr in ("impl", "fn", "func", "callable", "handler", "call", "op", "run"):
        candidate = getattr(op, attr, None)
        if callable(candidate):
            return candidate
    if callable(op):
        return op
    return None


def _registry_containers() -> list[Any]:
    try:
        reg = importlib.import_module("gamecollect.registry")
    except ImportError:
        return []
    containers = [
        getattr(reg, attr, None)
        for attr in ("REGISTRY", "registry", "OPERATIONS", "operations", "default_registry", "OPS")
    ]
    containers.append(reg)
    return [c for c in containers if c is not None]


def _lookup_in_container(container: Any, name: str) -> Callable[..., Any] | None:
    # Accessor-style: registry.get(name) / registry.resolve(name).
    for accessor in ("get", "resolve", "operation", "lookup"):
        fn = getattr(container, accessor, None)
        if callable(fn):
            try:
                op = fn(name)
            except (KeyError, LookupError, TypeError):
                op = None
            if op is not None:
                impl = _impl_of(op)
                if impl is not None:
                    return impl
    # Mapping-style: registry[name] or registry.operations[name].
    for holder in (
        container,
        getattr(container, "operations", None),
        getattr(container, "ops", None),
    ):
        if holder is None:
            continue
        try:
            op = holder[name]  # type: ignore[index]
        except (KeyError, TypeError):
            op = None
        if op is not None:
            impl = _impl_of(op)
            if impl is not None:
                return impl
    return None


def _resolve_pack_op(name: str) -> Callable[..., Any]:
    """Resolve a pack-contributed op impl after loading the football pack.

    Strategy (fix candidates here if the surface diverges):
      1. Load the pack (its load is the documented registration trigger).
      2. Look the op up in the operation registry.
      3. Fall back to importing it from a ``gamecollect_football`` submodule.
    Core ``gamecollect.client`` is intentionally NOT consulted — the point of
    a pack-contributed op is that core never owns it.
    """
    _load_football_pack()
    for container in _registry_containers():
        impl = _lookup_in_container(container, name)
        if impl is not None:
            return impl
    for modname in (
        "gamecollect_football.client",
        "gamecollect_football.ops",
        "gamecollect_football.operations",
        "gamecollect_football.reader",
        "gamecollect_football.registry",
        "gamecollect_football",
    ):
        try:
            mod = importlib.import_module(modname)
        except ImportError:
            continue
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn
    raise AssertionError(
        f"could not resolve pack-contributed op {name!r} via the registry or a "
        f"gamecollect_football submodule after loading pack {PACK_NAME!r}; update "
        f"the resolver candidates in tests/test_client.py"
    )


def _invoke_op_variants(
    impl: Callable[..., Any], path: Any, match_id: str, variants: list[tuple]
) -> Any:
    """Call a match-scoped pack op, trying trailing-arg variants until one fits.

    ``get_player_stats`` may be scoped by team/player in addition to ``match_id``
    (unpinned); each variant is the extra positional args to try after
    ``match_id``.
    """
    last_err: Exception | None = None
    for variant in variants:
        try:
            return _invoke(impl, path, args=(match_id, *variant))
        except TypeError as err:
            last_err = err
            continue
    raise AssertionError(f"no positional-arg variant fit the pack op: {last_err}")


# ---------------------------------------------------------------------------
# Seeding helpers (PartitionWriter — the plan-declared seeding path)
# ---------------------------------------------------------------------------


def _writer(db_path: Any, source: str = SRC, side_table_ddl: tuple = ()) -> tuple:
    conn = connect(db_path, side_table_ddl=side_table_ddl)
    return conn, PartitionWriter(conn, source)


def _seed_core(db_path: Any) -> dict[str, Any]:
    """Seed two sources' worth of matches/events/standings/entities.

    Returns the ids/facts the tests assert against.
    """
    m1 = f"{SRC}:760440"
    m2 = f"{SRC2}:900001"

    conn, w = _writer(db_path, SRC)
    writer_method(w, "entity")(entity_row("t-can", "Canada", kind="team"))
    writer_method(w, "entity")(entity_row("t-qat", "Qatar", kind="team"))
    writer_method(w, "match")(
        match_row(m1, status="IN_PLAY", score_home=6, score_away=0, home_entity="t-can")
    )
    # Contiguous seqs 0..4 (writer enforces monotonic-per-(source, match)).
    w.append_events(m1, [event_row(m1, seq, type="goal", minute=seq * 10) for seq in range(5)])
    writer_method(w, "standing")(standing_row("A", "t-can", points=9, rank=1))
    writer_method(w, "standing")(standing_row("A", "t-qat", points=0, rank=4))
    conn.commit()
    conn.close()

    # A DIFFERENT source, same group_key "A", overlapping seqs — proves explicit
    # source scoping on standings and internal source resolution on match ops.
    conn2, w2 = _writer(db_path, SRC2)
    writer_method(w2, "match")(match_row(m2, status="FINISHED", score_home=1, score_away=1))
    w2.append_events(m2, [event_row(m2, seq, type="goal") for seq in range(3)])
    writer_method(w2, "standing")(standing_row("A", "t-other", points=3, rank=2))
    conn2.commit()
    conn2.close()

    return {"m1": m1, "m2": m2}


# ---------------------------------------------------------------------------
# Core read ops
# ---------------------------------------------------------------------------


def test_list_matches_returns_all_seeded_matches(tmp_path):
    db = tmp_path / "client.db"
    ids = _seed_core(db)
    rows = _invoke(_client_fn("list_matches"), db)
    got = {_field(r, "match_id") for r in rows}
    assert {ids["m1"], ids["m2"]} <= got


def test_list_matches_source_filter_scopes_to_one_source(tmp_path):
    db = tmp_path / "client.db"
    ids = _seed_core(db)
    rows = _invoke(_client_fn("list_matches"), db, kwargs={"source": SRC})
    got = {_field(r, "match_id") for r in rows}
    assert got == {ids["m1"]}, "source filter must return only that source's matches"


def test_get_state_returns_typed_current_row(tmp_path):
    db = tmp_path / "client.db"
    ids = _seed_core(db)
    state = _invoke(_client_fn("get_state"), db, args=(ids["m1"],))
    assert state is not None
    assert _field(state, "match_id") == ids["m1"]
    assert _field(state, "status") == "IN_PLAY"
    assert _field(state, "score_home") == 6
    assert _field(state, "score_away") == 0


def test_get_state_unknown_match_returns_none(tmp_path):
    db = tmp_path / "client.db"
    _seed_core(db)
    assert _invoke(_client_fn("get_state"), db, args=("no-such-match",)) is None


def test_match_scoped_ops_resolve_source_internally(tmp_path):
    """No ``source`` is passed; the client resolves it via the matches row, so
    each globally-unique match_id yields ONLY its own partition's rows."""
    db = tmp_path / "client.db"
    ids = _seed_core(db)

    state = _invoke(_client_fn("get_state"), db, args=(ids["m2"],))
    assert _field(state, "source") == SRC2

    events = _invoke(_client_fn("get_events_since"), db, args=(ids["m1"], -1))
    sources = {_field(e, "source") for e in events}
    assert sources == {SRC}, "match-scoped events must not leak across sources"


def test_get_events_since_returns_all_in_monotonic_order(tmp_path):
    db = tmp_path / "client.db"
    ids = _seed_core(db)
    events = _invoke(_client_fn("get_events_since"), db, args=(ids["m1"], -1))
    seqs = [_field(e, "seq") for e in events]
    assert seqs == [0, 1, 2, 3, 4]


def test_get_events_since_paginates_strictly_after_cursor(tmp_path):
    db = tmp_path / "client.db"
    ids = _seed_core(db)

    # since=2 -> strictly greater than 2.
    page = _invoke(_client_fn("get_events_since"), db, args=(ids["m1"], 2))
    seqs = [_field(e, "seq") for e in page]
    assert seqs == [3, 4]
    assert all(s > 2 for s in seqs)
    assert seqs == sorted(seqs), "events must be monotonically ordered by seq"

    # Walk the log a page at a time using the last-seen seq as the next cursor.
    seen: list[int] = []
    cursor = -1
    for _ in range(10):
        chunk = _invoke(_client_fn("get_events_since"), db, args=(ids["m1"], cursor))
        chunk_seqs = [_field(e, "seq") for e in chunk]
        if not chunk_seqs:
            break
        assert all(s > cursor for s in chunk_seqs)
        seen.extend(chunk_seqs)
        cursor = chunk_seqs[-1]
    assert seen == [0, 1, 2, 3, 4]


def test_get_events_since_beyond_head_is_empty_not_error(tmp_path):
    db = tmp_path / "client.db"
    ids = _seed_core(db)
    result = _invoke(_client_fn("get_events_since"), db, args=(ids["m1"], 999))
    assert list(result) == [], "since_seq beyond the head must yield an empty list"


def test_get_events_since_normalizes_legacy_goal_payload_player_key(tmp_path):
    """A goal-family row written under the pre-rename convention (payload key
    "player", not "scorer" — see docs/dev_plans/20260710-feature-goal-event-
    participants.md) must be exposed via the PUBLIC read path (get_events_since
    / events --json) with the documented "scorer" key, not left in the old
    shape forever. engine._stored_events already normalizes this internally
    for engine reconciliation, but that fallback never reaches this path —
    Codex's adversarial review flagged that gap as a no-ship-severity issue:
    an upgraded consumer database's pre-change goal rows stayed externally
    indistinguishable from genuinely keyless rows.
    """
    db = tmp_path / "client.db"
    m1 = f"{SRC}:760440"
    conn, w = _writer(db, SRC)
    writer_method(w, "match")(match_row(m1, status="FINISHED"))
    w.append_events(
        m1,
        [
            event_row(
                m1,
                0,
                type="goal",
                payload={"team": "Canada", "player": "Jonathan David"},
            )
        ],
    )
    conn.commit()
    conn.close()

    events = _invoke(_client_fn("get_events_since"), db, args=(m1, -1))
    assert len(events) == 1
    payload = _field(events[0], "payload")
    assert payload == {"team": "Canada", "scorer": "Jonathan David"}, (
        f"legacy payload.player must be exposed as payload.scorer via the public "
        f"read path, got {payload!r}"
    )


def test_get_events_since_leaves_current_goal_payload_untouched(tmp_path):
    """A row already written under the current convention (payload key
    "scorer") must pass through unchanged — the normalization is additive,
    not a blanket rewrite."""
    db = tmp_path / "client.db"
    m1 = f"{SRC}:760440"
    conn, w = _writer(db, SRC)
    writer_method(w, "match")(match_row(m1, status="FINISHED"))
    w.append_events(
        m1,
        [
            event_row(
                m1,
                0,
                type="own_goal",
                payload={"team": "Qatar", "scorer": "Jonathan David", "assist": "Alphonso Davies"},
            )
        ],
    )
    conn.commit()
    conn.close()

    events = _invoke(_client_fn("get_events_since"), db, args=(m1, -1))
    payload = _field(events[0], "payload")
    assert payload == {"team": "Qatar", "scorer": "Jonathan David", "assist": "Alphonso Davies"}


def test_get_events_since_leaves_non_goal_player_payload_untouched(tmp_path):
    """A non-goal-family event's payload.player key must never be rewritten —
    the goal-family gate is the same one engine._GOAL_FAMILY_EVENT_TYPES uses,
    and it must not fire for a "sub"/"yellow"/"red"/"penalty" row even if that
    row happens to omit a "scorer" key (it always does; that's not a legacy
    marker for non-goal types)."""
    db = tmp_path / "client.db"
    m1 = f"{SRC}:760440"
    conn, w = _writer(db, SRC)
    writer_method(w, "match")(match_row(m1, status="FINISHED"))
    w.append_events(
        m1,
        [event_row(m1, 0, type="yellow", payload={"team": "Canada", "player": "Booked Player"})],
    )
    conn.commit()
    conn.close()

    events = _invoke(_client_fn("get_events_since"), db, args=(m1, -1))
    payload = _field(events[0], "payload")
    assert payload == {"team": "Canada", "player": "Booked Player"}


# ---------------------------------------------------------------------------
# get_standings — explicit source
# ---------------------------------------------------------------------------


def test_get_standings_requires_and_scopes_to_explicit_source(tmp_path):
    db = tmp_path / "client.db"
    _seed_core(db)
    # Same group_key "A" exists under both sources; the explicit source arg
    # must scope the result to exactly one partition.
    rows = _invoke(_client_fn("get_standings"), db, args=(SRC, "A"))
    entity_ids = {_field(r, "entity_id") for r in rows}
    assert entity_ids == {"t-can", "t-qat"}
    assert all(_field(r, "source") == SRC for r in rows)

    other = _invoke(_client_fn("get_standings"), db, args=(SRC2, "A"))
    assert {_field(r, "entity_id") for r in other} == {"t-other"}


def test_get_standings_group_key_optional(tmp_path):
    db = tmp_path / "client.db"
    _seed_core(db)
    # Without a group_key, all of the source's standings come back.
    rows = _invoke(_client_fn("get_standings"), db, args=(SRC,))
    assert {_field(r, "entity_id") for r in rows} == {"t-can", "t-qat"}


# ---------------------------------------------------------------------------
# Read-while-write under WAL (explicit commit-boundary interleaving)
# ---------------------------------------------------------------------------


def test_reader_sees_only_committed_batches_under_wal(tmp_path):
    """Writer commits batch 1; a reader on a separate read-only connection sees
    exactly batch 1 while the writer holds an OPEN transaction with uncommitted
    batch 2; after the writer commits, the reader sees batch 2. The reader
    never errors or blocks (WAL: readers do not wait on a writer)."""
    db = tmp_path / "wal.db"
    match_id = f"{SRC}:wal"

    wconn = connect(db)
    writer = PartitionWriter(wconn, SRC)
    writer.upsert_match({"match_id": match_id, "status": "IN_PLAY"})
    # Batch 1: append_events commits internally.
    writer.append_events(match_id, [event_row(match_id, seq, type="goal") for seq in (0, 1)])

    read_events = _client_fn("get_events_since")

    def _reader_seqs() -> list[int]:
        rows = _invoke(read_events, db, args=(match_id, -1))
        return [_field(e, "seq") for e in rows]

    assert _reader_seqs() == [0, 1]

    # Batch 2: raw INSERTs on the writer connection, deliberately NOT committed,
    # so we control the transaction boundary the writer methods would auto-close.
    wconn.execute("BEGIN")
    wconn.executemany(
        "INSERT INTO events (source, match_id, seq, type) VALUES (?, ?, ?, ?)",
        [(SRC, match_id, 2, "goal"), (SRC, match_id, 3, "goal")],
    )
    # Reader on its own connection must NOT see uncommitted batch 2, and must
    # return promptly rather than block on the open writer transaction.
    assert _reader_seqs() == [0, 1], "reader saw uncommitted rows"

    wconn.commit()
    assert _reader_seqs() == [0, 1, 2, 3], "reader did not see batch 2 after commit"

    wconn.close()


# ---------------------------------------------------------------------------
# Pack-contributed ops (football pack loaded in-test)
# ---------------------------------------------------------------------------


def _seed_football(db_path: Any) -> dict[str, Any]:
    """Seed core match rows plus the football side tables the pack ops read."""
    pack = _load_football_pack()
    match_id = f"{SRC}:760440"
    team = "Canada"
    star = "Jonathan David"

    conn, w = _writer(db_path, SRC, side_table_ddl=pack.side_table_ddl)
    writer_method(w, "match")(match_row(match_id, status="FINISHED", score_home=6, score_away=0))
    # Side tables are pack-owned: no PartitionWriter method for them, so seed
    # them directly (this is exactly what the football pack's own writer does).
    conn.executemany(
        "INSERT INTO football_lineups "
        "(source, match_id, team, athlete_id, display_name, name_folded, starter, position) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (SRC, match_id, team, "a1", star, fold(star), 1, "F"),
            (SRC, match_id, team, "a2", "Alphonso Davies", fold("Alphonso Davies"), 1, "D"),
            (SRC, match_id, "Qatar", "a3", "Akram Afif", fold("Akram Afif"), 1, "F"),
        ],
    )
    conn.execute(
        "INSERT INTO football_stats "
        "(source, match_id, team, possession, shots, shots_on_target, corners) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (SRC, match_id, team, 61.5, 15, 9, 7),
    )
    conn.commit()
    conn.close()
    return {"match_id": match_id, "team": team, "star": star}


def test_get_squad_reads_pack_lineups_for_match(tmp_path):
    db = tmp_path / "football.db"
    facts = _seed_football(db)
    impl = _resolve_pack_op("get_squad")
    # Match-scoped: resolves source internally, no source arg.
    result = _invoke_op_variants(impl, db, facts["match_id"], [(), (facts["team"],)])
    assert _nonempty(result), "get_squad returned nothing for a seeded match"
    assert facts["star"] in repr(result), "seeded player missing from squad result"


def test_get_player_stats_reads_pack_side_tables_for_match(tmp_path):
    db = tmp_path / "football.db"
    facts = _seed_football(db)
    impl = _resolve_pack_op("get_player_stats")
    result = _invoke_op_variants(
        impl,
        db,
        facts["match_id"],
        [(), (facts["team"],), (facts["star"],), (facts["team"], facts["star"])],
    )
    assert _nonempty(result), "get_player_stats returned nothing for a seeded match"
    # Whichever pack side table it reads, the seeded team appears in the result.
    assert facts["team"] in repr(result)


def test_pack_ops_registered_after_load_but_core_client_lacks_them(tmp_path):
    """The pack contributes ops into the registry at load; core client.py must
    never own them (dependency direction is pack -> core only)."""
    _load_football_pack()
    client = importlib.import_module("gamecollect.client")
    assert not hasattr(client, "get_squad"), "core client must not own get_squad"
    assert not hasattr(client, "get_player_stats"), "core client must not own get_player_stats"
    # And they ARE reachable now that the pack is loaded.
    assert callable(_resolve_pack_op("get_squad"))
    assert callable(_resolve_pack_op("get_player_stats"))


def test_core_registry_registers_the_core_ops_wired_to_client():
    """The operation registry is the single source of truth CLI/manifest derive
    from; the core ops must be registered, each backed by its client fn."""
    from gamecollect import client
    from gamecollect.registry import build_registry

    registry = build_registry()
    assert set(registry.names) == {"matches", "state", "events", "standings", "vocabulary"}
    impls = {op.name: op.impl for op in registry.operations}
    assert impls["matches"] is client.list_matches
    assert impls["state"] is client.get_state
    assert impls["events"] is client.get_events_since
    assert impls["standings"] is client.get_standings
    assert impls["vocabulary"] is client.get_vocabulary


def test_registry_merges_pack_contributed_ops_without_core_owning_them():
    """A pack contributes ops via SportPack.operations; build_registry merges
    them onto core. Core-only registry must NOT carry them (pack -> core)."""
    from gamecollect.registry import build_registry
    from gamecollect_football.operations import get_player_stats, get_squad

    pack = _load_football_pack()
    merged = set(build_registry([pack]).names)
    assert {"matches", "state", "events", "standings"} <= merged
    assert {"squad", "player-stats"} <= merged, "pack ops were not merged into the registry"

    impls = {op.name: op.impl for op in build_registry([pack]).operations}
    assert impls["squad"] is get_squad
    assert impls["player-stats"] is get_player_stats

    assert {"squad", "player-stats"}.isdisjoint(build_registry().names)
