"""Phase 2: PartitionWriter partition discipline, seq semantics, WAL contention.

Plan contract (docs/dev_plans/20260702-feature-collector-foundation.md §Phase 2):
- ``PartitionWriter(conn, source, taxonomy=None)`` stamps every written row
  with the constructor ``source`` and raises on any cross-source write.
- ``seq`` is provider-derived; the writer enforces monotonic-per-(source,
  match) and is idempotent via ``INSERT OR IGNORE`` on
  ``(source, match_id, seq)`` (re-poll semantics).
- Two writer PROCESSES, different sources, one file: writes serialize under
  WAL; busy_timeout=5000 absorbs SQLITE_BUSY (defined retry/backoff — no
  errors leak, no writes lost).
- Two writer processes, same (file, source): documented-unsupported; the
  outcome must be bounded (no corruption, no crash).
"""

import sqlite3
import subprocess
import sys

import pytest
from _phase2_helpers import (
    all_rows,
    entity_row,
    event_row,
    integrity_ok,
    match_row,
    standing_row,
    writer_method,
)

from gamecollect.db.connection import connect
from gamecollect.db.writer import CrossPartitionError, PartitionWriter

SRC = "wc2026-espn"
OTHER = "euro2028-espn"


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "writer.db"


def _writer(db_path, source=SRC):
    conn = connect(db_path)
    return conn, PartitionWriter(conn, source)


# --- source stamping --------------------------------------------------------------


def test_every_written_row_is_stamped_with_constructor_source(db_path):
    conn, w = _writer(db_path)
    writer_method(w, "match")(match_row("m1"))
    writer_method(w, "event")(event_row("m1", 1))
    writer_method(w, "entity")(entity_row("t1", "Qatar"))
    writer_method(w, "standing")(standing_row("A", "t1"))
    writer_method(w, "provider_map")("espn", "760440", "m1")
    conn.commit()
    conn.close()

    with sqlite3.connect(db_path) as c:
        for table in ("matches", "events", "entities", "standings", "provider_match_map"):
            sources = [r[0] for r in c.execute(f"SELECT source FROM {table}")]
            assert sources, f"no row written to {table}"
            assert set(sources) == {SRC}, f"{table} rows not stamped with writer source"


def test_rows_without_explicit_source_are_stamped(db_path):
    conn, w = _writer(db_path)
    row = match_row("m1")
    assert "source" not in row or row["source"] == SRC
    writer_method(w, "match")(row)
    conn.commit()
    conn.close()
    with sqlite3.connect(db_path) as c:
        assert c.execute("SELECT source FROM matches").fetchone()[0] == SRC


def test_cross_source_write_raises(db_path):
    conn, w = _writer(db_path)
    with pytest.raises(Exception, match="(?i)source"):
        writer_method(w, "match")(match_row("m1", source=OTHER))
    with pytest.raises(Exception, match="(?i)source"):
        writer_method(w, "event")(event_row("m1", 1, source=OTHER))
    conn.commit()
    conn.close()
    with sqlite3.connect(db_path) as c:
        assert c.execute("SELECT COUNT(*) FROM matches WHERE source=?", (OTHER,)).fetchone()[0] == 0
        assert c.execute("SELECT COUNT(*) FROM events WHERE source=?", (OTHER,)).fetchone()[0] == 0


def test_same_provider_match_id_does_not_collide_across_sources(db_path):
    conn_a, wa = _writer(db_path, SRC)
    writer_method(wa, "provider_map")("espn", "760440", "m-a")
    conn_a.commit()
    conn_a.close()
    conn_b, wb = _writer(db_path, OTHER)
    writer_method(wb, "provider_map")("espn", "760440", "m-b")
    conn_b.commit()
    conn_b.close()
    with sqlite3.connect(db_path) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM provider_match_map WHERE provider='espn' "
            "AND provider_match_id='760440'"
        ).fetchone()[0]
    assert n == 2, "two sources mapping the same provider-native id must both persist"


# --- seq semantics ------------------------------------------------------------------


def test_event_idempotent_replay_first_write_wins(db_path):
    conn, w = _writer(db_path)
    writer_method(w, "match")(match_row("m1"))
    writer_method(w, "event")(event_row("m1", 1, type="goal"))
    # Re-poll delivers the same (source, match_id, seq) again: INSERT OR
    # IGNORE — no error, no duplicate, first write retained.
    writer_method(w, "event")(event_row("m1", 1, type="yellow_card"))
    conn.commit()
    conn.close()
    rows = all_rows(db_path, "events")
    assert len(rows) == 1
    with sqlite3.connect(db_path) as c:
        assert c.execute("SELECT type FROM events WHERE seq=1").fetchone()[0] == "goal"


def test_repoll_full_event_list_appends_only_new(db_path):
    conn, w = _writer(db_path)
    writer_method(w, "match")(match_row("m1"))
    for seq in (0, 1, 2):
        writer_method(w, "event")(event_row("m1", seq))
    # ESPN re-poll re-sends the full keyEvents list plus one new event.
    for seq in (0, 1, 2, 3):
        writer_method(w, "event")(event_row("m1", seq))
    conn.commit()
    conn.close()
    with sqlite3.connect(db_path) as c:
        seqs = [r[0] for r in c.execute("SELECT seq FROM events WHERE match_id='m1' ORDER BY seq")]
    assert seqs == [0, 1, 2, 3]


def test_unseen_seq_below_partition_head_violates_monotonic_invariant(db_path):
    conn, w = _writer(db_path)
    writer_method(w, "match")(match_row("m1"))
    writer_method(w, "event")(event_row("m1", 1))
    writer_method(w, "event")(event_row("m1", 2))
    writer_method(w, "event")(event_row("m1", 5))
    with pytest.raises(Exception, match="(?i)(seq|monoton)"):
        # seq 3 was never seen and is below the partition head (5).
        writer_method(w, "event")(event_row("m1", 3))
    conn.close()


def test_seq_is_scoped_per_match_and_source(db_path):
    conn, w = _writer(db_path)
    writer_method(w, "match")(match_row("m1"))
    writer_method(w, "match")(match_row("m2"))
    writer_method(w, "event")(event_row("m1", 7))
    # A lower seq on a DIFFERENT match must be fine: monotonicity is
    # per-(source, match), not global.
    writer_method(w, "event")(event_row("m2", 1))
    conn.commit()
    conn.close()
    with sqlite3.connect(db_path) as c:
        assert c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2


# --- taxonomy=None skips validation ---------------------------------------------------


def test_taxonomy_none_accepts_any_event_type(db_path):
    conn, w = _writer(db_path)
    writer_method(w, "match")(match_row("m1"))
    writer_method(w, "event")(event_row("m1", 1, type="totally-novel-type"))
    conn.commit()
    conn.close()
    with sqlite3.connect(db_path) as c:
        assert c.execute("SELECT type FROM events").fetchone()[0] == "totally-novel-type"


# --- multiprocess contention ------------------------------------------------------------

_CHILD = r"""
import sys, sqlite3
db, source, match_id, start, count, tolerate = sys.argv[1:7]
start, count = int(start), int(count)
tolerate = tolerate == "1"
from gamecollect.db.connection import connect
from gamecollect.db.writer import PartitionWriter

def resolve(obj, names):
    for n in names:
        f = getattr(obj, n, None)
        if callable(f):
            return f
    raise SystemExit("no writer method among %r" % (names,))

conn = connect(db)
w = PartitionWriter(conn, source)
write_match = resolve(w, ("upsert_match", "write_match", "put_match", "insert_match"))
write_event = resolve(w, ("append_event", "insert_event", "write_event", "add_event",
                          "upsert_event"))
write_match({"match_id": match_id, "status": "IN_PLAY"})
try:
    conn.commit()
except sqlite3.ProgrammingError:
    pass
for i in range(start, start + count):
    try:
        write_event({"match_id": match_id, "seq": i, "type": "goal"})
        conn.commit()
    except sqlite3.DatabaseError:
        raise  # busy_timeout must absorb SQLITE_BUSY; corruption is fatal
    except Exception:
        if not tolerate:
            raise
conn.close()
"""


def _spawn(db_path, source, match_id, start, count, tolerate=False):
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            _CHILD,
            str(db_path),
            source,
            match_id,
            str(start),
            str(count),
            "1" if tolerate else "0",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


N_EVENTS = 500


def test_two_processes_different_sources_serialize_without_data_loss(db_path):
    connect(db_path).close()  # create schema before racing
    p1 = _spawn(db_path, SRC, "m-a", 0, N_EVENTS)
    p2 = _spawn(db_path, OTHER, "m-b", 0, N_EVENTS)
    out = [p.communicate(timeout=300) for p in (p1, p2)]
    assert p1.returncode == 0, f"writer 1 crashed under contention: {out[0][1]}"
    assert p2.returncode == 0, f"writer 2 crashed under contention: {out[1][1]}"

    assert integrity_ok(db_path)
    with sqlite3.connect(db_path) as c:
        counts = dict(c.execute("SELECT source, COUNT(*) FROM events GROUP BY source"))
        # busy_timeout retry/backoff means no writes are lost or errored.
        assert counts == {SRC: N_EVENTS, OTHER: N_EVENTS}
        cross = c.execute(
            "SELECT COUNT(*) FROM events WHERE (match_id='m-a' AND source!=?) "
            "OR (match_id='m-b' AND source!=?)",
            (SRC, OTHER),
        ).fetchone()[0]
        assert cross == 0, "rows leaked across partitions"


def test_two_processes_same_source_is_bounded_no_corruption_no_crash(db_path):
    """Documented-unsupported misconfiguration: outcome bounded, not prevented."""
    connect(db_path).close()
    p1 = _spawn(db_path, SRC, "m-shared", 0, N_EVENTS, tolerate=True)
    p2 = _spawn(db_path, SRC, "m-shared", 0, N_EVENTS, tolerate=True)
    out = [p.communicate(timeout=300) for p in (p1, p2)]
    assert p1.returncode == 0, f"same-source writer 1 crashed: {out[0][1]}"
    assert p2.returncode == 0, f"same-source writer 2 crashed: {out[1][1]}"

    assert integrity_ok(db_path)
    with sqlite3.connect(db_path) as c:
        rows = c.execute(
            "SELECT seq FROM events WHERE source=? AND match_id='m-shared' ORDER BY seq",
            (SRC,),
        ).fetchall()
    seqs = [r[0] for r in rows]
    assert len(seqs) == len(set(seqs)), "duplicate (source, match_id, seq) rows"
    assert set(seqs) <= set(range(N_EVENTS)), "seq values outside the written range"
    assert len(seqs) <= N_EVENTS


# --- child-table match ownership ----------------------------------------------------
# Review fix: events and provider-map writes must verify match_id ownership
# against matches.source, or a writer scoped to source B could build a shadow
# event partition under source A's match and break the match_id-global reader
# contract (get_events_since takes no source parameter).


def test_append_events_under_foreign_match_raises(db_path):
    conn_a, wa = _writer(db_path, SRC)
    writer_method(wa, "match")(match_row("m1"))
    conn_a.commit()
    conn_a.close()

    conn_b, wb = _writer(db_path, OTHER)
    with pytest.raises(CrossPartitionError, match="belongs to source"):
        writer_method(wb, "event")(event_row("m1", 1))
    conn_b.close()
    with sqlite3.connect(db_path) as c:
        assert c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


def test_map_provider_match_to_foreign_match_raises(db_path):
    conn_a, wa = _writer(db_path, SRC)
    writer_method(wa, "match")(match_row("m1"))
    conn_a.commit()
    conn_a.close()

    conn_b, wb = _writer(db_path, OTHER)
    with pytest.raises(CrossPartitionError, match="belongs to source"):
        writer_method(wb, "provider_map")("espn", "760440", "m1")
    conn_b.close()
    with sqlite3.connect(db_path) as c:
        assert c.execute("SELECT COUNT(*) FROM provider_match_map").fetchone()[0] == 0


def test_append_events_before_match_seeded_is_allowed(db_path):
    # Soft-ref semantics: an event may legally land before its match row.
    conn, w = _writer(db_path)
    inserted = writer_method(w, "event")(event_row("m-unseeded", 1))
    conn.commit()
    conn.close()
    assert inserted == 1


# --- fold stamping is not caller-overridable -----------------------------------------


def test_upsert_entity_rejects_mismatched_name_folded(db_path):
    from gamecollect.fold import fold

    conn, w = _writer(db_path)
    with pytest.raises(ValueError, match="name_folded"):
        writer_method(w, "entity")(entity_row("t1", "Türkiye", name_folded="not-the-fold"))
    # Supplying the CORRECT fold is tolerated; the writer stamps it anyway.
    writer_method(w, "entity")(entity_row("t2", "Türkiye", name_folded=fold("Türkiye")))
    conn.commit()
    conn.close()
    with sqlite3.connect(db_path) as c:
        stored = c.execute("SELECT name_folded FROM entities WHERE entity_id='t2'").fetchone()[0]
    assert stored == fold("Türkiye")


# --- cross-table keys fail loudly ----------------------------------------------------


def test_cross_table_key_is_rejected_not_dropped(db_path):
    conn, w = _writer(db_path)
    with pytest.raises(ValueError, match="unknown column"):
        writer_method(w, "match")(match_row("m1", seq=3))
    with pytest.raises(ValueError, match="unknown column"):
        writer_method(w, "entity")(entity_row("t1", "Qatar", group_key="A"))
    conn.close()
