"""Ported from gamealerts ``tests/test_card_counts.py`` (mandatory per plan).

gamealerts' original pins ``DAO.card_counts`` (per-team yellow/red derived from
card EVENTS, not stats) and that the second-yellow split's two rows both
persist through ``INSERT OR IGNORE``. This repo has no DAO/card_counts —
prose/projections belong to consuming apps — so the port pins the same
substance at this repo's seams:

  * events-as-source: the REPLAYED normalized events give the carded side
    (Canada) yellow=2 red=1 — the second yellow surfaces as a second YELLOW
    event (the 2-event split), which a stats block would not show;
  * the split YELLOW and RED persist as two distinct rows through the REAL
    write path (``PartitionWriter`` + the football pack's taxonomy +
    ``INSERT OR IGNORE`` on ``(source, match_id, seq)``) — they are NOT
    collapsed, and a full re-poll re-send is ignored, not duplicated.

Team/player attribution is carried in the event ``payload`` JSON by this
test's own row mapping (core ``events`` has no team column; entity refs are
soft in v1), so the counting SQL is self-consistent with the write.

All tests run fully offline (committed fixture only; no network).
"""

from __future__ import annotations

import collections
import json

import pytest
from _football_helpers import replay

from gamecollect.db.connection import connect
from gamecollect.db.writer import PartitionWriter

SOURCE = "wc2026-test"


def _event_rows(match) -> list[dict]:
    """Map ported NormalizedEvents onto core events-table rows.

    ``team``/``player`` ride in the payload JSON (test-owned mapping; the
    core schema keeps only sport-agnostic columns).
    """
    return [
        {
            "match_id": match.match_id,
            "seq": ev.seq,
            "type": ev.event_type,
            "minute": ev.minute,
            "importance": ev.importance,
            "detail": ev.detail,
            "payload": {"team": ev.team, "player": ev.player, "assist": ev.assist},
        }
        for ev in match.events
    ]


@pytest.fixture
def persisted(tmp_path, football_pack, replayed_match):
    """Replay the fixture and persist all events through the real write path."""
    conn = connect(tmp_path / "cards.db", side_table_ddl=football_pack.side_table_ddl)
    writer = PartitionWriter(conn, SOURCE, taxonomy=football_pack.event_types)
    writer.upsert_match(
        {"match_id": replayed_match.match_id, "status": replayed_match.status.value}
    )
    inserted = writer.append_events(replayed_match.match_id, _event_rows(replayed_match))
    yield conn, writer, replayed_match, inserted
    conn.close()


def _card_counts(conn, match_id: str) -> dict[str, dict[str, int]]:
    """Per-team yellow/red counts from stored card EVENTS (the ported query)."""
    rows = conn.execute(
        """
        SELECT json_extract(payload, '$.team') AS team, type
        FROM events
        WHERE match_id = ? AND type IN ('yellow', 'red')
        """,
        (match_id,),
    ).fetchall()
    counts: dict[str, dict[str, int]] = {}
    for team, type_ in rows:
        if team is None:
            continue  # unattributed cards are skipped, not grouped under None
        counts.setdefault(team, {"yellow": 0, "red": 0})[type_] += 1
    return counts


# ===========================================================================
# Events-as-source: the normalized events alone give the split-aware counts.
# ===========================================================================


class TestNormalizedCardCounts:
    def test_carded_side_shows_two_yellow_one_red(self):
        """Canada: real yellow (9') + split YELLOW (78') = 2; split RED (78') = 1."""
        match = replay()
        counts = collections.Counter(
            (ev.team, ev.event_type) for ev in match.events if ev.event_type in ("yellow", "red")
        )
        assert counts[("Canada", "yellow")] == 2
        assert counts[("Canada", "red")] == 1

    def test_other_side_counts_only_real_cards(self):
        """Qatar: 1 real yellow (62'), 2 real reds (33', 53') — the split adds
        nothing to the uncarded side."""
        match = replay()
        counts = collections.Counter(
            (ev.team, ev.event_type) for ev in match.events if ev.event_type in ("yellow", "red")
        )
        assert counts[("Qatar", "yellow")] == 1
        assert counts[("Qatar", "red")] == 2


# ===========================================================================
# Second-yellow via the REAL write path (PartitionWriter + pack taxonomy).
# ===========================================================================


class TestSecondYellowEndToEnd:
    def test_all_events_persist_through_the_taxonomy_gated_writer(self, persisted):
        conn, _writer, match, inserted = persisted
        assert inserted == 24
        (count,) = conn.execute(
            "SELECT COUNT(*) FROM events WHERE match_id = ?", (match.match_id,)
        ).fetchone()
        assert count == 24

    def test_stored_card_counts_match_the_oracle(self, persisted):
        conn, _writer, match, _ = persisted
        assert _card_counts(conn, match.match_id) == {
            "Canada": {"yellow": 2, "red": 1},
            "Qatar": {"yellow": 1, "red": 2},
        }

    def test_both_expanded_rows_persist_despite_insert_or_ignore(self, persisted):
        """The split YELLOW and RED carry distinct consecutive seq values, so
        BOTH persist — not collapsed by the ``(source, match_id, seq)`` PK +
        ``INSERT OR IGNORE``. Assert directly against the stored rows for the
        carded side at 78'."""
        conn, _writer, match, _ = persisted
        rows = conn.execute(
            """
            SELECT seq, type FROM events
            WHERE match_id = ? AND minute = 78
              AND json_extract(payload, '$.team') = 'Canada'
              AND type IN ('yellow', 'red')
            ORDER BY seq
            """,
            (match.match_id,),
        ).fetchall()
        assert len(rows) == 2, f"expected both split rows to persist; got {rows}"
        seqs = [r[0] for r in rows]
        assert len(set(seqs)) == 2, f"split rows collapsed onto one seq: {rows}"
        assert seqs[1] == seqs[0] + 1, f"split rows not consecutive: {rows}"
        types = collections.Counter(r[1] for r in rows)
        assert types == {"yellow": 1, "red": 1}, f"split row types drifted: {dict(types)}"

    def test_full_repoll_is_idempotent(self, persisted):
        """gamealerts' re-poll semantics survive: re-sending the full event
        list inserts nothing and changes no stored row."""
        conn, writer, match, _ = persisted
        before = conn.execute(
            "SELECT seq, type, detail FROM events WHERE match_id = ? ORDER BY seq",
            (match.match_id,),
        ).fetchall()
        inserted_again = writer.append_events(match.match_id, _event_rows(match))
        assert inserted_again == 0
        after = conn.execute(
            "SELECT seq, type, detail FROM events WHERE match_id = ? ORDER BY seq",
            (match.match_id,),
        ).fetchall()
        assert after == before

    def test_every_stored_row_is_source_stamped(self, persisted):
        conn, _writer, match, _ = persisted
        sources = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT source FROM events WHERE match_id = ?", (match.match_id,)
            )
        }
        assert sources == {SOURCE}

    def test_split_events_pass_the_football_taxonomy(self, football_pack, replayed_match):
        """Every replayed event type — including the split rows — is declared
        by the pack taxonomy, so the taxonomy-gated write above cannot have
        succeeded by accident."""
        produced = {ev.event_type for ev in replayed_match.events}
        assert produced <= football_pack.event_types, (
            f"adapter produced undeclared types: {produced - football_pack.event_types}"
        )

    def test_json_payload_team_attribution_round_trips(self, persisted):
        """The payload mapping used for attribution actually round-trips as
        JSON (guards against double-encoding in the writer)."""
        conn, _writer, match, _ = persisted
        (payload,) = conn.execute(
            "SELECT payload FROM events WHERE match_id = ? AND seq = 1", (match.match_id,)
        ).fetchone()
        decoded = json.loads(payload)
        assert decoded["team"] == "Canada"
        assert decoded["player"] == "Derek Cornelius"
