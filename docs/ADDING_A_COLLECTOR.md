# Adding a collector (sport pack)

This guide walks through building a new collector — a **sport pack** — from
scratch, using the built-in football/World Cup pack (`gamecollect_football`) as
the worked example. By the end you'll have a pip-installable package that the
collector discovers and runs without any change to the core.

If you only want the conceptual contract, read [`DESIGN.md`](DESIGN.md) §5
("Sport packs") and §6 ("Interfaces") first — this guide is the practical
how-to.

## What a pack is

A pack is an ordinary Python package that registers a **zero-arg factory** under
the `gamecollect.packs` entry-point group. The factory returns a
[`SportPack`](../src/gamecollect/packs/spec.py) describing everything the sport
contributes. The dependency direction is **pack → core**: your pack imports
`gamecollect`; core never imports your pack.

```
your package                          core (gamecollect)
─────────────                         ──────────────────
pack:pack()  ──returns──> SportPack ──validated by──> gamecollect.packs.registry
  provider_factory ──> MatchDataProvider (gamecollect.provider)
  taxonomy, side_table_ddl, operations, seed_match, persist_side_tables, …
```

Two packs (Champions League, F1, cricket, …) can coexist in one install; the
engine loads every registered pack.

## The two interfaces you implement

### 1. `MatchDataProvider` — where data comes from

Subclass [`gamecollect.provider.MatchDataProvider`](../src/gamecollect/provider.py)
and implement two methods:

| Method | Returns | Purpose |
| --- | --- | --- |
| `fetch_live_matches()` | `list[NormalizedMatch]` | The current slate — enough state to decide which matches are live and to seed/update core rows. Event lists **may be partial or empty** (ESPN scoreboard snapshots carry `events=[]`). |
| `fetch_match_detail(match_id)` | `NormalizedMatch` | The full current snapshot for one match: all known events plus any sport-specific `payload` extras. Must be a **pure read** of the same logical provider state (a replay provider must not advance replay time here). |

Both return [`NormalizedMatch`](../src/gamecollect/provider.py) dataclasses — the
sport-agnostic shape (identity, status, clock, score, `events`, and a free-form
`payload` dict for sport extras). Your adapter's job is to translate your
provider's wire format into `NormalizedMatch`.

The constructor **must be cheap and side-effect-free** — no network I/O at
construction. The registry instantiates a throwaway provider on every pack load
just to type-check it. Defer all real work to the fetch methods.

### 2. `SportPack` — everything the sport contributes

Your factory returns a [`SportPack`](../src/gamecollect/packs/spec.py). Required
fields:

| Field | What it is |
| --- | --- |
| `name` | Unique pack id (e.g. `"champions-league-2026"`). Matches your entry-point name. |
| `sport` | Sport slug (e.g. `"football"`, `"f1"`). |
| `provider_factory` | Zero-arg callable returning your `MatchDataProvider` (often the class itself). |
| `taxonomy` | `dict[str, EventTypeDecl]` — the legal `events.type` strings for this pack. Undeclared types are rejected loudly at write time. |
| `prompt_fragments` | Sport-context strings for downstream LLM prompting. |
| `preference_schema` | JSON-schema for user prefs (followed teams/drivers/…). |
| `display_metadata` | Display names, provider label, etc. |
| `compaction_boundaries` | Where the event log may be compacted. |

Optional fields (default to safe no-ops):

- `side_table_ddl` — additive, **pack-owned** DDL for your own typed side tables.
  Applied after core schema. Packs may own side tables but **never alter core
  tables**.
- `operations` — extra read ops over your side tables, merged into the CLI
  registry (football contributes `squad`/`player-stats`).
- `seed_match` — the per-match seed-before-child-write hook. Defaults to
  `default_seed_match`, which seeds under the provider-native `match_id` with no
  reconciliation. **Override this if you reconcile against a canonical schedule**
  (football does, to map ESPN ids onto fixed fixtures). It must ensure a
  `matches` row exists and return the canonical `match_id` (or `None` to skip
  child writes this poll).
- `persist_side_tables` — projects `NormalizedMatch.payload` into your side
  tables after seeding. Default is a no-op.

## Steps

1. **Create the package.** New directory `gamecollect_<sport>/` with a
   `pyproject.toml` depending on `gamecollect` (`requires-python >= 3.11`).

2. **Write the provider adapter** (`espn.py` is the football example). Subclass
   `MatchDataProvider`; translate your wire format into `NormalizedMatch`.

   **Payload vs side tables.** Sport-specific facts split two ways. Small
   scalar extras that belong with the match row travel in
   `NormalizedMatch.payload` (a JSON-serializable dict) — the football adapter
   carries half-time scores, penalty-shootout results, and `result_type`
   (`regulation`/`extra_time`/`penalties`) there; see how `_normalize_summary`
   in `gamecollect_football/espn.py` assembles the `payload` dict. Bulk,
   row-structured data (per-player stats, lineups) instead goes to **side
   tables** the pack owns, written by `persist_football_side_tables` in
   `gamecollect_football/pack.py` (see step 4). Rule of thumb: a handful of
   scalars per match → payload; repeating rows → a side table.

3. **Declare the taxonomy** (`taxonomy.py`) — one `EventTypeDecl(display_name,
   importance_default)` per event type. Importance uses the small-is-critical
   scale (1=critical … 4=low).

4. **(Optional) Side tables + operations** — DDL in the pack, `Operation`s whose
   `impl` lives in the pack, so core stays pack-agnostic.

5. **(Optional) Reconciliation** — if provider ids aren't canonical, override
   `seed_match`. See `gamecollect_football/reconcile.py` for the schedule-match
   pattern (including home/away orientation normalization).

6. **Write the `pack()` factory** (`pack.py`) returning the assembled
   `SportPack`.

7. **Register the entry point** in your `pyproject.toml`:

   ```toml
   [project.entry-points."gamecollect.packs"]
   champions-league-2026 = "gamecollect_champions.pack:pack"
   ```

   (The football pack registers `football-wc2026 = "gamecollect_football.pack:pack"`.)

8. **Install and verify.** `pip install -e .`, then the collector's pack registry
   discovers it. The registry's `validate_pack` will construct a throwaway
   provider and type-check the `SportPack` at load time — fix anything it rejects.

## Minimal skeleton (Champions League)

```python
# gamecollect_champions/provider.py
from gamecollect.provider import MatchDataProvider, NormalizedMatch

class UCLAdapter(MatchDataProvider):
    def __init__(self) -> None:  # cheap, no I/O
        ...

    def fetch_live_matches(self) -> list[NormalizedMatch]:
        ...  # translate provider slate -> NormalizedMatch (events may be [])

    def fetch_match_detail(self, match_id: str) -> NormalizedMatch:
        ...  # full snapshot for one match; pure read
```

```python
# gamecollect_champions/pack.py
from gamecollect.packs.spec import EventTypeDecl, SportPack
from .provider import UCLAdapter

TAXONOMY = {
    "goal": EventTypeDecl(display_name="Goal", importance_default=1),
    "card": EventTypeDecl(display_name="Card", importance_default=2),
    # …
}

def pack() -> SportPack:
    return SportPack(
        name="champions-league-2026",
        sport="football",
        provider_factory=UCLAdapter,
        taxonomy=TAXONOMY,
        prompt_fragments={"sport_context": "…", "event_call": "…"},
        preference_schema={"type": "object", "properties": {"followed_teams": {"type": "array", "items": {"type": "string"}}}},
        display_metadata={"sport_display": "Football", "tournament_display": "UEFA Champions League", "provider": "…"},
        compaction_boundaries=[…],
        # seed_match=…  # override only if you reconcile against a fixed schedule
    )
```

An F1 pack looks the same shape, with a motorsport `sport="f1"`, its own
taxonomy (`lap`, `pit-stop`, `overtake`, `retirement`, …), and a `preference_schema`
keyed on `followed_drivers`/`followed_teams` rather than football teams.

## Reference

- Contract source (read the docstrings — they're the spec):
  [`src/gamecollect/packs/spec.py`](../src/gamecollect/packs/spec.py)
- Provider ABC: [`src/gamecollect/provider.py`](../src/gamecollect/provider.py)
- Pack discovery/validation: `src/gamecollect/packs/registry.py`
- Worked example: everything under
  [`src/gamecollect_football/`](../src/gamecollect_football/) — `pack.py`,
  `espn.py`, `taxonomy.py`, `reconcile.py`, `operations.py`.
- Design rationale: [`DESIGN.md`](DESIGN.md) §5–§6.

Once your pack works, see [`CONTRIBUTING.md`](../CONTRIBUTING.md) for whether to
publish it as your own package or propose it into this repo.
