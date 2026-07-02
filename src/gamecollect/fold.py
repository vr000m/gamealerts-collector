"""Core-owned name folding.

The single implementation of the accent-insensitive fold shared by the writer
(``entities.name_folded`` is stamped at upsert time) and the reader (folded
lookups fold the query string with the same function), so write-fold ==
query-fold by construction. Sport packs' reconciliation helpers
(``canonical_*``) call into this too — changing the rule here (e.g. NFKD→NFKC,
extra Unicode categories) keeps every name-matching site in lockstep instead of
silently diverging.

Ported from the gamealerts repo (``data/reconcile.py::_fold``).
"""

from __future__ import annotations

import unicodedata


def fold(name: str) -> str:
    """Reduce a display name to an accent-insensitive comparison key.

    NFKD decomposition splits accented chars into base + combining mark;
    dropping the combining marks (category "Mn") strips the diacritics. The
    result is casefolded and trimmed, so "Maxime Crépeau" and "maxime crepeau"
    fold to the same key.

    Explicitly OUT of scope: alias expansion and suffix/punctuation
    equivalence (``Jr.`` ⇔ ``Junior``, initials, hyphen variants) — this is
    NFKD + combining-strip + casefold + trim only. Alias maps (e.g. Türkiye →
    Turkey) belong to pack reconciliation, layered on top of this fold.
    """
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return stripped.casefold().strip()
