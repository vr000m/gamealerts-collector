"""Football (FIFA WC2026) sport pack for gamecollect.

Ships in the same wheel as ``gamecollect`` but consumes the SDK only through
its public API + the ``gamecollect.packs`` entry point (``football-wc2026``),
proving the pack seam from the outside. Ported out of gamealerts
(``data/provider.py`` football shapes, ``data/espn.py``,
``data/reconcile.py``) with behavior parity as the acceptance bar; this
package never imports gamealerts.
"""

from __future__ import annotations
