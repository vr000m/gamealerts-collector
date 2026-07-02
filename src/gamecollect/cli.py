"""Console entry point for the ``gamecollect`` CLI.

Stub only: the real CLI lands with the interfaces plan
(``docs/dev_plans/20260702-feature-collector-interfaces.md``). This exists so
the console script resolves the moment the foundation plan merges.
"""

from __future__ import annotations

from gamecollect import __version__


def main() -> int:
    """Print a not-yet-implemented notice and exit cleanly."""
    print(
        f"gamecollect {__version__}: CLI not yet implemented "
        "(collector engine and commands land in the interfaces plan)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
