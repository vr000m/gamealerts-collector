# Contributing

Thanks for your interest in gamealerts-collector. Contributions fall into two
categories, and **collectors have their own path** — read the section that fits.

## Two ways to contribute a collector

The collector is a plugin system: a sport pack registers under the
`gamecollect.packs` entry point and is discovered at runtime, with **no change to
core required**. Because of that, you have a choice.

### A. Publish your own collector package (preferred)

If you're building a collector for a sport, league, or provider, the easiest and
most flexible path is to ship it as **your own pip-installable package** — you
own the repo, the release cadence, and the provider quirks. Users install it
alongside core and it just works.

- Follow [`docs/ADDING_A_COLLECTOR.md`](docs/ADDING_A_COLLECTOR.md).
- Name your package `gamecollect_<sport>` (or whatever you like) and register a
  unique entry-point name so it doesn't collide with other packs.
- This project is MIT-licensed (see [`LICENSE`](LICENSE)); you're free to license
  your own pack however you wish, including commercially.
- Tell us about it — open an issue or discussion and we're happy to link to
  community packs from the README.

This keeps your collector's provider-specific churn out of this repo and lets you
move at your own pace.

### B. Propose a collector into this repo (first-party packs)

Some packs are worth maintaining here as **blessed, first-party collectors**
(the football/World Cup pack, `gamecollect_football`, is the reference). If you
think yours belongs in-tree, open an issue **first** to discuss scope and
maintenance ownership before writing a large PR — we're selective about what we
commit to maintaining long-term.

If we agree it should live here, the PR should:

- [ ] Live under `src/gamecollect_<sport>/` mirroring the football pack layout
      (`pack.py`, provider adapter, `taxonomy.py`, plus `reconcile.py`/
      `operations.py` if needed).
- [ ] Register its entry point in `pyproject.toml` under
      `[project.entry-points."gamecollect.packs"]` with a unique name.
- [ ] Include tests under `tests/<sport>/` covering the provider adapter,
      taxonomy, and any reconciliation/side-table logic.
- [ ] Touch **only** pack-owned code — never alter core tables or import pack
      modules from core (dependency direction is pack → core).
- [ ] Pass the checks below.

## Development workflow (any PR)

- **Work on a feature branch**, never commit directly to `main`.
- Keep commits focused; one logical change per commit.
- Update docs alongside code (dev plans under `docs/dev_plans/`, `README.md`,
  `DESIGN.md`) — not after.
- Before opening/updating a PR:

  ```bash
  ruff format          # then verify clean:
  ruff check
  pytest               # full suite
  ```

- Open the PR against `main` at
  https://github.com/vr000m/gamealerts-collector.

## Non-collector contributions

Bug fixes, core improvements, and docs are welcome via normal PRs — same
workflow above. For anything non-trivial in core, open an issue first so we can
agree on the approach.

## License

By contributing, you agree that your contributions are licensed under the
project's [MIT License](LICENSE).
