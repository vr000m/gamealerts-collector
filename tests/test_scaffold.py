"""Phase 1 scaffold tests: package import, version, entry-point group.

Covers the plan contract for `tests/test_scaffold.py`:
- the `gamecollect` package imports,
- `__version__` is present and consistent with installed metadata,
- the `gamecollect.packs` entry-point group is queryable via
  `importlib.metadata` (it may be empty until a pack ships in Phase 3/4).
"""

import importlib.metadata


def test_package_imports():
    import gamecollect  # noqa: F401


def test_cli_module_imports_and_main_is_callable():
    from gamecollect import cli

    assert callable(cli.main)


def test_version_present():
    import gamecollect

    assert isinstance(gamecollect.__version__, str)
    assert gamecollect.__version__.strip() != ""


def test_version_matches_installed_metadata():
    import gamecollect

    assert importlib.metadata.version("gamecollect") == gamecollect.__version__


def test_console_script_declared():
    scripts = importlib.metadata.entry_points(group="console_scripts")
    names = {ep.name for ep in scripts}
    assert "gamecollect" in names
    (ep,) = [ep for ep in scripts if ep.name == "gamecollect"]
    assert ep.value == "gamecollect.cli:main"


def test_pack_entry_point_group_queryable():
    # Phase 1 ships no packs; the group must still be queryable without
    # error and yield an iterable of EntryPoint objects.
    eps = importlib.metadata.entry_points(group="gamecollect.packs")
    for ep in eps:
        assert isinstance(ep, importlib.metadata.EntryPoint)
