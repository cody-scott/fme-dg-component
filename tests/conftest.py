"""Shared fixtures. All HTTP is mocked with respx; no test touches the network."""

from __future__ import annotations

import sys
import textwrap
import uuid
import warnings
from collections.abc import Callable, Iterator
from pathlib import Path

import dagster as dg
import pytest

from dagster_fme import FMEResource
from tests.helpers import API_KEY, HOST


@pytest.fixture
def fme_resource() -> FMEResource:
    # Trailing slash on purpose: the resource must normalise it.
    return FMEResource(host=f"{HOST}/", api_key=API_KEY)


@pytest.fixture
def load_component_defs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Callable[[str], dg.Definitions]]:
    """Return a loader that puts a ``defs.yaml`` in a throwaway dg project and loads it.

    This goes through ``dg.load_from_defs_folder``, the same YAML-to-Definitions path
    users hit, and works on both the Dagster floor (1.11) and current releases.
    """
    module = f"fme_test_project_{uuid.uuid4().hex[:8]}"

    def _load(defs_yaml: str) -> dg.Definitions:
        (tmp_path / "pyproject.toml").write_text(f'[tool.dg.project]\nroot_module = "{module}"\n')
        component_dir = tmp_path / "src" / module / "defs" / "fme"
        component_dir.mkdir(parents=True, exist_ok=True)
        (tmp_path / "src" / module / "__init__.py").touch()
        (tmp_path / "src" / module / "defs" / "__init__.py").touch()
        (component_dir / "defs.yaml").write_text(textwrap.dedent(defs_yaml))
        monkeypatch.syspath_prepend(str(tmp_path / "src"))
        # `project_root` is deprecated after 1.11, but its replacement doesn't exist there.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message=".*project_root.*", category=DeprecationWarning
            )
            return dg.load_from_defs_folder(project_root=tmp_path)

    yield _load

    for name in [m for m in sys.modules if m == module or m.startswith(f"{module}.")]:
        del sys.modules[name]
