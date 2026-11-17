"""Tests for dg CLI integration: the registry entry point and the scaffolder."""

from __future__ import annotations

import dataclasses
import importlib.metadata
from collections.abc import Callable
from pathlib import Path

import dagster as dg
import pytest
import yaml
from dagster.components.core.package_entry import discover_entry_point_package_objects
from dagster.components.scaffold.scaffold import get_scaffolder

import dagster_fme
from dagster_fme import FMEComponent, FMEResource
from dagster_fme.scaffolder import FMEComponentScaffolder

Loader = Callable[[str], dg.Definitions]
TYPE_NAME = "dagster_fme.FMEComponent"

# `ScaffoldRequest.append` exists only after Dagster 1.11.
SUPPORTS_APPEND = "append" in {f.name for f in dataclasses.fields(dg.ScaffoldRequest)}


def _scaffold(tmp_path: Path, fmt: str = "yaml", *, append: bool = False) -> None:
    extra = {"append": append} if SUPPORTS_APPEND else {}
    FMEComponentScaffolder().scaffold(
        dg.ScaffoldRequest(
            type_name=TYPE_NAME,
            target_path=tmp_path,
            scaffold_format=fmt,  # type: ignore[arg-type]
            project_root=None,
            params=None,  # type: ignore[arg-type]
            **extra,
        )
    )


def test_entry_point_resolves_to_dagster_fme() -> None:
    matches = [
        ep
        for ep in importlib.metadata.entry_points(group="dagster_dg_cli.registry_modules")
        if ep.name == "dagster_fme"
    ]
    assert len(matches) == 1
    assert matches[0].value == "dagster_fme"
    assert matches[0].load() is dagster_fme


def test_dg_discovers_the_component() -> None:
    objects = discover_entry_point_package_objects()
    found = {k.to_typename(): v for k, v in objects.items() if k.namespace == "dagster_fme"}
    assert found == {TYPE_NAME: FMEComponent}


def test_component_has_the_scaffolder() -> None:
    assert isinstance(get_scaffolder(FMEComponent), FMEComponentScaffolder)


def test_scaffolded_yaml_loads(
    tmp_path: Path, load_component_defs: Loader, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The commented-out env() template must not be evaluated.
    monkeypatch.delenv("FME_API_KEY", raising=False)
    out = tmp_path / "scaffold"
    out.mkdir()
    _scaffold(out)

    text = (out / "defs.yaml").read_text()
    assert yaml.safe_load(text)["type"] == TYPE_NAME
    assert "# api_key: \"{{ env('FME_API_KEY') }}\"" in text

    defs = load_component_defs(text)
    keys = {a.key.to_user_string() for a in defs.assets or [] if isinstance(a, dg.AssetSpec)}
    assert keys == {
        "fme/example_workspace__fmw/workspace__fmw",
        "fme/example_workspace__fmw/example_table",
    }
    assert [s.name for s in defs.sensors or [] if isinstance(s, dg.SensorDefinition)] == [
        "fme_sensor"
    ]
    assert not defs.resources


def test_scaffolded_resource_template_loads_when_uncommented(
    tmp_path: Path, load_component_defs: Loader, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FME_API_KEY", "secret-from-env")
    out = tmp_path / "scaffold"
    out.mkdir()
    _scaffold(out)

    text = (out / "defs.yaml").read_text()
    text = text.replace("  # host:", "  host:").replace("  # api_key:", "  api_key:")

    defs = load_component_defs(text)
    resource = (defs.resources or {})["fme"]
    assert isinstance(resource, FMEResource)
    assert resource.host == "https://fme.example.com"
    assert resource.api_key == "secret-from-env"


@pytest.mark.skipif(not SUPPORTS_APPEND, reason="ScaffoldRequest.append needs Dagster > 1.11")
def test_scaffold_append_adds_a_second_document(tmp_path: Path) -> None:
    defs_yaml = tmp_path / "defs.yaml"
    defs_yaml.write_text("type: other.Component\nattributes: {}\n")
    _scaffold(defs_yaml, append=True)

    docs = list(yaml.safe_load_all(defs_yaml.read_text()))
    assert [d["type"] for d in docs] == ["other.Component", TYPE_NAME]


@pytest.mark.xfail(strict=True, reason="N-9: append joins --- onto a last line with no newline")
@pytest.mark.skipif(not SUPPORTS_APPEND, reason="ScaffoldRequest.append needs Dagster > 1.11")
def test_scaffold_append_without_trailing_newline(tmp_path: Path) -> None:
    defs_yaml = tmp_path / "defs.yaml"
    defs_yaml.write_text("type: other.Component\nattributes: {}")
    _scaffold(defs_yaml, append=True)

    docs = list(yaml.safe_load_all(defs_yaml.read_text()))
    assert [d["type"] for d in docs] == ["other.Component", TYPE_NAME]


def test_scaffold_python_format_writes_component_stub(tmp_path: Path) -> None:
    _scaffold(tmp_path, "python")

    assert not (tmp_path / "defs.yaml").exists()
    stub = (tmp_path / "component.py").read_text()
    assert "from dagster_fme import FMEComponent" in stub
