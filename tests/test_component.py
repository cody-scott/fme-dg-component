"""Tests for FMEComponent, loaded from YAML the way users load it."""

from __future__ import annotations

import re
from collections.abc import Callable

import dagster as dg
import pytest

from dagster_fme import FMEComponent, FMEResource, FMEWorkspaceSpec

DEFS_YAML = """
type: dagster_fme.FMEComponent
attributes:
  resource_key: fme
  sensor_name: my_fme_sensor
  sensor_check_minutes: 5
  fme_asset_specs:
    - workspace: alpha.fmw
      key_prefix: [fme]
      asset_list: [table_a, table_b]
    - workspace: Beta Load.fmw
      key_prefix: [fme]
      include_job_asset: false
      asset_list: [table_c]
"""

Loader = Callable[[str], dg.Definitions]


def _chain(exc: BaseException) -> str:
    """Every message in an exception chain. Dagster wraps component validation errors."""
    messages: list[str] = []
    err: BaseException | None = exc
    while err is not None:
        messages.append(str(err))
        err = err.__cause__ or err.__context__
    return "\n".join(messages)


def _asset_keys(defs: dg.Definitions) -> set[str]:
    return {a.key.to_user_string() for a in defs.assets or [] if isinstance(a, dg.AssetSpec)}


def test_component_loads_from_yaml(load_component_defs: Loader) -> None:
    defs = load_component_defs(DEFS_YAML)

    assert _asset_keys(defs) == {
        "fme/alpha__fmw/workspace__fmw",
        "fme/alpha__fmw/table_a",
        "fme/alpha__fmw/table_b",
        "fme/Beta__Load__fmw/table_c",
    }
    sensors = list(defs.sensors or [])
    assert len(sensors) == 1
    sensor = sensors[0]
    assert isinstance(sensor, dg.SensorDefinition)
    assert sensor.name == "my_fme_sensor"
    assert sensor.minimum_interval_seconds == 300
    assert sensor.required_resource_keys == {"fme"}


def test_default_key_prefix_is_lowercase_fme() -> None:
    assert FMEWorkspaceSpec(workspace="alpha.fmw").key_prefix == ["fme"]


def test_duplicate_workspaces_are_rejected(load_component_defs: Loader) -> None:
    with pytest.raises(Exception) as exc:
        load_component_defs(
            """
            type: dagster_fme.FMEComponent
            attributes:
              fme_asset_specs:
                - workspace: alpha.fmw
                - workspace: alpha.fmw
            """
        )
    assert re.search(r"Duplicate workspace\(s\) in fme_asset_specs: alpha\.fmw", _chain(exc.value))


def test_default_key_prefix_matches_builder(load_component_defs: Loader) -> None:
    defs = load_component_defs(
        """
        type: dagster_fme.FMEComponent
        attributes:
          fme_asset_specs:
            - workspace: alpha.fmw
              asset_list: [table_a]
        """
    )
    assert _asset_keys(defs) == {"fme/alpha__fmw/workspace__fmw", "fme/alpha__fmw/table_a"}


def test_job_asset_name_from_yaml(load_component_defs: Loader) -> None:
    defs = load_component_defs(
        """
        type: dagster_fme.FMEComponent
        attributes:
          fme_asset_specs:
            - workspace: alpha.fmw
              job_asset_name: run
              asset_list: [table_a]
            - workspace: beta.fmw
        """
    )
    assert _asset_keys(defs) == {
        "fme/alpha__fmw/run",
        "fme/alpha__fmw/table_a",
        "fme/beta__fmw/workspace__fmw",
    }


# --- optional in-component resource ----------------------------------------------------

SECRET = "secret-from-env"


def _resource_yaml(extra: str) -> str:
    return f"""
type: dagster_fme.FMEComponent
attributes:
  resource_key: my_fme
{extra}
  fme_asset_specs:
    - workspace: alpha.fmw
"""


def test_component_defines_resource_from_env(
    load_component_defs: Loader, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FME_API_KEY", SECRET)
    defs = load_component_defs(
        _resource_yaml(
            """  host: https://fme.example.com
  api_key: "{{ env('FME_API_KEY') }}"
  page_size: 50
  max_pages: 3"""
        )
    )
    resource = (defs.resources or {})["my_fme"]
    assert isinstance(resource, FMEResource)
    assert resource.host == "https://fme.example.com"
    assert resource.api_key == SECRET
    assert (resource.page_size, resource.max_pages) == (50, 3)
    # The sensor's required resource key is satisfied by the component's own resource.
    dg.Definitions.validate_loadable(defs)


def test_component_resource_uses_resource_defaults(load_component_defs: Loader) -> None:
    defs = load_component_defs(_resource_yaml("  host: https://fme.example.com\n  api_key: k"))
    resource = (defs.resources or {})["my_fme"]
    assert isinstance(resource, FMEResource)
    assert (resource.page_size, resource.max_pages) == (1000, 20)


def test_component_without_host_registers_no_resource(load_component_defs: Loader) -> None:
    defs = load_component_defs(_resource_yaml(""))
    assert not defs.resources


@pytest.mark.parametrize(
    ("extra", "match"),
    [
        ("  host: https://fme.example.com", "host and api_key must be set together"),
        ("  api_key: \"{{ env('FME_API_KEY') }}\"", "host and api_key must be set together"),
        ("  page_size: 10", "page_size and max_pages only apply"),
        ("  max_pages: 10", "page_size and max_pages only apply"),
    ],
)
def test_component_resource_settings_are_validated(
    load_component_defs: Loader, monkeypatch: pytest.MonkeyPatch, extra: str, match: str
) -> None:
    monkeypatch.setenv("FME_API_KEY", SECRET)
    with pytest.raises(Exception) as exc:
        load_component_defs(_resource_yaml(extra))
    assert match in _chain(exc.value)
    assert SECRET not in _chain(exc.value)


def test_component_api_key_not_in_repr() -> None:
    component = FMEComponent(host="https://fme.example.com", api_key=SECRET, fme_asset_specs=[])
    assert SECRET not in repr(component)


@pytest.mark.parametrize("model", [FMEComponent.get_model_cls(), FMEWorkspaceSpec])
def test_every_yaml_field_has_a_description(model: type[dg.Model]) -> None:
    """``dg`` shows these descriptions as field help, so none may be missing."""
    properties = model.model_json_schema()["properties"]
    missing = sorted(name for name, prop in properties.items() if not prop.get("description"))
    assert missing == []


def test_build_sensor_false_omits_sensor(load_component_defs: Loader) -> None:
    defs = load_component_defs(
        """
        type: dagster_fme.FMEComponent
        attributes:
          build_sensor: false
          fme_asset_specs:
            - workspace: alpha.fmw
        """
    )
    assert not list(defs.sensors or [])
    assert _asset_keys(defs) == {"fme/alpha__fmw/workspace__fmw"}
