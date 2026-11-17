"""Tests for FMEComponent, loaded from YAML the way users load it."""

from __future__ import annotations

import re
from collections.abc import Callable

import dagster as dg
import pytest

from dagster_fme import FMEWorkspaceSpec

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
    # Dagster wraps the validation error, so search the whole exception chain.
    messages: list[str] = []
    err: BaseException | None = exc.value
    while err is not None:
        messages.append(str(err))
        err = err.__cause__ or err.__context__
    assert re.search(
        r"Duplicate workspace\(s\) in fme_asset_specs: alpha\.fmw", "\n".join(messages)
    )


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
