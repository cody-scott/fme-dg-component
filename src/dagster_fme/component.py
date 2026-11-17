"""Dagster Component that builds FME workspace assets and a sensor from YAML."""

from __future__ import annotations

import dagster as dg
from pydantic import Field

from dagster_fme.assets import build_fme_assets
from dagster_fme.sensor import build_fme_sensor


class FMEWorkspaceSpec(dg.Model):
    """One FME workspace and the downstream assets it produces."""

    workspace: str
    key_prefix: list[str] = Field(default_factory=lambda: ["fme"])
    asset_list: list[str] = Field(default_factory=list)
    include_job_asset: bool = True


class FMEComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for connecting to FME jobs"""

    resource_key: str = "fme"

    fme_asset_specs: list[FMEWorkspaceSpec]

    build_sensor: bool = True
    sensor_name: str = "fme_sensor"
    sensor_check_minutes: int = 30

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build the asset specs for every workspace and one sensor over all of them."""
        all_assets = [
            build_fme_assets(
                spec.workspace,
                spec.key_prefix,
                spec.asset_list,
                include_job_asset=spec.include_job_asset,
            )
            for spec in self.fme_asset_specs
        ]

        all_specs = [spec for a in all_assets for spec in a.asset_specs]

        # build_sensor is declared but ignored (Phase 4).
        sensor = build_fme_sensor(
            all_assets,
            self.sensor_name,
            self.resource_key,
            self.sensor_check_minutes * 60,
        )

        return dg.Definitions(assets=all_specs, sensors=[sensor])
