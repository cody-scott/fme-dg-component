"""Dagster Component that builds FME workspace assets and a sensor from YAML."""

from __future__ import annotations

from collections import Counter

import dagster as dg
from pydantic import Field, field_validator

from dagster_fme.assets import DEFAULT_JOB_ASSET_NAME, build_fme_assets
from dagster_fme.sensor import build_fme_sensor


class FMEWorkspaceSpec(dg.Model):
    """One FME workspace and the downstream assets it produces."""

    workspace: str
    key_prefix: list[str] = Field(default_factory=lambda: ["fme"])
    asset_list: list[str] = Field(default_factory=list)
    include_job_asset: bool = True
    job_asset_name: str = DEFAULT_JOB_ASSET_NAME


class FMEComponent(dg.Component, dg.Model, dg.Resolvable):
    """Represent FME Flow workspaces as Dagster assets, with a sensor that materializes them.

    Each entry in ``fme_asset_specs`` becomes a job asset plus its downstream assets. The
    sensor watches FME Flow for successful jobs through the ``FMEResource`` registered
    under ``resource_key``.
    """

    resource_key: str = "fme"

    fme_asset_specs: list[FMEWorkspaceSpec]

    @field_validator("fme_asset_specs")
    @classmethod
    def _workspaces_are_unique(cls, specs: list[FMEWorkspaceSpec]) -> list[FMEWorkspaceSpec]:
        """Reject specs that share a workspace, since the sensor matches on that name."""
        counts = Counter(spec.workspace for spec in specs)
        duplicates = sorted(ws for ws, n in counts.items() if n > 1)
        if duplicates:
            raise ValueError(
                f"Duplicate workspace(s) in fme_asset_specs: {', '.join(duplicates)}. "
                "Each FME workspace may appear only once."
            )
        return specs

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
                job_asset_name=spec.job_asset_name,
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
