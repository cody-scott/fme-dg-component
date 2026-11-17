"""Dagster Component that builds FME workspace assets and a sensor from YAML."""

from __future__ import annotations

from collections import Counter

import dagster as dg
from pydantic import ConfigDict, Field, field_validator, model_validator

from dagster_fme.assets import DEFAULT_JOB_ASSET_NAME, build_fme_assets
from dagster_fme.resource import DEFAULT_MAX_PAGES, DEFAULT_PAGE_SIZE, FMEResource
from dagster_fme.scaffolder import FMEComponentScaffolder
from dagster_fme.sensor import build_fme_sensor


class FMEWorkspaceSpec(dg.Model):
    """One FME workspace and the downstream assets it produces."""

    workspace: str
    key_prefix: list[str] = Field(default_factory=lambda: ["fme"])
    asset_list: list[str] = Field(default_factory=list)
    include_job_asset: bool = True
    job_asset_name: str = DEFAULT_JOB_ASSET_NAME


@dg.scaffold_with(FMEComponentScaffolder)
class FMEComponent(dg.Component, dg.Model, dg.Resolvable):
    """Represent FME Flow workspaces as Dagster assets, with a sensor that materializes them.

    Each entry in ``fme_asset_specs`` becomes a job asset plus its downstream assets. The
    sensor watches FME Flow for successful jobs through the ``FMEResource`` registered
    under ``resource_key``.

    Set ``host`` and ``api_key`` to have the component register that resource itself.
    Leave both unset to use an ``FMEResource`` defined elsewhere under ``resource_key``.
    """

    # Keep resolved values (such as an env-templated api_key) out of validation errors.
    model_config = ConfigDict(hide_input_in_errors=True)

    resource_key: str = "fme"

    host: str | None = None
    api_key: str | None = Field(default=None, repr=False)
    page_size: int | None = Field(default=None, ge=1, le=1000)
    max_pages: int | None = Field(default=None, ge=1)

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

    @model_validator(mode="after")
    def _resource_settings_are_consistent(self) -> FMEComponent:
        """Require ``host`` and ``api_key`` together, and only tune a resource we build."""
        if (self.host is None) != (self.api_key is None):
            raise ValueError(
                "host and api_key must be set together to define the FME resource in this "
                "component, or both left unset to use an external resource."
            )
        if self.host is None and (self.page_size is not None or self.max_pages is not None):
            raise ValueError(
                "page_size and max_pages only apply when host and api_key are set. Configure "
                "them on the external FME resource instead."
            )
        return self

    def build_resource(self) -> FMEResource | None:
        """The ``FMEResource`` this component defines, or None to use an external one."""
        if self.host is None or self.api_key is None:
            return None
        return FMEResource(
            host=self.host,
            api_key=self.api_key,
            page_size=self.page_size if self.page_size is not None else DEFAULT_PAGE_SIZE,
            max_pages=self.max_pages if self.max_pages is not None else DEFAULT_MAX_PAGES,
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build the component's definitions.

        These are the asset specs for every workspace, one sensor over all of them unless
        ``build_sensor`` is False, and the FME resource if ``host`` and ``api_key`` are set.
        """
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

        sensors = (
            [
                build_fme_sensor(
                    all_assets,
                    self.sensor_name,
                    self.resource_key,
                    self.sensor_check_minutes * 60,
                )
            ]
            if self.build_sensor
            else []
        )

        resource = self.build_resource()
        resources = {self.resource_key: resource} if resource is not None else None
        return dg.Definitions(assets=all_specs, sensors=sensors, resources=resources)
