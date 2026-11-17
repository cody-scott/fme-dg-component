"""Dagster Component that builds FME workspace assets and a sensor from YAML."""

from __future__ import annotations

from collections import Counter
from typing import Annotated, Any

import dagster as dg
from pydantic import ConfigDict, Field, field_validator, model_validator

from dagster_fme.assets import DEFAULT_JOB_ASSET_NAME, DEFAULT_KEY_PREFIX, build_fme_assets
from dagster_fme.resource import DEFAULT_MAX_PAGES, DEFAULT_PAGE_SIZE, FMEResource
from dagster_fme.scaffolder import FMEComponentScaffolder
from dagster_fme.sensor import build_fme_sensor


class FMEWorkspaceSpec(dg.Model):
    """One FME workspace and the downstream assets it produces."""

    workspace: str = Field(
        description=(
            "FME workspace name exactly as FME Flow reports it in /jobs, e.g. "
            "'load_parcels.fmw'. The sensor matches on this raw name."
        )
    )
    key_prefix: list[str] = Field(
        default_factory=lambda: list(DEFAULT_KEY_PREFIX),
        description="Leading asset key components. Assets are keyed under "
        "key_prefix + [<cleaned workspace>].",
    )
    asset_list: list[str] = Field(
        default_factory=list,
        description="Names of the downstream assets (e.g. tables) the workspace writes. "
        "Each becomes key_prefix + [<cleaned workspace>, <name>].",
    )
    include_job_asset: bool = Field(
        default=True,
        description="Whether to add an asset for the workspace itself, keyed "
        "key_prefix + [<cleaned workspace>, job_asset_name].",
    )
    job_asset_name: str = Field(
        default=DEFAULT_JOB_ASSET_NAME,
        description="Last key component of the workspace's own asset.",
    )


def _doc(description: str) -> dg.Resolver:
    """Attach a description that ``dg`` shows for a component field.

    ``Field(description=...)`` is dropped for fields with a default when Dagster derives
    the YAML model, so component fields carry their description on the resolver instead.
    """
    return dg.Resolver.default(description=description)


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

    resource_key: Annotated[
        str,
        _doc(
            "Key of the FMEResource the sensor uses. The component registers its own "
            "resource under this key when host and api_key are set."
        ),
    ] = "fme"

    host: Annotated[
        str | None,
        _doc(
            "FME Flow base URL, e.g. https://fme.example.com. Set it together with api_key "
            "to define the resource in this component."
        ),
    ] = None
    api_key: Annotated[
        str | None,
        _doc(
            "FME Flow API token. Supply it with \"{{ env('FME_API_KEY') }}\" and never write "
            "the token into YAML."
        ),
    ] = Field(default=None, repr=False)
    page_size: Annotated[
        int | None,
        _doc(
            f"Jobs requested per page, 1 to 1000 (default {DEFAULT_PAGE_SIZE}). Only valid "
            "with host and api_key."
        ),
    ] = Field(default=None, ge=1, le=1000)
    max_pages: Annotated[
        int | None,
        _doc(
            f"Page cap for one job scan (default {DEFAULT_MAX_PAGES}). Only valid with host "
            "and api_key."
        ),
    ] = Field(default=None, ge=1)

    fme_asset_specs: Annotated[
        list[FMEWorkspaceSpec],
        _doc("The FME workspaces to represent. Each workspace may appear only once."),
    ]

    build_sensor: Annotated[
        bool, _doc("Whether to build the sensor that materializes assets from successful jobs.")
    ] = True
    sensor_name: Annotated[str, _doc("Name of the sensor.")] = "fme_sensor"
    sensor_check_minutes: Annotated[int, _doc("Minimum minutes between sensor ticks.")] = 30

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
        tuning: dict[str, Any] = {
            name: value
            for name, value in (("page_size", self.page_size), ("max_pages", self.max_pages))
            if value is not None
        }
        return FMEResource(host=self.host, api_key=self.api_key, **tuning)

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
