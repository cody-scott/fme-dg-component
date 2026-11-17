"""Asset specs for FME Flow workspaces."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import dagster as dg


class FMEWorkspace:
    """An FME workspace and the asset specs that represent it.

    The raw workspace name is kept as-is because it's what's matched against FME
    API responses. ``asset_name`` is the key-safe form.
    """

    _workspace: str
    _key_prefix: list[str]
    _asset_list: list[dg.AssetSpec]
    _include_job_asset: bool
    _asset_spec_kwargs: dict[str, Any]

    def __init__(
        self,
        workspace: str,
        key_prefix: Sequence[str] | None = None,
        asset_list: Sequence[dg.AssetSpec] | None = None,
        include_job_asset: bool = True,
        asset_spec_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Create an asset representing a FME job.

        Args:
            workspace: name of job
            key_prefix: key prefixes to pass to AssetSpec. Defaults to ["fme"].
            asset_list: List of assets associated with the package. Defaults to [].
            include_job_asset: Should the `asset_specs` property include an asset for the
                package itself. Defaults to True.
            asset_spec_kwargs: Parameters to pass to AssetSpec
        """
        self._workspace = workspace
        self._key_prefix = list(key_prefix) if key_prefix is not None else ["fme"]
        self._asset_list = list(asset_list) if asset_list is not None else []
        self._include_job_asset = include_job_asset
        self._asset_spec_kwargs = asset_spec_kwargs if asset_spec_kwargs is not None else {}

    @property
    def workspace(self) -> str:
        """The raw FME workspace name, as reported by the FME API."""
        return self._workspace

    @property
    def fqn(self) -> str:
        """The name used to match this workspace in FME responses and the sensor cursor."""
        return f"{self._workspace}"

    @property
    def asset_name(self) -> str:
        """The key-safe workspace name."""
        return self.clean_workspace(self._workspace)

    @staticmethod
    def clean_workspace(name: str) -> str:
        """Replace every character outside ``[A-Za-z0-9_]`` with ``__``."""
        return re.sub(r"[^A-Za-z0-9_]", "__", name)

    def __repr__(self) -> str:
        return self.asset_name

    @property
    def job_asset_spec(self) -> dg.AssetSpec:
        """The spec for the asset that represents the workspace itself."""
        key = [*self._key_prefix, "workspace__fmw"]
        spec_args: dict[str, Any] = self._asset_spec_kwargs | {"key": key}
        desc = f"""
        This is the asset that represents the workspace `{self._workspace}`.
        """
        return dg.AssetSpec(**spec_args, description=desc)

    @property
    def asset_specs(self) -> list[dg.AssetSpec]:
        """
        Returns the list of asset specs for the Job Asset.

        If include_job_asset is set to false on creation,
        the asset representing the job itself is not included.
        """
        base_asset = [self.job_asset_spec] if self._include_job_asset else []
        return base_asset + self._asset_list


def build_fme_assets(
    workspace: str,
    key_prefix: Sequence[str] | None = None,
    asset_list: Sequence[str] | None = None,
    include_job_asset: bool = True,
    asset_spec_kwargs: dict[str, Any] | None = None,
) -> FMEWorkspace:
    """
    Construct a MSSQLJob asset, and bundle together the list of assets provided.
    This places the assets in `asset_list` within the key structure
    of the job asset to allow it to check for events.
    """
    key_prefix = ["fme"] if key_prefix is None else key_prefix
    asset_list = [] if asset_list is None else asset_list
    clean_workspace = FMEWorkspace.clean_workspace(workspace)

    base_key = [*key_prefix, clean_workspace]
    asset_spec_kwargs = asset_spec_kwargs if asset_spec_kwargs is not None else {}
    table_specs: list[dg.AssetSpec] = []
    for _asset in asset_list:
        spec_args: dict[str, Any] = asset_spec_kwargs | {"key": [*base_key, _asset]}
        table_specs.append(dg.AssetSpec(**spec_args))

    return FMEWorkspace(
        workspace=workspace,
        key_prefix=base_key,
        asset_list=table_specs,
        include_job_asset=include_job_asset,
        asset_spec_kwargs=asset_spec_kwargs,
    )
