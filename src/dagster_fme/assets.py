"""Asset specs for FME Flow workspaces."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import dagster as dg

DEFAULT_JOB_ASSET_NAME = "workspace__fmw"
"""Default last key component of a workspace's job asset."""

DEFAULT_KEY_PREFIX: tuple[str, ...] = ("fme",)
"""Default leading asset key components."""


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
    _job_asset_name: str

    def __init__(
        self,
        workspace: str,
        key_prefix: Sequence[str] | None = None,
        asset_list: Sequence[dg.AssetSpec] | None = None,
        include_job_asset: bool = True,
        asset_spec_kwargs: dict[str, Any] | None = None,
        job_asset_name: str = DEFAULT_JOB_ASSET_NAME,
    ) -> None:
        """Bundle an FME workspace with the asset specs it produces.

        Most callers should use ``build_fme_assets``, which builds the child specs and
        nests their keys under the workspace.

        Args:
            workspace: Raw FME workspace name, exactly as the FME API reports it.
            key_prefix: Key prefix for the job asset, which is keyed
                ``key_prefix + [job_asset_name]``. Defaults to ``["fme"]``.
            asset_list: Specs for the downstream assets the workspace produces.
                Defaults to none.
            include_job_asset: Whether ``asset_specs`` includes the asset for the
                workspace itself. Defaults to True.
            asset_spec_kwargs: Extra keyword arguments for the job asset's ``AssetSpec``,
                such as ``group_name`` or ``owners``.
            job_asset_name: Last key component of the job asset. Defaults to
                ``"workspace__fmw"``.
        """
        self._workspace = workspace
        self._key_prefix = list(key_prefix if key_prefix is not None else DEFAULT_KEY_PREFIX)
        self._asset_list = list(asset_list) if asset_list is not None else []
        self._include_job_asset = include_job_asset
        self._asset_spec_kwargs = asset_spec_kwargs if asset_spec_kwargs is not None else {}
        self._job_asset_name = job_asset_name

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
        key = [*self._key_prefix, self._job_asset_name]
        spec_args: dict[str, Any] = self._asset_spec_kwargs | {"key": key}
        desc = f"The asset that represents the FME workspace `{self._workspace}`."
        return dg.AssetSpec(**spec_args, description=desc)

    @property
    def asset_specs(self) -> list[dg.AssetSpec]:
        """All asset specs for this workspace: the job asset, then the child assets.

        The job asset is left out when ``include_job_asset`` is False.
        """
        base_asset = [self.job_asset_spec] if self._include_job_asset else []
        return base_asset + self._asset_list


def build_fme_assets(
    workspace: str,
    key_prefix: Sequence[str] | None = None,
    asset_list: Sequence[str] | None = None,
    include_job_asset: bool = True,
    asset_spec_kwargs: dict[str, Any] | None = None,
    job_asset_name: str = DEFAULT_JOB_ASSET_NAME,
) -> FMEWorkspace:
    """Build the asset specs for one FME workspace.

    Every asset is nested under the cleaned workspace name. The job asset is keyed
    ``key_prefix + [clean_workspace(workspace), job_asset_name]`` and each child asset
    ``key_prefix + [clean_workspace(workspace), asset]``. The sensor materializes all of
    them when a newer successful job for the workspace finishes.

    Args:
        workspace: Raw FME workspace name, exactly as the FME API reports it.
        key_prefix: Leading key components. Defaults to ``["fme"]``.
        asset_list: Names of the downstream assets (e.g. tables) the workspace writes.
            Defaults to none.
        include_job_asset: Whether to include an asset for the workspace itself.
            Defaults to True.
        asset_spec_kwargs: Extra keyword arguments passed to every ``AssetSpec``, such
            as ``group_name`` or ``owners``.
        job_asset_name: Last key component of the job asset. Defaults to
            ``"workspace__fmw"``.

    Returns:
        The workspace and its asset specs.
    """
    key_prefix = DEFAULT_KEY_PREFIX if key_prefix is None else key_prefix
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
        job_asset_name=job_asset_name,
    )
