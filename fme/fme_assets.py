import re
from typing import Any

from dagster import (
    AssetSpec,
)


class FMEAsset:
    # this may be an invalid asset string but we need to keep it for the comparison to fme
    _workspace: str

    _key_prefix: list[str]
    _asset_list: list[AssetSpec]
    _include_job_asset: bool
    _asset_spec_kwargs: dict[str, Any]

    @property
    def fqn(self):
        return f"{self._workspace}"
    
    @property
    def asset_name(self):
        return self.clean_workspace(self._workspace)

    @staticmethod
    def clean_workspace(name: str):
        return re.sub(r"[^A-Za-z0-9_]", "__", name)

    def __init__(
        self,
        workspace: str,
        key_prefix: list[str] = ["fme"],
        asset_list: list[AssetSpec] = [],
        include_job_asset: bool = True,
        asset_spec_kwargs: dict[str, Any] | None = None,
    ):
        """Create an asset representing a FME job.

        Args:
            workspace (str): name of job
            key_prefix (list[str], optional): key prefixes to pass to AssetSpec. Defaults to ["fme"].
            asset_list (list[AssetSpec], optional): List of assets associated with the package. Defaults to [].
            include_job_asset (bool, optional): Should the `asset_specs` property include an asset for the package itself. Defaults to True.
            asset_spec_kwargs (dict[str, Any], optional): Parameters to pass to AssetSpec
        """
        self._workspace = workspace

        self._key_prefix = key_prefix
        self._asset_list = asset_list
        self._include_job_asset = include_job_asset

        asset_spec_kwargs = asset_spec_kwargs if asset_spec_kwargs is not None else {}
        self._asset_spec_kwargs = asset_spec_kwargs

    def __repr__(self):
        return self.asset_name

    @property
    def job_asset_spec(self):
        key = self._key_prefix + ['workspace__fmw']
        spec_args = self._asset_spec_kwargs | {"key": key}
        desc = f"""
        This is the asset that represents the workspace `{self._workspace}`.
        """
        return AssetSpec(**spec_args, description=desc)

    @property
    def asset_specs(self):
        """
        Returns the list of asset specs for the Job Asset.

        If include_job_asset is set to false on creation,
        the asset representing the job itself is not included.
        """
        base_asset = [self.job_asset_spec] if self._include_job_asset else []
        return base_asset + self._asset_list


def build_fme_assets(
    workspace: str,
    key_prefix: list[str] = ["fme"],
    asset_list: list[str] = [],
    include_job_asset: bool = True,
    asset_spec_kwargs: dict[str, Any] | None = None,
):
    """
    Construct a MSSQLJob asset, and bundle together the list of assets provided.
    This places the assets in `asset_list` within the key structure
    of the job asset to allow it to check for events.
    """
    clean_workspace = FMEAsset.clean_workspace(workspace)

    base_key = key_prefix + [clean_workspace]
    table_specs = []
    asset_spec_kwargs = asset_spec_kwargs if asset_spec_kwargs is not None else {}
    for _asset in asset_list:
        spec_args = asset_spec_kwargs | {
            "key": base_key + [_asset],
        }
        _spec = AssetSpec(**spec_args)
        table_specs.append(_spec)

    _asset = FMEAsset(
        workspace=workspace,
        key_prefix=key_prefix + [clean_workspace],
        asset_list=table_specs,
        include_job_asset=include_job_asset,
        asset_spec_kwargs=asset_spec_kwargs,
    )

    return _asset