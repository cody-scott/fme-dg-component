"""Dagster component for observing FME Flow workspaces as assets."""

from dagster_fme.assets import FMEWorkspace, build_fme_assets
from dagster_fme.component import FMEComponent, FMEWorkspaceSpec
from dagster_fme.resource import FMEPaginationLimitError, FMEResource, LatestJobs
from dagster_fme.sensor import build_fme_sensor

__all__ = [
    "FMEComponent",
    "FMEPaginationLimitError",
    "FMEResource",
    "FMEWorkspace",
    "FMEWorkspaceSpec",
    "LatestJobs",
    "build_fme_assets",
    "build_fme_sensor",
]
