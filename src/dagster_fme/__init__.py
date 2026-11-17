"""Dagster component for observing FME Flow workspaces as assets."""

from dagster_fme.assets import FMEWorkspace, build_fme_assets
from dagster_fme.component import FMEComponent, FMEWorkspaceSpec
from dagster_fme.resource import FMEResource
from dagster_fme.sensor import build_fme_sensor

__all__ = [
    "FMEComponent",
    "FMEResource",
    "FMEWorkspace",
    "FMEWorkspaceSpec",
    "build_fme_assets",
    "build_fme_sensor",
]
