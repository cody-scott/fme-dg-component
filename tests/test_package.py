"""Smoke tests for the package itself."""

import dagster_fme


def test_package_imports() -> None:
    assert isinstance(dagster_fme.__all__, list)
