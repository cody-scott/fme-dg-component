"""Smoke tests for the package itself."""

import re
from pathlib import Path

import dagster_fme


def test_package_imports() -> None:
    assert isinstance(dagster_fme.__all__, list)


def test_no_stale_ssis_or_mssql_wording() -> None:
    src = Path(dagster_fme.__file__).parent
    for path in src.glob("*.py"):
        text = path.read_text()
        assert not re.search(r"(?i)ssis|mssql|\bpackages?\b", text), path.name
