"""Tests for FMEWorkspace and build_fme_assets."""

from __future__ import annotations

import dagster as dg
import pytest

from dagster_fme import FMEWorkspace, build_fme_assets


@pytest.mark.parametrize(
    ("raw", "clean"),
    [
        ("already_clean_1", "already_clean_1"),
        ("Beta Load", "Beta__Load"),
        ("alpha.fmw", "alpha__fmw"),
        ("gamma-export", "gamma__export"),
        ("café", "caf__"),
        ("a b.c-d", "a__b__c__d"),
    ],
)
def test_clean_workspace(raw: str, clean: str) -> None:
    assert FMEWorkspace.clean_workspace(raw) == clean


def test_asset_key_shapes() -> None:
    ws = build_fme_assets("Beta Load.fmw", asset_list=["table_a", "table_b"])

    assert ws.workspace == "Beta Load.fmw"
    assert ws.fqn == "Beta Load.fmw"
    assert ws.asset_name == "Beta__Load__fmw"
    assert repr(ws) == "Beta__Load__fmw"
    assert [s.key for s in ws.asset_specs] == [
        dg.AssetKey(["fme", "Beta__Load__fmw", "workspace__fmw"]),
        dg.AssetKey(["fme", "Beta__Load__fmw", "table_a"]),
        dg.AssetKey(["fme", "Beta__Load__fmw", "table_b"]),
    ]
    assert "Beta Load.fmw" in (ws.job_asset_spec.description or "")


def test_custom_key_prefix() -> None:
    ws = build_fme_assets("alpha.fmw", key_prefix=["org", "fme"], asset_list=["t"])
    assert [s.key.path for s in ws.asset_specs] == [
        ["org", "fme", "alpha__fmw", "workspace__fmw"],
        ["org", "fme", "alpha__fmw", "t"],
    ]


def test_exclude_job_asset() -> None:
    ws = build_fme_assets("alpha.fmw", asset_list=["t"], include_job_asset=False)
    assert [s.key.path for s in ws.asset_specs] == [["fme", "alpha__fmw", "t"]]


def test_asset_spec_kwargs_reach_every_spec() -> None:
    ws = build_fme_assets(
        "alpha.fmw",
        asset_list=["t1", "t2"],
        asset_spec_kwargs={"group_name": "fme_group", "owners": ["team:data"]},
    )
    assert len(ws.asset_specs) == 3
    for spec in ws.asset_specs:
        assert spec.group_name == "fme_group"
        assert list(spec.owners) == ["team:data"]


def test_defaults_are_not_shared_between_calls() -> None:
    first = build_fme_assets("alpha.fmw")
    first_ws = FMEWorkspace("alpha.fmw")
    first._asset_list.append(dg.AssetSpec("leak"))  # pyright: ignore[reportPrivateUsage]
    first_ws._key_prefix.append("leak")  # pyright: ignore[reportPrivateUsage]

    assert [s.key for s in build_fme_assets("beta.fmw").asset_specs] == [
        dg.AssetKey(["fme", "beta__fmw", "workspace__fmw"])
    ]
    assert FMEWorkspace("beta.fmw").job_asset_spec.key == dg.AssetKey(["fme", "workspace__fmw"])


def test_caller_lists_are_copied() -> None:
    prefix = ["fme"]
    ws = FMEWorkspace("alpha.fmw", key_prefix=prefix)
    prefix.append("mutated")
    assert ws.job_asset_spec.key == dg.AssetKey(["fme", "workspace__fmw"])
