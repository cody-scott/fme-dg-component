"""Tests for the sensor and its pure helpers."""

from __future__ import annotations

import datetime
import inspect
import json
from typing import Any

import dagster as dg
import pytest
import respx

import dagster_fme.sensor as sensor_module
from dagster_fme import FMEResource, FMEWorkspace, build_fme_assets, build_fme_sensor
from dagster_fme.sensor import (
    evaluate_jobs,
    get_latest_time_for_responses,
    parse_cursor,
)
from tests.helpers import JOBS_URL, jobs_responder, load_fixture

UTC = datetime.timezone.utc


def _items(name: str) -> list[dict[str, Any]]:
    return load_fixture(name)["items"]


def _workspaces() -> list[FMEWorkspace]:
    return [
        build_fme_assets("alpha.fmw", asset_list=["table_a"]),
        build_fme_assets("Beta Load.fmw", asset_list=[]),
    ]


def _run_sensor(
    respx_mock: respx.MockRouter,
    fme_resource: FMEResource,
    fixture: str,
    cursor: dict[str, str] | None,
) -> tuple[dg.SensorResult, dict[str, str]]:
    respx_mock.get(JOBS_URL).mock(side_effect=jobs_responder(fixture))
    sensor = build_fme_sensor(_workspaces(), "fme_sensor", "fme")
    context = dg.build_sensor_context(
        cursor=json.dumps(cursor) if cursor is not None else None,
        resources={"fme": fme_resource},
    )
    result = sensor(context)
    assert isinstance(result, dg.SensorResult)
    # Phase 2 still persists via update_cursor (BUG-1), so read it back from the context.
    return result, parse_cursor(context.cursor)


# --- pure helpers ---------------------------------------------------------------------


@pytest.mark.parametrize("raw", [None, ""])
def test_parse_cursor_empty(raw: str | None) -> None:
    assert parse_cursor(raw) == {}


def test_parse_cursor_json() -> None:
    assert parse_cursor('{"a": "2026-01-01T00:00:00+00:00"}') == {"a": "2026-01-01T00:00:00+00:00"}


def test_latest_time_picks_latest_per_workspace_across_timezones() -> None:
    latest = get_latest_time_for_responses(_items("jobs_multiple_runs.json"))

    # 09:30-05:00 is 14:30Z, later than 12:15Z.
    assert latest["alpha.fmw"]["id"] == 302
    assert latest["alpha.fmw"]["time_finished"] == datetime.datetime(2026, 1, 4, 14, 30, tzinfo=UTC)
    # 07:20+02:00 is 05:20Z, later than 23:30Z the day before.
    assert latest["Beta Load.fmw"]["id"] == 304
    assert latest["Beta Load.fmw"]["time_finished"].utcoffset() == datetime.timedelta(hours=2)
    assert latest["Beta Load.fmw"]["data"]["repository"] == "Samples"


def test_latest_time_parses_z_suffix() -> None:
    latest = get_latest_time_for_responses(_items("jobs_page_1.json"))
    assert latest["alpha.fmw"]["time_finished"] == datetime.datetime(2026, 1, 2, 10, 5, tzinfo=UTC)


@pytest.mark.parametrize(
    ("fixture", "match"),
    [
        ("job_missing_workspace.json", "Workspace"),
        ("job_missing_time_finished.json", "Time finished"),
    ],
)
def test_latest_time_raises_on_missing_fields(fixture: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        get_latest_time_for_responses(_items(fixture))


def test_evaluate_first_sight_is_baseline_only() -> None:
    latest = get_latest_time_for_responses(_items("jobs_page_1.json"))
    events, cursor = evaluate_jobs({}, latest, _workspaces())
    assert events == []
    assert cursor == {
        "alpha.fmw": "2026-01-02T10:05:00+00:00",
        "Beta Load.fmw": "2026-01-02T11:01:00+00:00",
    }


# --- sensor ---------------------------------------------------------------------------


def test_sensor_first_sight_sets_baseline(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    result, cursor = _run_sensor(respx_mock, fme_resource, "jobs_page_1.json", None)
    assert result.asset_events == []
    assert cursor == {
        "alpha.fmw": "2026-01-02T10:05:00+00:00",
        "Beta Load.fmw": "2026-01-02T11:01:00+00:00",
    }


def test_sensor_newer_finish_emits_for_every_spec(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    old = {"alpha.fmw": "2026-01-01T00:00:00+00:00", "Beta Load.fmw": "2026-01-01T00:00:00+00:00"}
    result, cursor = _run_sensor(respx_mock, fme_resource, "jobs_page_1.json", old)

    keys = sorted(e.asset_key.to_user_string() for e in result.asset_events)
    assert keys == [
        "fme/Beta__Load__fmw/workspace__fmw",
        "fme/alpha__fmw/table_a",
        "fme/alpha__fmw/workspace__fmw",
    ]
    assert cursor == {
        "alpha.fmw": "2026-01-02T10:05:00+00:00",
        "Beta Load.fmw": "2026-01-02T11:01:00+00:00",
    }


@pytest.mark.parametrize("cursor_time", ["2026-01-02T10:05:00+00:00", "2026-01-03T00:00:00+00:00"])
def test_sensor_equal_or_older_finish_emits_nothing(
    respx_mock: respx.MockRouter, fme_resource: FMEResource, cursor_time: str
) -> None:
    result, cursor = _run_sensor(
        respx_mock, fme_resource, "jobs_page_1.json", {"alpha.fmw": cursor_time}
    )
    assert [e for e in result.asset_events if "alpha__fmw" in e.asset_key.path] == []
    assert cursor["alpha.fmw"] == cursor_time


def test_sensor_ignores_unconfigured_workspaces(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    respx_mock.get(JOBS_URL).mock(side_effect=jobs_responder("jobs_page_2.json"))
    sensor = build_fme_sensor(_workspaces(), "fme_sensor", "fme")
    context = dg.build_sensor_context(
        cursor=json.dumps({"gamma-export.fmw": "2026-01-01T00:00:00+00:00"}),
        resources={"fme": fme_resource},
    )
    result = sensor(context)
    assert isinstance(result, dg.SensorResult)
    assert result.asset_events == []
    assert "gamma-export.fmw" not in parse_cursor(context.cursor)


def test_sensor_definition_settings() -> None:
    sensor = build_fme_sensor(_workspaces(), "my_sensor", "my_fme", minimum_interval_seconds=120)
    assert sensor.name == "my_sensor"
    assert sensor.minimum_interval_seconds == 120
    assert sensor.required_resource_keys == {"my_fme"}


# --- known bugs (fixed in Phase 3) ----------------------------------------------------


@pytest.mark.xfail(
    strict=True, reason="BUG-1: workspaces missing from the response lose their cursor"
)
def test_sensor_carries_forward_unseen_workspaces(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    respx_mock.get(JOBS_URL).mock(side_effect=jobs_responder("jobs_page_2.json"))
    sensor = build_fme_sensor(_workspaces(), "fme_sensor", "fme")
    prior = {"alpha.fmw": "2026-01-01T00:00:00+00:00"}
    context = dg.build_sensor_context(cursor=json.dumps(prior), resources={"fme": fme_resource})
    result = sensor(context)
    assert isinstance(result, dg.SensorResult)
    assert result.cursor is not None
    assert json.loads(result.cursor) == prior


@pytest.mark.xfail(strict=True, reason="BUG-3: raw job dict passed as materialization metadata")
def test_materialization_metadata_is_typed() -> None:
    latest = get_latest_time_for_responses(_items("jobs_page_1.json"))
    events, _ = evaluate_jobs(
        {"alpha.fmw": "2026-01-01T00:00:00+00:00"}, latest, [build_fme_assets("alpha.fmw")]
    )
    md = events[0].metadata
    assert md["fme_job_id"] == dg.MetadataValue.int(101)
    assert md["workspace"] == dg.MetadataValue.text("alpha.fmw")
    assert md["repository"] == dg.MetadataValue.text("Samples")
    assert md["status"] == dg.MetadataValue.text("success")
    assert isinstance(md["time_started"], dg.TimestampMetadataValue)
    assert isinstance(md["time_finished"], dg.TimestampMetadataValue)
    assert isinstance(md["fme_job"], dg.JsonMetadataValue)


@pytest.mark.xfail(strict=True, reason="BUG-9: sensor reads resources.original_resource_dict")
def test_sensor_uses_public_resource_access() -> None:
    assert "original_resource_dict" not in inspect.getsource(sensor_module)
