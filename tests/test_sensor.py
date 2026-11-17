"""Tests for the sensor and its pure helpers."""

from __future__ import annotations

import datetime
import inspect
import json
from typing import Any

import dagster as dg
import httpx
import pytest
import respx

import dagster_fme.sensor as sensor_module
from dagster_fme import FMEResource, FMEWorkspace, build_fme_assets, build_fme_sensor
from dagster_fme.resource import JobScan
from dagster_fme.sensor import (
    build_materialization_metadata,
    dump_cursor,
    evaluate_jobs,
    load_cursor,
)
from tests.helpers import JOBS_URL, items_responder, jobs_responder, load_fixture, make_job

UTC = datetime.timezone.utc


def _items(name: str) -> list[dict[str, Any]]:
    return load_fixture(name)["items"]


def _latest(name: str) -> dict[str, dict[str, Any]]:
    """The latest job per workspace in a fixture, as the resource returns it."""
    scan = JobScan(targets=frozenset(j["workspace"] for j in _items(name)), since={})
    scan.add_page(_items(name))
    return scan.found


def _dt(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)


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
    assert result.cursor is not None
    return result, json.loads(result.cursor)


# --- pure helpers ---------------------------------------------------------------------


@pytest.mark.parametrize("raw", [None, ""])
def test_load_cursor_empty(raw: str | None) -> None:
    assert load_cursor(raw) == {}


def test_load_cursor_json() -> None:
    assert load_cursor('{"a": "2026-01-01T00:00:00+00:00"}') == {
        "a": datetime.datetime(2026, 1, 1, tzinfo=UTC)
    }


def test_load_cursor_treats_naive_time_as_utc() -> None:
    cursor = load_cursor('{"a": "2026-01-01T00:00:00"}')
    assert cursor == {"a": datetime.datetime(2026, 1, 1, tzinfo=UTC)}
    assert cursor["a"].tzinfo is not None
    assert dump_cursor(cursor) == '{"a": "2026-01-01T00:00:00+00:00"}'


def test_cursor_round_trips_byte_for_byte() -> None:
    # Written by the pre-Phase-7 sensor: json.dumps of isoformat() strings.
    written = json.dumps(
        {
            "alpha.fmw": "2026-01-04T09:30:00-05:00",
            "Beta Load.fmw": "2026-01-02T11:01:00.250000+00:00",
        }
    )
    assert dump_cursor(load_cursor(written)) == written


def test_evaluate_carries_forward_configured_and_drops_unconfigured() -> None:
    prior = {
        "alpha.fmw": _dt("2026-01-01T00:00:00+00:00"),
        "removed.fmw": _dt("2026-01-01T00:00:00+00:00"),
    }
    events, cursor = evaluate_jobs(prior, {}, _workspaces())
    assert events == []
    assert cursor == {"alpha.fmw": _dt("2026-01-01T00:00:00+00:00")}


def test_evaluate_first_sight_is_baseline_only() -> None:
    events, cursor = evaluate_jobs({}, _latest("jobs_page_1.json"), _workspaces())
    assert events == []
    assert dump_cursor(cursor) == json.dumps(
        {
            "alpha.fmw": "2026-01-02T10:05:00+00:00",
            "Beta Load.fmw": "2026-01-02T11:01:00+00:00",
        }
    )


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
    assert "gamma-export.fmw" not in load_cursor(result.cursor)


def test_sensor_definition_settings() -> None:
    sensor = build_fme_sensor(_workspaces(), "my_sensor", "my_fme", minimum_interval_seconds=120)
    assert sensor.name == "my_sensor"
    assert sensor.minimum_interval_seconds == 120
    assert sensor.required_resource_keys == {"my_fme"}


# --- known bugs (fixed in Phase 3) ----------------------------------------------------


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
    cursor = json.loads(result.cursor)
    # alpha keeps its entry (BUG-1). Beta was never seen, so it gets a baseline (N-3).
    assert cursor["alpha.fmw"] == prior["alpha.fmw"]
    assert cursor["Beta Load.fmw"] == "2026-01-02T12:30:00+00:00"


def test_evaluate_applies_baselines_only_to_first_sight() -> None:
    baseline = _dt("2026-02-01T00:00:00+00:00")
    prior = {"alpha.fmw": _dt("2026-01-01T00:00:00+00:00")}
    events, cursor = evaluate_jobs(
        prior, {}, _workspaces(), {"alpha.fmw": baseline, "Beta Load.fmw": baseline}
    )
    assert events == []
    assert cursor == {"alpha.fmw": prior["alpha.fmw"], "Beta Load.fmw": baseline}


def test_sensor_job_after_baseline_emits(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    # N-3: a workspace baselined without ever being found emits on its first real job.
    sensor = build_fme_sensor(_workspaces(), "fme_sensor", "fme")
    history = _items("jobs_page_2.json")
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(history))

    first = sensor(dg.build_sensor_context(resources={"fme": fme_resource}))
    assert isinstance(first, dg.SensorResult)
    assert first.asset_events == []
    assert set(load_cursor(first.cursor)) == {"alpha.fmw", "Beta Load.fmw"}

    new_job = make_job(900, "Beta Load.fmw", "2026-01-05T00:00:00Z")
    route.side_effect = items_responder([new_job, *history])
    second = sensor(dg.build_sensor_context(cursor=first.cursor, resources={"fme": fme_resource}))
    assert isinstance(second, dg.SensorResult)
    assert [e.asset_key.to_user_string() for e in second.asset_events] == [
        "fme/Beta__Load__fmw/workspace__fmw"
    ]


def test_sensor_looks_up_workspaces_with_repository_directly(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    job = make_job(77, "alpha.fmw", "2026-01-05T00:00:00Z")
    route = respx_mock.get(JOBS_URL).mock(
        return_value=httpx.Response(200, json={"items": [job], "totalCount": 1})
    )
    sensor = build_fme_sensor(
        [build_fme_assets("alpha.fmw", repository="Samples")], "fme_sensor", "fme"
    )
    context = dg.build_sensor_context(
        cursor=json.dumps({"alpha.fmw": "2026-01-01T00:00:00+00:00"}),
        resources={"fme": fme_resource},
    )
    result = sensor(context)
    assert isinstance(result, dg.SensorResult)
    assert [e.asset_key.to_user_string() for e in result.asset_events] == [
        "fme/alpha__fmw/workspace__fmw"
    ]
    assert route.call_count == 1
    assert route.calls[0].request.url.params["repository"] == "Samples"


def test_sensor_stalled_workspace_does_not_block_others(respx_mock: respx.MockRouter) -> None:
    # N-4: before, the stalled workspace raised and nothing was emitted for anyone.
    jobs = [make_job(i, "alpha.fmw", f"2026-01-0{i}T00:00:00Z") for i in range(2, 6)]
    respx_mock.get(JOBS_URL).mock(side_effect=items_responder(jobs))
    resource = FMEResource(host="https://fme.example.com", api_key="k", page_size=1, max_pages=1)
    sensor = build_fme_sensor(_workspaces(), "fme_sensor", "fme")
    old = "2026-01-01T00:00:00+00:00"
    context = dg.build_sensor_context(
        cursor=json.dumps({"alpha.fmw": old, "Beta Load.fmw": old}), resources={"fme": resource}
    )
    result = sensor(context)
    assert isinstance(result, dg.SensorResult)
    assert {e.asset_key.to_user_string() for e in result.asset_events} == {
        "fme/alpha__fmw/workspace__fmw",
        "fme/alpha__fmw/table_a",
    }
    cursor = json.loads(result.cursor or "{}")
    assert cursor == {"alpha.fmw": "2026-01-05T00:00:00+00:00", "Beta Load.fmw": old}


def test_materialization_metadata_is_typed() -> None:
    events, _ = evaluate_jobs(
        {"alpha.fmw": _dt("2026-01-01T00:00:00+00:00")},
        _latest("jobs_page_1.json"),
        [build_fme_assets("alpha.fmw")],
    )
    md = events[0].metadata
    assert md["fme_job_id"] == dg.MetadataValue.int(101)
    assert md["workspace"] == dg.MetadataValue.text("alpha.fmw")
    assert md["repository"] == dg.MetadataValue.text("Samples")
    assert md["status"] == dg.MetadataValue.text("success")
    assert isinstance(md["time_started"], dg.TimestampMetadataValue)
    assert isinstance(md["time_finished"], dg.TimestampMetadataValue)
    assert isinstance(md["fme_job"], dg.JsonMetadataValue)
    assert md["time_finished"].value == datetime.datetime(2026, 1, 2, 10, 5, tzinfo=UTC).timestamp()
    # elapsedTime is 60000 ms in the fixture.
    assert md["duration_seconds"] == dg.MetadataValue.float(60.0)


def test_metadata_duration_falls_back_to_start_and_finish() -> None:
    md = build_materialization_metadata(
        {
            "id": 7,
            "timeStarted": "2026-01-02T10:00:00Z",
            "timeFinished": "2026-01-02T10:05:00+00:00",
        }
    )
    assert md["duration_seconds"] == dg.MetadataValue.float(300.0)
    assert "workspace" not in md


def test_metadata_omits_missing_fields() -> None:
    md = build_materialization_metadata({"workspace": "alpha.fmw"})
    assert set(md) == {"workspace", "fme_job"}


def test_sensor_uses_public_resource_access() -> None:
    assert "original_resource_dict" not in inspect.getsource(sensor_module)


def test_sensor_passes_cursor_to_the_early_stop_scan(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(JOBS_URL).mock(side_effect=jobs_responder("jobs_multiple_runs.json"))
    resource = FMEResource(host="https://fme.example.com", api_key="k", page_size=1)
    sensor = build_fme_sensor(_workspaces(), "fme_sensor", "fme")
    # Both cursors are later than every job, so the first page already passes them.
    future = "2027-01-01T00:00:00+00:00"
    context = dg.build_sensor_context(
        cursor=json.dumps({"alpha.fmw": future, "Beta Load.fmw": future}),
        resources={"fme": resource},
    )
    result = sensor(context)
    assert isinstance(result, dg.SensorResult)
    assert result.asset_events == []
    assert route.call_count == 1
    assert route.calls[0].request.url.params["sort"] == "timeFinished_desc"
