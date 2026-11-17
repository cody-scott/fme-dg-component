"""Sensor that turns successful FME jobs into asset materializations.

The decision logic lives in pure functions (job JSON in, events and cursor out) so it
can be tested without Dagster contexts or HTTP. ``build_fme_sensor`` only wires them up.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, TypedDict, cast

import dagster as dg
import httpx

from dagster_fme.assets import FMEWorkspace
from dagster_fme.resource import FMEResource


class LatestJob(TypedDict):
    """The most recent finished job seen for a workspace."""

    id: Any
    time_finished: datetime.datetime
    data: dict[str, Any]


def parse_fme_time(value: str) -> datetime.datetime:
    """Parse an FME ISO-8601 timestamp, accepting a ``Z`` suffix or a UTC offset."""
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_materialization_metadata(job: Mapping[str, Any]) -> dict[str, dg.MetadataValue]:
    """Turn an FME job object into typed Dagster metadata.

    Fields missing from the job are left out. ``duration_seconds`` comes from
    ``elapsedTime`` (milliseconds) when present, otherwise from ``timeFinished``
    minus ``timeStarted``. The full job object is kept under ``fme_job``.

    Args:
        job: One job object from the FME ``/jobs`` endpoint.

    Returns:
        Metadata for an ``AssetMaterialization``.
    """
    metadata: dict[str, dg.MetadataValue] = {}

    job_id = job.get("id")
    if isinstance(job_id, int):
        metadata["fme_job_id"] = dg.MetadataValue.int(job_id)

    for key in ("workspace", "repository", "status"):
        value = job.get(key)
        if value is not None:
            metadata[key] = dg.MetadataValue.text(str(value))

    times: dict[str, datetime.datetime] = {}
    for api_key, md_key in (("timeStarted", "time_started"), ("timeFinished", "time_finished")):
        raw = job.get(api_key)
        if isinstance(raw, str):
            times[md_key] = parse_fme_time(raw)
            metadata[md_key] = dg.MetadataValue.timestamp(times[md_key])

    elapsed_ms = job.get("elapsedTime")
    if isinstance(elapsed_ms, (int, float)) and not isinstance(elapsed_ms, bool):
        metadata["duration_seconds"] = dg.MetadataValue.float(elapsed_ms / 1000)
    elif "time_started" in times and "time_finished" in times:
        duration = times["time_finished"] - times["time_started"]
        metadata["duration_seconds"] = dg.MetadataValue.float(duration.total_seconds())

    metadata["fme_job"] = dg.MetadataValue.json(dict(job))
    return metadata


def parse_cursor(cursor: str | None) -> dict[str, str]:
    """Parse the sensor cursor, ``{"<workspace>": "<ISO-8601 timeFinished>"}``.

    An empty or missing cursor parses to ``{}``.
    """
    if cursor is None or cursor == "":
        return {}
    return json.loads(cursor)


def extract_jobs(pages: Iterable[httpx.Response]) -> list[dict[str, Any]]:
    """Flatten the ``items`` of every ``/jobs`` page into one list."""
    return [item for page in pages for item in page.json().get("items") or []]


def get_latest_time_for_responses(responses: Sequence[dict[str, Any]]) -> dict[str, LatestJob]:
    """Pick the latest-finishing job for each workspace.

    Args:
        responses: Job objects from the FME ``/jobs`` endpoint.

    Returns:
        A mapping of raw workspace name to its latest job.

    Raises:
        ValueError: If a job has no ``workspace`` or no ``timeFinished``.
    """
    output: dict[str, LatestJob] = {}

    for job in responses:
        workspace = job.get("workspace")
        raw_time_finished = job.get("timeFinished")

        if workspace is None:
            dg.get_dagster_logger().error(job)
            raise ValueError("Workspace cannot be None")

        if raw_time_finished is None:
            dg.get_dagster_logger().error(job)
            raise ValueError("Time finished cannot be None")

        time_finished = parse_fme_time(raw_time_finished)
        save: LatestJob = {"id": job.get("id"), "time_finished": time_finished, "data": job}

        if workspace not in output or output[workspace]["time_finished"] < time_finished:
            output[workspace] = save

    return output


def evaluate_jobs(
    cursor: Mapping[str, str],
    latest: Mapping[str, LatestJob],
    fme_assets: Sequence[FMEWorkspace],
) -> tuple[list[dg.AssetMaterialization], dict[str, str]]:
    """Decide which materializations to emit and compute the next cursor.

    A workspace seen for the first time is recorded as a baseline and emits nothing.
    A strictly newer ``timeFinished`` than the cursor emits a materialization for
    every asset spec of that workspace. Configured workspaces that have no job in
    ``latest`` keep their existing cursor value. Cursor entries for workspaces that
    are no longer configured are dropped.

    Args:
        cursor: The parsed current cursor.
        latest: The latest job per workspace, from ``get_latest_time_for_responses``.
        fme_assets: The configured workspaces.

    Returns:
        The materializations to emit and the new cursor.
    """
    fqn_dict = {a.fqn: a for a in fme_assets}

    new_cursor: dict[str, str] = {ws: t for ws, t in cursor.items() if ws in fqn_dict}
    events: list[dg.AssetMaterialization] = []

    for workspace, item in latest.items():
        asset_obj = fqn_dict.get(workspace)
        if asset_obj is None:
            continue

        time_finished = item["time_finished"]
        last_time_str = cursor.get(workspace)

        if last_time_str is None:
            new_cursor[workspace] = time_finished.isoformat()
            continue

        cursor_time = last_time_str
        if time_finished > datetime.datetime.fromisoformat(last_time_str):
            cursor_time = time_finished.isoformat()
            metadata = build_materialization_metadata(item["data"])
            events.extend(
                dg.AssetMaterialization(spec.key, metadata=metadata)
                for spec in asset_obj.asset_specs
            )

        new_cursor[workspace] = cursor_time

    return events, new_cursor


def get_fme_data(fme: FMEResource) -> list[dict[str, Any]]:
    """Fetch every successful job from FME Flow."""
    return extract_jobs(fme.request_offset_paginated("/jobs", params={"status": "success"}))


def get_fme_states(fme: FMEResource) -> dict[str, LatestJob]:
    """Fetch successful jobs and reduce them to the latest job per workspace."""
    return get_latest_time_for_responses(get_fme_data(fme))


def build_fme_sensor(
    fme_assets: Sequence[FMEWorkspace],
    sensor_name: str,
    fme_resource_key: str,
    minimum_interval_seconds: int = 60,
) -> dg.SensorDefinition:
    """
    Return a sensor that checks for the status of FME packages
    Sensor will return materialization events for the assets defined in
    `FMEAsset.asset_list`
    """

    @dg.sensor(
        name=sensor_name,
        minimum_interval_seconds=minimum_interval_seconds,
        required_resource_keys={fme_resource_key},
    )
    def _sensor(context: dg.SensorEvaluationContext, **_resources: Any) -> dg.SensorResult:
        """
        cursor will be fqn of the ssis package + last runtime
        """
        # `**_resources` absorbs the resources Dagster passes as kwargs when the sensor
        # is invoked directly (`sensor(context)`); the resource is read from `context`.
        fme_resource = cast(FMEResource, getattr(context.resources, fme_resource_key))

        cursor = parse_cursor(context.cursor)
        latest = get_fme_states(fme_resource)
        events, new_cursor = evaluate_jobs(cursor, latest, fme_assets)

        return dg.SensorResult(asset_events=events, cursor=json.dumps(new_cursor))

    return _sensor
