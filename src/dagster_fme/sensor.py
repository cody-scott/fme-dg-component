"""Sensor that turns successful FME jobs into asset materializations.

The decision logic lives in pure functions (job JSON in, events and cursor out) so it
can be tested without Dagster contexts or HTTP. ``build_fme_sensor`` only wires them up.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Mapping, Sequence
from typing import Any, cast

import dagster as dg

from dagster_fme.assets import FMEWorkspace
from dagster_fme.resource import FMEResource, parse_fme_time


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


def load_cursor(raw: str | None) -> dict[str, datetime.datetime]:
    """Parse the sensor cursor, ``{"<workspace>": "<ISO-8601 timeFinished>"}``.

    An empty or missing cursor parses to ``{}``. A timestamp with no timezone (e.g. a
    hand-edited cursor) is treated as UTC, so it compares with FME's aware times.
    """
    cursor: dict[str, datetime.datetime] = {}
    for ws, value in json.loads(raw or "{}").items():
        t = parse_fme_time(value)
        cursor[ws] = t if t.tzinfo is not None else t.replace(tzinfo=datetime.timezone.utc)
    return cursor


def dump_cursor(cursor: Mapping[str, datetime.datetime]) -> str:
    """Serialise a cursor as JSON ``{"<workspace>": "<ISO-8601 timeFinished>"}``."""
    return json.dumps({ws: t.isoformat() for ws, t in cursor.items()})


def evaluate_jobs(
    cursor: Mapping[str, datetime.datetime],
    jobs: Mapping[str, Mapping[str, Any]],
    fme_assets: Sequence[FMEWorkspace],
) -> tuple[list[dg.AssetMaterialization], dict[str, datetime.datetime]]:
    """Decide which materializations to emit and compute the next cursor.

    A workspace seen for the first time is recorded as a baseline and emits nothing.
    A strictly newer ``timeFinished`` than the cursor emits a materialization for
    every asset spec of that workspace. Configured workspaces that have no job in
    ``jobs`` keep their existing cursor value. Cursor entries for workspaces that
    are no longer configured are dropped.

    Args:
        cursor: The current cursor, from ``load_cursor``.
        jobs: The latest successful job per raw workspace name, from
            ``FMEResource.get_latest_successful_jobs``.
        fme_assets: The configured workspaces.

    Returns:
        The materializations to emit and the new cursor.
    """
    events: list[dg.AssetMaterialization] = []
    new_cursor: dict[str, datetime.datetime] = {}

    for asset in fme_assets:
        workspace = asset.workspace
        previous = cursor.get(workspace)
        job = jobs.get(workspace)

        if job is None:
            if previous is not None:
                new_cursor[workspace] = previous
            continue

        finished = parse_fme_time(job["timeFinished"])
        if previous is None:
            new_cursor[workspace] = finished
            continue

        if finished > previous:
            metadata = build_materialization_metadata(job)
            events.extend(
                dg.AssetMaterialization(spec.key, metadata=metadata) for spec in asset.asset_specs
            )
            new_cursor[workspace] = finished
        else:
            new_cursor[workspace] = previous

    return events, new_cursor


def build_fme_sensor(
    fme_assets: Sequence[FMEWorkspace],
    sensor_name: str,
    fme_resource_key: str,
    minimum_interval_seconds: int = 60,
) -> dg.SensorDefinition:
    """Build a sensor that materializes FME workspace assets from successful jobs.

    On each tick the sensor asks FME Flow for the latest successful job of each
    workspace, using ``FMEResource.get_latest_successful_jobs``. A workspace's first
    finish is recorded in the cursor as a baseline and emits nothing. A strictly newer
    finish emits an ``AssetMaterialization`` for every asset spec of that workspace.

    Args:
        fme_assets: The workspaces to watch.
        sensor_name: Name of the sensor definition.
        fme_resource_key: Resource key of the ``FMEResource`` to query.
        minimum_interval_seconds: Minimum time between sensor ticks.

    Returns:
        The sensor definition.
    """

    @dg.sensor(
        name=sensor_name,
        minimum_interval_seconds=minimum_interval_seconds,
        required_resource_keys={fme_resource_key},
    )
    def _sensor(context: dg.SensorEvaluationContext, **_resources: Any) -> dg.SensorResult:
        """Materialize workspaces with newer jobs and advance the cursor.

        The cursor is JSON ``{"<workspace>": "<ISO-8601 timeFinished>"}``.
        """
        # `**_resources` absorbs the resources Dagster passes as kwargs when the sensor
        # is invoked directly (`sensor(context)`); the resource is read from `context`.
        fme_resource = cast(FMEResource, getattr(context.resources, fme_resource_key))

        cursor = load_cursor(context.cursor)
        jobs = fme_resource.get_latest_successful_jobs(
            [a.workspace for a in fme_assets], since=cursor
        )
        events, new_cursor = evaluate_jobs(cursor, jobs, fme_assets)

        return dg.SensorResult(asset_events=events, cursor=dump_cursor(new_cursor))

    return _sensor
