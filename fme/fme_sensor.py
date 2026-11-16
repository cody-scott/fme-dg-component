import datetime
import json

from dagster import (
    AssetMaterialization,
    SensorEvaluationContext,
    SensorResult,
    sensor,
    get_dagster_logger,
)

from snowflake_analytics.libs.fme.fme_assets import FMEAsset
from snowflake_analytics.libs.fme.fme_resource import FMEResource

from typing import Sequence, Any


def build_asset_sensor(
    fme_assets: Sequence[FMEAsset],
    sensor_name: str,
    fme_resource_key: str,
    minimum_interval_seconds: int = 60,
):
    """
    Return a sensor that checks for the status of FME packages
    Sensor will return materialization events for the assets defined in
    `FMEAsset.asset_list`
    """

    @sensor(
        name=sensor_name,
        minimum_interval_seconds=minimum_interval_seconds,
        required_resource_keys={fme_resource_key},
    )
    def _sensor(context: SensorEvaluationContext):
        """
        cursor will be fqn of the ssis package + last runtime
        """
        fme_resource: FMEResource = context.resources.original_resource_dict[
            fme_resource_key
        ]  # type: ignore

        fqn_dict = {_.fqn: _ for _ in fme_assets}

        # cursor state is expected to be {workspace: last_time}

        cursor = context.cursor
        if cursor is None or cursor == '':
            cursor = {}
        else:
            cursor = json.loads(cursor)

        new_state = get_fme_states(fme_resource)

        # if the state is none bail out
        if new_state is None:
            context.update_cursor(json.dumps(cursor))
            return

        new_cursor_state = {}

        asset_workspaces = [
            _._workspace for _ in fme_assets
        ]

        materializations_to_trigger = []

        for workspace in new_state:
            # skip objects that we don't need to observe
            if workspace not in asset_workspaces:
                continue

            item_data = new_state[workspace]
            data = item_data["data"]
            time_finished: datetime.datetime = item_data['time_finished']

            fqn = workspace
            

            # get the asset from the provided set
            asset_obj = fqn_dict.get(fqn)
            if asset_obj is None:
                continue
            

            last_time_str = cursor.get(fqn)

            # if nothing is set then we set the workspace key to the current time finished
            if last_time_str is None:
                new_cursor_state[fqn] = time_finished.isoformat()
                continue

            last_time = datetime.datetime.fromisoformat(last_time_str)
            current_time = time_finished

            cursor_time = last_time_str
            if current_time > last_time:
                # add to materializations_to_trigger
                cursor_time = time_finished.isoformat()

                for _asset in asset_obj.asset_specs:
                    asset_materialization = AssetMaterialization(
                        _asset.key,
                        metadata=data,
                    )
                    materializations_to_trigger.append(asset_materialization)

            new_cursor_state[fqn] = cursor_time

        context.update_cursor(json.dumps(new_cursor_state))

        for _ in cursor:
            if _ not in new_cursor_state:
                new_cursor_state[_] = cursor[_]

        return SensorResult(asset_events=materializations_to_trigger)

    return _sensor


def get_fme_states(fme: FMEResource):
    return_set = get_fme_data(fme)
    current_state = get_latest_time_for_responses(return_set)
    return current_state


def get_fme_data(fme: FMEResource):
    results = [
        _ for _ in fme.request_offset_paginated("/jobs", params={"status": "success"})
    ]
    return [__ for _ in results for __ in _.json().get("items") or []]


def get_latest_time_for_responses(responses: list[dict[str, Any]]):
    output = {}

    for _ in responses:
        id = _.get("id")
        time_finished = _.get("timeFinished")
        workspace = _.get("workspace")

        if workspace is None:
            get_dagster_logger().error(_)
            raise ValueError("Workspace cannot be None")

        if time_finished is None:
            get_dagster_logger().error(_)
            raise ValueError("Time finished cannot be None")

        time_finished = datetime.datetime.fromisoformat(
            time_finished.replace("Z", "+00:00")
        )

        save_dict = {"id": id, "time_finished": time_finished, "data": _}

        if workspace in output:
            output_time = output[workspace]["time_finished"]
            if output_time < time_finished:
                output[workspace] = save_dict
        else:
            output[workspace] = save_dict

    return output
