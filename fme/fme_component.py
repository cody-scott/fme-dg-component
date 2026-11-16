import dagster as dg
from snowflake_analytics.libs.fme.fme_assets import build_fme_assets
from snowflake_analytics.libs.fme.fme_sensor import build_asset_sensor


class FMEAssetSpec(dg.Model):
    workspace: str
    key_prefix: list[str] = ["FME"]
    asset_list: list[str] = []
    include_job_asset: bool = True


class FME(dg.Component, dg.Model, dg.Resolvable):
    """Component for connecting to FME jobs"""

    resource_key: str = "fme"

    fme_asset_specs: list[FMEAssetSpec]

    build_sensor: bool = True
    sensor_name: str = "fme_sensor"
    sensor_check_minutes: int = 30

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Add definition construction logic here.
        all_assets = []
        for spec in self.fme_asset_specs:
            fme_assets = build_fme_assets(
                spec.workspace,
                spec.key_prefix,
                spec.asset_list,
                include_job_asset=spec.include_job_asset,
            )
            all_assets.append(fme_assets)

        all_specs = [__ for _ in all_assets for __ in _.asset_specs]

        sensor = build_asset_sensor(
            all_assets,
            self.sensor_name,
            self.resource_key,
            self.sensor_check_minutes * 60,
        )

        return dg.Definitions(assets=all_specs, sensors=[sensor])
