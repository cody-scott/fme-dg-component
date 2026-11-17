# dagster-fme

A Dagster Component that represents [FME Flow](https://fme.safe.com/fme-flow/) workspaces as
Dagster assets. A sensor watches FME Flow for successful jobs and records a materialization for a
workspace's assets each time one of its jobs finishes.

The package doesn't run FME jobs. It only observes them, through the FME Flow REST API v4
(`{host}/fmeapiv4`) with token auth.

It has three parts:

- **`FMEResource`**: a small `httpx` client for the FME Flow v4 API.
- **Asset specs**: an optional *job asset* for each workspace, plus any number of downstream
  assets (tables, datasets) nested under the workspace's key.
- **Sensor**: polls `/jobs` for successful jobs and emits `AssetMaterialization` events when a
  workspace finishes a job newer than the one recorded in the sensor cursor.

Requires Python 3.10+ and `dagster>=1.11`.

## Install

From git, into a uv-managed Dagster project:

```bash
uv add "dagster-fme @ git+https://github.com/cody-scott/fme-dg-component"
```

The package registers itself with the `dg` CLI, so after installing:

```bash
dg list components                                  # shows dagster_fme.FMEComponent
dg scaffold defs dagster_fme.FMEComponent fme       # writes defs/fme/defs.yaml
```

## Configure with YAML

### With an external resource

Define an `FMEResource` elsewhere in your project under the key the component names
(`resource_key`, default `fme`):

```python
# defs/resources.py
import dagster as dg
from dagster_fme import FMEResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "fme": FMEResource(
                host="https://fme.example.com",
                api_key=dg.EnvVar("FME_API_KEY"),
            )
        }
    )
```

```yaml
# defs/fme/defs.yaml
type: dagster_fme.FMEComponent

attributes:
  resource_key: fme
  sensor_name: fme_sensor
  sensor_check_minutes: 30

  fme_asset_specs:
    - workspace: load_parcels.fmw
      asset_list: [parcels, parcel_owners]
    - workspace: Roads Export.fmw
      include_job_asset: false
      asset_list: [roads]
```

### With an in-component resource

Set `host` and `api_key` and the component registers the resource itself, under `resource_key`.
Always supply `api_key` from the environment:

```yaml
type: dagster_fme.FMEComponent

attributes:
  host: https://fme.example.com
  api_key: "{{ env('FME_API_KEY') }}"
  # Optional tuning for the job scan:
  # page_size: 1000
  # max_pages: 20

  fme_asset_specs:
    - workspace: load_parcels.fmw
      asset_list: [parcels, parcel_owners]
```

Use `{{ env('FME_API_KEY') }}`, the function form. It fails when the variable is unset, while
`{{ env.FME_API_KEY }}` quietly resolves to nothing. `host` and `api_key` must be set together,
and `page_size` and `max_pages` are only accepted alongside them.

### Fields

| Field | Default | Description |
|---|---|---|
| `fme_asset_specs` | required | The workspaces to represent. Each `workspace` may appear only once. |
| `resource_key` | `fme` | Key of the `FMEResource` the sensor uses. |
| `host`, `api_key` | unset | Define the resource in the component (set both or neither). |
| `page_size`, `max_pages` | `1000`, `20` | Job scan tuning, only with `host`/`api_key`. |
| `build_sensor` | `true` | Set `false` to build the assets without a sensor. |
| `sensor_name` | `fme_sensor` | Name of the sensor. |
| `sensor_check_minutes` | `30` | Minimum minutes between sensor ticks. |

Each entry in `fme_asset_specs`:

| Field | Default | Description |
|---|---|---|
| `workspace` | required | Workspace name exactly as FME Flow reports it, e.g. `load_parcels.fmw`. |
| `key_prefix` | `[fme]` | Leading asset key components. |
| `asset_list` | `[]` | Downstream assets the workspace writes. |
| `include_job_asset` | `true` | Whether to add an asset for the workspace itself. |
| `job_asset_name` | `workspace__fmw` | Last key component of the job asset. |
| `repository` | unset | FME Flow repository of the workspace. When set, the sensor looks it up with one direct request instead of the shared scan (see below). |

`dg` shows the same descriptions as field help.

## Use from Python

```python
import dagster as dg
from dagster_fme import FMEResource, build_fme_assets, build_fme_sensor

parcels = build_fme_assets(
    "load_parcels.fmw",
    asset_list=["parcels", "parcel_owners"],
    asset_spec_kwargs={"group_name": "fme"},
)
roads = build_fme_assets("Roads Export.fmw", asset_list=["roads"], include_job_asset=False)

defs = dg.Definitions(
    assets=[*parcels.asset_specs, *roads.asset_specs],
    sensors=[
        build_fme_sensor(
            [parcels, roads],
            sensor_name="fme_sensor",
            fme_resource_key="fme",
            minimum_interval_seconds=30 * 60,
        )
    ],
    resources={
        "fme": FMEResource(host="https://fme.example.com", api_key=dg.EnvVar("FME_API_KEY")),
    },
)
```

`asset_spec_kwargs` is passed to every `AssetSpec` the workspace builds (for example
`group_name`, `owners` or `tags`).

## Asset keys

The workspace name is cleaned for the key: every character outside `[A-Za-z0-9_]` becomes `__`.
The sensor still matches the **raw** name against FME Flow.

```
key_prefix          cleaned workspace         leaf
──────────          ─────────────────         ────────────────
fme         /       load_parcels__fmw    /    workspace__fmw     ← job asset (job_asset_name)
fme         /       load_parcels__fmw    /    parcels            ← asset_list entry
fme         /       load_parcels__fmw    /    parcel_owners      ← asset_list entry
fme         /       Roads__Export__fmw   /    roads              ← (job asset left out)
```

So the job asset is `key_prefix + [clean(workspace), job_asset_name]` and each downstream asset
is `key_prefix + [clean(workspace), asset]`.

## How the sensor works

On each tick the sensor asks FME Flow for the latest successful job of each workspace, in two ways:

- **Shared scan** for workspaces without `repository`: one newest-first read of
  `GET /jobs?status=success&sort=timeFinished_desc`. It stops paging as soon as it has found the
  latest job for every such workspace, or has scanned past the time already recorded for it. It
  reads at most `page_size × max_pages` jobs (20,000 at the defaults).
- **Direct lookup** for workspaces with `repository`: one request each,
  `GET /jobs?status=success&repository=<repo>&workspace=<name>&sort=timeFinished_desc&limit=1`
  (FME Flow only filters on `workspace` together with `repository`). This costs one request no
  matter how busy the server is, so set it for workspaces that run rarely. Matching and the
  cursor still use the workspace name alone.

The cursor is JSON mapping each raw workspace name to the `timeFinished` of the last job it
acted on:

```json
{"load_parcels.fmw": "2026-11-17T08:30:12+00:00", "Roads Export.fmw": "2026-11-16T22:01:45+00:00"}
```

For each configured workspace:

- **First sight (baseline).** If the cursor has no entry, the latest finish is recorded and
  **nothing is emitted**. Past jobs aren't replayed as materializations. The first
  materialization comes from the next job that finishes after the sensor starts watching.
- **First sight, not found by the scan.** If the shared scan reads every job, or reaches
  `max_pages`, without finding the workspace, the newest finish the scan saw is recorded as its
  baseline and a warning is logged. Its next real job then emits, and later ticks stop early
  instead of re-reading every page. A misspelled name shows up as that warning, once. (If the
  server didn't return jobs newest first and the scan stopped at the cap, no baseline is
  recorded.) A workspace with `repository` that has no job gets no baseline; its first job
  becomes the baseline as usual.
- **Newer finish.** If the latest `timeFinished` is strictly newer than the cursor, the sensor
  emits a materialization for every asset of that workspace and advances the cursor.
- **Same or older finish, or no job found.** Nothing is emitted and the cursor entry is kept.

Entries for workspaces that are no longer configured are dropped from the cursor. Failed or
cancelled jobs are ignored.

Each materialization carries typed metadata: `fme_job_id`, `workspace`, `repository`, `status`,
`time_started`, `time_finished`, `duration_seconds`, and the full job object as `fme_job`.

If the shared scan reaches `max_pages` while a workspace that already has a cursor entry is
unresolved (it hasn't run within the last `page_size × max_pages` successful jobs), that workspace
is **stalled**: its cursor is kept and an error naming it is logged. Every other workspace still
gets its events, so one idle workspace doesn't block the rest. A newer job for the stalled
workspace beyond the cap is missed until it's in reach, so raise `max_pages` or `page_size`, or
set `repository` on it to switch it to a direct lookup.

## Development

All commands go through [uv](https://docs.astral.sh/uv/):

```bash
uv sync                                                   # install the dev environment
uv run pytest --cov=dagster_fme --cov-report=term-missing # tests with coverage
uv run ruff check . && uv run ruff format --check .       # lint and format check
uv run pyright                                            # type check
uv run pre-commit run --all-files                         # every hook

# Prove the dagster>=1.11 floor, then restore the lockfile
uv sync --resolution lowest-direct && uv run pytest
git checkout uv.lock && uv sync --locked
```

Tests mock all HTTP with `respx` and never touch the network. See [`AGENTS.md`](AGENTS.md) for
the project's conventions and [`CHANGELOG.md`](CHANGELOG.md) for changes.
