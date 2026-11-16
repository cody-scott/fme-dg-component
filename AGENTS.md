# AGENTS.md

Guidance for coding agents working in this repository. This file is for **durable conventions**.
The active rebuild work is tracked in [`PLAN.md`](PLAN.md). Read both before you change anything.

## What this project is

`dagster-fme` (import name `dagster_fme`) is a Dagster **Component** that represents
[FME Flow](https://fme.safe.com/fme-flow/) workspaces as Dagster assets. It has three parts:

- **FMEResource**: a thin `httpx` client for the FME Flow REST API v4 (`{host}/fmeapiv4`), using token auth.
- **Asset specs**: one optional "job asset" per FME workspace, plus any number of downstream
  table/dataset assets nested under the workspace's key.
- **Sensor**: polls FME for successful jobs and emits `AssetMaterialization` events for a workspace's
  assets when a job finishes that is newer than the one recorded in the sensor cursor.

The package doesn't run FME jobs. It only observes them.

## Environment and commands

All commands go through `uv`. Don't call `pip`, `python -m venv` or the system `python3`, which is 3.8 on this machine and too old.

| Task | Command |
|---|---|
| Install / sync dev env | `uv sync` |
| Run tests | `uv run pytest` |
| Tests with coverage | `uv run pytest --cov=dagster_fme --cov-report=term-missing` |
| Lint | `uv run ruff check .` |
| Format | `uv run ruff format .` |
| Type check | `uv run pyright` |
| All pre-commit hooks | `uv run pre-commit run --all-files` |
| Lowest supported deps | `uv sync --resolution lowest-direct && uv run pytest` |
| Add a runtime dep | `uv add <pkg>` |
| Add a dev dep | `uv add --dev <pkg>` |

A change is done only when lint, format check, pyright and pytest all pass.

## Supported versions

- Python `>=3.10` (CI matrix: 3.10, 3.11, 3.12, 3.13)
- `dagster>=1.11` (Components API), `httpx`
- Don't use a Dagster API that only exists in versions after the floor unless you also raise the floor in
  `pyproject.toml` and record why in `CHANGELOG.md`. The `lowest-direct` CI job enforces this.

## Layout (target)

```
pyproject.toml          # uv_build backend, deps, ruff/pyright/pytest config, dg entry point
src/dagster_fme/
  __init__.py           # public API re-exports + __all__
  py.typed
  resource.py           # FMEResource (dg.ConfigurableResource, self-contained)
  assets.py             # FMEWorkspace (formerly FMEAsset) + build_fme_assets
  sensor.py             # build_fme_sensor + pure helpers (parse/aggregate/cursor logic)
  component.py          # FMEComponent + FMEWorkspaceSpec models
  scaffolder.py         # dg scaffolder that writes a starter defs.yaml
tests/
  conftest.py
  fixtures/             # FME API JSON fixtures (sanitised, no real hosts/tokens)
  test_resource.py
  test_assets.py
  test_sensor.py
  test_component.py
  test_scaffold.py
.github/workflows/ci.yml
.pre-commit-config.yaml
CHANGELOG.md
README.md
```

The package must not import `snowflake_analytics` or any other monorepo code.

## Coding conventions

- Fully type-annotate public functions. Pyright must pass in `standard` mode or stricter.
- No mutable default arguments. Use `None` and normalise, or `Field(default_factory=...)` in models.
- Keep the sensor's decision logic in **pure functions** (job JSON in, then cursor and events out), so it can be
  unit-tested without Dagster contexts or HTTP. The `@dg.sensor` closure should only wire those functions together.
- Sensors return `dg.SensorResult(asset_events=..., cursor=...)`. Don't call `context.update_cursor`.
- Cursor format is JSON `{"<workspace name>": "<ISO-8601 timeFinished with tz>"}`. Changing it is a
  breaking change and needs a backwards-compatible reader plus a CHANGELOG entry.
- Asset key shape is a public contract:
  `key_prefix + [clean_workspace(workspace)] + [job_asset_name]` for the job asset, and
  `key_prefix + [clean_workspace(workspace)] + [asset]` for the child assets. Default `key_prefix=["fme"]`,
  default `job_asset_name="workspace__fmw"`. Don't change these defaults.
- `clean_workspace` replaces every char outside `[A-Za-z0-9_]` with `__`. The **raw** workspace name is
  still what's matched against FME API responses.
- Materialization metadata must be valid Dagster metadata. Use typed values (`MetadataValue.*`), never raw
  nested dicts.
- Never log or embed the API key. `api_key` is a secret, so in YAML it comes from env templating.
- Docstrings use Google style. Don't leave SSIS or MSSQL wording copied from older code.

## Testing conventions

- `pytest` with tests in `tests/`. Use `respx` to mock **all** HTTP. Tests must never hit the network.
- Store FME responses as JSON fixtures in `tests/fixtures/`. Build them from the documented FME v4 `/jobs`
  response shape (`items`, `totalCount`, `offset`, `limit`). Fixtures must not contain real hostnames,
  tokens or workspace names from production.
- Test the sensor with `dg.build_sensor_context(cursor=..., resources={...})` and call the sensor
  directly. Assert on the returned `SensorResult` (events and cursor).
- Test the component by loading it from YAML through Dagster's component testing utilities, so the test
  covers the YAML-to-Definitions path that users hit. Check the installed Dagster version for the
  current helper (e.g. `dagster.components.testing`).
- A bug fix lands with a test that failed before the fix. While porting, mark known-buggy behaviour
  `@pytest.mark.xfail(strict=True, reason="BUG-n: ...")` and remove the marker in the commit that fixes it.
- Coverage target: at least 90% line coverage on `src/dagster_fme`.

## Git workflow

- `main` holds the initial import. All rebuild work happens on the `rebuild/uv-package` branch.
- Make one commit per completed phase in `PLAN.md`, or smaller commits within a phase. Each commit should leave the tree passing.
- Commit messages use the imperative mood and reference the phase or bug ID (e.g. `Fix BUG-1: preserve cursor for unseen workspaces`).
- Don't push, open PRs, or rewrite published history without explicit approval from the user.

## When unsure

- If you're unsure about FME Flow API behaviour (params, sort/filter support, field names), check the FME Flow
  REST API v4 docs. Don't guess. If you still can't settle it, write it under **Open questions** in
  `PLAN.md` and pick the conservative behaviour.
- Ask the user before changing any public contract (asset keys, cursor format, component YAML schema,
  resource field names).
