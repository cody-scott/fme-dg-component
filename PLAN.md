# PLAN.md: Rebuild `fme/` into the `dagster-fme` uv package

Status: **in progress** (Phases 0–3 done). Tick boxes as you go. Conventions are in [`AGENTS.md`](AGENTS.md).

## Goal

Turn the loose `fme/` module, which was lifted from the `snowflake_analytics` monorepo, into a self-contained,
tested, typed uv package called `dagster-fme` (import `dagster_fme`). The package must be discoverable by the
`dg` CLI. It also fixes the known bugs listed below, and a test proves each fix.

Out of scope: publishing or release automation (the package is for internal use, built in isolation for now)
and handling failed FME jobs (the sensor stays success-only).

## Decisions (agreed with user)

| Topic | Decision |
|---|---|
| Names | dist `dagster-fme`, import `dagster_fme` |
| HTTP base class | Self-contained `FMEResource(dg.ConfigurableResource)`. Drop the `snowflake_analytics...API` dependency |
| Versions | Python `>=3.10`, `dagster>=1.11` |
| Build | `uv` project, `src/` layout, `uv_build` backend |
| Bugs | Fix all of them. A test proves each fix (xfail during port, then flipped) |
| Job asset leaf | Configurable `job_asset_name`, default `"workspace__fmw"` |
| Default key prefix | `["fme"]` everywhere (component and builder) |
| Job fetching | Filter server-side so only recent or needed jobs are fetched (see Phase 4) |
| Resource wiring | Optional in-component resource (`host` / `api_key` in YAML), otherwise use the external `resource_key` |
| HTTP tests | `respx` mocks only |
| Tooling | ruff (lint+format), pyright, pre-commit, GitHub Actions CI |
| dg integration | Registry entry point and a custom scaffolder that writes a starter `defs.yaml` |
| Failed jobs | Not handled (success only) |
| Old `fme/` dir | Deleted after the port |
| Git | Initial import commit on `main`, then one `phase-<n>/<slug>` branch and PR to `main` per phase (see `AGENTS.md`) |

## Known bugs in the original code

| ID | Where | Problem | Intended fix |
|---|---|---|---|
| BUG-1 | `fme_sensor.py` | The cursor merge loop (carrying forward workspaces not in this tick's response) runs **after** `update_cursor`, so it does nothing. Workspaces missing from the latest response lose their cursor and silently re-baseline later. | Merge before persisting. Return the cursor via `SensorResult(cursor=...)`. |
| BUG-2 | `fme_component.py` vs `fme_assets.py` | The default `key_prefix` is `["FME"]` in the component but `["fme"]` in the builder. | A single default `["fme"]`. |
| BUG-3 | `fme_sensor.py` | The raw FME job dict, which contains nested objects, is passed as `metadata=`. Dagster may reject it or render it badly. | Use typed metadata: `fme_job_id`, `workspace`, `repository`, `status`, `time_started`, `time_finished` (timestamp), `duration` if derivable, and `fme_job` as `MetadataValue.json(full_payload)`. |
| BUG-4 | `fme_resource.py` | Pagination stops only when the response `offset >= totalCount`, so it always makes one extra empty request. | Stop when `offset + len(items) >= totalCount` or when `items` is empty. |
| BUG-5 | multiple | Mutable default args (`key_prefix=["fme"]`, `asset_list=[]`). | Use `None` plus normalisation, or `default_factory`. |
| BUG-6 | `fme_assets.py`, `fme_sensor.py` | Stale SSIS/MSSQL docstrings and comments. | Rewrite them for FME. |
| BUG-7 | `fme_resource.py` | `max_depth=20` and `limit=1000` are hard-coded, and exceeding the cap raises `RecursionError`. | Make `page_size` / `max_pages` resource fields, and raise a descriptive custom error (or log and stop). |
| BUG-8 | `fme_component.py` | Two specs with the same `workspace` silently overwrite each other in the sensor's lookup dict. | Validate uniqueness at component load and raise a clear error. |
| BUG-9 | `fme_sensor.py` | The resource is read via `context.resources.original_resource_dict[...]`, which is an internal detail. | Use `getattr(context.resources, resource_key)`. |

Behaviour that is **intentional** and stays: the first time a workspace is seen, the sensor records its
`timeFinished` as a baseline and emits nothing. Materializations come only from strictly newer finishes.
Pin this with a test.

---

## Phase 0: Repository bootstrap

- [x] Add `.gitignore` (Python, `__pycache__/`, `.venv/`, `.pytest_cache/`, `.ruff_cache/`, `.coverage`, `dist/`).
- [x] Rename branch `master` to `main`. Make the initial commit on `main`: the original `fme/` (no `__pycache__`), `AGENTS.md`, `CLAUDE.md`, `PLAN.md`, `.gitignore`.
- [x] Create and switch to branch `rebuild/uv-package`. (Superseded: from Phase 1 on, each phase uses its own `phase-<n>/<slug>` branch and PR.)

## Phase 1: uv project skeleton and tooling

- [x] `uv init --lib --build-backend uv --name dagster-fme`. Confirm it creates `src/dagster_fme/` and `py.typed`.
- [x] `pyproject.toml`: `requires-python = ">=3.10"`, deps `dagster>=1.11`, `httpx`. Add a description, a README, and the classifiers.
- [x] Dev deps (`[dependency-groups] dev`): `pytest`, `pytest-cov`, `respx`, `ruff`, `pyright`, `pre-commit`, plus `dagster-dg-cli` if it's needed to test scaffolding (deferred to Phase 5). Notes: `pyright[nodejs]` because the system Node (v10) is too old. (`pre-commit` was briefly capped `<4.6` for git 2.25; git is now 2.50 so the cap is removed.)
- [x] Ruff config: line length 100, target `py310`, rules `E,F,I,UP,B,SIM,RUF`, `ruff format`.
- [x] Pyright config: `include = ["src", "tests"]`, `typeCheckingMode = "standard"`, `pythonVersion = "3.10"`.
- [x] Pytest config: `testpaths = ["tests"]`, `addopts = "-ra --strict-markers"`, `xfail_strict = true`.
- [x] `.pre-commit-config.yaml`: ruff (check + format), basic hooks (trailing whitespace, EOF, check-yaml/toml), and a local hook for pyright via `uv run`.
- [x] `.github/workflows/ci.yml` (landed after Phase 2 once the token had `workflow` scope): use `astral-sh/setup-uv`, matrix Python 3.10–3.13, steps `uv sync --locked`, ruff check, ruff format `--check`, pyright, pytest with coverage. Add a separate job with `--resolution lowest-direct` to prove the `dagster>=1.11` floor.
- [x] Commit `uv.lock`.
- [x] Gate: `uv sync && uv run pytest` runs (no tests yet is fine), and lint and pyright are clean.
- [x] Commit: `Phase 1: uv project skeleton and tooling`.

## Phase 2: Port the code, keep behaviour, and pin current behaviour with tests

This phase only moves and decouples the code. It doesn't fix bugs, except where decoupling forces a change.

- [x] `resource.py`: `FMEResource(dg.ConfigurableResource)` with `host: str`, `api_key: str`,
      `api_path: str = "/fmeapiv4"`, and `timeout: float`. Implement `get_request` and `request_offset_paginated`
      using `httpx.Client`: auth header `Authorization: fmetoken token=<key>`, `raise_for_status()`.
      Normalise any trailing slash on `host`.
- [x] `assets.py`: port `FMEAsset` to `FMEWorkspace` and port `build_fme_assets`. Fix the imports.
- [x] `sensor.py`: port `build_asset_sensor` to `build_fme_sensor`, plus `get_fme_data` and
      `get_latest_time_for_responses`. Split out pure helpers.
- [x] `component.py`: port `FME` to `FMEComponent` and `FMEAssetSpec` to `FMEWorkspaceSpec`. Use the new imports.
- [x] `__init__.py`: export `FMEResource`, `FMEWorkspace`, `build_fme_assets`, `build_fme_sensor`,
      `FMEComponent`, and `FMEWorkspaceSpec`. No back-compat aliases (user decision).
- [x] Fixtures: `tests/fixtures/jobs_page_1.json`, `jobs_page_2.json`, `jobs_empty.json`, `job_missing_workspace.json`,
      `job_missing_time_finished.json`, and a fixture with multiple runs of the same workspace.
- [x] Tests that pin current, correct behaviour (these pass):
  - [x] `clean_workspace` on spaces, dots, dashes, unicode and already-clean names.
  - [x] Asset key shapes for the job asset and child assets, and `include_job_asset=False`.
  - [x] `asset_spec_kwargs` reaches every spec (e.g. `group_name`, `owners`).
  - [x] Pagination sends the auth header and correct `limit`/`offset`, and yields every page.
  - [x] `get_latest_time_for_responses`: picks the latest per workspace, handles `Z` and offset timezones, and raises on a missing workspace or `timeFinished`.
  - [x] Sensor: first sight sets a baseline with no events, a newer finish emits events for every spec, and an equal or older finish emits nothing.
  - [x] Sensor ignores workspaces that aren't configured.
  - [x] Component loads from YAML and builds the expected asset keys and one sensor with the configured name and interval.
- [x] `xfail(strict=True)` tests that describe the **corrected** behaviour for BUG-1, 2, 3, 4, 7, 8 and 9.
- [x] Gate: the suite is green (passing tests plus expected xfails), and lint and pyright are clean.
- [x] Notes from the port:
  - `FMEResource.timeout` defaults to `30.0` (user decision). `api_host` was renamed to `api_path`.
  - The sensor closure takes `**_resources`, because Dagster's direct `sensor(context)` invocation passes every
    `required_resource_keys` entry as a kwarg. The legacy closure couldn't be invoked directly, but daemon ticks were unaffected.
  - BUG-3 can't fail today, because Dagster turns a raw nested dict into `JsonMetadataValue`. Its xfail asserts the target typed keys.
  - Component tests build a throwaway dg project and call `dg.load_from_defs_folder(project_root=...)`. That's the only
    helper common to 1.11 and 1.13: `create_defs_folder_sandbox` and `ComponentLoadContext.for_test` each exist in only one.
  - Verified at the floor: `uv sync --resolution lowest-direct` (dagster 1.11.0, httpx 0.27.0) passes. That command
    rewrites `uv.lock`, so run `git checkout uv.lock && uv sync --locked` afterwards.
- [x] Commit: `Phase 2: port fme module into dagster_fme with characterization tests`.

## Phase 3: Fix the bugs

Do one bug per commit. In each commit, remove that bug's xfail marker so it becomes a passing test.

- [x] BUG-1: cursor carry-forward, returned via `SensorResult(cursor=...)`.
- [x] BUG-2: unified default `key_prefix=["fme"]`.
- [x] BUG-3: typed materialization metadata.
- [x] BUG-4: pagination termination with no extra request (assert on the respx call count).
- [x] BUG-5: no mutable defaults (also enable ruff `B006`).
- [x] BUG-6: rewritten docstrings.
- [x] BUG-7: configurable `page_size` / `max_pages`, and a clear error type when the cap is hit.
- [x] BUG-8: duplicate-workspace validation in the component.
- [x] BUG-9: public resource access in the sensor.
- [x] Record each fix in `CHANGELOG.md` under `0.1.0`.
- [x] Notes from the fixes:
  - BUG-1: the carry-forward covers **configured** workspaces only. Cursor entries for workspaces that are no longer
    configured are dropped, as the pre-existing `test_sensor_ignores_unconfigured_workspaces` already pinned.
  - BUG-3: the duration key is `duration_seconds` (float), taken from `elapsedTime` (ms) and falling back to finish
    minus start. The new pure helper `build_materialization_metadata` builds the metadata.
  - BUG-7: `page_size` (1–1000, default 1000) and `max_pages` (≥1, default 20). An explicit `limit` in `params`
    still overrides `page_size`. `FMEPaginationLimitError` (a `RuntimeError`) is exported.
  - BUG-8: Dagster wraps the validator's `ValueError`, so the test searches the exception chain for the message.
  - Gate: 47 tests, 100% line and branch coverage, `lowest-direct` (dagster 1.11.0) passes.

## Phase 4: Feature changes

- [ ] **Configurable job asset leaf**: add `job_asset_name: str = "workspace__fmw"` to `FMEWorkspaceSpec` and
      `build_fme_assets`. Test the default and an override.
- [ ] **Server-side job filtering.** Start by checking which query params FME Flow v4 `GET /jobs` supports
      (e.g. status, workspace/repository filter, sort field and direction, date range). Write the findings in
      *Open questions* below. Then implement the most efficient option that's supported, in this order of preference:
  1. Per-configured-workspace query: `status=success`, filtered to that workspace, sorted by finish time
     descending, `limit=1`. This makes one small request per workspace.
  2. A single query sorted by finish time descending, paginating only until every job is older than the
     oldest cursor value (early stop).
  3. Fall back to the current fetch-all, with the configurable cap from BUG-7.

  Keep the strategy behind a resource method such as `get_latest_successful_jobs(workspaces) -> dict[str, Job]`
  so the sensor doesn't depend on which strategy is used. Test every strategy you implement with respx, asserting
  on the request params and call count.
- [ ] **Optional in-component resource**: add optional `host` and `api_key` (plus optional
      `page_size`/`max_pages`) fields to `FMEComponent`. If `host` and `api_key` are both set, add
      `resources={resource_key: FMEResource(...)}` to the returned `Definitions`. If neither is set, register no
      resource and expect an external one. If only one is set, fail validation. Test all three cases. The
      YAML example must use env templating for `api_key` (check the exact syntax for the installed Dagster,
      e.g. `"{{ env.FME_API_KEY }}"`).
- [ ] **`build_sensor` flag**: the original component declares `build_sensor: bool` but ignores it.
      Implement it so that `False` omits the sensor. Test it.

## Phase 5: dg CLI integration

- [ ] Add the registry entry point in `pyproject.toml`. Check the exact group name for `dagster>=1.11`
      (expected: `[project.entry-points."dagster_dg_cli.registry_modules"] dagster_fme = "dagster_fme"`).
- [ ] `scaffolder.py`: a `dg.Scaffolder` subclass attached with `@dg.scaffold_with(...)` on `FMEComponent`. It writes a
      `defs.yaml` with one example workspace, a commented-out `host`/`api_key` env template, and the sensor settings.
- [ ] Test: entry-point metadata resolves to `dagster_fme` (via `importlib.metadata`).
- [ ] Test: run the scaffolder into a `tmp_path`. The generated `defs.yaml` must parse and load into
      `Definitions` without errors.
- [ ] Manual smoke check, recorded in the PR description: in a throwaway dg project, `uv add --editable <path>`,
      then `dg list components` shows `dagster_fme.FMEComponent` and `dg scaffold defs dagster_fme.FMEComponent fme` works.

## Phase 6: Docs and cleanup

- [ ] `README.md`: what the package does, install from git, a YAML example (external resource and
      in-component resource), a Python API example, an asset key diagram, how the cursor and baseline work,
      and the dev commands.
- [ ] Component docstring and field descriptions (`Field(description=...)`), so `dg` shows useful help.
- [ ] Delete the original `fme/` directory.
- [ ] Final gate: `uv run pre-commit run --all-files`, pytest coverage of at least 90%, and the lowest-direct job passes.
- [ ] Commit: `Phase 6: docs, remove legacy fme module`.
- [ ] Ask the user before pushing the `phase-6/...` branch or opening its PR.

---

## Open questions and things to verify

Agents: put findings here and don't silently guess.

- [x] FME Flow v4 `/jobs`: which filter and sort params exist? What are the exact field names (`workspace`,
      `repository`, `timeFinished`, `timeStarted`, `status`, `id`)? Is `timeFinished` always UTC with `Z`?
  - Source: the OpenAPI 3.0 spec behind the v4 docs, `https://docs.safe.com/fme/html/fmeapiv4/docs/schema/static.json`
    (read 2026-11-17).
  - Response `JobPage`: `items`, `totalCount`, `limit`, `offset`. Each item is a `JobSummaryResponse` with `id` (int64),
    `workspace`, `repository`, `status`, `timeQueued`/`timeStarted`/`timeFinished` (`format: date-time`), `elapsedTime`
    (ms), `cpuTime`, `engineName`, `sourceType`, `warningCount`, `errorCount` and more. No field is marked required.
  - `status` values: `queued`, `running`, `success`, `failure`, `cancelled`. The `status` query param is an array
    ("jobs matching any of the specified statuses").
  - Filters: `status`, `repository`, `workspace` ("if specified **along with repository**"), `runtimeUserID`,
    `engineName`, `sourceType`, `sourceID`, `queue`. There's no date-range filter.
  - `sort`: `<field>_asc|_desc` on `workspace`, `timeFinished`, `timeStarted` or `status`. The default is `timeFinished`
    descending. `limit` is 0–1000 (default 100), `offset` is ≥0.
  - Still open: the spec only says `date-time` and doesn't state a timezone, so the parser keeps accepting `Z` and offsets.
  - Implication for Phase 4: strategy 1 (per-workspace `limit=1`) needs the **repository** as well as the workspace,
    which the spec doesn't currently carry. Strategy 2 (default sort `timeFinished_desc`, early stop) needs nothing new.
- [ ] Are workspace names unique across FME repositories? If not, should matching use `repository/workspace`?
      Ask the user before changing the match key, because it changes the cursor format.
- [x] Do the monorepo callers need back-compat aliases (`FME`, `FMEAssetSpec`, `build_asset_sensor`)? No (user decision).
- [ ] What is the exact env-templating syntax and component-testing helper for the Dagster floor (1.11) compared with the latest?
  - Testing helper: settled, see the Phase 2 notes. Env templating is still to check in Phase 4.
