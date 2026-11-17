# PLAN.md: Rebuild `fme/` into the `dagster-fme` uv package

Status: **in progress** (Phases 0–6 done and merged, Phases 7 and 8 merged, Phase 9 in review. Phases 7–10 come from the code review in
[`docs/SPEC.md`](docs/SPEC.md); Phase 10 waits on user decisions). Tick boxes as you go. Conventions are in [`AGENTS.md`](AGENTS.md).

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

- [x] **Configurable job asset leaf**: add `job_asset_name: str = "workspace__fmw"` to `FMEWorkspaceSpec` and
      `build_fme_assets`. Test the default and an override.
- [x] **Server-side job filtering.** Start by checking which query params FME Flow v4 `GET /jobs` supports
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
- [x] **Optional in-component resource**: add optional `host` and `api_key` (plus optional
      `page_size`/`max_pages`) fields to `FMEComponent`. If `host` and `api_key` are both set, add
      `resources={resource_key: FMEResource(...)}` to the returned `Definitions`. If neither is set, register no
      resource and expect an external one. If only one is set, fail validation. Test all three cases. The
      YAML example must use env templating for `api_key` (check the exact syntax for the installed Dagster,
      e.g. `"{{ env.FME_API_KEY }}"`).
- [x] **`build_sensor` flag**: the original component declares `build_sensor: bool` but ignores it.
      Implement it so that `False` omits the sensor. Test it.
- [x] Notes from Phase 4:
  - Job fetching uses **strategy 2** (user decision). `FMEResource.get_latest_successful_jobs(workspaces, since)`
    requests `status=success&sort=timeFinished_desc` and folds pages into a pure `JobScan`. It stops once every
    workspace is found or the scan has gone past that workspace's cursor time (`oldest_seen <= since`). If a page
    arrives out of order, early stop is turned off. Strategy 3 is just strategy 2 without the early stop, so it has
    no separate code path. Strategy 1 is deferred (see Open questions).
  - At `max_pages`: raise `FMEPaginationLimitError` only if a workspace **with** a cursor is unresolved. If only
    first-sight workspaces are left, log a warning and return what was found (user decision).
  - Removed `extract_jobs`, `get_fme_data` and `get_fme_states`. `parse_fme_time` moved to `resource.py`. Jobs with
    no `workspace`, or with no `timeFinished` for an unconfigured workspace, are skipped instead of raising.
  - Env templating: use `"{{ env('FME_API_KEY') }}"` (it raises if unset, on both 1.11 and 1.13). `{{ env.X }}`
    resolves to `None` when unset.
  - Pydantic validation errors include the input, so a resolved `api_key` would leak. `FMEComponent` sets
    `hide_input_in_errors=True`, and `api_key` has `repr=False`. Tests check that the secret isn't in errors or repr.
  - Follow-up for Phase 6: `FMEResource` itself still shows `api_key` in its repr.

## Phase 5: dg CLI integration

- [x] Add the registry entry point in `pyproject.toml`. Check the exact group name for `dagster>=1.11`
      (expected: `[project.entry-points."dagster_dg_cli.registry_modules"] dagster_fme = "dagster_fme"`).
- [x] `scaffolder.py`: a `dg.Scaffolder` subclass attached with `@dg.scaffold_with(...)` on `FMEComponent`. It writes a
      `defs.yaml` with one example workspace, a commented-out `host`/`api_key` env template, and the sensor settings.
- [x] Test: entry-point metadata resolves to `dagster_fme` (via `importlib.metadata`).
- [x] Test: run the scaffolder into a `tmp_path`. The generated `defs.yaml` must parse and load into
      `Definitions` without errors.
- [x] Manual smoke check, recorded in the PR description: in a throwaway dg project, `uv add --editable <path>`,
      then `dg list components` shows `dagster_fme.FMEComponent` and `dg scaffold defs dagster_fme.FMEComponent fme` works.
- [x] Notes from Phase 5:
  - The entry point group `dagster_dg_cli.registry_modules` is the current one in both 1.11.0 and 1.13.
  - The scaffolder writes the YAML as text rather than through `yaml.dump`, so it can keep comments. The commented
    `{{ env('FME_API_KEY') }}` line isn't evaluated: the scaffolded file loads with `FME_API_KEY` unset. A test also
    uncomments the block and checks that the component builds the resource.
  - `ScaffoldRequest.append` (`dg scaffold defs --append`) doesn't exist in 1.11. The scaffolder reads it with
    `getattr(..., False)`, and the append test is skipped at the floor. The `python` format delegates to Dagster's
    `scaffold_component` stub.
  - Tests use two internal helpers, `discover_entry_point_package_objects` and `get_scaffolder`, to check what `dg`
    sees. Both exist in 1.11.0 and 1.13.
  - Smoke check (dagster and dagster-dg-cli 1.13.30): `dg list components` lists `dagster_fme.FMEComponent`,
    `dg scaffold defs dagster_fme.FMEComponent fme` writes the starter file, and `dg check defs` and `dg list defs`
    pass (two assets and `fme_sensor`).

## Phase 6: Docs and cleanup

- [x] `README.md`: what the package does, install from git, a YAML example (external resource and
      in-component resource), a Python API example, an asset key diagram, how the cursor and baseline work,
      and the dev commands.
- [x] Component docstring and field descriptions (`Field(description=...)`), so `dg` shows useful help.
- [x] Delete the original `fme/` directory.
- [x] Final gate: `uv run pre-commit run --all-files`, pytest coverage of at least 90%, and the lowest-direct job passes.
- [x] Commit: `Phase 6: docs, remove legacy fme module`.
- [x] Ask the user before pushing the `phase-6/...` branch or opening its PR. (Merged as PR #8.)
- [x] Notes from Phase 6:
  - When Dagster derives the YAML model for a `Resolvable` component, it overwrites
    `Field(description=...)` with the resolver's description (None) on every field that has a default.
    Component fields therefore carry their help through `Annotated[T, dg.Resolver.default(description=...)]`
    (the `_doc` helper), which works on 1.11.0 and 1.13. `FMEWorkspaceSpec` is a plain `dg.Model`, so
    `Field(description=...)` works there. A test checks that every YAML field has a description.
  - Closed the Phase 4 follow-up: `FMEResource.api_key` is `repr=False`.
  - Removed the `fme/` excludes from ruff config and pre-commit along with the directory.

---

## Review follow-up (Phases 7–10)

These phases fix what the review in [`docs/SPEC.md`](docs/SPEC.md) found. IDs `R*` are simplifications
and `N*` are bugs, risks or gaps, both defined there. Each phase gets its own `phase-<n>/<slug>` branch and PR,
as usual. Phases 7 and 8 change no public contract. Phases 9 and 10 change behaviour or the YAML schema, so
**don't start them until the user has answered the decisions listed in each one**.

### Phase 7: Simplify the sensor and resource (no contract change)

Branch: `phase-7/simplify`. Make one commit per item or small group. Every commit must keep the existing sensor,
component and resource tests green without editing what they assert. Delete tests only when they target a removed
private helper.

- [x] **R1**: delete `LatestJob` and `get_latest_time_for_responses`. Pass the resource's `{workspace: job}` straight
      to `evaluate_jobs`, which parses `timeFinished` itself. Delete or port the tests that call the removed function
      (`get_latest_time_for_responses` cases that still describe reachable behaviour move to `JobScan` tests).
- [x] **R2**: replace `parse_cursor` and `cursor_times` with `load_cursor(raw) -> dict[str, datetime]` and
      `dump_cursor(cursor) -> str`. `evaluate_jobs` works in datetimes and loops over the configured workspaces
      (sketch in `docs/SPEC.md`). The JSON written to the cursor must stay byte-compatible: add a test that
      round-trips a cursor written by the current code.
- [x] **R4**: `request_offset_paginated` yields each page's `items` list instead of the `httpx.Response`, so each page
      is decoded once. Inline `get_request`. Drop the caller overrides for `limit` and `offset`. Use
      `contextlib.closing` in `get_latest_successful_jobs`. Keep the respx assertions on params and call count.
- [x] **R5**: `FMEComponent.build_resource` passes `page_size`/`max_pages` only when set, so defaults live only on
      `FMEResource`.
- [x] **R6**: add `DEFAULT_KEY_PREFIX = ("fme",)` in `assets.py` and use it in `FMEWorkspace`, `build_fme_assets` and
      `FMEWorkspaceSpec`.
- [x] **R7**: move `FMEComponent`'s validators below all field declarations.
- [x] **R8**: make the job asset description a one-line f-string. Check the existing description test (if any) and
      update the expected text.
- [x] Gate: lint, format check, pyright, pytest with coverage ≥ 90%, and `lowest-direct`. Record the refactor in
      `CHANGELOG.md` under *Unreleased* (internal change, no user impact).
- [x] Notes from Phase 7:
  - The cursor JSON keeps its format, but key order now follows the configured workspace order rather than the
    previous cursor's order. JSON consumers don't depend on key order.
  - `load_cursor` parses values with `parse_fme_time`, so a hand-written `Z` suffix now loads on Python 3.10
    (it raised before). Naive values are still N-8 (Phase 8).
  - The `get_latest_time_for_responses` timezone case moved to a `JobScan` test. Its two `ValueError` cases were
    unreachable and are already covered by `test_scan_raises_for_target_job_without_time` and
    `test_scan_ignores_jobs_without_workspace_or_time_for_other_workspaces`.
  - R4 dropped the `limit` override, so the paginator tests now set `page_size=2` on the resource instead.

### Phase 8: Fix the low-risk bugs

Branch: `phase-8/fix-review-bugs`. Each bug lands as an `xfail(strict=True, reason="N-n: ...")` test first, then the
fix commit removes the marker, as in Phase 3.

- [x] **N-1**: `asset_spec_kwargs={"description": ...}` must not raise. The caller's description wins over the
      default. Test both the override and the default.
- [x] **N-5**: `FMEComponent` also rejects workspaces whose **cleaned** names collide (e.g. `a.b` and `a-b`), naming
      both raw workspaces in the error.
- [x] **N-8**: `load_cursor` treats a timestamp with no timezone as UTC. Test a hand-written naive cursor value.
- [x] **N-9**: scaffolder append mode writes a newline before `---` when the existing file doesn't end with one.
      Test with a file that has no trailing newline (skipped at the 1.11 floor, like the existing append test).
- [x] Record each fix in `CHANGELOG.md` under *Unreleased*.
- [x] Notes from Phase 8:
  - N-1: the caller's `description` also reaches the child assets, as every other `asset_spec_kwargs` key does.
  - N-5: names collide only when the `key_prefix` matches as well, so `a.b` under `fme` and `a-b` under `other`
    are still allowed (tested). The raw-name duplicate check runs first and keeps its own message.
  - N-8: the naive-as-UTC rule is in `load_cursor` only. Job times from FME still go through `parse_fme_time`
    unchanged (see the open question on `timeFinished` timezones).
  - Gate note: `uv sync --resolution lowest-direct && uv run pytest` silently tests the **locked** versions,
    because `uv run` re-syncs. Set `UV_RESOLUTION=lowest-direct` for every command, as CI does. Fixed in
    `AGENTS.md`. Verified at dagster 1.11.0 / httpx 0.27.0: 84 passed, 2 append tests skipped.

### Phase 9: Make the job scan robust on busy servers (needs user decisions)

Branch: `phase-9/scan-robustness`. Both risks come from the single newest-first scan reading up to
`page_size × max_pages` jobs (20,000 at the defaults).

Decisions to get from the user before starting:

1. **N-3** (a workspace with no cursor that is never found makes every tick scan to `max_pages`). Options:
   a. When a full scan ends without finding a first-sight workspace, record the newest `timeFinished` seen as its
      baseline. Its next real job then emits an event. This is a behaviour change, so pin it with a test and note it
      in `CHANGELOG.md`.
   b. Keep the behaviour, but log the warning only once per process and say in the README that misspelled names cost
      a full scan per tick.
2. **N-4** (a workspace with a cursor that hasn't run in more than the cap raises every tick and blocks every other
   workspace). Options:
   a. Don't raise. Emit events for resolved workspaces, keep the stuck workspace's cursor, and log an error naming it.
      Risk: a newer job beyond the cap is missed until the cap is raised.
   b. Add an optional `repository` to `FMEWorkspaceSpec`. Workspaces that set it use strategy 1 (`repository` +
      `workspace` filter, `limit=1`, one request each). Workspaces without it keep strategy 2. Doesn't fix N-4 for
      workspaces with no repository.
   c. Both a and b.

Decided (user, 2026-11-17): **N-3 option a**, **N-4 option c** (both a and b). The question of whether workspace
names are unique across repositories wasn't settled, so `repository` is a query hint only: matching and the cursor
stay keyed by the raw workspace name, and duplicate workspace names are still rejected. That question moves to
Phase 10.

Tasks once decided:

- [x] Implement the chosen N-3 option in `JobScan` / `get_latest_successful_jobs`, keeping the decision logic pure.
- [x] Implement the chosen N-4 option. If `repository` is added, it's a YAML schema addition: optional field, default
      `None`, documented with `Field(description=...)`, and covered by a component YAML test.
- [x] respx tests for each new path, asserting on params and call count.
- [x] README: update "how the cursor and baseline work" and the job fetching description.
- [x] `CHANGELOG.md` entries for every behaviour change.
- [x] Notes from Phase 9:
  - `JobScan` tracks `newest_seen`, and `JobScan.finish(reached_end)` (pure) returns `LatestJobs`: `jobs`,
    `baselines` (first-sight workspaces not found, set to `newest_seen`) and `stalled` (cursored workspaces left
    unresolved at the cap). Baselines need a trustworthy `newest_seen`, so they're only set when the scan read every
    page or the jobs arrived newest first.
  - `get_latest_successful_jobs` no longer raises at the cap. It logs an error for stalled workspaces and a warning
    for new baselines. `evaluate_jobs` takes `baselines` and applies them only to workspaces with no cursor entry.
  - Workspaces with `repository` go through `get_latest_job_in_repository` (one request, `limit=1`) and skip the
    scan. No baseline for them when they have no job: the next tick is one cheap request anyway.
  - Residual: a baselined workspace that never runs keeps its baseline cursor. Once that time drops out of the
    `page_size × max_pages` window it becomes stalled: one error log and a full scan per tick, but no longer
    blocking. Setting `repository` avoids it. Advancing idle cursors to the scan position would also fix it, but
    that changes what the cursor means, so it's not done here.

### Phase 10: Close the YAML gaps and tidy `FMEWorkspace` (needs user approval)

Branch: `phase-10/yaml-gaps`. Each item changes a public contract, so the user approves each one separately.

- [ ] **R3 / N-2** (Python API): make `FMEWorkspace` a frozen dataclass holding `workspace` and `specs`, built only by
      `build_fme_assets`. Remove `fqn` and the custom `__repr__`. This fixes N-2 (direct construction gives keys with
      no workspace segment). Breaking for anyone constructing `FMEWorkspace` directly or reading `.fqn`, so note it in
      `CHANGELOG.md`. If the user prefers no break, fix N-2 alone by adding the cleaned name inside the constructor.
- [ ] **N-6** (YAML schema): add an optional `asset_attributes` mapping to `FMEWorkspaceSpec` (e.g. `group_name`,
      `owners`, `tags`), passed through as `asset_spec_kwargs`. Decide with the user which keys to allow and whether
      `key` and `description` are rejected. Test that it reaches every spec through the YAML path.
- [ ] **N-7** (YAML schema): add optional `timeout` and `api_path` to `FMEComponent`, valid only with `host` and
      `api_key`, like `page_size`. Or, if the user prefers, document the external resource as the way to set them.
- [ ] **Q3, moved from Phase 9** (cursor format): are workspace names unique across FME repositories? If not, match on
      `repository/workspace`, which changes the cursor format and needs a backwards-compatible reader. Phase 9 added
      `repository` as a query hint only, keyed by workspace name. See *Open questions*.
- [ ] Update the scaffolder template and README for any new fields, and add `CHANGELOG.md` entries.

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
      Ask the user before changing the match key, because it changes the cursor format. (Moved to Phase 10.)
  - Related follow-up (deferred in Phase 4, user decision): an optional `repository` on `FMEWorkspaceSpec` would
    let those workspaces use strategy 1 (`repository` + `workspace` filter, `limit=1`, one request each).
- [x] Do the monorepo callers need back-compat aliases (`FME`, `FMEAssetSpec`, `build_asset_sensor`)? No (user decision).
- [x] What is the exact env-templating syntax and component-testing helper for the Dagster floor (1.11) compared with the latest?
  - Testing helper: settled, see the Phase 2 notes.
  - Env templating: `{{ env('VAR') }}` (raises if unset) and `{{ env.VAR }}` (gives `None` if unset) work on both
    1.11.0 and 1.13. Use the function form.
- [ ] Does FME Flow always return `timeFinished` in UTC? The v4 spec only says `date-time`. (Raised again by the
      review: affects N-8 and how cursor values compare.)
- [ ] Review decisions (from `docs/SPEC.md`): approval for R3, N-6 and N-7, and Q3 (see Phase 10). N-3 and N-4 were
      decided for Phase 9 (options a and c).
