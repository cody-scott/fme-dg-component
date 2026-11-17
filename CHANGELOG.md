# Changelog

All notable changes to `dagster-fme` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - Unreleased

First release as a standalone package, ported from the `fme/` module of the `snowflake_analytics`
monorepo. The entries below are changes from that module's behaviour.

### Added

- `job_asset_name` on `FMEWorkspaceSpec`, `build_fme_assets` and `FMEWorkspace` sets the last
  key component of the job asset. It defaults to `workspace__fmw`, so existing keys don't change.

### Fixed

- **BUG-4**: Offset pagination stops once `offset + len(items) >= totalCount`, or when a page has
  no items. It no longer makes an extra empty request after the last page.
- **BUG-7**: The pagination page size and page cap are now the `FMEResource` fields `page_size`
  (default 1000, range 1 to 1000) and `max_pages` (default 20). Going over the cap raises
  `FMEPaginationLimitError` with a message naming the setting, not a bare `RecursionError`.
- **BUG-1**: Configured workspaces that have no job in a tick's response keep their cursor value.
  Before, they were dropped and later re-baselined without emitting anything. Entries for
  workspaces that are no longer configured are still dropped. The sensor now returns its cursor
  through `SensorResult(cursor=...)` and no longer calls `context.update_cursor`.
- **BUG-3**: Materialization metadata is typed. Each event carries `fme_job_id`, `workspace`,
  `repository`, `status`, `time_started` and `time_finished` (timestamps), `duration_seconds`
  (from `elapsedTime`, or from finish minus start), and the full job object as `fme_job` (JSON).
  Fields missing from the job are left out. Before, the raw job dict was passed as metadata.
- **BUG-9**: The sensor gets its resource with `getattr(context.resources, resource_key)`
  instead of the internal `original_resource_dict`.
- **BUG-5**: Removed mutable default arguments. `FMEWorkspace` and `build_fme_assets` take
  `None` and normalise it, and copy the lists callers pass in. `FMEWorkspaceSpec` uses
  `Field(default_factory=...)`. Ruff `B006` is enforced with no suppressions.
- **BUG-2 (breaking)**: The `FMEWorkspaceSpec.key_prefix` default is now `["fme"]`, the same as
  `build_fme_assets`. It was `["FME"]`. YAML configs that relied on the default get asset keys
  under `fme/...` instead of `FME/...`. To keep the old keys, set `key_prefix: [FME]`.
- **BUG-8**: `FMEComponent` rejects `fme_asset_specs` entries that repeat a `workspace`, and the
  error names the duplicates. Before, a later spec silently replaced an earlier one in the
  sensor's lookup.
- **BUG-6**: Rewrote leftover SSIS/MSSQL docstrings in Google style for FME. A test guards
  against the old wording coming back.
