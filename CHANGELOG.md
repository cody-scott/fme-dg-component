# Changelog

All notable changes to `dagster-fme` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - Unreleased

First release as a standalone package, ported from the `fme/` module of the `snowflake_analytics`
monorepo. The entries below are changes from that module's behaviour.

### Fixed

- **BUG-4**: Offset pagination stops once `offset + len(items) >= totalCount`, or when a page has
  no items. It no longer makes an extra empty request after the last page.
- **BUG-7**: The pagination page size and page cap are now the `FMEResource` fields `page_size`
  (default 1000, range 1 to 1000) and `max_pages` (default 20). Going over the cap raises
  `FMEPaginationLimitError` with a message naming the setting, not a bare `RecursionError`.
