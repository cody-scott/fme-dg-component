"""Thin client for the FME Flow REST API v4."""

from __future__ import annotations

import datetime
from collections.abc import Generator, Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass, field
from typing import Any

import dagster as dg
import httpx
from pydantic import Field

DEFAULT_PAGE_SIZE = 1000
"""Default items per page. FME Flow allows at most 1000."""

DEFAULT_MAX_PAGES = 20
"""Default cap on pages fetched by one paginated request."""

JOBS_SORT = "timeFinished_desc"
"""Sort order requested from ``/jobs``, so the newest finish comes first."""


class FMEPaginationLimitError(RuntimeError):
    """Raised when an offset-paginated request needs more than ``max_pages`` pages."""


def parse_fme_time(value: str) -> datetime.datetime:
    """Parse an FME ISO-8601 timestamp, accepting a ``Z`` suffix or a UTC offset."""
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass
class JobScan:
    """Track a newest-first scan of ``/jobs`` for a set of workspaces.

    A workspace is *resolved* once the scan has found its latest job, or has gone
    past its cursor time, after which any older job can't be newer than the cursor.
    The scan is complete when every target workspace is resolved. If a page arrives
    out of order (a job finishing later than one already seen), the server isn't
    honouring the sort, so nothing counts as resolved and the scan reads every page.

    Attributes:
        targets: Raw names of the workspaces to look for.
        since: Cursor time per workspace. Workspaces without one are first sight.
        found: Latest job seen per target workspace.
        oldest_seen: Earliest ``timeFinished`` seen so far.
        newest_seen: Latest ``timeFinished`` seen so far.
        ordered: Whether every job so far arrived newest first.
    """

    targets: frozenset[str]
    since: Mapping[str, datetime.datetime]
    found: dict[str, dict[str, Any]] = field(default_factory=dict)
    oldest_seen: datetime.datetime | None = None
    newest_seen: datetime.datetime | None = None
    ordered: bool = True

    def add_page(self, items: Iterable[Mapping[str, Any]]) -> None:
        """Fold one page of jobs into the scan.

        Jobs with no ``workspace``, and jobs for other workspaces, only advance the
        scan position.

        Raises:
            ValueError: If a job for a target workspace has no ``timeFinished``.
        """
        for job in items:
            workspace = job.get("workspace")
            raw_time = job.get("timeFinished")
            is_target = workspace in self.targets

            if raw_time is None:
                if is_target:
                    raise ValueError(f"Job {job.get('id')} for {workspace} has no timeFinished")
                continue

            time_finished = parse_fme_time(raw_time)
            if self.oldest_seen is not None and time_finished > self.oldest_seen:
                self.ordered = False
            if self.oldest_seen is None or time_finished < self.oldest_seen:
                self.oldest_seen = time_finished
            if self.newest_seen is None or time_finished > self.newest_seen:
                self.newest_seen = time_finished

            if is_target:
                assert isinstance(workspace, str)
                current = self.found.get(workspace)
                if current is None or parse_fme_time(current["timeFinished"]) < time_finished:
                    self.found[workspace] = dict(job)

    def is_resolved(self, workspace: str) -> bool:
        """Whether nothing later in the scan can change the result for ``workspace``."""
        if not self.ordered:
            return False
        if workspace in self.found:
            return True
        cursor_time = self.since.get(workspace)
        return (
            cursor_time is not None
            and self.oldest_seen is not None
            and self.oldest_seen <= cursor_time
        )

    @property
    def unresolved(self) -> set[str]:
        """Target workspaces the scan hasn't resolved yet."""
        return {ws for ws in self.targets if not self.is_resolved(ws)}

    @property
    def is_complete(self) -> bool:
        """Whether every target workspace is resolved, so the scan can stop."""
        return not self.unresolved

    def finish(self, reached_end: bool) -> LatestJobs:
        """Summarise the scan once paging has stopped.

        A first-sight workspace the scan didn't find gets a baseline at ``newest_seen``,
        so its next job counts as newer and emits, and later ticks can stop early
        instead of reading every page again. That needs a trustworthy ``newest_seen``:
        either the scan read every job (``reached_end``) or the jobs arrived newest first.

        A workspace with a cursor time that is still unresolved when the scan stopped
        short is *stalled*: a newer job may exist beyond the pages read.

        Args:
            reached_end: Whether the scan read the last page of jobs, as opposed to
                stopping early or at the page cap.
        """
        unresolved = set() if reached_end else self.unresolved
        baselines: dict[str, datetime.datetime] = {}
        if self.newest_seen is not None and (reached_end or self.ordered):
            for ws in self.targets:
                if ws not in self.found and ws not in self.since:
                    baselines[ws] = self.newest_seen
        stalled = frozenset(ws for ws in unresolved if ws in self.since)
        return LatestJobs(jobs=dict(self.found), baselines=baselines, stalled=stalled)


@dataclass(frozen=True)
class LatestJobs:
    """What one job lookup learned about each workspace.

    Attributes:
        jobs: The latest successful job per raw workspace name. Workspaces with no job
            found are left out.
        baselines: Cursor times for first-sight workspaces that weren't found, so their
            next job emits. Empty when the lookup can't vouch for the newest job time.
        stalled: Workspaces with a cursor time that the lookup couldn't resolve within
            ``max_pages``. Their cursor should be kept as-is.
    """

    jobs: dict[str, dict[str, Any]] = field(default_factory=dict)
    baselines: dict[str, datetime.datetime] = field(default_factory=dict)
    stalled: frozenset[str] = frozenset()


class FMEResource(dg.ConfigurableResource):
    """Dagster resource for the FME Flow REST API v4, using token auth.

    Attributes:
        host: Base URL of the FME Flow server, e.g. ``https://fme.example.com``.
        api_key: FME Flow API token. Treat it as a secret and supply it from the
            environment. It's left out of the resource's repr.
        api_path: Path of the v4 REST API relative to ``host``.
        timeout: Request timeout in seconds.
        page_size: Items requested per page (FME Flow allows 1 to 1000).
        max_pages: Maximum pages fetched by one paginated request before giving up.
    """

    host: str = Field(description="Base URL of the FME Flow server, e.g. https://fme.example.com.")
    api_key: str = Field(
        repr=False, description="FME Flow API token. Supply it from the environment."
    )
    api_path: str = Field(default="/fmeapiv4", description="Path of the v4 REST API under host.")
    timeout: float = Field(default=30.0, description="Request timeout in seconds.")
    page_size: int = Field(
        default=DEFAULT_PAGE_SIZE, ge=1, le=1000, description="Items requested per page (1-1000)."
    )
    max_pages: int = Field(
        default=DEFAULT_MAX_PAGES, ge=1, description="Page cap for one paginated request."
    )

    @property
    def base_url(self) -> str:
        """The API root, ``host`` + ``api_path`` with no doubled or trailing slash."""
        return f"{self.host.rstrip('/')}/{self.api_path.strip('/')}"

    @property
    def auth_header(self) -> dict[str, str]:
        """The ``Authorization`` header for FME token auth."""
        return {"Authorization": f"fmetoken token={self.api_key}"}

    def request_offset_paginated(
        self, endpoint: str, params: Mapping[str, Any] | None = None
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Yield the ``items`` of every page of an offset-paginated endpoint.

        Pages hold ``page_size`` items and start at offset 0.

        Args:
            endpoint: Endpoint path relative to the API root, e.g. ``/jobs``.
            params: Extra query parameters.

        Yields:
            The items of one page.

        Raises:
            httpx.HTTPStatusError: If a response status is 4xx or 5xx.
            FMEPaginationLimitError: If more than ``max_pages`` pages would be needed.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        base_params = dict(params or {})
        offset = 0

        with httpx.Client(headers=self.auth_header, timeout=self.timeout) as client:
            for _ in range(self.max_pages):
                response = client.get(
                    url, params=base_params | {"limit": self.page_size, "offset": offset}
                )
                response.raise_for_status()
                body = response.json()
                items = body.get("items") or []
                yield items

                total_count = int(body.get("totalCount", 0))
                page_offset = int(body.get("offset", offset))
                if not items or page_offset + len(items) >= total_count:
                    return
                offset += self.page_size

        raise FMEPaginationLimitError(
            f"{endpoint} needed more than max_pages={self.max_pages} pages of "
            f"{self.page_size} items. Raise max_pages or page_size on the resource."
        )

    def get_latest_job_in_repository(
        self, workspace: str, repository: str
    ) -> dict[str, Any] | None:
        """Fetch the latest successful job for one workspace in one repository.

        Makes a single request filtered on ``repository`` and ``workspace`` (FME Flow
        only applies the ``workspace`` filter together with ``repository``), sorted
        newest first with ``limit=1``.

        Args:
            workspace: Raw workspace name.
            repository: FME Flow repository that holds the workspace.

        Returns:
            The latest successful job, or None if the workspace has none.

        Raises:
            httpx.HTTPStatusError: If the response status is 4xx or 5xx.
        """
        params = {
            "status": "success",
            "sort": JOBS_SORT,
            "repository": repository,
            "workspace": workspace,
            "limit": 1,
            "offset": 0,
        }
        with httpx.Client(headers=self.auth_header, timeout=self.timeout) as client:
            response = client.get(f"{self.base_url}/jobs", params=params)
            response.raise_for_status()
        # Reuse the scan so a job for another workspace is ignored and a missing
        # timeFinished is reported the same way.
        scan = JobScan(targets=frozenset({workspace}), since={})
        scan.add_page(response.json().get("items") or [])
        return scan.found.get(workspace)

    def get_latest_successful_jobs(
        self,
        workspaces: Iterable[str],
        since: Mapping[str, datetime.datetime] | None = None,
        repositories: Mapping[str, str] | None = None,
    ) -> LatestJobs:
        """Fetch the latest successful job for each workspace.

        Workspaces listed in ``repositories`` are looked up one request each with
        ``get_latest_job_in_repository``. The rest share one newest-first scan of
        ``/jobs`` (``sort=timeFinished_desc``), which stops as soon as every workspace
        has been found or the scan has gone past that workspace's ``since`` time.
        See ``JobScan``.

        The scan never raises for the page cap. A first-sight workspace it doesn't find
        gets a baseline (see ``JobScan.finish``), and a workspace with a ``since`` time
        it can't resolve within ``max_pages`` is reported as stalled and logged as an
        error. Other workspaces still get their jobs.

        Args:
            workspaces: Raw names of the workspaces to look for.
            since: Last recorded ``timeFinished`` per workspace, from the sensor cursor.
            repositories: Repository per raw workspace name, for workspaces to look up
                directly instead of through the scan.

        Returns:
            The jobs, baselines and stalled workspaces.
        """
        since = dict(since or {})
        repositories = dict(repositories or {})
        targets = frozenset(workspaces)

        direct = {ws: repositories[ws] for ws in targets if ws in repositories}
        direct_jobs: dict[str, dict[str, Any]] = {}
        for ws, repository in sorted(direct.items()):
            job = self.get_latest_job_in_repository(ws, repository)
            if job is not None:
                direct_jobs[ws] = job

        result = self._scan_latest_jobs(targets - direct.keys(), since)
        return LatestJobs(
            jobs=result.jobs | direct_jobs, baselines=result.baselines, stalled=result.stalled
        )

    def _scan_latest_jobs(
        self, targets: frozenset[str], since: Mapping[str, datetime.datetime]
    ) -> LatestJobs:
        """Run the shared newest-first scan for ``targets`` and log anything unresolved."""
        scan = JobScan(targets=targets, since=since)
        if not scan.targets:
            return LatestJobs()

        reached_end = False
        pages = self.request_offset_paginated(
            "/jobs", params={"status": "success", "sort": JOBS_SORT}
        )
        try:
            with closing(pages):
                for items in pages:
                    scan.add_page(items)
                    if scan.is_complete:
                        break
                else:
                    reached_end = True
        except FMEPaginationLimitError:
            pass

        result = scan.finish(reached_end)
        logger = dg.get_dagster_logger()
        if result.stalled:
            logger.error(
                "Reached max_pages=%s without resolving %s. Their cursor is kept and a newer "
                "job may be missed. Raise max_pages or page_size, or set repository on these "
                "workspaces so they're looked up directly.",
                self.max_pages,
                ", ".join(sorted(result.stalled)),
            )
        if result.baselines:
            logger.warning(
                "Found no successful job for %s. Recording a baseline at %s, so their next "
                "job materializes. Check the workspace name if this is unexpected.",
                ", ".join(sorted(result.baselines)),
                scan.newest_seen.isoformat() if scan.newest_seen else "",
            )
        missing = scan.targets - result.jobs.keys() - result.baselines.keys() - set(since)
        if missing:
            logger.warning(
                "Reached max_pages=%s before finding jobs for %s, and the jobs arrived out of "
                "order, so no baseline was recorded.",
                self.max_pages,
                ", ".join(sorted(missing)),
            )
        return result
