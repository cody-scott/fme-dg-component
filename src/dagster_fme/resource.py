"""Thin client for the FME Flow REST API v4."""

from __future__ import annotations

import datetime
from collections.abc import Generator, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import dagster as dg
import httpx
from pydantic import Field

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
        ordered: Whether every job so far arrived newest first.
    """

    targets: frozenset[str]
    since: Mapping[str, datetime.datetime]
    found: dict[str, dict[str, Any]] = field(default_factory=dict)
    oldest_seen: datetime.datetime | None = None
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


class FMEResource(dg.ConfigurableResource):
    """Dagster resource for the FME Flow REST API v4, using token auth.

    Attributes:
        host: Base URL of the FME Flow server, e.g. ``https://fme.example.com``.
        api_key: FME Flow API token. Treat as a secret; supply via env templating.
        api_path: Path of the v4 REST API relative to ``host``.
        timeout: Request timeout in seconds.
        page_size: Items requested per page (FME Flow allows 1 to 1000).
        max_pages: Maximum pages fetched by one paginated request before giving up.
    """

    host: str
    api_key: str
    api_path: str = "/fmeapiv4"
    timeout: float = 30.0
    page_size: int = Field(default=1000, ge=1, le=1000)
    max_pages: int = Field(default=20, ge=1)

    @property
    def base_url(self) -> str:
        """The API root, ``host`` + ``api_path`` with no doubled or trailing slash."""
        return f"{self.host.rstrip('/')}/{self.api_path.strip('/')}"

    @property
    def auth_header(self) -> dict[str, str]:
        """The ``Authorization`` header for FME token auth."""
        return {"Authorization": f"fmetoken token={self.api_key}"}

    def get_request(
        self, client: httpx.Client, url: str, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        """Send a GET request and raise on HTTP error status.

        Args:
            client: The client to send the request with.
            url: Absolute URL to request.
            params: Query parameters.

        Returns:
            The successful response.

        Raises:
            httpx.HTTPStatusError: If the response status is 4xx or 5xx.
        """
        response = client.get(url, params=params)
        response.raise_for_status()
        return response

    def request_offset_paginated(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> Generator[httpx.Response, None, None]:
        """Yield every page of an offset-paginated endpoint.

        Args:
            endpoint: Endpoint path relative to the API root, e.g. ``/jobs``.
            params: Extra query parameters. ``limit`` overrides ``page_size`` and
                ``offset`` sets the starting offset.

        Yields:
            One response per page.

        Raises:
            FMEPaginationLimitError: If more than ``max_pages`` pages would be needed.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        params = {} if params is None else params
        limit = params.get("limit", self.page_size)
        offset = params.get("offset", 0)

        pages_fetched = 0

        with httpx.Client(headers=self.auth_header, timeout=self.timeout) as client:
            while True:
                if pages_fetched >= self.max_pages:
                    raise FMEPaginationLimitError(
                        f"{endpoint} needed more than max_pages={self.max_pages} pages of "
                        f"{limit} items. Raise max_pages or page_size on the resource."
                    )
                pages_fetched += 1

                request_params = params | {"limit": limit, "offset": offset}
                response = self.get_request(client=client, url=url, params=request_params)

                yield response

                body = response.json()
                items = body.get("items") or []
                total_count = int(body.get("totalCount", 0))
                page_offset = int(body.get("offset", offset))

                if not items or page_offset + len(items) >= total_count:
                    break

                offset += limit

    def get_latest_successful_jobs(
        self,
        workspaces: Iterable[str],
        since: Mapping[str, datetime.datetime] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Fetch the latest successful job for each workspace.

        Jobs are requested newest first (``sort=timeFinished_desc``), and paging stops
        as soon as every workspace has been found or the scan has gone past that
        workspace's ``since`` time. See ``JobScan``.

        If ``max_pages`` is reached while workspaces are unresolved, this raises only
        when one of them has a ``since`` time, because a newer job could be missed.
        Workspaces without one are first sight and would only record a baseline, so
        a warning is logged and the jobs found so far are returned.

        Args:
            workspaces: Raw names of the workspaces to look for.
            since: Last recorded ``timeFinished`` per workspace, from the sensor cursor.

        Returns:
            The latest successful job object per workspace. Workspaces with no job
            found are left out.

        Raises:
            FMEPaginationLimitError: If the page cap is reached while a workspace that
                has a ``since`` time is unresolved.
        """
        scan = JobScan(targets=frozenset(workspaces), since=dict(since or {}))
        if not scan.targets:
            return {}

        pages = self.request_offset_paginated(
            "/jobs", params={"status": "success", "sort": JOBS_SORT}
        )
        try:
            for page in pages:
                scan.add_page(page.json().get("items") or [])
                if scan.is_complete:
                    break
        except FMEPaginationLimitError:
            if any(ws in scan.since for ws in scan.unresolved):
                raise
            dg.get_dagster_logger().warning(
                "Reached max_pages=%s before finding jobs for %s. They will get a "
                "baseline once a newer job finishes.",
                self.max_pages,
                ", ".join(sorted(scan.unresolved)),
            )
        finally:
            pages.close()

        return scan.found
