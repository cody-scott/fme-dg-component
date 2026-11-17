"""Thin client for the FME Flow REST API v4."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dagster as dg
import httpx
from pydantic import Field


class FMEPaginationLimitError(RuntimeError):
    """Raised when an offset-paginated request needs more than ``max_pages`` pages."""


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
    ) -> Iterator[httpx.Response]:
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
