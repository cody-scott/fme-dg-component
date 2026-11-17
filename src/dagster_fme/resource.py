"""Thin client for the FME Flow REST API v4."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dagster as dg
import httpx


class FMEResource(dg.ConfigurableResource):
    """Dagster resource for the FME Flow REST API v4, using token auth.

    Attributes:
        host: Base URL of the FME Flow server, e.g. ``https://fme.example.com``.
        api_key: FME Flow API token. Treat as a secret; supply via env templating.
        api_path: Path of the v4 REST API relative to ``host``.
        timeout: Request timeout in seconds.
    """

    host: str
    api_key: str
    api_path: str = "/fmeapiv4"
    timeout: float = 30.0

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
            params: Extra query parameters. ``limit`` and ``offset`` set the page
                size and starting offset.

        Yields:
            One response per page.

        Raises:
            RecursionError: If more than 20 pages are requested.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        params = {} if params is None else params
        limit = params.get("limit", 1000)
        offset = params.get("offset", 0)

        # BUG-7: hard-coded page cap, ported as-is.
        current_depth = 0
        max_depth = 20

        with httpx.Client(headers=self.auth_header, timeout=self.timeout) as client:
            while True:
                current_depth += 1
                if current_depth > max_depth:
                    raise RecursionError("Depth exceeded")

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
