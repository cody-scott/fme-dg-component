"""Shared constants and helpers for the test suite."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

FIXTURES = Path(__file__).parent / "fixtures"
HOST = "https://fme.example.com"
JOBS_URL = f"{HOST}/fmeapiv4/jobs"
API_KEY = "test-token"


def load_fixture(name: str) -> dict[str, Any]:
    """Load a JSON fixture from ``tests/fixtures``."""
    return json.loads((FIXTURES / name).read_text())


def jobs_responder(fixture: str) -> Callable[[httpx.Request], httpx.Response]:
    """Serve a fixture's jobs as an offset-paginated ``/jobs`` endpoint.

    Like the real server, each response echoes the requested ``offset`` and ``limit``,
    and pages past the end come back with no items.
    """
    items = load_fixture(fixture)["items"]

    def respond(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", 0))
        limit = int(request.url.params.get("limit", 100))
        page = {
            "items": items[offset : offset + limit],
            "totalCount": len(items),
            "offset": offset,
            "limit": limit,
        }
        return httpx.Response(200, json=page)

    return respond
