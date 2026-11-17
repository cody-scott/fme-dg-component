"""Shared constants and helpers for the test suite."""

from __future__ import annotations

import datetime
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
    return items_responder(items)


def items_responder(items: list[dict[str, Any]]) -> Callable[[httpx.Request], httpx.Response]:
    """Serve ``items`` as an offset-paginated ``/jobs`` endpoint.

    Honours ``sort=timeFinished_desc`` like the real server. Without ``sort``, items are
    served in the given order.
    """

    def respond(request: httpx.Request) -> httpx.Response:
        served = items
        if request.url.params.get("sort") == "timeFinished_desc":
            served = sorted(items, key=_finish_key, reverse=True)
        offset = int(request.url.params.get("offset", 0))
        limit = int(request.url.params.get("limit", 100))
        page = {
            "items": served[offset : offset + limit],
            "totalCount": len(served),
            "offset": offset,
            "limit": limit,
        }
        return httpx.Response(200, json=page)

    return respond


def _finish_key(item: dict[str, Any]) -> datetime.datetime:
    raw = item.get("timeFinished")
    if raw is None:
        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    return datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))


def make_job(job_id: int, workspace: str | None, time_finished: str | None) -> dict[str, Any]:
    """Build a minimal successful job object."""
    return {
        "id": job_id,
        "workspace": workspace,
        "repository": "Samples",
        "status": "success",
        "timeFinished": time_finished,
    }
