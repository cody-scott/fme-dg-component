"""Tests for FMEResource."""

from __future__ import annotations

import httpx
import pytest
import respx

from dagster_fme import FMEResource
from tests.helpers import API_KEY, HOST, JOBS_URL, jobs_responder, load_fixture


def test_base_url_normalises_slashes() -> None:
    assert FMEResource(host=f"{HOST}/", api_key="k").base_url == f"{HOST}/fmeapiv4"
    assert FMEResource(host=HOST, api_key="k", api_path="fmeapiv4/").base_url == (
        f"{HOST}/fmeapiv4"
    )


def test_get_request_raises_on_http_error(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    respx_mock.get(JOBS_URL).mock(return_value=httpx.Response(401))
    with httpx.Client() as client, pytest.raises(httpx.HTTPStatusError):
        fme_resource.get_request(client, JOBS_URL)


def test_pagination_sends_auth_and_yields_every_page(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        if offset == 0:
            return httpx.Response(200, json=load_fixture("jobs_page_1.json"))
        if offset == 2:
            return httpx.Response(200, json=load_fixture("jobs_page_2.json"))
        return httpx.Response(200, json={"items": [], "totalCount": 3, "offset": offset})

    route = respx_mock.get(JOBS_URL).mock(side_effect=respond)

    pages = list(
        fme_resource.request_offset_paginated("/jobs", params={"status": "success", "limit": 2})
    )

    items = [i["id"] for p in pages for i in p.json()["items"]]
    assert items == [101, 102, 103]
    first = route.calls[0].request
    assert first.headers["Authorization"] == f"fmetoken token={API_KEY}"
    assert first.url.params["status"] == "success"
    assert [(c.request.url.params["limit"], c.request.url.params["offset"]) for c in route.calls][
        :2
    ] == [("2", "0"), ("2", "2")]


def test_pagination_default_limit(respx_mock: respx.MockRouter, fme_resource: FMEResource) -> None:
    route = respx_mock.get(JOBS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("jobs_empty.json"))
    )
    list(fme_resource.request_offset_paginated("/jobs"))
    assert route.calls[0].request.url.params["limit"] == "1000"
    assert route.calls[0].request.url.params["offset"] == "0"


def test_pagination_stops_without_extra_request(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    # 5 jobs at limit=2 is pages at offsets 0, 2, 4. A fourth request is wasted.
    route = respx_mock.get(JOBS_URL).mock(side_effect=jobs_responder("jobs_multiple_runs.json"))
    pages = list(fme_resource.request_offset_paginated("/jobs", params={"limit": 2}))
    assert sum(len(p.json()["items"]) for p in pages) == 5
    assert route.call_count == 3


@pytest.mark.xfail(
    strict=True, reason="BUG-7: page cap is hard-coded and raises a bare RecursionError"
)
def test_pagination_cap_is_configurable_with_clear_error(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(JOBS_URL).mock(
        return_value=httpx.Response(
            200, json={"items": [{"id": 1}], "totalCount": 10_000, "offset": 0}
        )
    )
    resource = FMEResource(host=HOST, api_key="k", page_size=1, max_pages=2)  # type: ignore[call-arg]
    with pytest.raises(Exception, match="max_pages") as exc:
        list(resource.request_offset_paginated("/jobs"))
    assert not isinstance(exc.value, RecursionError)
