"""Tests for FMEResource."""

from __future__ import annotations

import datetime
import logging
from typing import Any

import httpx
import pytest
import respx

from dagster_fme import FMEPaginationLimitError, FMEResource, LatestJobs
from dagster_fme.resource import JobScan
from tests.helpers import (
    API_KEY,
    HOST,
    JOBS_URL,
    items_responder,
    jobs_responder,
    load_fixture,
    make_job,
)


def test_api_key_not_in_repr() -> None:
    resource = FMEResource(host=HOST, api_key="s3cret-token")
    assert "s3cret-token" not in repr(resource)
    assert "s3cret-token" not in str(resource)


def test_base_url_normalises_slashes() -> None:
    assert FMEResource(host=f"{HOST}/", api_key="k").base_url == f"{HOST}/fmeapiv4"
    assert FMEResource(host=HOST, api_key="k", api_path="fmeapiv4/").base_url == (
        f"{HOST}/fmeapiv4"
    )


def test_pagination_raises_on_http_error(
    respx_mock: respx.MockRouter, fme_resource: FMEResource
) -> None:
    respx_mock.get(JOBS_URL).mock(return_value=httpx.Response(401))
    with pytest.raises(httpx.HTTPStatusError):
        list(fme_resource.request_offset_paginated("/jobs"))


def test_pagination_sends_auth_and_yields_every_page(respx_mock: respx.MockRouter) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        if offset == 0:
            return httpx.Response(200, json=load_fixture("jobs_page_1.json"))
        if offset == 2:
            return httpx.Response(200, json=load_fixture("jobs_page_2.json"))
        return httpx.Response(200, json={"items": [], "totalCount": 3, "offset": offset})

    route = respx_mock.get(JOBS_URL).mock(side_effect=respond)

    resource = FMEResource(host=HOST, api_key=API_KEY, page_size=2)
    pages = list(resource.request_offset_paginated("/jobs", params={"status": "success"}))

    items = [i["id"] for p in pages for i in p]
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


def test_pagination_stops_without_extra_request(respx_mock: respx.MockRouter) -> None:
    # 5 jobs at limit=2 is pages at offsets 0, 2, 4. A fourth request is wasted.
    route = respx_mock.get(JOBS_URL).mock(side_effect=jobs_responder("jobs_multiple_runs.json"))
    resource = FMEResource(host=HOST, api_key=API_KEY, page_size=2)
    pages = list(resource.request_offset_paginated("/jobs"))
    assert sum(len(p) for p in pages) == 5
    assert route.call_count == 3


def test_pagination_cap_is_configurable_with_clear_error(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(JOBS_URL).mock(
        return_value=httpx.Response(
            200, json={"items": [{"id": 1}], "totalCount": 10_000, "offset": 0}
        )
    )
    route = respx_mock.routes[0]
    resource = FMEResource(host=HOST, api_key="k", page_size=1, max_pages=2)
    with pytest.raises(FMEPaginationLimitError, match="max_pages=2"):
        list(resource.request_offset_paginated("/jobs"))
    assert route.call_count == 2
    assert route.calls[0].request.url.params["limit"] == "1"


@pytest.mark.parametrize("kwargs", [{"page_size": 0}, {"page_size": 1001}, {"max_pages": 0}])
def test_pagination_settings_are_validated(kwargs: dict[str, int]) -> None:
    with pytest.raises(Exception, match=r"(?i)validation"):
        FMEResource(host=HOST, api_key="k", **kwargs)  # pyright: ignore[reportArgumentType]


# --- get_latest_successful_jobs: newest-first scan with early stop -------------------

T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


def _iso(hour: int) -> str:
    return (T0 + datetime.timedelta(hours=hour)).isoformat()


def _history() -> list[dict[str, Any]]:
    """Ten jobs, one per hour, alternating between alpha and beta, plus gamma at hour 0."""
    jobs = [make_job(i, "alpha.fmw" if i % 2 else "beta.fmw", _iso(i)) for i in range(1, 10)]
    return [*jobs, make_job(100, "gamma.fmw", _iso(0))]


def _small_pages(max_pages: int = 20) -> FMEResource:
    return FMEResource(host=HOST, api_key=API_KEY, page_size=2, max_pages=max_pages)


def test_latest_jobs_request_params(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    FMEResource(host=HOST, api_key=API_KEY).get_latest_successful_jobs(["alpha.fmw"])
    params = route.calls[0].request.url.params
    assert params["status"] == "success"
    assert params["sort"] == "timeFinished_desc"
    assert params["limit"] == "1000"
    assert params["offset"] == "0"


def test_latest_jobs_stops_when_every_workspace_is_found(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    found = _small_pages().get_latest_successful_jobs(["alpha.fmw", "beta.fmw"]).jobs
    assert {ws: j["id"] for ws, j in found.items()} == {"alpha.fmw": 9, "beta.fmw": 8}
    assert route.call_count == 1


def test_latest_jobs_keeps_paging_for_first_sight_workspace(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    found = _small_pages().get_latest_successful_jobs(["alpha.fmw", "gamma.fmw"]).jobs
    assert {ws: j["id"] for ws, j in found.items()} == {"alpha.fmw": 9, "gamma.fmw": 100}
    # gamma is the very last job, on the fifth page of two.
    assert route.call_count == 5


def test_latest_jobs_stops_after_passing_the_cursor(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    since = {"gamma.fmw": T0 + datetime.timedelta(hours=6, minutes=30)}
    found = _small_pages().get_latest_successful_jobs(["gamma.fmw"], since=since)
    assert found == LatestJobs()
    # Pages are hours [9, 8] and [7, 6]; hour 6 is older than the cursor, so stop.
    assert route.call_count == 2


def test_latest_jobs_cursor_equal_to_a_finish_counts_as_passed(
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    since = {"gamma.fmw": T0 + datetime.timedelta(hours=8)}
    _small_pages().get_latest_successful_jobs(["gamma.fmw"], since=since)
    assert route.call_count == 1


def test_latest_jobs_cap_reports_stalled_workspace_and_keeps_the_rest(
    respx_mock: respx.MockRouter, caplog: pytest.LogCaptureFixture
) -> None:
    # N-4: gamma's cursor is older than anything within the cap. That must not block alpha.
    respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    with caplog.at_level(logging.ERROR):
        result = _small_pages(max_pages=2).get_latest_successful_jobs(
            ["alpha.fmw", "gamma.fmw"], since={"alpha.fmw": T0, "gamma.fmw": T0}
        )
    assert {ws: j["id"] for ws, j in result.jobs.items()} == {"alpha.fmw": 9}
    assert result.stalled == frozenset({"gamma.fmw"})
    assert result.baselines == {}
    assert "gamma.fmw" in caplog.text
    assert "alpha.fmw" not in caplog.text
    assert API_KEY not in caplog.text


def test_latest_jobs_cap_baselines_first_sight_workspace(
    respx_mock: respx.MockRouter, caplog: pytest.LogCaptureFixture
) -> None:
    # N-3: gamma is never reached, so it gets the newest finish seen as its baseline.
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    with caplog.at_level(logging.WARNING):
        result = _small_pages(max_pages=2).get_latest_successful_jobs(
            ["alpha.fmw", "gamma.fmw"], since={"alpha.fmw": T0}
        )
    assert {ws: j["id"] for ws, j in result.jobs.items()} == {"alpha.fmw": 9}
    assert result.baselines == {"gamma.fmw": T0 + datetime.timedelta(hours=9)}
    assert result.stalled == frozenset()
    assert route.call_count == 2
    assert "gamma.fmw" in caplog.text
    assert API_KEY not in caplog.text


def test_latest_jobs_full_scan_baselines_never_run_workspace(
    respx_mock: respx.MockRouter,
) -> None:
    # N-3: reading the whole history without a match also gives a baseline.
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    result = _small_pages().get_latest_successful_jobs(["missing.fmw"])
    assert result.jobs == {}
    assert result.baselines == {"missing.fmw": T0 + datetime.timedelta(hours=9)}
    assert route.call_count == 5


def test_latest_jobs_baseline_lets_next_tick_stop_on_the_first_page(
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    resource = _small_pages(max_pages=2)
    first = resource.get_latest_successful_jobs(["missing.fmw"])
    calls_before = route.call_count
    resource.get_latest_successful_jobs(["missing.fmw"], since=first.baselines)
    assert route.call_count - calls_before == 1


def test_latest_jobs_out_of_order_cap_gives_no_baseline(
    respx_mock: respx.MockRouter, caplog: pytest.LogCaptureFixture
) -> None:
    # Without sort, newest_seen of a partial read isn't trustworthy.
    jobs = [make_job(1, "alpha.fmw", _iso(1)), make_job(2, "beta.fmw", _iso(5))] * 3
    respx_mock.get(JOBS_URL).mock(
        side_effect=lambda request: httpx.Response(
            200,
            json={
                "items": jobs[:2],
                "totalCount": 100,
                "offset": int(request.url.params["offset"]),
            },
        )
    )
    with caplog.at_level(logging.WARNING):
        result = _small_pages(max_pages=2).get_latest_successful_jobs(["gamma.fmw"])
    assert result == LatestJobs()
    assert "out of order" in caplog.text


def test_latest_jobs_handles_a_job_repeated_across_pages(respx_mock: respx.MockRouter) -> None:
    # A job finishing mid-scan pushes the list down, so page 2 repeats the last job of page 1.
    pages = [
        {"items": [make_job(2, "beta.fmw", _iso(2)), make_job(1, "alpha.fmw", _iso(1))]},
        {"items": [make_job(1, "alpha.fmw", _iso(1)), make_job(0, "gamma.fmw", _iso(0))]},
    ]

    def respond(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        return httpx.Response(200, json=pages[offset // 2] | {"totalCount": 5, "offset": offset})

    respx_mock.get(JOBS_URL).mock(side_effect=respond)
    found = _small_pages().get_latest_successful_jobs(["alpha.fmw", "gamma.fmw"]).jobs
    assert {ws: j["id"] for ws, j in found.items()} == {"alpha.fmw": 1, "gamma.fmw": 0}


def test_latest_jobs_no_workspaces_makes_no_request(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(JOBS_URL).mock(side_effect=items_responder(_history()))
    assert _small_pages().get_latest_successful_jobs([]) == LatestJobs()
    assert route.call_count == 0


# --- per-repository lookup (strategy 1) ----------------------------------------------


def test_repository_lookup_sends_one_filtered_request(respx_mock: respx.MockRouter) -> None:
    job = make_job(42, "alpha.fmw", _iso(3))
    route = respx_mock.get(JOBS_URL).mock(
        return_value=httpx.Response(200, json={"items": [job], "totalCount": 7})
    )
    result = _small_pages().get_latest_successful_jobs(
        ["alpha.fmw"], repositories={"alpha.fmw": "Samples"}
    )
    assert result.jobs == {"alpha.fmw": job}
    assert route.call_count == 1
    params = route.calls[0].request.url.params
    assert dict(params) == {
        "status": "success",
        "sort": "timeFinished_desc",
        "repository": "Samples",
        "workspace": "alpha.fmw",
        "limit": "1",
        "offset": "0",
    }
    assert route.calls[0].request.headers["Authorization"] == f"fmetoken token={API_KEY}"


def test_repository_lookup_with_no_job_keeps_nothing(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(JOBS_URL).mock(
        return_value=httpx.Response(200, json={"items": [], "totalCount": 0})
    )
    result = _small_pages().get_latest_successful_jobs(
        ["alpha.fmw"], since={"alpha.fmw": T0}, repositories={"alpha.fmw": "Samples"}
    )
    # One cheap request per tick, so no baseline and never stalled.
    assert result == LatestJobs()


def test_repository_lookup_ignores_a_job_for_another_workspace(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get(JOBS_URL).mock(
        return_value=httpx.Response(200, json={"items": [make_job(1, "beta.fmw", _iso(1))]})
    )
    resource = _small_pages()
    assert resource.get_latest_job_in_repository("alpha.fmw", "Samples") is None


def test_repository_lookup_raises_on_http_error(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(JOBS_URL).mock(return_value=httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        _small_pages().get_latest_job_in_repository("alpha.fmw", "Samples")


def test_mixed_lookup_scans_only_workspaces_without_repository(
    respx_mock: respx.MockRouter,
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if "repository" in request.url.params:
            return httpx.Response(200, json={"items": [make_job(500, "gamma.fmw", _iso(0))]})
        return items_responder(_history())(request)

    route = respx_mock.get(JOBS_URL).mock(side_effect=respond)
    result = _small_pages().get_latest_successful_jobs(
        ["alpha.fmw", "gamma.fmw"], since={"gamma.fmw": T0}, repositories={"gamma.fmw": "Samples"}
    )
    assert {ws: j["id"] for ws, j in result.jobs.items()} == {"alpha.fmw": 9, "gamma.fmw": 500}
    # One direct request for gamma, then one scan page finds alpha. Without the repository
    # gamma's old cursor would have forced every page to be read.
    assert route.call_count == 2
    assert result.stalled == frozenset()


# --- JobScan (pure) ------------------------------------------------------------------


def test_scan_out_of_order_page_disables_early_stop() -> None:
    scan = JobScan(targets=frozenset({"alpha.fmw"}), since={})
    scan.add_page([make_job(1, "alpha.fmw", _iso(1)), make_job(2, "beta.fmw", _iso(5))])
    assert not scan.ordered
    assert scan.unresolved == {"alpha.fmw"}
    # The newest job still wins once it turns up.
    scan.add_page([make_job(3, "alpha.fmw", _iso(9))])
    assert scan.found["alpha.fmw"]["id"] == 3


def test_scan_ignores_jobs_without_workspace_or_time_for_other_workspaces() -> None:
    scan = JobScan(targets=frozenset({"alpha.fmw"}), since={})
    scan.add_page([make_job(1, None, _iso(3)), make_job(2, "beta.fmw", None)])
    assert scan.found == {}
    assert scan.oldest_seen == T0 + datetime.timedelta(hours=3)


def test_scan_raises_for_target_job_without_time() -> None:
    scan = JobScan(targets=frozenset({"alpha.fmw"}), since={})
    with pytest.raises(ValueError, match="timeFinished"):
        scan.add_page([make_job(7, "alpha.fmw", None)])


def test_scan_finish_without_jobs_gives_no_baseline() -> None:
    scan = JobScan(targets=frozenset({"alpha.fmw"}), since={})
    assert scan.finish(reached_end=True) == LatestJobs()


def test_scan_finish_reached_end_resolves_out_of_order_scan() -> None:
    scan = JobScan(targets=frozenset({"alpha.fmw", "beta.fmw"}), since={"beta.fmw": T0})
    scan.add_page([make_job(1, "gamma.fmw", _iso(1)), make_job(2, "gamma.fmw", _iso(5))])
    assert not scan.ordered
    result = scan.finish(reached_end=True)
    # Every job was read, so the newest is known and nothing is stalled.
    assert result.baselines == {"alpha.fmw": T0 + datetime.timedelta(hours=5)}
    assert result.stalled == frozenset()


def test_scan_complete_with_no_jobs_only_when_nothing_targeted() -> None:
    assert JobScan(targets=frozenset(), since={}).is_complete
    assert not JobScan(targets=frozenset({"a"}), since={"a": T0}).is_complete


def test_scan_picks_latest_per_workspace_across_timezones() -> None:
    items = load_fixture("jobs_multiple_runs.json")["items"]
    scan = JobScan(targets=frozenset({"alpha.fmw", "Beta Load.fmw"}), since={})
    scan.add_page(items)
    # 09:30-05:00 is 14:30Z, later than 12:15Z.
    assert scan.found["alpha.fmw"]["id"] == 302
    # 07:20+02:00 is 05:20Z, later than 23:30Z the day before.
    assert scan.found["Beta Load.fmw"]["id"] == 304
    assert scan.found["Beta Load.fmw"]["repository"] == "Samples"
