"""Tests for India ATS coverage: migration columns, seed data, workday CXS crawler.

Coverage:
  - data/india_ats_sources.json: schema validation for every record
  - WorkdayCrawler._crawl_cxs: pagination, location facet, field mapping
  - WorkdayCrawler._crawl_sitemap: existing behaviour still works when
    location_filter is None
  - WorkdayCrawler.crawl: branches on location_filter presence
  - AtsSource model: country, location_filter, notes, career_site_url attributes
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.crawler.ats.workday import WorkdayCrawler
from app.crawler.exceptions import CrawlException, SlugNotFoundException
from app.models import AtsSource

DATA_FILE = (
    Path(__file__).resolve().parent.parent / "data" / "india_ats_sources.json"
)

# ── Seed data contract ────────────────────────────────────────────────────────

class TestIndiaSeedData:
    """Validates the static india_ats_sources.json file."""

    def setup_method(self):
        self.records = json.loads(DATA_FILE.read_text())

    def test_file_is_valid_json(self):
        assert isinstance(self.records, list)
        assert len(self.records) > 0

    def test_all_required_keys_present(self):
        required = {"company_name", "ats_type", "ats_slug", "country"}
        for rec in self.records:
            missing = required - rec.keys()
            assert not missing, f"Missing keys {missing} in record {rec!r}"

    def test_all_countries_are_IN(self):
        for rec in self.records:
            assert rec["country"] == "IN", f"Expected IN, got {rec['country']!r}: {rec}"

    def test_ats_type_is_known(self):
        allowed = {"workday", "greenhouse", "lever", "bamboohr", "ashby", "custom"}
        for rec in self.records:
            assert rec["ats_type"] in allowed, (
                f"Unknown ats_type {rec['ats_type']!r}: {rec}"
            )

    def test_custom_type_records_have_null_slug(self):
        for rec in self.records:
            if rec["ats_type"] == "custom":
                assert rec["ats_slug"] is None, (
                    f"Custom record should have null slug: {rec}"
                )

    def test_non_custom_records_have_slug(self):
        for rec in self.records:
            if rec["ats_type"] != "custom":
                assert rec["ats_slug"], (
                    f"Non-custom record must have ats_slug: {rec}"
                )

    def test_no_js_comments_in_json(self):
        """Ensures the file is pure JSON (no // comments)."""
        raw = DATA_FILE.read_text()
        for i, line in enumerate(raw.splitlines(), 1):
            stripped = line.strip()
            if "//" in stripped and not stripped.startswith('"'):
                pytest.fail(f"Line {i} may contain a JS comment: {line!r}")

    def test_workday_records_have_location_filter(self):
        for rec in self.records:
            if rec["ats_type"] == "workday":
                assert rec.get("location_filter") is not None, (
                    f"Workday India record should have location_filter: {rec}"
                )

    def test_greenhouse_india_has_null_location_filter(self):
        for rec in self.records:
            if rec["ats_type"] == "greenhouse":
                assert rec.get("location_filter") is None, (
                    f"Greenhouse record should have null location_filter: {rec}"
                )

    def test_at_least_one_workday_record(self):
        workday = [r for r in self.records if r["ats_type"] == "workday"]
        assert len(workday) >= 5

    def test_at_least_one_greenhouse_record(self):
        gh = [r for r in self.records if r["ats_type"] == "greenhouse"]
        assert len(gh) >= 1

    def test_at_least_one_custom_record(self):
        custom = [r for r in self.records if r["ats_type"] == "custom"]
        assert len(custom) >= 3


# ── AtsSource model attributes ────────────────────────────────────────────────

class TestAtsSourceModelNewColumns:
    """Verifies the ORM model exposes the new columns after migration 0013."""

    def test_country_attribute_exists(self):
        assert hasattr(AtsSource, "country")

    def test_location_filter_attribute_exists(self):
        assert hasattr(AtsSource, "location_filter")

    def test_notes_attribute_exists(self):
        assert hasattr(AtsSource, "notes")

    def test_career_site_url_attribute_exists(self):
        assert hasattr(AtsSource, "career_site_url")

    def test_unique_constraint_name(self):
        table_args = AtsSource.__table_args__
        constraint_names = {
            c.name
            for c in table_args
            if hasattr(c, "name") and isinstance(c.name, str)
        }
        assert "uq_ats_source_country" in constraint_names, (
            f"Expected uq_ats_source_country in {constraint_names}"
        )
        assert "uq_ats_source" not in constraint_names, (
            "Old constraint uq_ats_source should be gone"
        )


# ── WorkdayCrawler.crawl dispatch ─────────────────────────────────────────────

def _make_db_row(crawl_config: dict, location_filter: str | None):
    """Build a tuple that looks like a sqlalchemy Row for (crawl_config, location_filter)."""
    row = MagicMock()
    row.__getitem__ = lambda self, i: (crawl_config, location_filter)[i]
    row[0] = crawl_config
    row[1] = location_filter
    return (crawl_config, location_filter)


def _make_db_mock(fetchone_result) -> AsyncMock:
    """Build an AsyncMock `db` whose execute().fetchone() returns fetchone_result."""
    db = AsyncMock()
    execute_result = MagicMock()
    execute_result.fetchone.return_value = fetchone_result
    db.execute.return_value = execute_result
    return db


@pytest.mark.asyncio
class TestWorkdayCrawlerDispatch:
    """crawl() should branch on location_filter presence."""

    async def test_routes_to_cxs_when_location_filter_set(self):
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()
        db = _make_db_mock(({"instance": "wd3", "career_site_name": "External"}, "India"))

        with (
            patch("app.crawler.ats.workday.AsyncSessionFactory") as mock_sf,
            patch.object(crawler, "_crawl_cxs", new_callable=AsyncMock, return_value=[]) as mock_cxs,
            patch.object(crawler, "_crawl_sitemap", new_callable=AsyncMock, return_value=[]) as mock_sm,
        ):
            mock_sf.return_value.__aenter__.return_value = db
            await crawler.crawl("acme", source_id)

            mock_cxs.assert_awaited_once()
            mock_sm.assert_not_awaited()

    async def test_routes_to_sitemap_when_no_location_filter(self):
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()
        db = _make_db_mock((
            {"sitemap_url": "https://acme.wd5.myworkdayjobs.com/en-US/External-sitemap.xml"},
            None,
        ))

        with (
            patch("app.crawler.ats.workday.AsyncSessionFactory") as mock_sf,
            patch.object(crawler, "_crawl_cxs", new_callable=AsyncMock, return_value=[]) as mock_cxs,
            patch.object(crawler, "_crawl_sitemap", new_callable=AsyncMock, return_value=[]) as mock_sm,
        ):
            mock_sf.return_value.__aenter__.return_value = db
            await crawler.crawl("acme", source_id)

            mock_sm.assert_awaited_once()
            mock_cxs.assert_not_awaited()

    async def test_returns_empty_list_when_source_not_found(self):
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()
        db = _make_db_mock(None)

        with patch("app.crawler.ats.workday.AsyncSessionFactory") as mock_sf:
            mock_sf.return_value.__aenter__.return_value = db
            result = await crawler.crawl("acme", source_id)
            assert result == []


# ── WorkdayCrawler._crawl_cxs ────────────────────────────────────────────────

def _cxs_response(postings: list[dict], total: int | None = None) -> dict:
    return {
        "jobPostings": postings,
        "total": total or len(postings),
    }


def _make_posting(title: str, path: str, location: str = "Bangalore, India") -> dict:
    return {
        "title": title,
        "externalPath": path,
        "locationsText": location,
        "bulletFields": [path.rstrip("/").split("/")[-1]],
    }


@pytest.mark.asyncio
class TestWorkdayCxsCrawler:
    """Unit tests for _crawl_cxs() with mocked httpx."""

    async def _run_cxs(
        self,
        responses: list[dict],
        location_filter: str = "India",
        config: dict | None = None,
    ) -> list[dict[str, Any]]:
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()

        import httpx

        call_count = 0

        class FakeClient:
            def __init__(self, *a, **kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

            async def post(self, url, **kwargs):
                nonlocal call_count
                resp_data = responses[min(call_count, len(responses) - 1)]
                call_count += 1
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.json.return_value = resp_data
                return mock_resp

        with patch("httpx.AsyncClient", FakeClient):
            return await crawler._crawl_cxs(
                "acme",
                source_id,
                config or {"instance": "wd5", "career_site_name": "External"},
                location_filter,
            )

    async def test_single_page_returns_all_jobs(self):
        postings = [
            _make_posting("SWE", "/en-US/External/job/Bangalore/SWE_123"),
            _make_posting("PM", "/en-US/External/job/Mumbai/PM_456"),
        ]
        jobs = await self._run_cxs([_cxs_response(postings)])
        assert len(jobs) == 2
        titles = {j["title"] for j in jobs}
        assert titles == {"SWE", "PM"}

    async def test_pagination_fetches_multiple_pages(self):
        page1 = [_make_posting(f"Job{i}", f"/path/Job{i}_{i}") for i in range(20)]
        page2 = [_make_posting(f"Job{i}", f"/path/Job{i}_{i}") for i in range(20, 30)]
        jobs = await self._run_cxs([
            _cxs_response(page1),
            _cxs_response(page2),
        ])
        assert len(jobs) == 30

    async def test_empty_response_returns_empty_list(self):
        jobs = await self._run_cxs([_cxs_response([])])
        assert jobs == []

    async def test_location_facet_in_body(self):
        """Verify the applied facet is included (checked via side-effect)."""
        captured_bodies: list[dict] = []
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def post(self, url, json=None, **kw):
                captured_bodies.append(json or {})
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"jobPostings": []}
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            await crawler._crawl_cxs(
                "acme", source_id, {"instance": "wd5", "career_site_name": "Ext"}, "India"
            )

        assert len(captured_bodies) >= 1
        first_body = captured_bodies[0]
        assert "appliedFacets" in first_body
        assert first_body["appliedFacets"]["Location"] == ["India"]

    async def test_404_raises_slug_not_found(self):
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def post(self, url, **kw):
                resp = MagicMock()
                resp.status_code = 404
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            with pytest.raises(SlugNotFoundException):
                await crawler._crawl_cxs(
                    "bad-slug", source_id, {"instance": "wd5", "career_site_name": "Ext"}, "India"
                )

    async def test_500_raises_crawl_exception(self):
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def post(self, url, **kw):
                resp = MagicMock()
                resp.status_code = 500
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            with pytest.raises(CrawlException):
                await crawler._crawl_cxs(
                    "acme", source_id, {"instance": "wd5", "career_site_name": "Ext"}, "India"
                )

    async def test_source_url_constructed_correctly(self):
        posting = _make_posting("SWE", "/en-US/External/job/Bangalore/SWE_123")
        jobs = await self._run_cxs([_cxs_response([posting])])
        assert len(jobs) == 1
        assert "acme" in jobs[0]["source_url"]
        assert "wd5" in jobs[0]["source_url"]

    async def test_external_job_id_extracted_from_path(self):
        posting = _make_posting("SWE", "/en-US/External/job/Bangalore/Software-Engineer_JR-99999")
        jobs = await self._run_cxs([_cxs_response([posting])])
        assert len(jobs) == 1
        assert jobs[0]["external_job_id"] == "Software-Engineer_JR-99999"

    async def test_ats_source_id_set_on_jobs(self):
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()
        posting = _make_posting("Dev", "/en-US/External/job/India/Dev_001")

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def post(self, url, **kw):
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"jobPostings": [posting]}
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            jobs = await crawler._crawl_cxs(
                "acme", source_id, {"instance": "wd5", "career_site_name": "Ext"}, "India"
            )

        assert all(j["ats_source_id"] == source_id for j in jobs)

    async def test_remote_job_detected(self):
        posting = _make_posting("Remote SWE", "/en-US/External/job/Remote/SWE_001", "Remote")
        jobs = await self._run_cxs([_cxs_response([posting])])
        assert len(jobs) == 1
        assert jobs[0]["remote"] is True


# ── WorkdayCrawler._crawl_sitemap (regression) ───────────────────────────────

@pytest.mark.asyncio
class TestWorkdaySitemapRegression:
    """Ensures sitemap behaviour is unchanged for sources with no location_filter."""

    async def test_sitemap_parses_job_urls(self):
        sitemap = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://acme.wd5.myworkdayjobs.com/en-US/External/job/NY/Engineer_JR-001</loc></url>
          <url><loc>https://acme.wd5.myworkdayjobs.com/en-US/External/job/CA/PM_JR-002</loc></url>
        </urlset>"""

        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def get(self, url, **kw):
                resp = MagicMock()
                resp.status_code = 200
                resp.raise_for_status = MagicMock()
                resp.text = sitemap
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            jobs = await crawler._crawl_sitemap(
                "acme",
                source_id,
                {"instance": "wd5", "career_site_name": "External"},
            )

        assert len(jobs) == 2
        ids = {j["external_job_id"] for j in jobs}
        assert ids == {"JR-001", "JR-002"}

    async def test_sitemap_empty_returns_empty_list(self):
        sitemap = """<?xml version="1.0"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>"""

        crawler = WorkdayCrawler()

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def get(self, url, **kw):
                resp = MagicMock()
                resp.status_code = 200
                resp.raise_for_status = MagicMock()
                resp.text = sitemap
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            jobs = await crawler._crawl_sitemap(
                "acme", uuid.uuid4(), {"instance": "wd5", "career_site_name": "External"}
            )

        assert jobs == []


# ── WorkdayCrawler CSRF retry ─────────────────────────────────────────────────

@pytest.mark.asyncio
class TestWorkdayCsrfRetry:
    """_crawl_cxs must retry with CSRF token when the first POST returns 403/422."""

    async def _run_cxs_with_csrf(
        self,
        first_status: int,
        csrf_token: str,
        jobs_response: dict,
    ) -> list[dict[str, Any]]:
        """Simulate: first POST → first_status, GET → csrf_token cookie, second POST → 200."""
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()
        post_calls: list[dict] = []
        get_calls: list[str] = []

        class FakeResponse:
            def __init__(self, status: int, data: dict | None = None, cookies: dict | None = None, headers: dict | None = None):
                self.status_code = status
                self._data = data or {}
                self.cookies = cookies or {}
                self.headers = headers or {}

            def json(self):
                return self._data

        class FakeClient:
            def __init__(self, *a, **kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

            async def post(self, url, json=None, headers=None, cookies=None, **kw):
                post_calls.append({"json": json, "headers": headers or {}, "cookies": cookies or {}})
                if len(post_calls) == 1:
                    return FakeResponse(first_status)
                return FakeResponse(200, jobs_response)

            async def get(self, url, **kw):
                get_calls.append(url)
                return FakeResponse(200, cookies={"CALYPSO_CSRF_TOKEN": csrf_token})

        with patch("httpx.AsyncClient", FakeClient):
            return await crawler._crawl_cxs(
                "acme",
                source_id,
                {"instance": "wd5", "career_site_name": "External"},
                "India",
            ), post_calls, get_calls

    async def test_csrf_retry_on_403_fetches_token_and_succeeds(self):
        posting = _make_posting("SWE", "/en-US/External/job/Bangalore/SWE_123")
        jobs, post_calls, get_calls = await self._run_cxs_with_csrf(
            first_status=403,
            csrf_token="test-csrf-abc",
            jobs_response=_cxs_response([posting]),
        )
        assert len(jobs) == 1
        assert jobs[0]["title"] == "SWE"
        assert len(get_calls) == 1, "Should have made one GET for CSRF"
        assert len(post_calls) == 2, "Should have retried the POST"
        assert post_calls[1]["headers"].get("X-CSRF-Token") == "test-csrf-abc"

    async def test_csrf_retry_on_422_fetches_token_and_succeeds(self):
        posting = _make_posting("PM", "/en-US/External/job/Mumbai/PM_456")
        jobs, post_calls, get_calls = await self._run_cxs_with_csrf(
            first_status=422,
            csrf_token="test-csrf-xyz",
            jobs_response=_cxs_response([posting]),
        )
        assert len(jobs) == 1
        assert jobs[0]["title"] == "PM"
        assert len(get_calls) == 1, "Should have made one GET for CSRF"
        assert post_calls[1]["headers"].get("X-CSRF-Token") == "test-csrf-xyz"

    async def test_csrf_only_retried_once(self):
        """If POST still fails after CSRF retry, raise CrawlException (no infinite loop)."""
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()
        post_count = 0

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def post(self, url, **kw):
                nonlocal post_count
                post_count += 1
                resp = MagicMock()
                resp.status_code = 403
                return resp
            async def get(self, url, **kw):
                resp = MagicMock()
                resp.status_code = 200
                resp.cookies = {"CALYPSO_CSRF_TOKEN": "some-token"}
                resp.headers = {}
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            with pytest.raises(Exception):
                await crawler._crawl_cxs(
                    "acme", source_id, {"instance": "wd5", "career_site_name": "Ext"}, "India"
                )
        assert post_count == 2, f"Expected exactly 2 POST calls (initial + CSRF retry), got {post_count}"

    async def test_no_csrf_fetch_on_200(self):
        """Successful first POST must not trigger the CSRF GET."""
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()
        get_called = []

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def post(self, url, **kw):
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"jobPostings": []}
                return resp
            async def get(self, url, **kw):
                get_called.append(url)
                resp = MagicMock()
                resp.status_code = 200
                resp.cookies = {}
                resp.headers = {}
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            await crawler._crawl_cxs(
                "acme", source_id, {"instance": "wd5", "career_site_name": "Ext"}, "India"
            )

        assert get_called == [], "GET should NOT be called when POST succeeds immediately"

    async def test_referer_header_set_on_post(self):
        """Referer must point to the job-board HTML page."""
        crawler = WorkdayCrawler()
        source_id = uuid.uuid4()
        captured_headers: list[dict] = []

        class FakeClient:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            async def post(self, url, headers=None, **kw):
                captured_headers.append(headers or {})
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"jobPostings": []}
                return resp

        with patch("httpx.AsyncClient", FakeClient):
            await crawler._crawl_cxs(
                "acme", source_id, {"instance": "wd5", "career_site_name": "External"}, "India"
            )

        assert captured_headers, "POST was never called"
        referer = captured_headers[0].get("Referer", "")
        assert "acme.wd5.myworkdayjobs.com" in referer
        assert "External/jobs" in referer
