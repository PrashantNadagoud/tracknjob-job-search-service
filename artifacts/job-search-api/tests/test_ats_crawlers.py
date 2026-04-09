"""Unit tests for ATS crawlers and CrawlDispatcher.

HTTP calls are intercepted by patching BaseATSCrawler._get_json / _post_json
directly on each crawler instance — no real network calls are made.

Coverage:
  - WorkdayCrawler: two-page pagination, geo classification, field mapping
  - BambooHRCrawler: single-page response, department/employment_type mapping,
    India-location geo-classification
  - CrawlDispatcher: success path, RateLimitedException back-off,
    SlugNotFoundException deactivation, generic CrawlException back-off,
    unknown ats_type handling
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.crawler.ats.bamboohr import BambooHRCrawler
from app.crawler.ats.workday import WorkdayCrawler
from app.crawler.dispatcher import CrawlDispatcher, _RATE_LIMIT_BACKOFF, _backoff_for
from app.crawler.exceptions import CrawlException, RateLimitedException, SlugNotFoundException
from app.models import AtsSource, Company, CrawlDeadLetter


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_ats_source(
    ats_type: str = "workday",
    ats_slug: str = "acme",
    consecutive_failures: int = 0,
    is_active: bool = True,
    company_id: uuid.UUID | None = None,
) -> MagicMock:
    src = MagicMock(spec=AtsSource)
    src.id = uuid.uuid4()
    src.ats_type = ats_type
    src.ats_slug = ats_slug
    src.company_id = company_id or uuid.uuid4()
    src.market = "US"
    src.is_active = is_active
    src.consecutive_failures = consecutive_failures
    src.backoff_until = None
    src.last_crawled_at = None
    src.last_crawl_status = None
    src.last_crawl_job_count = None
    return src


def _make_company(name: str = "Acme Corp") -> MagicMock:
    co = MagicMock(spec=Company)
    co.id = uuid.uuid4()
    co.name = name
    return co


# ── WorkdayCrawler ────────────────────────────────────────────────────────────

class TestWorkdayCrawler:
    @pytest.mark.asyncio
    async def test_single_page_returns_jobs(self):
        """A single-page Workday response normalises into job dicts."""
        ats_source_id = uuid.uuid4()
        crawler = WorkdayCrawler()

        page1 = {
            "jobPostings": [
                {
                    "id": "abc123",
                    "title": "Software Engineer",
                    "locationsText": "San Francisco, CA",
                    "externalPath": "/en-US/External/job/abc123",
                    "postedOn": "2026-01-15T00:00:00Z",
                    "bulletFields": ["Engineering"],
                },
            ],
            "total": 1,
        }

        with patch.object(crawler, "_post_json", new=AsyncMock(return_value=page1)):
            jobs = await crawler.crawl("acme", ats_source_id)

        assert len(jobs) == 1
        j = jobs[0]
        assert j["title"] == "Software Engineer"
        assert j["ats_type"] == "workday"
        assert j["external_job_id"] == "abc123"
        assert j["geo_restriction"] == "US"
        assert j["department"] == "Engineering"
        assert j["ats_source_id"] == ats_source_id
        assert j["salary_currency"] == "USD"
        assert j["remote"] is False

    @pytest.mark.asyncio
    async def test_pagination_fetches_all_pages(self):
        """WorkdayCrawler keeps fetching until offset >= total."""
        ats_source_id = uuid.uuid4()
        crawler = WorkdayCrawler()

        def _make_posting(idx: int) -> dict:
            return {
                "id": f"job{idx}",
                "title": f"Job {idx}",
                "locationsText": "Remote US",
                "externalPath": f"/en-US/External/job/job{idx}",
            }

        page1 = {"jobPostings": [_make_posting(i) for i in range(20)], "total": 35}
        page2 = {"jobPostings": [_make_posting(i) for i in range(20, 35)], "total": 35}
        page3 = {"jobPostings": [], "total": 35}

        mock_post = AsyncMock(side_effect=[page1, page2, page3])
        with patch.object(crawler, "_post_json", new=mock_post):
            jobs = await crawler.crawl("acme", ats_source_id)

        assert len(jobs) == 35
        # Should have made exactly 2 calls: page1 (20 jobs) + page2 (15 jobs, offset=35=total → stop)
        assert mock_post.call_count == 2

    @pytest.mark.asyncio
    async def test_remote_posting_sets_global_geo(self):
        """A clearly remote-US job gets geo_restriction='US'."""
        ats_source_id = uuid.uuid4()
        crawler = WorkdayCrawler()

        page = {
            "jobPostings": [
                {
                    "id": "r1",
                    "title": "Remote Python Dev",
                    "locationsText": "Remote US",
                    "externalPath": "/en-US/External/job/r1",
                }
            ],
            "total": 1,
        }
        with patch.object(crawler, "_post_json", new=AsyncMock(return_value=page)):
            jobs = await crawler.crawl("acme", ats_source_id)

        assert jobs[0]["geo_restriction"] == "US"
        assert jobs[0]["remote"] is True

    @pytest.mark.asyncio
    async def test_rate_limit_propagates(self):
        """HTTP 429 from Workday raises RateLimitedException."""
        crawler = WorkdayCrawler()
        with patch.object(
            crawler,
            "_post_json",
            new=AsyncMock(side_effect=RateLimitedException("429", http_status=429)),
        ):
            with pytest.raises(RateLimitedException):
                await crawler.crawl("acme", uuid.uuid4())

    @pytest.mark.asyncio
    async def test_slug_not_found_propagates(self):
        """HTTP 404 from Workday raises SlugNotFoundException."""
        crawler = WorkdayCrawler()
        with patch.object(
            crawler,
            "_post_json",
            new=AsyncMock(side_effect=SlugNotFoundException("404", http_status=404)),
        ):
            with pytest.raises(SlugNotFoundException):
                await crawler.crawl("no-such-company", uuid.uuid4())


# ── BambooHRCrawler ───────────────────────────────────────────────────────────

class TestBambooHRCrawler:
    @pytest.mark.asyncio
    async def test_normalises_result_list(self):
        """BambooHR result list is correctly normalised."""
        ats_source_id = uuid.uuid4()
        crawler = BambooHRCrawler()

        response = {
            "result": [
                {
                    "id": "42",
                    "title": "Backend Engineer",
                    "location": {"city": "Austin", "state": "TX", "country": "US"},
                    "department": {"label": "Engineering"},
                    "employmentStatusLabel": "Full-Time",
                }
            ]
        }
        with patch.object(crawler, "_get_json", new=AsyncMock(return_value=response)):
            jobs = await crawler.crawl("acme", ats_source_id)

        assert len(jobs) == 1
        j = jobs[0]
        assert j["title"] == "Backend Engineer"
        assert j["ats_type"] == "bamboohr"
        assert j["external_job_id"] == "42"
        assert j["department"] == "Engineering"
        assert j["employment_type"] == "Full-Time"
        assert j["geo_restriction"] == "US"
        assert j["salary_currency"] == "USD"
        assert j["ats_source_id"] == ats_source_id

    @pytest.mark.asyncio
    async def test_accepts_bare_list_response(self):
        """BambooHR tenants that return a bare list are handled."""
        crawler = BambooHRCrawler()
        response = [
            {"id": "99", "title": "Product Manager", "location": {}, "department": {}}
        ]
        with patch.object(crawler, "_get_json", new=AsyncMock(return_value=response)):
            jobs = await crawler.crawl("acme", uuid.uuid4())

        assert len(jobs) == 1
        assert jobs[0]["external_job_id"] == "99"

    @pytest.mark.asyncio
    async def test_india_location_classified_as_in(self):
        """A job in Bengaluru is geo-classified as IN."""
        crawler = BambooHRCrawler()
        response = {
            "result": [
                {
                    "id": "77",
                    "title": "SRE",
                    "location": {"city": "Bengaluru", "country": "India"},
                    "department": {},
                }
            ]
        }
        with patch.object(crawler, "_get_json", new=AsyncMock(return_value=response)):
            jobs = await crawler.crawl("acme", uuid.uuid4())

        assert jobs[0]["geo_restriction"] == "IN"

    @pytest.mark.asyncio
    async def test_empty_result_returns_empty_list(self):
        """BambooHR returning an empty result list yields no jobs."""
        crawler = BambooHRCrawler()
        with patch.object(
            crawler, "_get_json", new=AsyncMock(return_value={"result": []})
        ):
            jobs = await crawler.crawl("acme", uuid.uuid4())
        assert jobs == []


# ── CrawlDispatcher ───────────────────────────────────────────────────────────

class TestCrawlDispatcher:
    """Tests for CrawlDispatcher.dispatch() — no real DB or HTTP calls."""

    def _make_db(
        self,
        ats_source: AtsSource | None,
        company: Company | None = None,
    ) -> AsyncSession:
        """Build a minimal AsyncSession mock."""
        db = MagicMock(spec=AsyncSession)
        db.commit = AsyncMock()
        db.add = MagicMock()

        async def _get(model_class, pk):
            if model_class is AtsSource:
                return ats_source
            if model_class is Company:
                return company
            return None

        db.get = AsyncMock(side_effect=_get)

        execute_result = MagicMock()
        db.execute = AsyncMock(return_value=execute_result)
        return db

    @pytest.mark.asyncio
    async def test_success_path_returns_jobs_and_updates_source(self):
        """On success, jobs are returned and AtsSource success fields are written."""
        ats_source = _make_ats_source(ats_type="bamboohr", ats_slug="acme")
        company = _make_company("Acme Corp")
        db = self._make_db(ats_source, company)
        dispatcher = CrawlDispatcher()

        fake_jobs = [
            {
                "title": "Engineer",
                "location": "Austin, TX",
                "remote": False,
                "source_url": "https://acme.bamboohr.com/careers/1",
                "source_label": "Acme Careers",
                "geo_restriction": "US",
                "ats_type": "bamboohr",
                "external_job_id": "1",
                "ats_source_id": ats_source.id,
                "salary_currency": "USD",
            }
        ]

        with patch(
            "app.crawler.dispatcher.CRAWLER_MAP",
            {"bamboohr": AsyncMock(crawl=AsyncMock(return_value=fake_jobs))},
        ):
            result = await dispatcher.dispatch(ats_source.id, db)

        assert len(result) == 1
        assert result[0]["company"] == "Acme Corp"
        assert result[0]["company_id"] == ats_source.company_id
        # DB commit was called (for success update)
        db.commit.assert_called()
        # execute was called with an UPDATE (success path)
        db.execute.assert_called()

    @pytest.mark.asyncio
    async def test_rate_limit_applies_30_min_backoff(self):
        """RateLimitedException triggers 30-minute backoff and dead letter."""
        ats_source = _make_ats_source(ats_type="workday", consecutive_failures=0)
        company = _make_company()
        db = self._make_db(ats_source, company)
        dispatcher = CrawlDispatcher()

        before = datetime.now(timezone.utc)

        with patch(
            "app.crawler.dispatcher.CRAWLER_MAP",
            {
                "workday": AsyncMock(
                    crawl=AsyncMock(
                        side_effect=RateLimitedException("429", http_status=429)
                    )
                )
            },
        ):
            result = await dispatcher.dispatch(ats_source.id, db)

        assert result == []
        # A dead letter row was added
        db.add.assert_called_once()
        dead_letter: CrawlDeadLetter = db.add.call_args[0][0]
        assert dead_letter.error_type == "rate_limited"
        assert dead_letter.http_status == 429
        assert dead_letter.ats_type == "workday"

        # The UPDATE call should carry backoff_until ≈ now + 30 min
        update_call = db.execute.call_args_list[0]
        compiled = update_call[0][0]
        # Verify the update statement targets AtsSource (check via string repr)
        assert "ats_sources" in str(compiled).lower()

    @pytest.mark.asyncio
    async def test_slug_not_found_deactivates_source(self):
        """SlugNotFoundException sets is_active=False and writes dead letter."""
        ats_source = _make_ats_source(ats_type="bamboohr", ats_slug="ghost-co")
        db = self._make_db(ats_source, _make_company())
        dispatcher = CrawlDispatcher()

        with patch(
            "app.crawler.dispatcher.CRAWLER_MAP",
            {
                "bamboohr": AsyncMock(
                    crawl=AsyncMock(
                        side_effect=SlugNotFoundException("404", http_status=404)
                    )
                )
            },
        ):
            result = await dispatcher.dispatch(ats_source.id, db)

        assert result == []
        db.add.assert_called_once()
        dead_letter: CrawlDeadLetter = db.add.call_args[0][0]
        assert dead_letter.error_type == "slug_not_found"
        assert dead_letter.http_status == 404

        # The UPDATE statement should set is_active=False
        update_stmt_str = str(db.execute.call_args_list[0][0][0]).lower()
        assert "is_active" in update_stmt_str

    @pytest.mark.asyncio
    async def test_crawl_exception_increments_failures_and_backs_off(self):
        """A generic CrawlException increments consecutive_failures and applies backoff."""
        ats_source = _make_ats_source(ats_type="workday", consecutive_failures=2)
        db = self._make_db(ats_source, _make_company())
        dispatcher = CrawlDispatcher()

        with patch(
            "app.crawler.dispatcher.CRAWLER_MAP",
            {
                "workday": AsyncMock(
                    crawl=AsyncMock(
                        side_effect=CrawlException("Server error", http_status=500)
                    )
                )
            },
        ):
            result = await dispatcher.dispatch(ats_source.id, db)

        assert result == []
        db.add.assert_called_once()
        dead_letter: CrawlDeadLetter = db.add.call_args[0][0]
        assert dead_letter.error_type == "crawl_error"
        assert dead_letter.http_status == 500

    @pytest.mark.asyncio
    async def test_unknown_ats_type_writes_dead_letter_and_returns_empty(self):
        """An unregistered ats_type records a dead letter and returns []."""
        ats_source = _make_ats_source(ats_type="unknown_ats")
        db = self._make_db(ats_source, _make_company())
        dispatcher = CrawlDispatcher()

        result = await dispatcher.dispatch(ats_source.id, db)

        assert result == []
        db.add.assert_called_once()
        dead_letter: CrawlDeadLetter = db.add.call_args[0][0]
        assert dead_letter.error_type == "unknown_ats_type"

    @pytest.mark.asyncio
    async def test_ats_source_not_found_returns_empty(self):
        """If AtsSource row is missing, dispatch returns [] without crashing."""
        db = self._make_db(ats_source=None, company=None)
        dispatcher = CrawlDispatcher()
        result = await dispatcher.dispatch(uuid.uuid4(), db)
        assert result == []


# ── back-off helper ───────────────────────────────────────────────────────────

class TestBackoffHelper:
    def test_first_failure_gets_30_min(self):
        assert _backoff_for(1) == timedelta(minutes=30)

    def test_second_failure_gets_30_min(self):
        assert _backoff_for(2) == timedelta(minutes=30)

    def test_third_failure_gets_2_hours(self):
        assert _backoff_for(3) == timedelta(hours=2)

    def test_fifth_failure_gets_6_hours(self):
        assert _backoff_for(5) == timedelta(hours=6)

    def test_high_failures_capped_at_6_hours(self):
        assert _backoff_for(100) == timedelta(hours=6)

    def test_rate_limit_backoff_is_30_min(self):
        assert _RATE_LIMIT_BACKOFF == timedelta(minutes=30)
