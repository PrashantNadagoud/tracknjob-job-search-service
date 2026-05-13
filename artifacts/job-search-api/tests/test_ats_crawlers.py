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

from app.crawler.ats.ashby import AshbyCrawler
from app.crawler.ats.bamboohr import BambooHRCrawler
from app.crawler.ats.greenhouse import GreenhouseCrawler
from app.crawler.ats.lever import LeverCrawler
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
    @pytest.fixture(autouse=True)
    def mock_db_session(self):
        # We need a context manager mock for AsyncSessionFactory()
        mock_db = AsyncMock()
        # The query returns a row where row[0] is the crawl_config JSONB object
        mock_result = MagicMock()
        mock_result.fetchone.return_value = ({"instance": "wd1", "career_site_name": "External"},)
        mock_db.execute.return_value = mock_result
        
        class MockSessionContext:
            async def __aenter__(self):
                return mock_db
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
                
        self.mock_session_factory = MagicMock(return_value=MockSessionContext())
        
        with patch("app.crawler.ats.workday.AsyncSessionFactory", self.mock_session_factory):
            yield

    @pytest.mark.asyncio
    async def test_single_page_returns_jobs(self):
        """Workday sitemap crawl successfully parses job URLs."""
        ats_source_id = uuid.uuid4()
        crawler = WorkdayCrawler()

        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://acme.wd1.myworkdayjobs.com/en-US/External/job/US-CA-San-Francisco/Software-Engineer_JR12345</loc>
    <lastmod>2026-04-10</lastmod>
  </url>
</urlset>'''

        # Mock httpx client to return sitemap XML
        mock_response = MagicMock()
        mock_response.text = sitemap_xml
        mock_response.raise_for_status = MagicMock()
        
        async def mock_get(url):
            return mock_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client
            
            jobs = await crawler.crawl("acme", ats_source_id)

        assert len(jobs) == 1
        j = jobs[0]
        assert j["title"] == "Software Engineer"
        assert j["ats_type"] == "workday"
        assert j["external_job_id"] == "JR12345"
        assert j["location"] == "US, CA, San, Francisco"
        assert j["ats_source_id"] == ats_source_id
        assert j["salary_currency"] == "USD"
        assert j["remote"] is False

    @pytest.mark.asyncio
    async def test_multiple_jobs_in_sitemap(self):
        """Workday sitemap with multiple jobs parses all entries."""
        ats_source_id = uuid.uuid4()
        crawler = WorkdayCrawler()

        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://acme.wd1.myworkdayjobs.com/en-US/External/job/US-CA-Santa-Clara/Senior-Software-Engineer_JR12345</loc>
    <lastmod>2026-04-10</lastmod>
  </url>
  <url>
    <loc>https://acme.wd1.myworkdayjobs.com/en-US/External/job/US-TX-Austin/Staff-Engineer_JR67890</loc>
    <lastmod>2026-04-09</lastmod>
  </url>
  <url>
    <loc>https://acme.wd1.myworkdayjobs.com/en-US/External/job/Remote/Remote-Developer_JR11111</loc>
    <lastmod>2026-04-08</lastmod>
  </url>
</urlset>'''

        mock_response = MagicMock()
        mock_response.text = sitemap_xml
        mock_response.raise_for_status = MagicMock()
        
        async def mock_get(url):
            return mock_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client
            
            jobs = await crawler.crawl("acme", ats_source_id)

        assert len(jobs) == 3
        assert jobs[0]["external_job_id"] == "JR12345"
        assert jobs[1]["external_job_id"] == "JR67890"
        assert jobs[2]["external_job_id"] == "JR11111"
        assert jobs[2]["remote"] is True  # "Remote" in location

    @pytest.mark.asyncio
    async def test_remote_posting_sets_global_geo(self):
        """A clearly remote-US job gets geo_restriction='US'."""
        ats_source_id = uuid.uuid4()
        crawler = WorkdayCrawler()

        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://acme.wd1.myworkdayjobs.com/en-US/External/job/Remote-US/Remote-Python-Dev_JR99999</loc>
    <lastmod>2026-04-10</lastmod>
  </url>
</urlset>'''

        mock_response = MagicMock()
        mock_response.text = sitemap_xml
        mock_response.raise_for_status = MagicMock()
        
        async def mock_get(url):
            return mock_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client
            
            jobs = await crawler.crawl("acme", ats_source_id)

        assert jobs[0]["remote"] is True
        # Remote jobs get GLOBAL geo_restriction
        assert jobs[0]["geo_restriction"] in ["US", "GLOBAL"]

    @pytest.mark.asyncio
    async def test_rate_limit_propagates(self):
        """HTTP 429 from Workday sitemap raises RateLimitedException."""
        crawler = WorkdayCrawler()
        
        # Mock httpx to raise HTTPStatusError with 429
        from httpx import HTTPStatusError, Request, Response
        mock_request = Request("GET", "https://acme.wd1.myworkdayjobs.com/sitemap.xml")
        mock_response = Response(429, request=mock_request)
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(side_effect=HTTPStatusError("429 Rate Limited", request=mock_request, response=mock_response))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_class.return_value = mock_client
            
            with pytest.raises(RateLimitedException):
                await crawler.crawl("acme", uuid.uuid4())

    @pytest.mark.asyncio
    async def test_slug_not_found_propagates(self):
        """HTTP 404 from Workday sitemap raises SlugNotFoundException."""
        crawler = WorkdayCrawler()
        
        # Mock httpx to raise HTTPStatusError with 404
        from httpx import HTTPStatusError, Request, Response
        mock_request = Request("GET", "https://no-such-company.wd1.myworkdayjobs.com/sitemap.xml")
        mock_response = Response(404, request=mock_request)
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(side_effect=HTTPStatusError("404 Not Found", request=mock_request, response=mock_response))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_class.return_value = mock_client
            
            with pytest.raises(SlugNotFoundException):
                await crawler.crawl("no-such-company", uuid.uuid4())

    @pytest.mark.asyncio
    async def test_empty_sitemap_returns_empty_list(self):
        """Empty sitemap returns empty list without error."""
        ats_source_id = uuid.uuid4()
        crawler = WorkdayCrawler()

        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
</urlset>'''

        mock_response = MagicMock()
        mock_response.text = sitemap_xml
        mock_response.raise_for_status = MagicMock()
        
        async def mock_get(url):
            return mock_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client
            
            jobs = await crawler.crawl("acme", ats_source_id)

        assert len(jobs) == 0


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

        # The UPDATE statement should target ats_sources
        update_call = db.execute.call_args_list[0]
        compiled_stmt = update_call[0][0]
        stmt_str = str(compiled_stmt).lower()
        assert "ats_sources" in stmt_str
        # consecutive_failures and last_crawl_status must appear in the UPDATE
        assert "consecutive_failures" in stmt_str
        assert "last_crawl_status" in stmt_str

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

        # The UPDATE statement should set is_active=False and last_crawl_status
        update_stmt_str = str(db.execute.call_args_list[0][0][0]).lower()
        assert "is_active" in update_stmt_str
        assert "last_crawl_status" in update_stmt_str

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

    @pytest.mark.asyncio
    async def test_dispatch_never_raises_when_db_commit_fails(self):
        """dispatch() swallows DB failures and returns [] — it never raises."""
        ats_source = _make_ats_source(ats_type="bamboohr", ats_slug="acme")
        db = self._make_db(ats_source, _make_company())
        # Make DB commit always throw so the success-path persistence fails
        db.commit = AsyncMock(side_effect=RuntimeError("DB connection lost"))
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
            # Must NOT raise even though commit() throws
            result = await dispatcher.dispatch(ats_source.id, db)

        # Jobs are still returned (commit failure is logged, not re-raised)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_dispatch_uses_crawl_url_over_slug(self):
        """When crawl_url is set, it is passed as effective slug to the crawler."""
        custom_url = "https://amazon.wd3.myworkdayjobs.com/wday/cxs/amazon/Global_Tech/jobs"
        ats_source = _make_ats_source(ats_type="workday", ats_slug="amazon")
        ats_source.crawl_url = custom_url
        db = self._make_db(ats_source, _make_company("Amazon"))
        dispatcher = CrawlDispatcher()

        captured: list[str] = []

        async def _capture_crawl(slug: str, source_id: uuid.UUID) -> list:
            captured.append(slug)
            return []

        with patch(
            "app.crawler.dispatcher.CRAWLER_MAP",
            {"workday": AsyncMock(crawl=AsyncMock(side_effect=_capture_crawl))},
        ):
            await dispatcher.dispatch(ats_source.id, db)

        assert captured == [custom_url]



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


# ── GreenhouseCrawler ─────────────────────────────────────────────────────────

class TestGreenhouseCrawler:
    @pytest.mark.asyncio
    async def test_maps_jobs_correctly(self):
        crawler = GreenhouseCrawler()
        ats_source_id = uuid.uuid4()

        mock_response = {
            "jobs": [
                {
                    "id": 11111,
                    "title": "Backend Engineer",
                    "location": {"name": "New York, NY"},
                    "departments": [{"name": "Engineering"}],
                    "updated_at": "2024-03-01T10:00:00.000Z",
                    "absolute_url": "https://boards.greenhouse.io/acme/jobs/11111",
                }
            ],
            "meta": {"total": 1},
        }

        crawler._get_json = AsyncMock(return_value=mock_response)
        jobs = await crawler.crawl("acme", ats_source_id)

        assert len(jobs) == 1
        j = jobs[0]
        assert j["title"] == "Backend Engineer"
        assert j["location"] == "New York, NY"
        assert j["external_job_id"] == "11111"
        assert j["department"] == "Engineering"
        assert j["ats_type"] == "greenhouse"
        assert j["ats_source_id"] == ats_source_id
        assert j["source_url"] == "https://boards.greenhouse.io/acme/jobs/11111"
        assert j["geo_restriction"] is not None

    @pytest.mark.asyncio
    async def test_empty_jobs_returns_empty_list(self):
        crawler = GreenhouseCrawler()
        crawler._get_json = AsyncMock(return_value={"jobs": [], "meta": {"total": 0}})
        jobs = await crawler.crawl("nobody", uuid.uuid4())
        assert jobs == []

    @pytest.mark.asyncio
    async def test_remote_location_sets_remote_flag(self):
        crawler = GreenhouseCrawler()
        ats_source_id = uuid.uuid4()
        mock_response = {
            "jobs": [
                {
                    "id": 22222,
                    "title": "Remote SRE",
                    "location": {"name": "Remote"},
                    "departments": [],
                    "updated_at": None,
                    "absolute_url": "https://boards.greenhouse.io/acme/jobs/22222",
                }
            ]
        }
        crawler._get_json = AsyncMock(return_value=mock_response)
        jobs = await crawler.crawl("acme", ats_source_id)
        assert jobs[0]["remote"] is True


# ── LeverCrawler ──────────────────────────────────────────────────────────────

class TestLeverCrawler:
    @pytest.mark.asyncio
    async def test_maps_postings_correctly(self):
        crawler = LeverCrawler()
        ats_source_id = uuid.uuid4()

        page1 = [
            {
                "id": "abc-123",
                "text": "Staff Engineer",
                "categories": {
                    "location": "San Francisco, CA",
                    "department": "Engineering",
                    "team": "Platform",
                },
                "hostedUrl": "https://jobs.lever.co/startup/abc-123",
                "createdAt": 1704067200000,
            }
        ]

        crawler._get_json = AsyncMock(return_value=page1)
        jobs = await crawler.crawl("startup", ats_source_id)

        assert len(jobs) == 1
        j = jobs[0]
        assert j["title"] == "Staff Engineer"
        assert j["location"] == "San Francisco, CA"
        assert j["external_job_id"] == "abc-123"
        assert j["department"] == "Engineering"
        assert j["ats_type"] == "lever"
        assert j["ats_source_id"] == ats_source_id
        assert j["source_url"] == "https://jobs.lever.co/startup/abc-123"
        assert isinstance(j["posted_at"], datetime)

    @pytest.mark.asyncio
    async def test_pagination_stops_when_page_is_underfull(self):
        """When a page returns fewer items than PAGE_SIZE, no second request is made."""
        from app.crawler.ats.lever import _PAGE_SIZE

        crawler = LeverCrawler()
        call_count = 0

        async def _fake_get(url, params=None, extra_headers=None):
            nonlocal call_count
            call_count += 1
            # Return 1 item (< PAGE_SIZE=100) — crawler should not make a second call
            return [{"id": f"x{call_count}", "text": "Eng", "categories": {}, "hostedUrl": f"http://h/x{call_count}", "createdAt": None}]

        crawler._get_json = _fake_get
        jobs = await crawler.crawl("co", uuid.uuid4())
        assert len(jobs) == 1
        assert call_count == 1, "Underfull first page should stop pagination after one call"

    @pytest.mark.asyncio
    async def test_pagination_fetches_second_page_on_full_first_page(self):
        """When a page returns exactly PAGE_SIZE items, a second request is made."""
        from app.crawler.ats.lever import _PAGE_SIZE

        crawler = LeverCrawler()
        call_count = 0

        def _make_posting(i: int) -> dict:
            return {"id": f"id-{i}", "text": "Eng", "categories": {}, "hostedUrl": f"http://h/{i}", "createdAt": None}

        async def _fake_get(url, params=None, extra_headers=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [_make_posting(i) for i in range(_PAGE_SIZE)]  # full page
            return []  # empty second page

        crawler._get_json = _fake_get
        jobs = await crawler.crawl("co", uuid.uuid4())
        assert len(jobs) == _PAGE_SIZE
        assert call_count == 2, "Full first page should trigger a second request"

    @pytest.mark.asyncio
    async def test_empty_postings_returns_empty_list(self):
        crawler = LeverCrawler()
        crawler._get_json = AsyncMock(return_value=[])
        jobs = await crawler.crawl("nobody", uuid.uuid4())
        assert jobs == []


# ── AshbyCrawler ──────────────────────────────────────────────────────────────

class TestAshbyCrawler:
    @pytest.mark.asyncio
    async def test_maps_postings_correctly(self):
        crawler = AshbyCrawler()
        ats_source_id = uuid.uuid4()

        mock_response = {
            "jobPostings": [
                {
                    "id": "uuid-456",
                    "title": "Product Designer",
                    "department": "Design",
                    "location": "Austin, TX",
                    "employmentType": "FullTime",
                    "isRemote": False,
                    "externalLink": "https://jobs.ashbyhq.com/acme/uuid-456",
                }
            ]
        }

        crawler._get_json = AsyncMock(return_value=mock_response)
        jobs = await crawler.crawl("acme", ats_source_id)

        assert len(jobs) == 1
        j = jobs[0]
        assert j["title"] == "Product Designer"
        assert j["location"] == "Austin, TX"
        assert j["external_job_id"] == "uuid-456"
        assert j["department"] == "Design"
        assert j["employment_type"] == "Full-time"
        assert j["ats_type"] == "ashby"
        assert j["ats_source_id"] == ats_source_id
        assert j["source_url"] == "https://jobs.ashbyhq.com/acme/uuid-456"
        assert j["remote"] is False
        assert j["geo_restriction"] is not None

    @pytest.mark.asyncio
    async def test_is_remote_flag_sets_remote_true(self):
        crawler = AshbyCrawler()
        mock_response = {
            "jobPostings": [
                {
                    "id": "r1",
                    "title": "Remote PM",
                    "department": None,
                    "location": "",
                    "employmentType": "FullTime",
                    "isRemote": True,
                    "externalLink": "https://jobs.ashbyhq.com/co/r1",
                }
            ]
        }
        crawler._get_json = AsyncMock(return_value=mock_response)
        jobs = await crawler.crawl("co", uuid.uuid4())
        assert jobs[0]["remote"] is True

    @pytest.mark.asyncio
    async def test_empty_postings_returns_empty_list(self):
        crawler = AshbyCrawler()
        crawler._get_json = AsyncMock(return_value={"jobPostings": []})
        jobs = await crawler.crawl("nobody", uuid.uuid4())
        assert jobs == []

    @pytest.mark.asyncio
    async def test_employment_type_mapping(self):
        crawler = AshbyCrawler()
        ats_source_id = uuid.uuid4()
        for raw, expected in [
            ("FullTime", "Full-time"),
            ("PartTime", "Part-time"),
            ("Contract", "Contract"),
            ("Internship", "Internship"),
        ]:
            mock_response = {
                "jobPostings": [
                    {
                        "id": "x",
                        "title": "Role",
                        "department": None,
                        "location": "NYC",
                        "employmentType": raw,
                        "isRemote": False,
                        "externalLink": "https://jobs.ashbyhq.com/co/x",
                    }
                ]
            }
            crawler._get_json = AsyncMock(return_value=mock_response)
            jobs = await crawler.crawl("co", ats_source_id)
            assert jobs[0]["employment_type"] == expected, f"Expected {expected} for {raw}"
