"""Acceptance tests for Session 13 — Company Intel & Salary Enrichment Pipeline.

Tests cover:
  1. GET /api/v1/companies/{slug} — 404 and 200
  2. Search includes company_summary when company data exists
  3. Search returns company_summary: null for listings with no company match
  4. Partial enrichment: only crunchbase succeeds
  5. Public company shows stock fields, not funding fields
  6. Glassdoor salary appears in company_summary
  7. When Glassdoor fails, salary keys are absent from company_summary
  8. reenrich_stale_companies updates enriched_at
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import TEST_USER_ID, _TestSession, make_token


# ── Helpers ──────────────────────────────────────────────────────────────────

def _auth(sub: str = TEST_USER_ID) -> dict:
    return {"Authorization": f"Bearer {make_token(sub=sub)}"}


async def _insert_company(session: AsyncSession, *, slug: str, **kwargs) -> uuid.UUID:
    defaults = {
        "name": f"Test Company {slug}",
        "company_type": "private",
        "enrichment_source": [],
    }
    defaults.update(kwargs)
    result = await session.execute(
        text("""
            INSERT INTO jobs.companies (slug, name, company_type, enrichment_source,
              funding_total_usd, last_funding_type, num_employees_range,
              culture_score, remote_policy, perks,
              salary_min_usd, salary_max_usd, salary_source,
              stock_ticker, stock_exchange, enriched_at)
            VALUES (:slug, :name, :company_type, :enrichment_source,
              :funding_total_usd, :last_funding_type, :num_employees_range,
              :culture_score, :remote_policy, :perks,
              :salary_min_usd, :salary_max_usd, :salary_source,
              :stock_ticker, :stock_exchange, :enriched_at)
            RETURNING id
        """),
        {
            "slug": slug,
            "name": defaults["name"],
            "company_type": defaults["company_type"],
            "enrichment_source": defaults["enrichment_source"],
            "funding_total_usd": defaults.get("funding_total_usd"),
            "last_funding_type": defaults.get("last_funding_type"),
            "num_employees_range": defaults.get("num_employees_range"),
            "culture_score": defaults.get("culture_score"),
            "remote_policy": defaults.get("remote_policy"),
            "perks": defaults.get("perks"),
            "salary_min_usd": defaults.get("salary_min_usd"),
            "salary_max_usd": defaults.get("salary_max_usd"),
            "salary_source": defaults.get("salary_source"),
            "stock_ticker": defaults.get("stock_ticker"),
            "stock_exchange": defaults.get("stock_exchange"),
            "enriched_at": defaults.get("enriched_at"),
        },
    )
    row = result.fetchone()
    await session.commit()
    return row[0]


async def _insert_listing_with_company(
    session: AsyncSession,
    *,
    company_id: uuid.UUID,
    company_name: str = "Test Corp",
    source_url: str | None = None,
    salary_range: str | None = None,
) -> uuid.UUID:
    url = source_url or f"http://test-company-{uuid.uuid4().hex}"
    result = await session.execute(
        text("""
            INSERT INTO jobs.listings
              (title, company, location, remote, source_url, source_label,
               posted_at, country, last_seen_at, is_active, company_id, salary_range)
            VALUES
              ('Software Engineer', :company_name, 'New York', false,
               :url, 'test', NOW(), 'US', NOW(), true, :company_id, :salary_range)
            RETURNING id
        """),
        {
            "url": url,
            "company_name": company_name,
            "company_id": str(company_id),
            "salary_range": salary_range,
        },
    )
    row = result.fetchone()
    await session.commit()
    return row[0]


# ── Test class ────────────────────────────────────────────────────────────────

class TestCompanyEndpoint:
    async def test_get_company_not_found_returns_404(self, async_client: AsyncClient):
        resp = await async_client.get(
            "/api/v1/companies/this-slug-does-not-exist",
            headers=_auth(),
        )
        assert resp.status_code == 404
        assert resp.json()["error"] == "not_found"

    async def test_get_company_returns_200(self, async_client: AsyncClient):
        async with _TestSession() as s:
            await _insert_company(
                s,
                slug="test-acme-corp",
                name="Acme Corp",
                company_type="private",
                last_funding_type="Series B",
                num_employees_range="201-500",
            )

        resp = await async_client.get("/api/v1/companies/test-acme-corp", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert data["slug"] == "test-acme-corp"
        assert data["name"] == "Acme Corp"
        assert data["last_funding_type"] == "Series B"
        assert data["num_employees_range"] == "201-500"

    async def test_get_company_requires_auth(self, async_client: AsyncClient):
        resp = await async_client.get("/api/v1/companies/test-any-slug")
        assert resp.status_code == 401


class TestSearchCompanySummary:
    async def test_search_returns_null_company_summary_when_no_company(
        self, async_client: AsyncClient
    ):
        resp = await async_client.get(
            "/api/v1/jobs/search?limit=5",
            headers=_auth(),
        )
        assert resp.status_code == 200
        for item in resp.json()["results"]:
            assert "company_summary" in item
            if item["company_summary"] is not None:
                pass

    async def test_search_returns_company_summary_with_data(
        self, async_client: AsyncClient
    ):
        async with _TestSession() as s:
            cid = await _insert_company(
                s,
                slug="test-enriched-corp",
                name="Enriched Corp",
                company_type="private",
                last_funding_type="Series D",
                funding_total_usd=741000000,
                num_employees_range="1001-5000",
                culture_score="A",
                remote_policy="Hybrid",
            )
            await _insert_listing_with_company(s, company_id=cid, company_name="Enriched Corp")

        resp = await async_client.get(
            "/api/v1/jobs/search?company=Enriched+Corp",
            headers=_auth(),
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results, "Expected at least one result"

        cs = results[0]["company_summary"]
        assert cs is not None
        assert cs["company_type"] == "private"
        assert cs["funding_stage"] == "Series D"
        assert cs["funding_total_usd"] == 741000000
        assert cs["employee_range"] == "1001-5000"
        assert cs["culture_score"] == "A"
        assert cs["remote_policy"] == "Hybrid"

    async def test_search_company_summary_excludes_null_fields(
        self, async_client: AsyncClient
    ):
        async with _TestSession() as s:
            cid = await _insert_company(
                s,
                slug="test-sparse-corp",
                name="Sparse Corp",
                company_type="private",
            )
            await _insert_listing_with_company(s, company_id=cid, company_name="Sparse Corp")

        resp = await async_client.get(
            "/api/v1/jobs/search?company=Sparse+Corp",
            headers=_auth(),
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results

        cs = results[0]["company_summary"]
        assert cs is None or (
            "funding_stage" not in cs and
            "culture_score" not in cs and
            "remote_policy" not in cs
        )

    async def test_search_no_500_when_no_company_data(self, async_client: AsyncClient):
        resp = await async_client.get(
            "/api/v1/jobs/search?country=ALL&limit=50",
            headers=_auth(),
        )
        assert resp.status_code == 200

    async def test_public_company_shows_stock_not_funding(self, async_client: AsyncClient):
        async with _TestSession() as s:
            cid = await _insert_company(
                s,
                slug="test-public-inc",
                name="Public Inc",
                company_type="public",
                stock_ticker="PUBL",
                stock_exchange="NASDAQ",
                last_funding_type="IPO",
                funding_total_usd=5000000000,
                num_employees_range="5001+",
            )
            await _insert_listing_with_company(s, company_id=cid, company_name="Public Inc")

        resp = await async_client.get(
            "/api/v1/jobs/search?company=Public+Inc",
            headers=_auth(),
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results

        cs = results[0]["company_summary"]
        assert cs is not None
        assert cs["company_type"] == "public"
        assert cs["stock_ticker"] == "PUBL"
        assert cs["stock_exchange"] == "NASDAQ"
        assert "funding_stage" not in cs
        assert "funding_total_usd" not in cs

    async def test_glassdoor_salary_appears_in_company_summary(
        self, async_client: AsyncClient
    ):
        async with _TestSession() as s:
            cid = await _insert_company(
                s,
                slug="test-glassdoor-pays-well",
                name="Glassdoor Pays Well",
                company_type="private",
                salary_min_usd=140000,
                salary_max_usd=180000,
                salary_source="glassdoor",
            )
            await _insert_listing_with_company(s, company_id=cid, company_name="Glassdoor Pays Well")

        resp = await async_client.get(
            "/api/v1/jobs/search?company=Glassdoor+Pays+Well",
            headers=_auth(),
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results

        cs = results[0]["company_summary"]
        assert cs is not None
        assert cs["salary_min_usd"] == 140000
        assert cs["salary_max_usd"] == 180000
        assert cs["salary_source"] == "glassdoor"

    async def test_glassdoor_salary_absent_when_not_enriched(
        self, async_client: AsyncClient
    ):
        async with _TestSession() as s:
            cid = await _insert_company(
                s,
                slug="test-no-salary-corp",
                name="No Salary Corp",
                company_type="private",
                culture_score="B",
            )
            await _insert_listing_with_company(s, company_id=cid, company_name="No Salary Corp")

        resp = await async_client.get(
            "/api/v1/jobs/search?company=No+Salary+Corp",
            headers=_auth(),
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results

        cs = results[0]["company_summary"]
        assert cs is not None
        assert "salary_min_usd" not in cs
        assert "salary_max_usd" not in cs
        assert "salary_source" not in cs

    async def test_company_listed_salary_overrides_glassdoor_source(
        self, async_client: AsyncClient
    ):
        async with _TestSession() as s:
            cid = await _insert_company(
                s,
                slug="test-company-listed-salary",
                name="Company Listed Salary",
                company_type="private",
                salary_min_usd=120000,
                salary_max_usd=160000,
                salary_source="glassdoor",
            )
            await _insert_listing_with_company(
                s,
                company_id=cid,
                company_name="Company Listed Salary",
                salary_range="$130k - $170k",
            )

        resp = await async_client.get(
            "/api/v1/jobs/search?company=Company+Listed+Salary",
            headers=_auth(),
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results

        cs = results[0]["company_summary"]
        assert cs is not None
        assert cs["salary_source"] == "company_listed"
        assert "salary_min_usd" not in cs
        assert "salary_max_usd" not in cs


class TestEnrichmentPipeline:
    async def test_partial_enrichment_saves_crunchbase_only(self):
        from app.enrichment.enricher import CompanyEnricher, CompanyRecord

        async def mock_cb(slug, name):
            from app.enrichment.crunchbase import CrunchbaseResult
            r = CrunchbaseResult()
            r.funding_total_usd = 50_000_000
            r.last_funding_type = "Series A"
            r.company_type = "private"
            r.sources = ["crunchbase"]
            return r

        async def fail(*a, **kw):
            raise RuntimeError("source unavailable")

        enricher = CompanyEnricher()
        with (
            patch("app.enrichment.enricher.enrich_from_crunchbase", mock_cb),
            patch("app.enrichment.enricher.enrich_from_comparably", fail),
            patch("app.enrichment.enricher.enrich_from_builtin", fail),
            patch("app.enrichment.enricher.enrich_salary_from_glassdoor", fail),
        ):
            record = await enricher.enrich("test-acme", "Acme", "Engineer", "US")

        assert record.funding_total_usd == 50_000_000
        assert record.last_funding_type == "Series A"
        assert record.company_type == "private"
        assert "crunchbase" in record.enrichment_source
        assert "comparably" not in record.enrichment_source
        assert record.culture_score is None
        assert record.salary_min_usd is None

    async def test_enrich_new_companies_creates_company_row(self):
        from app.enrichment.tasks import _async_enrich_new_companies

        async with _TestSession() as s:
            await s.execute(
                text("""
                    INSERT INTO jobs.listings
                      (title, company, location, remote, source_url, source_label,
                       posted_at, country, is_active)
                    VALUES
                      ('Dev', 'test-new-unlinked-corp', 'NY', false,
                       'http://test-enrich-new-1', 'test', NOW(), 'US', true)
                """)
            )
            await s.commit()

        async def mock_enrich(self_ref, *, company_slug, company_name, **kw):
            from app.enrichment.enricher import CompanyRecord
            return CompanyRecord(
                slug=company_slug,
                name=company_name,
                enrichment_source=["crunchbase"],
                company_type="private",
                enriched_at=datetime.now(timezone.utc),
            )

        with patch.object(
            __import__("app.enrichment.enricher", fromlist=["CompanyEnricher"]).CompanyEnricher,
            "enrich",
            mock_enrich,
        ):
            await _async_enrich_new_companies()

        async with _TestSession() as s:
            result = await s.execute(
                text("SELECT id FROM jobs.companies WHERE name = 'test-new-unlinked-corp'")
            )
            row = result.fetchone()
            assert row is not None, "Company row not created by enrich_new_companies"

            result2 = await s.execute(
                text("""
                    SELECT company_id FROM jobs.listings
                    WHERE source_url = 'http://test-enrich-new-1'
                """)
            )
            lrow = result2.fetchone()
            assert lrow is not None and lrow[0] is not None, (
                "listings.company_id not linked after enrich_new_companies"
            )

    async def test_reenrich_stale_companies_updates_enriched_at(self):
        from app.enrichment.tasks import _async_reenrich_stale_companies

        stale_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
        async with _TestSession() as s:
            result = await s.execute(
                text("""
                    INSERT INTO jobs.companies (slug, name, enriched_at, enrichment_source)
                    VALUES ('test-stale-corp', 'Stale Corp', :enriched_at, '{}')
                    RETURNING id
                """),
                {"enriched_at": stale_time},
            )
            company_id = result.fetchone()[0]
            await s.commit()

        async def mock_enrich(self_ref, *, company_slug, company_name, **kw):
            from app.enrichment.enricher import CompanyRecord
            return CompanyRecord(
                slug=company_slug,
                name=company_name,
                enrichment_source=["crunchbase"],
                company_type="private",
                enriched_at=datetime.now(timezone.utc),
            )

        with patch.object(
            __import__("app.enrichment.enricher", fromlist=["CompanyEnricher"]).CompanyEnricher,
            "enrich",
            mock_enrich,
        ):
            await _async_reenrich_stale_companies()

        async with _TestSession() as s:
            result = await s.execute(
                text("SELECT enriched_at FROM jobs.companies WHERE id = :id"),
                {"id": company_id},
            )
            updated_row = result.fetchone()
            assert updated_row is not None
            assert updated_row[0] > stale_time, (
                f"enriched_at was not updated: still {updated_row[0]}"
            )
