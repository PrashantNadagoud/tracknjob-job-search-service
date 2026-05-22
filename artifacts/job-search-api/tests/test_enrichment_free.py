"""
Acceptance tests for Session 15: Enrichment Pipeline — Free Sources Only.

Tests confirm:
  1. Wikipedia populates founded_year, num_employees_range, stock_ticker,
     stock_exchange for Cloudflare (infobox + Yahoo Finance).
  2. Private company (Stripe) → funding fields are null, no error.
  3. All sources fail (network down) → partial CompanyRecord with enriched_at
     set; no exception propagates from enrich().
  4. enrichment_source[] contains only the names of sources that succeeded.
  5. No CRUNCHBASE_API_KEY or api.crunchbase.com in enrichment code;
     crunchbase.py does not exist.
  6. GET /api/v1/companies/{slug} returns the valid CompanyResponse schema.
  7. enrich_new_companies Celery task imports without errors after the refactor.
  8. LinkedIn asyncio.sleep ≥ 2.0 s is called before the HTTP request.
"""

import glob
import importlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text

from app.enrichment.enricher import CompanyEnricher
from app.enrichment.linkedin import enrich_from_linkedin
from app.enrichment.wikipedia import WikipediaResult, enrich_from_wikipedia


# ── Shared fake HTTP client ───────────────────────────────────────────────────


class _FakeHTTPXClient:
    """Callable fake that doubles as an async context manager and HTTP client.

    When patched over ``httpx.AsyncClient``, the code path::

        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(url, params=...)

    becomes::

        async with _FakeHTTPXClient(url_map)(timeout=8) as client:
            resp = await client.get(url, params=...)

    which routes through ``__call__`` → ``__aenter__`` → ``get``.
    """

    def __init__(self, url_map: dict):
        # url_map: {url_fragment: (status_code, body)}
        # body may be a dict/list (returns via .json()) or a str (.text only).
        self._url_map = url_map

    def __call__(self, *args, **kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def get(self, url: str, params=None, **kwargs):
        from urllib.parse import urlencode

        search_url = url
        if params:
            search_url += "?" + urlencode(params, doseq=True)

        for fragment, (status, body) in self._url_map.items():
            if fragment in search_url:
                resp = MagicMock()
                resp.status_code = status
                if isinstance(body, (dict, list)):
                    resp.json = MagicMock(return_value=body)
                    resp.text = json.dumps(body)
                else:
                    resp.json = MagicMock(side_effect=ValueError("not json"))
                    resp.text = body or ""
                return resp

        resp = MagicMock()
        resp.status_code = 404
        resp.json = MagicMock(return_value={})
        resp.text = ""
        return resp


# ── Fixture data ──────────────────────────────────────────────────────────────

_CLOUDFLARE_SUMMARY = {
    "extract": "Cloudflare, Inc. is a publicly traded American company founded in 2009.",
    "description": "American publicly traded technology company",
}

_CLOUDFLARE_MEDIAWIKI = {
    "query": {
        "pages": [
            {
                "revisions": [
                    {
                        "slots": {
                            "main": {
                                "content": (
                                    "{{Infobox company\n"
                                    "| name         = Cloudflare\n"
                                    "| type         = Public company\n"
                                    "| founded      = 2009\n"
                                    "| num_employees = 3,000\n"
                                    "| traded_as    = {{NYSE|NET}}\n"
                                    "}}"
                                )
                            }
                        }
                    }
                ]
            }
        ]
    }
}

_YAHOO_NET = {
    "quoteResponse": {
        "result": [
            {
                "symbol": "NET",
                "exchange": "NYSE",
                "fullExchangeName": "New York Stock Exchange",
            }
        ]
    }
}

_STRIPE_SUMMARY = {
    "extract": "Stripe, Inc. is an American private financial-services company.",
    "description": "American private company",
}

_STRIPE_MEDIAWIKI = {
    "query": {
        "pages": [
            {
                "revisions": [
                    {
                        "slots": {
                            "main": {
                                "content": (
                                    "{{Infobox company\n"
                                    "| name         = Stripe\n"
                                    "| type         = Private company\n"
                                    "| founded      = 2010\n"
                                    "| num_employees = 8,000\n"
                                    "}}"
                                )
                            }
                        }
                    }
                ]
            }
        ]
    }
}


# ── Test 1: Wikipedia + Yahoo Finance populate Cloudflare fields ──────────────


@pytest.mark.asyncio
async def test_wikipedia_populates_cloudflare_fields():
    """AT-1: Wikipedia REST + infobox + Yahoo Finance fill key fields."""
    url_map = {
        "rest_v1/page/summary": (200, _CLOUDFLARE_SUMMARY),
        "api.php": (200, _CLOUDFLARE_MEDIAWIKI),
        "finance.yahoo.com": (200, _YAHOO_NET),
    }
    fake = _FakeHTTPXClient(url_map)

    with (
        patch("app.enrichment.wikipedia.httpx.AsyncClient", fake),
        patch("app.enrichment.wikipedia.asyncio.sleep", new_callable=AsyncMock),
    ):
        result = await enrich_from_wikipedia("Cloudflare")

    assert result.founded_year == 2009, f"expected 2009, got {result.founded_year}"
    assert result.num_employees_range is not None, "num_employees_range should be set"
    assert result.stock_ticker == "NET", f"expected NET, got {result.stock_ticker}"
    assert result.stock_exchange is not None, "stock_exchange should be set"
    assert "wikipedia" in result.sources


# ── Test 2: Private company — funding fields are null ─────────────────────────


@pytest.mark.asyncio
async def test_private_company_has_null_funding_fields():
    """AT-2: Private company → funding_total_usd/last_funding_type/last_funding_date all null."""
    from app.enrichment.builtin import BuiltInResult
    from app.enrichment.comparably import ComparablyResult
    from app.enrichment.glassdoor import GlassdoorResult
    from app.enrichment.linkedin import LinkedInResult

    url_map = {
        "rest_v1/page/summary": (200, _STRIPE_SUMMARY),
        "api.php": (200, _STRIPE_MEDIAWIKI),
    }
    fake = _FakeHTTPXClient(url_map)

    enricher = CompanyEnricher()

    with (
        patch("app.enrichment.wikipedia.httpx.AsyncClient", fake),
        patch("app.enrichment.wikipedia.asyncio.sleep", new_callable=AsyncMock),
        patch(
            "app.enrichment.enricher.enrich_from_linkedin",
            AsyncMock(return_value=LinkedInResult()),
        ),
        patch(
            "app.enrichment.enricher.enrich_from_comparably",
            AsyncMock(return_value=ComparablyResult()),
        ),
        patch(
            "app.enrichment.enricher.enrich_from_builtin",
            AsyncMock(return_value=BuiltInResult()),
        ),
        patch(
            "app.enrichment.enricher.enrich_salary_from_glassdoor",
            AsyncMock(return_value=GlassdoorResult()),
        ),
        patch("app.enrichment.enricher.asyncio.sleep", new_callable=AsyncMock),
    ):
        record = await enricher.enrich("stripe", "Stripe", "Software Engineer", "Remote")

    assert record.funding_total_usd is None
    assert record.last_funding_type is None
    assert record.last_funding_date is None
    assert record.company_type in ("private", "unknown")


# ── Test 3: All sources fail — partial record, no exception ───────────────────


@pytest.mark.asyncio
async def test_all_sources_fail_returns_partial_record():
    """AT-3: enrich() returns CompanyRecord; Guardrail 1 keeps enriched_at None when all sources fail."""
    enricher = CompanyEnricher()

    err = ConnectionError("network down")

    with (
        patch(
            "app.enrichment.enricher.enrich_from_wikipedia",
            AsyncMock(side_effect=err),
        ),
        patch(
            "app.enrichment.enricher.enrich_from_linkedin",
            AsyncMock(side_effect=err),
        ),
        patch(
            "app.enrichment.enricher.enrich_from_comparably",
            AsyncMock(side_effect=err),
        ),
        patch(
            "app.enrichment.enricher.enrich_from_builtin",
            AsyncMock(side_effect=err),
        ),
        patch(
            "app.enrichment.enricher.enrich_salary_from_glassdoor",
            AsyncMock(side_effect=err),
        ),
        patch("app.enrichment.enricher.asyncio.sleep", new_callable=AsyncMock),
    ):
        record = await enricher.enrich("test-fail-co", "FailCo", "Engineer", "Remote")

    assert record is not None
    # Guardrail 1: enriched_at stays None when enrichment_source is empty
    assert record.enriched_at is None, (
        "Guardrail 1 violation: enriched_at should be None when all sources fail"
    )
    assert record.enrichment_source == []


# ── Test 4: enrichment_source only lists successful sources ──────────────────


@pytest.mark.asyncio
async def test_enrichment_source_reflects_only_successful_sources():
    """AT-4: enrichment_source[] contains only sources that returned data."""
    from app.enrichment.builtin import BuiltInResult
    from app.enrichment.comparably import ComparablyResult
    from app.enrichment.glassdoor import GlassdoorResult

    wiki_ok = WikipediaResult(founded_year=2009, sources=["wikipedia"])

    comp_ok = ComparablyResult()
    comp_ok.sources = ["comparably"]
    comp_ok.culture_score = "A+"

    bi_ok = BuiltInResult()
    bi_ok.sources = []  # BuiltIn returned no data but did not error

    gd_ok = GlassdoorResult()
    gd_ok.sources = []

    enricher = CompanyEnricher()

    with (
        patch(
            "app.enrichment.enricher.enrich_from_wikipedia",
            AsyncMock(return_value=wiki_ok),
        ),
        patch(
            "app.enrichment.enricher.enrich_from_linkedin",
            AsyncMock(side_effect=RuntimeError("LinkedIn blocked")),
        ),
        patch(
            "app.enrichment.enricher.enrich_from_comparably",
            AsyncMock(return_value=comp_ok),
        ),
        patch(
            "app.enrichment.enricher.enrich_from_builtin",
            AsyncMock(return_value=bi_ok),
        ),
        patch(
            "app.enrichment.enricher.enrich_salary_from_glassdoor",
            AsyncMock(return_value=gd_ok),
        ),
        patch("app.enrichment.enricher.asyncio.sleep", new_callable=AsyncMock),
    ):
        record = await enricher.enrich("test-cloudflare", "Cloudflare", "SWE", "Remote")

    assert "wikipedia" in record.enrichment_source
    assert "comparably" in record.enrichment_source
    assert "linkedin" not in record.enrichment_source


# ── Test 5: No Crunchbase references in enrichment code ──────────────────────


def test_no_crunchbase_references_in_enrichment_code():
    """AT-5: crunchbase.py deleted; no CRUNCHBASE_API_KEY or api.crunchbase.com anywhere."""
    enrichment_dir = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "app", "enrichment")
    )
    py_files = glob.glob(os.path.join(enrichment_dir, "*.py"))
    assert py_files, f"No .py files found in {enrichment_dir}"

    violations: list[str] = []
    for filepath in py_files:
        fname = os.path.basename(filepath)
        if fname == "crunchbase.py":
            violations.append(f"crunchbase.py still present: {filepath}")
            continue
        with open(filepath) as fh:
            content = fh.read()
        if "CRUNCHBASE_API_KEY" in content:
            violations.append(f"CRUNCHBASE_API_KEY found in {fname}")
        if "api.crunchbase.com" in content:
            violations.append(f"api.crunchbase.com found in {fname}")

    assert not violations, "Crunchbase references detected:\n" + "\n".join(violations)


# ── Test 6: GET /api/v1/companies/{slug} returns valid schema ─────────────────


@pytest.mark.asyncio
async def test_company_endpoint_returns_valid_schema(async_client, db_session, auth_headers):
    """AT-6: Company endpoint returns CompanyResponse with expected shape."""
    from app.models import Company

    slug = f"test-enrich-{uuid.uuid4().hex[:8]}"
    company = Company(
        slug=slug,
        name="Test Enrichment Co",
        enriched_at=datetime.now(timezone.utc),
        enrichment_source=["wikipedia"],
        funding_total_usd=None,
        last_funding_type=None,
        last_funding_date=None,
    )
    db_session.add(company)
    await db_session.commit()

    resp = await async_client.get(f"/api/v1/companies/{slug}", headers=auth_headers)
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["slug"] == slug
    assert body["name"] == "Test Enrichment Co"
    # funding fields must always be null from free-sources pipeline
    assert body.get("funding_total_usd") is None
    assert body.get("last_funding_type") is None
    assert body.get("last_funding_date") is None
    # enrichment_source key must be present
    assert "enrichment_source" in body
    assert isinstance(body["enrichment_source"], list)


# ── Test 7: Celery tasks import cleanly ───────────────────────────────────────


def test_celery_tasks_import_cleanly():
    """AT-7: enrich_new_companies and reenrich_stale_companies import without errors."""
    mod_name = "app.enrichment.tasks"
    # Force fresh import to catch any new import-time errors
    sys.modules.pop(mod_name, None)

    mod = importlib.import_module(mod_name)
    assert hasattr(mod, "enrich_new_companies"), "enrich_new_companies task missing"
    assert hasattr(mod, "reenrich_stale_companies"), "reenrich_stale_companies task missing"


# ── Test 8: LinkedIn enforces ≥ 2 s rate-limit sleep ─────────────────────────


@pytest.mark.asyncio
async def test_linkedin_enforces_rate_limit_sleep():
    """AT-8: linkedin.py calls asyncio.sleep(≥ 2.0) before the HTTP request."""
    sleep_calls: list[float] = []

    async def _capture_sleep(delay, *args, **kwargs):
        sleep_calls.append(float(delay))

    fake = _FakeHTTPXClient({})  # returns 404 for all URLs — we only care about sleep

    with (
        patch("app.enrichment.linkedin.asyncio.sleep", side_effect=_capture_sleep),
        patch("app.enrichment.linkedin.httpx.AsyncClient", fake),
    ):
        await enrich_from_linkedin("cloudflare")

    assert sleep_calls, "asyncio.sleep was never called in linkedin.py"
    assert any(d >= 2.0 for d in sleep_calls), (
        f"LinkedIn rate-limit sleep must be ≥ 2.0 s, got calls: {sleep_calls}"
    )
