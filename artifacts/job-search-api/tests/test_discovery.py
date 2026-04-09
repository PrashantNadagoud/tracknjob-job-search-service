"""Tests for the YC Discovery & ATS Seed Pipeline (Task #7).

Covers:
    - YCScraper filtering (inactive, acquired, no website)
    - YCScraper JSON vs HTML fallback
    - ATSProber._derive_slug priority logic
    - ATSProber.probe() — match / 404-skip / 429-warn / no-match
    - SeedOrchestrator.run() — dry_run, skipped, matched, rejected
    - GET /api/v1/admin/seed-status response shape
    - app.tasks.run_yc_seed Celery task signature
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient, ASGITransport

from app.discovery.ats_prober import ATSProber, _derive_slug, ATS_PROBE_PATTERNS
from app.discovery.yc_scraper import YCScraper


# ── YCScraper ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_yc_scraper_filters_no_website():
    raw = [
        {"name": "HasWebsite", "website": "https://example.com", "slug": "ex", "status": "Active"},
        {"name": "NoWebsite", "website": None, "slug": "nw", "status": "Active"},
        {"name": "EmptyWebsite", "website": "", "slug": "ew", "status": "Active"},
    ]

    async def _fake_fetch_json(self):
        return raw

    with patch.object(YCScraper, "_fetch_json", _fake_fetch_json):
        scraper = YCScraper()
        result = await scraper.fetch()

    names = [c["name"] for c in result]
    assert "HasWebsite" in names
    assert "NoWebsite" not in names
    assert "EmptyWebsite" not in names


@pytest.mark.asyncio
async def test_yc_scraper_filters_inactive_and_acquired():
    raw = [
        {"name": "Active", "website": "https://a.com", "slug": "a", "status": "Active"},
        {"name": "Inactive", "website": "https://b.com", "slug": "b", "status": "Inactive"},
        {"name": "Acquired", "website": "https://c.com", "slug": "c", "status": "Acquired"},
        {"name": "Dead", "website": "https://d.com", "slug": "d", "status": "dead"},
    ]

    async def _fake_fetch_json(self):
        return raw

    with patch.object(YCScraper, "_fetch_json", _fake_fetch_json):
        scraper = YCScraper()
        result = await scraper.fetch()

    names = [c["name"] for c in result]
    assert names == ["Active"]


@pytest.mark.asyncio
async def test_yc_scraper_falls_back_to_html_on_403():
    html_companies = [
        {"name": "HTMLCo", "website": "https://htmlco.com", "slug": "htmlco"},
    ]

    async def _fake_fetch_json(self):
        return None

    async def _fake_scrape_html(self):
        return html_companies

    with (
        patch.object(YCScraper, "_fetch_json", _fake_fetch_json),
        patch.object(YCScraper, "_scrape_html", _fake_scrape_html),
    ):
        scraper = YCScraper()
        result = await scraper.fetch()

    assert len(result) == 1
    assert result[0]["name"] == "HTMLCo"


@pytest.mark.asyncio
async def test_yc_scraper_normalises_website():
    raw = [
        {"name": "NoProt", "website": "example.com", "slug": "np", "status": "Active"},
    ]

    async def _fake_fetch_json(self):
        return raw

    with patch.object(YCScraper, "_fetch_json", _fake_fetch_json):
        scraper = YCScraper()
        result = await scraper.fetch()

    assert result[0]["website"] == "https://example.com"


# ── _derive_slug ──────────────────────────────────────────────────────────────


def test_derive_slug_prefers_yc_slug():
    company = {"yc_slug": "stripe", "website": "https://stripe.com", "name": "Stripe"}
    assert _derive_slug(company) == "stripe"


def test_derive_slug_falls_back_to_website():
    company = {"yc_slug": "", "website": "https://www.stripe.com", "name": "Stripe"}
    slug = _derive_slug(company)
    assert slug == "stripe"


def test_derive_slug_falls_back_to_name():
    company = {"yc_slug": None, "website": None, "name": "My Cool Co"}
    slug = _derive_slug(company)
    assert slug == "my-cool-co"


def test_derive_slug_name_cleans_special_chars():
    company = {"yc_slug": None, "website": None, "name": "A.B.C & D!"}
    slug = _derive_slug(company)
    assert "&" not in slug
    assert "!" not in slug


# ── ATSProber.probe ───────────────────────────────────────────────────────────


class _FakeResponse:
    def __init__(self, status_code: int, body: Any = None):
        self.status_code = status_code
        self.is_success = 200 <= status_code < 300
        self._body = body

    def json(self):
        return self._body


@pytest.mark.asyncio
async def test_prober_returns_none_on_all_404():
    """All patterns returning 404 → probe() returns None."""

    async def _fake_get(self, url, **kw):
        return _FakeResponse(404)

    async def _fake_post(self, url, **kw):
        return _FakeResponse(404)

    with (
        patch("httpx.AsyncClient.get", _fake_get),
        patch("httpx.AsyncClient.post", _fake_post),
    ):
        prober = ATSProber()
        result = await prober.probe({"name": "NoATS", "website": "https://noats.io", "yc_slug": "noats"})

    assert result is None


@pytest.mark.asyncio
async def test_prober_returns_match_on_greenhouse_success():
    """Greenhouse returning 200 with 'jobs' key → match returned."""
    called: list[str] = []

    async def _fake_get(self, url, **kw):
        called.append(url)
        if "greenhouse.io" in url:
            return _FakeResponse(200, {"jobs": [{"id": 1}]})
        return _FakeResponse(404)

    async def _fake_post(self, url, **kw):
        return _FakeResponse(404)

    with (
        patch("httpx.AsyncClient.get", _fake_get),
        patch("httpx.AsyncClient.post", _fake_post),
    ):
        prober = ATSProber()
        result = await prober.probe(
            {"name": "GreenComp", "website": "https://greencomp.com", "yc_slug": "greencomp"}
        )

    assert result is not None
    assert result["ats_type"] == "greenhouse"
    assert result["ats_slug"] == "greencomp"
    assert "greenhouse.io" in result["crawl_url"]


@pytest.mark.asyncio
async def test_prober_skips_429_with_warning(caplog):
    """429 on all patterns → probe() returns None and logs a warning."""
    import logging

    async def _fake_get(self, url, **kw):
        return _FakeResponse(429)

    async def _fake_post(self, url, **kw):
        return _FakeResponse(429)

    with (
        patch("httpx.AsyncClient.get", _fake_get),
        patch("httpx.AsyncClient.post", _fake_post),
        caplog.at_level(logging.WARNING, logger="app.discovery.ats_prober"),
    ):
        prober = ATSProber()
        result = await prober.probe(
            {"name": "RateComp", "website": "https://ratecomp.com", "yc_slug": "ratecomp"}
        )

    assert result is None
    assert any("429" in m or "Rate limited" in m for m in caplog.messages)


# ── SeedOrchestrator ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_seed_orchestrator_dry_run_skips_db_writes():
    """dry_run=True: counts are accurate but no DB writes happen."""
    from app.discovery.seed_orchestrator import SeedOrchestrator

    companies = [
        {"name": "Stripe", "website": "https://stripe.com", "yc_slug": "stripe"},
        {"name": "Airbnb", "website": "https://airbnb.com", "yc_slug": "airbnb"},
    ]

    async def _fake_fetch(self):
        return companies

    async def _fake_probe(self, company):
        if company["name"] == "Stripe":
            return {"ats_type": "greenhouse", "ats_slug": "stripe", "crawl_url": "https://boards-api.greenhouse.io/v1/boards/stripe/jobs"}
        return None

    fake_session = MagicMock()
    fake_session.execute = AsyncMock(return_value=MagicMock(fetchall=lambda: [], scalar=lambda: 0))
    fake_session.commit = AsyncMock()

    with (
        patch.object(YCScraper, "fetch", _fake_fetch),
        patch.object(ATSProber, "probe", _fake_probe),
    ):
        orchestrator = SeedOrchestrator(db_session=fake_session, batch_size=50)
        counts = await orchestrator.run(market="US", dry_run=True)

    assert counts["total"] == 2
    assert counts["probed"] == 2
    assert counts["matched"] == 1
    assert counts["rejected"] == 1
    fake_session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_seed_orchestrator_skips_known_websites():
    """Companies whose website is already in existing set are skipped."""
    from app.discovery.seed_orchestrator import SeedOrchestrator

    companies = [
        {"name": "Stripe", "website": "https://stripe.com", "yc_slug": "stripe"},
        {"name": "NewCo", "website": "https://newco.io", "yc_slug": "newco"},
    ]

    async def _fake_fetch(self):
        return companies

    async def _fake_probe(self, company):
        return None

    probe_calls: list[str] = []
    original_probe = _fake_probe

    async def _counting_probe(self, company):
        probe_calls.append(company["name"])
        return None

    fake_execute_result = MagicMock()
    fake_execute_result.fetchall.return_value = [("https://stripe.com",)]
    fake_execute_result.scalar.return_value = 0

    fake_session = MagicMock()
    fake_session.execute = AsyncMock(return_value=fake_execute_result)
    fake_session.commit = AsyncMock()

    with (
        patch.object(YCScraper, "fetch", _fake_fetch),
        patch.object(ATSProber, "probe", _counting_probe),
    ):
        orchestrator = SeedOrchestrator(db_session=fake_session, batch_size=50)
        counts = await orchestrator.run(market="US", dry_run=True)

    assert counts["skipped"] == 1
    assert "Stripe" not in probe_calls
    assert "NewCo" in probe_calls


# ── Admin /seed-status endpoint ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_seed_status_endpoint_shape(async_client):
    """GET /api/v1/admin/seed-status returns expected top-level keys."""
    resp = await async_client.get("/api/v1/admin/seed-status")
    assert resp.status_code == 200
    body = resp.json()
    assert "discovery_queue" in body
    assert "ats_sources" in body
    assert "last_crawl_run" in body
    assert "total_active_listings" in body
    assert "by_status" in body["discovery_queue"]
    assert "total" in body["discovery_queue"]
    assert "by_type" in body["ats_sources"]
    assert "active" in body["ats_sources"]
    assert "inactive" in body["ats_sources"]


# ── Celery task ───────────────────────────────────────────────────────────────


def test_run_yc_seed_task_is_registered():
    """run_yc_seed task is discoverable via the Celery app."""
    from app.celery_app import celery_app
    import app.tasks  # noqa: F401

    assert "app.tasks.run_yc_seed" in celery_app.tasks


def test_run_yc_seed_task_has_correct_config():
    """run_yc_seed task has bind=True and max_retries=0."""
    import app.tasks  # noqa: F401
    from app.celery_app import celery_app

    task = celery_app.tasks["app.tasks.run_yc_seed"]
    assert task.max_retries == 0


def test_ats_probe_patterns_cover_all_7_types():
    """ATS_PROBE_PATTERNS covers all 7 required ATS types."""
    expected = {"greenhouse", "lever", "ashby", "workday", "bamboohr", "smartrecruiters", "rippling"}
    actual = {p["ats_type"] for p in ATS_PROBE_PATTERNS}
    assert expected == actual
