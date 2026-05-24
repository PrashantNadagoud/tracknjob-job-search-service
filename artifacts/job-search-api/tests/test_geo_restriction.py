"""Acceptance tests for geo-restriction filtering — Session 14.

Covers:
  1. Greenhouse job with offices.name="Remote - EMEA" → geo_restriction="EU",
     excluded from default US feed.
  2. Greenhouse job with offices.location="United States" → geo_restriction="US",
     included in US feed.
  3. listing location_raw="Remote", no signals, work_type="remote" → "GLOBAL",
     included in US feed.
  4. listing location_raw="Bangalore, India" → "IN", excluded from US feed.
  5. GET /search returns zero EU or IN listings when no ?market= param.
  6. GET /search?market=EU returns only EU + GLOBAL listings.
  7. Backfill logic: classify_listing correctly categorises a batch of rows.
  8. Legacy geo_restriction=NULL rows are still served in the default US feed.
"""

import uuid
from datetime import datetime, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.crawler.geo_classifier import (
    classify_listing,
    parse_greenhouse_location,
)
from app.models import Listing
from tests.conftest import TEST_USER_ID


# ---------------------------------------------------------------------------
# Unit-level: parse_greenhouse_location
# ---------------------------------------------------------------------------

class TestParseGreenhouseLocation:
    def test_emea_office_name_returns_eu(self):
        job = {
            "offices": [{"name": "Remote - EMEA", "location": ""}],
            "location": {"name": "Remote"},
        }
        location_raw, country_code = parse_greenhouse_location(job)
        assert country_code == "EU"

    def test_us_office_location_returns_us(self):
        job = {
            "offices": [{"name": "HQ", "location": "United States"}],
            "location": {"name": "Remote"},
        }
        location_raw, country_code = parse_greenhouse_location(job)
        assert country_code == "US"

    def test_empty_offices_falls_back_to_location_name(self):
        job = {
            "offices": [],
            "location": {"name": "San Francisco, CA"},
        }
        location_raw, country_code = parse_greenhouse_location(job)
        assert location_raw == "San Francisco, CA"
        assert country_code is None


# ---------------------------------------------------------------------------
# Unit-level: classify_listing
# ---------------------------------------------------------------------------

class TestClassifyListing:
    def test_greenhouse_emea_office_classifies_as_eu(self):
        """Acceptance test 1: Greenhouse EMEA office → EU."""
        result = classify_listing(
            location_raw="remote - emea",
            description="",
            work_type="remote",
            country="EU",
        )
        assert result == "EU"

    def test_greenhouse_us_office_classifies_as_us(self):
        """Acceptance test 2: Greenhouse US office → US."""
        result = classify_listing(
            location_raw="Remote",
            description="",
            work_type="remote",
            country="US",
        )
        assert result == "US"

    def test_plain_remote_no_signals_classifies_as_global(self):
        """Acceptance test 3: 'Remote' with no geo signals → GLOBAL."""
        result = classify_listing(
            location_raw="Remote",
            description="",
            work_type="remote",
            country=None,
        )
        assert result == "GLOBAL"

    def test_bangalore_india_classifies_as_in(self):
        """Acceptance test 4: 'Bangalore, India' → IN."""
        result = classify_listing(
            location_raw="Bangalore, India",
            description="",
            work_type="",
            country=None,
        )
        assert result == "IN"

    def test_structured_country_in_returns_in(self):
        result = classify_listing(
            location_raw="Remote",
            description="",
            work_type="remote",
            country="IN",
        )
        assert result == "IN"

    def test_structured_country_iso_gb_returns_eu(self):
        result = classify_listing(
            location_raw="London",
            description="",
            work_type="",
            country="GB",
        )
        assert result == "EU"

    def test_description_signal_eu_beats_no_location(self):
        result = classify_listing(
            location_raw="",
            description="You must have the right to work in Europe. CET timezone preferred.",
            work_type="remote",
            country=None,
        )
        assert result == "EU"

    def test_us_signal_beats_eu_in_description(self):
        result = classify_listing(
            location_raw="Remote US",
            description="We also have offices in Germany and France.",
            work_type="remote",
            country=None,
        )
        assert result == "US"


# ---------------------------------------------------------------------------
# Integration: search endpoint geo filtering
# ---------------------------------------------------------------------------

async def _insert_listing(
    db: AsyncSession,
    *,
    geo_restriction: str | None,
    suffix: str,
) -> Listing:
    listing = Listing(
        title=f"Geo Test Job {suffix}",
        company="GeoTestCorp",
        location="Somewhere",
        remote=True,
        source_url=f"http://test-geo-{suffix}-{uuid.uuid4().hex}",
        source_label="GeoTestCorp Careers",
        posted_at=datetime.now(timezone.utc),
        country="US",
        last_seen_at=datetime.now(timezone.utc),
        is_active=True,
        geo_restriction=geo_restriction,
    )
    db.add(listing)
    await db.flush()
    await db.refresh(listing)
    await db.commit()
    return listing


@pytest.mark.asyncio
async def test_default_feed_excludes_eu_and_in(
    async_client: AsyncClient,
    db_session: AsyncSession,
    auth_headers: dict,
):
    """Acceptance test 5: Default US feed excludes EU and IN listings."""
    eu_job = await _insert_listing(db_session, geo_restriction="EU", suffix="eu-only")
    in_job = await _insert_listing(db_session, geo_restriction="IN", suffix="in-only")

    resp = await async_client.get(
        "/api/v1/jobs/search",
        params={"company": "GeoTestCorp"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()

    returned_ids = {r["id"] for r in data["results"]}
    assert str(eu_job.id) not in returned_ids, "EU job should not appear in default US feed"
    assert str(in_job.id) not in returned_ids, "IN job should not appear in default US feed"


@pytest.mark.asyncio
async def test_default_feed_includes_us_and_global(
    async_client: AsyncClient,
    db_session: AsyncSession,
    auth_headers: dict,
):
    """Default US feed includes US, GLOBAL, and NULL (legacy) listings."""
    us_job = await _insert_listing(db_session, geo_restriction="US", suffix="us-yes")
    global_job = await _insert_listing(db_session, geo_restriction="GLOBAL", suffix="global-yes")
    null_job = await _insert_listing(db_session, geo_restriction=None, suffix="null-yes")

    resp = await async_client.get(
        "/api/v1/jobs/search",
        params={"company": "GeoTestCorp"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()

    returned_ids = {r["id"] for r in data["results"]}
    assert str(us_job.id) in returned_ids, "US job must appear in default feed"
    assert str(global_job.id) in returned_ids, "GLOBAL job must appear in default feed"
    assert str(null_job.id) in returned_ids, "Legacy NULL row must appear in default feed"


@pytest.mark.asyncio
async def test_market_eu_returns_eu_and_global_only(
    async_client: AsyncClient,
    db_session: AsyncSession,
    auth_headers: dict,
):
    """Acceptance test 6: ?market=EU returns only EU + GLOBAL listings."""
    eu_job = await _insert_listing(db_session, geo_restriction="EU", suffix="eu-mkt")
    global_job = await _insert_listing(db_session, geo_restriction="GLOBAL", suffix="glb-mkt")
    us_job = await _insert_listing(db_session, geo_restriction="US", suffix="us-mkt")
    in_job = await _insert_listing(db_session, geo_restriction="IN", suffix="in-mkt")

    resp = await async_client.get(
        "/api/v1/jobs/search",
        params={"company": "GeoTestCorp", "market": "EU"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()

    returned_ids = {r["id"] for r in data["results"]}
    assert str(eu_job.id) in returned_ids, "EU job must appear in EU market"
    assert str(global_job.id) in returned_ids, "GLOBAL job must appear in EU market"
    assert str(us_job.id) not in returned_ids, "US job must not appear in EU market"
    assert str(in_job.id) not in returned_ids, "IN job must not appear in EU market"


@pytest.mark.asyncio
async def test_market_in_returns_in_and_global_only(
    async_client: AsyncClient,
    db_session: AsyncSession,
    auth_headers: dict,
):
    """?market=IN returns only IN + GLOBAL listings."""
    in_job = await _insert_listing(db_session, geo_restriction="IN", suffix="in-in")
    global_job = await _insert_listing(db_session, geo_restriction="GLOBAL", suffix="glb-in")
    eu_job = await _insert_listing(db_session, geo_restriction="EU", suffix="eu-in")

    resp = await async_client.get(
        "/api/v1/jobs/search",
        params={"company": "GeoTestCorp", "market": "IN"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()

    returned_ids = {r["id"] for r in data["results"]}
    assert str(in_job.id) in returned_ids
    assert str(global_job.id) in returned_ids
    assert str(eu_job.id) not in returned_ids


@pytest.mark.asyncio
async def test_legacy_null_row_appears_in_default_feed(
    async_client: AsyncClient,
    db_session: AsyncSession,
    auth_headers: dict,
):
    """Acceptance test 8: NULL geo_restriction rows still served in default US feed."""
    null_job = await _insert_listing(db_session, geo_restriction=None, suffix="legacy-null")

    resp = await async_client.get(
        "/api/v1/jobs/search",
        params={"company": "GeoTestCorp"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    returned_ids = {r["id"] for r in resp.json()["results"]}
    assert str(null_job.id) in returned_ids


@pytest.mark.asyncio
async def test_no_500_after_migration(
    async_client: AsyncClient,
    auth_headers: dict,
):
    """Acceptance test 8 (partial): no endpoint returns 500 after migration."""
    resp = await async_client.get("/api/v1/jobs/search", headers=auth_headers)
    assert resp.status_code == 200

    resp2 = await async_client.get("/api/v1/jobs/sources", headers=auth_headers)
    assert resp2.status_code == 200


# ---------------------------------------------------------------------------
# Acceptance test 7: Backfill classify_listing logic on a mock batch
# ---------------------------------------------------------------------------

class TestBackfillLogic:
    """Verify that the backfill classification logic works correctly on
    representative rows — without actually running the standalone script."""

    _cases = [
        # (location, description, remote, country_hint, expected_geo)
        ("San Francisco, CA", "", False, "US", "US"),
        ("Remote", "", True, None, "GLOBAL"),
        ("Berlin, Germany", "", False, None, "EU"),
        ("Bangalore, India", "", False, None, "IN"),
        ("Hyderabad", "", False, "IN", "IN"),
        ("Remote", "Must be located in the US", True, None, "US"),
        ("Remote - EMEA", "", True, "EU", "EU"),
        ("", "", False, None, "US"),  # no signals → default US
    ]

    def test_backfill_classifications(self):
        for location, description, remote, country_hint, expected in self._cases:
            work_type = "remote" if remote else ""
            result = classify_listing(
                location_raw=location,
                description=description,
                work_type=work_type,
                country=country_hint,
            )
            assert result == expected, (
                f"classify_listing({location!r}, remote={remote}, country={country_hint!r}) "
                f"returned {result!r}, expected {expected!r}"
            )
