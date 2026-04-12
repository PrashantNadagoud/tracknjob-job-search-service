"""Tests for _async_reactivate_sources.

Covers:
- Errored non-probe AtsSource rows ARE reactivated.
- Errored probe AtsSource rows (company slug ending with '-probe') are NOT
  reactivated, preserving the discovery queue's controlled lifecycle.
- AtsSource rows with non-error statuses remain untouched.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import _TestSession


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _insert_company(session: AsyncSession, slug: str) -> uuid.UUID:
    result = await session.execute(
        text(
            """
            INSERT INTO jobs.companies (slug, name)
            VALUES (:slug, :name)
            RETURNING id
            """
        ),
        {"slug": slug, "name": f"Company {slug}"},
    )
    return result.scalar_one()


async def _insert_ats_source(
    session: AsyncSession,
    company_id: uuid.UUID,
    *,
    ats_type: str = "workday",
    is_active: bool = True,
    last_crawl_status: str | None = None,
    consecutive_failures: int = 0,
) -> uuid.UUID:
    result = await session.execute(
        text(
            """
            INSERT INTO jobs.ats_sources
                (company_id, ats_type, ats_slug, market, is_active,
                 last_crawl_status, consecutive_failures)
            VALUES
                (:company_id, :ats_type, :ats_slug, :market, :is_active,
                 :last_crawl_status, :consecutive_failures)
            RETURNING id
            """
        ),
        {
            "company_id": company_id,
            "ats_type": ats_type,
            "ats_slug": "test-slug",
            "market": "US",
            "is_active": is_active,
            "last_crawl_status": last_crawl_status,
            "consecutive_failures": consecutive_failures,
        },
    )
    return result.scalar_one()


async def _get_source_state(source_id: uuid.UUID) -> dict:
    async with _TestSession() as s:
        row = await s.execute(
            text(
                "SELECT is_active, consecutive_failures, backoff_until "
                "FROM jobs.ats_sources WHERE id = :id"
            ),
            {"id": source_id},
        )
        r = row.one()
        return {
            "is_active": r[0],
            "consecutive_failures": r[1],
            "backoff_until": r[2],
        }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestReactivateErroredSources:
    """Integration tests for _async_reactivate_sources."""

    @pytest.mark.asyncio
    async def test_errored_non_probe_source_is_reactivated(self):
        """A regular (non-probe) errored source is reactivated."""
        from app.crawler.tasks import _async_reactivate_sources

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            company_id = await _insert_company(s, f"test-react-co-{suffix}")
            source_id = await _insert_ats_source(
                s,
                company_id,
                is_active=False,
                last_crawl_status="error",
                consecutive_failures=3,
            )
            await s.commit()

        count = await _async_reactivate_sources()

        assert count >= 1
        state = await _get_source_state(source_id)
        assert state["is_active"] is True
        assert state["consecutive_failures"] == 0
        assert state["backoff_until"] is None

    @pytest.mark.asyncio
    async def test_errored_probe_source_is_not_reactivated(self):
        """A probe AtsSource (company slug ends with '-probe') is NOT reactivated."""
        from app.crawler.tasks import _async_reactivate_sources

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            # Create a probe company (slug ends with '-probe')
            probe_company_id = await _insert_company(
                s, f"test-acme-{suffix}-probe"
            )
            probe_source_id = await _insert_ats_source(
                s,
                probe_company_id,
                is_active=False,
                last_crawl_status="error",
                consecutive_failures=2,
            )
            await s.commit()

        await _async_reactivate_sources()

        state = await _get_source_state(probe_source_id)
        assert state["is_active"] is False, (
            "Probe AtsSource should NOT be reactivated"
        )
        assert state["consecutive_failures"] == 2, (
            "Probe consecutive_failures should remain unchanged"
        )

    @pytest.mark.asyncio
    async def test_slug_not_found_source_is_not_reactivated(self):
        """AtsSource with last_crawl_status='slug_not_found' is NOT reactivated."""
        from app.crawler.tasks import _async_reactivate_sources

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            company_id = await _insert_company(s, f"test-slug404-co-{suffix}")
            source_id = await _insert_ats_source(
                s,
                company_id,
                is_active=False,
                last_crawl_status="slug_not_found",
            )
            await s.commit()

        await _async_reactivate_sources()

        state = await _get_source_state(source_id)
        assert state["is_active"] is False, (
            "slug_not_found source should NOT be reactivated"
        )

    @pytest.mark.asyncio
    async def test_mixed_sources_only_non_probe_errored_reactivated(self):
        """When both probe and non-probe errored sources exist,
        only the non-probe source is reactivated."""
        from app.crawler.tasks import _async_reactivate_sources

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            # Regular errored source
            real_co_id = await _insert_company(s, f"test-real-co-{suffix}")
            real_source_id = await _insert_ats_source(
                s,
                real_co_id,
                is_active=False,
                last_crawl_status="error",
                consecutive_failures=1,
            )

            # Probe errored source
            probe_co_id = await _insert_company(
                s, f"test-mixed-{suffix}-probe"
            )
            probe_source_id = await _insert_ats_source(
                s,
                probe_co_id,
                is_active=False,
                last_crawl_status="error",
                consecutive_failures=1,
            )
            await s.commit()

        await _async_reactivate_sources()

        real_state = await _get_source_state(real_source_id)
        assert real_state["is_active"] is True, (
            "Non-probe errored source should be reactivated"
        )

        probe_state = await _get_source_state(probe_source_id)
        assert probe_state["is_active"] is False, (
            "Probe errored source should NOT be reactivated"
        )
