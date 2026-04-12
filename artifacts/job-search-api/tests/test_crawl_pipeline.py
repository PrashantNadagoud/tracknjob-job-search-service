"""Integration tests for _async_run_crawl_pipeline.

Covers:
1. Due-source selection: sources with is_active=TRUE and last_crawled_at past
   the 20-hour window are selected; sources crawled recently are skipped.
2. 3-day ATS stale deactivation: ATS-tracked listings (ats_type IS NOT NULL)
   with last_seen_at older than 3 days are deactivated; listings seen recently
   or without ats_type are left untouched.
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import _TestSession


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Helpers: create minimal DB fixtures directly for integration tests
# ---------------------------------------------------------------------------

async def _insert_company(session: AsyncSession, suffix: str) -> uuid.UUID:
    slug = f"test-crawl-co-{suffix}"
    result = await session.execute(
        text(
            """
            INSERT INTO jobs.companies (slug, name)
            VALUES (:slug, :name)
            RETURNING id
            """
        ),
        {"slug": slug, "name": f"Test Crawl Co {suffix}"},
    )
    return result.scalar_one()


async def _insert_ats_source(
    session: AsyncSession,
    company_id: uuid.UUID,
    *,
    ats_type: str = "workday",
    is_active: bool = True,
    last_crawled_at: datetime | None = None,
    last_crawl_status: str | None = None,
    backoff_until: datetime | None = None,
) -> uuid.UUID:
    result = await session.execute(
        text(
            """
            INSERT INTO jobs.ats_sources
                (company_id, ats_type, ats_slug, market, is_active,
                 last_crawled_at, last_crawl_status, backoff_until)
            VALUES
                (:company_id, :ats_type, :ats_slug, :market, :is_active,
                 :last_crawled_at, :last_crawl_status, :backoff_until)
            RETURNING id
            """
        ),
        {
            "company_id": company_id,
            "ats_type": ats_type,
            "ats_slug": "test-slug",
            "market": "US",
            "is_active": is_active,
            "last_crawled_at": last_crawled_at,
            "last_crawl_status": last_crawl_status,
            "backoff_until": backoff_until,
        },
    )
    return result.scalar_one()


async def _insert_listing(
    session: AsyncSession,
    suffix: str,
    *,
    ats_type: str | None = "workday",
    last_seen_at: datetime,
    is_active: bool = True,
) -> uuid.UUID:
    url = f"http://test-pipeline-{suffix}"
    result = await session.execute(
        text(
            """
            INSERT INTO jobs.listings
                (title, company, location, remote, source_url, source_label,
                 posted_at, country, last_seen_at, is_active, ats_type)
            VALUES
                (:title, :company, :location, :remote, :source_url, :source_label,
                 :posted_at, :country, :last_seen_at, :is_active, :ats_type)
            RETURNING id
            """
        ),
        {
            "title": "Test Engineer",
            "company": f"PipelineCo-{suffix}",
            "location": "Remote",
            "remote": True,
            "source_url": url,
            "source_label": "test",
            "posted_at": _now() - timedelta(days=5),
            "country": "US",
            "last_seen_at": last_seen_at,
            "is_active": is_active,
            "ats_type": ats_type,
        },
    )
    return result.scalar_one()


async def _get_listing_active(listing_id: uuid.UUID) -> bool:
    async with _TestSession() as s:
        row = await s.execute(
            text("SELECT is_active FROM jobs.listings WHERE id = :id"),
            {"id": listing_id},
        )
        return row.scalar_one()


async def _get_ats_source_active(ats_source_id: uuid.UUID) -> bool:
    async with _TestSession() as s:
        row = await s.execute(
            text("SELECT is_active FROM jobs.ats_sources WHERE id = :id"),
            {"id": ats_source_id},
        )
        return row.scalar_one()


# ---------------------------------------------------------------------------
# Test: due-source selection
# ---------------------------------------------------------------------------

class TestCrawlPipelineDueSourceSelection:
    """_async_run_crawl_pipeline only processes due, active sources."""

    @pytest.mark.asyncio
    async def test_due_source_is_dispatched(self):
        """An active source with last_crawled_at > 20h ago is selected."""
        from app.crawler.tasks import _async_run_crawl_pipeline

        suffix = uuid.uuid4().hex[:8]
        dispatched_ids: list[uuid.UUID] = []

        async with _TestSession() as s:
            company_id = await _insert_company(s, suffix)
            # Source last crawled 25h ago → due
            due_id = await _insert_ats_source(
                s,
                company_id,
                is_active=True,
                last_crawled_at=_now() - timedelta(hours=25),
            )
            await s.commit()

        async def mock_dispatch(source_id, session):
            dispatched_ids.append(source_id)
            return []

        mock_dispatcher = MagicMock()
        mock_dispatcher.dispatch = AsyncMock(side_effect=mock_dispatch)

        with patch("app.crawler.dispatcher.CrawlDispatcher", return_value=mock_dispatcher):
            await _async_run_crawl_pipeline()

        assert due_id in dispatched_ids, (
            f"Expected due source {due_id} to be dispatched, got {dispatched_ids}"
        )

    @pytest.mark.asyncio
    async def test_recently_crawled_source_is_skipped(self):
        """An active source crawled only 1h ago is NOT selected."""
        from app.crawler.tasks import _async_run_crawl_pipeline

        suffix = uuid.uuid4().hex[:8]
        dispatched_ids: list[uuid.UUID] = []

        async with _TestSession() as s:
            company_id = await _insert_company(s, suffix)
            # Source last crawled 1h ago → not due
            recent_id = await _insert_ats_source(
                s,
                company_id,
                is_active=True,
                last_crawled_at=_now() - timedelta(hours=1),
            )
            await s.commit()

        async def mock_dispatch(source_id, session):
            dispatched_ids.append(source_id)
            return []

        mock_dispatcher = MagicMock()
        mock_dispatcher.dispatch = AsyncMock(side_effect=mock_dispatch)

        with patch("app.crawler.dispatcher.CrawlDispatcher", return_value=mock_dispatcher):
            await _async_run_crawl_pipeline()

        assert recent_id not in dispatched_ids, (
            "Recently crawled source should NOT be dispatched"
        )

    @pytest.mark.asyncio
    async def test_inactive_source_is_skipped(self):
        """An is_active=False source is never dispatched."""
        from app.crawler.tasks import _async_run_crawl_pipeline

        suffix = uuid.uuid4().hex[:8]
        dispatched_ids: list[uuid.UUID] = []

        async with _TestSession() as s:
            company_id = await _insert_company(s, suffix)
            inactive_id = await _insert_ats_source(
                s,
                company_id,
                is_active=False,
                last_crawled_at=None,
            )
            await s.commit()

        async def mock_dispatch(source_id, session):
            dispatched_ids.append(source_id)
            return []

        mock_dispatcher = MagicMock()
        mock_dispatcher.dispatch = AsyncMock(side_effect=mock_dispatch)

        with patch("app.crawler.dispatcher.CrawlDispatcher", return_value=mock_dispatcher):
            await _async_run_crawl_pipeline()

        assert inactive_id not in dispatched_ids, (
            "Inactive source should NOT be dispatched"
        )

    @pytest.mark.asyncio
    async def test_backoff_source_is_skipped(self):
        """A source with backoff_until in the future is not dispatched."""
        from app.crawler.tasks import _async_run_crawl_pipeline

        suffix = uuid.uuid4().hex[:8]
        dispatched_ids: list[uuid.UUID] = []

        async with _TestSession() as s:
            company_id = await _insert_company(s, suffix)
            backoff_id = await _insert_ats_source(
                s,
                company_id,
                is_active=True,
                last_crawled_at=None,
                backoff_until=_now() + timedelta(hours=6),
            )
            await s.commit()

        async def mock_dispatch(source_id, session):
            dispatched_ids.append(source_id)
            return []

        mock_dispatcher = MagicMock()
        mock_dispatcher.dispatch = AsyncMock(side_effect=mock_dispatch)

        with patch("app.crawler.dispatcher.CrawlDispatcher", return_value=mock_dispatcher):
            await _async_run_crawl_pipeline()

        assert backoff_id not in dispatched_ids, (
            "Backoff-active source should NOT be dispatched"
        )

    @pytest.mark.asyncio
    async def test_never_crawled_source_is_dispatched(self):
        """An active source with last_crawled_at=NULL is treated as due."""
        from app.crawler.tasks import _async_run_crawl_pipeline

        suffix = uuid.uuid4().hex[:8]
        dispatched_ids: list[uuid.UUID] = []

        async with _TestSession() as s:
            company_id = await _insert_company(s, suffix)
            null_id = await _insert_ats_source(
                s,
                company_id,
                is_active=True,
                last_crawled_at=None,
            )
            await s.commit()

        async def mock_dispatch(source_id, session):
            dispatched_ids.append(source_id)
            return []

        mock_dispatcher = MagicMock()
        mock_dispatcher.dispatch = AsyncMock(side_effect=mock_dispatch)

        with patch("app.crawler.dispatcher.CrawlDispatcher", return_value=mock_dispatcher):
            await _async_run_crawl_pipeline()

        assert null_id in dispatched_ids, (
            "Never-crawled source should be dispatched"
        )


# ---------------------------------------------------------------------------
# Test: 3-day ATS stale deactivation
# ---------------------------------------------------------------------------

class TestCrawlPipelineStaleDeactivation:
    """_async_run_crawl_pipeline deactivates ATS listings unseen for 3 days."""

    @pytest.mark.asyncio
    async def test_ats_listing_stale_3_days_deactivated(self):
        """ATS listing last seen >3 days ago is deactivated by the pipeline."""
        from app.crawler.tasks import _async_run_crawl_pipeline

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            stale_id = await _insert_listing(
                s,
                f"stale-{suffix}",
                ats_type="workday",
                last_seen_at=_now() - timedelta(days=4),
                is_active=True,
            )
            await s.commit()

        mock_dispatcher = MagicMock()
        mock_dispatcher.dispatch = AsyncMock(return_value=[])

        with patch("app.crawler.dispatcher.CrawlDispatcher", return_value=mock_dispatcher):
            await _async_run_crawl_pipeline()

        assert await _get_listing_active(stale_id) is False, (
            "ATS listing >3 days stale should be deactivated"
        )

    @pytest.mark.asyncio
    async def test_ats_listing_recent_not_deactivated(self):
        """ATS listing seen <3 days ago is NOT deactivated."""
        from app.crawler.tasks import _async_run_crawl_pipeline

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            fresh_id = await _insert_listing(
                s,
                f"fresh-{suffix}",
                ats_type="greenhouse",
                last_seen_at=_now() - timedelta(hours=12),
                is_active=True,
            )
            await s.commit()

        mock_dispatcher = MagicMock()
        mock_dispatcher.dispatch = AsyncMock(return_value=[])

        with patch("app.crawler.dispatcher.CrawlDispatcher", return_value=mock_dispatcher):
            await _async_run_crawl_pipeline()

        assert await _get_listing_active(fresh_id) is True, (
            "ATS listing seen <3 days ago should remain active"
        )

    @pytest.mark.asyncio
    async def test_legacy_listing_old_but_not_deactivated_by_pipeline(self):
        """Legacy (ats_type=NULL) listings are NOT touched by the 3-day pipeline rule."""
        from app.crawler.tasks import _async_run_crawl_pipeline

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            legacy_id = await _insert_listing(
                s,
                f"legacy-{suffix}",
                ats_type=None,
                last_seen_at=_now() - timedelta(days=10),
                is_active=True,
            )
            await s.commit()

        mock_dispatcher = MagicMock()
        mock_dispatcher.dispatch = AsyncMock(return_value=[])

        with patch("app.crawler.dispatcher.CrawlDispatcher", return_value=mock_dispatcher):
            await _async_run_crawl_pipeline()

        # Legacy listings are governed by _async_deactivate_stale (12h rule),
        # NOT the pipeline 3-day rule; we verify the pipeline doesn't touch them.
        assert await _get_listing_active(legacy_id) is True, (
            "Legacy (ats_type=NULL) listing should NOT be deactivated by pipeline stale rule"
        )


# ---------------------------------------------------------------------------
# Test: reactivate_errored_sources excludes discovery probe AtsSource rows
# ---------------------------------------------------------------------------

class TestReactivateErroredSources:
    """_async_reactivate_sources must not reactivate probe AtsSource rows."""

    @pytest.mark.asyncio
    async def test_legitimate_errored_source_is_reactivated(self):
        """A real (non-probe) errored source should be reactivated."""
        from app.crawler.tasks import _async_reactivate_sources

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            company_id = await _insert_company(s, suffix)
            src_id = await _insert_ats_source(
                s,
                company_id,
                is_active=False,
                last_crawl_status="error",
                last_crawled_at=_now() - timedelta(hours=6),
            )
            await s.commit()

        count = await _async_reactivate_sources()

        assert count >= 1, "Expected at least one source reactivated"
        assert await _get_ats_source_active(src_id) is True, (
            "Legitimate errored source should be reactivated"
        )

    @pytest.mark.asyncio
    async def test_probe_errored_source_is_not_reactivated(self):
        """A probe AtsSource (company slug ending in '-probe') must NOT be reactivated."""
        from app.crawler.tasks import _async_reactivate_sources

        suffix = uuid.uuid4().hex[:8]
        probe_slug = f"test-crawl-co-{suffix}-probe"

        async with _TestSession() as s:
            # Insert a probe company with slug ending in '-probe'
            result = await s.execute(
                text(
                    """
                    INSERT INTO jobs.companies (slug, name)
                    VALUES (:slug, :name)
                    RETURNING id
                    """
                ),
                {"slug": probe_slug, "name": f"Test Probe Co {suffix}"},
            )
            probe_company_id = result.scalar_one()

            probe_src_id = await _insert_ats_source(
                s,
                probe_company_id,
                is_active=False,
                last_crawl_status="error",
                last_crawled_at=_now() - timedelta(hours=1),
            )
            await s.commit()

        await _async_reactivate_sources()

        assert await _get_ats_source_active(probe_src_id) is False, (
            "Probe AtsSource (company slug ending in '-probe') must NOT be reactivated"
        )

    @pytest.mark.asyncio
    async def test_slug_not_found_source_is_not_reactivated(self):
        """A source with last_crawl_status='slug_not_found' should not be reactivated."""
        from app.crawler.tasks import _async_reactivate_sources

        suffix = uuid.uuid4().hex[:8]

        async with _TestSession() as s:
            company_id = await _insert_company(s, suffix)
            src_id = await _insert_ats_source(
                s,
                company_id,
                is_active=False,
                last_crawl_status="slug_not_found",
                last_crawled_at=_now() - timedelta(hours=6),
            )
            await s.commit()

        await _async_reactivate_sources()

        assert await _get_ats_source_active(src_id) is False, (
            "Source with slug_not_found status should NOT be reactivated"
        )
