"""Part 4 — Ghost job detection tests.

_upsert_jobs and _async_deactivate_stale create their own DB sessions
internally, so these tests verify via direct DB queries rather than
trying to share a session.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.crawler.tasks import _upsert_jobs, _async_deactivate_stale
from app.models import Listing


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TestGhostJobs:
    async def test_duplicate_detection_skips_similar_title(
        self, db_session: AsyncSession
    ):
        """Job with very similar title+company should not create a new row."""
        suffix = uuid.uuid4().hex[:10]
        seed_url = f"http://test-dup-seed-{suffix}"
        new_url = f"http://test-dup-new-{suffix}"

        # Seed: insert original job directly
        seed = Listing(
            title="Senior Backend Engineer",
            company=f"DupCorp-{suffix}",
            location="New York",
            remote=False,
            source_url=seed_url,
            source_label="test",
            posted_at=_now(),
            country="US",
            last_seen_at=_now(),
            is_active=True,
        )
        db_session.add(seed)
        await db_session.flush()
        seed_id = seed.id
        seed_last_seen = seed.last_seen_at
        await db_session.commit()

        # Upsert a job with the SAME title (similarity=1.0 > 0.85) but different URL
        # posted_at must be a datetime object — asyncpg rejects ISO strings
        await _upsert_jobs(
            [
                {
                    "title": "Senior Backend Engineer",
                    "company": f"DupCorp-{suffix}",
                    "location": "New York",
                    "remote": False,
                    "source_url": new_url,
                    "source_label": "test",
                    "posted_at": _now(),
                    "country": "US",
                }
            ]
        )

        # No new row should exist for the new URL
        row = (
            await db_session.execute(
                select(Listing).where(Listing.source_url == new_url)
            )
        ).scalar_one_or_none()
        assert row is None, "Duplicate should NOT have been inserted"

        # Original row's last_seen_at should be updated
        await db_session.refresh(seed)
        # Use a fresh session to avoid stale cache
        from tests.conftest import _TestSession
        async with _TestSession() as fresh:
            updated = await fresh.get(Listing, seed_id)
        assert updated is not None
        # last_seen_at must be >= original value (updated by duplicate handler)
        assert updated.last_seen_at >= seed_last_seen

    async def test_duplicate_detection_allows_different_role(
        self, db_session: AsyncSession
    ):
        """Jobs with sufficiently different titles should both be inserted."""
        suffix = uuid.uuid4().hex[:10]
        seed_url = f"http://test-diff-seed-{suffix}"
        new_url = f"http://test-diff-new-{suffix}"

        seed = Listing(
            title="Senior Backend Engineer",
            company=f"AllowCorp-{suffix}",
            location="Seattle",
            remote=False,
            source_url=seed_url,
            source_label="test",
            posted_at=_now(),
            country="US",
            last_seen_at=_now(),
            is_active=True,
        )
        db_session.add(seed)
        await db_session.commit()

        # Upsert a clearly different title — pg_trgm similarity will be low
        await _upsert_jobs(
            [
                {
                    "title": "Junior iOS Developer",
                    "company": f"AllowCorp-{suffix}",
                    "location": "Seattle",
                    "remote": False,
                    "source_url": new_url,
                    "source_label": "test",
                    "posted_at": _now(),
                    "country": "US",
                }
            ]
        )

        from tests.conftest import _TestSession
        async with _TestSession() as fresh:
            new_row = (
                await fresh.execute(
                    select(Listing).where(Listing.source_url == new_url)
                )
            ).scalar_one_or_none()
        assert new_row is not None, "Different role should have been inserted"
        assert new_row.title == "Junior iOS Developer"

    async def test_stale_job_deactivated(self, db_session: AsyncSession):
        """Job not seen in 13 hours should be marked is_active=False."""
        suffix = uuid.uuid4().hex[:10]
        stale = Listing(
            title="Stale Job",
            company="StaleCorp",
            location="Remote",
            remote=True,
            source_url=f"http://test-stale-{suffix}",
            source_label="test",
            posted_at=_now(),
            country="US",
            last_seen_at=_now() - timedelta(hours=13),
            is_active=True,
        )
        db_session.add(stale)
        await db_session.flush()
        stale_id = stale.id
        await db_session.commit()

        await _async_deactivate_stale()

        from tests.conftest import _TestSession
        async with _TestSession() as fresh:
            updated = await fresh.get(Listing, stale_id)
        assert updated is not None
        assert updated.is_active is False

    async def test_fresh_job_not_deactivated(self, db_session: AsyncSession):
        """Job seen 6 hours ago should remain active."""
        suffix = uuid.uuid4().hex[:10]
        fresh_job = Listing(
            title="Fresh Job",
            company="FreshCorp",
            location="Remote",
            remote=True,
            source_url=f"http://test-fresh-{suffix}",
            source_label="test",
            posted_at=_now(),
            country="US",
            last_seen_at=_now() - timedelta(hours=6),
            is_active=True,
        )
        db_session.add(fresh_job)
        await db_session.flush()
        job_id = fresh_job.id
        await db_session.commit()

        await _async_deactivate_stale()

        from tests.conftest import _TestSession
        async with _TestSession() as s:
            row = await s.get(Listing, job_id)
        assert row is not None
        assert row.is_active is True

    async def test_reactivation_of_returned_job(self, db_session: AsyncSession):
        """A previously inactive job seen again should be reactivated."""
        suffix = uuid.uuid4().hex[:10]
        src_url = f"http://test-react-{suffix}"
        inactive = Listing(
            title="Returned Job",
            company="ReturnCorp",
            location="Austin",
            remote=False,
            source_url=src_url,
            source_label="test",
            posted_at=_now(),
            country="US",
            last_seen_at=_now() - timedelta(hours=24),
            is_active=False,
        )
        db_session.add(inactive)
        await db_session.flush()
        job_id = inactive.id
        await db_session.commit()

        # Upsert with same source_url — triggers reactivation path
        await _upsert_jobs(
            [
                {
                    "title": "Returned Job",
                    "company": "ReturnCorp",
                    "location": "Austin",
                    "remote": False,
                    "source_url": src_url,
                    "source_label": "test",
                    "posted_at": _now(),
                    "country": "US",
                }
            ]
        )

        from tests.conftest import _TestSession
        async with _TestSession() as s:
            row = await s.get(Listing, job_id)
        assert row is not None
        assert row.is_active is True
        assert row.last_seen_at >= inactive.last_seen_at

    async def test_ats_listing_not_deactivated_by_12h_rule(self, db_session: AsyncSession):
        """ATS-tracked listings (ats_type IS NOT NULL) are exempt from the 12-hour
        deactivation rule; they use the 3-day window in run_crawl_pipeline."""
        suffix = uuid.uuid4().hex[:10]
        ats_job = Listing(
            title="ATS Job",
            company="ATSCorp",
            location="Remote",
            remote=True,
            source_url=f"http://test-ats-stale-{suffix}",
            source_label="workday",
            posted_at=_now(),
            country="US",
            last_seen_at=_now() - timedelta(hours=25),  # >12h but <3d
            is_active=True,
            ats_type="workday",
            external_job_id=f"ats-ext-{suffix}",
        )
        db_session.add(ats_job)
        await db_session.flush()
        ats_job_id = ats_job.id
        await db_session.commit()

        await _async_deactivate_stale()

        from tests.conftest import _TestSession
        async with _TestSession() as s:
            row = await s.get(Listing, ats_job_id)
        assert row is not None
        assert row.is_active is True, (
            "ATS listing must remain active at 25h — only the 3-day pipeline rule applies"
        )
