"""Part 6 — Job alert email tests.

All Resend calls are mocked via pytest-mock. The async task functions
create their own DB sessions, so we verify state via a fresh DB query.
"""

import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.crawler.tasks import _async_send_job_alerts
from app.models import Listing, SavedSearch
from tests.conftest import TEST_USER_ID


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TestJobAlerts:
    async def test_alert_sent_for_new_jobs(
        self, db_session: AsyncSession, mocker
    ):
        """When new jobs exist, email is sent and DB state is updated."""
        suffix = uuid.uuid4().hex[:10]

        # Two matching jobs
        j1 = Listing(
            title="Alert Job One",
            company="AlertCo",
            location="SF",
            remote=False,
            source_url=f"http://test-alert-j1-{suffix}",
            source_label="test",
            posted_at=_now(),
            country="US",
            last_seen_at=_now(),
            is_active=True,
        )
        j2 = Listing(
            title="Alert Job Two",
            company="AlertCo",
            location="SF",
            remote=False,
            source_url=f"http://test-alert-j2-{suffix}",
            source_label="test",
            posted_at=_now(),
            country="US",
            last_seen_at=_now(),
            is_active=True,
        )
        db_session.add(j1)
        db_session.add(j2)
        await db_session.flush()

        search = SavedSearch(
            user_id=uuid.UUID(TEST_USER_ID),
            name=f"Alert Test Search {suffix}",
            filters={"source": f"test", "country": "US"},
            alert_email=True,
            user_email="alert-test@tracknjob.com",
            last_alerted_job_ids=[],
        )
        db_session.add(search)
        await db_session.flush()
        search_id = search.id
        await db_session.commit()

        mock_send = mocker.patch("app.email.send_job_alert_email", return_value=None)

        result = await _async_send_job_alerts()

        # Email should have been sent exactly once
        mock_send.assert_called_once()

        # last_alerted_job_ids should now include the job IDs
        from tests.conftest import _TestSession
        async with _TestSession() as s:
            row = await s.get(SavedSearch, search_id)
        assert row.last_alerted_at is not None
        alerted_ids = row.last_alerted_job_ids or []
        assert str(j1.id) in alerted_ids or str(j2.id) in alerted_ids
        assert result["emails_sent"] >= 1

    async def test_no_alert_sent_when_no_new_jobs(
        self, db_session: AsyncSession, mocker
    ):
        """When all current job IDs are already in last_alerted_job_ids, no email."""
        suffix = uuid.uuid4().hex[:10]

        j1 = Listing(
            title="Already Alerted Job",
            company="NoCo",
            location="NY",
            remote=False,
            source_url=f"http://test-noalert-{suffix}",
            source_label=f"noalert-{suffix}",
            posted_at=_now(),
            country="US",
            last_seen_at=_now(),
            is_active=True,
        )
        db_session.add(j1)
        await db_session.flush()

        # Pre-populate last_alerted_job_ids with this job's ID
        search = SavedSearch(
            user_id=uuid.UUID(TEST_USER_ID),
            name=f"No-Alert Search {suffix}",
            filters={"source": f"noalert-{suffix}", "country": "US"},
            alert_email=True,
            user_email="noalert@tracknjob.com",
            last_alerted_job_ids=[str(j1.id)],
        )
        db_session.add(search)
        await db_session.commit()

        mock_send = mocker.patch("app.email.send_job_alert_email", return_value=None)

        await _async_send_job_alerts()

        mock_send.assert_not_called()

    async def test_alert_not_sent_when_alert_email_false(
        self, db_session: AsyncSession, mocker
    ):
        """Saved searches with alert_email=False are not processed."""
        suffix = uuid.uuid4().hex[:10]

        j1 = Listing(
            title="Opted-Out Job",
            company="OptOutCo",
            location="LA",
            remote=False,
            source_url=f"http://test-optout-{suffix}",
            source_label=f"optout-{suffix}",
            posted_at=_now(),
            country="US",
            last_seen_at=_now(),
            is_active=True,
        )
        db_session.add(j1)
        await db_session.flush()

        search = SavedSearch(
            user_id=uuid.UUID(TEST_USER_ID),
            name=f"OptOut Search {suffix}",
            filters={"source": f"optout-{suffix}", "country": "US"},
            alert_email=False,   # alerts disabled
            user_email="optout@tracknjob.com",
            last_alerted_job_ids=[],
        )
        db_session.add(search)
        await db_session.commit()

        mock_send = mocker.patch("app.email.send_job_alert_email", return_value=None)

        await _async_send_job_alerts()

        mock_send.assert_not_called()
