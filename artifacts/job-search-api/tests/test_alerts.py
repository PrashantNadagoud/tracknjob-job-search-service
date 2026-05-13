"""Tests for the Job Alerts & Motivational Email feature (Task #8).

Covers:
  - POST /api/v1/alerts/subscribe        — create and upsert (auth required)
  - GET  /api/v1/alerts/subscription/:id — retrieve (auth; own record)
  - PATCH /api/v1/alerts/subscription/:id — partial update (auth; own record)
  - DELETE /api/v1/alerts/unsubscribe/:id — soft-delete API (auth; own record)
  - GET /api/v1/alerts/unsubscribe/:id   — one-click email link (no auth)
  - POST /api/v1/alerts/test-send/:id    — trigger immediate send (auth; own record)
  - Motivational service static fallback and OpenAI path
  - Celery task query logic
  - Cross-user access denied (403)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Listing
from tests.conftest import make_token

ALERT_USER_1 = "test-alert-user-1"
ALERT_USER_2 = "test-alert-user-2"
BASE = "/api/v1/alerts"


def _auth(user_id: str) -> dict:
    """Bearer auth header for the given user_id (used as JWT sub)."""
    return {"Authorization": f"Bearer {make_token(sub=user_id)}"}


def _subscribe_payload(**overrides) -> dict:
    return {
        "user_id": ALERT_USER_1,
        "email": "alertuser1@example.com",
        "name": "Alert User",
        "keywords": ["python", "fastapi"],
        "locations": ["Remote", "San Francisco"],
        "employment_types": ["full_time"],
        "motivational_email_enabled": True,
        "delivery_time_utc": 9,
        **overrides,
    }


# ── POST /subscribe ───────────────────────────────────────────────────────────

class TestSubscribe:
    async def test_subscribe_creates_subscription(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(),
            headers=_auth(ALERT_USER_1),
        )
        assert r.status_code == 200
        data = r.json()
        assert "subscription_id" in data
        assert data["message"] == "Subscribed successfully"

    async def test_subscribe_upserts_on_second_call(self, async_client: AsyncClient):
        r1 = await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        id1 = r1.json()["subscription_id"]

        r2 = await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(email="updated@example.com"),
            headers=_auth(ALERT_USER_1),
        )
        id2 = r2.json()["subscription_id"]
        assert id1 == id2

    async def test_subscribe_with_minimal_fields(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/subscribe",
            json={"user_id": ALERT_USER_2, "email": "min@example.com"},
            headers=_auth(ALERT_USER_2),
        )
        assert r.status_code == 200

    async def test_subscribe_missing_email_returns_422(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/subscribe",
            json={"user_id": ALERT_USER_1},
            headers=_auth(ALERT_USER_1),
        )
        assert r.status_code == 422

    async def test_subscribe_without_auth_returns_401(self, async_client: AsyncClient):
        r = await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())
        assert r.status_code == 401

    async def test_subscribe_for_other_user_returns_403(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(user_id=ALERT_USER_2),
            headers=_auth(ALERT_USER_1),
        )
        assert r.status_code == 403


# ── GET /subscription/{user_id} ───────────────────────────────────────────────

class TestGetSubscription:
    async def test_get_returns_all_fields(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.get(
            f"{BASE}/subscription/{ALERT_USER_1}", headers=_auth(ALERT_USER_1)
        )
        assert r.status_code == 200
        data = r.json()
        assert data["user_id"] == ALERT_USER_1
        assert data["email"] == "alertuser1@example.com"
        assert data["is_active"] is True
        assert "python" in data["keywords"]
        assert data["delivery_time_utc"] == 9
        assert "id" in data
        assert "created_at" in data

    async def test_get_nonexistent_returns_404(self, async_client: AsyncClient):
        r = await async_client.get(
            f"{BASE}/subscription/no-such-user-xyz",
            headers=_auth("no-such-user-xyz"),
        )
        assert r.status_code == 404

    async def test_get_other_user_subscription_returns_403(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.get(
            f"{BASE}/subscription/{ALERT_USER_1}", headers=_auth(ALERT_USER_2)
        )
        assert r.status_code == 403

    async def test_get_without_auth_returns_401(self, async_client: AsyncClient):
        r = await async_client.get(f"{BASE}/subscription/{ALERT_USER_1}")
        assert r.status_code == 401


# ── PATCH /subscription/{user_id} ────────────────────────────────────────────

class TestPatchSubscription:
    async def test_patch_updates_keywords_and_hour(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.patch(
            f"{BASE}/subscription/{ALERT_USER_1}",
            json={"keywords": ["django", "postgres"], "delivery_time_utc": 14},
            headers=_auth(ALERT_USER_1),
        )
        assert r.status_code == 200
        data = r.json()
        assert "django" in data["keywords"]
        assert data["delivery_time_utc"] == 14

    async def test_patch_nonexistent_returns_404(self, async_client: AsyncClient):
        r = await async_client.patch(
            f"{BASE}/subscription/ghost-user-zzz",
            json={"delivery_time_utc": 10},
            headers=_auth("ghost-user-zzz"),
        )
        assert r.status_code == 404

    async def test_patch_empty_body_returns_current_state(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.patch(
            f"{BASE}/subscription/{ALERT_USER_1}",
            json={},
            headers=_auth(ALERT_USER_1),
        )
        assert r.status_code == 200
        assert r.json()["delivery_time_utc"] == 9

    async def test_patch_other_user_returns_403(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.patch(
            f"{BASE}/subscription/{ALERT_USER_1}",
            json={"delivery_time_utc": 7},
            headers=_auth(ALERT_USER_2),
        )
        assert r.status_code == 403


# ── DELETE /unsubscribe/{user_id} ─────────────────────────────────────────────

class TestUnsubscribeApi:
    async def test_unsubscribe_sets_is_active_false(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.delete(
            f"{BASE}/unsubscribe/{ALERT_USER_1}", headers=_auth(ALERT_USER_1)
        )
        assert r.status_code == 200

        get_r = await async_client.get(
            f"{BASE}/subscription/{ALERT_USER_1}", headers=_auth(ALERT_USER_1)
        )
        assert get_r.json()["is_active"] is False

    async def test_resubscribe_reactivates(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        await async_client.delete(
            f"{BASE}/unsubscribe/{ALERT_USER_1}", headers=_auth(ALERT_USER_1)
        )
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        get_r = await async_client.get(
            f"{BASE}/subscription/{ALERT_USER_1}", headers=_auth(ALERT_USER_1)
        )
        assert get_r.json()["is_active"] is True

    async def test_unsubscribe_nonexistent_returns_404(self, async_client: AsyncClient):
        r = await async_client.delete(
            f"{BASE}/unsubscribe/nobody-zzz", headers=_auth("nobody-zzz")
        )
        assert r.status_code == 404

    async def test_unsubscribe_other_user_returns_403(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.delete(
            f"{BASE}/unsubscribe/{ALERT_USER_1}", headers=_auth(ALERT_USER_2)
        )
        assert r.status_code == 403


# ── GET /unsubscribe/{user_id} (one-click email link) ─────────────────────────

class TestUnsubscribeEmailLink:
    async def test_email_link_unsubscribes_without_auth(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.get(f"{BASE}/unsubscribe/{ALERT_USER_1}")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "unsubscribed" in r.text.lower()

        get_r = await async_client.get(
            f"{BASE}/subscription/{ALERT_USER_1}", headers=_auth(ALERT_USER_1)
        )
        assert get_r.json()["is_active"] is False

    async def test_email_link_for_nonexistent_user_returns_200_html(
        self, async_client: AsyncClient
    ):
        r = await async_client.get(f"{BASE}/unsubscribe/never-existed-xyz")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]

    async def test_email_link_already_unsubscribed_returns_200(
        self, async_client: AsyncClient
    ):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        await async_client.get(f"{BASE}/unsubscribe/{ALERT_USER_1}")
        r = await async_client.get(f"{BASE}/unsubscribe/{ALERT_USER_1}")
        assert r.status_code == 200
        assert "already" in r.text.lower()


# ── POST /test-send/{user_id} ─────────────────────────────────────────────────

class TestTestSend:
    async def test_test_send_skipped_when_no_matching_jobs(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(keywords=["zzzz-nonexistent-role-xyzzy-9999"]),
            headers=_auth(ALERT_USER_1),
        )
        r = await async_client.post(
            f"{BASE}/test-send/{ALERT_USER_1}", headers=_auth(ALERT_USER_1)
        )
        assert r.status_code == 200
        assert r.json()["jobs_found"] == 0

    async def test_test_send_nonexistent_user_returns_404(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/test-send/ghost-user-xyz", headers=_auth("ghost-user-xyz")
        )
        assert r.status_code == 404

    async def test_test_send_other_user_returns_403(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.post(
            f"{BASE}/test-send/{ALERT_USER_1}", headers=_auth(ALERT_USER_2)
        )
        assert r.status_code == 403

    async def test_test_send_without_auth_returns_401(self, async_client: AsyncClient):
        r = await async_client.post(f"{BASE}/test-send/{ALERT_USER_1}")
        assert r.status_code == 401

    async def test_test_send_calls_render_and_send_when_jobs_exist(
        self, async_client: AsyncClient, db_session: AsyncSession
    ):
        job = Listing(
            title="Python Engineer Unique",
            company="AlertTestCo",
            location="Remote",
            remote=True,
            source_url=f"http://test-alert-send-{uuid.uuid4().hex}",
            source_label="AlertTestCo Careers",
            posted_at=datetime.now(timezone.utc),
            country="US",
            last_seen_at=datetime.now(timezone.utc),
            is_active=True,
        )
        db_session.add(job)
        await db_session.commit()

        await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(keywords=["Python Engineer Unique"]),
            headers=_auth(ALERT_USER_1),
        )

        with patch("app.alert_tasks._render_and_send", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"status": "sent", "resend_message_id": "msg-abc"}
            r = await async_client.post(
                f"{BASE}/test-send/{ALERT_USER_1}", headers=_auth(ALERT_USER_1)
            )

        assert r.status_code == 200
        assert r.json()["jobs_found"] >= 1
        mock_send.assert_called_once()


# ── Motivational service ──────────────────────────────────────────────────────

class TestMotivationalService:
    def test_returns_static_fallback_when_no_api_key(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "")
        from importlib import reload
        import app.services.motivational as mod
        reload(mod)

        result = mod.generate_motivational_intro({
            "name": "Alice", "days_searching": 5,
            "jobs_found_today": 3, "top_job_title": "Backend Engineer",
            "top_company": "Acme",
        })
        assert isinstance(result, str)
        assert len(result) > 10

    def test_returns_fallback_on_openai_error(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-fake")
        from importlib import reload
        import app.services.motivational as mod
        reload(mod)

        with patch("openai.OpenAI") as mock_cls:
            mock_cls.return_value.chat.completions.create.side_effect = Exception("API error")
            result = mod.generate_motivational_intro({
                "name": "Bob", "days_searching": 2,
                "jobs_found_today": 1, "top_job_title": "SRE", "top_company": "Corp",
            })
        assert isinstance(result, str)
        assert len(result) > 10

    def test_returns_text_from_openai_on_success(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-fake")
        from importlib import reload
        import app.services.motivational as mod
        reload(mod)

        mock_response = MagicMock()
        mock_response.choices[0].message.content = "Keep going, great things ahead."

        with patch("openai.OpenAI") as mock_cls:
            mock_cls.return_value.chat.completions.create.return_value = mock_response
            result = mod.generate_motivational_intro({
                "name": "Carol", "days_searching": 10,
                "jobs_found_today": 5, "top_job_title": "PM", "top_company": "BigCo",
            })
        assert result == "Keep going, great things ahead."


# ── Celery task query logic ───────────────────────────────────────────────────

class TestAlertTaskLogic:
    async def test_query_matching_jobs_filters_by_keyword(self, db_session: AsyncSession):
        from app.alert_tasks import _query_matching_jobs

        unique_kw = f"zxcvbnmq-{uuid.uuid4().hex[:8]}"
        job = Listing(
            title=f"Senior {unique_kw} Developer",
            company="TaskTestCo",
            location="Remote",
            remote=True,
            source_url=f"http://test-task-{uuid.uuid4().hex}",
            source_label="TaskTestCo Careers",
            posted_at=datetime.now(timezone.utc),
            country="US",
            last_seen_at=datetime.now(timezone.utc),
            is_active=True,
        )
        db_session.add(job)
        await db_session.commit()

        sub = MagicMock()
        sub.keywords = [unique_kw]
        sub.locations = []
        sub.employment_types = []
        sub.ats_types = None

        jobs = await _query_matching_jobs(sub, db_session)
        assert len(jobs) >= 1
        assert any(unique_kw.lower() in j["title"].lower() for j in jobs)

    async def test_query_returns_empty_list_for_no_match(self, db_session: AsyncSession):
        from app.alert_tasks import _query_matching_jobs

        sub = MagicMock()
        sub.keywords = ["zzz-absolutely-no-match-99999xyzzy"]
        sub.locations = []
        sub.employment_types = []
        sub.ats_types = None

        jobs = await _query_matching_jobs(sub, db_session)
        assert jobs == []

    async def test_celery_task_returns_zero_when_alerts_disabled(self):
        from app.alert_tasks import _async_send_daily_alerts
        with patch("app.config.get_settings") as mock_settings:
            settings_obj = MagicMock()
            settings_obj.ALERTS_ENABLED = False
            mock_settings.return_value = settings_obj
            result = await _async_send_daily_alerts()
        assert result["processed"] == 0
        assert result["sent"] == 0

    async def test_integrity_error_on_duplicate_sent_is_skipped_not_crashed(
        self, db_session: AsyncSession
    ):
        """When the pre-send claim INSERT raises IntegrityError (concurrent worker race),
        _async_send_daily_alerts must skip without crashing AND without sending the email.

        This validates the claim-first deduplication: the email is never sent to a second
        worker that loses the race — preventing duplicate emails entirely.
        """
        from unittest.mock import AsyncMock, patch, MagicMock
        from sqlalchemy.exc import IntegrityError as SAIntegrityError
        from app.alert_tasks import _async_send_daily_alerts

        with patch("app.config.get_settings") as mock_settings, \
             patch("app.alert_tasks._make_session") as mock_make_session, \
             patch("app.alert_tasks._query_matching_jobs", new_callable=AsyncMock) as mock_jobs, \
             patch("app.alert_tasks._render_and_send", new_callable=AsyncMock) as mock_send:

            settings_obj = MagicMock()
            settings_obj.ALERTS_ENABLED = True
            mock_settings.return_value = settings_obj

            mock_jobs.return_value = [{"id": "1", "title": "Eng", "company": "Co",
                                        "location": "Remote", "employment_type": None,
                                        "source_url": "http://x.com"}]
            mock_send.return_value = {"status": "sent", "resend_message_id": "msg-1"}

            sub = MagicMock()
            sub.id = "sub-uuid-1"
            sub.email = "test@example.com"
            sub.user_id = "test-user"

            mock_db = AsyncMock()
            mock_db.__aenter__ = AsyncMock(return_value=mock_db)
            mock_db.__aexit__ = AsyncMock(return_value=False)

            fetch_result = MagicMock()
            fetch_result.fetchall.return_value = [sub]

            execute_call_count = 0

            async def execute_side_effect(sql, params=None):
                nonlocal execute_call_count
                execute_call_count += 1
                if execute_call_count == 1:
                    # First call: return subscriptions list
                    return fetch_result
                # Second call: the claim INSERT — simulate concurrent worker won
                raise SAIntegrityError("duplicate key", {}, Exception("unique violation"))

            mock_db.execute = AsyncMock(side_effect=execute_side_effect)

            mock_session_factory = MagicMock()
            mock_session_factory.return_value = mock_db
            mock_make_session.return_value = mock_session_factory

            result = await _async_send_daily_alerts()

        # Email must NOT have been sent — the claim was lost before _render_and_send
        mock_send.assert_not_called()
        assert result["skipped"] == 1
        assert result["sent"] == 0
        assert result["failed"] == 0

    def test_beat_schedule_retains_existing_30min_entry_and_adds_daily_alerts(self):
        from app.celery_config import beat_schedule
        assert "send-job-alerts-every-30-minutes" in beat_schedule, (
            "Old 30-minute job-alert Beat entry must not be removed"
        )
        assert "send-daily-alerts" in beat_schedule, (
            "New hourly send-daily-alerts entry must be present"
        )
        assert beat_schedule["send-daily-alerts"]["task"] == "app.alert_tasks.send_daily_alerts"

    def test_beat_schedule_has_prune_old_deliveries(self):
        from app.celery_config import beat_schedule
        assert "prune-old-deliveries-nightly" in beat_schedule, (
            "Nightly prune-old-deliveries Beat entry must be present"
        )
        assert beat_schedule["prune-old-deliveries-nightly"]["task"] == (
            "app.alert_tasks.prune_old_deliveries"
        )

    async def test_prune_old_deliveries_removes_old_rows_and_keeps_recent(
        self, db_session: AsyncSession
    ):
        """Rows older than 90 days are deleted; rows within 90 days are kept.

        Patches _make_session to return the test session factory so that
        _async_prune_old_deliveries executes its production DELETE against the
        test database — this validates the real SQL, not a duplicate copy.
        """
        from datetime import timedelta
        from tests.conftest import _TestSession
        from app.alert_tasks import _async_prune_old_deliveries

        sub_id_result = await db_session.execute(
            text("""
                INSERT INTO jobs.alert_subscriptions
                    (user_id, email, is_active, delivery_time_utc)
                VALUES (:uid, :email, true, 9)
                ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email
                RETURNING id
            """),
            {"uid": "prune-test-user-retention", "email": "prune@test.com"},
        )
        sub_id = sub_id_result.scalar_one()
        await db_session.commit()

        now = datetime.now(timezone.utc)
        old_ts = now - timedelta(days=91)
        recent_ts = now - timedelta(days=10)

        await db_session.execute(
            text("""
                INSERT INTO jobs.alert_deliveries
                    (subscription_id, delivered_at, jobs_sent, status)
                VALUES (:sid, :ts, 0, 'failed')
            """),
            {"sid": str(sub_id), "ts": old_ts},
        )
        await db_session.execute(
            text("""
                INSERT INTO jobs.alert_deliveries
                    (subscription_id, delivered_at, jobs_sent, status)
                VALUES (:sid, :ts, 3, 'sent')
            """),
            {"sid": str(sub_id), "ts": recent_ts},
        )
        await db_session.commit()

        with patch("app.alert_tasks._make_session", return_value=_TestSession):
            result = await _async_prune_old_deliveries()

        assert result["deleted"] >= 1, "At least the 91-day-old row must have been deleted"

        remaining = (await db_session.execute(
            text("""
                SELECT status FROM jobs.alert_deliveries
                WHERE subscription_id = :sid
                  AND delivered_at >= now() - 90 * interval '1 day'
            """),
            {"sid": str(sub_id)},
        )).fetchall()
        statuses = [r.status for r in remaining]
        assert "sent" in statuses, "Recent 'sent' row must be preserved"

        gone = (await db_session.execute(
            text("""
                SELECT id FROM jobs.alert_deliveries
                WHERE subscription_id = :sid
                  AND delivered_at < now() - 90 * interval '1 day'
            """),
            {"sid": str(sub_id)},
        )).fetchall()
        assert len(gone) == 0, "Old rows must have been deleted"

    async def test_async_prune_returns_deleted_count(self):
        """_async_prune_old_deliveries returns a dict with 'deleted' key."""
        from app.alert_tasks import _async_prune_old_deliveries

        mock_result = MagicMock()
        mock_result.rowcount = 7

        mock_db = AsyncMock()
        mock_db.__aenter__ = AsyncMock(return_value=mock_db)
        mock_db.__aexit__ = AsyncMock(return_value=False)
        mock_db.execute = AsyncMock(return_value=mock_result)
        mock_db.commit = AsyncMock()

        mock_session_factory = MagicMock()
        mock_session_factory.return_value = mock_db

        with patch("app.alert_tasks._make_session", return_value=mock_session_factory):
            result = await _async_prune_old_deliveries()

        assert result == {"deleted": 7}
        mock_db.execute.assert_called_once()
        mock_db.commit.assert_called_once()


# ── Retry failed deliveries ───────────────────────────────────────────────────

class TestRetryFailedDeliveries:
    """Tests for _async_retry_failed_deliveries and the Beat schedule entry."""

    def test_beat_schedule_has_retry_entry(self):
        from app.celery_config import beat_schedule
        assert "retry-failed-deliveries-every-5-minutes" in beat_schedule, (
            "retry-failed-deliveries-every-5-minutes Beat entry must be present"
        )
        assert beat_schedule["retry-failed-deliveries-every-5-minutes"]["task"] == (
            "app.alert_tasks.retry_failed_deliveries"
        )

    async def test_retry_returns_zeros_when_alerts_disabled(self):
        from app.alert_tasks import _async_retry_failed_deliveries
        with patch("app.config.get_settings") as mock_settings:
            settings_obj = MagicMock()
            settings_obj.ALERTS_ENABLED = False
            mock_settings.return_value = settings_obj
            result = await _async_retry_failed_deliveries()
        assert result["processed"] == 0
        assert result["retried"] == 0
        assert result["sent"] == 0

    async def test_retry_sends_when_previous_attempt_failed(self, db_session: AsyncSession):
        """A subscription with one 'failed' row old enough for backoff is retried and sent."""
        from datetime import timedelta
        from unittest.mock import AsyncMock, patch, MagicMock
        from tests.conftest import _TestSession
        from app.alert_tasks import _async_retry_failed_deliveries, _RETRY_BASE_MINUTES

        sub_id_result = await db_session.execute(
            text("""
                INSERT INTO jobs.alert_subscriptions
                    (user_id, email, name, is_active, delivery_time_utc, keywords)
                VALUES (:uid, :email, 'Retry User', true, 9, ARRAY['python'])
                ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email
                RETURNING id
            """),
            {"uid": "retry-test-user-1", "email": "retry1@example.com"},
        )
        sub_id = sub_id_result.scalar_one()
        await db_session.commit()

        # Set delivered_at to 2× the base interval ago so the backoff window is satisfied.
        old_enough = datetime.now(timezone.utc) - timedelta(minutes=_RETRY_BASE_MINUTES * 2)
        await db_session.execute(
            text("""
                INSERT INTO jobs.alert_deliveries
                    (subscription_id, delivered_at, jobs_sent, status, error_message)
                VALUES (:sid, :ts, 0, 'failed', 'Resend API timeout')
            """),
            {"sid": str(sub_id), "ts": old_enough},
        )
        await db_session.commit()

        mock_jobs = [
            {"id": "1", "title": "Python Dev", "company": "Co",
             "location": "Remote", "employment_type": None, "source_url": "http://x.com"}
        ]

        with patch("app.alert_tasks._make_session", return_value=_TestSession), \
             patch("app.alert_tasks._query_matching_jobs", new_callable=AsyncMock) as mock_qj, \
             patch("app.alert_tasks._render_and_send", new_callable=AsyncMock) as mock_send:
            mock_qj.return_value = mock_jobs
            mock_send.return_value = {"status": "sent", "resend_message_id": "msg-retry-1"}
            result = await _async_retry_failed_deliveries()

        assert result["processed"] == 1
        assert result["retried"] == 1
        assert result["sent"] == 1
        assert result["failed"] == 0
        mock_send.assert_called_once()

        rows = (await db_session.execute(
            text("""
                SELECT status FROM jobs.alert_deliveries
                WHERE subscription_id = :sid
                ORDER BY delivered_at
            """),
            {"sid": str(sub_id)},
        )).fetchall()
        statuses = [r.status for r in rows]
        assert "failed" in statuses
        assert "sent" in statuses

        await db_session.execute(
            text("DELETE FROM jobs.alert_subscriptions WHERE user_id = 'retry-test-user-1'")
        )
        await db_session.commit()

    async def test_retry_gives_up_after_max_retries(self, db_session: AsyncSession):
        """A subscription with _MAX_ALERT_RETRIES failed rows today is not retried again."""
        from datetime import timedelta
        from unittest.mock import AsyncMock, patch, MagicMock
        from tests.conftest import _TestSession
        from app.alert_tasks import _async_retry_failed_deliveries, _MAX_ALERT_RETRIES, _RETRY_BASE_MINUTES

        sub_id_result = await db_session.execute(
            text("""
                INSERT INTO jobs.alert_subscriptions
                    (user_id, email, name, is_active, delivery_time_utc, keywords)
                VALUES (:uid, :email, 'Retry User 2', true, 9, ARRAY['python'])
                ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email
                RETURNING id
            """),
            {"uid": "retry-test-user-2", "email": "retry2@example.com"},
        )
        sub_id = sub_id_result.scalar_one()
        await db_session.commit()

        old_enough = datetime.now(timezone.utc) - timedelta(minutes=_RETRY_BASE_MINUTES * 4)
        for _ in range(_MAX_ALERT_RETRIES):
            await db_session.execute(
                text("""
                    INSERT INTO jobs.alert_deliveries
                        (subscription_id, delivered_at, jobs_sent, status, error_message)
                    VALUES (:sid, :ts, 0, 'failed', 'transient error')
                """),
                {"sid": str(sub_id), "ts": old_enough},
            )
        await db_session.commit()

        with patch("app.alert_tasks._make_session", return_value=_TestSession), \
             patch("app.alert_tasks._render_and_send", new_callable=AsyncMock) as mock_send:
            result = await _async_retry_failed_deliveries()

        assert result["processed"] == 0, (
            "Subscription with max failed rows must not be picked up for retry"
        )
        mock_send.assert_not_called()

        await db_session.execute(
            text("DELETE FROM jobs.alert_subscriptions WHERE user_id = 'retry-test-user-2'")
        )
        await db_session.commit()

    async def test_retry_waits_when_backoff_not_elapsed(self, db_session: AsyncSession):
        """A subscription whose most recent failure is too recent is not retried yet."""
        from unittest.mock import AsyncMock, patch
        from tests.conftest import _TestSession
        from app.alert_tasks import _async_retry_failed_deliveries

        sub_id_result = await db_session.execute(
            text("""
                INSERT INTO jobs.alert_subscriptions
                    (user_id, email, name, is_active, delivery_time_utc, keywords)
                VALUES (:uid, :email, 'Retry User 3', true, 9, ARRAY['python'])
                ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email
                RETURNING id
            """),
            {"uid": "retry-test-user-3", "email": "retry3@example.com"},
        )
        sub_id = sub_id_result.scalar_one()
        await db_session.commit()

        # delivered_at = now() (0 seconds ago) — well inside the 5-minute backoff window
        await db_session.execute(
            text("""
                INSERT INTO jobs.alert_deliveries
                    (subscription_id, delivered_at, jobs_sent, status, error_message)
                VALUES (:sid, now(), 0, 'failed', 'transient error')
            """),
            {"sid": str(sub_id)},
        )
        await db_session.commit()

        with patch("app.alert_tasks._make_session", return_value=_TestSession), \
             patch("app.alert_tasks._render_and_send", new_callable=AsyncMock) as mock_send:
            result = await _async_retry_failed_deliveries()

        assert result["processed"] == 0, (
            "Subscription failed just now must not be retried before backoff elapses"
        )
        mock_send.assert_not_called()

        await db_session.execute(
            text("DELETE FROM jobs.alert_subscriptions WHERE user_id = 'retry-test-user-3'")
        )
        await db_session.commit()

    async def test_retry_skips_when_no_matching_jobs(self, db_session: AsyncSession):
        """If no jobs match during retry, inserts skipped_no_matches and stops retrying."""
        from datetime import timedelta
        from unittest.mock import AsyncMock, patch
        from tests.conftest import _TestSession
        from app.alert_tasks import _async_retry_failed_deliveries, _RETRY_BASE_MINUTES

        sub_id_result = await db_session.execute(
            text("""
                INSERT INTO jobs.alert_subscriptions
                    (user_id, email, name, is_active, delivery_time_utc, keywords)
                VALUES (:uid, :email, 'Retry User 4', true, 9, ARRAY['zzz-nomatches'])
                ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email
                RETURNING id
            """),
            {"uid": "retry-test-user-4", "email": "retry4@example.com"},
        )
        sub_id = sub_id_result.scalar_one()
        await db_session.commit()

        old_enough = datetime.now(timezone.utc) - timedelta(minutes=_RETRY_BASE_MINUTES * 2)
        await db_session.execute(
            text("""
                INSERT INTO jobs.alert_deliveries
                    (subscription_id, delivered_at, jobs_sent, status, error_message)
                VALUES (:sid, :ts, 0, 'failed', 'transient error')
            """),
            {"sid": str(sub_id), "ts": old_enough},
        )
        await db_session.commit()

        with patch("app.alert_tasks._make_session", return_value=_TestSession), \
             patch("app.alert_tasks._query_matching_jobs", new_callable=AsyncMock) as mock_qj, \
             patch("app.alert_tasks._render_and_send", new_callable=AsyncMock) as mock_send:
            mock_qj.return_value = []
            result = await _async_retry_failed_deliveries()

        assert result["processed"] == 1
        assert result["skipped"] == 1
        mock_send.assert_not_called()

        rows = (await db_session.execute(
            text("""
                SELECT status FROM jobs.alert_deliveries
                WHERE subscription_id = :sid
            """),
            {"sid": str(sub_id)},
        )).fetchall()
        statuses = [r.status for r in rows]
        assert "skipped_no_matches" in statuses

        await db_session.execute(
            text("DELETE FROM jobs.alert_subscriptions WHERE user_id = 'retry-test-user-4'")
        )
        await db_session.commit()

    async def test_retry_does_not_resend_already_sent_subscription(self, db_session: AsyncSession):
        """A subscription that has a 'sent' row today is not picked up for retry."""
        from datetime import timedelta
        from unittest.mock import AsyncMock, patch
        from tests.conftest import _TestSession
        from app.alert_tasks import _async_retry_failed_deliveries, _RETRY_BASE_MINUTES

        sub_id_result = await db_session.execute(
            text("""
                INSERT INTO jobs.alert_subscriptions
                    (user_id, email, name, is_active, delivery_time_utc)
                VALUES (:uid, :email, 'Retry User 5', true, 9)
                ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email
                RETURNING id
            """),
            {"uid": "retry-test-user-5", "email": "retry5@example.com"},
        )
        sub_id = sub_id_result.scalar_one()
        await db_session.commit()

        old_enough = datetime.now(timezone.utc) - timedelta(minutes=_RETRY_BASE_MINUTES * 2)
        await db_session.execute(
            text("""
                INSERT INTO jobs.alert_deliveries
                    (subscription_id, delivered_at, jobs_sent, status, error_message)
                VALUES (:sid, :ts, 0, 'failed', 'transient error')
            """),
            {"sid": str(sub_id), "ts": old_enough},
        )
        await db_session.execute(
            text("""
                INSERT INTO jobs.alert_deliveries
                    (subscription_id, jobs_sent, status)
                VALUES (:sid, 5, 'sent')
            """),
            {"sid": str(sub_id)},
        )
        await db_session.commit()

        with patch("app.alert_tasks._make_session", return_value=_TestSession), \
             patch("app.alert_tasks._render_and_send", new_callable=AsyncMock) as mock_send:
            result = await _async_retry_failed_deliveries()

        assert result["processed"] == 0
        mock_send.assert_not_called()

        await db_session.execute(
            text("DELETE FROM jobs.alert_subscriptions WHERE user_id = 'retry-test-user-5'")
        )
        await db_session.commit()


# ── Validation bounds ─────────────────────────────────────────────────────────

class TestValidationBounds:
    async def test_delivery_time_utc_above_23_rejected(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(delivery_time_utc=25),
            headers=_auth(ALERT_USER_1),
        )
        assert r.status_code == 422

    async def test_delivery_time_utc_negative_rejected(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(delivery_time_utc=-1),
            headers=_auth(ALERT_USER_1),
        )
        assert r.status_code == 422

    async def test_patch_delivery_time_utc_above_23_rejected(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe", json=_subscribe_payload(), headers=_auth(ALERT_USER_1)
        )
        r = await async_client.patch(
            f"{BASE}/subscription/{ALERT_USER_1}",
            json={"delivery_time_utc": 24},
            headers=_auth(ALERT_USER_1),
        )
        assert r.status_code == 422
