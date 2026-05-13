"""Tests for the Job Alerts & Motivational Email feature (Task #8).

Covers:
  - POST /api/v1/alerts/subscribe        — create and upsert
  - GET  /api/v1/alerts/subscription/:id — retrieve
  - PATCH /api/v1/alerts/subscription/:id — partial update
  - DELETE /api/v1/alerts/unsubscribe/:id — soft-delete
  - POST /api/v1/alerts/test-send/:id    — trigger immediate send
  - Motivational service static fallback and OpenAI path
  - Celery task query logic
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Listing

ALERT_USER_1 = "test-alert-user-1"
ALERT_USER_2 = "test-alert-user-2"
BASE = "/api/v1/alerts"


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
        r = await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())
        assert r.status_code == 200
        data = r.json()
        assert "subscription_id" in data
        assert data["message"] == "Subscribed successfully"

    async def test_subscribe_upserts_on_second_call(self, async_client: AsyncClient):
        r1 = await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())
        id1 = r1.json()["subscription_id"]

        r2 = await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(email="updated@example.com"),
        )
        id2 = r2.json()["subscription_id"]

        assert id1 == id2

    async def test_subscribe_with_minimal_fields(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/subscribe",
            json={"user_id": ALERT_USER_2, "email": "min@example.com"},
        )
        assert r.status_code == 200

    async def test_subscribe_missing_email_returns_422(self, async_client: AsyncClient):
        r = await async_client.post(
            f"{BASE}/subscribe", json={"user_id": ALERT_USER_1}
        )
        assert r.status_code == 422


# ── GET /subscription/{user_id} ───────────────────────────────────────────────

class TestGetSubscription:
    async def test_get_returns_all_fields(self, async_client: AsyncClient):
        await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())

        r = await async_client.get(f"{BASE}/subscription/{ALERT_USER_1}")
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
        r = await async_client.get(f"{BASE}/subscription/no-such-user")
        assert r.status_code == 404


# ── PATCH /subscription/{user_id} ────────────────────────────────────────────

class TestPatchSubscription:
    async def test_patch_updates_keywords_and_hour(self, async_client: AsyncClient):
        await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())

        r = await async_client.patch(
            f"{BASE}/subscription/{ALERT_USER_1}",
            json={"keywords": ["django", "postgres"], "delivery_time_utc": 14},
        )
        assert r.status_code == 200
        data = r.json()
        assert "django" in data["keywords"]
        assert data["delivery_time_utc"] == 14

    async def test_patch_nonexistent_returns_404(self, async_client: AsyncClient):
        r = await async_client.patch(
            f"{BASE}/subscription/ghost-user", json={"delivery_time_utc": 10}
        )
        assert r.status_code == 404

    async def test_patch_empty_body_returns_current_state(self, async_client: AsyncClient):
        await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())
        r = await async_client.patch(f"{BASE}/subscription/{ALERT_USER_1}", json={})
        assert r.status_code == 200
        assert r.json()["delivery_time_utc"] == 9


# ── DELETE /unsubscribe/{user_id} ─────────────────────────────────────────────

class TestUnsubscribe:
    async def test_unsubscribe_sets_is_active_false(self, async_client: AsyncClient):
        await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())

        r = await async_client.delete(f"{BASE}/unsubscribe/{ALERT_USER_1}")
        assert r.status_code == 200

        get_r = await async_client.get(f"{BASE}/subscription/{ALERT_USER_1}")
        assert get_r.json()["is_active"] is False

    async def test_resubscribe_reactivates(self, async_client: AsyncClient):
        await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())
        await async_client.delete(f"{BASE}/unsubscribe/{ALERT_USER_1}")
        await async_client.post(f"{BASE}/subscribe", json=_subscribe_payload())

        get_r = await async_client.get(f"{BASE}/subscription/{ALERT_USER_1}")
        assert get_r.json()["is_active"] is True

    async def test_unsubscribe_nonexistent_returns_404(self, async_client: AsyncClient):
        r = await async_client.delete(f"{BASE}/unsubscribe/nobody")
        assert r.status_code == 404


# ── POST /test-send/{user_id} ─────────────────────────────────────────────────

class TestTestSend:
    async def test_test_send_skipped_when_no_matching_jobs(self, async_client: AsyncClient):
        await async_client.post(
            f"{BASE}/subscribe",
            json=_subscribe_payload(keywords=["zzzz-nonexistent-role-xyzzy-9999"]),
        )
        r = await async_client.post(f"{BASE}/test-send/{ALERT_USER_1}")
        assert r.status_code == 200
        data = r.json()
        assert data["jobs_found"] == 0

    async def test_test_send_nonexistent_user_returns_404(self, async_client: AsyncClient):
        r = await async_client.post(f"{BASE}/test-send/ghost-user-xyz")
        assert r.status_code == 404

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
        )

        with patch("app.alert_tasks._render_and_send", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"status": "sent", "resend_message_id": "msg-abc"}
            r = await async_client.post(f"{BASE}/test-send/{ALERT_USER_1}")

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
