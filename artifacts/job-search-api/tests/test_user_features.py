"""Part 7 — Saved searches and hidden jobs tests."""

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Listing
from tests.conftest import TEST_USER_ID, TEST_USER_ID_2, make_token


def _make_headers(user_id: str) -> dict:
    return {"Authorization": f"Bearer {make_token(sub=user_id)}"}


class TestSavedSearches:
    async def test_save_search_creates_record(self, async_client, auth_headers):
        resp = await async_client.post(
            "/api/v1/jobs/saved-searches",
            headers=auth_headers,
            json={
                "name": "Remote Python Jobs",
                "filters": {"q": "python", "remote": True},
                "alert_email": False,
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        # id must be a valid UUID
        uuid.UUID(body["id"])
        assert body["name"] == "Remote Python Jobs"
        assert body["alert_email"] is False

    async def test_get_saved_searches_returns_only_own(
        self, async_client, db_session: AsyncSession
    ):
        """Two users each create a search; each sees only their own."""
        headers1 = _make_headers(TEST_USER_ID)
        headers2 = _make_headers(TEST_USER_ID_2)

        # User 1 creates a search
        r1 = await async_client.post(
            "/api/v1/jobs/saved-searches",
            headers=headers1,
            json={"name": "User1 Search", "filters": {"q": "python"}},
        )
        assert r1.status_code == 201

        # User 2 creates a search
        r2 = await async_client.post(
            "/api/v1/jobs/saved-searches",
            headers=headers2,
            json={"name": "User2 Search", "filters": {"q": "java"}},
        )
        assert r2.status_code == 201

        # User 1 fetches their searches
        resp = await async_client.get("/api/v1/jobs/saved-searches", headers=headers1)
        assert resp.status_code == 200
        names = [s["name"] for s in resp.json()["results"]]
        assert "User1 Search" in names
        assert "User2 Search" not in names

        # User 2 fetches their searches
        resp2 = await async_client.get("/api/v1/jobs/saved-searches", headers=headers2)
        assert resp2.status_code == 200
        names2 = [s["name"] for s in resp2.json()["results"]]
        assert "User2 Search" in names2
        assert "User1 Search" not in names2


class TestHiddenJobs:
    async def test_hide_job_excludes_from_search(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-hidesc-{uuid.uuid4().hex[:8]}"
        j1 = Listing(
            title="Visible Job",
            company="TestCo",
            location="NYC",
            remote=False,
            source_url=f"http://test-{src}-1",
            source_label=src,
            posted_at=None,
            country="US",
            last_seen_at=None,
            is_active=True,
        )
        j2 = Listing(
            title="Hidden Job",
            company="TestCo",
            location="NYC",
            remote=False,
            source_url=f"http://test-{src}-2",
            source_label=src,
            posted_at=None,
            country="US",
            last_seen_at=None,
            is_active=True,
        )
        db_session.add(j1)
        db_session.add(j2)
        await db_session.flush()
        j1_id = str(j1.id)
        j2_id = str(j2.id)
        await db_session.commit()

        # Hide j2
        hide_resp = await async_client.post(
            "/api/v1/jobs/hidden",
            headers=auth_headers,
            json={"job_id": j2_id},
        )
        assert hide_resp.status_code == 204

        # Search: only j1 should appear
        search_resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&country=US", headers=auth_headers
        )
        assert search_resp.status_code == 200
        body = search_resp.json()
        assert body["total"] == 1
        assert body["results"][0]["id"] == j1_id

    async def test_hide_job_conflict_returns_409(
        self, async_client, auth_headers, sample_job
    ):
        job_id = str(sample_job.id)

        # First hide — success
        r1 = await async_client.post(
            "/api/v1/jobs/hidden",
            headers=auth_headers,
            json={"job_id": job_id},
        )
        assert r1.status_code == 204

        # Second hide — conflict
        r2 = await async_client.post(
            "/api/v1/jobs/hidden",
            headers=auth_headers,
            json={"job_id": job_id},
        )
        assert r2.status_code == 409
        body = r2.json()
        assert body["error"] == "conflict"
        assert body["status_code"] == 409

    async def test_hide_nonexistent_job_returns_404(
        self, async_client, auth_headers
    ):
        resp = await async_client.post(
            "/api/v1/jobs/hidden",
            headers=auth_headers,
            json={"job_id": str(uuid.uuid4())},
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "not_found"
        assert body["status_code"] == 404
