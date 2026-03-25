"""Part 2 — Authentication tests."""

import pytest
from tests.conftest import make_token, TEST_USER_ID


class TestAuth:
    async def test_no_token_returns_401(self, async_client):
        resp = await async_client.get("/api/v1/jobs/search")
        assert resp.status_code == 401
        body = resp.json()
        assert body["error"] == "unauthorized"
        assert "message" in body
        assert body["details"] is None
        assert body["status_code"] == 401

    async def test_invalid_token_returns_401(self, async_client):
        resp = await async_client.get(
            "/api/v1/jobs/search",
            headers={"Authorization": "Bearer invalidtoken"},
        )
        assert resp.status_code == 401
        body = resp.json()
        assert body["error"] == "unauthorized"
        assert body["status_code"] == 401

    async def test_expired_token_returns_401(self, async_client):
        # Token expired 1 second ago
        expired_token = make_token(sub=TEST_USER_ID, exp_seconds=-1)
        resp = await async_client.get(
            "/api/v1/jobs/search",
            headers={"Authorization": f"Bearer {expired_token}"},
        )
        assert resp.status_code == 401
        body = resp.json()
        # Auth handler raises _UnauthorizedError → error="unauthorized"
        assert body["error"] == "unauthorized"
        assert body["status_code"] == 401

    async def test_valid_token_returns_200(self, async_client, auth_headers):
        resp = await async_client.get("/api/v1/jobs/search", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert "results" in body
        assert "total" in body
