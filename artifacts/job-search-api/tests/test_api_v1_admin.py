from unittest.mock import patch
from datetime import datetime, timezone

class TestAdmin:
    async def test_seed_status_unauthorized(self, async_client):
        resp = await async_client.get("/api/v1/admin/seed-status")
        assert resp.status_code == 401

    async def test_seed_status_forbidden(self, async_client, auth_headers):
        resp = await async_client.get("/api/v1/admin/seed-status", headers=auth_headers)
        assert resp.status_code == 403

    @patch('app.api.v1.admin.AsyncSession.execute')
    async def test_seed_status_authorized(self, mock_execute, async_client, admin_headers):
        # By mocking execute we skip the DB connection
        class MockResultQueue:
            def scalar(self): return 10
            def fetchall(self): return []

        class MockResultDate:
            def scalar(self): return datetime.now(timezone.utc)
            def fetchall(self): return []

        # The endpoint makes multiple queries. The ones for count need an int, the one for MAX(last_seen_at) needs datetime
        # We can just use side_effect
        def execute_side_effect(stmt, *args, **kwargs):
            if "MAX" in str(stmt):
                return MockResultDate()
            return MockResultQueue()

        mock_execute.side_effect = execute_side_effect

        resp = await async_client.get("/api/v1/admin/seed-status", headers=admin_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "discovery_queue" in data
