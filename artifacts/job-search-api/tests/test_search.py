"""Part 3 — Search endpoint tests.

Each test uses a unique source_label so the `?source=` filter isolates
only the seeded rows, preventing interference from real production data.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import HiddenJob, Listing
from tests.conftest import TEST_USER_ID


def _job(
    *,
    suffix: str,
    source_label: str,
    title: str = "Test Job",
    company: str = "TestCo",
    remote: bool = False,
    country: str = "US",
    posted_at: datetime | None = None,
    is_active: bool = True,
) -> Listing:
    return Listing(
        title=title,
        company=company,
        location="Test City",
        remote=remote,
        source_url=f"http://test-{suffix}",
        source_label=source_label,
        posted_at=posted_at or datetime.now(timezone.utc),
        country=country,
        last_seen_at=datetime.now(timezone.utc),
        is_active=is_active,
    )


class TestSearch:
    async def test_search_returns_paginated_results(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-pag-{uuid.uuid4().hex[:8]}"
        for i in range(5):
            db_session.add(_job(suffix=f"{src}-{i}", source_label=src))
        await db_session.commit()

        resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&limit=3", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 5
        assert len(body["results"]) == 3
        assert body["page"] == 1
        assert body["limit"] == 3

    async def test_search_remote_filter(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-rem-{uuid.uuid4().hex[:8]}"
        for i in range(3):
            db_session.add(_job(suffix=f"{src}-r{i}", source_label=src, remote=True))
        for i in range(2):
            db_session.add(_job(suffix=f"{src}-n{i}", source_label=src, remote=False))
        await db_session.commit()

        resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&remote=true", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert all(r["remote"] is True for r in body["results"])
        assert body["total"] == 3

    async def test_search_country_filter_us(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-cus-{uuid.uuid4().hex[:8]}"
        for i in range(2):
            db_session.add(_job(suffix=f"{src}-us{i}", source_label=src, country="US"))
        for i in range(2):
            db_session.add(_job(suffix=f"{src}-in{i}", source_label=src, country="IN"))
        await db_session.commit()

        # country=US with source filter (source filter is source_label exact match)
        resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&country=US", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 2
        assert all(r["country"] == "US" for r in body["results"])

    async def test_search_country_filter_india(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-cin-{uuid.uuid4().hex[:8]}"
        for i in range(2):
            db_session.add(_job(suffix=f"{src}-us{i}", source_label=src, country="US"))
        for i in range(2):
            db_session.add(_job(suffix=f"{src}-in{i}", source_label=src, country="IN"))
        await db_session.commit()

        resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&country=IN", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 2
        assert all(r["country"] == "IN" for r in body["results"])

    async def test_search_country_all(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-call-{uuid.uuid4().hex[:8]}"
        for i in range(2):
            db_session.add(_job(suffix=f"{src}-us{i}", source_label=src, country="US"))
        for i in range(2):
            db_session.add(_job(suffix=f"{src}-in{i}", source_label=src, country="IN"))
        await db_session.commit()

        resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&country=ALL", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 4

    async def test_search_excludes_inactive_jobs(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-inact-{uuid.uuid4().hex[:8]}"
        db_session.add(_job(suffix=f"{src}-a", source_label=src, is_active=True))
        db_session.add(_job(suffix=f"{src}-i", source_label=src, is_active=False))
        await db_session.commit()

        resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&country=US", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert body["results"][0]["source_label"] == src

    async def test_search_excludes_hidden_jobs(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-hid-{uuid.uuid4().hex[:8]}"
        job1 = _job(suffix=f"{src}-1", source_label=src)
        job2 = _job(suffix=f"{src}-2", source_label=src)
        db_session.add(job1)
        db_session.add(job2)
        await db_session.flush()

        # Hide job2 for the test user
        hidden = HiddenJob(user_id=uuid.UUID(TEST_USER_ID), job_id=job1.id)
        db_session.add(hidden)
        await db_session.commit()

        resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&country=US", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        ids = [r["id"] for r in body["results"]]
        assert str(job1.id) not in ids
        assert str(job2.id) in ids

    async def test_search_fulltext_query(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-fts-{uuid.uuid4().hex[:8]}"
        db_session.add(
            _job(
                suffix=f"{src}-py",
                source_label=src,
                title="Senior Pythonista Developer",
                company="UniqueTestCo",
            )
        )
        await db_session.commit()

        resp = await async_client.get(
            "/api/v1/jobs/search?q=pythonista&country=US", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        titles = [r["title"] for r in body["results"]]
        assert "Senior Pythonista Developer" in titles

    async def test_search_posted_filter(
        self, async_client, auth_headers, db_session: AsyncSession
    ):
        src = f"test-pf-{uuid.uuid4().hex[:8]}"
        # 2 days ago — should match ?posted=7d
        db_session.add(
            _job(
                suffix=f"{src}-new",
                source_label=src,
                posted_at=datetime.now(timezone.utc) - timedelta(days=2),
            )
        )
        # 10 days ago — should NOT match ?posted=7d
        db_session.add(
            _job(
                suffix=f"{src}-old",
                source_label=src,
                posted_at=datetime.now(timezone.utc) - timedelta(days=10),
            )
        )
        await db_session.commit()

        resp = await async_client.get(
            f"/api/v1/jobs/search?source={src}&posted=7d", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert body["results"][0]["source_url"].endswith("new")
