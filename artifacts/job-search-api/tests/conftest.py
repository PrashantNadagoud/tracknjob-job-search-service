"""
Shared fixtures for the TrackNJob test suite.

Environment variables are set BEFORE any app module is imported so that
lru_cached Settings objects pick up the test values.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

# ── MUST come first: configure env before any app import ─────────────────────
TEST_USER_ID = "00000000-0000-0000-0000-000000000001"
TEST_USER_ID_2 = "00000000-0000-0000-0000-000000000002"
TEST_ADMIN_ID = "test-admin-uuid"
TEST_SECRET = "test-secret-key-for-ci-only"

_defaults = {
    "TNJ_SECRET_KEY": TEST_SECRET,
    "TNJ_FRONTEND_URL": "http://localhost:3000",
    "ADMIN_USER_ID": TEST_ADMIN_ID,
    "RESEND_API_KEY": "test_key",
    "OPENAI_API_KEY": "test_key",
}
for k, v in _defaults.items():
    os.environ.setdefault(k, v)

# TEST_DATABASE_URL overrides DATABASE_URL so tasks that read os.environ also
# hit the test database.
if os.environ.get("TEST_DATABASE_URL"):
    os.environ["DATABASE_URL"] = os.environ["TEST_DATABASE_URL"]

# Clear lru_cache so Settings is rebuilt with our env vars.
from app.config import get_settings  # noqa: E402

get_settings.cache_clear()

# ── Standard imports (after env setup) ───────────────────────────────────────
import pytest  # noqa: E402
from httpx import AsyncClient, ASGITransport  # noqa: E402
from jose import jwt  # noqa: E402
from sqlalchemy import pool, text  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.main import app  # noqa: E402
from app.models import HiddenJob, JobPreference, Listing, SavedSearch  # noqa: E402


# ── Test engine (NullPool so each call gets a fresh connection) ───────────────

def _build_asyncpg_url(raw: str) -> str:
    parsed = urlparse(raw)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("sslmode", None)
    return urlunparse(
        parsed._replace(
            scheme="postgresql+asyncpg",
            query=urlencode({k: v[0] for k, v in params.items()}),
        )
    )


_test_engine = create_async_engine(
    _build_asyncpg_url(os.environ["DATABASE_URL"]),
    poolclass=pool.NullPool,
)
_TestSession = async_sessionmaker(
    _test_engine, class_=AsyncSession, expire_on_commit=False
)


# ── Helper: make a signed JWT ─────────────────────────────────────────────────

def make_token(sub: str, email: str = "test@tracknjob.com", exp_seconds: int = 3600) -> str:
    return jwt.encode(
        {
            "sub": sub,
            "email": email,
            "exp": datetime.now(timezone.utc) + timedelta(seconds=exp_seconds),
        },
        os.environ["TNJ_SECRET_KEY"],
        algorithm="HS256",
    )


# ── Core fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture
async def db_session():
    """Raw async DB session for test setup / verification."""
    async with _TestSession() as session:
        yield session


@pytest.fixture(autouse=True)
async def cleanup_test_data():
    """Delete all test-owned rows after every test."""
    yield
    async with _TestSession() as s:
        await s.execute(
            text(
                "DELETE FROM jobs.hidden_jobs WHERE user_id IN ("
                "  '00000000-0000-0000-0000-000000000001'::uuid,"
                "  '00000000-0000-0000-0000-000000000002'::uuid"
                ")"
            )
        )
        await s.execute(
            text(
                "DELETE FROM jobs.job_preferences WHERE user_id IN ("
                "  '00000000-0000-0000-0000-000000000001'::uuid,"
                "  '00000000-0000-0000-0000-000000000002'::uuid"
                ")"
            )
        )
        await s.execute(
            text(
                "DELETE FROM jobs.saved_searches WHERE user_id IN ("
                "  '00000000-0000-0000-0000-000000000001'::uuid,"
                "  '00000000-0000-0000-0000-000000000002'::uuid"
                ")"
            )
        )
        await s.execute(
            text("DELETE FROM jobs.listings WHERE source_url LIKE 'http://test-%'")
        )
        await s.commit()


@pytest.fixture
async def async_client():
    """ASGI test client wired directly to the FastAPI app (no real HTTP).

    Overrides get_db to use _test_engine (NullPool) so endpoint calls never
    touch the QueuePool engine from app/db.py.
    """
    from app.db import get_db

    async def _override_get_db():
        async with _TestSession() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    app.dependency_overrides[get_db] = _override_get_db
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.fixture
def auth_headers():
    """Valid Bearer token for the test user."""
    return {"Authorization": f"Bearer {make_token(sub=TEST_USER_ID)}"}


@pytest.fixture
def admin_headers():
    """Valid Bearer token for the test admin."""
    return {"Authorization": f"Bearer {make_token(sub=TEST_ADMIN_ID)}"}


@pytest.fixture
async def sample_job(db_session: AsyncSession):
    """One active US job, committed to the DB."""
    job = Listing(
        title="Senior Python Developer",
        company="TestCorp",
        location="San Francisco, CA",
        remote=False,
        source_url=f"http://test-sample-{uuid.uuid4().hex}",
        source_label="TestCorp Careers",
        posted_at=datetime.now(timezone.utc),
        country="US",
        last_seen_at=datetime.now(timezone.utc),
        is_active=True,
    )
    db_session.add(job)
    await db_session.flush()
    await db_session.refresh(job)
    await db_session.commit()
    return job


@pytest.fixture
async def sample_preference(db_session: AsyncSession):
    """Job preferences for the test user."""
    pref = JobPreference(
        user_id=uuid.UUID(TEST_USER_ID),
        desired_title="Senior Backend Engineer",
        skills=["Python", "FastAPI", "PostgreSQL"],
        preferred_location="San Francisco",
        remote_only=True,
        seniority="senior",
    )
    db_session.add(pref)
    await db_session.flush()
    await db_session.refresh(pref)
    await db_session.commit()
    return pref
