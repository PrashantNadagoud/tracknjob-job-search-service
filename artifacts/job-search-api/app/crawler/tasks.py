"""Celery task definitions for the TrackNJob crawler."""

import asyncio
import logging
import os
import uuid
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from typing import Any

from sqlalchemy import pool, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.celery_app import celery_app
from app.models import Listing

logger = logging.getLogger(__name__)


def _make_session() -> async_sessionmaker[AsyncSession]:
    """Create a fresh engine+sessionmaker for use inside a Celery task.

    Uses NullPool so asyncpg connections are not shared across asyncio.run()
    calls, which each create a new event loop.
    """
    raw_url = os.environ["DATABASE_URL"]
    parsed = urlparse(raw_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    sslmode = params.pop("sslmode", [None])[0]
    new_query = urlencode({k: v[0] for k, v in params.items()})
    clean_url = urlunparse(
        parsed._replace(scheme="postgresql+asyncpg", query=new_query)
    )
    connect_args: dict[str, Any] = (
        {} if sslmode in ("disable", "allow", None) else {"ssl": True}
    )
    engine = create_async_engine(
        clean_url, connect_args=connect_args, poolclass=pool.NullPool
    )
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _sanitize_job(job: dict[str, Any]) -> dict[str, Any]:
    """Return only the fields accepted by the Listing model."""
    return {
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "location": job.get("location") or None,
        "remote": bool(job.get("remote", False)),
        "source_url": job.get("source_url", ""),
        "source_label": job.get("source_label") or None,
        "posted_at": job.get("posted_at"),
    }


# ---------------------------------------------------------------------------
# Async helpers
# ---------------------------------------------------------------------------

async def _upsert_jobs(jobs: list[dict[str, Any]]) -> list[str]:
    """Upsert job listings. Returns list of newly inserted job UUIDs."""
    Session = _make_session()
    new_ids: list[str] = []

    async with Session() as session:
        for job in jobs:
            source_url = job.get("source_url", "").strip()
            if not source_url:
                continue

            stmt = select(Listing).where(Listing.source_url == source_url)
            existing = (await session.execute(stmt)).scalar_one_or_none()

            if existing is None:
                listing = Listing(**_sanitize_job(job))
                session.add(listing)
                await session.flush()
                new_ids.append(str(listing.id))
                logger.info(
                    "Inserted new job: %s @ %s",
                    job.get("title"),
                    job.get("company"),
                )
            elif not existing.is_active:
                existing.is_active = True
                logger.info(
                    "Reactivated job: %s @ %s",
                    existing.title,
                    existing.company,
                )
            # else: already active — skip

        await session.commit()

    return new_ids


async def _async_crawl_all() -> None:
    """Run all crawlers, upsert results, queue summary generation."""
    from app.crawler.companies.cloudflare import CloudflareCrawler
    from app.crawler.companies.linear import LinearCrawler
    from app.crawler.companies.notion import NotionCrawler
    from app.crawler.companies.stripe import StripeCrawler
    from app.crawler.companies.vercel import VercelCrawler

    crawlers = [
        StripeCrawler(),
        NotionCrawler(),
        LinearCrawler(),
        VercelCrawler(),
        CloudflareCrawler(),
    ]

    for crawler in crawlers:
        try:
            logger.info("Starting crawl: %s", crawler.source_label)
            jobs = await crawler.fetch_jobs()
            logger.info("%s: fetched %d jobs", crawler.source_label, len(jobs))
            new_ids = await _upsert_jobs(jobs)
            queued = 0
            for job_id in new_ids:
                try:
                    generate_job_summary.delay(job_id)
                    queued += 1
                except Exception:
                    logger.warning(
                        "Could not queue summarization for %s (broker unavailable?); "
                        "job is in DB, summary will remain null",
                        job_id,
                    )
            logger.info(
                "%s: %d new jobs queued for summarization",
                crawler.source_label,
                len(new_ids),
            )
        except Exception:
            logger.exception("Crawler failed: %s", crawler.source_label)


async def _async_summarize(job_id: str) -> None:
    """Fetch job row and update summary/tags/salary_range via OpenAI."""
    from app.crawler.summarizer import generate_summary

    Session = _make_session()

    async with Session() as session:
        try:
            listing = await session.get(Listing, uuid.UUID(job_id))
        except ValueError:
            logger.error("generate_job_summary: invalid UUID %s", job_id)
            return

        if listing is None:
            logger.warning("generate_job_summary: job %s not found", job_id)
            return

        result = await generate_summary(
            title=listing.title,
            company=listing.company,
            location=listing.location,
        )

        listing.summary = result.get("summary")
        listing.tags = result.get("tags")
        listing.salary_range = result.get("salary_range")
        await session.commit()
        logger.info("Updated AI summary for job %s (%s)", job_id, listing.title)


# ---------------------------------------------------------------------------
# Celery tasks
# ---------------------------------------------------------------------------

@celery_app.task(bind=True, name="app.crawler.tasks.crawl_all_companies")
def crawl_all_companies(self):  # type: ignore[override]
    """Crawl all registered company career pages and upsert into jobs.listings."""
    asyncio.run(_async_crawl_all())
    return {"status": "completed"}


@celery_app.task(bind=True, name="app.crawler.tasks.generate_job_summary")
def generate_job_summary(self, job_id: str):  # type: ignore[override]
    """Generate AI summary, tags, and salary_range for a single job listing."""
    asyncio.run(_async_summarize(job_id))
    return {"status": "completed", "job_id": job_id}
