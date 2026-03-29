"""Celery task definitions for the TrackNJob crawler."""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from typing import Any

from sqlalchemy import pool, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.celery_app import celery_app
from app.models import Listing, SavedSearch

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
        "country": job.get("country", "US"),
        "geo_restriction": job.get("geo_restriction") or None,
    }


# ---------------------------------------------------------------------------
# Async helpers
# ---------------------------------------------------------------------------

async def _check_duplicate(
    session: AsyncSession,
    title: str,
    company: str,
    country: str,
) -> uuid.UUID | None:
    """Return the UUID of an existing active job that looks like a duplicate.

    Uses pg_trgm similarity (threshold 0.85) against jobs posted in the last
    30 days from the same company and country.
    """
    row = (
        await session.execute(
            text("""
                SELECT id FROM jobs.listings
                WHERE company  = :company
                  AND similarity(title, :title) > 0.85
                  AND posted_at > NOW() - INTERVAL '30 days'
                  AND country  = :country
                  AND is_active = TRUE
                LIMIT 1
            """),
            {"company": company, "title": title, "country": country},
        )
    ).fetchone()
    return row[0] if row else None


async def _upsert_jobs(jobs: list[dict[str, Any]]) -> list[str]:
    """Upsert job listings with ghost-job + duplicate detection.

    Logic per job:
    - EXISTS by source_url + is_active=TRUE  → bump last_seen_at
    - EXISTS by source_url + is_active=FALSE → reactivate + bump last_seen_at
    - NEW, duplicate found via pg_trgm       → bump last_seen_at on match, skip insert
    - NEW, no duplicate                      → INSERT with last_seen_at=NOW()

    Returns list of newly inserted job UUIDs (for summarization queue).
    """
    Session = _make_session()
    new_ids: list[str] = []
    now = datetime.now(timezone.utc)

    async with Session() as session:
        for job in jobs:
            source_url = job.get("source_url", "").strip()
            if not source_url:
                continue

            existing = (
                await session.execute(
                    select(Listing).where(Listing.source_url == source_url)
                )
            ).scalar_one_or_none()

            if existing is not None:
                # Job already known by this exact URL
                existing.last_seen_at = now
                if not existing.is_active:
                    existing.is_active = True
                    logger.info(
                        "Reactivated job: %s @ %s", existing.title, existing.company
                    )
                # (active jobs: last_seen_at bumped silently)

            else:
                # Potentially new job — run duplicate check before inserting
                sanitized = _sanitize_job(job)
                title   = sanitized["title"]
                company = sanitized["company"]
                country = sanitized["country"]

                dup_id = await _check_duplicate(session, title, company, country)

                if dup_id is not None:
                    logger.info(
                        "Duplicate detected: %r at %s — skipping insert",
                        title,
                        company,
                    )
                    await session.execute(
                        text(
                            "UPDATE jobs.listings "
                            "SET last_seen_at = NOW() "
                            "WHERE id = :id"
                        ),
                        {"id": str(dup_id)},
                    )
                else:
                    # Truly new job — insert
                    listing = Listing(**sanitized, last_seen_at=now)
                    session.add(listing)
                    await session.flush()
                    new_ids.append(str(listing.id))
                    logger.info("Inserted new job: %s @ %s", title, company)

        await session.commit()

    return new_ids


async def _async_crawl_all(country: str = "ALL") -> None:
    """Run crawlers filtered by country, upsert results, queue summary generation.

    country: "US" → US crawlers only, "IN" → India crawlers only, "ALL" → all.
    """
    from app.crawler.companies.cloudflare import CloudflareCrawler
    from app.crawler.companies.linear import LinearCrawler
    from app.crawler.companies.notion import NotionCrawler
    from app.crawler.companies.stripe import StripeCrawler
    from app.crawler.companies.vercel import VercelCrawler
    from app.crawler.companies.india.amazon import AmazonIndiaCrawler
    from app.crawler.companies.india.flipkart import FlipkartCrawler
    from app.crawler.companies.india.google import GoogleIndiaCrawler
    from app.crawler.companies.india.microsoft import MicrosoftIndiaCrawler
    from app.crawler.companies.india.razorpay import RazorpayCrawler

    all_crawlers = [
        StripeCrawler(),
        NotionCrawler(),
        LinearCrawler(),
        VercelCrawler(),
        CloudflareCrawler(),
        GoogleIndiaCrawler(),
        MicrosoftIndiaCrawler(),
        AmazonIndiaCrawler(),
        FlipkartCrawler(),
        RazorpayCrawler(),
    ]

    country_upper = country.upper()
    if country_upper == "ALL":
        crawlers = all_crawlers
    else:
        crawlers = [c for c in all_crawlers if c.country == country_upper]

    logger.info(
        "crawl_all_companies: country=%s → running %d crawler(s)",
        country_upper,
        len(crawlers),
    )

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


async def _async_deactivate_stale() -> int:
    """Set is_active=FALSE for all jobs not seen in the last 12 hours.

    Returns the number of rows deactivated.
    """
    Session = _make_session()

    async with Session() as session:
        result = await session.execute(
            text("""
                UPDATE jobs.listings
                SET    is_active = FALSE
                WHERE  last_seen_at < NOW() - INTERVAL '12 hours'
                  AND  is_active = TRUE
            """)
        )
        await session.commit()
        count: int = result.rowcount
        logger.info("deactivate_stale_jobs: deactivated %d job(s)", count)
        return count


async def _async_send_job_alerts() -> dict[str, int]:
    """Process all saved searches with alert_email=TRUE.

    For each search:
    - Re-runs its stored filters against jobs.listings
    - Computes new_job_ids = current_ids − last_alerted_job_ids
    - If new jobs exist, sends an email via Resend and records the alert
    - Skips searches with no user_email configured

    Returns {"searches_processed": N, "emails_sent": M}
    """
    from datetime import timedelta

    from app.email import send_job_alert_email

    Session = _make_session()
    searches_processed = 0
    emails_sent = 0

    _posted_cutoffs: dict[str, timedelta] = {
        "24h": timedelta(days=1),
        "3d":  timedelta(days=3),
        "7d":  timedelta(days=7),
        "30d": timedelta(days=30),
    }

    async with Session() as session:
        # Fetch every saved search that has alert_email enabled and a stored email
        result = await session.execute(
            select(SavedSearch).where(
                SavedSearch.alert_email == True,  # noqa: E712
                SavedSearch.user_email.is_not(None),
            )
        )
        searches = result.scalars().all()
        logger.info("send_job_alerts: processing %d alert-enabled search(es)", len(searches))

        for search in searches:
            searches_processed += 1
            filters: dict = search.filters or {}

            # ── Re-run search filters ─────────────────────────────────────
            stmt = select(Listing.id).where(Listing.is_active == True)  # noqa: E712

            q = filters.get("q")
            if q:
                stmt = stmt.where(
                    text(
                        "to_tsvector('english', title || ' ' || company || ' ' || COALESCE(location,''))"
                        " @@ plainto_tsquery('english', :fts_q)"
                    ).bindparams(fts_q=q)
                )

            location = filters.get("location")
            if location:
                stmt = stmt.where(Listing.location.ilike(f"%{location}%"))

            if filters.get("remote"):
                stmt = stmt.where(Listing.remote == True)  # noqa: E712

            source = filters.get("source")
            if source:
                stmt = stmt.where(Listing.source_label == source)

            company = filters.get("company")
            if company:
                stmt = stmt.where(Listing.company.ilike(f"%{company}%"))

            country = (filters.get("country") or "US").upper()
            if country in ("US", "IN"):
                stmt = stmt.where(Listing.country == country)

            posted = filters.get("posted")
            if posted and posted in _posted_cutoffs:
                cutoff = datetime.now(timezone.utc) - _posted_cutoffs[posted]
                stmt = stmt.where(Listing.posted_at >= cutoff)

            id_rows = (await session.execute(stmt)).scalars().all()
            current_ids: list[str] = [str(row) for row in id_rows]

            # ── Diff against last alerted ─────────────────────────────────
            last_alerted: list[str] = search.last_alerted_job_ids or []
            last_alerted_set = set(last_alerted)
            new_job_ids = [jid for jid in current_ids if jid not in last_alerted_set]

            if not new_job_ids:
                logger.debug(
                    "send_job_alerts: search %s (%s) — no new jobs, skipping",
                    search.id,
                    search.name,
                )
                continue

            # ── Fetch full job objects for new ids ────────────────────────
            new_uuid_ids = []
            for jid in new_job_ids:
                try:
                    new_uuid_ids.append(uuid.UUID(jid))
                except ValueError:
                    continue

            job_rows_result = await session.execute(
                select(Listing).where(Listing.id.in_(new_uuid_ids))
            )
            new_job_objects = job_rows_result.scalars().all()

            new_jobs_payload = [
                {
                    "title": j.title,
                    "company": j.company,
                    "location": j.location,
                    "source_url": j.source_url,
                    "salary_range": j.salary_range,
                }
                for j in new_job_objects
            ]

            # ── Send email ────────────────────────────────────────────────
            try:
                send_job_alert_email(
                    to_email=search.user_email,
                    search_name=search.name,
                    new_jobs=new_jobs_payload,
                )
                emails_sent += 1
                logger.info(
                    "send_job_alerts: emailed %s — %d new job(s) for search '%s'",
                    search.user_email,
                    len(new_jobs_payload),
                    search.name,
                )
            except Exception:
                logger.exception(
                    "send_job_alerts: failed to send email for search %s", search.id
                )
                continue

            # ── Update last_alerted state ─────────────────────────────────
            await session.execute(
                text("""
                    UPDATE jobs.saved_searches
                    SET last_alerted_at       = NOW(),
                        last_alerted_job_ids  = :job_ids ::jsonb
                    WHERE id = :search_id
                """),
                {"job_ids": json.dumps(current_ids), "search_id": str(search.id)},
            )

        await session.commit()

    logger.info(
        "send_job_alerts: done — %d search(es) processed, %d email(s) sent",
        searches_processed,
        emails_sent,
    )
    return {"searches_processed": searches_processed, "emails_sent": emails_sent}


# ---------------------------------------------------------------------------
# Celery tasks
# ---------------------------------------------------------------------------

@celery_app.task(bind=True, name="app.crawler.tasks.crawl_all_companies")
def crawl_all_companies(self, country: str = "ALL"):  # type: ignore[override]
    """Crawl company career pages and upsert into jobs.listings.

    country: "ALL" (default) → all crawlers; "US" → US only; "IN" → India only.
    """
    asyncio.run(_async_crawl_all(country=country))
    return {"status": "completed", "country": country}


@celery_app.task(bind=True, name="app.crawler.tasks.generate_job_summary")
def generate_job_summary(self, job_id: str):  # type: ignore[override]
    """Generate AI summary, tags, and salary_range for a single job listing."""
    asyncio.run(_async_summarize(job_id))
    return {"status": "completed", "job_id": job_id}


@celery_app.task(name="app.crawler.tasks.deactivate_stale_jobs")
def deactivate_stale_jobs() -> dict[str, int]:  # type: ignore[override]
    """Deactivate job listings not seen by any crawler in the last 12 hours."""
    count = asyncio.run(_async_deactivate_stale())
    return {"deactivated_count": count}


@celery_app.task(name="app.crawler.tasks.send_job_alerts")
def send_job_alerts() -> dict[str, int]:  # type: ignore[override]
    """Send job alert emails for all saved searches that have new results."""
    return asyncio.run(_async_send_job_alerts())
