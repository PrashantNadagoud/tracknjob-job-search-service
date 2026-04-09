"""Celery task definitions for the TrackNJob crawler."""

import asyncio
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from typing import Any

from sqlalchemy import pool, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.celery_app import celery_app
from app.models import AtsSource, Company, CompanyDiscoveryQueue, Listing, SavedSearch

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

    Also populates ``title_normalized`` and ``seniority_level`` on every path.

    Returns list of newly inserted job UUIDs (for summarization queue).
    """
    from app.utils.title_normalizer import extract_seniority, normalize_title

    Session = _make_session()
    new_ids: list[str] = []
    now = datetime.now(timezone.utc)

    async with Session() as session:
        for job in jobs:
            source_url = job.get("source_url", "").strip()
            if not source_url:
                continue

            raw_title: str = job.get("title") or ""
            title_normalized = normalize_title(raw_title) or raw_title
            seniority_level = extract_seniority(title_normalized)

            existing = (
                await session.execute(
                    select(Listing).where(Listing.source_url == source_url)
                )
            ).scalar_one_or_none()

            if existing is not None:
                # Job already known by this exact URL
                existing.last_seen_at = now
                existing.title_normalized = title_normalized
                existing.seniority_level = seniority_level
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
                    listing = Listing(
                        **sanitized,
                        last_seen_at=now,
                        title_normalized=title_normalized,
                        seniority_level=seniority_level,
                    )
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
    """Set is_active=FALSE for legacy (non-ATS) jobs not seen in the last 12 hours.

    ATS-tracked listings (ats_type IS NOT NULL) use a separate 3-day staleness
    window enforced by ``run_crawl_pipeline``; they are intentionally excluded here
    so the 12h rule does not override the 3-day ATS rule.

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
                  AND  ats_type IS NULL
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


# ---------------------------------------------------------------------------
# ATS pipeline async helpers
# ---------------------------------------------------------------------------

async def _upsert_ats_jobs(
    jobs: list[dict[str, Any]],
    session_maker: async_sessionmaker[AsyncSession],
) -> list[str]:
    """Upsert ATS-sourced job listings using the ATS dedup key.

    Dedup priority:
    1. (company_id, ats_type, external_job_id) — ATS-native key (when all set)
    2. source_url — fall back for jobs without external_job_id
    3. INSERT — truly new job

    Also populates ``title_normalized`` and ``seniority_level`` via the
    title normalizer.

    Returns list of newly inserted UUIDs.
    """
    from app.utils.title_normalizer import extract_seniority, normalize_title

    new_ids: list[str] = []
    now = datetime.now(timezone.utc)

    async with session_maker() as session:
        for job in jobs:
            source_url = (job.get("source_url") or "").strip()
            if not source_url:
                continue

            raw_title: str = job.get("title") or ""
            title_normalized = normalize_title(raw_title) or raw_title
            seniority_level = extract_seniority(title_normalized)

            company_id: uuid.UUID | None = job.get("company_id")
            ats_type: str | None = job.get("ats_type")
            external_job_id: str | None = job.get("external_job_id")

            existing: Listing | None = None

            # 1. ATS dedup key
            if company_id and ats_type and external_job_id:
                existing = (
                    await session.execute(
                        select(Listing).where(
                            Listing.company_id == company_id,
                            Listing.ats_type == ats_type,
                            Listing.external_job_id == external_job_id,
                        )
                    )
                ).scalar_one_or_none()

            # 2. source_url fallback
            if existing is None and source_url:
                existing = (
                    await session.execute(
                        select(Listing).where(Listing.source_url == source_url)
                    )
                ).scalar_one_or_none()

            if existing is not None:
                existing.last_seen_at = now
                existing.title_normalized = title_normalized
                existing.seniority_level = seniority_level
                if not existing.is_active:
                    existing.is_active = True
                    logger.info(
                        "Reactivated ATS job: %s @ %s", existing.title, existing.company
                    )
                # Backfill ATS fields if they were missing
                if existing.ats_type is None and ats_type:
                    existing.ats_type = ats_type
                if existing.ats_source_id is None and job.get("ats_source_id"):
                    existing.ats_source_id = job["ats_source_id"]
                if existing.external_job_id is None and external_job_id:
                    existing.external_job_id = external_job_id
            else:
                # New listing
                listing = Listing(
                    title=raw_title,
                    company=job.get("company") or "",
                    location=job.get("location"),
                    remote=bool(job.get("remote", False)),
                    source_url=source_url,
                    source_label=job.get("source_label"),
                    posted_at=job.get("posted_at"),
                    country=job.get("country") or "US",
                    geo_restriction=job.get("geo_restriction"),
                    company_id=company_id,
                    ats_type=ats_type,
                    ats_source_id=job.get("ats_source_id"),
                    external_job_id=external_job_id,
                    title_normalized=title_normalized,
                    seniority_level=seniority_level,
                    employment_type=job.get("employment_type"),
                    department=job.get("department"),
                    salary_currency=job.get("salary_currency") or "USD",
                    salary_min_local=job.get("salary_min_local"),
                    salary_max_local=job.get("salary_max_local"),
                    salary_period=job.get("salary_period") or "annual",
                    last_seen_at=now,
                )
                session.add(listing)
                await session.flush()
                new_ids.append(str(listing.id))
                logger.info(
                    "Inserted ATS job: %s @ %s [%s]", raw_title, job.get("company"), ats_type
                )

        await session.commit()

    return new_ids


def _slugify(name: str) -> str:
    """Convert a company name to a URL-safe slug."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "unknown"


async def _async_run_crawl_pipeline() -> dict[str, Any]:
    """Query due ATS sources, dispatch each, upsert results, mark stale.

    - Queries ats_sources WHERE is_active AND last_crawled_at due AND backoff clear.
    - Dispatches each through CrawlDispatcher.
    - Upserts with ATS dedup key (company_id, ats_type, external_job_id).
    - Marks ATS-tracked listings with last_seen_at > 3 days old as is_active=FALSE.
    """
    from app.crawler.dispatcher import CrawlDispatcher

    Session = _make_session()
    dispatcher = CrawlDispatcher()

    # Fetch all due source IDs (read-only, separate session)
    async with Session() as session:
        result = await session.execute(
            text("""
                SELECT id FROM jobs.ats_sources
                WHERE is_active = TRUE
                  AND (
                    last_crawled_at IS NULL
                    OR last_crawled_at < NOW() - INTERVAL '20 hours'
                  )
                  AND (
                    backoff_until IS NULL
                    OR backoff_until < NOW()
                  )
                ORDER BY last_crawled_at ASC NULLS FIRST
            """)
        )
        source_ids: list[uuid.UUID] = [row[0] for row in result]

    logger.info("run_crawl_pipeline: %d due source(s) to crawl", len(source_ids))

    sources_crawled = 0
    total_jobs = 0
    new_ids_all: list[str] = []

    for source_id in source_ids:
        try:
            async with Session() as session:
                jobs = await dispatcher.dispatch(source_id, session)

            if jobs:
                new_ids = await _upsert_ats_jobs(jobs, Session)
                new_ids_all.extend(new_ids)
                total_jobs += len(jobs)

            sources_crawled += 1
        except Exception:
            logger.exception("run_crawl_pipeline: unhandled error for source_id=%s", source_id)

    # Queue AI summaries for newly inserted jobs
    queued = 0
    for job_id in new_ids_all:
        try:
            generate_job_summary.delay(job_id)
            queued += 1
        except Exception:
            logger.warning(
                "run_crawl_pipeline: could not queue summarization for %s", job_id
            )

    # Mark ATS-tracked listings stale after 3 days (not seen in this cycle)
    async with Session() as session:
        result = await session.execute(
            text("""
                UPDATE jobs.listings
                SET    is_active = FALSE
                WHERE  ats_type IS NOT NULL
                  AND  last_seen_at < NOW() - INTERVAL '3 days'
                  AND  is_active = TRUE
            """)
        )
        stale_count: int = result.rowcount
        await session.commit()

    logger.info(
        "run_crawl_pipeline: sources=%d jobs=%d new=%d stale_deactivated=%d",
        sources_crawled, total_jobs, len(new_ids_all), stale_count,
    )
    return {
        "sources_crawled": sources_crawled,
        "total_jobs": total_jobs,
        "new_job_ids": len(new_ids_all),
        "summaries_queued": queued,
        "stale_deactivated": stale_count,
    }


async def _process_discovery_item(
    item_id: uuid.UUID,
    Session: async_sessionmaker[AsyncSession],
) -> str:
    """Probe a single CompanyDiscoveryQueue item via CrawlDispatcher.

    Strategy: controlled temporary-source probe with cleanup.
    - A probe Company (slug ``{real_slug}-probe``) and AtsSource (is_active=False)
      are created (or reused from a previous attempt) in DB so that
      ``CrawlDispatcher.dispatch()`` can look them up.
    - is_active=False keeps the probe AtsSource invisible to run_crawl_pipeline.
    - On success: the probe Company is renamed to the real slug (or reassigned),
      AtsSource is activated, and the queue item is marked 'resolved'.
    - On rejection (>= 3 attempts): probe Company + AtsSource are deleted.
    - Between failed attempts (< 3): probe rows persist for the next run.

    Returns one of: 'resolved', 'failed', 'rejected', 'skipped'.
    """
    from app.crawler.dispatcher import CrawlDispatcher

    dispatcher = CrawlDispatcher()

    # Step 1: Read item details and ensure probe Company + AtsSource rows exist
    async with Session() as session:
        item = await session.get(CompanyDiscoveryQueue, item_id)
        if item is None or item.status != "pending":
            return "skipped"

        company_name: str = item.company_name
        website: str | None = item.website
        ats_type: str = item.suspected_ats or "workday"
        suspected_slug: str | None = item.suspected_slug
        market: str = item.market
        source: str = item.source
        attempt_count: int = item.attempt_count or 0

        probe_slug = _slugify(company_name) + "-probe"

        # Reuse probe Company from a previous attempt if it exists
        probe_company: Company | None = (
            await session.execute(
                select(Company).where(Company.slug == probe_slug)
            )
        ).scalar_one_or_none()

        if probe_company is None:
            probe_company = Company(
                slug=probe_slug,
                name=company_name,
                website=website,
                primary_ats_type=ats_type,
            )
            session.add(probe_company)
            await session.flush()

        probe_company_id: uuid.UUID = probe_company.id

        # Reuse probe AtsSource from a previous attempt if it exists
        probe_ats: AtsSource | None = (
            await session.execute(
                select(AtsSource).where(
                    AtsSource.company_id == probe_company_id,
                    AtsSource.ats_type == ats_type,
                )
            )
        ).scalar_one_or_none()

        if probe_ats is None:
            probe_ats = AtsSource(
                company_id=probe_company_id,
                ats_type=ats_type,
                ats_slug=suspected_slug,
                market=market,
                discovery_source=source,
                is_active=False,  # Hidden from run_crawl_pipeline until resolved
            )
            session.add(probe_ats)
            await session.flush()

        probe_ats_id: uuid.UUID = probe_ats.id
        await session.commit()

    # Step 2: Probe via CrawlDispatcher (uses probe AtsSource row in DB)
    async with Session() as session:
        jobs = await dispatcher.dispatch(probe_ats_id, session)

    now = datetime.now(timezone.utc)
    new_attempt_count = attempt_count + 1

    if jobs:
        # SUCCESS: promote probe entities to permanent, update queue item.
        # Must guard against uq_ats_source(company_id, ats_type) when a real
        # Company already exists and already has an AtsSource for this ats_type.
        async with Session() as session:
            real_slug = _slugify(company_name)

            # Check if a canonical Company with this slug already exists
            real_company: Company | None = (
                await session.execute(
                    select(Company).where(Company.slug == real_slug)
                )
            ).scalar_one_or_none()

            if real_company is None:
                # No canonical company: rename probe Company → real slug (in-place)
                p_company = await session.get(Company, probe_company_id)
                if p_company:
                    p_company.slug = real_slug
                real_company_id: uuid.UUID = probe_company_id
                # Activate probe AtsSource for the renamed company
                p_ats = await session.get(AtsSource, probe_ats_id)
                if p_ats:
                    p_ats.is_active = True
                active_ats_id: uuid.UUID = probe_ats_id
            else:
                # Canonical company exists: check for existing AtsSource constraint
                real_company_id = real_company.id
                existing_ats: AtsSource | None = (
                    await session.execute(
                        select(AtsSource).where(
                            AtsSource.company_id == real_company_id,
                            AtsSource.ats_type == ats_type,
                        )
                    )
                ).scalar_one_or_none()

                if existing_ats is None:
                    # No conflict: reassign probe AtsSource to canonical company
                    p_ats = await session.get(AtsSource, probe_ats_id)
                    if p_ats:
                        p_ats.company_id = real_company_id
                        p_ats.is_active = True
                    active_ats_id = probe_ats_id
                else:
                    # Conflict: canonical company already has (company_id, ats_type)
                    # Merge: delete probe AtsSource, use the existing one
                    p_ats = await session.get(AtsSource, probe_ats_id)
                    if p_ats:
                        await session.delete(p_ats)
                    # Activate existing AtsSource if it was dormant
                    existing_ats.is_active = True
                    active_ats_id = existing_ats.id

                # Delete probe Company — canonical company takes ownership
                p_company = (
                    await session.execute(
                        select(Company).where(Company.slug == probe_slug)
                    )
                ).scalar_one_or_none()
                if p_company:
                    await session.delete(p_company)

            # Update queue item
            q_item = await session.get(CompanyDiscoveryQueue, item_id)
            if q_item:
                q_item.status = "resolved"
                q_item.resolved_company_id = real_company_id
                q_item.last_attempted_at = now
                q_item.attempt_count = new_attempt_count

            await session.commit()

        # Re-inject correct company context into job dicts, then upsert
        for job in jobs:
            job.setdefault("company", company_name)
            job["company_id"] = real_company_id
            job["ats_source_id"] = active_ats_id
            job["ats_type"] = ats_type
            job.setdefault("country", market)

        await _upsert_ats_jobs(jobs, Session)

        logger.info(
            "run_discovery_queue: resolved company=%s ats_type=%s jobs=%d",
            company_name, ats_type, len(jobs),
        )
        return "resolved"

    # FAILURE path — increment attempt_count; reject and clean up at >= 3
    async with Session() as session:
        q_item = await session.get(CompanyDiscoveryQueue, item_id)
        if q_item is None:
            return "skipped"

        q_item.last_attempted_at = now
        q_item.attempt_count = new_attempt_count

        if new_attempt_count >= 3:
            q_item.status = "rejected"
            q_item.error_message = (
                f"Exhausted {new_attempt_count} probe attempts for ats_type={ats_type!r}"
            )

            # Clean up probe entities — they never resolved
            p_ats = await session.get(AtsSource, probe_ats_id)
            if p_ats:
                await session.delete(p_ats)
            p_company = (
                await session.execute(
                    select(Company).where(Company.slug == probe_slug)
                )
            ).scalar_one_or_none()
            if p_company:
                await session.delete(p_company)

            await session.commit()
            logger.info(
                "run_discovery_queue: rejected company=%s ats_type=%s after %d attempt(s)",
                company_name, ats_type, new_attempt_count,
            )
            return "rejected"

        await session.commit()
        logger.info(
            "run_discovery_queue: attempt %d/3 failed for company=%s ats_type=%s",
            new_attempt_count, company_name, ats_type,
        )
        return "failed"


async def _async_run_discovery_queue() -> dict[str, Any]:
    """Process up to 50 pending rows from company_discovery_queue.

    Probes each candidate using the real crawler (via CRAWLER_MAP).
    Company + AtsSource rows are created ONLY on a successful probe.
    On failure: increments attempt_count; rejects at >= 3 attempts.
    No temporary DB entities are created for failed probes.
    """
    Session = _make_session()

    async with Session() as session:
        result = await session.execute(
            text("""
                SELECT id FROM jobs.company_discovery_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 50
            """)
        )
        item_ids: list[uuid.UUID] = [row[0] for row in result]

    logger.info("run_discovery_queue: %d pending item(s) to process", len(item_ids))

    counts: dict[str, int] = {"resolved": 0, "failed": 0, "rejected": 0, "skipped": 0}

    for item_id in item_ids:
        try:
            outcome = await _process_discovery_item(item_id, Session)
            counts[outcome] = counts.get(outcome, 0) + 1
        except Exception:
            logger.exception(
                "run_discovery_queue: unhandled error for queue item %s", item_id
            )
            counts["failed"] += 1
            # Increment attempt_count even on unexpected exceptions so the item
            # can eventually reach rejection rather than staying pending forever.
            try:
                async with Session() as s:
                    q_item = await s.get(CompanyDiscoveryQueue, item_id)
                    if q_item and q_item.status == "pending":
                        q_item.attempt_count = (q_item.attempt_count or 0) + 1
                        if q_item.attempt_count >= 3:
                            q_item.status = "rejected"
                            q_item.error_message = "Unexpected error during probe"
                        await s.commit()
            except Exception:
                logger.exception(
                    "run_discovery_queue: failed to update attempt_count for item %s", item_id
                )

    logger.info(
        "run_discovery_queue: done — resolved=%d failed=%d rejected=%d skipped=%d",
        counts["resolved"], counts["failed"], counts["rejected"], counts["skipped"],
    )
    return {"items_processed": len(item_ids), **counts}


# ---------------------------------------------------------------------------
# New Celery tasks — ATS pipeline & discovery queue
# ---------------------------------------------------------------------------

@celery_app.task(name="app.crawler.tasks.run_crawl_pipeline")
def run_crawl_pipeline() -> dict[str, Any]:  # type: ignore[override]
    """Crawl all active + due ATS sources and upsert listings."""
    return asyncio.run(_async_run_crawl_pipeline())


@celery_app.task(name="app.crawler.tasks.run_discovery_queue")
def run_discovery_queue() -> dict[str, Any]:  # type: ignore[override]
    """Process pending company discovery queue rows (up to 50 per run)."""
    return asyncio.run(_async_run_discovery_queue())
