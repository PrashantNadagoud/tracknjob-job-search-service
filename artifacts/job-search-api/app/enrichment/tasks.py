"""Celery tasks for company enrichment."""
import asyncio
import logging
import os
import re
from datetime import datetime, timezone

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.celery_app import celery_app
from app.enrichment.enricher import CompanyEnricher

logger = logging.getLogger(__name__)


def _make_session() -> AsyncSession:
    from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

    raw_url = os.environ["DATABASE_URL"]
    for prefix, replacement in [
        ("postgresql://", "postgresql+asyncpg://"),
        ("postgres://", "postgresql+asyncpg://"),
    ]:
        if raw_url.startswith(prefix):
            raw_url = replacement + raw_url[len(prefix):]
            break

    parsed = urlparse(raw_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("sslmode", None)
    clean_url = urlunparse(parsed._replace(query=urlencode({k: v[0] for k, v in params.items()})))

    engine = create_async_engine(clean_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    return factory()


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower())
    return slug.strip("-")


async def _async_enrich_new_companies() -> None:
    async with _make_session() as session:
        async with session.begin():
            rows = await session.execute(
                text("""
                    SELECT DISTINCT company
                    FROM jobs.listings
                    WHERE company_id IS NULL
                      AND is_active = TRUE
                    ORDER BY company
                """)
            )
            company_names: list[str] = [r[0] for r in rows.fetchall()]

    if not company_names:
        logger.info("No companies to enrich")
        return

    logger.info("Enriching %d new companies", len(company_names))
    enricher = CompanyEnricher()

    for name in company_names:
        slug = _slugify(name)
        try:
            async with _make_session() as session:
                async with session.begin():
                    await session.execute(
                        text("""
                            INSERT INTO jobs.companies (slug, name)
                            VALUES (:slug, :name)
                            ON CONFLICT (slug) DO NOTHING
                        """),
                        {"slug": slug, "name": name},
                    )

            record = await enricher.enrich(
                company_slug=slug,
                company_name=name,
                primary_role="Software Engineer",
                location="United States",
            )

            async with _make_session() as session:
                async with session.begin():
                    await session.execute(
                        text("""
                            UPDATE jobs.companies SET
                                funding_total_usd   = :funding_total_usd,
                                last_funding_type   = :last_funding_type,
                                last_funding_date   = :last_funding_date,
                                num_employees_range = :num_employees_range,
                                founded_year        = :founded_year,
                                company_type        = :company_type,
                                stock_ticker        = :stock_ticker,
                                stock_exchange      = :stock_exchange,
                                culture_score       = :culture_score,
                                ceo_approval_pct    = :ceo_approval_pct,
                                work_life_score     = :work_life_score,
                                remote_policy       = :remote_policy,
                                perks               = :perks,
                                salary_min_usd      = :salary_min_usd,
                                salary_max_usd      = :salary_max_usd,
                                salary_source       = :salary_source,
                                enriched_at         = :enriched_at,
                                enrichment_source   = :enrichment_source
                            WHERE slug = :slug
                        """),
                        {
                            "slug": slug,
                            "funding_total_usd": record.funding_total_usd,
                            "last_funding_type": record.last_funding_type,
                            "last_funding_date": record.last_funding_date,
                            "num_employees_range": record.num_employees_range,
                            "founded_year": record.founded_year,
                            "company_type": record.company_type,
                            "stock_ticker": record.stock_ticker,
                            "stock_exchange": record.stock_exchange,
                            "culture_score": record.culture_score,
                            "ceo_approval_pct": record.ceo_approval_pct,
                            "work_life_score": (
                                float(record.work_life_score)
                                if record.work_life_score is not None
                                else None
                            ),
                            "remote_policy": record.remote_policy,
                            "perks": record.perks,
                            "salary_min_usd": record.salary_min_usd,
                            "salary_max_usd": record.salary_max_usd,
                            "salary_source": record.salary_source,
                            "enriched_at": record.enriched_at,
                            "enrichment_source": record.enrichment_source or [],
                        },
                    )

                    row = await session.execute(
                        text("SELECT id FROM jobs.companies WHERE slug = :slug"),
                        {"slug": slug},
                    )
                    company_row = row.fetchone()
                    if company_row:
                        company_id = company_row[0]
                        await session.execute(
                            text("""
                                UPDATE jobs.listings
                                SET company_id = :company_id
                                WHERE company = :name AND company_id IS NULL
                            """),
                            {"company_id": company_id, "name": name},
                        )

        except Exception:
            logger.exception("Failed to enrich company: %s", name)


async def _async_reenrich_stale_companies() -> None:
    async with _make_session() as session:
        async with session.begin():
            rows = await session.execute(
                text("""
                    SELECT id, slug, name
                    FROM jobs.companies
                    WHERE enriched_at IS NULL
                       OR enriched_at < NOW() - INTERVAL '7 days'
                """)
            )
            stale: list[tuple] = rows.fetchall()

    if not stale:
        logger.info("No stale companies to re-enrich")
        return

    logger.info("Re-enriching %d stale companies", len(stale))
    enricher = CompanyEnricher()

    for company_id, slug, name in stale:
        try:
            record = await enricher.enrich(
                company_slug=slug,
                company_name=name,
                primary_role="Software Engineer",
                location="United States",
            )

            async with _make_session() as session:
                async with session.begin():
                    await session.execute(
                        text("""
                            UPDATE jobs.companies SET
                                funding_total_usd   = :funding_total_usd,
                                last_funding_type   = :last_funding_type,
                                last_funding_date   = :last_funding_date,
                                num_employees_range = :num_employees_range,
                                founded_year        = :founded_year,
                                company_type        = :company_type,
                                stock_ticker        = :stock_ticker,
                                stock_exchange      = :stock_exchange,
                                culture_score       = :culture_score,
                                ceo_approval_pct    = :ceo_approval_pct,
                                work_life_score     = :work_life_score,
                                remote_policy       = :remote_policy,
                                perks               = :perks,
                                salary_min_usd      = :salary_min_usd,
                                salary_max_usd      = :salary_max_usd,
                                salary_source       = :salary_source,
                                enriched_at         = :enriched_at,
                                enrichment_source   = :enrichment_source
                            WHERE id = :company_id
                        """),
                        {
                            "company_id": company_id,
                            "funding_total_usd": record.funding_total_usd,
                            "last_funding_type": record.last_funding_type,
                            "last_funding_date": record.last_funding_date,
                            "num_employees_range": record.num_employees_range,
                            "founded_year": record.founded_year,
                            "company_type": record.company_type,
                            "stock_ticker": record.stock_ticker,
                            "stock_exchange": record.stock_exchange,
                            "culture_score": record.culture_score,
                            "ceo_approval_pct": record.ceo_approval_pct,
                            "work_life_score": (
                                float(record.work_life_score)
                                if record.work_life_score is not None
                                else None
                            ),
                            "remote_policy": record.remote_policy,
                            "perks": record.perks,
                            "salary_min_usd": record.salary_min_usd,
                            "salary_max_usd": record.salary_max_usd,
                            "salary_source": record.salary_source,
                            "enriched_at": record.enriched_at,
                            "enrichment_source": record.enrichment_source or [],
                        },
                    )

        except Exception:
            logger.exception("Failed to re-enrich company slug=%s", slug)


@celery_app.task(name="app.enrichment.tasks.enrich_new_companies", bind=True, max_retries=2)
def enrich_new_companies(self):
    try:
        asyncio.get_event_loop().run_until_complete(_async_enrich_new_companies())
    except Exception as exc:
        logger.exception("enrich_new_companies task failed")
        raise self.retry(exc=exc, countdown=300)


@celery_app.task(name="app.enrichment.tasks.reenrich_stale_companies", bind=True, max_retries=2)
def reenrich_stale_companies(self):
    try:
        asyncio.get_event_loop().run_until_complete(_async_reenrich_stale_companies())
    except Exception as exc:
        logger.exception("reenrich_stale_companies task failed")
        raise self.retry(exc=exc, countdown=600)
