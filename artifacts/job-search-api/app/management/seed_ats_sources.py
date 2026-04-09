"""Seed ats_sources from environment-configured keyword/slug lists.

Usage:
    python -m app.management.seed_ats_sources

Reads three env vars (with defaults from .env.example):
    NAUKRI_KEYWORD_LIST   — comma-separated keyword search terms for Naukri
    FOUNDIT_KEYWORD_LIST  — comma-separated keyword search terms for Foundit
    WORKDAY_SEED_SLUGS    — comma-separated company slugs for Workday

For each keyword/slug, creates a Company (slug = ats-type--keyword-slug) and
a corresponding AtsSource row IF one does not already exist.  Rows are
idempotent: running the script multiple times is safe.
"""

import asyncio
import logging
import re
import sys

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings
from app.models import AtsSource, Company

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "unknown"


async def _seed(session: AsyncSession, company_slug: str, company_name: str, ats_type: str, ats_slug: str, market: str) -> str:
    """Insert Company + AtsSource if they don't already exist. Returns 'created' or 'skipped'."""
    existing_ats: AtsSource | None = (
        await session.execute(
            select(AtsSource).join(Company, AtsSource.company_id == Company.id).where(
                Company.slug == company_slug,
                AtsSource.ats_type == ats_type,
            )
        )
    ).scalar_one_or_none()

    if existing_ats is not None:
        return "skipped"

    company: Company | None = (
        await session.execute(select(Company).where(Company.slug == company_slug))
    ).scalar_one_or_none()

    if company is None:
        company = Company(slug=company_slug, name=company_name, primary_ats_type=ats_type)
        session.add(company)
        await session.flush()

    ats = AtsSource(
        company_id=company.id,
        ats_type=ats_type,
        ats_slug=ats_slug,
        market=market,
        is_active=True,
    )
    session.add(ats)
    return "created"


async def _run() -> None:
    settings = get_settings()

    from urllib.parse import urlparse
    raw_url = settings.DATABASE_URL
    if raw_url.startswith("postgresql://"):
        raw_url = raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(raw_url, echo=False)
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    totals: dict[str, int] = {"created": 0, "skipped": 0}

    async with Session() as session:
        # Naukri keywords (India market)
        for keyword in settings.naukri_keywords():
            slug = f"naukri--{_slugify(keyword)}"
            outcome = await _seed(session, slug, keyword, "naukri", keyword, "IN")
            totals[outcome] += 1
            logger.info("naukri  keyword=%-30s -> %s", keyword, outcome)

        # Foundit keywords (India market)
        for keyword in settings.foundit_keywords():
            slug = f"foundit--{_slugify(keyword)}"
            outcome = await _seed(session, slug, keyword, "foundit", keyword, "IN")
            totals[outcome] += 1
            logger.info("foundit keyword=%-30s -> %s", keyword, outcome)

        # Workday company slugs (global / US market)
        for company_slug in settings.workday_seed_slugs():
            slug = f"workday--{_slugify(company_slug)}"
            company_name = company_slug.title()
            outcome = await _seed(session, slug, company_name, "workday", company_slug, "US")
            totals[outcome] += 1
            logger.info("workday slug=%-30s -> %s", company_slug, outcome)

        await session.commit()

    await engine.dispose()
    logger.info("Done — created=%d skipped=%d", totals["created"], totals["skipped"])


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
