"""CrawlDispatcher: routes AtsSource rows to the correct ATS crawler.

Responsibilities:
- Map ats_type → crawler instance via CRAWLER_MAP
- Call crawler.crawl() and inject company name / company_id into results
- Handle all exception types with the correct back-off strategy:
    * RateLimitedException  → backoff 30 min, consecutive_failures += 1
    * SlugNotFoundException → deactivate (is_active=False), write dead letter
    * Any other Exception   → increment failures, exponential backoff, dead letter
- Update AtsSource fields on success and failure (never raises)
- Write CrawlDeadLetter rows on every failure
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.crawler.ats.bamboohr import BambooHRCrawler
from app.crawler.ats.foundit import FounditCrawler
from app.crawler.ats.jazzhr import JazzHRCrawler
from app.crawler.ats.naukri import NaukriCrawler
from app.crawler.ats.rippling import RipplingCrawler
from app.crawler.ats.smartrecruiters import SmartRecruitersCrawler
from app.crawler.ats.workday import WorkdayCrawler
from app.crawler.ats_base import BaseATSCrawler
from app.crawler.exceptions import CrawlerError, RateLimitedException, SlugNotFoundException
from app.models import AtsSource, Company, CrawlDeadLetter

logger = logging.getLogger(__name__)

CRAWLER_MAP: dict[str, BaseATSCrawler] = {
    "workday": WorkdayCrawler(),
    "smartrecruiters": SmartRecruitersCrawler(),
    "bamboohr": BambooHRCrawler(),
    "rippling": RipplingCrawler(),
    "jazzhr": JazzHRCrawler(),
    "naukri": NaukriCrawler(),
    "foundit": FounditCrawler(),
}

# Back-off durations indexed by consecutive_failures (after increment)
_BACKOFF_TABLE: list[timedelta] = [
    timedelta(minutes=30),   # 1 failure
    timedelta(minutes=30),   # 2 failures
    timedelta(hours=2),      # 3 failures
    timedelta(hours=2),      # 4 failures
    timedelta(hours=6),      # 5+ failures
]

_RATE_LIMIT_BACKOFF = timedelta(minutes=30)


def _backoff_for(failures: int) -> timedelta:
    idx = min(failures - 1, len(_BACKOFF_TABLE) - 1)
    return _BACKOFF_TABLE[idx]


class CrawlDispatcher:
    """Routes an AtsSource to the correct crawler and manages back-off state.

    ``dispatch()`` is guaranteed not to raise — all exceptions are caught,
    logged, and stored in crawl_dead_letters.
    """

    async def dispatch(
        self,
        ats_source_id: uuid.UUID,
        db: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Crawl the ATS source and return normalized job dicts.

        On failure returns an empty list.
        Always updates the AtsSource row in the DB.
        Writes a CrawlDeadLetter row on every failure.
        """
        ats_source: AtsSource | None = await db.get(AtsSource, ats_source_id)
        if ats_source is None:
            logger.error("dispatch: AtsSource %s not found", ats_source_id)
            return []

        # Look up company name for enriching job dicts
        company: Company | None = await db.get(Company, ats_source.company_id)
        company_name: str = company.name if company else "Unknown"

        crawler = CRAWLER_MAP.get(ats_source.ats_type)
        if crawler is None:
            logger.error(
                "dispatch: no crawler registered for ats_type=%s", ats_source.ats_type
            )
            await self._record_failure(
                db=db,
                ats_source=ats_source,
                error_type="unknown_ats_type",
                error_message=f"No crawler for ats_type={ats_source.ats_type!r}",
                http_status=None,
                raw_response=None,
            )
            return []

        ats_slug: str = ats_source.ats_slug or ""

        try:
            jobs = await crawler.crawl(ats_slug, ats_source.id)
        except RateLimitedException as exc:
            logger.warning(
                "dispatch: rate-limited slug=%s ats_type=%s",
                ats_slug, ats_source.ats_type,
            )
            await self._handle_rate_limit(db, ats_source, exc)
            return []
        except SlugNotFoundException as exc:
            logger.warning(
                "dispatch: slug not found slug=%s ats_type=%s",
                ats_slug, ats_source.ats_type,
            )
            await self._handle_slug_not_found(db, ats_source, exc)
            return []
        except Exception as exc:
            logger.exception(
                "dispatch: crawl error slug=%s ats_type=%s",
                ats_slug, ats_source.ats_type,
            )
            await self._handle_generic_error(db, ats_source, exc)
            return []

        # Success path — inject company context and update AtsSource
        now = datetime.now(timezone.utc)
        for job in jobs:
            job.setdefault("company", company_name)
            job["company_id"] = ats_source.company_id
            job.setdefault("country", ats_source.market)

        await db.execute(
            sa.update(AtsSource)
            .where(AtsSource.id == ats_source.id)
            .values(
                last_crawled_at=now,
                last_crawl_status="ok",
                last_crawl_job_count=len(jobs),
                consecutive_failures=0,
                backoff_until=None,
                updated_at=now,
            )
        )
        await db.commit()

        logger.info(
            "dispatch: success slug=%s ats_type=%s jobs=%d",
            ats_slug, ats_source.ats_type, len(jobs),
        )
        return jobs

    async def _handle_rate_limit(
        self,
        db: AsyncSession,
        ats_source: AtsSource,
        exc: RateLimitedException,
    ) -> None:
        now = datetime.now(timezone.utc)
        new_failures = ats_source.consecutive_failures + 1
        backoff_until = now + _RATE_LIMIT_BACKOFF

        await db.execute(
            sa.update(AtsSource)
            .where(AtsSource.id == ats_source.id)
            .values(
                consecutive_failures=new_failures,
                backoff_until=backoff_until,
                last_crawl_status="rate_limited",
                last_crawled_at=now,
                updated_at=now,
            )
        )
        await self._write_dead_letter(
            db=db,
            ats_source=ats_source,
            error_type="rate_limited",
            error_message=str(exc),
            http_status=exc.http_status,
            raw_response=None,
        )
        await db.commit()

    async def _handle_slug_not_found(
        self,
        db: AsyncSession,
        ats_source: AtsSource,
        exc: SlugNotFoundException,
    ) -> None:
        now = datetime.now(timezone.utc)
        await db.execute(
            sa.update(AtsSource)
            .where(AtsSource.id == ats_source.id)
            .values(
                is_active=False,
                last_crawl_status="slug_not_found",
                last_crawled_at=now,
                updated_at=now,
            )
        )
        await self._write_dead_letter(
            db=db,
            ats_source=ats_source,
            error_type="slug_not_found",
            error_message=str(exc),
            http_status=exc.http_status,
            raw_response=None,
        )
        await db.commit()

    async def _handle_generic_error(
        self,
        db: AsyncSession,
        ats_source: AtsSource,
        exc: Exception,
    ) -> None:
        now = datetime.now(timezone.utc)
        new_failures = ats_source.consecutive_failures + 1
        backoff_until = now + _backoff_for(new_failures)
        http_status = getattr(exc, "http_status", None)

        await db.execute(
            sa.update(AtsSource)
            .where(AtsSource.id == ats_source.id)
            .values(
                consecutive_failures=new_failures,
                backoff_until=backoff_until,
                last_crawl_status="error",
                last_crawled_at=now,
                updated_at=now,
            )
        )
        await self._write_dead_letter(
            db=db,
            ats_source=ats_source,
            error_type="crawl_error",
            error_message=str(exc),
            http_status=http_status,
            raw_response=None,
        )
        await db.commit()

    async def _record_failure(
        self,
        db: AsyncSession,
        ats_source: AtsSource,
        error_type: str,
        error_message: str,
        http_status: int | None,
        raw_response: str | None,
    ) -> None:
        """Convenience wrapper used for configuration-level failures."""
        await self._write_dead_letter(
            db=db,
            ats_source=ats_source,
            error_type=error_type,
            error_message=error_message,
            http_status=http_status,
            raw_response=raw_response,
        )
        await db.commit()

    @staticmethod
    async def _write_dead_letter(
        db: AsyncSession,
        ats_source: AtsSource,
        error_type: str,
        error_message: str,
        http_status: int | None,
        raw_response: str | None,
    ) -> None:
        dead_letter = CrawlDeadLetter(
            ats_source_id=ats_source.id,
            ats_type=ats_source.ats_type,
            ats_slug=ats_source.ats_slug,
            error_type=error_type,
            http_status=http_status,
            error_message=error_message[:4096] if error_message else None,
            raw_response=raw_response[:8192] if raw_response else None,
        )
        db.add(dead_letter)
