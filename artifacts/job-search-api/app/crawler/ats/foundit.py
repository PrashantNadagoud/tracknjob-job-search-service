"""Foundit.in (formerly Monster India) ATS crawler — India market.

Uses Foundit's public middleware search API (paginated GET):
    GET https://www.foundit.in/middleware/jobsearch/v2/search
        ?query={slug}
        &limit=50
        &page={page}

All listings produced by this crawler force:
    geo_restriction = "IN"
    salary_currency = "INR"
    country = "IN"
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from app.crawler.ats_base import BaseATSCrawler

logger = logging.getLogger(__name__)

_API_URL = "https://www.foundit.in/middleware/jobsearch/v2/search"
_JOB_BASE = "https://www.foundit.in/job/{job_id}"
_PAGE_SIZE = 50


class FounditCrawler(BaseATSCrawler):
    ats_type = "foundit"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        page = 0

        while True:
            data = await self._get_json(
                _API_URL,
                params={"query": ats_slug, "limit": _PAGE_SIZE, "page": page},
            )

            result: dict = (data.get("data") or {}).get("jobSearchResult") or data.get("jobSearchResult") or {}
            postings: list[dict] = result if isinstance(result, list) else result.get("data") or []
            total: int = int(
                (data.get("data") or {}).get("totalCount")
                or data.get("totalCount")
                or 0
            )

            if not postings:
                break

            for p in postings:
                job_id: str = str(p.get("id") or p.get("jobId") or "")
                title: str = p.get("title") or p.get("jobTitle") or ""
                location_raw: str = p.get("location") or p.get("city") or ""

                posted_at: datetime | None = None
                ts_raw = p.get("postedDate") or p.get("freshness")
                if ts_raw and isinstance(ts_raw, (int, float)):
                    try:
                        posted_at = datetime.fromtimestamp(ts_raw / 1000, tz=timezone.utc)
                    except (ValueError, OSError):
                        posted_at = None

                source_url: str = p.get("applyUrl") or p.get("jobUrl") or _JOB_BASE.format(job_id=job_id)
                dept: str | None = p.get("functionalArea") or p.get("category")
                emp_type: str | None = p.get("employmentType") or p.get("jobType")

                jobs.append(
                    {
                        "title": title,
                        "location": location_raw,
                        "remote": False,
                        "source_url": source_url,
                        "source_label": "Foundit",
                        "posted_at": posted_at,
                        "geo_restriction": "IN",
                        "country": "IN",
                        "ats_type": self.ats_type,
                        "external_job_id": job_id,
                        "department": dept,
                        "employment_type": emp_type,
                        "ats_source_id": ats_source_id,
                        "salary_currency": "INR",
                    }
                )

            page += 1
            if page * _PAGE_SIZE >= total:
                break

        logger.info("foundit: slug=%s crawled %d jobs", ats_slug, len(jobs))
        return jobs
