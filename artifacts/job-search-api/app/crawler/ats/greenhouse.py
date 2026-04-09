"""Greenhouse ATS crawler.

Public API (single GET, no pagination cursor — returns all jobs at once):
    GET https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true

Response schema:
    {
        "jobs": [
            {
                "id": 12345,
                "title": "...",
                "location": {"name": "..."},
                "departments": [{"name": "..."}],
                "updated_at": "2024-01-15T10:00:00.000Z",
                "absolute_url": "https://boards.greenhouse.io/..."
            }
        ],
        "meta": {"total": N}
    }

404 on unknown slug → SlugNotFoundException.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_API_BASE = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"


class GreenhouseCrawler(BaseATSCrawler):
    ats_type = "greenhouse"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        api_url = _API_BASE.format(slug=ats_slug)

        data = await self._get_json(api_url, params={"content": "true"})

        raw_jobs: list[dict] = data.get("jobs") or []
        jobs: list[dict[str, Any]] = []

        for p in raw_jobs:
            job_id: str = str(p.get("id") or "")
            title: str = p.get("title") or ""
            location_raw: str = (p.get("location") or {}).get("name") or ""
            source_url: str = p.get("absolute_url") or (
                f"https://boards.greenhouse.io/{ats_slug}/jobs/{job_id}"
            )

            departments: list[dict] = p.get("departments") or []
            dept: str | None = departments[0].get("name") if departments else None

            posted_at: datetime | None = None
            updated_raw = p.get("updated_at")
            if updated_raw:
                try:
                    posted_at = datetime.fromisoformat(
                        str(updated_raw).replace("Z", "+00:00")
                    )
                except ValueError:
                    posted_at = datetime.now(timezone.utc)

            is_remote = "remote" in location_raw.lower()
            work_type = "remote" if is_remote else "onsite"
            geo_restriction = classify_listing(
                location_raw=location_raw,
                description="",
                work_type=work_type,
                country=None,
            )

            jobs.append(
                {
                    "title": title,
                    "location": location_raw,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": f"{ats_slug} Careers (Greenhouse)",
                    "posted_at": posted_at,
                    "geo_restriction": geo_restriction,
                    "ats_type": self.ats_type,
                    "external_job_id": job_id,
                    "department": dept,
                    "employment_type": None,
                    "ats_source_id": ats_source_id,
                    "salary_currency": "USD",
                }
            )

        logger.info("greenhouse: slug=%s crawled %d jobs", ats_slug, len(jobs))
        return jobs
