"""Lever ATS crawler.

Public API (paginated GET via skip/limit):
    GET https://api.lever.co/v0/postings/{slug}?mode=json&limit=100&skip={skip}

Response schema (array at top level):
    [
        {
            "id": "uuid-...",
            "text": "Job Title",
            "categories": {
                "location": "San Francisco, CA",
                "department": "Engineering",
                "team": "Backend"
            },
            "hostedUrl": "https://jobs.lever.co/company/uuid",
            "createdAt": 1704067200000
        }
    ]

An empty array signals end of pagination.
404 on unknown slug → SlugNotFoundException.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_API_BASE = "https://api.lever.co/v0/postings/{slug}"
_PAGE_SIZE = 100


class LeverCrawler(BaseATSCrawler):
    ats_type = "lever"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        api_url = _API_BASE.format(slug=ats_slug)
        jobs: list[dict[str, Any]] = []
        skip = 0

        while True:
            postings: list[dict] = await self._get_json(
                api_url,
                params={"mode": "json", "limit": _PAGE_SIZE, "skip": skip},
            )

            if not postings:
                break

            for p in postings:
                job_id: str = str(p.get("id") or "")
                title: str = p.get("text") or ""
                categories: dict = p.get("categories") or {}
                location_raw: str = categories.get("location") or ""
                dept: str | None = categories.get("department") or categories.get("team")
                source_url: str = p.get("hostedUrl") or (
                    f"https://jobs.lever.co/{ats_slug}/{job_id}"
                )

                posted_at: datetime | None = None
                created_raw = p.get("createdAt")
                if created_raw and isinstance(created_raw, (int, float)):
                    try:
                        posted_at = datetime.fromtimestamp(
                            created_raw / 1000, tz=timezone.utc
                        )
                    except (ValueError, OSError):
                        posted_at = None

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
                        "source_label": f"{ats_slug} Careers (Lever)",
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

            skip += len(postings)
            if len(postings) < _PAGE_SIZE:
                break

        logger.info("lever: slug=%s crawled %d jobs", ats_slug, len(jobs))
        return jobs
