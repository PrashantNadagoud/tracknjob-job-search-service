"""Vercel Careers crawler — Greenhouse ATS (boards-api.greenhouse.io/vercel)."""

import logging
from datetime import datetime, timezone
from typing import Any

from app.crawler.base import BaseCrawler
from app.crawler.geo_classifier import classify_listing, parse_greenhouse_location

logger = logging.getLogger(__name__)

_GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/vercel/jobs"


class VercelCrawler(BaseCrawler):
    source_label = "Vercel Careers"
    careers_url = "https://vercel.com/careers"
    country = "US"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        data: dict[str, Any] = await self._get_json(_GREENHOUSE_API)
        jobs: list[dict[str, Any]] = []
        for item in data.get("jobs", []):
            title: str = item.get("title", "") or ""
            source_url: str = item.get("absolute_url", "") or ""
            if not source_url or not title:
                continue

            location_raw, country_hint = parse_greenhouse_location(item)
            location: str = location_raw or ""

            updated_raw: str | None = item.get("updated_at")
            posted_at: datetime | None = None
            if updated_raw:
                try:
                    posted_at = datetime.fromisoformat(
                        updated_raw.replace("Z", "+00:00")
                    )
                except ValueError:
                    posted_at = datetime.now(timezone.utc)

            work_type = "remote" if "remote" in location.lower() else ""
            geo_restriction = classify_listing(
                location_raw=location,
                description="",
                work_type=work_type,
                country=country_hint,
            )

            jobs.append(
                {
                    "title": title,
                    "company": "Vercel",
                    "location": location,
                    "remote": "remote" in location.lower(),
                    "source_url": source_url,
                    "source_label": self.source_label,
                    "posted_at": posted_at,
                    "geo_restriction": geo_restriction,
                }
            )
        return jobs
