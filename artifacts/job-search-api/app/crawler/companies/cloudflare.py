"""Cloudflare Careers crawler — Greenhouse ATS."""

import logging
from datetime import datetime, timezone
from typing import Any

from app.crawler.base import BaseCrawler

logger = logging.getLogger(__name__)

_GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/cloudflare/jobs"


class CloudflareCrawler(BaseCrawler):
    source_label = "Cloudflare Careers"
    careers_url = "https://www.cloudflare.com/careers/jobs/"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        data: dict[str, Any] = await self._get_json(_GREENHOUSE_API)
        jobs: list[dict[str, Any]] = []
        for item in data.get("jobs", []):
            title: str = item.get("title", "") or ""
            source_url: str = item.get("absolute_url", "") or ""
            if not source_url or not title:
                continue

            loc_obj = item.get("location", {})
            location: str = (
                loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj or "")
            )

            updated_raw: str | None = item.get("updated_at")
            posted_at: datetime | None = None
            if updated_raw:
                try:
                    posted_at = datetime.fromisoformat(
                        updated_raw.replace("Z", "+00:00")
                    )
                except ValueError:
                    posted_at = datetime.now(timezone.utc)

            jobs.append(
                {
                    "title": title,
                    "company": "Cloudflare",
                    "location": location,
                    "remote": "remote" in location.lower(),
                    "source_url": source_url,
                    "source_label": self.source_label,
                    "posted_at": posted_at,
                }
            )
        return jobs
