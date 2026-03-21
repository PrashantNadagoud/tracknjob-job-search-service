"""Notion Careers crawler — Ashby ATS (jobs.ashbyhq.com/notion)."""

import logging
from datetime import datetime, timezone
from typing import Any

from app.crawler.base import BaseCrawler

logger = logging.getLogger(__name__)

_ASHBY_API = "https://api.ashbyhq.com/posting-api/job-board/notion"


class NotionCrawler(BaseCrawler):
    source_label = "Notion Careers"
    careers_url = "https://www.notion.so/careers"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        data: dict[str, Any] = await self._get_json(_ASHBY_API)
        jobs: list[dict[str, Any]] = []
        for item in data.get("jobs", []):
            title: str = item.get("title", "") or ""
            source_url: str = item.get("jobUrl", "") or ""
            if not source_url or not title:
                continue

            location: str = item.get("location", "") or ""
            is_remote: bool = bool(item.get("isRemote")) or (
                "remote" in location.lower()
            )

            published_raw: str | None = item.get("publishedAt")
            posted_at: datetime | None = None
            if published_raw:
                try:
                    posted_at = datetime.fromisoformat(
                        published_raw.replace("Z", "+00:00")
                    )
                except ValueError:
                    posted_at = datetime.now(timezone.utc)

            jobs.append(
                {
                    "title": title,
                    "company": "Notion",
                    "location": location,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": self.source_label,
                    "posted_at": posted_at,
                }
            )
        return jobs
