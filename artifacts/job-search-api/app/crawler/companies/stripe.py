"""Stripe Careers crawler.

Stripe uses a JavaScript-rendered career page. We first attempt their
/jobs.json endpoint (httpx); if that fails we fall back to Playwright and
parse anchor tags that link to individual job pages.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from app.crawler.base import BaseCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_JSON_URL = "https://stripe.com/jobs.json"
_BASE_URL = "https://stripe.com"


class StripeCrawler(BaseCrawler):
    source_label = "Official"
    careers_url = "https://stripe.com/jobs"
    country = "US"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        try:
            data = await self._get_json(_JSON_URL)
            jobs = self._parse_json(data)
            if jobs:
                return jobs
        except Exception:
            logger.warning(
                "Stripe JSON endpoint unavailable; falling back to Playwright"
            )

        return await self._fetch_rendered_jobs()

    def _parse_json(self, data: Any) -> list[dict[str, Any]]:
        items = data if isinstance(data, list) else data.get("jobs", [])
        jobs: list[dict[str, Any]] = []
        for item in items:
            loc = item.get("location", {})
            location = loc.get("name", "") if isinstance(loc, dict) else str(loc or "")
            source_url = item.get("absolute_url") or item.get("url") or ""
            title = item.get("title") or item.get("name") or ""
            if not source_url or not title:
                continue

            posted_raw = item.get("published_at") or item.get("created_at")
            posted_at: datetime | None = None
            if posted_raw:
                try:
                    posted_at = datetime.fromisoformat(
                        str(posted_raw).replace("Z", "+00:00")
                    )
                except ValueError:
                    posted_at = datetime.now(timezone.utc)

            is_remote = "remote" in location.lower()
            work_type = "remote" if is_remote else ""
            geo_restriction = classify_listing(
                location_raw=location,
                description="",
                work_type=work_type,
                country=None,
            )

            jobs.append(
                {
                    "title": title,
                    "company": "Stripe",
                    "location": location,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": self.source_label,
                    "posted_at": posted_at,
                    "geo_restriction": geo_restriction,
                }
            )
        return jobs

    async def _fetch_rendered_jobs(self) -> list[dict[str, Any]]:
        try:
            html = await self._get_rendered(self.careers_url)
            if not html:
                return []
            soup = BeautifulSoup(html, "html.parser")
            jobs: list[dict[str, Any]] = []
            seen: set[str] = set()
            for a in soup.find_all("a", href=True):
                href: str = a["href"]
                if "/jobs/" not in href:
                    continue
                url = href if href.startswith("http") else f"{_BASE_URL}{href}"
                if url in seen:
                    continue
                seen.add(url)
                title = a.get_text(strip=True)
                if not title:
                    continue
                geo_restriction = classify_listing(
                    location_raw="",
                    description="",
                    work_type="",
                    country=None,
                )
                jobs.append(
                    {
                        "title": title,
                        "company": "Stripe",
                        "location": "",
                        "remote": False,
                        "source_url": url,
                        "source_label": self.source_label,
                        "posted_at": None,
                        "geo_restriction": geo_restriction,
                    }
                )
            return jobs
        except Exception:
            logger.exception("Stripe rendered fallback failed")
            return []
