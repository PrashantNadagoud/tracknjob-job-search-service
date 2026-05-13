"""Amazon India Careers crawler.

Primary: amazon.jobs JSON search API.
Fallback: Playwright-rendered page.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from app.crawler.base import BaseCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_JSON_API = "https://www.amazon.jobs/en/search.json"
_CAREERS_URL = "https://www.amazon.jobs/en/search?country=IN"


class AmazonIndiaCrawler(BaseCrawler):
    source_label = "Official"
    careers_url = _CAREERS_URL
    country = "IN"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        # Try JSON API first
        try:
            jobs = await self._fetch_via_json()
            if jobs:
                return jobs
        except Exception:
            logger.warning("AmazonIndiaCrawler: JSON API failed, trying Playwright")

        return await self._fetch_via_playwright()

    async def _fetch_via_json(self) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        offset = 0
        page_size = 100

        while True:
            data: dict[str, Any] = await self._get_json(
                _JSON_API,
                params={
                    "country": "IN",
                    "radius": "24km",
                    "facets[]": "country",
                    "offset": offset,
                    "result_limit": page_size,
                },
            )
            batch = data.get("jobs", [])
            if not batch:
                break
            for item in batch:
                title = item.get("title", "")
                job_path = item.get("job_path", "")
                source_url = (
                    f"https://www.amazon.jobs{job_path}"
                    if job_path.startswith("/")
                    else job_path
                )
                if not title or not source_url:
                    continue
                location = item.get("location", "India")
                posted_raw = item.get("posted_date") or item.get("updated_time")
                posted_at: datetime | None = None
                if posted_raw:
                    try:
                        posted_at = datetime.fromisoformat(
                            str(posted_raw).replace("Z", "+00:00")
                        )
                    except ValueError:
                        posted_at = datetime.now(timezone.utc)

                is_remote = "remote" in location.lower()
                geo_restriction = classify_listing(
                    location_raw=location,
                    description="",
                    work_type="remote" if is_remote else "",
                    country=self.country,
                )
                dept = item.get("business_category") or item.get("job_category") or item.get("category")
                jobs.append(
                    {
                        "title": title,
                        "company": "Amazon",
                        "location": location,
                        "remote": is_remote,
                        "source_url": source_url,
                        "source_label": self.source_label,
                        "posted_at": posted_at,
                        "country": self.country,
                        "geo_restriction": geo_restriction,
                        "department": dept,
                    }
                )
            if len(batch) < page_size:
                break
            offset += page_size

        logger.info("AmazonIndiaCrawler (JSON): fetched %d jobs", len(jobs))
        return jobs

    async def _fetch_via_playwright(self) -> list[dict[str, Any]]:
        html = await self._get_rendered(_CAREERS_URL)
        if not html:
            logger.warning("AmazonIndiaCrawler: no HTML from Playwright")
            return []

        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        cards = (
            soup.select(".job-tile")
            or soup.select("[class*='job-card']")
            or soup.select("li[data-job-id]")
        )
        for card in cards:
            link_el = card.find("a", href=True)
            if not link_el:
                continue
            href = link_el["href"]
            source_url = (
                f"https://www.amazon.jobs{href}"
                if href.startswith("/")
                else href
            )
            title_el = card.find(attrs={"class": lambda c: c and "title" in c.lower()})
            title = title_el.get_text(strip=True) if title_el else link_el.get_text(strip=True)
            if not title:
                continue
            loc_el = card.find(attrs={"class": lambda c: c and "location" in c.lower()})
            location = loc_el.get_text(strip=True) if loc_el else "India"

            dept_el = card.find(attrs={"class": lambda c: c and "department" in c.lower()}) or \
                      card.find(attrs={"class": lambda c: c and "team" in c.lower()}) or \
                      card.find(attrs={"class": lambda c: c and "category" in c.lower()})
            dept = dept_el.get_text(strip=True) if dept_el else None

            is_remote = "remote" in location.lower()
            geo_restriction = classify_listing(
                location_raw=location,
                description="",
                work_type="remote" if is_remote else "",
                country=self.country,
            )
            jobs.append(
                {
                    "title": title,
                    "company": "Amazon",
                    "location": location,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": self.source_label,
                    "posted_at": now,
                    "country": self.country,
                    "geo_restriction": geo_restriction,
                    "department": dept,
                }
            )
        logger.info("AmazonIndiaCrawler (Playwright): parsed %d jobs", len(jobs))
        return jobs
