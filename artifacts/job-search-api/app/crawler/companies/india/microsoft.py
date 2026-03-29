"""Microsoft India Careers crawler — JS-rendered via Playwright."""

import logging
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from app.crawler.base import BaseCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_CAREERS_URL = "https://jobs.microsoft.com/us/en/search?location=India"


class MicrosoftIndiaCrawler(BaseCrawler):
    source_label = "Microsoft India Careers"
    careers_url = _CAREERS_URL
    country = "IN"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        html = await self._get_rendered(_CAREERS_URL)
        if not html:
            logger.warning("MicrosoftIndiaCrawler: no HTML returned (Playwright unavailable?)")
            return []

        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        # Microsoft careers renders job cards with data attributes
        cards = (
            soup.select("[data-automation-id='jobTitle']")
            or soup.select(".ms-DocumentCard")
            or soup.select("[class*='job-card']")
            or soup.select("a[href*='/us/en/job/']")
        )

        seen: set[str] = set()
        for card in cards:
            # If we selected link elements directly, use them; otherwise find child link
            if card.name == "a":
                link_el = card
            else:
                link_el = card.find("a", href=lambda h: h and "/job/" in h)
            if not link_el:
                continue

            href = link_el.get("href", "")
            source_url = (
                f"https://jobs.microsoft.com{href}"
                if href.startswith("/")
                else href
            )
            if source_url in seen:
                continue
            seen.add(source_url)

            title = link_el.get_text(strip=True) or card.get_text(strip=True)[:80]
            if not title:
                continue

            loc_el = card.find_next(attrs={"class": lambda c: c and "location" in c.lower()})
            location = loc_el.get_text(strip=True) if loc_el else "India"

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
                    "company": "Microsoft",
                    "location": location,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": self.source_label,
                    "posted_at": now,
                    "country": self.country,
                    "geo_restriction": geo_restriction,
                }
            )

        logger.info("MicrosoftIndiaCrawler: parsed %d jobs", len(jobs))
        return jobs
