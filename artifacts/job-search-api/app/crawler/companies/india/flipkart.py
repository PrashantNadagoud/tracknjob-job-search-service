"""Flipkart Careers crawler — JS-rendered via Playwright."""

import logging
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from app.crawler.base import BaseCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_CAREERS_URL = "https://www.flipkartcareers.com/#!/joblist"


class FlipkartCrawler(BaseCrawler):
    source_label = "Official"
    careers_url = _CAREERS_URL
    country = "IN"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        html = await self._get_rendered(_CAREERS_URL)
        if not html:
            logger.warning("FlipkartCrawler: no HTML returned (Playwright unavailable?)")
            return []

        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        # Flipkart careers is a React SPA with hash routing.
        # Job cards typically rendered as list items with title + location.
        cards = (
            soup.select("[class*='job-list-item']")
            or soup.select("[class*='jobCard']")
            or soup.select("[class*='job-card']")
            or soup.select("li[class*='job']")
        )

        seen: set[str] = set()
        for card in cards:
            link_el = card.find("a", href=True)
            if not link_el:
                continue
            href = link_el["href"]
            source_url = (
                f"https://www.flipkartcareers.com{href}"
                if href.startswith("/")
                else href
            )
            if source_url in seen:
                continue
            seen.add(source_url)

            title_el = card.find("h2") or card.find("h3") or card.find("h4") or link_el
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            loc_el = card.find(attrs={"class": lambda c: c and "location" in c.lower()})
            location = loc_el.get_text(strip=True) if loc_el else "Bangalore, India"

            dept_el = card.find(attrs={"class": lambda c: c and "department" in c.lower()}) or \
                      card.find(attrs={"class": lambda c: c and "team" in c.lower()})
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
                    "company": "Flipkart",
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

        logger.info("FlipkartCrawler: parsed %d jobs", len(jobs))
        return jobs
