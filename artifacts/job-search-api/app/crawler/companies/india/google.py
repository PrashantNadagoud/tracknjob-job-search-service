"""Google India Careers crawler — JS-rendered via Playwright."""

import logging
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from app.crawler.base import BaseCrawler

logger = logging.getLogger(__name__)

_CAREERS_URL = "https://careers.google.com/jobs/results/?location=India"


class GoogleIndiaCrawler(BaseCrawler):
    source_label = "Google India Careers"
    careers_url = _CAREERS_URL
    country = "IN"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        html = await self._get_rendered(_CAREERS_URL)
        if not html:
            logger.warning("GoogleIndiaCrawler: no HTML returned (Playwright unavailable?)")
            return []

        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        # Google careers renders job cards as <li> elements inside a job list
        # Try multiple selectors in order of specificity
        cards = (
            soup.select("li[class*='lLd3Je']")
            or soup.select("li[class*='job']")
            or soup.select("[role='listitem']")
        )

        for card in cards:
            link_el = card.find("a", href=True)
            if not link_el:
                continue
            href = link_el["href"]
            source_url = (
                f"https://careers.google.com{href}"
                if href.startswith("/")
                else href
            )
            title_el = (
                card.find("h2")
                or card.find("h3")
                or link_el
            )
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            loc_el = card.find(attrs={"class": lambda c: c and "location" in c.lower()})
            location = loc_el.get_text(strip=True) if loc_el else "India"

            jobs.append(
                {
                    "title": title,
                    "company": "Google",
                    "location": location,
                    "remote": "remote" in location.lower(),
                    "source_url": source_url,
                    "source_label": self.source_label,
                    "posted_at": now,
                    "country": self.country,
                }
            )

        logger.info("GoogleIndiaCrawler: parsed %d jobs", len(jobs))
        return jobs
