"""Razorpay Careers crawler — JS-rendered via Playwright."""

import logging
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from app.crawler.base import BaseCrawler

logger = logging.getLogger(__name__)

_CAREERS_URL = "https://razorpay.com/jobs/"


class RazorpayCrawler(BaseCrawler):
    source_label = "Razorpay Careers"
    careers_url = _CAREERS_URL
    country = "IN"

    async def fetch_jobs(self) -> list[dict[str, Any]]:
        html = await self._get_rendered(_CAREERS_URL)
        if not html:
            logger.warning("RazorpayCrawler: no HTML returned (Playwright unavailable?)")
            return []

        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        # Razorpay uses a Next.js app. Job cards are typically rendered as
        # anchor elements or list items linking to /jobs/<slug>
        cards = (
            soup.select("a[href*='/jobs/']")
            or soup.select("[class*='job-card']")
            or soup.select("[class*='JobCard']")
            or soup.select("li[class*='job']")
        )

        seen: set[str] = set()
        for card in cards:
            if card.name == "a":
                link_el = card
            else:
                link_el = card.find("a", href=lambda h: h and "/jobs/" in h)
            if not link_el:
                continue

            href = link_el.get("href", "")
            # Skip the main /jobs/ listing page itself
            if href.rstrip("/") in ("/jobs", "https://razorpay.com/jobs"):
                continue
            source_url = (
                f"https://razorpay.com{href}"
                if href.startswith("/")
                else href
            )
            if source_url in seen:
                continue
            seen.add(source_url)

            title_el = (
                link_el.find("h2")
                or link_el.find("h3")
                or link_el.find("h4")
                or (card.find("h2") or card.find("h3") if card.name != "a" else None)
            )
            title = title_el.get_text(strip=True) if title_el else link_el.get_text(strip=True)[:80]
            if not title:
                continue

            loc_el = (
                card.find(attrs={"class": lambda c: c and "location" in c.lower()})
                if card.name != "a"
                else None
            )
            location = loc_el.get_text(strip=True) if loc_el else "Bangalore, India"

            jobs.append(
                {
                    "title": title,
                    "company": "Razorpay",
                    "location": location,
                    "remote": "remote" in location.lower(),
                    "source_url": source_url,
                    "source_label": self.source_label,
                    "posted_at": now,
                    "country": self.country,
                }
            )

        logger.info("RazorpayCrawler: parsed %d jobs", len(jobs))
        return jobs
