"""YCScraper — fetch company listings from the Y Combinator directory.

Primary path: GET https://www.ycombinator.com/companies.json
Fallback:     scrape HTML with BeautifulSoup (data-name / data-website / data-slug attrs)

Returns a list of dicts:
    {name: str, website: str | None, yc_slug: str | None}

Filtered out:
    - Companies without a website
    - Companies with batch == "Inactive" or "Acquired" status keywords
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_YC_API_URL = "https://api.ycombinator.com/v0.1/companies"
_YC_COMPANIES_URL = "https://www.ycombinator.com/companies"

_EXCLUDED_STATUSES = {"inactive", "acquired", "dead", "exited"}

_CLIENT_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)
_BOT_UA = "Mozilla/5.0 (compatible; TrackNJob-Bot/1.0; +https://tracknjob.com/bot)"
_TIMEOUT = 8.0


def _is_excluded(company: dict[str, Any]) -> bool:
    """Return True if this company should be filtered out."""
    status = (company.get("status") or "").lower()
    if status in _EXCLUDED_STATUSES:
        return True
    batch = (company.get("batch") or "").lower()
    if "inactive" in batch or "acquired" in batch:
        return True
    return False


def _normalize_website(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return raw.rstrip("/")


class YCScraper:
    """Fetches and normalises the YC company directory."""

    def __init__(self) -> None:
        self._headers = {
            "User-Agent": _BOT_UA,
            "Accept": "application/json, text/html",
        }

    async def fetch(self, max_pages: int = 100) -> list[dict[str, Any]]:
        """Return a list of company dicts from the YC directory.

        Each dict has keys: name, website, yc_slug.
        """
        companies = await self._fetch_from_api(max_pages=max_pages)
        if not companies:
            logger.warning("YC API returned no companies; falling back to HTML scrape")
            companies = await self._scrape_html()

        result: list[dict[str, Any]] = []
        for c in companies:
            if _is_excluded(c):
                continue
            website = _normalize_website(c.get("website") or c.get("url"))
            if not website:
                continue
            result.append(
                {
                    "name": (c.get("name") or "").strip(),
                    "website": website,
                    "yc_slug": c.get("slug") or c.get("yc_slug"),
                }
            )

        logger.info("YCScraper: %d companies after filtering", len(result))
        return result

    async def _fetch_from_api(self, max_pages: int = 10) -> list[dict[str, Any]]:
        """Fetch companies from the YC public API with pagination."""
        all_companies: list[dict[str, Any]] = []
        
        try:
            async with httpx.AsyncClient(
                timeout=_TIMEOUT,
                follow_redirects=True,
                limits=_CLIENT_LIMITS,
                headers=self._headers,
            ) as client:
                for page in range(1, max_pages + 1):
                    logger.info("Fetching YC API page %d...", page)
                    resp = await client.get(
                        _YC_API_URL, 
                        params={"page": page, "per_page": 100}
                    )
                    
                    if resp.status_code != 200:
                        logger.debug("YC API returned %s on page %d", resp.status_code, page)
                        break
                        
                    data = resp.json()
                    page_companies = []
                    
                    if isinstance(data, list):
                        page_companies = data
                    elif isinstance(data, dict):
                        # Handle different possible JSON shapes
                        page_companies = data.get("companies") or data.get("results") or data.get("data") or []
                    
                    if not page_companies:
                        logger.debug("No more companies found on page %d", page)
                        break
                        
                    logger.info("Page %d returned %d companies", page, len(page_companies))
                    if page_companies:
                        logger.info("Sample company keys: %s", list(page_companies[0].keys()))
                    all_companies.extend(page_companies)
                    
                    # Small delay between pages
                    await asyncio.sleep(0.5)
                    
            return all_companies
        except Exception as exc:
            logger.error("YC API fetch failed: %s", exc, exc_info=True)
            return []

    async def _scrape_html(self) -> list[dict[str, Any]]:
        """Scrape the YC companies HTML page for data attributes."""
        try:
            async with httpx.AsyncClient(
                timeout=_TIMEOUT,
                follow_redirects=False,
                limits=_CLIENT_LIMITS,
                headers={**self._headers, "Accept": "text/html"},
            ) as client:
                resp = await client.get(_YC_COMPANIES_URL)
                resp.raise_for_status()
                html = resp.text
        except Exception as exc:
            logger.error("YC HTML scrape failed: %s", exc)
            return []

        soup = BeautifulSoup(html, "html.parser")
        companies: list[dict[str, Any]] = []

        for tag in soup.find_all(attrs={"data-name": True}):
            name = tag.get("data-name", "").strip()
            website = tag.get("data-website") or tag.get("data-url")
            slug = tag.get("data-slug") or tag.get("data-yc-slug")
            batch = (tag.get("data-batch") or "").lower()
            status = (tag.get("data-status") or "").lower()

            if not name:
                continue
            if status in _EXCLUDED_STATUSES:
                continue
            if "inactive" in batch or "acquired" in batch:
                continue

            companies.append(
                {
                    "name": name,
                    "website": website,
                    "slug": slug,
                }
            )

        logger.info("HTML scrape found %d company tags", len(companies))
        return companies
