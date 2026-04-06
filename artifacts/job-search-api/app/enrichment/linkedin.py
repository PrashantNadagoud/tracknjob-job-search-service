"""LinkedIn public company page scraper.

Fetches the public /about/ page for a company (no auth required).
Extracts: company size, founded year.

Rate-limited to ≥ 2 seconds before each request to avoid soft-blocking.
429 responses are skipped (no retry).
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_LI_BASE = "https://www.linkedin.com/company"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


@dataclass
class LinkedInResult:
    num_employees_range: str | None = None
    founded_year: int | None = None
    sources: list[str] = field(default_factory=list)


async def enrich_from_linkedin(slug: str) -> LinkedInResult:
    """Scrape the LinkedIn public company /about/ page.

    Applies a 2.0 s rate-limit sleep before the HTTP call.
    Skips gracefully on 404 (DEBUG) or 429 (WARNING).
    """
    result = LinkedInResult()
    await asyncio.sleep(2.0)

    url = f"{_LI_BASE}/{slug}/about/"
    try:
        async with httpx.AsyncClient(
            timeout=12, follow_redirects=True, headers=_HEADERS
        ) as client:
            resp = await client.get(url)

        if resp.status_code == 404:
            logger.debug("LinkedIn 404 for slug=%s", slug)
            return result

        if resp.status_code == 429:
            logger.warning("LinkedIn 429 (rate-limited) for slug=%s — skipping", slug)
            return result

        if resp.status_code != 200:
            logger.debug("LinkedIn returned %d for slug=%s", resp.status_code, slug)
            return result

        soup = BeautifulSoup(resp.text, "html.parser")
        full_text = soup.get_text(separator=" ")

        # Company size: "1,001-5,000 employees" or "201-500 employees"
        if result.num_employees_range is None:
            m = re.search(
                r"([\d,]+)\s*[–-]\s*([\d,]+)\s+employees",
                full_text,
                re.IGNORECASE,
            )
            if m:
                lo = m.group(1).replace(",", "")
                hi = m.group(2).replace(",", "")
                result.num_employees_range = f"{lo}-{hi}"

        # Founded year: "Founded 2009" or "Founded in 2009"
        if result.founded_year is None:
            m = re.search(r"[Ff]ounded\s+(?:in\s+)?(\d{4})", full_text)
            if m:
                try:
                    result.founded_year = int(m.group(1))
                except ValueError:
                    pass

        if result.num_employees_range or result.founded_year:
            result.sources.append("linkedin")

    except Exception:
        logger.exception("LinkedIn enrichment failed for slug=%s", slug)

    return result
