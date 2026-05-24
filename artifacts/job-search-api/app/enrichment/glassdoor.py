"""Glassdoor salary enrichment source.

Fetches salary data from Glassdoor salary pages by parsing JSON-LD.
If no JSON-LD block is found, salary fields are left null.
"""
import json
import logging

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_BASE = "https://www.glassdoor.com/Salaries"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class GlassdoorResult:
    def __init__(self):
        self.salary_min_usd: int | None = None
        self.salary_max_usd: int | None = None
        self.salary_source: str | None = None
        self.sources: list[str] = []


async def enrich_salary_from_glassdoor(role: str, location: str) -> GlassdoorResult:
    result = GlassdoorResult()
    try:
        safe_role = role.replace(" ", "-")
        n = len(role)
        url = f"{_BASE}/{safe_role}-Salaries-SRCH_KO0,{n}.htm"

        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            resp = await client.get(url, headers=_HEADERS)

        if resp.status_code != 200:
            logger.debug("Glassdoor returned %d for role=%s", resp.status_code, role)
            return result

        soup = BeautifulSoup(resp.text, "html.parser")
        scripts = soup.find_all("script", type="application/ld+json")

        for script in scripts:
            try:
                data = json.loads(script.string or "")
                if not isinstance(data, dict):
                    continue

                min_val = data.get("minValue")
                max_val = data.get("maxValue")

                if min_val is not None and max_val is not None:
                    result.salary_min_usd = int(float(min_val))
                    result.salary_max_usd = int(float(max_val))
                    result.salary_source = "glassdoor"
                    result.sources.append("glassdoor")
                    break
            except (json.JSONDecodeError, ValueError, TypeError):
                continue

    except Exception:
        logger.exception("Glassdoor enrichment failed for role=%s", role)

    return result
