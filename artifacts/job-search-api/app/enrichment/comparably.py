"""Comparably enrichment source.

Scrapes culture grade, CEO approval %, and work-life balance score.
Uses plain GET with a browser-like User-Agent — no headless browser.
"""
import logging
import re

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_BASE = "https://www.comparably.com/companies"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class ComparablyResult:
    def __init__(self):
        self.culture_score: str | None = None
        self.ceo_approval_pct: int | None = None
        self.work_life_score: float | None = None
        self.sources: list[str] = []


async def enrich_from_comparably(slug: str) -> ComparablyResult:
    result = ComparablyResult()
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            resp = await client.get(f"{_BASE}/{slug}", headers=_HEADERS)

        if resp.status_code != 200:
            logger.debug("Comparably returned %d for slug=%s", resp.status_code, slug)
            return result

        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup.find_all(True):
            classes = " ".join(tag.get("class", []))
            text = tag.get_text(strip=True)

            if result.culture_score is None and re.search(
                r"\b(grade|rating|score)\b", classes, re.IGNORECASE
            ):
                m = re.search(r"\b([A-F][+-]?)\b", text)
                if m:
                    result.culture_score = m.group(1)

            if result.ceo_approval_pct is None:
                m = re.search(r"(\d{1,3})%\s*(?:approve|ceo approval)", text, re.IGNORECASE)
                if m:
                    val = int(m.group(1))
                    if 0 <= val <= 100:
                        result.ceo_approval_pct = val

            if result.work_life_score is None:
                m = re.search(r"work.life.*?(\d+\.?\d*)", text, re.IGNORECASE)
                if m:
                    try:
                        val = float(m.group(1))
                        if 0 <= val <= 5:
                            result.work_life_score = round(val, 1)
                    except ValueError:
                        pass

        if any([result.culture_score, result.ceo_approval_pct, result.work_life_score]):
            result.sources.append("comparably")

    except Exception:
        logger.exception("Comparably enrichment failed for slug=%s", slug)

    return result
