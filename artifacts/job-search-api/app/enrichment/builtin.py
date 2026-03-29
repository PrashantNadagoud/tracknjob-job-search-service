"""BuiltIn enrichment source.

Scrapes remote/hybrid/on-site policy and perks list.
Uses plain GET with a browser-like User-Agent — no headless browser.
"""
import logging
import re

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_BASE = "https://builtin.com/company"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

_REMOTE_PATTERNS = [
    (re.compile(r"\bfully\s*remote\b", re.IGNORECASE), "Remote"),
    (re.compile(r"\bremote\b", re.IGNORECASE), "Remote"),
    (re.compile(r"\bhybrid\b", re.IGNORECASE), "Hybrid"),
    (re.compile(r"\bon.?site\b", re.IGNORECASE), "On-site"),
    (re.compile(r"\bin.?office\b", re.IGNORECASE), "On-site"),
]

_PERK_KEYWORDS = {
    "401k": re.compile(r"\b401[k]?\b", re.IGNORECASE),
    "equity": re.compile(r"\bequity\b|\bstock\s+options?\b", re.IGNORECASE),
    "health": re.compile(r"\bhealth\s+insurance\b|\bmedical\b", re.IGNORECASE),
    "parental leave": re.compile(r"\bparental\s+leave\b|\bmaternity\b|\bpaternity\b", re.IGNORECASE),
    "unlimited PTO": re.compile(r"\bunlimited\s+pto\b|\bunlimited\s+vacation\b", re.IGNORECASE),
}


class BuiltInResult:
    def __init__(self):
        self.remote_policy: str | None = None
        self.perks: list[str] = []
        self.sources: list[str] = []


async def enrich_from_builtin(slug: str) -> BuiltInResult:
    result = BuiltInResult()
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            resp = await client.get(f"{_BASE}/{slug}", headers=_HEADERS)

        if resp.status_code != 200:
            logger.debug("BuiltIn returned %d for slug=%s", resp.status_code, slug)
            return result

        soup = BeautifulSoup(resp.text, "html.parser")
        full_text = soup.get_text(separator=" ")

        for pattern, label in _REMOTE_PATTERNS:
            if pattern.search(full_text):
                result.remote_policy = label
                break

        found_perks: list[str] = []
        for perk_name, pattern in _PERK_KEYWORDS.items():
            if pattern.search(full_text):
                found_perks.append(perk_name)
        result.perks = found_perks

        if result.remote_policy or result.perks:
            result.sources.append("builtin")

    except Exception:
        logger.exception("BuiltIn enrichment failed for slug=%s", slug)

    return result
