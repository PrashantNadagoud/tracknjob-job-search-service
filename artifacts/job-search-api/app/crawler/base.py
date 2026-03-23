import logging
from abc import ABC, abstractmethod
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class BaseCrawler(ABC):
    """Abstract base class for all company job crawlers.

    Subclasses must set `source_label` and `careers_url` as class attributes
    and implement `fetch_jobs()`.

    Use `_get_json()` / `_get_html()` for static pages (httpx).
    Use `_get_rendered()` for JavaScript-rendered pages (Playwright).
    """

    source_label: str
    careers_url: str
    country: str = "US"  # override to "IN" in India crawlers

    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; TrackNJobBot/1.0; "
            "+https://tracknJob.example.com/bot)"
        )
    }

    @abstractmethod
    async def fetch_jobs(self) -> list[dict[str, Any]]:
        """Return a list of job dicts.

        Each dict must contain:
            title, company, location, remote, source_url, source_label, posted_at
        """

    async def _get_json(self, url: str, **kwargs: Any) -> Any:
        """Fetch and parse JSON from a static/API endpoint using httpx."""
        async with httpx.AsyncClient(
            timeout=30, follow_redirects=True, headers=self._HEADERS
        ) as client:
            resp = await client.get(url, **kwargs)
            resp.raise_for_status()
            return resp.json()

    async def _get_html(self, url: str, **kwargs: Any) -> str:
        """Fetch raw HTML from a static page using httpx."""
        async with httpx.AsyncClient(
            timeout=30, follow_redirects=True, headers=self._HEADERS
        ) as client:
            resp = await client.get(url, **kwargs)
            resp.raise_for_status()
            return resp.text

    async def _get_rendered(self, url: str) -> str:
        """Fetch HTML from a JavaScript-rendered page using Playwright."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error(
                "playwright is not installed; cannot render JS pages. "
                "Run: playwright install chromium"
            )
            return ""

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.goto(url, wait_until="networkidle", timeout=30_000)
                return await page.content()
            finally:
                await browser.close()
