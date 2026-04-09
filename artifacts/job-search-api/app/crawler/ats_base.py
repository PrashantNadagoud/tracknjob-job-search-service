"""Base class for ATS (Applicant Tracking System) crawlers.

All ATS crawlers implement `crawl(ats_slug, ats_source_id)` rather than the
`fetch_jobs()` interface used by company-specific crawlers.
"""

import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any

import httpx

from app.crawler.exceptions import CrawlException, RateLimitedException, SlugNotFoundException

logger = logging.getLogger(__name__)

_BOT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; TrackNJobBot/1.0; "
        "+https://tracknJob.example.com/bot)"
    )
}


class BaseATSCrawler(ABC):
    """Abstract base for ATS crawlers.

    Subclasses must:
    - Set the ``ats_type`` class attribute (e.g. ``"workday"``)
    - Implement ``crawl(ats_slug, ats_source_id)``

    Each returned job dict must contain at minimum:
        title, location, remote, source_url, source_label,
        geo_restriction, ats_type, external_job_id
    """

    ats_type: str

    def _map_http_error(self, status_code: int, url: str) -> None:
        """Raise the appropriate exception for a non-2xx HTTP status."""
        if status_code == 404:
            raise SlugNotFoundException(
                f"404 Not Found for {url}", http_status=404
            )
        if status_code == 429:
            raise RateLimitedException(
                f"429 Rate Limited for {url}", http_status=429
            )
        raise CrawlException(
            f"HTTP {status_code} for {url}", http_status=status_code
        )

    async def _get_json(
        self,
        url: str,
        params: dict | None = None,
        extra_headers: dict | None = None,
    ) -> Any:
        """GET a URL and return parsed JSON; maps HTTP errors to exceptions."""
        headers = {**_BOT_HEADERS, **(extra_headers or {})}
        try:
            async with httpx.AsyncClient(
                timeout=30, follow_redirects=True, headers=headers
            ) as client:
                resp = await client.get(url, params=params)
                if not resp.is_success:
                    self._map_http_error(resp.status_code, url)
                return resp.json()
        except (RateLimitedException, SlugNotFoundException, CrawlException):
            raise
        except httpx.TimeoutException as exc:
            raise CrawlException(f"Timeout fetching {url}") from exc
        except Exception as exc:
            raise CrawlException(f"Unexpected error fetching {url}: {exc}") from exc

    async def _post_json(
        self,
        url: str,
        payload: dict,
        extra_headers: dict | None = None,
    ) -> Any:
        """POST JSON to a URL and return parsed JSON; maps HTTP errors to exceptions."""
        headers = {**_BOT_HEADERS, **(extra_headers or {})}
        try:
            async with httpx.AsyncClient(
                timeout=30, follow_redirects=True, headers=headers
            ) as client:
                resp = await client.post(url, json=payload)
                if not resp.is_success:
                    self._map_http_error(resp.status_code, url)
                return resp.json()
        except (RateLimitedException, SlugNotFoundException, CrawlException):
            raise
        except httpx.TimeoutException as exc:
            raise CrawlException(f"Timeout posting to {url}") from exc
        except Exception as exc:
            raise CrawlException(f"Unexpected error posting to {url}: {exc}") from exc

    @abstractmethod
    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        """Crawl the ATS for the given slug and return normalized job dicts.

        Args:
            ats_slug:      Company identifier in the ATS (e.g. Workday company slug).
            ats_source_id: UUID of the AtsSource row driving this crawl.

        Returns:
            List of job dicts.  Each dict contains listing fields the ATS
            provides plus ``ats_type`` and ``external_job_id`` at minimum.
            ``company`` and ``company_id`` are NOT set here — the dispatcher
            injects those before persisting.
        """
