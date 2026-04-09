"""Workday ATS crawler.

Uses Workday's public CXS jobs API (paginated POST).

Constructed URL:
    https://{ats_slug}.wd1.myworkdayjobs.com/wday/cxs/{ats_slug}/External/jobs

Companies that use a different Workday subdomain (wd3, wd5) or a custom
career-site name should set ``crawl_url`` on the AtsSource row; the
dispatcher will override ``ats_slug`` with that URL for routing purposes.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_PAGE_SIZE = 20
_DEFAULT_WD_BASE = "https://{slug}.wd1.myworkdayjobs.com/wday/cxs/{slug}/External/jobs"
_JOB_LINK_BASE = "https://{slug}.wd1.myworkdayjobs.com/en-US/External"


def _resolve_workday_urls(ats_slug: str) -> tuple[str, str]:
    """Return (api_url, link_base) for a Workday slug or full URL override.

    When the dispatcher sets ``crawl_url`` on an AtsSource, it is passed as
    ``ats_slug``.  A full URL (starts with ``http``) is used directly as the
    API endpoint; everything else is treated as a plain slug and routed to the
    default ``wd1`` tenant URL.

    Returns:
        api_url:   POST endpoint for the Workday jobs API.
        link_base: Base for constructing individual job page URLs.
    """
    if ats_slug.startswith("http"):
        # Full crawl_url provided — use as-is; derive link_base from scheme+host
        from urllib.parse import urlparse
        parsed = urlparse(ats_slug)
        host_base = f"{parsed.scheme}://{parsed.netloc}"
        # Best-effort link base: replace /wday/cxs/.../jobs with /en-US
        link_base = host_base + "/en-US/External"
        return ats_slug, link_base
    else:
        api_url = _DEFAULT_WD_BASE.format(slug=ats_slug)
        link_base = _JOB_LINK_BASE.format(slug=ats_slug)
        return api_url, link_base


class WorkdayCrawler(BaseATSCrawler):
    ats_type = "workday"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        offset = 0
        api_url, link_base = _resolve_workday_urls(ats_slug)
        # Use the last segment of the host for display (e.g. "amazon" from amazon.wd3...)
        display_slug = ats_slug.split(".")[0].lstrip("https://") if ats_slug.startswith("http") else ats_slug

        while True:
            data = await self._post_json(
                api_url,
                {
                    "appliedFacets": {},
                    "limit": _PAGE_SIZE,
                    "offset": offset,
                    "searchText": "",
                },
            )
            postings: list[dict] = data.get("jobPostings") or []
            if not postings:
                break

            for p in postings:
                external_path: str = p.get("externalPath") or ""
                # job id is the last path segment of externalPath
                job_id = external_path.rstrip("/").rsplit("/", 1)[-1] if external_path else ""
                if not job_id:
                    job_id = str(p.get("id") or "")

                location: str = p.get("locationsText") or ""
                is_remote = "remote" in location.lower()
                work_type = "remote" if is_remote else "onsite"
                geo_restriction = classify_listing(
                    location_raw=location,
                    description="",
                    work_type=work_type,
                    country=None,
                )

                posted_at: datetime | None = None
                posted_raw = p.get("postedOn") or p.get("postedDate")
                if posted_raw:
                    try:
                        posted_at = datetime.fromisoformat(
                            str(posted_raw).replace("Z", "+00:00")
                        )
                    except ValueError:
                        posted_at = datetime.now(timezone.utc)

                source_url = (
                    f"{link_base}{external_path}" if external_path else api_url
                )

                # department comes from bulletFields (array of strings) when present
                bullet_fields: list[str] = p.get("bulletFields") or []
                department = bullet_fields[0] if bullet_fields else None

                jobs.append(
                    {
                        "title": p.get("title") or "",
                        "location": location,
                        "remote": is_remote,
                        "source_url": source_url,
                        "source_label": f"{display_slug} Careers (Workday)",
                        "posted_at": posted_at,
                        "geo_restriction": geo_restriction,
                        "ats_type": self.ats_type,
                        "external_job_id": job_id,
                        "department": department,
                        "ats_source_id": ats_source_id,
                        "salary_currency": "USD",
                    }
                )

            total: int = data.get("total", 0) or len(postings)
            offset += len(postings)
            if offset >= total:
                break

        logger.info(
            "workday: slug=%s crawled %d jobs", ats_slug, len(jobs)
        )
        return jobs
