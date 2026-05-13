"""BambooHR ATS crawler.

Public API (single GET, no pagination):
    GET https://{slug}.bamboohr.com/careers/list

Response schema:
    {
        "result": [
            {
                "id": "...",
                "title": "...",
                "location": {"city": "...", "state": "..."},
                "department": {"label": "..."},
                "employmentStatusLabel": "Full-Time"
            },
            ...
        ]
    }
"""

import logging
import uuid
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_API_URL = "https://{slug}.bamboohr.com/careers/list"
_JOB_URL = "https://{slug}.bamboohr.com/careers/{job_id}"


class BambooHRCrawler(BaseATSCrawler):
    ats_type = "bamboohr"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        api_url = _API_URL.format(slug=ats_slug)
        data = await self._get_json(api_url)

        # BambooHR returns {"result": [...]} but some tenants return a bare list
        raw: list[dict] = (
            data.get("result") if isinstance(data, dict) else data
        ) or []

        jobs: list[dict[str, Any]] = []
        for p in raw:
            job_id: str = str(p.get("id") or "")
            title: str = p.get("title") or ""

            loc: dict = p.get("location") or {}
            city: str = loc.get("city") or ""
            state: str = loc.get("state") or ""
            country: str = loc.get("country") or ""
            location_raw = ", ".join(filter(None, [city, state, country]))

            is_remote = "remote" in location_raw.lower() or "remote" in title.lower()
            work_type = "remote" if is_remote else "onsite"
            geo_restriction = classify_listing(
                location_raw=location_raw,
                description="",
                work_type=work_type,
                country=country or None,
            )

            dept: str | None = (p.get("department") or {}).get("label")
            emp_type: str | None = p.get("employmentStatusLabel")
            source_url = _JOB_URL.format(slug=ats_slug, job_id=job_id)

            jobs.append(
                {
                    "title": title,
                    "location": location_raw,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": "BambooHR",
                    "posted_at": None,
                    "geo_restriction": geo_restriction,
                    "ats_type": self.ats_type,
                    "external_job_id": job_id,
                    "department": dept,
                    "employment_type": emp_type,
                    "ats_source_id": ats_source_id,
                    "salary_currency": "USD",
                }
            )

        logger.info("bamboohr: slug=%s crawled %d jobs", ats_slug, len(jobs))
        return jobs
