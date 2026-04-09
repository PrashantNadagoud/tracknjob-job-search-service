"""Rippling ATS crawler.

Public jobs feed (single GET, returns array):
    GET https://app.rippling.com/api/recruiting/jobs-feed/{slug}

Each element:
    {
        "id": "...",
        "title": "...",
        "location": "San Francisco, CA",
        "department": "Engineering",
        "employmentType": "FULL_TIME"
    }
"""

import logging
import uuid
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_API_URL = "https://app.rippling.com/api/recruiting/jobs-feed/{slug}"
_JOB_URL = "https://app.rippling.com/jobs/{slug}/{job_id}"

_EMP_TYPE_MAP = {
    "FULL_TIME": "Full-time",
    "PART_TIME": "Part-time",
    "CONTRACTOR": "Contract",
    "INTERN": "Internship",
}


class RipplingCrawler(BaseATSCrawler):
    ats_type = "rippling"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        api_url = _API_URL.format(slug=ats_slug)
        raw = await self._get_json(api_url)

        # Rippling returns a bare JSON array
        if isinstance(raw, dict):
            raw = raw.get("jobs") or raw.get("data") or []

        jobs: list[dict[str, Any]] = []
        for p in raw or []:
            job_id: str = str(p.get("id") or "")
            title: str = p.get("title") or ""
            location_raw: str = p.get("location") or ""

            is_remote = "remote" in location_raw.lower() or "remote" in title.lower()
            work_type = "remote" if is_remote else "onsite"
            geo_restriction = classify_listing(
                location_raw=location_raw,
                description="",
                work_type=work_type,
                country=None,
            )

            emp_type_raw: str = p.get("employmentType") or ""
            emp_type: str | None = _EMP_TYPE_MAP.get(emp_type_raw, emp_type_raw or None)
            dept: str | None = p.get("department")
            source_url = _JOB_URL.format(slug=ats_slug, job_id=job_id)

            jobs.append(
                {
                    "title": title,
                    "location": location_raw,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": f"{ats_slug} Careers (Rippling)",
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

        logger.info("rippling: slug=%s crawled %d jobs", ats_slug, len(jobs))
        return jobs
