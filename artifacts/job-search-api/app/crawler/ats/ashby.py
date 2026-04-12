"""Ashby ATS crawler.

Public API (single GET — returns all open postings):
    GET https://api.ashbyhq.com/posting-api/job-board/{slug}

Response schema:
    {
        "jobPostings": [
            {
                "id": "uuid-...",
                "title": "...",
                "department": "Engineering",
                "location": "New York, NY",
                "employmentType": "FullTime",
                "isRemote": true,
                "externalLink": "https://jobs.ashbyhq.com/company/uuid"
            }
        ]
    }

404 on unknown slug → SlugNotFoundException.
"""

import logging
import uuid
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_API_BASE = "https://api.ashbyhq.com/posting-api/job-board/{slug}"

_EMP_TYPE_MAP: dict[str, str] = {
    "FullTime": "Full-time",
    "PartTime": "Part-time",
    "Contract": "Contract",
    "Temporary": "Temporary",
    "Internship": "Internship",
    "Volunteer": "Volunteer",
}


class AshbyCrawler(BaseATSCrawler):
    ats_type = "ashby"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        api_url = _API_BASE.format(slug=ats_slug)

        data = await self._get_json(api_url)

        raw_postings: list[dict] = data.get("jobPostings") or []
        jobs: list[dict[str, Any]] = []

        for p in raw_postings:
            job_id: str = str(p.get("id") or "")
            title: str = p.get("title") or ""
            location_raw: str = p.get("location") or ""
            dept: str | None = p.get("department")
            is_remote: bool = bool(p.get("isRemote"))
            source_url: str = p.get("externalLink") or (
                f"https://jobs.ashbyhq.com/{ats_slug}/{job_id}"
            )
            emp_type_raw: str | None = p.get("employmentType")
            emp_type: str | None = _EMP_TYPE_MAP.get(emp_type_raw or "", emp_type_raw)

            work_type = "remote" if is_remote else "onsite"
            # Prefer location text for geo; fall back to isRemote flag
            effective_location = location_raw or ("remote" if is_remote else "")
            geo_restriction = classify_listing(
                location_raw=effective_location,
                description="",
                work_type=work_type,
                country=None,
            )

            jobs.append(
                {
                    "title": title,
                    "location": location_raw,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": "Ashby",
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

        logger.info("ashby: slug=%s crawled %d jobs", ats_slug, len(jobs))
        return jobs
