"""JazzHR ATS crawler.

JazzHR uses the company slug as the API key:
    GET https://api.jazz.co/api/jobs?apikey={slug}&status=Open&count=100

Response: JSON array of job objects.

Each job:
    {
        "id": "...",
        "title": "...",
        "city": "San Francisco",
        "state": "CA",
        "country": "US",
        "type": "Full Time"
    }
"""

import logging
import uuid
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_API_URL = "https://api.jazz.co/api/jobs"
_JOB_URL = "https://{slug}.applytojob.com/apply/{job_id}"


class JazzHRCrawler(BaseATSCrawler):
    ats_type = "jazzhr"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        raw = await self._get_json(
            _API_URL,
            params={"apikey": ats_slug, "status": "Open", "count": 100},
        )

        # JazzHR may return {"jobs": [...]} or a bare list
        if isinstance(raw, dict):
            raw = raw.get("jobs") or []

        jobs: list[dict[str, Any]] = []
        for p in raw or []:
            job_id: str = str(p.get("id") or "")
            title: str = p.get("title") or ""

            city: str = p.get("city") or ""
            state: str = p.get("state") or ""
            country: str = p.get("country") or ""
            location_raw = ", ".join(filter(None, [city, state, country]))

            is_remote = "remote" in location_raw.lower() or "remote" in title.lower()
            work_type = "remote" if is_remote else "onsite"
            geo_restriction = classify_listing(
                location_raw=location_raw,
                description="",
                work_type=work_type,
                country=country or None,
            )

            emp_type: str | None = p.get("type")
            source_url = _JOB_URL.format(slug=ats_slug, job_id=job_id)

            jobs.append(
                {
                    "title": title,
                    "location": location_raw,
                    "remote": is_remote,
                    "source_url": source_url,
                    "source_label": f"{ats_slug} Careers (JazzHR)",
                    "posted_at": None,
                    "geo_restriction": geo_restriction,
                    "ats_type": self.ats_type,
                    "external_job_id": job_id,
                    "employment_type": emp_type,
                    "ats_source_id": ats_source_id,
                    "salary_currency": "USD",
                }
            )

        logger.info("jazzhr: slug=%s crawled %d jobs", ats_slug, len(jobs))
        return jobs
