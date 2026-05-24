"""SmartRecruiters ATS crawler.

Public API (paginated GET):
    GET https://api.smartrecruiters.com/v1/companies/{slug}/postings
        ?limit=100&offset={offset}

Response schema:
    {
        "content": [...],
        "totalFound": N
    }

Each posting:
    {
        "id": "...",
        "name": "...",
        "location": {"city": "...", "country": "...", "remote": true|false},
        "department": {"label": "..."},
        "typeOfEmployment": {"label": "Full-time"}
    }
"""

import logging
import uuid
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing

logger = logging.getLogger(__name__)

_API_BASE = "https://api.smartrecruiters.com/v1/companies/{slug}/postings"
_JOB_BASE = "https://jobs.smartrecruiters.com/{slug}/{job_id}"
_PAGE_SIZE = 100


class SmartRecruitersCrawler(BaseATSCrawler):
    ats_type = "smartrecruiters"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        offset = 0
        api_url = _API_BASE.format(slug=ats_slug)

        while True:
            data = await self._get_json(
                api_url,
                params={"limit": _PAGE_SIZE, "offset": offset},
            )
            content: list[dict] = data.get("content") or []
            if not content:
                break

            for p in content:
                job_id: str = str(p.get("id") or "")
                title: str = p.get("name") or ""
                loc: dict = p.get("location") or {}
                city: str = loc.get("city") or ""
                country_code: str = loc.get("country") or ""
                is_remote: bool = bool(loc.get("remote"))
                location_raw = f"{city}, {country_code}".strip(", ")
                work_type = "remote" if is_remote else "onsite"
                geo_restriction = classify_listing(
                    location_raw=location_raw,
                    description="",
                    work_type=work_type,
                    country=country_code or None,
                )
                dept: str | None = (p.get("department") or {}).get("label")
                emp_type: str | None = (p.get("typeOfEmployment") or {}).get("label")
                source_url = _JOB_BASE.format(slug=ats_slug, job_id=job_id)

                jobs.append(
                    {
                        "title": title,
                        "location": location_raw,
                        "remote": is_remote,
                        "source_url": source_url,
                        "source_label": "SmartRecruiters",
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

            total: int = data.get("totalFound") or 0
            offset += len(content)
            if offset >= total:
                break

        logger.info(
            "smartrecruiters: slug=%s crawled %d jobs", ats_slug, len(jobs)
        )
        return jobs
