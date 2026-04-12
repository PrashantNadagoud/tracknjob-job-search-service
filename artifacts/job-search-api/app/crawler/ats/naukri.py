"""Naukri.com ATS crawler — India market.

Uses the Naukri public search API (paginated GET):
    GET https://www.naukri.com/jobapi/v3/search
        ?noOfResults=20
        &urlType=search_by_key_wo_lj
        &searchType=adv
        &src=directsearch
        &keyword={slug}
        &pageNo={page}

Required headers:
    appid: 109
    systemid: 109

All listings produced by this crawler force:
    geo_restriction = "IN"
    salary_currency = "INR"
    country = "IN"
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from app.crawler.ats_base import BaseATSCrawler

logger = logging.getLogger(__name__)

_API_URL = "https://www.naukri.com/jobapi/v3/search"
_NAUKRI_HEADERS = {"appid": "109", "systemid": "109"}
_PAGE_SIZE = 20


class NaukriCrawler(BaseATSCrawler):
    ats_type = "naukri"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        page = 1

        while True:
            data = await self._get_json(
                _API_URL,
                params={
                    "noOfResults": _PAGE_SIZE,
                    "urlType": "search_by_key_wo_lj",
                    "searchType": "adv",
                    "src": "directsearch",
                    "keyword": ats_slug,
                    "pageNo": page,
                },
                extra_headers=_NAUKRI_HEADERS,
            )

            job_details: list[dict] = data.get("jobDetails") or []
            if not job_details:
                break

            for p in job_details:
                job_id: str = str(p.get("jobId") or p.get("job_id") or "")
                title: str = p.get("title") or p.get("jobTitle") or ""
                location_raw: str = p.get("placeholders", [{}])[0].get("label", "") if p.get("placeholders") else p.get("location") or ""

                posted_at: datetime | None = None
                ts_raw = p.get("jobCreatedDate") or p.get("footerPlaceholderLabel")
                if ts_raw and isinstance(ts_raw, (int, float)):
                    try:
                        posted_at = datetime.fromtimestamp(ts_raw / 1000, tz=timezone.utc)
                    except (ValueError, OSError):
                        posted_at = None

                source_url: str = p.get("jdURL") or p.get("applyRedirectURL") or (
                    f"https://www.naukri.com/-{job_id}"
                )
                dept: str | None = p.get("functionalArea")
                emp_type: str | None = p.get("employmentType") or p.get("label")

                jobs.append(
                    {
                        "title": title,
                        "location": location_raw,
                        "remote": False,
                        "source_url": source_url,
                        "source_label": "Naukri",
                        "posted_at": posted_at,
                        "geo_restriction": "IN",
                        "country": "IN",
                        "ats_type": self.ats_type,
                        "external_job_id": job_id,
                        "department": dept,
                        "employment_type": emp_type,
                        "ats_source_id": ats_source_id,
                        "salary_currency": "INR",
                    }
                )

            total: int = int(data.get("noOfJobs") or data.get("total") or 0)
            if page * _PAGE_SIZE >= total:
                break
            page += 1

        logger.info("naukri: slug=%s crawled %d jobs", ats_slug, len(jobs))
        return jobs
