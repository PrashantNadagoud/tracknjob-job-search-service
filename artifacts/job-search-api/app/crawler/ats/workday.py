"""Workday ATS crawler.

Strategy (chosen per source):
- If source.location_filter is set  → Workday CXS API with location facet
  (paginated POST to /wday/cxs/{slug}/{site}/jobs)
- If location_filter is null         → sitemap.xml approach (existing behaviour)
  (more reliable for sources where CSRF tokens are not needed)
"""

import logging
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing
from app.db import AsyncSessionFactory

logger = logging.getLogger(__name__)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html,*/*",
    "Accept-Language": "en-US,en;q=0.5",
}


class WorkdayCrawler(BaseATSCrawler):
    ats_type = "workday"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        from sqlalchemy import text

        async with AsyncSessionFactory() as db:
            row = (
                await db.execute(
                    text(
                        "SELECT crawl_config, location_filter "
                        "FROM jobs.ats_sources WHERE id = :id"
                    ),
                    {"id": ats_source_id},
                )
            ).fetchone()

            if not row:
                logger.warning("No ats_sources row found for id=%s", ats_source_id)
                return []

            config = row[0] or {}
            location_filter: str | None = row[1]

        if location_filter:
            return await self._crawl_cxs(ats_slug, ats_source_id, config, location_filter)
        return await self._crawl_sitemap(ats_slug, ats_source_id, config)

    # ── CXS API (used when location_filter is set) ────────────────────────────

    async def _crawl_cxs(
        self,
        ats_slug: str,
        ats_source_id: uuid.UUID,
        config: dict,
        location_filter: str,
    ) -> list[dict[str, Any]]:
        import httpx
        from app.crawler.exceptions import CrawlException, RateLimitedException, SlugNotFoundException

        instance = config.get("instance", "wd5")
        career_site_name = config.get("career_site_name", "External")

        base_url = f"https://{ats_slug}.{instance}.myworkdayjobs.com"
        api_url = f"{base_url}/wday/cxs/{ats_slug}/{career_site_name}/jobs"

        jobs: list[dict[str, Any]] = []
        offset = 0
        limit = 20

        headers = {**_BROWSER_HEADERS, "Content-Type": "application/json"}

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0),
            headers=headers,
        ) as client:
            while True:
                body: dict[str, Any] = {
                    "appliedFacets": {"Location": [location_filter]},
                    "limit": limit,
                    "offset": offset,
                }
                try:
                    resp = await client.post(api_url, json=body)
                except Exception as exc:
                    raise CrawlException(f"CXS request failed for {ats_slug}: {exc}")

                if resp.status_code == 404:
                    raise SlugNotFoundException(
                        f"CXS 404 for {api_url}", http_status=404
                    )
                if resp.status_code == 429:
                    raise RateLimitedException(
                        f"CXS 429 for {api_url}", http_status=429
                    )
                if resp.status_code != 200:
                    raise CrawlException(
                        f"CXS HTTP {resp.status_code} for {api_url}",
                        http_status=resp.status_code,
                    )

                data = resp.json()
                postings = data.get("jobPostings") or []
                if not postings:
                    break

                for posting in postings:
                    external_id = posting.get("bulletFields", [None])[0] or str(posting.get("title", ""))
                    ext_path = posting.get("externalPath", "")
                    source_url = f"{base_url}{ext_path}" if ext_path else api_url

                    title = posting.get("title", "Position")
                    location = posting.get("locationsText", location_filter)
                    is_remote = "remote" in location.lower() or "remote" in title.lower()
                    work_type = "remote" if is_remote else "onsite"

                    geo_restriction = classify_listing(
                        location_raw=location,
                        description="",
                        work_type=work_type,
                        country=None,
                    )

                    jobs.append({
                        "title": title,
                        "location": location,
                        "remote": is_remote,
                        "source_url": source_url,
                        "source_label": "Workday",
                        "posted_at": datetime.now(timezone.utc),
                        "geo_restriction": geo_restriction,
                        "ats_type": self.ats_type,
                        "external_job_id": ext_path.rstrip("/").split("/")[-1] if ext_path else external_id,
                        "department": None,
                        "ats_source_id": ats_source_id,
                        "salary_currency": "USD",
                    })

                if len(postings) < limit:
                    break
                offset += limit

        logger.info(
            "workday CXS: slug=%s location=%r crawled %d jobs",
            ats_slug, location_filter, len(jobs),
        )
        return jobs

    # ── Sitemap approach (existing behaviour when no location_filter) ──────────

    async def _crawl_sitemap(
        self,
        ats_slug: str,
        ats_source_id: uuid.UUID,
        config: dict,
    ) -> list[dict[str, Any]]:
        import httpx
        from app.crawler.exceptions import CrawlException, RateLimitedException, SlugNotFoundException

        sitemap_url = config.get("sitemap_url")
        instance = config.get("instance", "wd5")
        career_site_name = config.get("career_site_name", "External")

        if not sitemap_url:
            sitemap_url = (
                f"https://{ats_slug}.{instance}.myworkdayjobs.com"
                f"/en-US/{career_site_name}-sitemap.xml"
            )

        sitemap_headers = {
            **_BROWSER_HEADERS,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0),
            headers=sitemap_headers,
        ) as client:
            try:
                resp = await client.get(sitemap_url)
                resp.raise_for_status()
                sitemap_text = resp.text
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise SlugNotFoundException(
                        f"404 Not Found for {sitemap_url}", http_status=404
                    )
                elif e.response.status_code == 429:
                    raise RateLimitedException(
                        f"429 Rate Limited for {sitemap_url}", http_status=429
                    )
                else:
                    raise CrawlException(
                        f"HTTP {e.response.status_code} for {sitemap_url}",
                        http_status=e.response.status_code,
                    )

        try:
            root = ET.fromstring(sitemap_text)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            job_urls = [loc.text for loc in root.findall(".//ns:loc", ns) if loc.text]
            if not job_urls:
                job_urls = [loc.text for loc in root.findall(".//loc") if loc.text]
        except ET.ParseError as e:
            logger.error("Failed to parse Workday sitemap for %s: %s", ats_slug, e)
            return []

        jobs: list[dict[str, Any]] = []
        base_url = f"https://{ats_slug}.{instance}.myworkdayjobs.com"

        for url in job_urls:
            if not url or "/job/" not in url:
                continue

            try:
                path_parts = url.rstrip("/").split("/")
                last_segment = path_parts[-1]

                if "_" in last_segment:
                    external_job_id = last_segment.split("_")[-1]
                    title_slug = last_segment.split("_")[0]
                    title = title_slug.replace("-", " ").title()
                else:
                    external_job_id = last_segment
                    title = "Position"

                location = ""
                if len(path_parts) > 1:
                    location_slug = path_parts[-2]
                    if location_slug and location_slug != "job":
                        location = location_slug.replace("-", ", ")

                is_remote = "remote" in location.lower() or "remote" in title.lower()
                work_type = "remote" if is_remote else "onsite"

                geo_restriction = classify_listing(
                    location_raw=location,
                    description="",
                    work_type=work_type,
                    country=None,
                )

                jobs.append({
                    "title": title,
                    "location": location,
                    "remote": is_remote,
                    "source_url": url,
                    "source_label": "Workday",
                    "posted_at": datetime.now(timezone.utc),
                    "geo_restriction": geo_restriction,
                    "ats_type": self.ats_type,
                    "external_job_id": external_job_id,
                    "department": None,
                    "ats_source_id": ats_source_id,
                    "salary_currency": "USD",
                })
            except Exception as e:
                logger.debug("Failed to parse Workday job URL %s: %s", url, e)
                continue

        logger.info(
            "workday sitemap: slug=%s crawled %d jobs", ats_slug, len(jobs)
        )
        return jobs
