"""Workday ATS crawler.

Strategy (chosen per source):
- If source.location_filter is set  → Workday CXS API with location facet
  (paginated POST to /wday/cxs/{slug}/{site}/jobs)
- If location_filter is null         → sitemap.xml approach (existing behaviour)
  (more reliable for sources where CSRF tokens are not needed)

# TODO: Enterprise Workday CSRF Fix
# Companies: IBM, Microsoft, Accenture, Deloitte, EY,
#             Cognizant, KPMG, SAP, Oracle, Capgemini (India sources)
# Issue: These tenants return HTTP 500 on the CSRF GET endpoint
#         (/wday/cxs/{slug}/{site}/jobs returns 422; GET /{site}/jobs returns 500)
#         so session cookies + CSRF token cannot be harvested server-side.
# Fix: Requires Playwright headless browser to establish a real browser session,
#      extract session cookies, and replay them in the CXS POST request.
# Priority: Post-Railway-deploy enhancement
# DB status: last_crawl_status = 'requires_browser_crawl' for these sources
# Workaround: These companies post India jobs on LinkedIn and their US Workday
#             slugs work fine for US jobs via the sitemap approach.

CSRF handling:
  Some Workday instances enforce CSRF protection on the CXS endpoint.
  When a 403 or 422 is returned on the first POST, the crawler does a
  preliminary GET to the job-board HTML page to harvest the session
  cookies and CSRF token (CALYPSO_CSRF_TOKEN or similar), then retries
  the POST with those cookies and an X-CSRF-Token header.  This retry
  only happens once per crawl to avoid infinite loops.
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

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
_BROWSER_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Accept": "application/json, text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

_CSRF_COOKIE_NAMES = ("CALYPSO_CSRF_TOKEN", "csrf-token", "wd-csrf-token", "CSRF-TOKEN")
_CSRF_HEADER_NAMES = ("x-csrf-token", "X-CSRF-Token")


async def _fetch_csrf_token(
    client: Any,
    slug: str,
    instance: str,
    career_site_name: str,
) -> tuple[str | None, dict]:
    """GET the Workday job-board page to harvest session cookies + CSRF token.

    Returns (csrf_token_or_None, cookies_dict).  Never raises — errors are
    swallowed so the caller can decide whether to proceed without CSRF.
    """
    page_url = (
        f"https://{slug}.{instance}.myworkdayjobs.com/{career_site_name}/jobs"
    )
    try:
        resp = await client.get(
            page_url,
            headers={
                "User-Agent": _BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        cookies = dict(resp.cookies)
        for name in _CSRF_COOKIE_NAMES:
            token = cookies.get(name)
            if token:
                logger.debug(
                    "CSRF token found in cookie %r for %s/%s", name, slug, instance
                )
                return token, cookies
        for name in _CSRF_HEADER_NAMES:
            token = resp.headers.get(name)
            if token:
                logger.debug(
                    "CSRF token found in header %r for %s/%s", name, slug, instance
                )
                return token, cookies
        logger.debug("No CSRF token in GET response for %s/%s; proceeding without", slug, instance)
        return None, cookies
    except Exception as exc:
        logger.debug("CSRF GET failed for %s/%s: %s", slug, instance, exc)
        return None, {}


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
        referer = f"{base_url}/{career_site_name}/jobs"

        jobs: list[dict[str, Any]] = []
        offset = 0
        limit = 20

        post_headers: dict[str, str] = {
            **_BROWSER_HEADERS,
            "Content-Type": "application/json",
            "Referer": referer,
        }
        extra_cookies: dict = {}
        csrf_fetched = False

        use_search_text = config.get("search_text_mode", False)
        _SEARCH_TEXT_MAX_JOBS = 500  # cap for searchText mode (not a precise location filter)

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0),
            follow_redirects=True,
        ) as client:
            while True:
                if use_search_text:
                    body: dict[str, Any] = {
                        "searchText": location_filter,
                        "limit": limit,
                        "offset": offset,
                    }
                else:
                    body = {
                        "appliedFacets": {"Location": [location_filter]},
                        "limit": limit,
                        "offset": offset,
                    }
                try:
                    resp = await client.post(
                        api_url,
                        json=body,
                        headers=post_headers,
                        cookies=extra_cookies,
                    )
                except Exception as exc:
                    raise CrawlException(f"CXS request failed for {ats_slug}: {exc}")

                # ── CSRF retry (once only) ─────────────────────────────────
                if resp.status_code == 400 and not use_search_text:
                    logger.info(
                        "workday CXS: Location facet rejected (400) for %s — switching to searchText mode",
                        ats_slug,
                    )
                    use_search_text = True
                    offset = 0
                    continue

                if resp.status_code in (403, 422) and not csrf_fetched:
                    csrf_fetched = True
                    csrf_token, extra_cookies = await _fetch_csrf_token(
                        client, ats_slug, instance, career_site_name
                    )
                    if csrf_token:
                        post_headers = {**post_headers, "X-CSRF-Token": csrf_token}
                        logger.info(
                            "workday CXS: CSRF token acquired for %s/%s — retrying",
                            ats_slug, instance,
                        )
                    else:
                        logger.debug(
                            "workday CXS: no CSRF token found for %s/%s — retrying anyway",
                            ats_slug, instance,
                        )
                    continue  # Retry the same offset with the updated credentials

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
                if use_search_text and len(jobs) >= _SEARCH_TEXT_MAX_JOBS:
                    logger.info(
                        "workday CXS: searchText mode hit %d-job cap for %s — stopping pagination",
                        _SEARCH_TEXT_MAX_JOBS, ats_slug,
                    )
                    break

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
