"""Workday ATS crawler.

Uses Workday's public sitemap.xml to discover all job listings.
This approach is more reliable than the /wday/cxs/ API which requires CSRF tokens.

Strategy:
1. Fetch sitemap URL from crawl_config (stored during probing)
2. Parse sitemap XML to extract all job URLs
3. Extract job details from URL structure and patterns
"""

import logging
import re
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from app.crawler.ats_base import BaseATSCrawler
from app.crawler.geo_classifier import classify_listing
from app.db import AsyncSessionFactory

logger = logging.getLogger(__name__)


class WorkdayCrawler(BaseATSCrawler):
    ats_type = "workday"

    async def crawl(
        self, ats_slug: str, ats_source_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        """Crawl Workday jobs using sitemap.xml approach."""
        from sqlalchemy import text
        
        # Fetch crawl_config from database
        async with AsyncSessionFactory() as db:
            row = (
                await db.execute(
                    text("SELECT crawl_config FROM jobs.ats_sources WHERE id = :id"),
                    {"id": ats_source_id}
                )
            ).fetchone()
            
            if not row or not row[0]:
                logger.warning(f"No crawl_config found for Workday source {ats_source_id}")
                return []
            
            config = row[0]
            sitemap_url = config.get("sitemap_url")
            instance = config.get("instance", "wd5")
            career_site_name = config.get("career_site_name", "External")
            
            if not sitemap_url:
                # Fallback: construct sitemap URL from config
                sitemap_url = f"https://{ats_slug}.{instance}.myworkdayjobs.com/en-US/{career_site_name}-sitemap.xml"
        
        # Fetch sitemap XML (use httpx directly since it's XML, not JSON)
        import httpx
        from app.crawler.exceptions import RateLimitedException, SlugNotFoundException, CrawlException
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        
        async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
            try:
                resp = await client.get(sitemap_url)
                resp.raise_for_status()
                sitemap_text = resp.text
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise SlugNotFoundException(f"404 Not Found for {sitemap_url}", http_status=404)
                elif e.response.status_code == 429:
                    raise RateLimitedException(f"429 Rate Limited for {sitemap_url}", http_status=429)
                else:
                    raise CrawlException(f"HTTP {e.response.status_code} for {sitemap_url}", http_status=e.response.status_code)
        
        # Parse XML sitemap
        try:
            root = ET.fromstring(sitemap_text)
            # Handle XML namespace
            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            job_urls = [loc.text for loc in root.findall('.//ns:loc', ns) if loc.text]
            
            # Fallback if no namespace
            if not job_urls:
                job_urls = [loc.text for loc in root.findall('.//loc') if loc.text]
        except ET.ParseError as e:
            logger.error(f"Failed to parse Workday sitemap for {ats_slug}: {e}")
            return []
        
        jobs: list[dict[str, Any]] = []
        base_url = f"https://{ats_slug}.{instance}.myworkdayjobs.com"
        
        # Extract job details from each URL
        # Example URL: https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite/job/US-CA-Santa-Clara/Senior-Engineer_JR12345
        for url in job_urls:
            if not url or '/job/' not in url:
                continue
            
            try:
                # Extract job ID (last segment after underscore)
                # e.g., "Senior-Engineer_JR12345" -> "JR12345"
                path_parts = url.rstrip('/').split('/')
                last_segment = path_parts[-1]
                
                if '_' in last_segment:
                    external_job_id = last_segment.split('_')[-1]
                    title_slug = last_segment.split('_')[0]
                    title = title_slug.replace('-', ' ').title()
                else:
                    external_job_id = last_segment
                    title = "Position"
                
                # Extract location from URL path (segment before job title)
                # e.g., "/US-CA-Santa-Clara/Senior-Engineer" -> "US-CA-Santa-Clara"
                location = ""
                if len(path_parts) > 1:
                    location_slug = path_parts[-2]
                    if location_slug and location_slug != 'job':
                        location = location_slug.replace('-', ', ')
                
                # Determine if remote
                is_remote = 'remote' in location.lower() or 'remote' in title.lower()
                work_type = "remote" if is_remote else "onsite"
                
                # Classify geo restriction
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
                    "posted_at": datetime.now(timezone.utc),  # Sitemap doesn't include dates
                    "geo_restriction": geo_restriction,
                    "ats_type": self.ats_type,
                    "external_job_id": external_job_id,
                    "department": None,  # Not available in sitemap
                    "ats_source_id": ats_source_id,
                    "salary_currency": "USD",
                })
            except Exception as e:
                logger.debug(f"Failed to parse Workday job URL {url}: {e}")
                continue
        
        logger.info(f"workday: slug={ats_slug} crawled {len(jobs)} jobs from sitemap")
        return jobs
