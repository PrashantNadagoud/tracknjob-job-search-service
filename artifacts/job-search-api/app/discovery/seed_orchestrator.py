"""SeedOrchestrator — full YC→ATS seed pipeline.

Pipeline:
  1. Fetch company list via YCScraper
  2. Deduplicate against existing jobs.companies.website and
     jobs.company_discovery_queue.website
  3. Probe each candidate via ATSProber
  4a. ATS match  → INSERT jobs.companies + jobs.ats_sources + queue row (resolved)
  4b. No match   → INSERT queue row (rejected)

Process in batches of `batch_size` with a 2-second pause between batches.
Log progress every 50 companies and a final summary.
`dry_run=True` skips all DB writes.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.discovery.ats_prober import ATSProber
from app.discovery.yc_scraper import YCScraper

logger = logging.getLogger(__name__)

_SLUGIFY_RE = re.compile(r"[^\w\s-]")
_SPACE_RE = re.compile(r"[\s_]+")
_MULTI_DASH_RE = re.compile(r"-+")


def _slugify(name: str) -> str:
    slug = name.lower()
    slug = _SLUGIFY_RE.sub("", slug)
    slug = _SPACE_RE.sub("-", slug)
    slug = _MULTI_DASH_RE.sub("-", slug)
    return slug.strip("-")


class SeedOrchestrator:
    """Runs the end-to-end YC discovery and ATS seed pipeline."""

    def __init__(self, db_session: AsyncSession, batch_size: int = 50) -> None:
        self._db = db_session
        self._batch_size = batch_size

    async def run(
        self, market: str = "US", dry_run: bool = False
    ) -> dict[str, int]:
        """Execute the full seed pipeline.

        Returns:
            Summary dict: {total, skipped, probed, matched, rejected}
        """
        counts: dict[str, int] = {
            "total": 0,
            "skipped": 0,
            "probed": 0,
            "matched": 0,
            "rejected": 0,
        }

        logger.info("SeedOrchestrator.run: market=%s dry_run=%s", market, dry_run)

        scraper = YCScraper()
        companies = await scraper.fetch()
        counts["total"] = len(companies)
        logger.info("Fetched %d companies from YC", counts["total"])

        existing_websites = await self._fetch_existing_websites()

        candidates = [
            c for c in companies
            if c.get("website") and c["website"] not in existing_websites
        ]
        counts["skipped"] = counts["total"] - len(candidates)
        logger.info(
            "%d already known; %d candidates to probe",
            counts["skipped"],
            len(candidates),
        )

        prober = ATSProber()

        for batch_start in range(0, len(candidates), self._batch_size):
            batch = candidates[batch_start: batch_start + self._batch_size]

            for idx, company in enumerate(batch):
                global_idx = batch_start + idx + 1
                if global_idx % 50 == 0:
                    logger.info("Progress: %d / %d probed so far", global_idx, len(candidates))

                counts["probed"] += 1
                match = await prober.probe(company)

                if match:
                    counts["matched"] += 1
                    if not dry_run:
                        await self._insert_matched(company, match, market)
                else:
                    counts["rejected"] += 1
                    if not dry_run:
                        await self._insert_rejected(company, market)

            if not dry_run:
                await self._db.commit()

            if batch_start + self._batch_size < len(candidates):
                await asyncio.sleep(2)

        logger.info(
            "SeedOrchestrator complete: total=%d skipped=%d probed=%d matched=%d rejected=%d",
            counts["total"],
            counts["skipped"],
            counts["probed"],
            counts["matched"],
            counts["rejected"],
        )
        return counts

    async def _fetch_existing_websites(self) -> set[str]:
        """Return set of websites already in companies or discovery queue."""
        rows_companies = (
            await self._db.execute(
                text("SELECT website FROM jobs.companies WHERE website IS NOT NULL")
            )
        ).fetchall()

        rows_queue = (
            await self._db.execute(
                text(
                    "SELECT website FROM jobs.company_discovery_queue "
                    "WHERE website IS NOT NULL"
                )
            )
        ).fetchall()

        websites: set[str] = set()
        for (w,) in rows_companies:
            if w:
                websites.add(w.rstrip("/"))
        for (w,) in rows_queue:
            if w:
                websites.add(w.rstrip("/"))

        return websites

    async def _insert_matched(
        self,
        company: dict[str, Any],
        match: dict[str, Any],
        market: str,
    ) -> None:
        """Insert company, ats_source, and resolved queue row."""
        name = company["name"]
        website = company.get("website")
        slug = _slugify(name) if name else match["ats_slug"]
        ats_type = match["ats_type"]
        ats_slug = match["ats_slug"]
        crawl_url = match["crawl_url"]

        company_row = await self._db.execute(
            text("""
                INSERT INTO jobs.companies (slug, name, website, company_type)
                VALUES (:slug, :name, :website, 'unknown')
                ON CONFLICT DO NOTHING
                RETURNING id
            """),
            {"slug": slug, "name": name, "website": website},
        )
        row = company_row.fetchone()

        if row is None:
            row = (
                await self._db.execute(
                    text("SELECT id FROM jobs.companies WHERE slug = :slug"),
                    {"slug": slug},
                )
            ).fetchone()

        if row is None:
            logger.warning("Could not find or insert company with slug=%s", slug)
            return

        company_id = row[0]

        await self._db.execute(
            text("""
                INSERT INTO jobs.ats_sources
                    (company_id, ats_type, ats_slug, crawl_url, market, discovery_source)
                VALUES
                    (:company_id, :ats_type, :ats_slug, :crawl_url, :market, 'yc_directory')
                ON CONFLICT DO NOTHING
            """),
            {
                "company_id": company_id,
                "ats_type": ats_type,
                "ats_slug": ats_slug,
                "crawl_url": crawl_url,
                "market": market,
            },
        )

        await self._db.execute(
            text("""
                INSERT INTO jobs.company_discovery_queue
                    (company_name, website, suspected_ats, suspected_slug,
                     source, market, status, resolved_company_id)
                VALUES
                    (:name, :website, :ats_type, :ats_slug,
                     'yc_directory', :market, 'resolved', :company_id)
                ON CONFLICT DO NOTHING
            """),
            {
                "name": name,
                "website": website,
                "ats_type": ats_type,
                "ats_slug": ats_slug,
                "market": market,
                "company_id": company_id,
            },
        )

    async def _insert_rejected(
        self,
        company: dict[str, Any],
        market: str,
    ) -> None:
        """Insert a rejected queue row for a company with no ATS match."""
        name = company["name"]
        website = company.get("website")

        await self._db.execute(
            text("""
                INSERT INTO jobs.company_discovery_queue
                    (company_name, website, source, market, status)
                VALUES
                    (:name, :website, 'yc_directory', :market, 'rejected')
                ON CONFLICT DO NOTHING
            """),
            {"name": name, "website": website, "market": market},
        )
