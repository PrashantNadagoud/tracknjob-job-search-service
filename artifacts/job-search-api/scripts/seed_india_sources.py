#!/usr/bin/env python3
"""Seed India ATS sources from data/india_ats_sources.json.

Usage:
    python scripts/seed_india_sources.py           # live upsert
    python scripts/seed_india_sources.py --dry-run # preview, no DB writes

Rules:
- Skips records where ats_type='custom' or ats_slug is null (reference only).
- For Workday records with a career_site_url, parses instance + career_site_name
  and persists them in crawl_config so the crawler can use the CXS API immediately.
- Never modifies existing rows where country='US'.
- Sets is_active=false, last_crawl_status='pending_validation' on fresh inserts.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text

from app.db import AsyncSessionFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("seed_india")

DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "india_ats_sources.json"

# Matches: https://<slug>.<instance>.myworkdayjobs.com/<career_site_name>
_WORKDAY_URL_RE = re.compile(
    r"https://[\w-]+\.(wd\d+)\.myworkdayjobs\.com/([^/?#\s]+)"
)


def _slugify(name: str) -> str:
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def _parse_workday_config(career_site_url: str | None) -> dict | None:
    """Extract {'instance': 'wd3', 'career_site_name': 'AccentureCareers'} from a Workday URL.

    Returns None if the URL doesn't match the expected Workday pattern.
    """
    if not career_site_url:
        return None
    m = _WORKDAY_URL_RE.search(career_site_url)
    if not m:
        return None
    return {"instance": m.group(1), "career_site_name": m.group(2)}


async def run(dry_run: bool) -> None:
    records = json.loads(DATA_FILE.read_text())

    inserted = 0
    updated = 0
    skipped = 0

    async with AsyncSessionFactory() as db:
        for rec in records:
            ats_type = rec.get("ats_type")
            ats_slug = rec.get("ats_slug")

            if ats_type == "custom" or not ats_slug:
                skipped += 1
                if dry_run:
                    print(f"  SKIP (custom/null slug): {rec['company_name']}")
                continue

            company_name: str = rec["company_name"]
            career_site_url: str | None = rec.get("career_site_url")
            country: str = rec.get("country", "IN")
            location_filter: str | None = rec.get("location_filter")
            notes: str | None = rec.get("notes")

            # Build crawl_config for Workday sources so the crawler can hit CXS API directly.
            crawl_config: dict | None = None
            if ats_type == "workday":
                crawl_config = _parse_workday_config(career_site_url)

            company_slug = _slugify(company_name)

            if dry_run:
                config_info = f" crawl_config={crawl_config}" if crawl_config else ""
                print(
                    f"  WOULD UPSERT: {company_name!r} | "
                    f"ats_type={ats_type} slug={ats_slug} country={country}{config_info}"
                )
                inserted += 1
                continue

            company_row = (await db.execute(
                text("""
                    INSERT INTO jobs.companies (name, slug)
                    VALUES (:name, :slug)
                    ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id
                """),
                {"name": company_name, "slug": company_slug},
            )).fetchone()
            company_id = company_row[0]

            existing = (await db.execute(
                text("""
                    SELECT id FROM jobs.ats_sources
                    WHERE ats_type = :ats_type
                      AND ats_slug  = :ats_slug
                      AND country   = :country
                """),
                {"ats_type": ats_type, "ats_slug": ats_slug, "country": country},
            )).fetchone()

            if existing:
                await db.execute(
                    text("""
                        UPDATE jobs.ats_sources
                        SET career_site_url  = :career_site_url,
                            location_filter  = :location_filter,
                            notes            = :notes,
                            crawl_config     = COALESCE(:crawl_config::jsonb, crawl_config),
                            updated_at       = now()
                        WHERE id = :id
                    """),
                    {
                        "id": existing[0],
                        "career_site_url": career_site_url,
                        "location_filter": location_filter,
                        "notes": notes,
                        "crawl_config": json.dumps(crawl_config) if crawl_config else None,
                    },
                )
                updated += 1
                logger.info("Updated: %s (%s/%s/%s)", company_name, ats_type, ats_slug, country)
            else:
                await db.execute(
                    text("""
                        INSERT INTO jobs.ats_sources
                            (company_id, ats_type, ats_slug, career_site_url,
                             country, location_filter, notes, crawl_config,
                             is_active, last_crawl_status)
                        VALUES
                            (:company_id, :ats_type, :ats_slug, :career_site_url,
                             :country, :location_filter, :notes, :crawl_config::jsonb,
                             false, 'pending_validation')
                    """),
                    {
                        "company_id": company_id,
                        "ats_type": ats_type,
                        "ats_slug": ats_slug,
                        "career_site_url": career_site_url,
                        "country": country,
                        "location_filter": location_filter,
                        "notes": notes,
                        "crawl_config": json.dumps(crawl_config) if crawl_config else "{}",
                    },
                )
                inserted += 1
                logger.info("Inserted: %s (%s/%s/%s)", company_name, ats_type, ats_slug, country)

        if not dry_run:
            await db.commit()

    print(
        f"\nIndia sources: {inserted} inserted | {updated} updated | {skipped} skipped (custom/null)"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed India ATS sources")
    ap.add_argument("--dry-run", action="store_true", help="Preview changes without writing to DB")
    args = ap.parse_args()
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
