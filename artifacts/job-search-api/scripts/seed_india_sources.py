#!/usr/bin/env python3
"""Seed India ATS sources from data/india_ats_sources.json.

Usage:
    python scripts/seed_india_sources.py           # live upsert
    python scripts/seed_india_sources.py --dry-run # preview, no DB writes

Rules:
- Skips records where ats_type='custom' or ats_slug is null
- For valid records: find-or-create a jobs.companies placeholder row, then
  upsert jobs.ats_sources keyed on (ats_type, ats_slug, country)
- Sets is_active=false and last_crawl_status='pending_validation' on insert
- Never modifies existing rows where country='US'
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


def _slugify(name: str) -> str:
    """Convert a company name to a URL-safe slug."""
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug


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

            company_slug = _slugify(company_name)

            if dry_run:
                print(
                    f"  WOULD UPSERT: {company_name!r} | "
                    f"ats_type={ats_type} slug={ats_slug} country={country}"
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
                            updated_at       = now()
                        WHERE id = :id
                    """),
                    {
                        "id": existing[0],
                        "career_site_url": career_site_url,
                        "location_filter": location_filter,
                        "notes": notes,
                    },
                )
                updated += 1
                logger.info("Updated: %s (%s/%s/%s)", company_name, ats_type, ats_slug, country)
            else:
                await db.execute(
                    text("""
                        INSERT INTO jobs.ats_sources
                            (company_id, ats_type, ats_slug, career_site_url,
                             country, location_filter, notes,
                             is_active, last_crawl_status)
                        VALUES
                            (:company_id, :ats_type, :ats_slug, :career_site_url,
                             :country, :location_filter, :notes,
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
