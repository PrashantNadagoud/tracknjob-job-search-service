#!/usr/bin/env python3
"""Validate India ATS sources by probing each ATS API.

For each jobs.ats_sources row where country='IN', is_active=false,
and ats_slug IS NOT NULL: send a probe request to the appropriate ATS
endpoint and mark the source active/inactive based on the response.

Usage:
    python scripts/validate_india_sources.py             # validate all
    python scripts/validate_india_sources.py --limit 10  # first 10 only
    python scripts/validate_india_sources.py --dry-run   # no DB writes
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

import httpx
from sqlalchemy import text

from app.db import AsyncSessionFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("validate_india")

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
_WORKDAY_INSTANCES = ["wd1", "wd3", "wd5", "wd12"]
_TIMEOUT = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=5.0)


def _probe_workday_sync(slug: str, location_filter: str | None) -> dict[str, Any]:
    """Try Workday CXS API across wd1/wd3/wd5/wd12. Returns {active, career_site_url}."""
    headers = {
        "User-Agent": _BROWSER_UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    body: dict[str, Any] = {"limit": 5, "offset": 0}
    if location_filter:
        body["appliedFacets"] = {"Location": [location_filter]}

    for instance in _WORKDAY_INSTANCES:
        for site_name in ["External", "Careers", "ExternalCareers"]:
            url = (
                f"https://{slug}.{instance}.myworkdayjobs.com"
                f"/wday/cxs/{slug}/{site_name}/jobs"
            )
            try:
                resp = httpx.post(url, json=body, headers=headers, timeout=_TIMEOUT)
                time.sleep(1)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("jobPostings") is not None:
                        career_url = (
                            f"https://{slug}.{instance}.myworkdayjobs.com/{site_name}"
                        )
                        logger.info(
                            "Workday %s validated: %s (%d postings)",
                            slug,
                            url,
                            len(data["jobPostings"]),
                        )
                        return {"active": True, "career_site_url": career_url}
            except Exception as exc:
                logger.debug("Workday probe %s failed: %s", url, exc)
                time.sleep(1)

    return {"active": False, "career_site_url": None}


def _probe_greenhouse_sync(slug: str) -> dict[str, Any]:
    """Probe Greenhouse public jobs board API."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    try:
        resp = httpx.get(url, timeout=_TIMEOUT)
        time.sleep(1)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data.get("jobs"), list):
                logger.info(
                    "Greenhouse %s validated: %d jobs", slug, len(data["jobs"])
                )
                return {
                    "active": True,
                    "career_site_url": f"https://boards.greenhouse.io/{slug}",
                }
    except Exception as exc:
        logger.debug("Greenhouse probe %s failed: %s", url, exc)
    time.sleep(1)
    return {"active": False, "career_site_url": None}


def _probe_lever_sync(slug: str) -> dict[str, Any]:
    """Probe Lever public postings API."""
    url = f"https://api.lever.co/v0/postings/{slug}"
    try:
        resp = httpx.get(url, timeout=_TIMEOUT)
        time.sleep(1)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                logger.info("Lever %s validated: %d postings", slug, len(data))
                return {
                    "active": True,
                    "career_site_url": f"https://jobs.lever.co/{slug}",
                }
    except Exception as exc:
        logger.debug("Lever probe %s failed: %s", url, exc)
    time.sleep(1)
    return {"active": False, "career_site_url": None}


def _probe(ats_type: str, ats_slug: str, location_filter: str | None) -> dict[str, Any]:
    if ats_type == "workday":
        return _probe_workday_sync(ats_slug, location_filter)
    elif ats_type == "greenhouse":
        return _probe_greenhouse_sync(ats_slug)
    elif ats_type == "lever":
        return _probe_lever_sync(ats_slug)
    else:
        logger.warning("No probe strategy for ats_type=%s", ats_type)
        return {"active": False, "career_site_url": None}


async def run(dry_run: bool, limit: int | None) -> None:
    async with AsyncSessionFactory() as db:
        query = text("""
            SELECT id, ats_type, ats_slug, location_filter, company_id,
                   career_site_url
            FROM jobs.ats_sources
            WHERE country = 'IN'
              AND is_active = false
              AND ats_slug IS NOT NULL
            ORDER BY created_at
        """ + (f" LIMIT {int(limit)}" if limit else ""))

        rows = (await db.execute(query)).fetchall()

    if not rows:
        print("No pending India sources to validate.")
        return

    print(f"\nValidating {len(rows)} India source(s)  [dry_run={dry_run}]\n")
    validated = failed = 0

    for row in rows:
        source_id, ats_type, ats_slug, location_filter, _, _ = row

        if dry_run:
            print(f"  WOULD PROBE: {ats_slug!r} ({ats_type})")
            continue

        result = _probe(ats_type, ats_slug, location_filter)
        status = "validated" if result["active"] else "validation_failed"
        icon = "✓" if result["active"] else "✗"
        print(f"  {icon} {ats_slug} ({ats_type}): {status}")

        async with AsyncSessionFactory() as db:
            await db.execute(
                text("""
                    UPDATE jobs.ats_sources
                    SET is_active         = :active,
                        last_crawl_status = :status,
                        career_site_url   = COALESCE(:url, career_site_url),
                        updated_at        = now()
                    WHERE id = :id
                """),
                {
                    "id": source_id,
                    "active": result["active"],
                    "status": status,
                    "url": result.get("career_site_url"),
                },
            )
            await db.commit()

        if result["active"]:
            validated += 1
        else:
            failed += 1

    if not dry_run:
        print(f"\nValidation complete: {validated} active | {failed} failed")


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate India ATS sources")
    ap.add_argument("--dry-run", action="store_true", help="No DB writes")
    ap.add_argument("--limit", type=int, default=None, help="Max sources to probe")
    args = ap.parse_args()
    asyncio.run(run(dry_run=args.dry_run, limit=args.limit))


if __name__ == "__main__":
    main()
