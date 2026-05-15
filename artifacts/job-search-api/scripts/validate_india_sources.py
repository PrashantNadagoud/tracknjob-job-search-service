#!/usr/bin/env python3
"""Validate India ATS sources by probing each ATS API.

For each jobs.ats_sources row where country='IN', is_active=false,
and ats_slug IS NOT NULL: send a probe request to the appropriate ATS
endpoint and mark the source active/inactive based on the response.

On a successful Workday probe the confirmed instance + career_site_name
are written back into crawl_config so WorkdayCrawler can use the CXS
API immediately without guessing.

CSRF handling:
  Some Workday instances enforce CSRF on the CXS endpoint.  The prober
  first tries a POST without any CSRF token.  If the response is 403 or
  422, it does a GET to the job-board HTML page to harvest the session
  cookies and CSRF token (CALYPSO_CSRF_TOKEN or similar), then retries
  the POST once with those credentials and an X-CSRF-Token header.

Usage:
    python scripts/validate_india_sources.py             # validate all
    python scripts/validate_india_sources.py --limit 10  # first 10 only
    python scripts/validate_india_sources.py --dry-run   # no DB writes
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
_WORKDAY_INSTANCES = ["wd1", "wd3", "wd5", "wd12"]
_WORKDAY_SITE_NAMES = ["External", "Careers", "ExternalCareers"]
_TIMEOUT = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=5.0)

_CSRF_COOKIE_NAMES = ("CALYPSO_CSRF_TOKEN", "csrf-token", "wd-csrf-token", "CSRF-TOKEN")
_CSRF_HEADER_NAMES = ("x-csrf-token", "X-CSRF-Token")

# Parses instance + site_name out of a Workday career site URL.
_WORKDAY_URL_RE = re.compile(
    r"https://[\w-]+\.(wd\d+)\.myworkdayjobs\.com/([^/?#\s]+)"
)


def _parse_workday_url(career_site_url: str | None) -> tuple[str, str] | None:
    """Return (instance, career_site_name) parsed from a Workday URL, or None."""
    if not career_site_url:
        return None
    m = _WORKDAY_URL_RE.search(career_site_url)
    if not m:
        return None
    return m.group(1), m.group(2)


def _fetch_csrf_sync(
    client: httpx.Client,
    slug: str,
    instance: str,
    site_name: str,
) -> tuple[str | None, dict]:
    """GET the Workday job-board HTML page to harvest session cookies + CSRF token.

    Returns (csrf_token_or_None, cookies_dict).  Never raises.
    """
    page_url = (
        f"https://{slug}.{instance}.myworkdayjobs.com/{site_name}/jobs"
    )
    try:
        resp = client.get(
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
                logger.info(
                    "CSRF token found in cookie %r for %s/%s/%s",
                    name, slug, instance, site_name,
                )
                return token, cookies
        for name in _CSRF_HEADER_NAMES:
            token = resp.headers.get(name)
            if token:
                logger.info(
                    "CSRF token found in header %r for %s/%s/%s",
                    name, slug, instance, site_name,
                )
                return token, cookies
        logger.debug(
            "No CSRF token in GET response for %s/%s/%s", slug, instance, site_name
        )
        return None, cookies
    except Exception as exc:
        logger.debug(
            "CSRF GET failed for %s/%s/%s: %s", slug, instance, site_name, exc
        )
        return None, {}


def _try_workday_cxs(
    slug: str,
    instance: str,
    site_name: str,
    location_filter: str | None,
) -> dict[str, Any] | None:
    """POST one CXS probe with automatic CSRF retry.

    Flow:
      1. POST without CSRF token (fast path for non-CSRF-protected sites).
      2. On 403 or 422: GET the job-board page to fetch session cookies +
         CSRF token, then retry the POST once with those credentials.
      3. When a location_filter is supplied, we try it first; if the server
         returns 422 (plain-text location not a valid Workday facet ID), we
         retry the same endpoint without any filter.  A 200 response that
         contains a ``jobPostings`` key is sufficient to confirm the endpoint
         is live — the crawler applies the location filter at crawl time.

    Returns a result dict on success, or None on any failure.
    """
    url = (
        f"https://{slug}.{instance}.myworkdayjobs.com"
        f"/wday/cxs/{slug}/{site_name}/jobs"
    )
    referer = f"https://{slug}.{instance}.myworkdayjobs.com/{site_name}/jobs"

    base_headers = {
        "User-Agent": _BROWSER_UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Referer": referer,
    }

    bodies_to_try: list[dict[str, Any]] = []
    if location_filter:
        bodies_to_try.append(
            {"limit": 5, "offset": 0, "appliedFacets": {"Location": [location_filter]}}
        )
    bodies_to_try.append({"limit": 5, "offset": 0})

    with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:

        # ── Pass 1: POST without CSRF ──────────────────────────────────────
        last_status: int | None = None
        for body in bodies_to_try:
            try:
                resp = client.post(url, json=body, headers=base_headers)
            except Exception as exc:
                logger.debug(
                    "Workday CXS probe %s/%s/%s failed: %s", slug, instance, site_name, exc
                )
                time.sleep(1)
                return None

            if resp.status_code == 200:
                data = resp.json()
                if data.get("jobPostings") is not None:
                    return _success_result(slug, instance, site_name, len(data["jobPostings"]))
            last_status = resp.status_code
            if resp.status_code == 404:
                time.sleep(1)
                return None
            # 422 with location filter → fall through to retry without filter
            # 403 → fall through; will trigger CSRF retry below

        # ── Pass 2: CSRF retry (only if last attempt was 403 or 422) ──────
        if last_status not in (403, 422):
            time.sleep(1)
            return None

        csrf_token, csrf_cookies = _fetch_csrf_sync(client, slug, instance, site_name)
        csrf_headers = dict(base_headers)
        if csrf_token:
            csrf_headers["X-CSRF-Token"] = csrf_token
            logger.info(
                "Workday CSRF retry: %s/%s/%s with token", slug, instance, site_name
            )
        else:
            logger.debug(
                "Workday CSRF retry: %s/%s/%s without token (hoping cookies help)",
                slug, instance, site_name,
            )

        for body in bodies_to_try:
            try:
                resp = client.post(
                    url, json=body, headers=csrf_headers, cookies=csrf_cookies
                )
            except Exception as exc:
                logger.debug(
                    "Workday CSRF retry %s/%s/%s failed: %s", slug, instance, site_name, exc
                )
                break

            if resp.status_code == 200:
                data = resp.json()
                if data.get("jobPostings") is not None:
                    return _success_result(slug, instance, site_name, len(data["jobPostings"]))
            if resp.status_code == 404:
                break
            # 422/403 still → next body

    time.sleep(1)
    return None


def _success_result(
    slug: str, instance: str, site_name: str, n_jobs: int
) -> dict[str, Any]:
    career_url = f"https://{slug}.{instance}.myworkdayjobs.com/{site_name}"
    logger.info(
        "Workday %s validated: %s/%s (%d postings)", slug, instance, site_name, n_jobs
    )
    time.sleep(1)
    return {
        "active": True,
        "career_site_url": career_url,
        "crawl_config": {"instance": instance, "career_site_name": site_name},
    }


def _probe_workday_sync(
    slug: str,
    location_filter: str | None,
    career_site_url: str | None,
) -> dict[str, Any]:
    """Probe Workday: try the source's own career_site_url first, then brute-force."""
    # 1. Try instance/site from the seeded career_site_url (most likely to work)
    parsed = _parse_workday_url(career_site_url)
    if parsed:
        instance, site_name = parsed
        result = _try_workday_cxs(slug, instance, site_name, location_filter)
        if result:
            return result

    # 2. Brute-force remaining combinations
    for instance in _WORKDAY_INSTANCES:
        for site_name in _WORKDAY_SITE_NAMES:
            if parsed and (instance, site_name) == parsed:
                continue  # already tried above
            result = _try_workday_cxs(slug, instance, site_name, location_filter)
            if result:
                return result

    return {"active": False, "career_site_url": None, "crawl_config": None}


def _probe_greenhouse_sync(slug: str) -> dict[str, Any]:
    """Probe Greenhouse public jobs board API."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    try:
        resp = httpx.get(url, timeout=_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data.get("jobs"), list):
                logger.info(
                    "Greenhouse %s validated: %d jobs", slug, len(data["jobs"])
                )
                time.sleep(1)
                return {
                    "active": True,
                    "career_site_url": f"https://boards.greenhouse.io/{slug}",
                    "crawl_config": None,
                }
    except Exception as exc:
        logger.debug("Greenhouse probe %s failed: %s", slug, exc)
    time.sleep(1)
    return {"active": False, "career_site_url": None, "crawl_config": None}


def _probe_lever_sync(slug: str) -> dict[str, Any]:
    """Probe Lever public postings API."""
    url = f"https://api.lever.co/v0/postings/{slug}"
    try:
        resp = httpx.get(url, timeout=_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                logger.info("Lever %s validated: %d postings", slug, len(data))
                time.sleep(1)
                return {
                    "active": True,
                    "career_site_url": f"https://jobs.lever.co/{slug}",
                    "crawl_config": None,
                }
    except Exception as exc:
        logger.debug("Lever probe %s failed: %s", slug, exc)
    time.sleep(1)
    return {"active": False, "career_site_url": None, "crawl_config": None}


def _probe(
    ats_type: str,
    ats_slug: str,
    location_filter: str | None,
    career_site_url: str | None,
) -> dict[str, Any]:
    if ats_type == "workday":
        return _probe_workday_sync(ats_slug, location_filter, career_site_url)
    elif ats_type == "greenhouse":
        return _probe_greenhouse_sync(ats_slug)
    elif ats_type == "lever":
        return _probe_lever_sync(ats_slug)
    else:
        logger.warning("No probe strategy for ats_type=%s", ats_type)
        return {"active": False, "career_site_url": None, "crawl_config": None}


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
        source_id, ats_type, ats_slug, location_filter, _, career_site_url = row

        if dry_run:
            print(f"  WOULD PROBE: {ats_slug!r} ({ats_type})")
            continue

        result = _probe(ats_type, ats_slug, location_filter, career_site_url)
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
                        crawl_config      = CASE
                                               WHEN CAST(:crawl_config AS text) IS NOT NULL
                                               THEN CAST(:crawl_config AS jsonb)
                                               ELSE crawl_config
                                           END,
                        updated_at        = now()
                    WHERE id = :id
                """),
                {
                    "id": source_id,
                    "active": result["active"],
                    "status": status,
                    "url": result.get("career_site_url"),
                    "crawl_config": (
                        json.dumps(result["crawl_config"])
                        if result.get("crawl_config")
                        else None
                    ),
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
