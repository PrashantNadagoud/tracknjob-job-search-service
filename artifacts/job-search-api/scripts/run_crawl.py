"""Run ATS crawlers for a sample of sources.

Usage:
    # Dry run (no DB writes)
    python scripts/run_crawl.py --ats-type workday --limit 5 --dry-run

    # Live crawl (persists listings via CrawlDispatcher)
    python scripts/run_crawl.py --ats-type workday --limit 82 --batch-size 5 --delay 2

Flags:
    --ats-type    ATS type to crawl (e.g., workday, greenhouse, lever)
    --limit       Max number of ats_sources to crawl (default: 5)
    --batch-size  Number of sources processed concurrently per batch (default: 5)
    --delay       Seconds to sleep between batches (default: 2.0)
    --dry-run     Only print results; do not persist listings to DB
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Make parent dir importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from app.crawler.dispatcher import CRAWLER_MAP, CrawlDispatcher
from app.crawler.tasks import _upsert_ats_jobs
from app.db import AsyncSessionFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)-30s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("run_crawl")


async def _crawl_one_dry(crawler, source_id, ats_slug, crawl_url, company_name):
    effective_slug = crawl_url or ats_slug
    try:
        jobs = await crawler.crawl(effective_slug, source_id)
    except Exception as exc:
        print(f"  [{company_name}] ERROR: {type(exc).__name__}: {exc}")
        return [], 0
    if jobs:
        j0 = jobs[0]
        title = j0.get("title")
        loc = j0.get("location")
        apply_url = j0.get("source_url") or j0.get("apply_url")
        print(f"  [{company_name}] {len(jobs)} jobs. sample: {title!r} | {loc!r} | {apply_url}")
    else:
        print(f"  [{company_name}] 0 jobs")
    return jobs, len(jobs)


async def _crawl_one_live(dispatcher, source_id, company_name):
    async with AsyncSessionFactory() as session:
        try:
            jobs = await dispatcher.dispatch(source_id, session)
        except Exception as exc:
            print(f"  [{company_name}] DISPATCH ERROR: {type(exc).__name__}: {exc}")
            return 0, 0
        if jobs:
            new_ids = await _upsert_ats_jobs(jobs, AsyncSessionFactory)
        else:
            new_ids = []
        await session.commit()
        print(f"  [{company_name}] {len(jobs)} jobs fetched, {len(new_ids)} newly inserted")
        return len(jobs), len(new_ids)


async def run(ats_type: str, limit: int, batch_size: int, delay: float, dry_run: bool) -> None:
    crawler = CRAWLER_MAP.get(ats_type)
    if crawler is None:
        raise SystemExit(f"Unknown ats_type={ats_type}. Known: {list(CRAWLER_MAP.keys())}")

    async with AsyncSessionFactory() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT s.id, s.ats_slug, s.crawl_url, c.name AS company_name
                    FROM jobs.ats_sources s
                    JOIN jobs.companies c ON c.id = s.company_id
                    WHERE s.ats_type = :ats_type
                      AND s.is_active = true
                    ORDER BY s.last_crawled_at NULLS FIRST, s.created_at DESC
                    LIMIT :lim
                    """
                ),
                {"ats_type": ats_type, "lim": limit},
            )
        ).fetchall()

    if not rows:
        print(f"No active ats_sources found for ats_type={ats_type}")
        return

    print("=" * 70)
    print(f"Running crawl for {len(rows)} {ats_type} sources")
    print(f"  dry_run={dry_run} batch_size={batch_size} delay={delay}s")
    print("=" * 70)

    total_jobs = 0
    total_new = 0
    companies_with_jobs = 0
    dispatcher = CrawlDispatcher() if not dry_run else None

    # Process in batches of batch_size with delay between batches
    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start: batch_start + batch_size]
        print(f"\n--- batch {batch_start // batch_size + 1} ({batch_start + 1}–{batch_start + len(batch)} of {len(rows)}) ---")

        if dry_run:
            tasks = [
                _crawl_one_dry(crawler, sid, slug, crawl_url, cname)
                for (sid, slug, crawl_url, cname) in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, tuple):
                    _, n = res
                    total_jobs += n
                    if n > 0:
                        companies_with_jobs += 1
        else:
            tasks = [
                _crawl_one_live(dispatcher, sid, cname)
                for (sid, _slug, _crawl_url, cname) in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, tuple):
                    n, new_n = res
                    total_jobs += n
                    total_new += new_n
                    if n > 0:
                        companies_with_jobs += 1

        # Sleep between batches (skip after last)
        if batch_start + batch_size < len(rows) and delay > 0:
            await asyncio.sleep(delay)

    print("\n" + "=" * 70)
    print("Crawl Summary")
    print("=" * 70)
    print(f"  total_sources       : {len(rows)}")
    print(f"  companies_with_jobs : {companies_with_jobs}")
    print(f"  total_jobs_fetched  : {total_jobs}")
    if not dry_run:
        print(f"  newly_inserted_rows : {total_new}")
    print("=" * 70)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ats-type", required=True)
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--batch-size", type=int, default=5)
    ap.add_argument("--delay", type=float, default=2.0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    asyncio.run(run(args.ats_type, args.limit, args.batch_size, args.delay, args.dry_run))


if __name__ == "__main__":
    main()
