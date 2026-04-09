#!/usr/bin/env python3
"""CLI to run the YC discovery & ATS seed pipeline manually.

Usage:
    python scripts/run_yc_seed.py [--dry-run] [--market US] [--limit 100]

Options:
    --dry-run       Probe and log but do not write to the database.
    --market STR    Market tag to attach to inserted rows (default: US).
    --limit N       Process at most N companies (applied after YC fetch, before probing).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logger = logging.getLogger("run_yc_seed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the YC seed pipeline.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Probe without writing to the database.",
    )
    parser.add_argument(
        "--market",
        default="US",
        help="Market tag for inserted rows (default: US).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of companies processed.",
    )
    return parser.parse_args()


async def run(market: str, dry_run: bool, limit: int | None) -> None:
    from app.db import AsyncSessionFactory
    from app.discovery.seed_orchestrator import SeedOrchestrator
    from app.discovery.yc_scraper import YCScraper

    if limit is not None:
        scraper = YCScraper()
        companies = await scraper.fetch()
        companies = companies[:limit]

        async with AsyncSessionFactory() as session:
            orchestrator = SeedOrchestrator(db_session=session, batch_size=50)
            existing = await orchestrator._fetch_existing_websites()
            candidates = [
                c for c in companies
                if c.get("website") and c["website"] not in existing
            ]

            logger.info(
                "--limit %d applied: %d companies after dedup",
                limit,
                len(candidates),
            )

            from app.discovery.ats_prober import ATSProber
            import asyncio as _asyncio

            prober = ATSProber()
            counts = {
                "total": len(companies),
                "skipped": len(companies) - len(candidates),
                "probed": 0,
                "matched": 0,
                "rejected": 0,
            }

            for idx, company in enumerate(candidates):
                counts["probed"] += 1
                match = await prober.probe(company)
                if match:
                    counts["matched"] += 1
                    if not dry_run:
                        await orchestrator._insert_matched(company, match, market)
                else:
                    counts["rejected"] += 1
                    if not dry_run:
                        await orchestrator._insert_rejected(company, market)

                if (idx + 1) % 50 == 0:
                    logger.info("Progress: %d / %d", idx + 1, len(candidates))

                if not dry_run and (idx + 1) % 50 == 0:
                    await session.commit()

            if not dry_run:
                await session.commit()
    else:
        async with AsyncSessionFactory() as session:
            orchestrator = SeedOrchestrator(db_session=session, batch_size=50)
            counts = await orchestrator.run(market=market, dry_run=dry_run)

    print()
    print("=" * 50)
    print("YC Seed Pipeline Results")
    print("=" * 50)
    print(f"  market   : {market}")
    print(f"  dry_run  : {dry_run}")
    print(f"  total    : {counts['total']}")
    print(f"  skipped  : {counts['skipped']}  (already known)")
    print(f"  probed   : {counts['probed']}")
    print(f"  matched  : {counts['matched']}  (ATS found)")
    print(f"  rejected : {counts['rejected']}  (no ATS detected)")
    print("=" * 50)


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(market=args.market, dry_run=args.dry_run, limit=args.limit))
