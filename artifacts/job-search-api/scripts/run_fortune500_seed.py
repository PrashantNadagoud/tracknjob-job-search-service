#!/usr/bin/env python3
"""
Usage:
    python scripts/run_fortune500_seed.py                   # live run
    python scripts/run_fortune500_seed.py --dry-run         # preview only
    python scripts/run_fortune500_seed.py --limit 50        # first 50 only
    python scripts/run_fortune500_seed.py --rank-limit 100  # top 100 by rank
"""
import asyncio
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--market', default='US')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--rank-limit', type=int, default=None,
                        help='Only process companies ranked <= this value')
    parser.add_argument('--company', type=str, default=None,
                        help='Only process a specific company by name (e.g. nvidia)')
    args = parser.parse_args()

    from app.db import AsyncSessionFactory
    from app.discovery.fortune500_scraper import Fortune500Scraper
    from app.discovery.seed_orchestrator import SeedOrchestrator

    async with AsyncSessionFactory() as session:
        scraper = Fortune500Scraper()
        if args.rank_limit:
            scraper.rank_limit = args.rank_limit
        orchestrator = SeedOrchestrator(session, scraper=scraper)
        
        # Patch the scraper's fetch to only return the specified company if requested
        if args.company:
            original_fetch = scraper.fetch
            async def filtered_fetch():
                companies = await original_fetch()
                return [c for c in companies if c['company_name'].lower() == args.company.lower()]
            scraper.fetch = filtered_fetch
            # When testing a specific company, we want to force probe it even if it's known
            async def _empty_websites(): return set()
            orchestrator._fetch_existing_websites = _empty_websites
            
        counts = await orchestrator.run(market=args.market, dry_run=args.dry_run, limit=args.limit)

    print()
    print("=" * 50)
    print("Fortune 500 Seed Pipeline Results")
    print("=" * 50)
    print(f"  market   : {args.market}")
    print(f"  dry_run  : {args.dry_run}")
    if args.limit is not None:
        print(f"  limit    : {args.limit}")
    if args.rank_limit is not None:
        print(f"  rank_limit: {args.rank_limit}")
    print(f"  total    : {counts['total']}")
    print(f"  skipped  : {counts['skipped']}  (already known)")
    print(f"  probed   : {counts['probed']}")
    print(f"  matched  : {counts['matched']}  (ATS found)")
    print(f"  rejected : {counts['rejected']}  (no ATS detected)")
    print("=" * 50)

if __name__ == '__main__':
    asyncio.run(main())
