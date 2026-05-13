#!/usr/bin/env python3
"""
Re-probes all existing Workday ats_sources using the new _probe_workday logic.
Updates crawl_config and crawl_url, setting is_active=true if successful.

Usage:
    python scripts/reprobe_workday.py [--dry-run] [--limit N]
"""

import argparse
import asyncio
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

logger = logging.getLogger("reprobe_workday")

async def main(dry_run: bool = False, limit: int | None = None):
    from app.db import AsyncSessionFactory
    from app.models import AtsSource
    from sqlalchemy import select, update
    from app.discovery.ats_prober import ATSProber
    import httpx
    
    prober = ATSProber()
    
    async with AsyncSessionFactory() as session:
        # Get all workday sources
        result = await session.execute(
            select(AtsSource).where(AtsSource.ats_type == "workday")
        )
        sources = result.scalars().all()
        
        total = len(sources)
        if limit:
            sources = sources[:limit]
            logger.info(f"Found {total} Workday sources, processing first {limit} (limit applied).")
        else:
            logger.info(f"Found {total} Workday sources to reprobe.")
        
        if dry_run:
            logger.info("DRY RUN MODE - No database changes will be made.")
        
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=False) as client:
            for idx, source in enumerate(sources, 1):
                logger.info(f"[{idx}/{len(sources)}] Probing {source.ats_slug}...")
                sem = asyncio.Semaphore(5)
                # Call internal _probe_workday to force re-evaluation of just workday variants
                match = await prober._probe_workday(client, source.ats_slug, sem)
                
                if match:
                    logger.info(f"  ✓ SUCCESS {source.ats_slug} -> {match.get('crawl_config')}")
                    if not dry_run:
                        # Update the record
                        source.crawl_url = match["crawl_url"]
                        source.crawl_config = match.get("crawl_config", {})
                        source.is_active = True
                        source.last_crawl_status = None
                        source.consecutive_failures = 0
                        await session.commit()
                else:
                    logger.warning(f"  ✗ FAILED {source.ats_slug} - Could not resolve career site or API")
                    if not dry_run:
                        source.is_active = False
                        await session.commit()

        logger.info("Reprobe complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reprobe Workday ATS sources")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--limit", type=int, help="Limit number of sources to process")
    args = parser.parse_args()
    
    asyncio.run(main(dry_run=args.dry_run, limit=args.limit))
