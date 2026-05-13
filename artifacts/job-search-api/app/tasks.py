"""Top-level Celery tasks for TrackNJob (non-crawler, non-enrichment).

Current tasks:
    run_yc_seed — manually-triggered YC discovery & ATS seed pipeline.
"""

from __future__ import annotations

import asyncio
import logging

from app.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, max_retries=0, name="app.tasks.run_yc_seed")
def run_yc_seed(
    self,
    market: str = "US",
    dry_run: bool = False,
) -> dict:
    """Run the full YC discovery & ATS seed pipeline synchronously.

    Args:
        market:  Market tag to attach to inserted ats_sources / queue rows.
        dry_run: If True, probe and log but skip all DB writes.

    Returns:
        Summary dict: {total, skipped, probed, matched, rejected}
    """
    from app.db import AsyncSessionFactory
    from app.discovery.seed_orchestrator import SeedOrchestrator
    from app.discovery.yc_scraper import YCScraper

    logger.info("run_yc_seed started: market=%s dry_run=%s", market, dry_run)

    async def _run() -> dict:
        async with AsyncSessionFactory() as session:
            scraper = YCScraper()
            orchestrator = SeedOrchestrator(db_session=session, scraper=scraper, batch_size=50)
            return await orchestrator.run(market=market, dry_run=dry_run)

    result = asyncio.run(_run())
    logger.info("run_yc_seed finished: %s", result)
    return result


@celery_app.task(bind=True, max_retries=0)
def run_fortune500_seed(self, dry_run: bool = False, rank_limit: int | None = None) -> dict:
    """
    One-off task to seed company_discovery_queue from Fortune 500 list.
    Manually triggered only — not on beat schedule.
    """
    from app.db import AsyncSessionFactory
    from app.discovery.seed_orchestrator import SeedOrchestrator
    from app.discovery.fortune500_scraper import Fortune500Scraper

    logger.info("run_fortune500_seed started: dry_run=%s rank_limit=%s", dry_run, rank_limit)

    async def _run() -> dict:
        async with AsyncSessionFactory() as session:
            scraper = Fortune500Scraper()
            if rank_limit is not None:
                scraper.rank_limit = rank_limit
            orchestrator = SeedOrchestrator(db_session=session, scraper=scraper, batch_size=50)
            return await orchestrator.run(market="US", dry_run=dry_run)

    result = asyncio.run(_run())
    logger.info("run_fortune500_seed finished: %s", result)
    return result
