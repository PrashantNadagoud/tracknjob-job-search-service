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

    logger.info("run_yc_seed started: market=%s dry_run=%s", market, dry_run)

    async def _run() -> dict:
        async with AsyncSessionFactory() as session:
            orchestrator = SeedOrchestrator(db_session=session, batch_size=50)
            return await orchestrator.run(market=market, dry_run=dry_run)

    result = asyncio.run(_run())
    logger.info("run_yc_seed finished: %s", result)
    return result
