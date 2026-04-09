"""Admin API — internal-use endpoints, no authentication required.

Endpoints:
    GET /seed-status   — aggregate counts for discovery queue, ATS sources, listings.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db

router = APIRouter()


@router.get("/seed-status", response_model=None)
async def seed_status(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """Return aggregate stats for the seed pipeline and crawl infrastructure."""

    queue_rows = (
        await db.execute(
            text("""
                SELECT status, COUNT(*) AS cnt
                FROM jobs.company_discovery_queue
                GROUP BY status
                ORDER BY status
            """)
        )
    ).fetchall()
    discovery_queue: dict[str, Any] = {
        "by_status": {row.status: int(row.cnt) for row in queue_rows},
        "total": sum(int(row.cnt) for row in queue_rows),
    }

    ats_total_row = (
        await db.execute(
            text("SELECT COUNT(*) FROM jobs.ats_sources")
        )
    ).scalar()

    ats_active_row = (
        await db.execute(
            text("SELECT COUNT(*) FROM jobs.ats_sources WHERE is_active = true")
        )
    ).scalar()

    ats_inactive_row = (
        await db.execute(
            text("SELECT COUNT(*) FROM jobs.ats_sources WHERE is_active = false")
        )
    ).scalar()

    ats_type_rows = (
        await db.execute(
            text("""
                SELECT ats_type, COUNT(*) AS cnt
                FROM jobs.ats_sources
                GROUP BY ats_type
                ORDER BY ats_type
            """)
        )
    ).fetchall()

    ats_sources: dict[str, Any] = {
        "total": int(ats_total_row or 0),
        "active": int(ats_active_row or 0),
        "inactive": int(ats_inactive_row or 0),
        "by_type": {row.ats_type: int(row.cnt) for row in ats_type_rows},
    }

    last_crawl_row = (
        await db.execute(
            text("SELECT MAX(last_seen_at) FROM jobs.listings")
        )
    ).scalar()

    total_active_row = (
        await db.execute(
            text("SELECT COUNT(*) FROM jobs.listings WHERE is_active = true")
        )
    ).scalar()

    return {
        "discovery_queue": discovery_queue,
        "ats_sources": ats_sources,
        "last_crawl_run": last_crawl_row.isoformat() if last_crawl_row else None,
        "total_active_listings": int(total_active_row or 0),
    }
