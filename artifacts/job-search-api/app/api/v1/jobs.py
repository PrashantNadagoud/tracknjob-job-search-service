import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.db import get_db
from app.models import HiddenJob, Listing
from app.schemas.jobs import JobListingItem, JobSearchResponse

router = APIRouter()


class PostedFilter(str, Enum):
    h24 = "24h"
    d3 = "3d"
    d7 = "7d"
    d30 = "30d"
    any = "any"


_POSTED_CUTOFFS: dict[str, timedelta] = {
    "24h": timedelta(days=1),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}


@router.get("/search", response_model=JobSearchResponse, summary="Search job listings")
async def search_jobs(
    q: str | None = Query(default=None, description="Full-text search: title, company, location"),
    location: str | None = Query(default=None, description="Filter by location (partial match)"),
    remote: bool = Query(default=False, description="Return remote-only jobs"),
    source: str | None = Query(default=None, description="Filter by source_label"),
    company: str | None = Query(default=None, description="Filter by company name (partial match)"),
    posted: PostedFilter = Query(default=PostedFilter.any, description="Filter by posted_at recency"),
    page: int = Query(default=1, ge=1, description="Page number"),
    limit: int = Query(default=20, ge=1, le=50, description="Results per page (max 50)"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> JobSearchResponse:
    try:
        return await _execute_search(
            db=db,
            user_id_str=current_user["sub"],
            q=q,
            location=location,
            remote=remote,
            source=source,
            company=company,
            posted=posted,
            page=page,
            limit=limit,
        )
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")


async def _execute_search(
    db: AsyncSession,
    user_id_str: str,
    q: str | None,
    location: str | None,
    remote: bool,
    source: str | None,
    company: str | None,
    posted: PostedFilter,
    page: int,
    limit: int,
) -> JobSearchResponse:
    stmt = select(Listing).where(Listing.is_active == True)  # noqa: E712

    # Exclude jobs hidden by current user
    try:
        user_uuid = uuid.UUID(user_id_str)
        hidden_subq = select(HiddenJob.job_id).where(HiddenJob.user_id == user_uuid)
        stmt = stmt.where(~Listing.id.in_(hidden_subq))
    except ValueError:
        pass

    # Full-text search — uses the existing idx_jobs_fts GIN index
    if q:
        stmt = stmt.where(
            text(
                "to_tsvector('english', title || ' ' || company || ' ' || COALESCE(location,''))"
                " @@ plainto_tsquery('english', :fts_q)"
            ).bindparams(fts_q=q)
        )

    # Location partial match
    if location:
        stmt = stmt.where(Listing.location.ilike(f"%{location}%"))

    # Remote-only filter
    if remote:
        stmt = stmt.where(Listing.remote == True)  # noqa: E712

    # Source label exact match
    if source:
        stmt = stmt.where(Listing.source_label == source)

    # Company partial match
    if company:
        stmt = stmt.where(Listing.company.ilike(f"%{company}%"))

    # posted_at recency filter
    if posted != PostedFilter.any:
        cutoff = datetime.now(timezone.utc) - _POSTED_CUTOFFS[posted.value]
        stmt = stmt.where(Listing.posted_at >= cutoff)

    # Total count via subquery (before pagination)
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total: int = (await db.scalar(count_stmt)) or 0

    # Apply ordering and pagination
    offset = (page - 1) * limit
    paginated = (
        stmt.order_by(Listing.posted_at.desc().nulls_last())
        .offset(offset)
        .limit(limit)
    )

    rows = (await db.execute(paginated)).scalars().all()

    return JobSearchResponse(
        total=total,
        page=page,
        limit=limit,
        results=[JobListingItem.model_validate(row) for row in rows],
    )
