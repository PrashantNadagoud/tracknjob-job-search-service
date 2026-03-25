import re
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.config import get_settings
from app.db import get_db
from app.models import HiddenJob, Listing, SavedSearch
from app.schemas.jobs import (
    HideJobRequest,
    JobListingDetail,
    JobListingItem,
    JobSearchResponse,
    JobSourceItem,
    JobSourcesResponse,
    SavedSearchCreate,
    SavedSearchListResponse,
    SavedSearchResponse,
)


class CrawlTriggerRequest(BaseModel):
    country: str = "ALL"  # "US", "IN", or "ALL"

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
    country: str = Query(default="US", description="Country filter: US, IN, or ALL"),
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
            country=country,
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
    country: str,
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

    # Country filter: "US" or "IN" → exact match; "ALL" → no filter
    country_upper = country.upper()
    if country_upper in ("US", "IN"):
        stmt = stmt.where(Listing.country == country_upper)

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


def _slugify(label: str) -> str:
    """Lowercase, collapse non-alphanumeric runs to hyphens, strip edge hyphens."""
    slug = re.sub(r"[^a-z0-9]+", "-", label.strip().lower())
    return slug.strip("-")


@router.get("/sources", response_model=JobSourcesResponse, summary="List crawled job sources")
async def get_job_sources(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> JobSourcesResponse:
    try:
        stmt = (
            select(
                Listing.source_label,
                func.count(Listing.id).label("job_count"),
                func.max(Listing.crawled_at).label("last_crawled"),
            )
            .where(Listing.is_active == True)  # noqa: E712
            .where(Listing.source_label.is_not(None))
            .group_by(Listing.source_label)
            .order_by(Listing.source_label)
        )
        rows = (await db.execute(stmt)).all()
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")

    return JobSourcesResponse(
        sources=[
            JobSourceItem(
                id=_slugify(row.source_label),
                label=row.source_label,
                job_count=row.job_count,
                last_crawled=row.last_crawled,
            )
            for row in rows
        ]
    )


@router.post(
    "/saved-searches",
    response_model=SavedSearchResponse,
    status_code=201,
    summary="Save a search filter set",
)
async def create_saved_search(
    body: SavedSearchCreate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> SavedSearchResponse:
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="name must not be empty")
    if not body.filters:
        raise HTTPException(status_code=400, detail="filters must not be empty")

    try:
        user_uuid = uuid.UUID(current_user["sub"])
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid user id in token")

    try:
        record = SavedSearch(
            user_id=user_uuid,
            name=body.name.strip(),
            filters=body.filters,
            alert_email=body.alert_email,
            user_email=body.user_email or None,
        )
        db.add(record)
        await db.flush()
        await db.refresh(record)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")

    return SavedSearchResponse.model_validate(record)


@router.get(
    "/saved-searches",
    response_model=SavedSearchListResponse,
    summary="List saved searches for current user",
)
async def list_saved_searches(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> SavedSearchListResponse:
    try:
        user_uuid = uuid.UUID(current_user["sub"])
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid user id in token")

    try:
        stmt = (
            select(SavedSearch)
            .where(SavedSearch.user_id == user_uuid)
            .order_by(SavedSearch.created_at.desc())
        )
        rows = (await db.execute(stmt)).scalars().all()
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")

    return SavedSearchListResponse(
        total=len(rows),
        results=[SavedSearchResponse.model_validate(r) for r in rows],
    )


@router.post("/hidden", status_code=204, summary="Hide a job listing for the current user")
async def hide_job(
    body: HideJobRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> Response:
    try:
        user_uuid = uuid.UUID(current_user["sub"])
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid user id in token")

    try:
        listing = await db.get(Listing, body.job_id)
        if listing is None:
            raise HTTPException(status_code=404, detail="Job not found")

        record = HiddenJob(user_id=user_uuid, job_id=body.job_id)
        db.add(record)
        await db.flush()
    except HTTPException:
        raise
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Job already hidden")
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")

    return Response(status_code=204)


@router.post(
    "/crawl/trigger",
    summary="Trigger a crawl — admin only",
)
async def trigger_crawl(
    body: CrawlTriggerRequest = CrawlTriggerRequest(),
    current_user: dict = Depends(get_current_user),
) -> dict:
    settings = get_settings()
    if not settings.ADMIN_USER_ID or current_user["sub"] != settings.ADMIN_USER_ID:
        raise HTTPException(status_code=403, detail="Admin access required")

    country = body.country.upper()
    if country not in ("US", "IN", "ALL"):
        raise HTTPException(status_code=422, detail="country must be 'US', 'IN', or 'ALL'")

    from app.crawler.tasks import crawl_all_companies  # lazy — avoids Celery init at startup

    result = crawl_all_companies.delay(country)
    return {"status": "crawl started", "task_id": result.id}


@router.post(
    "/maintenance/trigger-alerts",
    summary="Manually trigger job alert emails — admin only",
)
async def trigger_alerts(
    current_user: dict = Depends(get_current_user),
) -> dict:
    settings = get_settings()
    if not settings.ADMIN_USER_ID or current_user["sub"] != settings.ADMIN_USER_ID:
        raise HTTPException(status_code=403, detail="Admin access required")

    from app.crawler.tasks import send_job_alerts  # lazy import

    result = send_job_alerts.delay()
    return {"status": "alerts triggered", "task_id": result.id}


@router.post(
    "/maintenance/deactivate-stale",
    summary="Manually trigger stale job deactivation — admin only",
)
async def trigger_deactivate_stale(
    current_user: dict = Depends(get_current_user),
) -> dict:
    settings = get_settings()
    if not settings.ADMIN_USER_ID or current_user["sub"] != settings.ADMIN_USER_ID:
        raise HTTPException(status_code=403, detail="Admin access required")

    from app.crawler.tasks import deactivate_stale_jobs  # lazy import

    result = deactivate_stale_jobs.delay()
    return {"status": "deactivation started", "task_id": result.id}


@router.get("/{job_id}", response_model=JobListingDetail, summary="Get job listing detail")
async def get_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> JobListingDetail:
    try:
        row = await db.get(Listing, job_id)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")

    if row is None or not row.is_active:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobListingDetail.model_validate(row)
