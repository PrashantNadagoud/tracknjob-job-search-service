import logging
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
from app.models import Company, HiddenJob, JobPreference, Listing, SavedSearch
from app.schemas.companies import CompanySummary
from app.schemas.jobs import (
    HideJobRequest,
    JobListingDetail,
    JobListingItem,
    JobPreferencesCreate,
    JobPreferencesResponse,
    JobSearchResponse,
    JobSourceItem,
    JobSourcesResponse,
    SavedSearchCreate,
    SavedSearchListResponse,
    SavedSearchResponse,
)
from app.scoring import compute_match_score, get_match_label

logger = logging.getLogger(__name__)


class CrawlTriggerRequest(BaseModel):
    country: str = "ALL"


router = APIRouter()


class PostedFilter(str, Enum):
    h24 = "24h"
    d3 = "3d"
    d7 = "7d"
    d30 = "30d"
    any = "any"


class SortBy(str, Enum):
    posted_at = "posted_at"
    match_score = "match_score"


_POSTED_CUTOFFS: dict[str, timedelta] = {
    "24h": timedelta(days=1),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}


def _build_company_summary(
    company: Company | None,
    listing_salary_range: str | None,
) -> CompanySummary | None:
    if company is None:
        return None

    kwargs: dict = {}

    company_type = company.company_type or "unknown"
    kwargs["company_type"] = company_type

    if company_type == "public":
        if company.stock_ticker:
            kwargs["stock_ticker"] = company.stock_ticker
        if company.stock_exchange:
            kwargs["stock_exchange"] = company.stock_exchange
    else:
        if company.last_funding_type:
            kwargs["funding_stage"] = company.last_funding_type
        if company.funding_total_usd:
            kwargs["funding_total_usd"] = company.funding_total_usd

    if company.num_employees_range:
        kwargs["employee_range"] = company.num_employees_range
    if company.culture_score:
        kwargs["culture_score"] = company.culture_score
    if company.remote_policy:
        kwargs["remote_policy"] = company.remote_policy
    if company.perks:
        kwargs["perks"] = company.perks

    if listing_salary_range:
        kwargs["salary_source"] = "company_listed"
    elif company.salary_min_usd is not None and company.salary_max_usd is not None:
        kwargs["salary_min_usd"] = company.salary_min_usd
        kwargs["salary_max_usd"] = company.salary_max_usd
        if company.salary_source:
            kwargs["salary_source"] = company.salary_source

    if len(kwargs) <= 1 and list(kwargs.keys()) == ["company_type"]:
        return None

    return CompanySummary(**kwargs)


@router.get("/search", response_model=JobSearchResponse, summary="Search job listings")
async def search_jobs(
    q: str | None = Query(default=None, description="Full-text search: title, company, location"),
    location: str | None = Query(default=None, description="Filter by location (partial match)"),
    remote: bool = Query(default=False, description="Return remote-only jobs"),
    source: str | None = Query(default=None, description="Filter by source_label"),
    company: str | None = Query(default=None, description="Filter by company name (partial match)"),
    posted: PostedFilter = Query(default=PostedFilter.any, description="Filter by posted_at recency"),
    country: str = Query(default="US", description="Country filter: US, IN, or ALL"),
    sort_by: SortBy = Query(default=SortBy.posted_at, description="Sort results by: posted_at, match_score"),
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
            sort_by=sort_by,
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
    sort_by: SortBy,
    page: int,
    limit: int,
) -> JobSearchResponse:
    stmt = select(Listing).where(Listing.is_active == True)  # noqa: E712

    try:
        user_uuid = uuid.UUID(user_id_str)
        hidden_subq = select(HiddenJob.job_id).where(HiddenJob.user_id == user_uuid)
        stmt = stmt.where(~Listing.id.in_(hidden_subq))
    except ValueError:
        user_uuid = None

    if q:
        stmt = stmt.where(
            text(
                "to_tsvector('english', title || ' ' || company || ' ' || COALESCE(location,''))"
                " @@ plainto_tsquery('english', :fts_q)"
            ).bindparams(fts_q=q)
        )

    if location:
        stmt = stmt.where(Listing.location.ilike(f"%{location}%"))

    if remote:
        stmt = stmt.where(Listing.remote == True)  # noqa: E712

    if source:
        stmt = stmt.where(Listing.source_label == source)

    if company:
        stmt = stmt.where(Listing.company.ilike(f"%{company}%"))

    country_upper = country.upper()
    if country_upper in ("US", "IN"):
        stmt = stmt.where(Listing.country == country_upper)

    if posted != PostedFilter.any:
        cutoff = datetime.now(timezone.utc) - _POSTED_CUTOFFS[posted.value]
        stmt = stmt.where(Listing.posted_at >= cutoff)

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total: int = (await db.scalar(count_stmt)) or 0

    offset = (page - 1) * limit
    paginated = (
        stmt.order_by(Listing.posted_at.desc().nulls_last())
        .offset(offset)
        .limit(limit)
    )

    rows = (await db.execute(paginated)).scalars().all()

    company_ids = {row.company_id for row in rows if row.company_id is not None}
    company_map: dict[uuid.UUID, Company] = {}
    if company_ids:
        try:
            co_stmt = select(Company).where(Company.id.in_(company_ids))
            co_rows = (await db.execute(co_stmt)).scalars().all()
            company_map = {co.id: co for co in co_rows}
        except Exception:
            logger.warning("Failed to fetch company data for search results")

    prefs: dict | None = None
    if user_uuid is not None:
        try:
            pref_row = await db.get(JobPreference, user_uuid)
            if pref_row is not None:
                prefs = {
                    "desired_title": pref_row.desired_title,
                    "skills": pref_row.skills or [],
                    "preferred_location": pref_row.preferred_location,
                    "remote_only": pref_row.remote_only,
                    "seniority": pref_row.seniority,
                }
        except Exception:
            logger.warning("Failed to fetch job preferences for scoring — defaulting to null scores")
            prefs = None

    results: list[JobListingItem] = []
    for row in rows:
        item = JobListingItem.model_validate(row)

        co = company_map.get(row.company_id) if row.company_id else None
        item = item.model_copy(update={
            "company_summary": _build_company_summary(co, row.salary_range),
        })

        if prefs is not None:
            job_dict = {
                "title": row.title,
                "tags": row.tags,
                "remote": row.remote,
                "location": row.location,
            }
            score = compute_match_score(job_dict, prefs)
            item = item.model_copy(update={
                "match_score": score,
                "match_label": get_match_label(score),
            })
        results.append(item)

    if sort_by == SortBy.match_score and prefs is not None:
        results.sort(key=lambda x: (x.match_score or 0), reverse=True)

    return JobSearchResponse(
        total=total,
        page=page,
        limit=limit,
        results=results,
    )


def _slugify(label: str) -> str:
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
    "/preferences",
    response_model=JobPreferencesResponse,
    summary="Save or update job preferences (upsert)",
)
async def upsert_preferences(
    body: JobPreferencesCreate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> JobPreferencesResponse:
    try:
        user_uuid = uuid.UUID(current_user["sub"])
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid user id in token")

    try:
        await db.execute(
            text("""
                INSERT INTO jobs.job_preferences
                  (user_id, desired_title, skills, preferred_location, remote_only, seniority, updated_at)
                VALUES
                  (:user_id, :desired_title, :skills, :preferred_location, :remote_only, :seniority, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                  desired_title      = EXCLUDED.desired_title,
                  skills             = EXCLUDED.skills,
                  preferred_location = EXCLUDED.preferred_location,
                  remote_only        = EXCLUDED.remote_only,
                  seniority          = EXCLUDED.seniority,
                  updated_at         = NOW()
            """),
            {
                "user_id": str(user_uuid),
                "desired_title": body.desired_title,
                "skills": body.skills or [],
                "preferred_location": body.preferred_location,
                "remote_only": body.remote_only,
                "seniority": body.seniority,
            },
        )
        await db.flush()
        record = await db.get(JobPreference, user_uuid)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")

    return JobPreferencesResponse.model_validate(record)


@router.get(
    "/preferences",
    response_model=JobPreferencesResponse,
    summary="Get current user's job preferences",
)
async def get_preferences(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> JobPreferencesResponse:
    try:
        user_uuid = uuid.UUID(current_user["sub"])
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid user id in token")

    try:
        record = await db.get(JobPreference, user_uuid)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")

    if record is None:
        raise HTTPException(status_code=404, detail="No preferences set")

    return JobPreferencesResponse.model_validate(record)


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

    from app.crawler.tasks import crawl_all_companies

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

    from app.crawler.tasks import send_job_alerts

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

    from app.crawler.tasks import deactivate_stale_jobs

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
