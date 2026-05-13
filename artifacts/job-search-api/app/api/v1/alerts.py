"""Job alert subscription endpoints.

Routes (all under /api/v1/alerts):
    POST   /subscribe               — upsert subscription
    GET    /subscription/{user_id}  — get subscription
    PATCH  /subscription/{user_id}  — update subscription fields
    DELETE /unsubscribe/{user_id}   — soft-delete (is_active=false)
    POST   /test-send/{user_id}     — trigger immediate test email
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Request / Response schemas ────────────────────────────────────────────────

class SubscribeRequest(BaseModel):
    user_id: str
    email: str
    name: str | None = None
    keywords: list[str] | None = None
    locations: list[str] | None = None
    employment_types: list[str] | None = None
    ats_types: list[str] | None = None
    job_search_started_at: datetime | None = None
    motivational_email_enabled: bool = True
    delivery_time_utc: int = 13


class PatchRequest(BaseModel):
    email: str | None = None
    name: str | None = None
    keywords: list[str] | None = None
    locations: list[str] | None = None
    employment_types: list[str] | None = None
    ats_types: list[str] | None = None
    job_search_started_at: datetime | None = None
    motivational_email_enabled: bool | None = None
    delivery_time_utc: int | None = None
    is_active: bool | None = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_dict(row) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "user_id": row.user_id,
        "email": row.email,
        "name": row.name,
        "is_active": row.is_active,
        "keywords": list(row.keywords or []),
        "locations": list(row.locations or []),
        "employment_types": list(row.employment_types or []),
        "ats_types": list(row.ats_types or []) if row.ats_types else None,
        "job_search_started_at": row.job_search_started_at.isoformat() if row.job_search_started_at else None,
        "motivational_email_enabled": row.motivational_email_enabled,
        "delivery_time_utc": row.delivery_time_utc,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


async def _get_subscription(user_id: str, db: AsyncSession) -> Any:
    row = (await db.execute(
        text("SELECT * FROM jobs.alert_subscriptions WHERE user_id = :uid"),
        {"uid": user_id},
    )).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return row


async def _send_alert_for_subscription(sub_row, db: AsyncSession) -> dict[str, Any]:
    """Shared logic used by test-send and the Celery task."""
    from app.alert_tasks import _query_matching_jobs, _render_and_send
    jobs = await _query_matching_jobs(sub_row, db)
    if not jobs:
        await db.execute(
            text("""
                INSERT INTO jobs.alert_deliveries
                    (subscription_id, jobs_sent, status)
                VALUES (:sid, 0, 'skipped_no_matches')
            """),
            {"sid": sub_row.id},
        )
        return {"status": "skipped_no_matches", "jobs_found": 0}

    result = await _render_and_send(sub_row, jobs)
    await db.execute(
        text("""
            INSERT INTO jobs.alert_deliveries
                (subscription_id, jobs_sent, status, resend_message_id, error_message)
            VALUES (:sid, :cnt, :status, :mid, :err)
        """),
        {
            "sid": sub_row.id,
            "cnt": len(jobs),
            "status": result["status"],
            "mid": result.get("resend_message_id"),
            "err": result.get("error_message"),
        },
    )
    return {"status": result["status"], "jobs_found": len(jobs)}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/subscribe")
async def subscribe(body: SubscribeRequest, db: AsyncSession = Depends(get_db)) -> dict:
    """Upsert a job alert subscription (one per user_id)."""
    row = (await db.execute(
        text("""
            INSERT INTO jobs.alert_subscriptions
                (user_id, email, name, keywords, locations, employment_types, ats_types,
                 job_search_started_at, motivational_email_enabled, delivery_time_utc)
            VALUES
                (:user_id, :email, :name, :keywords, :locations, :employment_types,
                 :ats_types, :job_search_started_at, :motivational_email_enabled,
                 :delivery_time_utc)
            ON CONFLICT (user_id) DO UPDATE SET
                email                       = EXCLUDED.email,
                name                        = EXCLUDED.name,
                keywords                    = EXCLUDED.keywords,
                locations                   = EXCLUDED.locations,
                employment_types            = EXCLUDED.employment_types,
                ats_types                   = EXCLUDED.ats_types,
                job_search_started_at       = EXCLUDED.job_search_started_at,
                motivational_email_enabled  = EXCLUDED.motivational_email_enabled,
                delivery_time_utc           = EXCLUDED.delivery_time_utc,
                is_active                   = true,
                updated_at                  = now()
            RETURNING id
        """),
        {
            "user_id": body.user_id,
            "email": body.email,
            "name": body.name,
            "keywords": body.keywords,
            "locations": body.locations,
            "employment_types": body.employment_types,
            "ats_types": body.ats_types,
            "job_search_started_at": body.job_search_started_at,
            "motivational_email_enabled": body.motivational_email_enabled,
            "delivery_time_utc": body.delivery_time_utc,
        },
    )).fetchone()

    return {
        "subscription_id": str(row.id),
        "message": "Subscribed successfully",
    }


@router.get("/subscription/{user_id}")
async def get_subscription(user_id: str, db: AsyncSession = Depends(get_db)) -> dict:
    """Return the full subscription object for a user."""
    row = await _get_subscription(user_id, db)
    return _row_to_dict(row)


@router.patch("/subscription/{user_id}")
async def patch_subscription(
    user_id: str, body: PatchRequest, db: AsyncSession = Depends(get_db)
) -> dict:
    """Update one or more fields on a subscription."""
    await _get_subscription(user_id, db)

    updates: dict[str, Any] = {}
    if body.email is not None:
        updates["email"] = body.email
    if body.name is not None:
        updates["name"] = body.name
    if body.keywords is not None:
        updates["keywords"] = body.keywords
    if body.locations is not None:
        updates["locations"] = body.locations
    if body.employment_types is not None:
        updates["employment_types"] = body.employment_types
    if body.ats_types is not None:
        updates["ats_types"] = body.ats_types
    if body.job_search_started_at is not None:
        updates["job_search_started_at"] = body.job_search_started_at
    if body.motivational_email_enabled is not None:
        updates["motivational_email_enabled"] = body.motivational_email_enabled
    if body.delivery_time_utc is not None:
        updates["delivery_time_utc"] = body.delivery_time_utc
    if body.is_active is not None:
        updates["is_active"] = body.is_active

    if not updates:
        row = await _get_subscription(user_id, db)
        return _row_to_dict(row)

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["user_id"] = user_id
    await db.execute(
        text(f"UPDATE jobs.alert_subscriptions SET {set_clause}, updated_at = now() WHERE user_id = :user_id"),
        updates,
    )
    row = await _get_subscription(user_id, db)
    return _row_to_dict(row)


@router.delete("/unsubscribe/{user_id}")
async def unsubscribe(user_id: str, db: AsyncSession = Depends(get_db)) -> dict:
    """Soft-delete a subscription by setting is_active=false."""
    await _get_subscription(user_id, db)
    await db.execute(
        text("UPDATE jobs.alert_subscriptions SET is_active = false, updated_at = now() WHERE user_id = :uid"),
        {"uid": user_id},
    )
    return {"message": "Unsubscribed successfully"}


@router.post("/test-send/{user_id}")
async def test_send(user_id: str, db: AsyncSession = Depends(get_db)) -> dict:
    """Trigger an immediate test alert email for this user."""
    row = await _get_subscription(user_id, db)
    result = await _send_alert_for_subscription(row, db)
    return {
        "message": "Test email sent" if result["status"] == "sent" else result["status"],
        "jobs_found": result["jobs_found"],
    }
