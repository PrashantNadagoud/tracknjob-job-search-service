"""Celery tasks for job alert email delivery.

Task: send_daily_alerts
    Runs every hour via Beat. Self-filters per subscription on delivery_time_utc
    and a daily-send guard (no duplicate email per subscription per day).
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.celery_app import celery_app

logger = logging.getLogger(__name__)

_TEMPLATE_PATH = Path(__file__).parent / "templates" / "alert_email.html"


# ── DB session factory (NullPool — required for Celery + asyncio.run()) ────────

def _make_session() -> async_sessionmaker[AsyncSession]:
    raw_url = os.environ["DATABASE_URL"]
    parsed = urlparse(raw_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    sslmode = params.pop("sslmode", [None])[0]
    new_query = urlencode({k: v[0] for k, v in params.items()})
    clean_url = urlunparse(
        parsed._replace(scheme="postgresql+asyncpg", query=new_query)
    )
    connect_args: dict[str, Any] = (
        {} if sslmode in ("disable", "allow", None) else {"ssl": True}
    )
    engine = create_async_engine(
        clean_url, connect_args=connect_args, poolclass=pool.NullPool
    )
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ── Core helpers (also imported by the API test-send endpoint) ───────────────

async def _query_matching_jobs(sub, db: AsyncSession) -> list[dict[str, Any]]:
    """Return up to 10 listings matching the subscription filters posted in the last 24h."""
    keywords: list[str] = list(sub.keywords or [])
    locations: list[str] = list(sub.locations or [])
    employment_types: list[str] = list(sub.employment_types or [])
    ats_types: list[str] = list(sub.ats_types or []) if sub.ats_types else []

    conditions = ["l.is_active = true", "l.posted_at >= now() - interval '24 hours'"]
    params: dict[str, Any] = {}

    if keywords:
        kw_conditions = " OR ".join(f"l.title ILIKE :kw_{i}" for i in range(len(keywords)))
        conditions.append(f"({kw_conditions})")
        for i, kw in enumerate(keywords):
            params[f"kw_{i}"] = f"%{kw}%"

    if locations:
        loc_conditions = " OR ".join(
            f"l.location ILIKE :loc_{i}" for i in range(len(locations))
        )
        conditions.append(f"({loc_conditions})")
        for i, loc in enumerate(locations):
            params[f"loc_{i}"] = f"%{loc}%"

    if employment_types:
        params["employment_types"] = employment_types
        conditions.append("(l.employment_type IS NULL OR l.employment_type = ANY(:employment_types))")

    if ats_types:
        params["ats_types"] = ats_types
        conditions.append("l.ats_type = ANY(:ats_types)")

    where_clause = " AND ".join(conditions)
    sql = text(f"""
        SELECT
            l.id, l.title, l.company, l.location,
            l.employment_type, l.source_url, l.posted_at
        FROM jobs.listings l
        WHERE {where_clause}
        ORDER BY l.posted_at DESC
        LIMIT 10
    """)

    rows = (await db.execute(sql, params)).fetchall()
    return [
        {
            "id": str(r.id),
            "title": r.title,
            "company": r.company,
            "location": r.location,
            "employment_type": r.employment_type,
            "source_url": r.source_url,
        }
        for r in rows
    ]


async def _render_and_send(sub, jobs: list[dict[str, Any]]) -> dict[str, Any]:
    """Render the Jinja2 template and send via Resend. Returns status dict."""
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_PATH.parent)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template(_TEMPLATE_PATH.name)

    motivational_text = ""
    if sub.motivational_email_enabled:
        from app.services.motivational import generate_motivational_intro

        days_searching = 1
        if sub.job_search_started_at:
            delta = datetime.now(timezone.utc) - sub.job_search_started_at.replace(
                tzinfo=timezone.utc
            ) if sub.job_search_started_at.tzinfo is None else datetime.now(timezone.utc) - sub.job_search_started_at
            days_searching = max(1, delta.days)

        motivational_text = generate_motivational_intro(
            {
                "name": sub.name or "there",
                "days_searching": days_searching,
                "jobs_found_today": len(jobs),
                "top_job_title": jobs[0]["title"] if jobs else "Software Engineer",
                "top_company": jobs[0]["company"] if jobs else "a great company",
            }
        )

    from app.config import get_settings
    settings = get_settings()
    frontend_url = settings.TNJ_FRONTEND_URL
    # The API base URL is derived from the frontend URL host; fall back to a
    # relative path so links in the email point to the correct service.
    api_base_url = os.environ.get("API_BASE_URL", frontend_url.rstrip("/"))

    html_body = template.render(
        name=sub.name or "there",
        motivational_text=motivational_text,
        jobs=jobs,
        keywords=list(sub.keywords or []),
        locations=list(sub.locations or []),
        user_id=sub.user_id,
        frontend_url=frontend_url,
        api_base_url=api_base_url,
    )

    subject = f"☀️ {len(jobs)} new job{'s' if len(jobs) != 1 else ''} for you today, {sub.name or 'there'}"
    from_email = os.environ.get("RESEND_FROM_EMAIL", "alerts@tracknjob.com")

    try:
        import resend as resend_lib
        resend_lib.api_key = os.environ.get("RESEND_API_KEY", "")
        response = resend_lib.Emails.send({
            "from": f"TrackNJob Alerts <{from_email}>",
            "to": sub.email,
            "subject": subject,
            "html": html_body,
        })
        msg_id = response.get("id") if isinstance(response, dict) else str(response)
        return {"status": "sent", "resend_message_id": msg_id}
    except Exception as exc:
        logger.error("Failed to send alert email to %s: %s", sub.email, exc)
        return {"status": "failed", "error_message": str(exc)}


# ── Celery task ───────────────────────────────────────────────────────────────

@celery_app.task(bind=True, max_retries=0, name="app.alert_tasks.send_daily_alerts")
def send_daily_alerts(self) -> dict[str, int]:
    """Send job alert emails for all subscriptions due in the current UTC hour.

    Self-filters: only runs for subscriptions where delivery_time_utc matches
    the current UTC hour AND no delivery row exists for today (UTC).
    """
    return asyncio.run(_async_send_daily_alerts())


async def _async_send_daily_alerts() -> dict[str, int]:
    from app.config import get_settings
    settings = get_settings()
    if not settings.ALERTS_ENABLED:
        logger.info("ALERTS_ENABLED=false; skipping send_daily_alerts")
        return {"processed": 0, "sent": 0, "skipped": 0, "failed": 0}

    current_hour = datetime.now(timezone.utc).hour
    Session = _make_session()
    counts = {"processed": 0, "sent": 0, "skipped": 0, "failed": 0}

    async with Session() as db:
        subs = (await db.execute(
            text("""
                SELECT s.*
                FROM jobs.alert_subscriptions s
                WHERE s.is_active = true
                  AND s.delivery_time_utc = :hour
                  AND NOT EXISTS (
                      SELECT 1 FROM jobs.alert_deliveries d
                      WHERE d.subscription_id = s.id
                        AND d.delivered_at >= now()::date
                        AND d.status IN ('sent', 'skipped_no_matches')
                  )
            """),
            {"hour": current_hour},
        )).fetchall()

        logger.info(
            "send_daily_alerts: hour=%d, %d subscription(s) due", current_hour, len(subs)
        )

        for sub in subs:
            counts["processed"] += 1
            try:
                jobs = await _query_matching_jobs(sub, db)

                if not jobs:
                    await db.execute(
                        text("""
                            INSERT INTO jobs.alert_deliveries
                                (subscription_id, jobs_sent, status)
                            VALUES (:sid, 0, 'skipped_no_matches')
                        """),
                        {"sid": sub.id},
                    )
                    await db.commit()
                    counts["skipped"] += 1
                    logger.info("Skipped %s (no matches)", sub.user_id)
                    continue

                result = await _render_and_send(sub, jobs)
                await db.execute(
                    text("""
                        INSERT INTO jobs.alert_deliveries
                            (subscription_id, jobs_sent, status, resend_message_id, error_message)
                        VALUES (:sid, :cnt, :status, :mid, :err)
                    """),
                    {
                        "sid": sub.id,
                        "cnt": len(jobs),
                        "status": result["status"],
                        "mid": result.get("resend_message_id"),
                        "err": result.get("error_message"),
                    },
                )
                await db.commit()

                if result["status"] == "sent":
                    counts["sent"] += 1
                    logger.info("Sent alert to %s (%d jobs)", sub.email, len(jobs))
                else:
                    counts["failed"] += 1

            except Exception as exc:
                logger.exception("Unexpected error for subscription %s: %s", sub.id, exc)
                counts["failed"] += 1
                await db.rollback()

    logger.info("send_daily_alerts complete: %s", counts)
    return counts
