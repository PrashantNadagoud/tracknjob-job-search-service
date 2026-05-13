# TrackNJob — Job Search API

A FastAPI microservice that aggregates, enriches, and serves job listings from ATS platforms and company career pages. Powers the TrackNJob frontend with job search, personalization, AI enrichment, company intelligence, and daily email alerts.

---

## Table of Contents

- [Features](#features)
- [Tech Stack](#tech-stack)
- [Architecture Overview](#architecture-overview)
- [Getting Started](#getting-started)
- [Environment Variables](#environment-variables)
- [API Reference](#api-reference)
- [Database Schema](#database-schema)
- [Crawler System](#crawler-system)
- [Background Tasks (Celery Beat)](#background-tasks-celery-beat)
- [Job Alerts](#job-alerts)
- [Running Tests](#running-tests)
- [Database Migrations](#database-migrations)

---

## Features

- **Multi-ATS Crawling** — Greenhouse, Lever, Ashby, Workday, SmartRecruiters, BambooHR, Rippling, JazzHR, Naukri, Foundit (10 platforms)
- **Direct Company Crawlers** — Stripe, Cloudflare, Notion, Linear, Vercel, Amazon India, Google India, Microsoft India, Flipkart, Razorpay
- **Fortune 500 Discovery** — Automatically probes company websites to detect which ATS they use
- **AI Enrichment** — OpenAI-powered job summaries, tag extraction, and salary inference
- **Company Intelligence** — Funding rounds, headcount, culture scores, HQ location, India presence
- **Match Scoring** — PostgreSQL `pg_trgm` similarity score between user preferences and listings
- **Ghost Job Detection** — Deactivates listings not seen in recent crawls (12–72 h window)
- **Job Alert Emails** — Daily personalised emails via Resend with AI motivational intros (Jinja2 templates)
- **Geo Filtering** — US / EU / IN / GLOBAL market segmentation
- **Full-Text Search** — GIN index over title, company, and location fields

---

## Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI + Uvicorn |
| ORM | SQLAlchemy 2 (async) + asyncpg |
| Migrations | Alembic |
| Background tasks | Celery 5 + Redis |
| AI | OpenAI GPT-4o-mini |
| Email | Resend + Jinja2 |
| Web scraping | Playwright + BeautifulSoup4 |
| Auth | HS256 JWT (shared secret with TrackNJob core) |
| Database | PostgreSQL (schema: `jobs`) |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   FastAPI App                        │
│  /api/v1/jobs   /api/v1/alerts   /api/v1/companies  │
│  /api/v1/admin                                       │
└────────────────────┬────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
┌───────▼────────┐     ┌─────────▼──────────┐
│   PostgreSQL   │     │   Celery Workers    │
│  (jobs schema) │     │                     │
│                │     │  Crawlers           │
│  listings      │     │  Enrichment         │
│  companies     │     │  Alert delivery     │
│  alert_*       │     │  Discovery queue    │
│  ats_sources   │◄────│  Stale job cleanup  │
│  ...           │     └────────┬────────────┘
└────────────────┘              │
                        ┌───────▼──────┐
                        │    Redis     │
                        │  (broker +   │
                        │   results)   │
                        └──────────────┘
```

---

## Getting Started

### Prerequisites

- Python 3.10+
- PostgreSQL 14+
- Redis

### Installation

```bash
git clone https://github.com/PrashantNadagoud/tracknjob-job-search-service.git
cd tracknjob-job-search-service/artifacts/job-search-api

pip install -r requirements.txt
playwright install chromium
```

### Configuration

```bash
cp .env.example .env
# Edit .env with your values
```

### Run migrations

```bash
alembic upgrade head
```

### Start the API server

```bash
uvicorn app.main:app --reload --port 8001
```

### Start Celery workers

```bash
# Worker
celery -A app.celery_app worker --loglevel=info

# Beat scheduler (separate terminal)
celery -A app.celery_app beat --loglevel=info
```

API docs (Swagger UI) are available at `http://localhost:8001/docs`.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `REDIS_URL` | Yes | Redis broker URL |
| `TNJ_SECRET_KEY` | Yes | HS256 JWT secret — must match TrackNJob core |
| `TNJ_FRONTEND_URL` | Yes | Frontend origin (used for CORS and email links) |
| `ADMIN_USER_ID` | Yes | User ID allowed to hit admin endpoints |
| `OPENAI_API_KEY` | No | Enables AI job summaries and motivational email intros |
| `RESEND_API_KEY` | No | Enables transactional email via Resend |
| `RESEND_FROM_EMAIL` | No | Sender address (default: `alerts@tracknjob.com`) |
| `ALERTS_ENABLED` | No | Set `false` to disable all alert email sending (default: `true`) |
| `CRUNCHBASE_API_KEY` | No | Enriches company funding data |
| `PORT` | No | API server port (default: `8001`) |

Crawler seed lists (comma-separated):

| Variable | Default |
|---|---|
| `NAUKRI_KEYWORD_LIST` | `software engineer,data engineer,product manager` |
| `FOUNDIT_KEYWORD_LIST` | `software engineer,backend developer` |
| `WORKDAY_SEED_SLUGS` | `google,microsoft,amazon,apple,meta,...` |

---

## API Reference

All endpoints return JSON. Auth-required routes expect `Authorization: Bearer <JWT>`.

### Jobs — `/api/v1/jobs`

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/search` | Yes | Search listings with filters |
| `GET` | `/{job_id}` | Yes | Get a single job detail |
| `GET` | `/sources` | Yes | List active sources with counts |
| `POST` | `/preferences` | Yes | Upsert job preferences (skills, title, seniority) |
| `GET` | `/preferences` | Yes | Get current job preferences |
| `POST` | `/saved-searches` | Yes | Save a search filter set |
| `GET` | `/saved-searches` | Yes | List saved searches |
| `DELETE` | `/saved-searches/{id}` | Yes | Delete a saved search |
| `POST` | `/hidden` | Yes | Hide a job listing |

**Search parameters:** `q`, `location`, `remote`, `source`, `company`, `posted` (hours), `country`, `market`, `page`, `page_size`

### Alerts — `/api/v1/alerts`

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/subscribe` | Yes | Create or update alert subscription |
| `GET` | `/subscription/{user_id}` | Yes (own) | Get subscription details |
| `PATCH` | `/subscription/{user_id}` | Yes (own) | Update subscription fields |
| `DELETE` | `/unsubscribe/{user_id}` | Yes (own) | Soft-delete subscription (API) |
| `GET` | `/unsubscribe/{user_id}` | No | One-click unsubscribe from email link |
| `POST` | `/test-send/{user_id}` | Yes (own) | Trigger an immediate test email |

**Subscribe payload fields:** `user_id`, `email`, `name`, `keywords[]`, `locations[]`, `employment_types[]`, `ats_types[]`, `motivational_email_enabled`, `delivery_time_utc` (0–23)

### Companies — `/api/v1/companies`

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/{slug}` | Yes | Company profile with funding, culture, ATS info |

### Admin — `/api/v1/admin`

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/seed-status` | Admin | Discovery queue and ATS source stats |
| `POST` | `/trigger-crawl` | Admin | Manually kick off the crawl pipeline |

### Health

```
GET /health  →  {"status": "ok", "service": "job-search"}
```

---

## Database Schema

All tables live in the `jobs` PostgreSQL schema.

| Table | Description |
|---|---|
| `listings` | Job listings — title, company, location, salary, ATS metadata, AI tags |
| `companies` | Company profiles — funding, headcount, culture scores, HQ, India presence |
| `ats_sources` | Per-company ATS config, crawl state, back-off tracking |
| `company_discovery_queue` | Websites pending ATS type detection |
| `crawl_dead_letters` | Failed crawl attempts logged for debugging |
| `saved_searches` | User-saved filter sets with optional alert subscription |
| `job_preferences` | User preferences used for match scoring |
| `hidden_jobs` | Jobs dismissed by a user |
| `alert_subscriptions` | User alert config (keywords, delivery hour, filters) |
| `alert_deliveries` | Email send history with status and Resend message IDs |

Migration history: `0001` → `0011` (Alembic, async).

---

## Crawler System

### ATS Platform Crawlers

Each crawler in `app/crawler/ats/` handles a specific platform's job API:

| File | Platform | Method |
|---|---|---|
| `greenhouse.py` | Greenhouse | JSON board API |
| `lever.py` | Lever | Postings API v0 |
| `ashby.py` | Ashby | GraphQL |
| `workday.py` | Workday | Playwright (JS-rendered) |
| `smartrecruiters.py` | SmartRecruiters | REST API |
| `bamboohr.py` | BambooHR | XML feed |
| `rippling.py` | Rippling | JSON API |
| `jazzhr.py` | JazzHR | XML feed |
| `naukri.py` | Naukri (India) | Playwright |
| `foundit.py` | Foundit (India) | Playwright |

### Company-Specific Crawlers (`app/crawler/companies/`)

Direct parsers for Cloudflare, Linear, Notion, Stripe, Vercel, and India-focused crawlers for Amazon, Google, Microsoft, Flipkart, Razorpay.

### Discovery Pipeline

`app/discovery/ats_prober.py` probes unknown company websites, identifies their ATS type, and populates `ats_sources`.  
`app/discovery/fortune500_scraper.py` seeds the queue with Fortune 500 company websites automatically.

---

## Background Tasks (Celery Beat)

| Schedule | Task | Description |
|---|---|---|
| Every hour (`:00`) | `run_crawl_pipeline` | Full ATS crawl across all active sources |
| Every hour (`:00`) | `send_daily_alerts` | Send job alert emails for subscriptions due this hour |
| Every 6 hours | `run_discovery_queue` | Probe queued websites for ATS detection |
| Every 6 hours | `reactivate_errored_sources` | Reset sources recovered from errors |
| Every 12 hours | `deactivate_stale_jobs` | Mark unseen listings as inactive |
| Every 30 minutes | `send_job_alerts` | Saved-search–based alert emails |
| Daily at 02:00 UTC | `enrich_new_companies` | AI enrichment for newly discovered companies |
| Sundays at 03:00 UTC | `reenrich_stale_companies` | Re-enrich companies not updated in 30 days |
| Daily at 04:30 UTC | `prune_old_deliveries` | Delete alert delivery rows older than 90 days |

---

## Job Alerts

The alert system (`app/alert_tasks.py`, `app/api/v1/alerts.py`) delivers personalised daily job digests:

1. **Subscription** — User subscribes with keyword / location / type filters and a preferred delivery hour (UTC 0–23)
2. **Matching** — Each hour, subscriptions due are queried; listings are filtered by keywords (ILIKE), locations, and employment type posted in the last 24 hours
3. **Email** — Jinja2 template (`app/templates/alert_email.html`) renders a responsive HTML email with up to 10 matched jobs
4. **Motivational intro** — If `motivational_email_enabled` and `OPENAI_API_KEY` is set, GPT-4o-mini generates a personalised 2–3 sentence opener; falls back to a warm static message on error
5. **Deduplication** — A partial unique index on `alert_deliveries(subscription_id, delivered_at::date) WHERE status = 'sent'` prevents duplicate sends from concurrent workers using a claim-before-send pattern
6. **One-click unsubscribe** — `GET /api/v1/alerts/unsubscribe/{user_id}` works without authentication so email-client clicks work immediately

---

## Running Tests

```bash
cd artifacts/job-search-api
pytest tests/ -q
```

248 tests covering:

- All API endpoints (jobs, alerts, companies, admin)
- Auth enforcement (401 unauthenticated / 403 cross-user access)
- Celery task logic (query matching, deduplication, pruning)
- Motivational service (OpenAI success path + static fallback)
- Beat schedule integrity
- `delivery_time_utc` validation bounds (0–23)

---

## Database Migrations

```bash
# Apply all pending migrations
alembic upgrade head

# Roll back one step
alembic downgrade -1

# Check current revision
alembic current

# Generate a new migration
alembic revision -m "short_description"
```

Migrations live in `app/migrations/versions/` and use async SQLAlchemy with `NullPool` to avoid event-loop conflicts.
