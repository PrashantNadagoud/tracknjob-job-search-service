# TrackNJob - Job Search Service

An intelligent job aggregation and enrichment engine that crawls career pages, classifies listings by geo-restriction, and enhances them with AI-generated summaries and company metadata.

TrackNJob is a standalone FastAPI microservice that aggregates job listings from company career pages, stores them in PostgreSQL, and enriches them with AI-generated summaries via OpenAI. It exposes a REST API for searching, filtering, and saving job searches, secured with HS256 JWT authentication (shared secret with TrackNJob Core). Background crawling is handled by Celery workers scheduled via Celery Beat.

## 🚀 Key Features

- **Multi-Source Crawling**: Native support for Greenhouse, Ashby, Lever, and custom scrapers.
- **Geo-Restriction Classification**: Automatically maps "Remote" jobs to `US`, `EU`, `IN`, or `GLOBAL` markets.
- **AI-Powered Summarization**: Uses OpenAI to generate concise job summaries and extract technical tags.
- **Company Enrichment**: Concurrent enrichment from Wikipedia, LinkedIn, Glassdoor, and more.
- **Match Scoring**: Personalized 0-100 relevance scoring based on user preferences.
- **Automated Alerts**: Email notifications for new jobs matching saved searches.

## 🛠 Tech Stack

- **Backend**: Python 3.11+, FastAPI
- **Database**: PostgreSQL (SQLAlchemy + Alembic)
- **Task Queue**: Celery + Redis
- **Automation**: Playwright (Browser rendering), HTTPX (API clients)
- **AI**: OpenAI GPT models
- **Email**: Resend API

## 📂 Project Structure

```text
app/
├── api/             # REST API endpoints (v1)
├── crawler/         # ATS crawlers and ingest logic
├── enrichment/      # Company metadata scrapers
├── models.py        # Database schema definitions
├── scoring.py       # Match scoring algorithm
└── celery_app.py    # Background task configuration
docs/                # Comprehensive documentation
```

## 📖 Documentation

For detailed guides, please refer to the following in the `docs/` folder:

- [**System Architecture**](./docs/ARCHITECTURE.md) - High-level design and data flow.
- [**API Reference**](./docs/API_REFERENCE.md) - Search endpoints and request schemas.
- [**Data Model**](./docs/DATA_MODEL.md) - Database tables and relationships.
- [**Crawler Guide**](./docs/CRAWLER_GUIDE.md) - Ingestion and deduplication logic.
- [**Enrichment Guide**](./docs/ENRICHMENT_GUIDE.md) - Company metadata pipeline.
- [**Local Development**](./docs/DEVELOPMENT.md) - Setup, environment variables, and commands.
- [**AI Agent Context**](./docs/AGENT_CONTEXT.md) - Condensed reference for AI coding agents.

## 🚦 Getting Started

### Local Setup (Docker Compose)
**Prerequisites:** Docker and Docker Compose installed.

1. **Clone the repository** and enter the project directory.
2. **Copy the example environment file**:
   ```bash
   cp .env.example .env
   ```
3. **Set the shared JWT secret** in your `.env`:
   ```bash
   TNJ_SECRET_KEY=<copy the SECRET_KEY value from your TrackNJob Core .env>
   ```
   *Both services must use the exact same value so tokens issued by Core are accepted here.*
4. **Start all services**:
   ```bash
   docker compose up --build
   ```
   This starts the API server, PostgreSQL, Redis, the Celery worker, and Celery Beat.
5. **Run database migrations** (first time only):
   ```bash
   docker compose exec api alembic upgrade head
   ```

The API will be available at `http://localhost:8000`.
API documentation (Swagger UI) is at `http://localhost:8000/docs`.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | PostgreSQL connection string (`postgresql+asyncpg://...`) |
| `REDIS_URL` | Yes | Redis URL used for general cache/broker (`redis://...`) |
| `TNJ_SECRET_KEY` | Yes | HS256 shared secret — must match `SECRET_KEY` in TrackNJob Core |
| `TNJ_FRONTEND_URL` | Yes | Allowed CORS origin (e.g. `http://localhost:3000`) |
| `OPENAI_API_KEY` | No | OpenAI API key for AI summary generation |
| `RESEND_API_KEY` | No | API key for Resend email alerts |
| `ADMIN_USER_ID` | No | JWT `sub` value granted access to admin endpoints |
| `PORT` | No | Port the API server listens on (default: `8000`) |

These are also used internally by docker-compose:

| Variable | Description |
|---|---|
| `CELERY_BROKER_URL` | Celery broker URL (defaults to `REDIS_URL`) |
| `CELERY_RESULT_BACKEND` | Celery result backend URL (defaults to `REDIS_URL` db 1) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | PostgreSQL credentials for the `db` service |

---
### Manual Local Setup (No Docker)
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```
2. **Database Migrations**:
   ```bash
   alembic upgrade head
   ```
3. **Run Services**:
   - API: `uvicorn app.main:app --reload`
   - Worker: `celery -A app.celery_app worker --loglevel=info`
   - Beat: `celery -A app.celery_app beat --loglevel=info`

## 🧪 Admin & Maintenance

Admin endpoints (requires `ADMIN_USER_ID` match in JWT `sub`):
- `POST /api/v1/jobs/crawl/trigger` - Start manual crawl.
- `POST /api/v1/jobs/maintenance/trigger-alerts` - Process saved search alerts.
- `POST /api/v1/jobs/maintenance/deactivate-stale` - Mark old listings as inactive.

The Celery Beat scheduler also triggers the crawl automatically every 6 hours (at 00:00, 06:00, 12:00, 18:00 UTC).

---

## API Endpoints

```text
GET    /health
GET    /api/v1/jobs/search
GET    /api/v1/jobs/sources
GET    /api/v1/jobs/{job_id}
POST   /api/v1/jobs/saved-searches
GET    /api/v1/jobs/saved-searches
POST   /api/v1/jobs/hidden
POST   /api/v1/jobs/crawl/trigger
```

Monitor progress in the worker logs:
```bash
docker compose logs -f celery-worker
```

---
© 2026 TrackNJob. All rights reserved.
