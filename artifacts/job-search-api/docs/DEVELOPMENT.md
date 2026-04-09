# Local Development Guide

## Prerequisites
- **Python**: 3.11+
- **Database**: PostgreSQL 14+ with `pg_trgm` extension enabled.
- **Cache/Broker**: Redis
- **System**: `playwright` (for certain crawlers)

## Environment Variables
Create a `.env` file in the root directory.

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | - | PostgreSQL connection string (`postgresql+asyncpg://...`) |
| `REDIS_URL` | Yes | `redis://localhost:6379/0` | Redis for Celery and caching |
| `TNJ_SECRET_KEY` | Yes | - | Secret key for JWT verification |
| `TNJ_FRONTEND_URL`| Yes | - | Origin for CORS |
| `OPENAI_API_KEY` | No* | - | Required for job summarization (`generate_job_summary`) |
| `RESEND_API_KEY` | No* | - | Required for job alert emails |
| `ADMIN_USER_ID` | No | - | `sub` of the user allowed to trigger admin tasks |

## Local Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

2. **Run Migrations**:
   ```bash
   alembic upgrade head
   ```

3. **Start the API**:
   ```bash
   uvicorn app.main:app --reload
   ```

4. **Start Celery Worker**:
   ```bash
   celery -A app.celery_app worker --loglevel=info
   ```

5. **Start Celery Beat**:
   ```bash
   celery -A app.celery_app beat --loglevel=info
   ```

## Common Tasks

### Triggering a Manual Crawl
Use the API (as Admin):
```bash
curl -X POST http://localhost:8000/api/v1/jobs/crawl/trigger -H "Authorization: Bearer <ADMIN_TOKEN>"
```

### Running Tests
```bash
pytest
```

## Known Issues
- **Playwright in Docker**: Ensure the Dockerfile includes necessary system dependencies for Chromium if running in a container.
- **Database Schema**: Ensure the `jobs` schema exists before running migrations if not handled by your setup scripts.
