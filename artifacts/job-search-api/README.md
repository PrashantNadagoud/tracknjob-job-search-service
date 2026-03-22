# TrackNJob — Job Search Microservice

TrackNJob is a standalone FastAPI microservice that aggregates job listings from
company career pages (Cloudflare, Notion, Linear, Vercel, Stripe), stores them in
PostgreSQL, and enriches them with AI-generated summaries via OpenAI. It exposes a
REST API for searching, filtering, and saving job searches, secured with HS256 JWT
authentication (shared secret with TrackNJob Core). Background crawling is handled by Celery workers scheduled every
6 hours via Celery Beat.

---

## Local Setup (Docker Compose)

**Prerequisites:** Docker and Docker Compose installed.

1. Clone the repository and enter the project directory.

2. Copy the example environment file and fill in the values:
   ```
   cp .env.example .env
   ```

3. Set the shared JWT secret in your `.env`:
   ```
   TNJ_SECRET_KEY=<copy the SECRET_KEY value from your TrackNJob Core .env>
   ```
   Both services must use the exact same value so tokens issued by Core are
   accepted here.

4. Start all services:
   ```
   docker compose up --build
   ```
   This starts the API server, PostgreSQL, Redis, the Celery worker, and Celery Beat.

5. Run database migrations (first time only):
   ```
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
| `OPENAI_API_KEY` | No | OpenAI API key for AI summary generation; leave empty to skip |
| `ADMIN_USER_ID` | No | JWT `sub` value granted access to `POST /crawl/trigger` |
| `PORT` | No | Port the API server listens on (default: `8000`) |

These are also used internally by docker-compose but not exposed in `.env.example`:

| Variable | Description |
|---|---|
| `CELERY_BROKER_URL` | Celery broker URL (defaults to `REDIS_URL`) |
| `CELERY_RESULT_BACKEND` | Celery result backend URL (defaults to `REDIS_URL` db 1) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | PostgreSQL credentials for the `db` service |

---

## Triggering a Manual Crawl

The crawl can be triggered immediately via the admin endpoint. You need a valid JWT
whose `sub` claim matches `ADMIN_USER_ID`.

```
POST /api/v1/jobs/crawl/trigger
Authorization: Bearer <admin-jwt>
```

Successful response:
```json
{"status": "crawl started", "task_id": "<celery-task-id>"}
```

The crawl runs asynchronously in the Celery worker. Monitor progress in the worker
container logs:
```
docker compose logs -f celery-worker
```

The Celery Beat scheduler also triggers the crawl automatically every 6 hours
(at :00 of 00:00, 06:00, 12:00, 18:00 UTC).

---

## API Endpoints

```
GET    /health
GET    /api/v1/jobs/search
GET    /api/v1/jobs/sources
GET    /api/v1/jobs/{job_id}
POST   /api/v1/jobs/saved-searches
GET    /api/v1/jobs/saved-searches
POST   /api/v1/jobs/hidden
POST   /api/v1/jobs/crawl/trigger
```
