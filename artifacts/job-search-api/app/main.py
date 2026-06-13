import asyncio
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.v1.admin import router as admin_router
from app.api.v1.alerts import router as alerts_router
from app.api.v1.companies import router as companies_router
from app.api.v1.jobs import router as jobs_router
from app.auth import _UnauthorizedError
from app.config import get_settings
from app.crawler.geo_classifier import load_geonames_index
from app.db import AsyncSessionFactory

logger = logging.getLogger(__name__)
settings = get_settings()


async def _load_geonames_background() -> None:
    """Load GeoNames city index in the background after server starts."""
    try:
        async with AsyncSessionFactory() as session:
            rows = (
                await asyncio.wait_for(
                    session.execute(
                        text("SELECT name, ascii_name, country_code FROM geo.cities ORDER BY population DESC")
                    ),
                    timeout=15.0,
                )
            ).fetchall()
            load_geonames_index([(r[0], r[1], r[2]) for r in rows])
    except Exception:
        logger.warning(
            "GeoNames index could not be loaded (geo.cities table may not exist yet). "
            "Signal-string fallback will be used.",
            exc_info=True,
        )


async def _maybe_seed_sources() -> None:
    """If no active ATS sources exist, enqueue seed + crawl tasks automatically.

    Runs once at startup so Railway deploys are self-seeding without any
    manual admin API calls.
    """
    try:
        async with AsyncSessionFactory() as session:
            count = (
                await asyncio.wait_for(
                    session.execute(
                        text("SELECT COUNT(*) FROM jobs.ats_sources WHERE is_active = TRUE")
                    ),
                    timeout=10.0,
                )
            ).scalar()

        if not count:
            logger.info("No active ATS sources found — enqueueing seed_startup_sources + run_crawl_pipeline")
            from app.celery_app import celery_app as _celery
            _celery.send_task("app.crawler.tasks.seed_startup_sources")
            _celery.send_task("app.crawler.tasks.run_crawl_pipeline", countdown=60)
        else:
            logger.info("Active ATS sources found (%d) — skipping auto-seed", count)
    except Exception:
        logger.warning("Auto-seed check failed — will rely on beat schedule", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background tasks after server is ready to serve requests."""
    asyncio.create_task(_load_geonames_background())
    asyncio.create_task(_maybe_seed_sources())
    yield


app = FastAPI(
    title="Job Search API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


_HTTP_ERROR_SLUGS: dict[int, str] = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    408: "request_timeout",
    409: "conflict",
    410: "gone",
    422: "unprocessable_entity",
    429: "too_many_requests",
    500: "internal_server_error",
    502: "bad_gateway",
    503: "service_unavailable",
}


def _error_body(error: str, message: str, status_code: int, details=None) -> dict:
    return {
        "error": error,
        "message": message,
        "details": details,
        "status_code": status_code,
    }


@app.exception_handler(StarletteHTTPException)
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    slug = _HTTP_ERROR_SLUGS.get(exc.status_code, f"http_{exc.status_code}")
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_body(
            slug,
            exc.detail if isinstance(exc.detail, str) else str(exc.detail),
            exc.status_code,
        ),
    )


@app.exception_handler(_UnauthorizedError)
async def unauthorized_handler(request: Request, exc: _UnauthorizedError) -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content=_error_body("unauthorized", exc.message, 401),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=_error_body("unprocessable_entity", "Request validation failed", 422),
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content=_error_body("internal_server_error", "An unexpected error occurred", 500),
    )


app.include_router(jobs_router, prefix="/api/v1/jobs", tags=["jobs"])
app.include_router(companies_router, prefix="/api/v1/companies", tags=["companies"])
app.include_router(admin_router, prefix="/api/v1/admin", tags=["admin"])
app.include_router(alerts_router, prefix="/api/v1/alerts", tags=["alerts"])


@app.get("/health", tags=["health"])
async def health_check() -> dict:
    return {"status": "ok", "service": "job-search"}
