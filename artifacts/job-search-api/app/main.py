import os

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.v1.companies import router as companies_router
from app.api.v1.jobs import router as jobs_router
from app.auth import _UnauthorizedError
from app.config import get_settings

settings = get_settings()

app = FastAPI(
    title="Job Search API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.TNJ_FRONTEND_URL],
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


@app.get("/health", tags=["health"])
async def health_check() -> dict:
    return {"status": "ok", "service": "job-search"}
