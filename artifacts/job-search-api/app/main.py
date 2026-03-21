import os

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

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
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_body(
            type(exc).__name__,
            exc.detail if isinstance(exc.detail, str) else str(exc.detail),
            exc.status_code,
        ),
    )


@app.exception_handler(_UnauthorizedError)
async def unauthorized_handler(request: Request, exc: _UnauthorizedError) -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content=_error_body("Unauthorized", exc.message, 401),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=_error_body(
            "ValidationError",
            "Request validation failed",
            422,
            details=exc.errors(),
        ),
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content=_error_body("InternalServerError", "An unexpected error occurred", 500),
    )


@app.get("/health", tags=["health"])
async def health_check() -> dict:
    return {"status": "ok", "service": "job-search"}
