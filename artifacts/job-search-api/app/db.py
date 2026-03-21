from collections.abc import AsyncGenerator
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings


def _build_asyncpg_url(raw_url: str) -> tuple[str, dict]:
    """Convert a standard postgresql:// URL to asyncpg-compatible form.

    asyncpg does not accept 'sslmode' as a query param; it uses ssl= in
    connect_args instead. Strip sslmode and map it to the right ssl value.
    """
    url = raw_url
    for prefix, replacement in [
        ("postgresql://", "postgresql+asyncpg://"),
        ("postgres://", "postgresql+asyncpg://"),
    ]:
        if url.startswith(prefix):
            url = replacement + url[len(prefix):]
            break

    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    sslmode = params.pop("sslmode", [None])[0]

    new_query = urlencode({k: v[0] for k, v in params.items()})
    clean_url = urlunparse(parsed._replace(query=new_query))

    connect_args: dict = {}
    if sslmode and sslmode not in ("disable", "allow"):
        connect_args["ssl"] = True
    else:
        connect_args["ssl"] = False

    return clean_url, connect_args


settings = get_settings()
_async_url, _connect_args = _build_asyncpg_url(settings.DATABASE_URL)

engine = create_async_engine(
    _async_url,
    connect_args=_connect_args,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
)

AsyncSessionFactory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
