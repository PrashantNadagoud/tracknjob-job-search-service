#!/usr/bin/env python3
"""One-time backfill script: classify geo_restriction for all legacy listings.

Run after deploying migration 0007:
    python scripts/backfill_geo_restriction.py

Processes all rows where geo_restriction IS NULL in batches of 500.
Prints a summary breakdown at the end.
"""

import asyncio
import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from typing import Any

# Allow running from the repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.crawler.geo_classifier import classify_listing

BATCH_SIZE = 500


def _make_session() -> async_sessionmaker[AsyncSession]:
    raw_url = os.environ["DATABASE_URL"]
    parsed = urlparse(raw_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("sslmode", None)
    new_query = urlencode({k: v[0] for k, v in params.items()})
    clean_url = urlunparse(
        parsed._replace(scheme="postgresql+asyncpg", query=new_query)
    )
    engine = create_async_engine(clean_url, poolclass=pool.NullPool)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def run() -> None:
    Session = _make_session()
    counts: dict[str, int] = {"US": 0, "EU": 0, "IN": 0, "GLOBAL": 0}
    total = 0

    async with Session() as session:
        while True:
            rows = (
                await session.execute(
                    text("""
                        SELECT id, location, summary, remote, country
                        FROM jobs.listings
                        WHERE geo_restriction IS NULL
                        LIMIT :batch
                    """),
                    {"batch": BATCH_SIZE},
                )
            ).fetchall()

            if not rows:
                break

            for row in rows:
                row_id, location, summary, remote, country = row

                # Use the legacy country field as a structured hint
                country_hint: str | None = None
                if country and country.upper() in ("US", "IN"):
                    country_hint = country.upper()

                work_type = "remote" if remote else ""
                geo = classify_listing(
                    location_raw=location or "",
                    description=summary or "",
                    work_type=work_type,
                    country=country_hint,
                )

                await session.execute(
                    text(
                        "UPDATE jobs.listings "
                        "SET geo_restriction = :geo "
                        "WHERE id = :id"
                    ),
                    {"geo": geo, "id": row_id},
                )
                counts[geo] = counts.get(geo, 0) + 1
                total += 1

            await session.commit()
            print(f"  Processed {total} rows so far ...", flush=True)

    print()
    print(f"Total processed: {total}")
    print(f"  US:     {counts.get('US', 0)}")
    print(f"  EU:     {counts.get('EU', 0)}")
    print(f"  IN:     {counts.get('IN', 0)}")
    print(f"  GLOBAL: {counts.get('GLOBAL', 0)}")


if __name__ == "__main__":
    asyncio.run(run())
