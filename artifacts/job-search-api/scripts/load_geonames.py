#!/usr/bin/env python3
"""One-time loader: import GeoNames cities500 dataset into geo.cities.

Usage
-----
1. Download cities500.zip from http://download.geonames.org/export/dump/
2. Unzip to get cities500.txt (tab-separated, UTF-8)
3. Run:
       python scripts/load_geonames.py --file /path/to/cities500.txt

The script is idempotent — it truncates and reloads the table on every run
so it can be re-run safely after data updates.

GeoNames cities500.txt column layout (0-indexed):
  0  geonameid
  1  name           ← we use this
  2  asciiname      ← we use this
  3  alternatenames
  4  latitude
  5  longitude
  6  feature class
  7  feature code
  8  country code   ← we use this
  9  cc2
  10 admin1 code
  11 admin2 code
  12 admin3 code
  13 admin4 code
  14 population     ← we use this
  15 elevation
  16 dem
  17 timezone
  18 modification date
"""

import argparse
import asyncio
import csv
import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

BATCH_SIZE = 2000


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


def _parse_cities500(filepath: str):
    """Yield (name, ascii_name, country_code, population) tuples."""
    with open(filepath, encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        for cols in reader:
            if len(cols) < 15:
                continue
            name = cols[1].strip()
            ascii_name = cols[2].strip()
            country_code = cols[8].strip().upper()
            try:
                population = int(cols[14])
            except ValueError:
                population = 0
            if name and country_code and len(country_code) == 2:
                yield name, ascii_name, country_code, population


async def run(filepath: str) -> None:
    Session = _make_session()

    async with Session() as session:
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS geo"))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS geo.cities (
                id           SERIAL PRIMARY KEY,
                name         TEXT NOT NULL,
                ascii_name   TEXT NOT NULL,
                country_code CHAR(2) NOT NULL,
                population   INTEGER NOT NULL DEFAULT 0
            )
        """))
        await session.execute(text("TRUNCATE geo.cities RESTART IDENTITY"))
        await session.commit()
        print("Truncated geo.cities. Loading ...", flush=True)

    total = 0
    batch: list[dict] = []

    async with Session() as session:
        for name, ascii_name, country_code, population in _parse_cities500(filepath):
            batch.append({
                "name": name,
                "ascii_name": ascii_name,
                "country_code": country_code,
                "population": population,
            })
            if len(batch) >= BATCH_SIZE:
                await session.execute(
                    text("""
                        INSERT INTO geo.cities (name, ascii_name, country_code, population)
                        VALUES (:name, :ascii_name, :country_code, :population)
                    """),
                    batch,
                )
                await session.commit()
                total += len(batch)
                print(f"  Inserted {total} rows ...", flush=True)
                batch = []

        if batch:
            await session.execute(
                text("""
                    INSERT INTO geo.cities (name, ascii_name, country_code, population)
                    VALUES (:name, :ascii_name, :country_code, :population)
                """),
                batch,
            )
            await session.commit()
            total += len(batch)

    print(f"\nDone. Total rows loaded: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load GeoNames cities500 into geo.cities")
    parser.add_argument(
        "--file",
        required=True,
        help="Path to cities500.txt (unzipped from download.geonames.org)",
    )
    args = parser.parse_args()
    asyncio.run(run(args.file))
