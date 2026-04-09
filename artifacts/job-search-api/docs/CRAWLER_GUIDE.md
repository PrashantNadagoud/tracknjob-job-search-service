# Crawler & Ingest Pipeline Guide

## Overview
The crawler system is built on a modular architecture where each company or job board has its own crawler class inheriting from `BaseCrawler` (`app/crawler/base.py`).

## Supported Sources

### 1. Greenhouse ATS
- **Fetching**: JSON via `boards-api.greenhouse.io/v1/boards/{slug}/jobs`
- **Companies**: Cloudflare, Vercel
- **Geo Logic**: Parses `offices[]` array for structured location and country codes.

### 2. Ashby ATS
- **Fetching**: JSON API (internal boards API)
- **Companies**: Linear, Notion
- **Geo Logic**: Parses `officeLocations[]` for structured country codes; detects `workplaceType`.

### 3. Lever ATS
- **Fetching**: JSON API (`api.lever.co/v0/postings/{slug}`)
- **Companies**: (Inferred support via `geo_classifier.py` and base logic)

### 4. Custom API/Scrapers
- **Stripe**: Custom JSON API crawler.
- **Amazon (India)**: Scrapes specific India-filtered job paths.
- **Google (India)**: Scrapes Career page with India filters.
- **Microsoft (India)**: Scrapes Career page with India filters.
- **Flipkart**: Custom scraper for Flipkart career site.
- **Razorpay**: Custom scraper for Razorpay career site.

## Core Logic

### Deduplication (`app/crawler/tasks.py`)
1. **URL Match**: First checks if `source_url` exists in `jobs.listings`.
2. **Ghost Job Recovery**: If URL matches but `is_active=false`, it reactivates the job and updates `last_seen_at`.
3. **Similarity Check**: If URL is new, it runs a `pg_trgm` similarity check on `(title, company, country)` for jobs posted in the last 30 days.
4. **Threshold**: Similarity > 0.85 is considered a duplicate. The existing job's `last_seen_at` is bumped, but no new record is inserted.

### Geo-Restriction Classification
The `classify_listing()` function in `geo_classifier.py` determines the `geo_restriction` column:
- **Priority 1**: Structured `country` field from ATS.
- **Priority 2**: Keyword signals in location text or description (e.g., "EMEIA", "APAC", "Remote US").
- **Priority 3**: `work_type` fallback — if `remote`, it becomes `GLOBAL`.
- **Default**: Defaults to `US`.

### Work Type Determination
- **Ashby**: Uses `workplaceType` field (Remote, Hybrid, Onsite).
- **Greenhouse**: Heuristic check for "remote" in location strings.
- **Base**: Standardized to `remote`, `hybrid`, or `onsite`.

### Job Expiry (`deactivate_stale_jobs`)
A scheduled task runs every 12 hours. Any job listing with `last_seen_at` older than 12 hours is marked `is_active = false`. This ensures that if a job is removed from the company careers page, it disappears from TrackNJob within 12-24 hours.

## Adding a New Crawler
1. Create a new file in `app/crawler/companies/`.
2. Inherit from `BaseCrawler`.
3. Implement `fetch_jobs()` returning the required schema.
4. Register the crawler in `_async_crawl_all()` in `app/crawler/tasks.py`.
