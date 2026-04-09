# System Architecture

## Overview
The Job Search Service is a backend component of the TrackNJob platform responsible for crawling, ingesting, classifying, and enriching job listings from various career pages and ATS platforms. It provides a REST API for the frontend to search and filter jobs with advanced features like geo-restriction classification and AI-powered summarization.

## Technology Stack
- **Language**: Python 3.11+
- **Framework**: FastAPI (Asynchronous REST API)
- **Database**: PostgreSQL (with `pg_trgm` and `tsvector` for search)
- **Task Queue**: Celery (with Redis as broker)
- **Worker/Beat**: Celery Worker for task execution, Celery Beat for scheduling
- **HTTP Client**: `httpx` (for API-based crawling)
- **Browser Automation**: Playwright (for JS-rendered pages)
- **AI/LLM**: OpenAI API (for job summarization and tagging)
- **Email**: Resend (for job alerts)

## System Boundaries
- **Owned**: Job listings, company data, user preferences, saved searches, enrichment logic, and geo-classification.
- **External Calls**:
    - **ATS APIs**: Greenhouse, Ashby, Lever (via public boards APIs)
    - **Enrichment**: Wikipedia, LinkedIn, Comparably, BuiltIn, Glassdoor, Yahoo Finance
    - **AI**: OpenAI (gpt-3.5-turbo/gpt-4)
    - **Email**: Resend API

## Data Flow Diagram

```text
[External Job Boards / ATS APIs]
        |
        v
  [Crawler Layer] (app/crawler/)
  (Greenhouse, Lever, Ashby, Stripe, Notion, etc.)
        |
        v
  [Geo Classifier] (app/crawler/geo_classifier.py)
        |
        v
  [Ingest Pipeline] (app/crawler/tasks.py -> _upsert_jobs)
        |
        v
  [PostgreSQL: jobs.listings]
        |
        v
  [Celery Beat Tasks] (Scheduled)
        |
        +------> [Job Summarizer] (app/crawler/summarizer.py) -> OpenAI
        |
        +------> [Company Enricher] (app/enrichment/enricher.py)
        |           |
        |           v
        |        [Wikipedia, LinkedIn, Comparably, BuiltIn, Glassdoor]
        |
        v
  [PostgreSQL: jobs.companies]
        |
        v
  [FastAPI REST API] (app/api/v1/jobs.py)
        |
        v
  [TrackNJob Frontend]
```

## Enrichment Pipeline Flow
1. **New Listing Ingested**: `_upsert_jobs` identifies new listings.
2. **Summarization**: `generate_job_summary` task uses OpenAI to extract tags and create a concise summary.
3. **Company Discovery**: `enrich_new_companies` task finds listings without a `company_id`.
4. **Orchestration**: `CompanyEnricher` runs multiple scrapers/API calls concurrently (Wikipedia, LinkedIn, Comparably, BuiltIn, Glassdoor).
5. **Data Merging**: Wikipedia is the primary source for corporate data; LinkedIn fills gaps. Comparably/BuiltIn provide culture and perks.
6. **Persistence**: Company record updated; all associated listings linked via `company_id`.

## Geo-Restriction Classification
- **Classification Point**: Happens during the crawl/ingest phase in the `BaseCrawler` subclasses.
- **Logic**: Uses `app/crawler/geo_classifier.py` which combines structured country data (if available from ATS) with heuristic keyword matching in location strings and descriptions.
- **Market Mapping**: The API `?market=` parameter filters based on these pre-computed `geo_restriction` labels.

## Deployment Topology
Inferable from `docker-compose.yml`, `railway.json`, and `Procfile`:
- **Web Process**: FastAPI running with `uvicorn`.
- **Worker Process**: Celery worker for background tasks.
- **Beat Process**: Celery beat for task scheduling.
- **Managed Services**: PostgreSQL (Railway/Neon), Redis (Railway/Upstash).
