# AI Agent Context

## Service Overview
The Job Search Service is an asynchronous Python/FastAPI service that crawls, classifies, and enriches job listings. It serves as the primary data engine for the TrackNJob platform, transforming raw career page data into high-value, searchable job listings with AI summaries and company insights.

## Scope Boundaries
- **Responsible for**: Job crawling, geo-restriction classification, AI summarization, company enrichment (scrapers), search API, saved searches, and email alerts.
- **NOT responsible for**: User authentication (handled by external provider/core API), job application tracking (handled by core API), direct employer integrations (uses public APIs only).

## Key File Map

- `app/main.py`: Entry point and global configuration.
- `app/models.py`: Authoritative SQLAlchemy models (listings, companies, preferences).
- `app/api/v1/jobs.py`: Primary search and preference logic.
- `app/crawler/base.py`: Abstract crawler logic and HTTP/JS fetchers.
- `app/crawler/geo_classifier.py`: Critical geo-restriction heuristic logic.
- `app/crawler/tasks.py`: Ingest pipeline, deduplication, and task registration.
- `app/enrichment/enricher.py`: Concurrent orchestration of company metadata scrapers.
- `app/scoring.py`: Match scoring algorithm between jobs and user preferences.

## Naming Conventions
- **Asynchronous**: Most database and network operations use `async/await`.
- **Snake Case**: Standard Python naming for variables and functions.
- **Suffixes**: `_res` for result objects, `_stmt` for SQLAlchemy select statements.

## Database Conventions
- **Schema**: Always use the `jobs` schema (defined in `__table_args__`).
- **Migrations**: Alembic is used; always verify `schema="jobs"` is present in new migration files.
- **Soft Deletes**: Use `is_active = FALSE` instead of deleting listings.

## Critical Business Rules
1. **Deduplication**: Never insert a job with a duplicate `source_url`. For new URLs, use `pg_trgm` similarity (> 0.85) to avoid cross-posting duplicates.
2. **Geo-Restriction**: If a job is "Remote", it is `GLOBAL` only if no regional signals are found. Default is `US`.
3. **Salary Priority**: Listing-derived salary > Enriched salary > Null.
4. **Stale Job Policy**: Any job not seen in 12 hours is considered expired.
5. **Enrichment**: Wikipedia is the "source of truth" for core company facts; LinkedIn is fallback.

## Technical Debt & Limitations
- **Funding Data**: `funding_total_usd` is currently a placeholder (always null).
- **Scraper Fragility**: Comparably/BuiltIn/Glassdoor scrapers rely on public HTML structures and may break if UI changes.
- **Match Scoring**: Title similarity is based on simple SequenceMatcher; could be improved with embeddings.

## Recommended Next Steps
1. **Embeddings**: Replace SequenceMatcher with vector embeddings for better title/skill matching.
2. **ATS Connectors**: Add support for more ATS platforms (Workday, Oracle Cloud).
3. **Paid Enrichment**: Integrate Crunchbase API for reliable funding data.
4. **Proxy Support**: Add rotating proxies to the scrapers to improve reliability and bypass rate limits.
