# Company Enrichment Pipeline Guide

## Overview
The `CompanyEnricher` (`app/enrichment/enricher.py`) orchestrates the collection of additional metadata for companies discovered during job crawls. It uses free/public sources only.

## Enrichment Sources

| Source | Method | Fields Provided |
|---|---|---|
| **Wikipedia** | REST API | `founded_year`, `employee_range`, `company_type`, `stock_ticker` |
| **LinkedIn** | Public Scrape | `employee_range`, `founded_year`, `website` |
| **Comparably**| Public Scrape | `culture_score`, `ceo_approval_pct`, `work_life_score` |
| **BuiltIn** | Public Scrape | `remote_policy`, `perks` |
| **Glassdoor** | Salary Scrape | `salary_min_usd`, `salary_max_usd` |
| **Yahoo Finance**| Sequential | `stock_exchange` (triggered if ticker is found) |

## Orchestration Logic
1. **Concurrent Fetching**: Uses `asyncio.gather` to trigger all scrapers simultaneously.
2. **Rate Limiting**: Wraps specific sources (Comparably, BuiltIn, Glassdoor) with a `_rate_limited` helper to avoid detection.
3. **Merge Priority**:
    - **Wikipedia** is the primary source for basic corporate data.
    - **LinkedIn** fills gaps if Wikipedia fails or has missing fields.
    - **Comparably/BuiltIn** are used for culture and policy data.
4. **Post-Processing**: If a `stock_ticker` is identified, a secondary call to Yahoo Finance resolves the `stock_exchange`.

## Key Implementation Details

### Slug Generation
The `generate_slugs()` function produces source-specific slug variants from the raw company name (e.g., "Google, Inc." -> "Google" for Wikipedia, "google-inc" for LinkedIn).

### Salary Data Priority
1. **Listing Data**: If a job listing contains a `salary_range`, it takes absolute precedence.
2. **Glassdoor**: If no listing data exists, enriched Glassdoor data for "Software Engineer" roles is used as a fallback.
3. **Null**: If neither is available, salary fields remain null.

### Funding Information
**IMPORTANT**: Funding fields (`funding_total_usd`, `last_funding_type`) are currently **NOT IMPLEMENTED** because no free public source reliably provides this data via scrape. Columns exist in the database for future compatibility with paid APIs like Crunchbase.

### Re-enrichment Schedule
- **New Companies**: `enrich_new_companies` task runs nightly to pick up any newly crawled companies.
- **Stale Data**: `reenrich_stale_companies` runs weekly (Sunday 3 AM UTC) to refresh data for existing companies.

## Failure Behavior
The `CompanyEnricher` is resilient:
- If a single source fails, its exception is caught individually.
- The `enrichment_source` array tracks which sources successfully contributed data.
- Partial data is saved even if some scrapers fail.
- If all sources fail, the company record remains with just the name and slug, and `enriched_at` is updated to prevent immediate retries.
