# Background Task Reference

## Overview
Background tasks are handled by Celery with Redis as the broker. Tasks are divided into crawler-related (`app/crawler/tasks.py`) and enrichment-related (`app/enrichment/tasks.py`).

---

## Crawler & Ingest Tasks

### `app.crawler.tasks.crawl_all_companies`
- **Schedule**: Not strictly scheduled in code; triggered via API or manual trigger.
- **Trigger**: `POST /api/v1/jobs/crawl/trigger`
- **Description**: Iterates through all registered `BaseCrawler` subclasses, fetches listings, and runs the `_upsert_jobs` logic.
- **Reads**: None
- **Writes**: `jobs.listings` (INSERT/UPDATE)
- **Downstream**: Queues `generate_job_summary` for every new listing.

### `app.crawler.tasks.generate_job_summary`
- **Schedule**: On-demand (queued after ingest)
- **Trigger**: `_async_crawl_all`
- **Description**: Uses OpenAI to generate a concise summary, extract tech tags, and parse salary ranges from the raw job description.
- **Reads**: `jobs.listings`
- **Writes**: `jobs.listings` (`summary`, `tags`, `salary_range`)
- **External Calls**: OpenAI API

### `app.crawler.tasks.deactivate_stale_jobs`
- **Schedule**: Nightly / Periodically
- **Trigger**: Celery Beat
- **Description**: Finds active jobs not seen by any crawler in the last 12 hours and marks them inactive.
- **Writes**: `jobs.listings` (`is_active = FALSE`)

### `app.crawler.tasks.send_job_alerts`
- **Schedule**: Periodically
- **Trigger**: Celery Beat
- **Description**: Processes all `saved_searches` with `alert_email=TRUE`. Finds new jobs matching the search filters since the last alert and sends an email.
- **Reads**: `jobs.saved_searches`, `jobs.listings`
- **Writes**: `jobs.saved_searches` (`last_alerted_at`, `last_alerted_job_ids`)
- **External Calls**: Resend API

---

## Enrichment Tasks

### `app.enrichment.tasks.enrich_new_companies`
- **Schedule**: Nightly (02:00 UTC inferred)
- **Trigger**: Celery Beat
- **Description**: Identifies `listings` without a `company_id`, creates a new `company` record if missing, runs the `CompanyEnricher` pipeline, and links the listings.
- **Reads**: `jobs.listings` (DISTINCT company WHERE company_id IS NULL)
- **Writes**: `jobs.companies` (INSERT/UPDATE), `jobs.listings` (UPDATE `company_id`)
- **External Calls**: Wikipedia, LinkedIn, Comparably, BuiltIn, Glassdoor

### `app.enrichment.tasks.reenrich_stale_companies`
- **Schedule**: Weekly (Sunday 03:00 UTC)
- **Trigger**: Celery Beat
- **Description**: Refreshes enrichment data for companies that haven't been updated in 7 days.
- **Reads**: `jobs.companies`
- **Writes**: `jobs.companies` (UPDATE)
- **External Calls**: Same as `enrich_new_companies`
