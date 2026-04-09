# Database Schema Reference

## Overview
The service uses a PostgreSQL database with a dedicated `jobs` schema. All tables are defined using SQLAlchemy ORM in `app/models.py`.

## Entity Relationship Diagram

```text
[job_preferences]
    (user_id) PK

[saved_searches]
    (id) PK
    (user_id)

[hidden_jobs]
    (user_id, job_id) PK

[companies] <───────────┐
    (id) PK             │
    (slug) UNIQUE       │
                        │ (1:N)
[listings] ─────────────┘
    (id) PK
    (company_id) FK -> companies.id
    (source_url) UNIQUE
```

## Table: jobs.companies
Stores enriched data about companies found in job listings.

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| id | UUID | No | gen_random_uuid() | Primary key |
| slug | Text | No | - | URL-friendly identifier (unique) |
| name | Text | No | - | Full company name |
| website | Text | Yes | - | Corporate website URL |
| funding_total_usd | BigInteger | Yes | - | Total funding raised (preserved for compat) |
| last_funding_type | Text | Yes | - | e.g., Series B, Seed |
| last_funding_date | Date | Yes | - | Date of last funding round |
| num_employees_range| Text | Yes | - | e.g., "1001-5000" |
| founded_year | Integer | Yes | - | Year founded |
| culture_score | Text | Yes | - | Culture rating (e.g., from Comparably) |
| ceo_approval_pct | Integer | Yes | - | CEO approval percentage |
| work_life_score | Numeric(3,1)| Yes | - | Work-life balance score |
| remote_policy | Text | Yes | - | e.g., "Remote-first", "Hybrid" |
| perks | JSONB | Yes | - | List of company perks/benefits |
| salary_min_usd | Integer | Yes | - | Estimated min salary for SE roles |
| salary_max_usd | Integer | Yes | - | Estimated max salary for SE roles |
| salary_source | Text | Yes | - | Source of salary data (Glassdoor, etc.) |
| enriched_at | TIMESTAMP | Yes | - | Last enrichment timestamp |
| enrichment_source | ARRAY(Text) | Yes | - | List of sources used (wikipedia, linkedin, etc.) |
| company_type | Text | No | "unknown" | "public", "private", "subsidiary", etc. |
| stock_ticker | Text | Yes | - | Stock symbol |
| stock_exchange | Text | Yes | - | Exchange (NYSE, NASDAQ, etc.) |

**Indexes:**
- Primary Key on `id`
- Unique index on `slug`

---

## Table: jobs.listings
Stores individual job postings crawled from various sources.

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| id | UUID | No | gen_random_uuid() | Primary key |
| title | Text | No | - | Job title |
| company | Text | No | - | Raw company name from source |
| location | Text | Yes | - | Raw location string |
| remote | Boolean | No | false | Whether the job is remote |
| source_url | Text | No | - | Original job posting URL (unique) |
| source_label | Text | Yes | - | Human-readable source name |
| posted_at | TIMESTAMP | Yes | - | Original posting date |
| crawled_at | TIMESTAMP | Yes | now() | Date listing was first crawled |
| summary | Text | Yes | - | AI-generated summary |
| tags | ARRAY(Text) | Yes | - | AI-extracted skills/tags |
| salary_range | Text | Yes | - | Salary range from listing (if any) |
| is_active | Boolean | No | true | Soft-delete flag for expired jobs |
| country | String(2) | No | "US" | ISO country code |
| last_seen_at | TIMESTAMP | Yes | now() | Last time crawler saw this listing |
| company_id | UUID | Yes | - | FK to jobs.companies |
| geo_restriction | Text | Yes | - | US, EU, IN, or GLOBAL |

**Indexes:**
- Primary Key on `id`
- Unique index on `source_url`
- `idx_jobs_fts`: GIN index on `title`, `company`, `location` for full-text search
- `idx_jobs_remote`: B-tree on `remote`
- `idx_jobs_posted_at`: B-tree on `posted_at DESC`
- `idx_jobs_company`: B-tree on `company`
- `idx_jobs_last_seen`: B-tree on `last_seen_at`
- `idx_jobs_title_trgm`: GIN index with `gin_trgm_ops` for partial title matching

---

## Table: jobs.saved_searches
Stores user-defined search filters and alert preferences.

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| id | UUID | No | gen_random_uuid() | Primary key |
| user_id | UUID | No | - | Owner user ID |
| name | Text | No | - | Search name |
| filters | JSONB | No | - | JSON object of search parameters |
| alert_email | Boolean | No | false | Whether to send email alerts |
| user_email | Text | Yes | - | Email address for alerts |
| last_alerted_at | TIMESTAMP | Yes | - | Last alert sent timestamp |
| last_alerted_job_ids| JSONB | Yes | '[]' | List of job IDs included in last alert |
| created_at | TIMESTAMP | Yes | now() | Creation timestamp |

---

## Table: jobs.hidden_jobs
Tracks jobs that users have explicitly hidden from their feed.

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| user_id | UUID | No | - | User who hid the job |
| job_id | UUID | No | - | Job ID that was hidden |

**Constraints:**
- Composite Primary Key on `(user_id, job_id)`

---

## Table: jobs.job_preferences
Stores user preferences for match scoring.

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| user_id | UUID | No | - | Primary key |
| desired_title | Text | Yes | - | Preferred job title |
| skills | ARRAY(Text) | Yes | '{}' | List of user's skills |
| preferred_location | Text | Yes | - | Preferred location |
| remote_only | Boolean | No | false | Remote preference |
| seniority | Text | Yes | - | e.g., "Senior", "Junior" |
| updated_at | TIMESTAMP | Yes | now() | Last update timestamp |
