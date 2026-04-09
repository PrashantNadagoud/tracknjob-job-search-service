# REST API Reference

## Authentication
Most endpoints require a Bearer Token in the `Authorization` header.
`Authorization: Bearer <jwt_token>`

The token's `sub` claim is used to identify the user.

---

## Jobs API

### GET /api/v1/jobs/search
Returns paginated job listings with filtering and match scoring.

**Query Params:**

| Param | Type | Required | Default | Description |
|---|---|---|---|---|
| q | string | No | - | Full-text search (title, company, location) |
| location | string | No | - | Partial location match |
| remote | boolean | No | false | Remote-only filter |
| source | string | No | - | Filter by `source_label` |
| company | string | No | - | Partial company name match |
| posted | string | No | "any" | Recency: `24h`, `3d`, `7d`, `30d`, `any` |
| country | string | No | "US" | Country filter: `US`, `IN`, `ALL` |
| market | string | No | "US" | Geo-restriction: `US`, `EU`, `IN` |
| sort_by | string | No | "posted_at" | `posted_at`, `match_score` |
| page | integer | No | 1 | Page number (>= 1) |
| limit | integer | No | 20 | Results per page (1-50) |

**Response:** `JobSearchResponse`
```json
{
  "total": 142,
  "page": 1,
  "limit": 20,
  "results": [
    {
      "id": "uuid",
      "title": "Software Engineer",
      "company": "Stripe",
      "location": "San Francisco, CA",
      "remote": false,
      "posted_at": "2024-03-20T10:00:00Z",
      "source_url": "...",
      "source_label": "Stripe Careers",
      "summary": "AI summary text...",
      "tags": ["Python", "React"],
      "match_score": 85,
      "match_label": "Strong Match",
      "company_summary": {
        "company_type": "private",
        "employee_range": "5001-10000",
        "culture_score": "4.5"
      }
    }
  ]
}
```

---

### GET /api/v1/jobs/{job_id}
Returns full details for a single job listing.

**Response:** `JobListingDetail`

---

### GET /api/v1/jobs/sources
Lists all active job sources and their listing counts.

**Response:** `JobSourcesResponse`

---

### POST /api/v1/jobs/hidden
Hides a job listing for the current user.

**Request Body:** `HideJobRequest`
```json
{
  "job_id": "uuid"
}
```

---

## User Preferences & Searches

### GET /api/v1/jobs/preferences
Retrieve current user's job preferences for match scoring.

**Response:** `JobPreferencesResponse`

---

### POST /api/v1/jobs/preferences
Upsert (create or update) job preferences.

**Request Body:** `JobPreferencesCreate`
```json
{
  "desired_title": "Senior Backend Engineer",
  "skills": ["Python", "PostgreSQL", "FastAPI"],
  "preferred_location": "Remote",
  "remote_only": true,
  "seniority": "Senior"
}
```

---

### GET /api/v1/jobs/saved-searches
List all saved searches for the user.

**Response:** `SavedSearchListResponse`

---

### POST /api/v1/jobs/saved-searches
Save current search filters.

**Request Body:** `SavedSearchCreate`
```json
{
  "name": "Backend Remote US",
  "filters": {
    "q": "Backend",
    "remote": true,
    "market": "US"
  },
  "alert_email": true,
  "user_email": "user@example.com"
}
```

---

### DELETE /api/v1/jobs/saved-searches/{search_id}
Delete a saved search.

---

## Maintenance (Admin Only)

### POST /api/v1/jobs/crawl/trigger
Manually trigger a crawl for a specific country or all.

**Request Body:** `CrawlTriggerRequest`
```json
{
  "country": "US"
}
```

### POST /api/v1/jobs/maintenance/trigger-alerts
Manually trigger processing of job alert emails.

### POST /api/v1/jobs/maintenance/deactivate-stale
Manually trigger deactivation of listings not seen in 12 hours.

---

## Health Check
### GET /health
Basic health check endpoint. No auth required.
