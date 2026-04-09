# Documentation Index

## Documentation Files

| File | Description |
|---|---|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | High-level system design, tech stack, and data flow. |
| [DATA_MODEL.md](./DATA_MODEL.md) | Database schema reference and entity relationships. |
| [API_REFERENCE.md](./API_REFERENCE.md) | REST API endpoints, parameters, and schemas. |
| [CRAWLER_GUIDE.md](./CRAWLER_GUIDE.md) | Details on job ingestion, source support, and deduplication. |
| [ENRICHMENT_GUIDE.md](./ENRICHMENT_GUIDE.md) | Company metadata collection pipeline and source priority. |
| [GEO_CLASSIFICATION.md](./GEO_CLASSIFICATION.md) | Logic for mapping jobs to US, EU, IN, or GLOBAL markets. |
| [CELERY_TASKS.md](./CELERY_TASKS.md) | Background task definitions, schedules, and failure modes. |
| [MATCH_SCORING.md](./MATCH_SCORING.md) | How relevance scores are calculated for user preferences. |
| [DEVELOPMENT.md](./DEVELOPMENT.md) | Local setup, environment variables, and run commands. |
| [AGENT_CONTEXT.md](./AGENT_CONTEXT.md) | Condensed reference for AI coding agents. |

## Coverage Gaps & Implementation Notes
- **Funding Data**: Columns exist in `jobs.companies` but are currently not populated due to a lack of free sources.
- **Deduplication**: The `pg_trgm` similarity threshold is set to `0.85`; this might need tuning based on observed duplicates.
- **Auth**: The service assumes JWT verification is handled correctly by the `get_current_user` dependency, but the token issuer/config is external to this service.

## Maintenance
These documents should be updated whenever:
1. A new database migration is added.
2. A new API endpoint is created or modified.
3. A new crawler or enrichment source is integrated.
4. Core classification or scoring logic is changed.

---
**Last Reviewed**: April 8, 2026
