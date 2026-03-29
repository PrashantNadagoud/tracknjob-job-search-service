# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Structure

```text
artifacts-monorepo/
├── artifacts/              # Deployable applications
│   └── api-server/         # Express API server
├── lib/                    # Shared libraries
│   ├── api-spec/           # OpenAPI spec + Orval codegen config
│   ├── api-client-react/   # Generated React Query hooks
│   ├── api-zod/            # Generated Zod schemas from OpenAPI
│   └── db/                 # Drizzle ORM schema + DB connection
├── scripts/                # Utility scripts (single workspace package)
│   └── src/                # Individual .ts scripts, run via `pnpm --filter @workspace/scripts run <script>`
├── pnpm-workspace.yaml     # pnpm workspace (artifacts/*, lib/*, lib/integrations/*, scripts)
├── tsconfig.base.json      # Shared TS options (composite, bundler resolution, es2022)
├── tsconfig.json           # Root TS project references
└── package.json            # Root package with hoisted devDeps
```

## TypeScript & Composite Projects

Every package extends `tsconfig.base.json` which sets `composite: true`. The root `tsconfig.json` lists all packages as project references. This means:

- **Always typecheck from the root** — run `pnpm run typecheck` (which runs `tsc --build --emitDeclarationOnly`). This builds the full dependency graph so that cross-package imports resolve correctly. Running `tsc` inside a single package will fail if its dependencies haven't been built yet.
- **`emitDeclarationOnly`** — we only emit `.d.ts` files during typecheck; actual JS bundling is handled by esbuild/tsx/vite...etc, not `tsc`.
- **Project references** — when package A depends on package B, A's `tsconfig.json` must list B in its `references` array. `tsc --build` uses this to determine build order and skip up-to-date packages.

## Root Scripts

- `pnpm run build` — runs `typecheck` first, then recursively runs `build` in all packages that define it
- `pnpm run typecheck` — runs `tsc --build --emitDeclarationOnly` using project references

## Packages

### `artifacts/api-server` (`@workspace/api-server`)

Express 5 API server. Routes live in `src/routes/` and use `@workspace/api-zod` for request and response validation and `@workspace/db` for persistence.

- Entry: `src/index.ts` — reads `PORT`, starts Express
- App setup: `src/app.ts` — mounts CORS, JSON/urlencoded parsing, routes at `/api`
- Routes: `src/routes/index.ts` mounts sub-routers; `src/routes/health.ts` exposes `GET /health` (full path: `/api/health`)
- Depends on: `@workspace/db`, `@workspace/api-zod`
- `pnpm --filter @workspace/api-server run dev` — run the dev server
- `pnpm --filter @workspace/api-server run build` — production esbuild bundle (`dist/index.cjs`)
- Build bundles an allowlist of deps (express, cors, pg, drizzle-orm, zod, etc.) and externalizes the rest

### `lib/db` (`@workspace/db`)

Database layer using Drizzle ORM with PostgreSQL. Exports a Drizzle client instance and schema models.

- `src/index.ts` — creates a `Pool` + Drizzle instance, exports schema
- `src/schema/index.ts` — barrel re-export of all models
- `src/schema/<modelname>.ts` — table definitions with `drizzle-zod` insert schemas (no models definitions exist right now)
- `drizzle.config.ts` — Drizzle Kit config (requires `DATABASE_URL`, automatically provided by Replit)
- Exports: `.` (pool, db, schema), `./schema` (schema only)

Production migrations are handled by Replit when publishing. In development, we just use `pnpm --filter @workspace/db run push`, and we fallback to `pnpm --filter @workspace/db run push-force`.

### `lib/api-spec` (`@workspace/api-spec`)

Owns the OpenAPI 3.1 spec (`openapi.yaml`) and the Orval config (`orval.config.ts`). Running codegen produces output into two sibling packages:

1. `lib/api-client-react/src/generated/` — React Query hooks + fetch client
2. `lib/api-zod/src/generated/` — Zod schemas

Run codegen: `pnpm --filter @workspace/api-spec run codegen`

### `lib/api-zod` (`@workspace/api-zod`)

Generated Zod schemas from the OpenAPI spec (e.g. `HealthCheckResponse`). Used by `api-server` for response validation.

### `lib/api-client-react` (`@workspace/api-client-react`)

Generated React Query hooks and fetch client from the OpenAPI spec (e.g. `useHealthCheck`, `healthCheck`).

### `scripts` (`@workspace/scripts`)

Utility scripts package. Each script is a `.ts` file in `src/` with a corresponding npm script in `package.json`. Run scripts via `pnpm --filter @workspace/scripts run <script>`. Scripts can import any workspace package (e.g., `@workspace/db`) by adding it as a dependency in `scripts/package.json`.

---

## TrackNJob — Job Search API (Python microservice)

Standalone Python FastAPI service at `artifacts/job-search-api/`. All work is pushed to `PrashantNadagoud/tracknjob-job-search-service` on GitHub.

### Stack
- **Python 3.10**, FastAPI 0.104+
- **SQLAlchemy 2.0 async** (asyncpg), Alembic migrations
- **PostgreSQL 14** — `jobs` schema, `pg_trgm` extension for FTS
- **Redis + Celery 5** beat scheduler
- **JWT HS256** via python-jose (`TNJ_SECRET_KEY` env var)
- **httpx + BeautifulSoup4** for enrichment scraping

### Features (Sessions 1–13)
- Auth: HS256 JWT, `get_current_user` dependency
- Search: FTS (tsvector), remote/country/source/company/posted filters, pagination, sort by match_score
- Ghost job detection: pg_trgm similarity > 0.85, stale deactivation
- Saved searches + job alerts via Resend email
- Hidden jobs per user
- Job match scoring against user preferences
- Company Intel & Salary Enrichment Pipeline (Session 13):
  - `jobs.companies` table with 20+ fields (migration 0006)
  - `company_id` FK on `jobs.listings`
  - `CompanyEnricher`: Crunchbase, Comparably, BuiltIn, Glassdoor (concurrent via `asyncio.gather`)
  - Wikipedia API fallback + Yahoo Finance for public companies
  - Celery beat: `enrich_new_companies` (nightly 2AM UTC), `reenrich_stale_companies` (Sunday 3AM UTC)
  - `GET /api/v1/companies/{slug}` endpoint
  - `company_summary` nested object on search results (null-fields omitted via Pydantic `model_serializer`)
  - Salary display rule: `company_listed` wins over Glassdoor; both absent → keys omitted entirely

### Migrations
- 0001: initial (listings, users)
- 0002: country column
- 0003: ghost job detection (pg_trgm)
- 0004: job alerts (saved_searches)
- 0005: job preferences (match scoring)
- 0006: companies table + company_id FK on listings

### Tests
- 53 pytest tests (asyncio_mode=auto, NullPool engine, autouse cleanup by UUID + source_url prefix)
- `pytest.ini` in `artifacts/job-search-api/`
- CI: `.github/workflows/test.yml` (PostgreSQL 14 service, pg_trgm, Alembic, pytest)
