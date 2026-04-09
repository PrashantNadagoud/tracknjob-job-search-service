"""ATS infrastructure: ats_sources, company_discovery_queue, crawl_dead_letters,
and new columns on listings / companies.

Revision ID: 0008
Revises: 0007
Create Date: 2026-04-09
"""
from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── New table: jobs.ats_sources ─────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS jobs.ats_sources (
            id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            company_id           UUID NOT NULL REFERENCES jobs.companies(id) ON DELETE CASCADE,
            ats_type             TEXT NOT NULL,
            ats_slug             TEXT,
            crawl_url            TEXT,
            market               TEXT NOT NULL DEFAULT 'US',
            is_active            BOOLEAN NOT NULL DEFAULT true,
            last_crawled_at      TIMESTAMPTZ,
            last_crawl_status    TEXT,
            last_crawl_job_count INTEGER,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            backoff_until        TIMESTAMPTZ,
            discovery_source     TEXT,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_ats_source UNIQUE (company_id, ats_type)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_ats_sources_company "
        "ON jobs.ats_sources(company_id)"
    )
    # NOTE: The spec's partial predicate used `backoff_until < now()` which is
    # a STABLE function — PostgreSQL requires IMMUTABLE expressions in index
    # predicates. The WHERE clause is therefore simplified to `is_active = true`
    # and the query layer handles the backoff_until check at runtime.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_ats_sources_crawl_due "
        "ON jobs.ats_sources(last_crawled_at ASC NULLS FIRST) "
        "WHERE is_active = true"
    )

    # ── New table: jobs.company_discovery_queue ─────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS jobs.company_discovery_queue (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            company_name        TEXT NOT NULL,
            website             TEXT,
            suspected_ats       TEXT,
            suspected_slug      TEXT,
            source              TEXT NOT NULL,
            market              TEXT NOT NULL DEFAULT 'US',
            status              TEXT NOT NULL DEFAULT 'pending',
            resolved_company_id UUID REFERENCES jobs.companies(id),
            attempt_count       INTEGER NOT NULL DEFAULT 0,
            last_attempted_at   TIMESTAMPTZ,
            error_message       TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_discovery_queue_pending "
        "ON jobs.company_discovery_queue(created_at) "
        "WHERE status = 'pending'"
    )

    # ── New table: jobs.crawl_dead_letters ──────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS jobs.crawl_dead_letters (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            ats_source_id   UUID REFERENCES jobs.ats_sources(id) ON DELETE SET NULL,
            ats_type        TEXT NOT NULL,
            ats_slug        TEXT,
            error_type      TEXT NOT NULL,
            http_status     INTEGER,
            error_message   TEXT,
            raw_response    TEXT,
            attempted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            resolved        BOOLEAN NOT NULL DEFAULT false,
            resolved_at     TIMESTAMPTZ,
            resolution_note TEXT
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_dead_letters_unresolved "
        "ON jobs.crawl_dead_letters(attempted_at DESC) "
        "WHERE resolved = false"
    )

    # ── New columns on jobs.listings ────────────────────────────────────────
    op.execute("""
        ALTER TABLE jobs.listings
            ADD COLUMN IF NOT EXISTS ats_type          TEXT,
            ADD COLUMN IF NOT EXISTS ats_source_id     UUID
                REFERENCES jobs.ats_sources(id) ON DELETE SET NULL,
            ADD COLUMN IF NOT EXISTS external_job_id   TEXT,
            ADD COLUMN IF NOT EXISTS title_normalized  TEXT,
            ADD COLUMN IF NOT EXISTS seniority_level   TEXT,
            ADD COLUMN IF NOT EXISTS employment_type   TEXT,
            ADD COLUMN IF NOT EXISTS department        TEXT,
            ADD COLUMN IF NOT EXISTS salary_currency   CHAR(3) NOT NULL DEFAULT 'USD',
            ADD COLUMN IF NOT EXISTS salary_min_local  NUMERIC(12,2),
            ADD COLUMN IF NOT EXISTS salary_max_local  NUMERIC(12,2),
            ADD COLUMN IF NOT EXISTS salary_period     TEXT DEFAULT 'annual',
            ADD COLUMN IF NOT EXISTS expires_at        TIMESTAMPTZ
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_ats_dedup
            ON jobs.listings(company_id, ats_type, external_job_id)
            WHERE external_job_id IS NOT NULL AND company_id IS NOT NULL
    """)

    # ── New columns on jobs.companies ───────────────────────────────────────
    op.execute("""
        ALTER TABLE jobs.companies
            ADD COLUMN IF NOT EXISTS crunchbase_uuid    TEXT,
            ADD COLUMN IF NOT EXISTS linkedin_slug      TEXT,
            ADD COLUMN IF NOT EXISTS primary_ats_type   TEXT,
            ADD COLUMN IF NOT EXISTS india_presence     BOOLEAN,
            ADD COLUMN IF NOT EXISTS india_offices      TEXT[],
            ADD COLUMN IF NOT EXISTS description        TEXT,
            ADD COLUMN IF NOT EXISTS hq_city            TEXT,
            ADD COLUMN IF NOT EXISTS hq_country         CHAR(2),
            ADD COLUMN IF NOT EXISTS categories         TEXT[],
            ADD COLUMN IF NOT EXISTS last_enrichment_attempt TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS enrichment_failures INTEGER NOT NULL DEFAULT 0
    """)


def downgrade() -> None:
    # Drop new company columns
    op.execute("""
        ALTER TABLE jobs.companies
            DROP COLUMN IF EXISTS crunchbase_uuid,
            DROP COLUMN IF EXISTS linkedin_slug,
            DROP COLUMN IF EXISTS primary_ats_type,
            DROP COLUMN IF EXISTS india_presence,
            DROP COLUMN IF EXISTS india_offices,
            DROP COLUMN IF EXISTS description,
            DROP COLUMN IF EXISTS hq_city,
            DROP COLUMN IF EXISTS hq_country,
            DROP COLUMN IF EXISTS categories,
            DROP COLUMN IF EXISTS last_enrichment_attempt,
            DROP COLUMN IF EXISTS enrichment_failures
    """)
    # Drop new listing columns (index is dropped automatically)
    op.execute("DROP INDEX IF EXISTS jobs.idx_listings_ats_dedup")
    op.execute("""
        ALTER TABLE jobs.listings
            DROP COLUMN IF EXISTS ats_type,
            DROP COLUMN IF EXISTS ats_source_id,
            DROP COLUMN IF EXISTS external_job_id,
            DROP COLUMN IF EXISTS title_normalized,
            DROP COLUMN IF EXISTS seniority_level,
            DROP COLUMN IF EXISTS employment_type,
            DROP COLUMN IF EXISTS department,
            DROP COLUMN IF EXISTS salary_currency,
            DROP COLUMN IF EXISTS salary_min_local,
            DROP COLUMN IF EXISTS salary_max_local,
            DROP COLUMN IF EXISTS salary_period,
            DROP COLUMN IF EXISTS expires_at
    """)
    # Drop new tables (reverse creation order to respect FK deps)
    op.execute("DROP TABLE IF EXISTS jobs.crawl_dead_letters")
    op.execute("DROP TABLE IF EXISTS jobs.company_discovery_queue")
    op.execute("DROP TABLE IF EXISTS jobs.ats_sources")
