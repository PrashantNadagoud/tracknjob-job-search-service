"""Add jobs.companies table and company_id FK on listings

Revision ID: 0006
Revises: 0005
Create Date: 2026-03-29
"""
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS jobs.companies (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            slug                TEXT UNIQUE NOT NULL,
            name                TEXT NOT NULL,
            website             TEXT,
            funding_total_usd   BIGINT,
            last_funding_type   TEXT,
            last_funding_date   DATE,
            num_employees_range TEXT,
            founded_year        INT,
            culture_score       TEXT,
            ceo_approval_pct    INT,
            work_life_score     DECIMAL(3,1),
            remote_policy       TEXT,
            perks               JSONB,
            salary_min_usd      INT,
            salary_max_usd      INT,
            salary_source       TEXT,
            enriched_at         TIMESTAMPTZ,
            enrichment_source   TEXT[],
            company_type        TEXT DEFAULT 'unknown',
            stock_ticker        TEXT,
            stock_exchange      TEXT
        )
    """)
    op.execute("""
        ALTER TABLE jobs.listings
        ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES jobs.companies(id)
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE jobs.listings DROP COLUMN IF EXISTS company_id")
    op.execute("DROP TABLE IF EXISTS jobs.companies")
