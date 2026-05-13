"""ATS sources India columns: career_site_url, country, location_filter, notes.

Also widens the unique constraint from (company_id, ats_type) to
(company_id, ats_type, country) so US and IN variants of the same source
can coexist.

Revision ID: 0013
Revises: 0012
Create Date: 2026-05-13
"""
from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.ats_sources
            ADD COLUMN IF NOT EXISTS career_site_url  TEXT,
            ADD COLUMN IF NOT EXISTS country          VARCHAR(2)   NOT NULL DEFAULT 'US',
            ADD COLUMN IF NOT EXISTS location_filter  VARCHAR(255),
            ADD COLUMN IF NOT EXISTS notes            TEXT
    """)

    op.execute("""
        ALTER TABLE jobs.ats_sources
            DROP CONSTRAINT IF EXISTS uq_ats_source
    """)

    op.execute("""
        ALTER TABLE jobs.ats_sources
            ADD CONSTRAINT uq_ats_source_country
            UNIQUE (company_id, ats_type, country)
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.ats_sources
            DROP CONSTRAINT IF EXISTS uq_ats_source_country
    """)
    op.execute("""
        ALTER TABLE jobs.ats_sources
            ADD CONSTRAINT uq_ats_source
            UNIQUE (company_id, ats_type)
    """)
    op.execute("""
        ALTER TABLE jobs.ats_sources
            DROP COLUMN IF EXISTS career_site_url,
            DROP COLUMN IF EXISTS country,
            DROP COLUMN IF EXISTS location_filter,
            DROP COLUMN IF EXISTS notes
    """)
