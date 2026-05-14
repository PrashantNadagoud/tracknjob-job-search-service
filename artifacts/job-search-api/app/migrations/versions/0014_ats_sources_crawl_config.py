"""Add crawl_config JSONB column to jobs.ats_sources.

This column was present in the ORM model (AtsSource.crawl_config) but
was accidentally omitted from the original 0008_ats_infrastructure migration.
Workday sources use it to cache the probed instance + career_site_name so the
crawler can call the CXS API directly without slug-brute-forcing on every run.

Revision ID: 0014
Revises: 0013
Create Date: 2026-05-13
"""
from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.ats_sources
            ADD COLUMN IF NOT EXISTS crawl_config JSONB NOT NULL DEFAULT '{}'
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.ats_sources
            DROP COLUMN IF EXISTS crawl_config
    """)
