"""Initial schema: jobs.listings, jobs.saved_searches, jobs.hidden_jobs

Revision ID: 0001
Revises:
Create Date: 2026-03-21

"""
from typing import Sequence, Union

from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS jobs")

    op.execute("""
        CREATE TABLE jobs.listings (
          id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          title         TEXT NOT NULL,
          company       TEXT NOT NULL,
          location      TEXT,
          remote        BOOLEAN DEFAULT FALSE,
          source_url    TEXT UNIQUE NOT NULL,
          source_label  TEXT,
          posted_at     TIMESTAMPTZ,
          crawled_at    TIMESTAMPTZ DEFAULT NOW(),
          summary       TEXT,
          tags          TEXT[],
          salary_range  TEXT,
          is_active     BOOLEAN DEFAULT TRUE
        )
    """)

    op.execute("""
        CREATE INDEX idx_jobs_fts ON jobs.listings
          USING gin(to_tsvector('english', title || ' ' || company || ' ' || COALESCE(location,'')))
    """)

    op.execute("""
        CREATE INDEX idx_jobs_remote ON jobs.listings(remote)
    """)

    op.execute("""
        CREATE INDEX idx_jobs_posted_at ON jobs.listings(posted_at DESC)
    """)

    op.execute("""
        CREATE INDEX idx_jobs_company ON jobs.listings(company)
    """)

    op.execute("""
        CREATE TABLE jobs.saved_searches (
          id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          user_id      UUID NOT NULL,
          name         TEXT NOT NULL,
          filters      JSONB NOT NULL,
          alert_email  BOOLEAN DEFAULT FALSE,
          created_at   TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE TABLE jobs.hidden_jobs (
          user_id UUID NOT NULL,
          job_id  UUID NOT NULL,
          PRIMARY KEY (user_id, job_id)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS jobs.hidden_jobs")
    op.execute("DROP TABLE IF EXISTS jobs.saved_searches")
    op.execute("DROP TABLE IF EXISTS jobs.listings")
    op.execute("DROP SCHEMA IF EXISTS jobs")
