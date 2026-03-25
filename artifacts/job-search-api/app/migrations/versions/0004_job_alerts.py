"""Job alerts: last_alerted_at, last_alerted_job_ids, user_email on saved_searches

Revision ID: 0004
Revises: 0003
Create Date: 2026-03-24
"""
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.saved_searches
        ADD COLUMN IF NOT EXISTS last_alerted_at  TIMESTAMPTZ DEFAULT NULL,
        ADD COLUMN IF NOT EXISTS last_alerted_job_ids JSONB DEFAULT '[]',
        ADD COLUMN IF NOT EXISTS user_email        TEXT DEFAULT NULL
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.saved_searches
        DROP COLUMN IF EXISTS last_alerted_at,
        DROP COLUMN IF EXISTS last_alerted_job_ids,
        DROP COLUMN IF EXISTS user_email
    """)
