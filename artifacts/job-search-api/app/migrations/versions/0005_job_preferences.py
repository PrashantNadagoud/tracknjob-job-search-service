"""Add jobs.job_preferences table for match scoring

Revision ID: 0005
Revises: 0004
Create Date: 2026-03-25
"""
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS jobs.job_preferences (
            user_id           UUID PRIMARY KEY,
            desired_title     TEXT,
            skills            TEXT[]  DEFAULT '{}',
            preferred_location TEXT,
            remote_only       BOOLEAN DEFAULT FALSE,
            seniority         TEXT,
            updated_at        TIMESTAMPTZ DEFAULT NOW()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS jobs.job_preferences")
