"""Add geo_restriction column to jobs.listings

Revision ID: 0007
Revises: 0006
Create Date: 2026-03-29
"""
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.listings
        ADD COLUMN IF NOT EXISTS geo_restriction TEXT
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE jobs.listings DROP COLUMN IF EXISTS geo_restriction")
