"""Add country column to jobs.listings

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-23

"""
from typing import Sequence, Union

from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE jobs.listings ADD COLUMN IF NOT EXISTS country CHAR(2) NOT NULL DEFAULT 'US'"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_country ON jobs.listings(country)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS jobs.idx_jobs_country")
    op.execute("ALTER TABLE jobs.listings DROP COLUMN IF EXISTS country")
