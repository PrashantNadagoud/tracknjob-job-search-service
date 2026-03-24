"""Ghost job detection: last_seen_at column, pg_trgm extension, indexes

Revision ID: 0003
Revises: 0002
Create Date: 2026-03-23

"""
from typing import Sequence, Union

from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE jobs.listings "
        "ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ DEFAULT NOW()"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs.listings(last_seen_at)"
    )
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_title_trgm "
        "ON jobs.listings USING gin(title gin_trgm_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS jobs.idx_jobs_title_trgm")
    op.execute("DROP INDEX IF EXISTS jobs.idx_jobs_last_seen")
    op.execute(
        "ALTER TABLE jobs.listings DROP COLUMN IF EXISTS last_seen_at"
    )
