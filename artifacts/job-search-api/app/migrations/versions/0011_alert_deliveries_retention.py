"""Add delivered_at index on alert_deliveries for efficient retention pruning.

The existing idx_alert_deliveries_sub index is (subscription_id, delivered_at DESC),
which is efficient for the daily-send guard lookup but is NOT used for a
full-table DELETE WHERE delivered_at < cutoff (subscription_id is the leading
column, so Postgres cannot range-scan on delivered_at alone with that index).

This migration adds a dedicated idx_alert_deliveries_delivered_at index so the
nightly prune_old_deliveries Celery task can efficiently delete rows older than
90 days without performing a sequential scan.

Revision ID: 0011
Revises: 0010
Create Date: 2026-05-13
"""
from alembic import op

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_delivered_at "
        "ON jobs.alert_deliveries(delivered_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS jobs.idx_alert_deliveries_delivered_at")
