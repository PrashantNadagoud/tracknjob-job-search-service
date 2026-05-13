"""Add unique partial index on alert_deliveries for deduplication.

Prevents duplicate 'sent' delivery rows for the same subscription on the same
UTC date, making the daily-send guard bulletproof even when concurrent Beat
workers or manual re-triggers race.

Revision ID: 0010
Revises: 0009
Create Date: 2026-05-13
"""
from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_deliveries_sub_date_sent
        ON jobs.alert_deliveries (subscription_id, (delivered_at::date))
        WHERE status = 'sent'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS jobs.uq_alert_deliveries_sub_date_sent")
