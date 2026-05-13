"""Add delivery_date column and unique partial index on alert_deliveries for deduplication.

Prevents duplicate 'sent' delivery rows for the same subscription on the same
UTC date, making the daily-send guard bulletproof even when concurrent Beat
workers or manual re-triggers race.

Using an explicit delivery_date DATE column avoids the IMMUTABLE requirement
that PostgreSQL enforces for index expressions — casting TIMESTAMPTZ to DATE
via ::date is STABLE (timezone-dependent) and cannot be used directly in a
unique index expression.

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
        ALTER TABLE jobs.alert_deliveries
        ADD COLUMN IF NOT EXISTS delivery_date DATE NOT NULL DEFAULT CURRENT_DATE
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_deliveries_sub_date_sent
        ON jobs.alert_deliveries (subscription_id, delivery_date)
        WHERE status = 'sent'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS jobs.uq_alert_deliveries_sub_date_sent")
    op.execute("ALTER TABLE jobs.alert_deliveries DROP COLUMN IF EXISTS delivery_date")
