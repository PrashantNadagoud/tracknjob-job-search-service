"""Fix dedup index: replace STABLE expression index with delivery_date column index.

Fresh databases get the correct schema from migration 0010 (fixed).
Existing installations that ran the original 0010 still have the
expression-based index on (subscription_id, (delivered_at::date)) and no
delivery_date column.  This migration brings them in line:

  1. Drop the old expression index if it exists.
  2. Add delivery_date DATE column (no-op if 0010 fix already added it).
  3. Backfill delivery_date from delivered_at for any NULL rows.
  4. Set NOT NULL + DEFAULT CURRENT_DATE.
  5. Recreate the correct unique index on (subscription_id, delivery_date).

Revision ID: 0012
Revises: 0011
Create Date: 2026-05-13
"""
from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS jobs.uq_alert_deliveries_sub_date_sent")

    op.execute("""
        ALTER TABLE jobs.alert_deliveries
        ADD COLUMN IF NOT EXISTS delivery_date DATE
    """)

    op.execute("""
        UPDATE jobs.alert_deliveries
        SET delivery_date = (delivered_at AT TIME ZONE 'UTC')::date
        WHERE delivery_date IS NULL
    """)

    op.execute("""
        ALTER TABLE jobs.alert_deliveries
        ALTER COLUMN delivery_date SET NOT NULL,
        ALTER COLUMN delivery_date SET DEFAULT CURRENT_DATE
    """)

    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_deliveries_sub_date_sent
        ON jobs.alert_deliveries (subscription_id, delivery_date)
        WHERE status = 'sent'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS jobs.uq_alert_deliveries_sub_date_sent")
    op.execute("ALTER TABLE jobs.alert_deliveries DROP COLUMN IF EXISTS delivery_date")
