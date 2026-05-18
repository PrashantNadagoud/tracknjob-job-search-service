"""Rename alert_deliveries.resend_message_id to email_message_id.

Provider-agnostic column name now that email is sent via Brevo.

Revision ID: 0015
Revises: 0014
Create Date: 2026-05-17
"""
from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.alert_deliveries
        RENAME COLUMN resend_message_id TO email_message_id
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE jobs.alert_deliveries
        RENAME COLUMN email_message_id TO resend_message_id
    """)
