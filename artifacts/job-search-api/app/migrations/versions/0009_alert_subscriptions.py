"""Alert subscriptions and deliveries tables.

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-13
"""
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS jobs.alert_subscriptions (
            id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id                     VARCHAR NOT NULL,
            email                       VARCHAR(255) NOT NULL,
            name                        VARCHAR(255),
            is_active                   BOOLEAN NOT NULL DEFAULT true,
            keywords                    TEXT[],
            locations                   TEXT[],
            employment_types            TEXT[],
            ats_types                   TEXT[],
            job_search_started_at       TIMESTAMPTZ,
            motivational_email_enabled  BOOLEAN NOT NULL DEFAULT true,
            delivery_time_utc           INTEGER NOT NULL DEFAULT 13,
            created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_alert_subscription_user UNIQUE (user_id)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_subscriptions_user "
        "ON jobs.alert_subscriptions(user_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_subscriptions_active "
        "ON jobs.alert_subscriptions(delivery_time_utc) "
        "WHERE is_active = true"
    )

    op.execute("""
        CREATE TABLE IF NOT EXISTS jobs.alert_deliveries (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            subscription_id     UUID NOT NULL
                REFERENCES jobs.alert_subscriptions(id) ON DELETE CASCADE,
            delivered_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            jobs_sent           INTEGER,
            status              VARCHAR(50) NOT NULL,
            resend_message_id   VARCHAR,
            error_message       TEXT
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_sub "
        "ON jobs.alert_deliveries(subscription_id, delivered_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS jobs.alert_deliveries")
    op.execute("DROP TABLE IF EXISTS jobs.alert_subscriptions")
