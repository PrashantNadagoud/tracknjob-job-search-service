"""Create geo.cities table for GeoNames city lookup.

Revision ID: 0010
Revises: (no prior revision tracked in versions/)
Create Date: 2026-05-25

This migration creates the geo schema and geo.cities table used by the
GeoNames-based geo-restriction classifier introduced in TRA-362.
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0010"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS geo")

    op.create_table(
        "cities",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("ascii_name", sa.Text, nullable=False),
        sa.Column("country_code", sa.CHAR(2), nullable=False),
        sa.Column("population", sa.Integer, nullable=False, server_default="0"),
        schema="geo",
    )

    op.create_index(
        "idx_geo_cities_name_lower",
        "cities",
        [sa.text("lower(name)")],
        schema="geo",
    )
    op.create_index(
        "idx_geo_cities_ascii_lower",
        "cities",
        [sa.text("lower(ascii_name)")],
        schema="geo",
    )


def downgrade() -> None:
    op.drop_index("idx_geo_cities_ascii_lower", table_name="cities", schema="geo")
    op.drop_index("idx_geo_cities_name_lower", table_name="cities", schema="geo")
    op.drop_table("cities", schema="geo")
    op.execute("DROP SCHEMA IF EXISTS geo CASCADE")
