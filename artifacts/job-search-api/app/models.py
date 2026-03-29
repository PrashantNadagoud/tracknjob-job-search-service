import uuid
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy import (
    Boolean,
    Date,
    Index,
    Integer,
    Numeric,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    JSONB,
    TIMESTAMP,
    UUID,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Company(Base):
    __tablename__ = "companies"
    __table_args__ = {"schema": "jobs"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    website: Mapped[str | None] = mapped_column(Text, nullable=True)
    funding_total_usd: Mapped[int | None] = mapped_column(sa.BigInteger, nullable=True)
    last_funding_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_funding_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    num_employees_range: Mapped[str | None] = mapped_column(Text, nullable=True)
    founded_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    culture_score: Mapped[str | None] = mapped_column(Text, nullable=True)
    ceo_approval_pct: Mapped[int | None] = mapped_column(Integer, nullable=True)
    work_life_score: Mapped[Decimal | None] = mapped_column(Numeric(3, 1), nullable=True)
    remote_policy: Mapped[str | None] = mapped_column(Text, nullable=True)
    perks: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    salary_min_usd: Mapped[int | None] = mapped_column(Integer, nullable=True)
    salary_max_usd: Mapped[int | None] = mapped_column(Integer, nullable=True)
    salary_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    enriched_at: Mapped[TIMESTAMP | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    enrichment_source: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    company_type: Mapped[str] = mapped_column(Text, server_default="unknown", nullable=False)
    stock_ticker: Mapped[str | None] = mapped_column(Text, nullable=True)
    stock_exchange: Mapped[str | None] = mapped_column(Text, nullable=True)


class Listing(Base):
    __tablename__ = "listings"
    __table_args__ = (
        Index(
            "idx_jobs_fts",
            sa.text(
                "to_tsvector('english', title || ' ' || company || ' ' || COALESCE(location,''))"
            ),
            postgresql_using="gin",
        ),
        Index("idx_jobs_remote", "remote"),
        Index("idx_jobs_posted_at", sa.text("posted_at DESC")),
        Index("idx_jobs_company", "company"),
        Index("idx_jobs_last_seen", "last_seen_at"),
        Index(
            "idx_jobs_title_trgm",
            "title",
            postgresql_using="gin",
            postgresql_ops={"title": "gin_trgm_ops"},
        ),
        {"schema": "jobs"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    company: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[str | None] = mapped_column(Text, nullable=True)
    remote: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False)
    source_url: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    source_label: Mapped[str | None] = mapped_column(Text, nullable=True)
    posted_at: Mapped[TIMESTAMP | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    crawled_at: Mapped[TIMESTAMP | None] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=True
    )
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    tags: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    salary_range: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, server_default="true", nullable=False)
    country: Mapped[str] = mapped_column(
        sa.String(2), server_default="US", nullable=False
    )
    last_seen_at: Mapped[TIMESTAMP | None] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=True
    )
    company_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        sa.ForeignKey("jobs.companies.id"),
        nullable=True,
    )
    geo_restriction: Mapped[str | None] = mapped_column(Text, nullable=True)


class SavedSearch(Base):
    __tablename__ = "saved_searches"
    __table_args__ = {"schema": "jobs"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    filters: Mapped[dict] = mapped_column(JSONB, nullable=False)
    alert_email: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False)
    user_email: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_alerted_at: Mapped[TIMESTAMP | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    last_alerted_job_ids: Mapped[list | None] = mapped_column(JSONB, server_default="'[]'", nullable=True)
    created_at: Mapped[TIMESTAMP | None] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=True
    )


class HiddenJob(Base):
    __tablename__ = "hidden_jobs"
    __table_args__ = {"schema": "jobs"}

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, nullable=False
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, nullable=False
    )


class JobPreference(Base):
    __tablename__ = "job_preferences"
    __table_args__ = {"schema": "jobs"}

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, nullable=False
    )
    desired_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    skills: Mapped[list[str] | None] = mapped_column(ARRAY(Text), server_default="{}", nullable=True)
    preferred_location: Mapped[str | None] = mapped_column(Text, nullable=True)
    remote_only: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False)
    seniority: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[TIMESTAMP | None] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=True
    )
