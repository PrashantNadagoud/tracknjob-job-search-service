import uuid
from datetime import datetime
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
    # ── Columns added in migration 0008 ─────────────────────────────────────
    crunchbase_uuid: Mapped[str | None] = mapped_column(Text, nullable=True)
    linkedin_slug: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_ats_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    india_presence: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    india_offices: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    hq_city: Mapped[str | None] = mapped_column(Text, nullable=True)
    hq_country: Mapped[str | None] = mapped_column(sa.CHAR(2), nullable=True)
    categories: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    last_enrichment_attempt: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    enrichment_failures: Mapped[int] = mapped_column(
        Integer, server_default="0", nullable=False
    )


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
        # ATS dedup index (partial) — added in migration 0008
        Index(
            "idx_listings_ats_dedup",
            "company_id",
            "ats_type",
            "external_job_id",
            unique=True,
            postgresql_where=sa.text(
                "external_job_id IS NOT NULL AND company_id IS NOT NULL"
            ),
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
    # ── Columns added in migration 0008 ─────────────────────────────────────
    ats_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    ats_source_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        sa.ForeignKey("jobs.ats_sources.id", ondelete="SET NULL"),
        nullable=True,
    )
    external_job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    title_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    seniority_level: Mapped[str | None] = mapped_column(Text, nullable=True)
    employment_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    department: Mapped[str | None] = mapped_column(Text, nullable=True)
    salary_currency: Mapped[str] = mapped_column(
        sa.CHAR(3), server_default="USD", nullable=False
    )
    salary_min_local: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), nullable=True
    )
    salary_max_local: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), nullable=True
    )
    salary_period: Mapped[str | None] = mapped_column(
        Text, server_default="annual", nullable=True
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )


class AtsSource(Base):
    """Tracks per-company ATS configuration and crawl state."""

    __tablename__ = "ats_sources"
    __table_args__ = (
        sa.UniqueConstraint("company_id", "ats_type", name="uq_ats_source"),
        Index("idx_ats_sources_company", "company_id"),
        Index(
            "idx_ats_sources_crawl_due",
            sa.text("last_crawled_at ASC NULLS FIRST"),
            postgresql_where=sa.text("is_active = true"),
        ),
        {"schema": "jobs"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    company_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        sa.ForeignKey("jobs.companies.id", ondelete="CASCADE"),
        nullable=False,
    )
    ats_type: Mapped[str] = mapped_column(Text, nullable=False)
    ats_slug: Mapped[str | None] = mapped_column(Text, nullable=True)
    crawl_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    crawl_config: Mapped[dict | None] = mapped_column(JSONB, server_default="'{}'", nullable=True)
    market: Mapped[str] = mapped_column(Text, server_default="US", nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean, server_default="true", nullable=False
    )
    last_crawled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    last_crawl_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_crawl_job_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(
        Integer, server_default="0", nullable=False
    )
    backoff_until: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    discovery_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class CompanyDiscoveryQueue(Base):
    """Candidate companies waiting to be verified and registered."""

    __tablename__ = "company_discovery_queue"
    __table_args__ = (
        Index(
            "idx_discovery_queue_pending",
            "created_at",
            postgresql_where=sa.text("status = 'pending'"),
        ),
        {"schema": "jobs"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    company_name: Mapped[str] = mapped_column(Text, nullable=False)
    website: Mapped[str | None] = mapped_column(Text, nullable=True)
    suspected_ats: Mapped[str | None] = mapped_column(Text, nullable=True)
    suspected_slug: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    market: Mapped[str] = mapped_column(Text, server_default="US", nullable=False)
    status: Mapped[str] = mapped_column(
        Text, server_default="pending", nullable=False
    )
    resolved_company_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        sa.ForeignKey("jobs.companies.id"),
        nullable=True,
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, server_default="0", nullable=False
    )
    last_attempted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class CrawlDeadLetter(Base):
    """Failed crawl attempts that require investigation or resolution."""

    __tablename__ = "crawl_dead_letters"
    __table_args__ = (
        Index(
            "idx_dead_letters_unresolved",
            sa.text("attempted_at DESC"),
            postgresql_where=sa.text("resolved = false"),
        ),
        {"schema": "jobs"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    ats_source_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        sa.ForeignKey("jobs.ats_sources.id", ondelete="SET NULL"),
        nullable=True,
    )
    ats_type: Mapped[str] = mapped_column(Text, nullable=False)
    ats_slug: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_type: Mapped[str] = mapped_column(Text, nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    resolved: Mapped[bool] = mapped_column(
        Boolean, server_default="false", nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)


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
