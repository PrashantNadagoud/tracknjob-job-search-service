import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class JobListingItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    title: str
    company: str
    location: str | None = None
    remote: bool
    posted_at: datetime | None = None
    source_url: str
    source_label: str | None = None
    summary: str | None = None
    tags: list[str] | None = None
    salary_range: str | None = None


class JobSearchResponse(BaseModel):
    total: int
    page: int
    limit: int
    results: list[JobListingItem]


class JobListingDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    title: str
    company: str
    location: str | None = None
    remote: bool
    posted_at: datetime | None = None
    source_url: str
    source_label: str | None = None
    summary: str | None = None
    tags: list[str] | None = None
    salary_range: str | None = None
    crawled_at: datetime | None = None
    is_active: bool


class JobSourceItem(BaseModel):
    id: str
    label: str
    job_count: int
    last_crawled: datetime | None = None


class JobSourcesResponse(BaseModel):
    sources: list[JobSourceItem]
