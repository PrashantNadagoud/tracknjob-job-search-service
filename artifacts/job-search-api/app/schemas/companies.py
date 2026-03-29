import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, model_serializer


class CompanyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    slug: str
    name: str
    website: str | None = None
    company_type: str | None = None
    stock_ticker: str | None = None
    stock_exchange: str | None = None
    funding_total_usd: int | None = None
    last_funding_type: str | None = None
    last_funding_date: date | None = None
    num_employees_range: str | None = None
    founded_year: int | None = None
    culture_score: str | None = None
    ceo_approval_pct: int | None = None
    work_life_score: Decimal | None = None
    remote_policy: str | None = None
    perks: list[str] | None = None
    salary_min_usd: int | None = None
    salary_max_usd: int | None = None
    salary_source: str | None = None
    enriched_at: datetime | None = None
    enrichment_source: list[str] | None = None


class CompanySummary(BaseModel):
    company_type: str | None = None
    funding_stage: str | None = None
    funding_total_usd: int | None = None
    stock_ticker: str | None = None
    stock_exchange: str | None = None
    employee_range: str | None = None
    culture_score: str | None = None
    remote_policy: str | None = None
    perks: list[str] | None = None
    salary_min_usd: int | None = None
    salary_max_usd: int | None = None
    salary_source: str | None = None

    @model_serializer(mode="wrap")
    def _exclude_none(self, handler: Any) -> dict:
        data = handler(self)
        return {k: v for k, v in data.items() if v is not None}
