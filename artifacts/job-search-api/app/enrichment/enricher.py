"""CompanyEnricher — orchestrates all enrichment sources concurrently."""
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal

from app.enrichment.builtin import enrich_from_builtin
from app.enrichment.comparably import enrich_from_comparably
from app.enrichment.crunchbase import enrich_from_crunchbase
from app.enrichment.glassdoor import enrich_salary_from_glassdoor

logger = logging.getLogger(__name__)


@dataclass
class CompanyRecord:
    slug: str
    name: str
    website: str | None = None
    funding_total_usd: int | None = None
    last_funding_type: str | None = None
    last_funding_date: date | None = None
    num_employees_range: str | None = None
    founded_year: int | None = None
    company_type: str = "unknown"
    stock_ticker: str | None = None
    stock_exchange: str | None = None
    culture_score: str | None = None
    ceo_approval_pct: int | None = None
    work_life_score: Decimal | None = None
    remote_policy: str | None = None
    perks: list[str] | None = None
    salary_min_usd: int | None = None
    salary_max_usd: int | None = None
    salary_source: str | None = None
    enriched_at: datetime | None = None
    enrichment_source: list[str] = field(default_factory=list)


class CompanyEnricher:
    async def enrich(
        self,
        company_slug: str,
        company_name: str,
        primary_role: str,
        location: str,
    ) -> CompanyRecord:
        record = CompanyRecord(slug=company_slug, name=company_name)

        cb_res, comp_res, bi_res, gd_res = await asyncio.gather(
            enrich_from_crunchbase(company_slug, company_name),
            enrich_from_comparably(company_slug),
            enrich_from_builtin(company_slug),
            enrich_salary_from_glassdoor(primary_role, location),
            return_exceptions=True,
        )

        if not isinstance(cb_res, Exception):
            record.funding_total_usd = cb_res.funding_total_usd
            record.last_funding_type = cb_res.last_funding_type
            record.last_funding_date = cb_res.last_funding_date
            record.num_employees_range = cb_res.num_employees_range
            record.founded_year = cb_res.founded_year
            record.company_type = cb_res.company_type
            record.stock_ticker = cb_res.stock_ticker
            record.stock_exchange = cb_res.stock_exchange
            record.enrichment_source.extend(cb_res.sources)
        else:
            logger.error("Crunchbase gather exception: %s", cb_res)

        if not isinstance(comp_res, Exception):
            record.culture_score = comp_res.culture_score
            record.ceo_approval_pct = comp_res.ceo_approval_pct
            if comp_res.work_life_score is not None:
                record.work_life_score = Decimal(str(comp_res.work_life_score))
            record.enrichment_source.extend(comp_res.sources)
        else:
            logger.error("Comparably gather exception: %s", comp_res)

        if not isinstance(bi_res, Exception):
            record.remote_policy = bi_res.remote_policy
            record.perks = bi_res.perks or None
            record.enrichment_source.extend(bi_res.sources)
        else:
            logger.error("BuiltIn gather exception: %s", bi_res)

        if not isinstance(gd_res, Exception):
            record.salary_min_usd = gd_res.salary_min_usd
            record.salary_max_usd = gd_res.salary_max_usd
            record.salary_source = gd_res.salary_source
            record.enrichment_source.extend(gd_res.sources)
        else:
            logger.error("Glassdoor gather exception: %s", gd_res)

        record.enriched_at = datetime.now(timezone.utc)
        return record
