"""CompanyEnricher — orchestrates all enrichment sources concurrently.

Free sources only (no paid API keys required):
  1. Wikipedia REST API + wikitext infobox  (primary: founded_year, employees, type, ticker)
  2. LinkedIn public /about/ page           (secondary: employees, founded_year)
  3. Comparably scrape                      (culture_score, ceo_approval_pct, work_life_score)
  4. BuiltIn scrape                         (remote_policy, perks)
  5. Glassdoor salary scrape               (salary_min_usd, salary_max_usd)

Yahoo Finance runs sequentially AFTER the gather if a stock_ticker was found.

Funding fields (funding_total_usd, last_funding_type, last_funding_date) are
always null — no free source populates them. The DB columns are preserved for
schema compatibility.

Data-integrity guardrails (added in Session 16):
  G1 — enriched_at is only set when at least one source returned real data.
  G2 — _apply_validated() enforces field-level bounds before writing.
  G3 — Enrichment is additive-only: null/empty/existing values are never overwritten.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal

from app.enrichment.builtin import enrich_from_builtin
from app.enrichment.comparably import enrich_from_comparably
from app.enrichment.glassdoor import enrich_salary_from_glassdoor
from app.enrichment.linkedin import enrich_from_linkedin
from app.enrichment.wikipedia import _enrich_from_yahoo_finance, enrich_from_wikipedia

logger = logging.getLogger(__name__)

# Seconds to sleep before each source's HTTP call (applied via _rate_limited
# wrapper for existing sources that we do not modify internally).
_RATE_LIMITS = {
    "comparably": 1.0,
    "builtin": 1.0,
    "glassdoor": 1.5,
}

# Canonical num_employees_range values accepted by _apply_validated (G2).
_VALID_EMPLOYEE_RANGES: frozenset[str] = frozenset({
    "1-10", "11-50", "51-200", "201-500", "501-1000",
    "1001-5000", "5001-10000", "10001+",
})


def generate_slugs(company_name: str) -> dict[str, str]:
    """Produce per-source slug variants from a company name.

    Returns a dict with keys: linkedin, comparably, builtin, wikipedia.

    Example:
        generate_slugs("Cloudflare, Inc.") → {
            "linkedin":   "cloudflare-inc",
            "comparably": "cloudflare-inc",
            "builtin":    "cloudflare-inc",
            "wikipedia":  "Cloudflare,_Inc.",
        }
    """
    base = company_name.lower()
    base = re.sub(r"[^\w\s-]", "", base)
    base = re.sub(r"\s+", "-", base.strip())
    base = re.sub(r"-+", "-", base)
    return {
        "linkedin": base,
        "comparably": base,
        "builtin": base,
        "wikipedia": company_name.replace(" ", "_"),
    }


async def _rate_limited(delay: float, coro):
    """Sleep for *delay* seconds then await *coro*."""
    await asyncio.sleep(delay)
    return await coro


@dataclass
class CompanyRecord:
    slug: str
    name: str
    website: str | None = None
    # Funding fields preserved for schema compat but always null (no free source)
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
    def _apply_validated(self, record: CompanyRecord, data: dict) -> None:
        """Validate and apply a dict of enrichment fields to a CompanyRecord,
        skipping invalid or empty values.

        Guardrail 2 — field-specific bounds checking:
          funding_total_usd  : float, > 0 and < 1 000 000 000 000
          culture_score      : parseable as float, 0.0–5.0
          ceo_approval_pct   : parseable as int, 0–100
          work_life_score    : parseable as float, 0.0–5.0 (stored as Decimal)
          founded_year       : parseable as int, 1800–current year
          num_employees_range: must be one of _VALID_EMPLOYEE_RANGES
          salary_min_usd /
          salary_max_usd     : float, > 0 and < 10 000 000
          all other fields   : accepted if non-None and non-empty string

        Guardrail 3 — additive-only writes:
          • Incoming None / "" / [] are silently skipped.
          • Fields already populated on *record* are not overwritten
            (treats "unknown" and [] as "not yet set" sentinels).

        Validation failures are logged at DEBUG with the field name, value,
        and company slug.
        """
        current_year = datetime.now(timezone.utc).year

        for fld, value in data.items():
            # G3: skip empty/null incoming values
            if value is None or value == "" or value == []:
                continue

            # G3: additive-only — skip if record field is already populated
            # Treat "unknown" (company_type sentinel) and [] as "not yet set"
            existing = getattr(record, fld, None)
            if existing is not None and existing != "unknown" and existing != []:
                continue

            # G2: field-specific validation
            valid = True
            try:
                if fld == "funding_total_usd":
                    v = float(value)
                    if not (0 < v < 1_000_000_000_000):
                        raise ValueError
                    value = int(v)

                elif fld == "culture_score":
                    v = float(value)          # rejects letter grades like "A+"
                    if not (0.0 <= v <= 5.0):
                        raise ValueError

                elif fld == "ceo_approval_pct":
                    v = int(value)
                    if not (0 <= v <= 100):
                        raise ValueError
                    value = v

                elif fld == "work_life_score":
                    v = float(value)
                    if not (0.0 <= v <= 5.0):
                        raise ValueError
                    value = Decimal(str(round(v, 2)))

                elif fld == "founded_year":
                    v = int(value)
                    if not (1800 <= v <= current_year):
                        raise ValueError
                    value = v

                elif fld == "num_employees_range":
                    if value not in _VALID_EMPLOYEE_RANGES:
                        raise ValueError

                elif fld in ("salary_min_usd", "salary_max_usd"):
                    v = float(value)
                    if not (0 < v < 10_000_000):
                        raise ValueError
                    value = int(v)

                # All other fields pass if non-None and non-empty (checked above)

            except (ValueError, TypeError):
                valid = False

            if not valid:
                logger.debug(
                    "Enricher validation rejected field '%s' value '%s' for company %s",
                    fld, value, record.slug,
                )
                continue

            setattr(record, fld, value)

    async def enrich(
        self,
        company_slug: str,
        company_name: str,
        primary_role: str,
        location: str,
    ) -> CompanyRecord:
        record = CompanyRecord(slug=company_slug, name=company_name)
        slugs = generate_slugs(company_name)

        # ── Concurrent gather across all five sources ─────────────────────────
        # Wikipedia and LinkedIn handle their own internal rate-limit sleeps.
        # Comparably, BuiltIn, Glassdoor are wrapped with _rate_limited so we
        # don't need to modify those existing files.
        (
            wiki_res,
            li_res,
            comp_res,
            bi_res,
            gd_res,
        ) = await asyncio.gather(
            enrich_from_wikipedia(slugs["wikipedia"]),
            enrich_from_linkedin(slugs["linkedin"]),
            _rate_limited(_RATE_LIMITS["comparably"], enrich_from_comparably(slugs["comparably"])),
            _rate_limited(_RATE_LIMITS["builtin"], enrich_from_builtin(slugs["builtin"])),
            _rate_limited(_RATE_LIMITS["glassdoor"], enrich_salary_from_glassdoor(primary_role, location)),
            return_exceptions=True,
        )

        # ── Merge: Wikipedia first, then LinkedIn fills gaps ──────────────────
        if not isinstance(wiki_res, Exception):
            self._apply_validated(record, {
                "num_employees_range": wiki_res.num_employees_range,
                "founded_year": wiki_res.founded_year,
                # Pass None instead of "unknown" so additive-only skips it cleanly
                "company_type": wiki_res.company_type if wiki_res.company_type != "unknown" else None,
                "stock_ticker": wiki_res.stock_ticker,
                "stock_exchange": wiki_res.stock_exchange,
            })
            record.enrichment_source.extend(wiki_res.sources)
        else:
            logger.warning("Wikipedia enrichment exception: %s", wiki_res)

        if not isinstance(li_res, Exception):
            self._apply_validated(record, {
                "num_employees_range": li_res.num_employees_range,
                "founded_year": li_res.founded_year,
            })
            record.enrichment_source.extend(li_res.sources)
        else:
            logger.warning("LinkedIn enrichment exception: %s", li_res)

        if not isinstance(comp_res, Exception):
            self._apply_validated(record, {
                "culture_score": comp_res.culture_score,
                "ceo_approval_pct": comp_res.ceo_approval_pct,
                "work_life_score": comp_res.work_life_score,
            })
            record.enrichment_source.extend(comp_res.sources)
        else:
            logger.warning("Comparably enrichment exception: %s", comp_res)

        if not isinstance(bi_res, Exception):
            self._apply_validated(record, {
                "remote_policy": bi_res.remote_policy,
                "perks": bi_res.perks,
            })
            record.enrichment_source.extend(bi_res.sources)
        else:
            logger.warning("BuiltIn enrichment exception: %s", bi_res)

        if not isinstance(gd_res, Exception):
            self._apply_validated(record, {
                "salary_min_usd": gd_res.salary_min_usd,
                "salary_max_usd": gd_res.salary_max_usd,
                "salary_source": gd_res.salary_source,
            })
            record.enrichment_source.extend(gd_res.sources)
        else:
            logger.warning("Glassdoor enrichment exception: %s", gd_res)

        # ── Yahoo Finance: only if ticker already resolved ────────────────────
        # Runs sequentially after the gather so we use the merged ticker value.
        if record.stock_ticker and record.stock_exchange is None:
            try:
                from app.enrichment.wikipedia import WikipediaResult
                yf_proxy = WikipediaResult(stock_ticker=record.stock_ticker)
                await _enrich_from_yahoo_finance(yf_proxy)
                self._apply_validated(record, {
                    "stock_exchange": yf_proxy.stock_exchange,
                    "stock_ticker": yf_proxy.stock_ticker,
                })
                for src in yf_proxy.sources:
                    if src not in record.enrichment_source:
                        record.enrichment_source.append(src)
            except Exception:
                logger.warning("Yahoo Finance post-gather step failed for slug=%s", company_slug)

        # ── Funding fields: always null — no free source populates them ───────
        record.funding_total_usd = None
        record.last_funding_type = None
        record.last_funding_date = None

        # ── Guardrail 1: only timestamp when at least one source produced data ─
        if record.enrichment_source:
            record.enriched_at = datetime.now(timezone.utc)
        else:
            logger.warning("Enrichment returned no data for company: %s", record.slug)

        return record
