"""Crunchbase enrichment source.

Fetches funding, headcount, founding year, and IPO status.
Falls back to Wikipedia API when Crunchbase returns 404 or missing data.
"""
import logging
import os
import re
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

_CB_BASE = "https://api.crunchbase.com/api/v4/entities/organizations"
_WIKI_BASE = "https://en.wikipedia.org/api/rest_v1/page/summary"
_YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/quote"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}


@dataclass
class CrunchbaseResult:
    funding_total_usd: int | None = None
    last_funding_type: str | None = None
    last_funding_date: str | None = None
    num_employees_range: str | None = None
    founded_year: int | None = None
    company_type: str = "unknown"
    stock_ticker: str | None = None
    stock_exchange: str | None = None
    sources: list[str] | None = None

    def __post_init__(self):
        if self.sources is None:
            self.sources = []


async def enrich_from_crunchbase(slug: str, company_name: str) -> CrunchbaseResult:
    result = CrunchbaseResult()
    api_key = os.environ.get("CRUNCHBASE_API_KEY", "")

    cb_ok = False
    if api_key:
        try:
            url = f"{_CB_BASE}/{slug}"
            params = {
                "field_ids": "short_description,funding_total,last_funding_type,last_funding_at,num_employees_enum,founded_on,ipo_status,stock_exchange_symbol",
                "user_key": api_key,
            }
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params=params, headers=_HEADERS)

            if resp.status_code == 200:
                data = resp.json().get("properties", {})

                ft = data.get("funding_total") or {}
                if ft.get("value_usd"):
                    result.funding_total_usd = int(ft["value_usd"])

                result.last_funding_type = data.get("last_funding_type")

                lfa = data.get("last_funding_at")
                if lfa:
                    result.last_funding_date = lfa[:10]

                emp = data.get("num_employees_enum")
                if emp:
                    result.num_employees_range = emp.replace("_", "-")

                fo = data.get("founded_on") or {}
                if fo.get("value"):
                    try:
                        result.founded_year = int(str(fo["value"])[:4])
                    except (ValueError, TypeError):
                        pass

                ipo = data.get("ipo_status", "")
                if ipo == "public":
                    result.company_type = "public"
                    stock_sym = data.get("stock_exchange_symbol")
                    if stock_sym:
                        parts = stock_sym.split(":")
                        if len(parts) == 2:
                            result.stock_exchange = parts[0]
                            result.stock_ticker = parts[1]
                        else:
                            result.stock_ticker = stock_sym
                else:
                    result.company_type = "private"

                result.sources.append("crunchbase")
                cb_ok = True
        except Exception:
            logger.exception("Crunchbase enrichment failed for slug=%s", slug)

    if not cb_ok:
        await _enrich_from_wikipedia(result, company_name)

    if result.company_type == "public" and result.stock_ticker:
        await _enrich_stock_from_yahoo(result, result.stock_ticker)

    return result


async def _enrich_from_wikipedia(result: CrunchbaseResult, company_name: str) -> None:
    try:
        safe_name = company_name.replace(" ", "_")
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(f"{_WIKI_BASE}/{safe_name}", headers=_HEADERS)
        if resp.status_code != 200:
            return

        description = resp.json().get("extract", "")

        year_m = re.search(r"founded\s+in\s+(\d{4})", description, re.IGNORECASE)
        if year_m and result.founded_year is None:
            result.founded_year = int(year_m.group(1))

        emp_m = re.search(r"([\d,]+)\s+employees", description, re.IGNORECASE)
        if emp_m and result.num_employees_range is None:
            try:
                count = int(emp_m.group(1).replace(",", ""))
                if count < 50:
                    result.num_employees_range = "1-49"
                elif count < 200:
                    result.num_employees_range = "50-199"
                elif count < 500:
                    result.num_employees_range = "200-499"
                elif count < 1000:
                    result.num_employees_range = "500-999"
                elif count < 5000:
                    result.num_employees_range = "1001-5000"
                else:
                    result.num_employees_range = "5001+"
            except ValueError:
                pass

        result.sources.append("wikipedia")
    except Exception:
        logger.exception("Wikipedia fallback failed for company=%s", company_name)


async def _enrich_stock_from_yahoo(result: CrunchbaseResult, ticker: str) -> None:
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                _YAHOO_BASE,
                params={"symbols": ticker},
                headers=_HEADERS,
            )
        if resp.status_code != 200:
            return

        data = resp.json()
        quotes = (
            data.get("quoteResponse", {})
            .get("result", [])
        )
        if not quotes:
            return

        q = quotes[0]
        exchange = q.get("fullExchangeName") or q.get("exchange")
        symbol = q.get("symbol")
        if exchange and result.stock_exchange is None:
            result.stock_exchange = exchange
        if symbol and result.stock_ticker is None:
            result.stock_ticker = symbol

        result.sources.append("yahoo_finance")
    except Exception:
        logger.exception("Yahoo Finance enrichment failed for ticker=%s", ticker)
