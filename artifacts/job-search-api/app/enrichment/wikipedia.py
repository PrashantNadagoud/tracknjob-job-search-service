"""Wikipedia enrichment source — primary free data source.

Fetches company metadata via the Wikipedia REST summary endpoint and the
MediaWiki wikitext API.  After finding a stock ticker in the infobox, runs
a Yahoo Finance lookup to confirm the exchange name.  No API key required.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)

_WIKI_REST = "https://en.wikipedia.org/api/rest_v1"
_WIKI_API  = "https://en.wikipedia.org/w/api.php"
_YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/quote"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}


@dataclass
class WikipediaResult:
    num_employees_range: str | None = None
    founded_year: int | None = None
    company_type: str = "unknown"
    stock_ticker: str | None = None
    stock_exchange: str | None = None
    sources: list[str] = field(default_factory=list)


def _count_to_range(count: int) -> str:
    if count < 50:
        return "1-49"
    if count < 200:
        return "50-199"
    if count < 500:
        return "200-499"
    if count < 1_000:
        return "500-999"
    if count < 5_001:
        return "1001-5000"
    return "5001+"


def _parse_wikitext_infobox(wikitext: str, result: WikipediaResult) -> None:
    """Extract structured fields from a Wikipedia article wikitext infobox.

    Parses ``| field = value`` lines with regex only — no full wikitext parser.
    """
    # founded year: | founded = {{Start date|2009|9|15}} or | founded = 2009
    if result.founded_year is None:
        m = re.search(r"\|\s*founded\s*=[^\n]*?(\d{4})", wikitext, re.IGNORECASE)
        if m:
            try:
                result.founded_year = int(m.group(1))
            except ValueError:
                pass

    # num_employees: | num_employees = {{increase}} 3,214 (2022)
    if result.num_employees_range is None:
        m = re.search(
            r"\|\s*num_employees\s*=[^\n]*?([\d,]{2,})", wikitext, re.IGNORECASE
        )
        if m:
            try:
                count = int(m.group(1).replace(",", ""))
                result.num_employees_range = _count_to_range(count)
            except ValueError:
                pass

    # traded_as: | traded_as = {{NYSE|NET}} or {{Nasdaq|AMZN}}
    m = re.search(
        r"\|\s*traded_as\s*=[^\n]*?\{\{(\w+)\|(\w+)\}\}", wikitext, re.IGNORECASE
    )
    if m:
        result.stock_exchange = m.group(1).upper()
        result.stock_ticker = m.group(2).upper()

    # type: | type = [[Public company|Public]] or Private
    if result.company_type == "unknown":
        m = re.search(r"\|\s*type\s*=\s*([^\n|{]+)", wikitext, re.IGNORECASE)
        if m:
            type_text = m.group(1).lower()
            if "public" in type_text:
                result.company_type = "public"
            elif "private" in type_text:
                result.company_type = "private"
            elif "subsidiary" in type_text:
                result.company_type = "subsidiary"


async def enrich_from_wikipedia(company_name: str) -> WikipediaResult:
    """Fetch company metadata from Wikipedia REST API and wikitext infobox.

    Applies a 0.5 s rate-limit sleep before the first HTTP call.
    """
    result = WikipediaResult()
    await asyncio.sleep(0.5)

    safe_name = company_name.replace(" ", "_")
    got_data = False

    # ── Step 1: REST summary endpoint ────────────────────────────────────────
    try:
        async with httpx.AsyncClient(
            timeout=8, follow_redirects=True, headers=_HEADERS
        ) as client:
            resp = await client.get(f"{_WIKI_REST}/page/summary/{safe_name}")

        if resp.status_code == 200:
            data = resp.json()
            extract = data.get("extract", "") or ""
            description = data.get("description", "") or ""

            # founding year from plain text
            m = re.search(
                r"(?:founded|incorporated)\s+in\s+(\d{4})", extract, re.IGNORECASE
            )
            if m:
                result.founded_year = int(m.group(1))

            # employee count from plain text
            m = re.search(r"([\d,]+)\s+employees", extract, re.IGNORECASE)
            if m:
                try:
                    result.num_employees_range = _count_to_range(
                        int(m.group(1).replace(",", ""))
                    )
                except ValueError:
                    pass

            # company type from description line
            desc_lower = description.lower()
            if "publicly traded" in desc_lower or "public company" in desc_lower:
                result.company_type = "public"
            elif "private" in desc_lower:
                result.company_type = "private"

            got_data = True

        elif resp.status_code == 404:
            logger.debug("Wikipedia: page not found for %s", company_name)
        else:
            logger.warning(
                "Wikipedia REST returned %d for %s", resp.status_code, company_name
            )
    except Exception:
        logger.warning("Wikipedia REST summary failed for company=%s", company_name)

    # ── Step 2: MediaWiki wikitext infobox ───────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=8, headers=_HEADERS) as client:
            resp = await client.get(
                _WIKI_API,
                params={
                    "action": "query",
                    "titles": safe_name,
                    "prop": "revisions",
                    "rvprop": "content",
                    "rvslots": "main",
                    "format": "json",
                    "formatversion": "2",
                },
            )

        if resp.status_code == 200:
            pages = resp.json().get("query", {}).get("pages", [])
            for page in pages:
                revisions = page.get("revisions", [])
                if revisions:
                    wikitext = (
                        revisions[0]
                        .get("slots", {})
                        .get("main", {})
                        .get("content", "")
                    )
                    if wikitext:
                        _parse_wikitext_infobox(wikitext, result)
                        got_data = True
                break
    except Exception:
        logger.warning("Wikipedia infobox failed for company=%s", company_name)

    if got_data:
        result.sources.append("wikipedia")

    # ── Step 3: Yahoo Finance if ticker was resolved ──────────────────────────
    if result.stock_ticker:
        await _enrich_from_yahoo_finance(result)

    return result


async def _enrich_from_yahoo_finance(result: WikipediaResult) -> None:
    """Resolve full exchange name from Yahoo Finance for a known ticker.

    Appends ``"yahoo_finance"`` to result.sources on success.
    """
    try:
        await asyncio.sleep(0.5)
        async with httpx.AsyncClient(timeout=8, headers=_HEADERS) as client:
            resp = await client.get(
                _YAHOO_BASE, params={"symbols": result.stock_ticker}
            )

        if resp.status_code != 200:
            logger.debug(
                "Yahoo Finance returned %d for ticker=%s",
                resp.status_code,
                result.stock_ticker,
            )
            return

        quotes = resp.json().get("quoteResponse", {}).get("result", [])
        if not quotes:
            return

        q = quotes[0]
        exchange = q.get("exchange") or q.get("fullExchangeName")
        symbol = q.get("symbol")

        if exchange:
            result.stock_exchange = exchange
        if symbol:
            result.stock_ticker = symbol

        if exchange or symbol:
            if "yahoo_finance" not in result.sources:
                result.sources.append("yahoo_finance")

    except Exception:
        logger.warning(
            "Yahoo Finance enrichment failed for ticker=%s", result.stock_ticker
        )
