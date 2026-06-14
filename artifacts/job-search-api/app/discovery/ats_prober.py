"""ATSProber — detect and probe a company's ATS.

Two-stage strategy:
1. detect_ats_from_careers_page(website) — fetch the company's /careers or /jobs
   page and fingerprint ATS domain patterns in HTML links/scripts.  One request,
   returns (ats_type, ats_slug) or None.  Fast and accurate for ~85% of companies.
2. ATSProber.probe(company) — concurrent 7-way probe as fallback when fingerprinting
   finds nothing (JS-rendered pages, custom portals, etc.).

probe(company) returns the first successful match as:
    {"ats_type": str, "ats_slug": str, "crawl_url": str | None}
or None if no ATS is found.

Rate-limiting:
    - Global cap: asyncio.Semaphore(20) simultaneous requests
    - Per-pattern concurrency per company: asyncio.Semaphore(5)
    - Per-domain rate limit: max 1 req/s via domain-keyed asyncio.Semaphore
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import defaultdict
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BOT_UA = "Mozilla/5.0 (compatible; TrackNJob-Bot/1.0; +https://tracknjob.com/bot)"
_CLIENT_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)
_PROBE_TIMEOUT = 8.0

_GLOBAL_SEM = asyncio.Semaphore(20)
_DOMAIN_LAST_REQUEST: dict[str, float] = defaultdict(float)
_DOMAIN_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

ATS_PROBE_PATTERNS: list[dict[str, Any]] = [
    {
        "ats_type": "greenhouse",
        "method": "GET",
        "url_template": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
        "success_key": "jobs",
    },
    {
        "ats_type": "lever",
        "method": "GET",
        "url_template": "https://api.lever.co/v0/postings/{slug}?limit=1",
        "success_check": lambda data: isinstance(data, list),
    },
    {
        "ats_type": "ashby",
        "method": "GET",
        "url_template": "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams",
        "extra_headers": {"ashby-organization-id": "{slug}"},
        "success_key": "data",
    },
    {
        "ats_type": "workday",
        "method": "POST",
        "url_template": "https://{slug}.wd1.myworkdayjobs.com/wday/cxs/{slug}/External/jobs",
        "post_body": {"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""},
        "success_key": "jobPostings",
    },
    {
        "ats_type": "bamboohr",
        "method": "GET",
        "url_template": "https://api.bamboohr.com/api/gateway.php/{slug}/v1/applicant_tracking/jobs",
        "extra_headers": {"Accept": "application/json"},
        "success_check": lambda data: isinstance(data, list) and len(data) > 0,
    },
    {
        "ats_type": "smartrecruiters",
        "method": "GET",
        "url_template": "https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1",
        "success_check": lambda data: (
            isinstance(data, dict)
            and data.get("totalFound", 0) > 0
            and "content" in data
        ),
    },
    {
        "ats_type": "rippling",
        "method": "GET",
        "url_template": "https://api.rippling.com/platform/api/ats/v1/jobs/?company_slug={slug}&limit=1",
        "success_check": lambda data: isinstance(data, dict) and data.get("count", 0) > 0,
    },
    {
        "ats_type": "icims",
        "url_template": "https://careers.{slug}.icims.com/jobs/search",
        "success_check": "status_200",
        "method": "GET",
        "validate_fn": lambda r: r.status_code == 200 and "icims" in r.text.lower()
    },
    {
        "ats_type": "taleo",
        "url_template": "https://{slug}.taleo.net/careersection/jobsearch.ftl",
        "success_check": "status_200",
        "method": "GET",
        "validate_fn": lambda r: r.status_code == 200 and "taleo" in r.text.lower()
    },
    {
        "ats_type": "successfactors",
        "url_template": "https://{slug}.jobs.com/search",
        "success_check": "status_200",
        "method": "GET",
        "validate_fn": lambda r: r.status_code == 200 and len(r.text) > 1000
    },
]


def _extract_domain(url: str) -> str:
    """Extract base domain from URL for per-domain rate limiting."""
    match = re.search(r"https?://([^/]+)", url)
    if match:
        parts = match.group(1).split(".")
        return ".".join(parts[-2:]) if len(parts) > 1 else parts[0]
    return url


def _derive_slug_from_website(website: str) -> str:
    """Strip protocol, www, and TLD from website URL to derive a slug candidate."""
    clean = re.sub(r"^https?://", "", website)
    clean = re.sub(r"^www\.", "", clean)
    clean = re.sub(r"\.[a-z]{2,6}(/.*)?$", "", clean)
    clean = re.split(r"[/\?#]", clean)[0]
    return clean.lower().strip("-").replace(".", "-")


def _slugify(name: str) -> str:
    """Convert a company name to a URL-safe slug."""
    slug = name.lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


# ---------------------------------------------------------------------------
# Career-page ATS fingerprinting
# ---------------------------------------------------------------------------

_CAREERS_PAGE_PATHS = [
    "/careers",
    "/jobs",
    "/about/careers",
    "/company/careers",
    "/work-with-us",
    "/join-us",
    "/join",
    "/opportunities",
]

_ATS_FINGERPRINTS: list[tuple[str, str]] = [
    # (regex pattern to match against any href/src/text, ats_type)
    (r"boards\.greenhouse\.io/([^/\"'\s?#]+)", "greenhouse"),
    (r"boards-api\.greenhouse\.io/v1/boards/([^/\"'\s?#]+)", "greenhouse"),
    (r"job-boards\.greenhouse\.io/([^/\"'\s?#]+)", "greenhouse"),
    (r"jobs\.lever\.co/([^/\"'\s?#]+)", "lever"),
    (r"jobs\.ashbyhq\.com/([^/\"'\s?#]+)", "ashby"),
    (r"app\.ashbyhq\.com/jobs/([^/\"'\s?#]+)", "ashby"),
    (r"careers\.smartrecruiters\.com/([^/\"'\s?#]+)", "smartrecruiters"),
    (r"jobs\.smartrecruiters\.com/([^/\"'\s?#]+)", "smartrecruiters"),
    (r"([a-z0-9-]+)\.wd\d\.myworkdayjobs\.com", "workday"),
    (r"([a-z0-9-]+)\.myworkdayjobs\.com", "workday"),
    (r"([a-z0-9-]+)\.bamboohr\.com/jobs", "bamboohr"),
    (r"app\.bamboohr\.com/jobs/([^/\"'\s?#]+)", "bamboohr"),
    (r"([a-z0-9-]+)\.rippling\.com/jobs", "rippling"),
    (r"app\.rippling\.com/jobs/([^/\"'\s?#]+)", "rippling"),
    (r"apply\.workable\.com/([^/\"'\s?#]+)", "workable"),
    (r"([a-z0-9-]+)\.workable\.com", "workable"),
]

_FINGERPRINT_TIMEOUT = 10.0
_CAREERS_PAGE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _extract_slug_from_match(match: re.Match, ats_type: str) -> str:
    """Pull the most useful capture group from a fingerprint regex match."""
    groups = [g for g in match.groups() if g]
    if not groups:
        return ""
    slug = groups[0].lower().rstrip("/").split("?")[0].split("#")[0]
    return slug


async def detect_ats_from_careers_page(
    website: str,
) -> dict[str, str] | None:
    """Fetch a company's careers page and fingerprint the ATS from HTML links.

    Tries a short list of common career page paths under *website* and scans
    all ``href`` / ``src`` / ``action`` attributes plus inline ``<script>``
    text for known ATS domain patterns.

    Returns:
        {"ats_type": str, "ats_slug": str} on first match, or None.
    """
    if not website:
        return None

    base = website.rstrip("/")
    compiled = [(re.compile(pat, re.IGNORECASE), ats_type) for pat, ats_type in _ATS_FINGERPRINTS]

    async with httpx.AsyncClient(
        timeout=_FINGERPRINT_TIMEOUT,
        follow_redirects=True,
        limits=_CLIENT_LIMITS,
        headers=_CAREERS_PAGE_HEADERS,
    ) as client:
        for path in _CAREERS_PAGE_PATHS:
            url = base + path
            try:
                resp = await client.get(url)
            except Exception as exc:
                logger.debug("careers-page fingerprint: failed to fetch %s: %s", url, exc)
                continue

            if resp.status_code not in (200, 301, 302):
                continue

            html = resp.text

            for pattern, ats_type in compiled:
                m = pattern.search(html)
                if m:
                    slug = _extract_slug_from_match(m, ats_type)
                    if slug:
                        logger.info(
                            "careers-page fingerprint: %s → %s (slug=%s) via %s",
                            base, ats_type, slug, url,
                        )
                        return {"ats_type": ats_type, "ats_slug": slug}

            # Also scan the final redirected URL itself — some companies redirect
            # their /careers straight to the ATS board
            final_url = str(resp.url)
            for pattern, ats_type in compiled:
                m = pattern.search(final_url)
                if m:
                    slug = _extract_slug_from_match(m, ats_type)
                    if slug:
                        logger.info(
                            "careers-page fingerprint: %s → %s (slug=%s) via redirect to %s",
                            base, ats_type, slug, final_url,
                        )
                        return {"ats_type": ats_type, "ats_slug": slug}

    logger.debug("careers-page fingerprint: no ATS found for %s", base)
    return None


# Fortune 500 companies often use abbreviated slugs
KNOWN_SLUG_OVERRIDES = {
    # Fortune 500 — Workday slug overrides
    "walmart": "walmart",
    "amazon": "amazon_dsp",           # Amazon's main Workday instance
    "apple": "apple",
    "cvs health": "cvshealth",
    "unitedhealth group": "uhg",
    "exxon mobil": "exxonmobil",
    "alphabet": "google",
    "mckesson": "mckesson",
    "at&t": "att",
    "microsoft": "microsoft",
    "costco": "costco",
    "jpmorgan chase": "jpmorgan",
    "chevron": "chevron",
    "home depot": "homedepot",
    "walgreens": "walgreens",
    "bank of america": "bankofamerica",
    "verizon": "verizon",
    "ford motor": "ford",
    "general motors": "generalmotors",
    "meta platforms": "meta",
    "comcast": "comcast",
    "target": "target",
    "humana": "humana",
    "goldman sachs": "goldmansachs",
    "boeing": "boeing",
    "lockheed martin": "lmco",
    "hp": "hp",
    "ups": "ups",
    "abbvie": "abbvie",
    "johnson & johnson": "jnj",
    "pfizer": "pfizer",
    "caterpillar": "caterpillar",
    "ibm": "ibm",
    "intel": "intel",
    "salesforce": "salesforce",
    "oracle": "oracle",
    "netflix": "netflix",
    "berkshire hathaway": "berkshirehathaway",

    # Missing overrides to improve match rate
    "procter & gamble": "pg",
    "procter and gamble": "pg",
    "archer daniels midland": "adm",
    "archer-daniels-midland": "adm",
    "raytheon technologies": "raytheoncareer",
    "rtx": "raytheoncareer",
    "energy transfer": "energytransfer",
    "energy transfer partners": "energytransfer",
    "albertsons": "albertsons-apply",
    "fedex": "fedex",                    # fedex works on wd1 not wd5
    "lowe's": "lowes",
    "lowes": "lowes",
    "hca healthcare": "hcahealthcare",
    "marathon petroleum": "marathonpetroleum",
    "phillips 66": "phillips66",
    "valero energy": "valero",
    "publix": "publix",
    "general dynamics": "gdms",
    "northrop grumman": "northropgrumman",
    "cigna": "thecignagroup",
    "elevance health": "elevance",       # formerly Anthem
    "anthem": "elevance",
    "cardinal health": "cardinalhealth",
    "sysco": "sysco",
    "tyson foods": "tysonfoods",
    "deere & company": "deere",
    "john deere": "deere",
}

def _derive_slug(company: dict[str, Any]) -> str:
    """Priority: KNOWN_SLUG_OVERRIDES → yc_slug → stripped website → slugified name."""
    name = company.get("name") or company.get("company_name") or ""
    name_lower = name.lower()
    
    if name_lower in KNOWN_SLUG_OVERRIDES:
        return KNOWN_SLUG_OVERRIDES[name_lower]

    yc_slug = company.get("yc_slug")
    if yc_slug and isinstance(yc_slug, str) and yc_slug.strip():
        return yc_slug.strip().lower()

    website = company.get("website") or ""
    if website:
        website_slug = _derive_slug_from_website(website)
        if website_slug:
            return website_slug

    return _slugify(name)


async def _rate_limit_domain(domain: str) -> None:
    """Enforce max 1 req/s per domain."""
    lock = _DOMAIN_LOCKS[domain]
    async with lock:
        now = time.monotonic()
        elapsed = now - _DOMAIN_LAST_REQUEST[domain]
        if elapsed < 1.0:
            await asyncio.sleep(1.0 - elapsed)
        _DOMAIN_LAST_REQUEST[domain] = time.monotonic()


async def _probe_pattern(
    client: httpx.AsyncClient,
    pattern: dict[str, Any],
    slug: str,
    company_sem: asyncio.Semaphore,
) -> dict[str, Any] | None:
    """Fire a single ATS pattern probe and return match dict or None."""
    ats_type = pattern["ats_type"]
    method = pattern["method"]
    url = pattern["url_template"].replace("{slug}", slug)
    extra_headers: dict[str, str] = {
        k: v.replace("{slug}", slug)
        for k, v in (pattern.get("extra_headers") or {}).items()
    }

    domain = _extract_domain(url)

    async with company_sem:
        async with _GLOBAL_SEM:
            await _rate_limit_domain(domain)
            try:
                if method == "GET":
                    resp = await client.get(url, headers=extra_headers)
                else:
                    body = pattern.get("post_body", {})
                    resp = await client.post(url, json=body, headers=extra_headers)
            except httpx.TimeoutException:
                logger.debug("Timeout probing %s for %s", ats_type, slug)
                return None
            except Exception as exc:
                logger.debug("Error probing %s for %s: %s", ats_type, slug, exc)
                return None

    status = resp.status_code

    if status == 404:
        return None
    if status == 429:
        logger.warning("Rate limited probing %s for slug=%s", ats_type, slug)
        return None
    if not resp.is_success:
        logger.debug("HTTP %s probing %s for slug=%s", status, ats_type, slug)
        return None

    if "validate_fn" in pattern:
        try:
            matched = pattern["validate_fn"](resp)
        except Exception:
            matched = False
            
        if matched:
            return {
                "ats_type": ats_type,
                "ats_slug": slug,
                "crawl_url": url if ats_type == "workday" else None,
            }
        return None

    try:
        data = resp.json()
    except Exception:
        logger.debug("Non-JSON response from %s for slug=%s", ats_type, slug)
        return None

    success_key = pattern.get("success_key")
    success_check = pattern.get("success_check")

    matched = False
    if success_key:
        matched = isinstance(data, dict) and success_key in data
    elif success_check:
        try:
            matched = bool(success_check(data))
        except Exception:
            matched = False

    if matched:
        return {
            "ats_type": ats_type,
            "ats_slug": slug,
            "crawl_url": url if ats_type == "workday" else None,
        }

    return None


class ATSProber:
    """Probes a company dict against ATS patterns concurrently."""

    def _extract_career_site_name(self, sitemap_url: str, slug: str) -> str:
        """
        Extract career site name from Workday sitemap URL.
        Falls back to slug if URL is malformed or ambiguous.

        Examples:
          https://sysco.wd5.myworkdayjobs.com/syscocareers-sitemap.xml  → "syscocareers"
          https://walmart.wd5.myworkdayjobs.com/en-US/WalmartExternal-sitemap.xml → "WalmartExternal"
          https://lowes.wd5.myworkdayjobs.com/sitemap.xml  → "lowes" (slug fallback)
          https://pg.wd5.myworkdayjobs.com:1000/sitemap.xml → "pg" (slug fallback)
        """
        try:
            from urllib.parse import urlparse
            parsed = urlparse(sitemap_url)

            # Get path segments, filter out empty strings and locale segments like "en-US"
            segments = [
                s for s in parsed.path.strip("/").split("/")
                if s and not re.match(r'^[a-z]{2}-[A-Z]{2}$', s)
            ]

            if not segments:
                return slug

            # Last segment is typically "{career_site_name}-sitemap.xml" or "sitemap.xml"
            last = segments[-1]

            # Strip sitemap suffix
            career_site = re.sub(r'-?sitemap\.xml$', '', last, flags=re.IGNORECASE)
            career_site = re.sub(r'\.xml$', '', career_site, flags=re.IGNORECASE)

            # Reject if result looks like a port, number, or hostname
            if not career_site or career_site.isdigit() or '.' in career_site:
                return slug

            return career_site

        except Exception:
            return slug

    async def _probe_workday_single(
        self,
        client: httpx.AsyncClient,
        slug: str,
        instance: str,
        company_sem: asyncio.Semaphore,
    ) -> dict[str, Any] | None:
        """Probe one slug+instance combination via robots.txt."""
        base_url = f"https://{slug}.{instance}.myworkdayjobs.com"
        domain = _extract_domain(base_url)

        async with company_sem:
            async with _GLOBAL_SEM:
                await _rate_limit_domain(domain)
                try:
                    robots_resp = await client.get(
                        f"{base_url}/robots.txt",
                        timeout=8.0,
                        follow_redirects=True,
                    )
                except Exception as exc:
                    logger.debug("Workday probe failed for %s on %s: %s", slug, instance, exc)
                    return None

        if robots_resp.status_code != 200 or "myworkdayjobs" not in str(robots_resp.url):
            return None

        sitemap_url = None
        for line in robots_resp.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemap_url = line.split(":", 1)[1].strip()
                break

        if not sitemap_url:
            sitemap_url = f"{base_url}/sitemap.xml"

        career_site = self._extract_career_site_name(sitemap_url, slug)
        logger.info("Workday probe SUCCESS: %s on %s -> career_site=%s", slug, instance, career_site)

        return {
            "ats_type": "workday",
            "ats_slug": slug,
            "crawl_url": sitemap_url,
            "crawl_config": {
                "instance": instance,
                "career_site_name": career_site,
                "sitemap_url": sitemap_url,
            },
        }

    async def _probe_workday(self, client: httpx.AsyncClient, base_slug: str, company_sem: asyncio.Semaphore) -> dict[str, Any] | None:
        """Probe all Workday slug+instance combinations concurrently."""
        workday_variants = [
            base_slug,
            f"{base_slug}careers",
            f"{base_slug}ext",
            f"{base_slug}global",
        ]
        instances = ["wd5", "wd1", "wd3"]

        tasks = [
            self._probe_workday_single(client, slug, instance, company_sem)
            for slug in workday_variants
            for instance in instances
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, dict):
                return result
        return None

    async def probe(
        self,
        company: dict[str, Any],
        skip_ats: frozenset[str] | None = None,
    ) -> dict[str, Any] | None:
        """Return first matching ATS dict or None.

        Args:
            company:  dict with keys name, website, yc_slug (any may be None).
            skip_ats: optional set of ats_type strings to skip entirely, e.g.
                      frozenset({"workday"}) for YC companies that never use it.

        Returns:
            {"ats_type": str, "ats_slug": str, "crawl_url": str} or None.
        """
        slug = _derive_slug(company)
        if not slug:
            logger.debug("Could not derive slug for company: %s", company.get("name"))
            return None

        skip_ats = skip_ats or frozenset()
        company_sem = asyncio.Semaphore(5)
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

        async with httpx.AsyncClient(
            timeout=_PROBE_TIMEOUT,
            follow_redirects=True,
            limits=_CLIENT_LIMITS,
            headers=headers,
        ) as client:
            tasks = []
            for pattern in ATS_PROBE_PATTERNS:
                if pattern["ats_type"] in skip_ats:
                    continue
                if pattern["ats_type"] == "workday":
                    tasks.append(self._probe_workday(client, slug, company_sem))
                else:
                    tasks.append(_probe_pattern(client, pattern, slug, company_sem))
                    
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict):
                logger.info(
                    "Probe match: %s → %s (slug=%s)",
                    company.get("name"),
                    result["ats_type"],
                    slug,
                )
                return result

        logger.debug("No ATS match for %s (slug=%s)", company.get("name"), slug)
        return None
