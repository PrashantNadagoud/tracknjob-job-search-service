"""Geo-restriction classifier for job listings.

Classifies each listing as one of:
  'US'     — US-specific role (on-site, hybrid, or US-remote)
  'EU'     — European-specific role
  'IN'     — India-specific role
  'GLOBAL' — Truly remote with no geographic restriction
  None     — Unprocessed legacy row (treated as US in the default feed)

Classification pipeline (in priority order):
  1. Structured country code from ATS JSON (Greenhouse/Ashby/Lever)
  2. GeoNames city lookup  ← TRA-362
  3. Existing INDIA/EU/US signal strings (safety net)
  4. Remote work_type → GLOBAL
  5. Fallback → OTHER / US
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

EUROPE_SIGNALS = [
    "germany", "france", "uk", "united kingdom", "netherlands",
    "spain", "sweden", "poland", "ireland", "portugal", "italy",
    "switzerland", "denmark", "norway", "finland", "austria",
    "belgium", "czech republic", "romania",
    "europe", "european union", "eu only", "emea",
    "cet", "cest", "gmt+1", "gmt+2", "bst", "wet",
    "right to work in europe", "eu work permit",
]

INDIA_SIGNALS = [
    "india", "bangalore", "bengaluru", "hyderabad", "pune",
    "mumbai", "chennai", "noida", "gurgaon", "gurugram",
    "kolkata", "calcutta", "new delhi", "delhi", "ahmedabad",
    "jaipur", "kochi", "coimbatore", "apac",
]

US_SIGNALS = [
    "united states", "usa", "u.s.", "new york", "san francisco",
    "seattle", "austin", "chicago", "boston", "los angeles",
    "denver", "atlanta", "dallas", "remote us", "remote - us",
    "us only", "must be located in the us",
]


# ---------------------------------------------------------------------------
# GeoNames city lookup (TRA-362)
# ---------------------------------------------------------------------------

# ISO 3166-1 alpha-2 codes for EU/EEA countries.
_EU_COUNTRY_CODES: frozenset[str] = frozenset({
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
    "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT",
    "NL", "PL", "PT", "RO", "SE", "SI", "SK",
    # EEA non-EU but typically treated as EU market
    "IS", "LI", "NO",
    # UK (post-Brexit still common in EU job boards)
    "GB",
    # Switzerland
    "CH",
})

# In-memory dict: lowercase city name/ascii_name → ISO country code (2-char).
# Loaded lazily at first use via load_geonames_index().
_geonames_index: dict[str, str] | None = None


def load_geonames_index(city_rows: list[tuple[str, str, str]]) -> None:
    """Populate the in-memory GeoNames index from DB rows.

    Args:
        city_rows: List of (name, ascii_name, country_code) tuples as returned
                   by a query on geo.cities.  Call this once at app startup.
    """
    global _geonames_index
    index: dict[str, str] = {}
    for name, ascii_name, country_code in city_rows:
        cc = country_code.upper()
        key_name = name.strip().lower()
        key_ascii = ascii_name.strip().lower()
        if key_name:
            index.setdefault(key_name, cc)
        if key_ascii and key_ascii != key_name:
            index.setdefault(key_ascii, cc)
    _geonames_index = index
    logger.info("GeoNames index loaded: %d city entries", len(index))


def _country_to_market(country_code: str) -> str | None:
    """Map an ISO country code to a geo-restriction market label.

    Returns 'US', 'EU', 'IN', or None (no match — will fall through to OTHER).
    """
    cc = country_code.upper()
    if cc in ("US",):
        return "US"
    if cc in ("IN",):
        return "IN"
    if cc in _EU_COUNTRY_CODES:
        return "EU"
    return None


def _tokenize_location(location_raw: str) -> list[str]:
    """Split a raw location string into candidate city-name tokens.

    Handles common patterns such as:
      "San Francisco, CA"  → ["san francisco", "ca", "san francisco ca"]
      "Ho Chi Minh City, VN" → ["ho chi minh city", "vn"]
      "Berlin"             → ["berlin"]
    """
    text = location_raw.strip().lower()
    parts = [p.strip() for p in re.split(r"[,/|]", text) if p.strip()]
    tokens: list[str] = list(parts)
    if len(parts) >= 2:
        tokens.append(" ".join(parts[:2]))
    tokens.append(text)
    return list(dict.fromkeys(t for t in tokens if t))


def classify_by_geonames(location_raw: str) -> str | None:
    """Return a market label ('US', 'EU', 'IN') by looking up city names from
    the GeoNames index, or None if no match is found.

    Falls back gracefully to None when the index has not been loaded yet so
    that the pipeline continues to the signal-string fallback.
    """
    if _geonames_index is None:
        return None

    for token in _tokenize_location(location_raw):
        cc = _geonames_index.get(token)
        if cc:
            market = _country_to_market(cc)
            if market:
                return market
    return None


def detect_geo_restriction(location_raw: str, description: str = "") -> str | None:
    """Return 'US', 'EU', 'IN', or None (no clear signal).

    Checks location_raw first, then first 2000 chars of description.
    US signals take priority — if a job says 'Remote US' it's US even if the
    description mentions European offices.
    """
    text = (location_raw + " " + description[:2000]).lower()

    if any(sig in text for sig in US_SIGNALS):
        return "US"
    if any(sig in text for sig in EUROPE_SIGNALS):
        return "EU"
    if any(sig in text for sig in INDIA_SIGNALS):
        return "IN"
    return None


def classify_listing(
    location_raw: str,
    description: str,
    work_type: str,
    country: str | None = None,
) -> str:
    """Return the final geo-restriction label used at ingest time.

    Args:
        location_raw: Raw location string from the ATS/job board.
        description:  Full job description text (used for signal matching).
        work_type:    Work arrangement: 'remote', 'fully_remote', 'hybrid', 'onsite', etc.
        country:      Structured ISO country code from ATS JSON (Ashby/Greenhouse/Lever).
                      Pass None if not available.

    Returns:
        'US', 'EU', 'IN', 'GLOBAL', or 'OTHER'
    """
    # Step 1: Structured country code from ATS JSON
    if country:
        c = country.lower().strip()
        if c in ("us", "usa", "united states"):
            return "US"
        if c in (
            "gb", "uk", "de", "fr", "nl", "ie", "se", "pl", "es",
            "pt", "it", "ch", "dk", "no", "fi", "at", "be", "cz", "ro",
        ):
            return "EU"
        if c in ("in", "india"):
            return "IN"

    # Step 2: GeoNames city lookup (TRA-362)
    geonames_result = classify_by_geonames(location_raw)
    if geonames_result:
        return geonames_result

    # Step 3: Signal-string heuristics (safety net)
    restriction = detect_geo_restriction(location_raw, description)

    if restriction:
        return restriction

    if work_type in ("remote", "fully_remote"):
        return "GLOBAL"

    # Only classify as OTHER when there's a non-empty location that didn't
    # match any known signal — empty/null location rows default to US.
    if location_raw and location_raw.strip():
        return "OTHER"

    return "US"


def parse_greenhouse_location(job: dict[str, Any]) -> tuple[str, str | None]:
    """Return (location_raw, country_code | None) from a Greenhouse job dict.

    Prioritises the structured offices[] array over the top-level location.name
    field.  The boards-api list endpoint may not include offices[], in which
    case this falls back to location.name and returns country_code=None so that
    the text heuristics in classify_listing() take over.

    Example job detail endpoint:
        https://boards-api.greenhouse.io/v1/boards/cloudflare/jobs/7742347
    """
    offices: list[dict[str, Any]] = job.get("offices", []) or []

    for office in offices:
        office_name = (office.get("name") or "").lower()
        office_location = (office.get("location") or "").lower()
        combined = f"{office_name} {office_location}"

        if any(sig in combined for sig in [
            "germany", "france", "united kingdom", "uk", "netherlands",
            "ireland", "sweden", "poland", "europe", "emea",
            "spain", "portugal", "italy", "switzerland", "denmark",
        ]):
            return combined, "EU"

        if any(sig in combined for sig in ["india", "bangalore", "hyderabad", "pune", "gurugram", "gurgaon", "kolkata", "noida", "mumbai", "chennai", "delhi"]):
            return combined, "IN"

        if any(sig in combined for sig in [
            "united states", "us", "usa", "remote us", "remote - us",
        ]):
            return combined, "US"

    location_name: str = (job.get("location") or {}).get("name") or ""
    return location_name, None


def parse_ashby_location(job: dict[str, Any]) -> tuple[str, str | None, str]:
    """Return (location_raw, country_code | None, work_type) from an Ashby job dict.

    Checks officeLocations[].countryCode for a structured country hint.
    Falls back to the location text field.

    Returns:
        location_raw: Text location string.
        country_code: ISO country code or None.
        work_type:    'remote', 'hybrid', 'onsite', or ''.
    """
    workplace_type: str = (job.get("workplaceType") or "").lower()
    if workplace_type in ("remote",):
        work_type = "remote"
    elif workplace_type in ("hybrid",):
        work_type = "hybrid"
    elif workplace_type in ("onsite",):
        work_type = "onsite"
    else:
        work_type = ""

    country_code: str | None = None
    office_locs: list[dict] = job.get("officeLocations", []) or []
    for ol in office_locs:
        cc = ol.get("countryCode") or ol.get("country")
        if cc:
            country_code = cc
            break

    location_raw: str = job.get("locationName") or job.get("location") or ""
    return location_raw, country_code, work_type
