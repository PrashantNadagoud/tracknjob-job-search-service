"""Geo-restriction classifier for job listings.

Classifies each listing as one of:
  'US'     — US-specific role (on-site, hybrid, or US-remote)
  'EU'     — European-specific role
  'IN'     — India-specific role
  'GLOBAL' — Truly remote with no geographic restriction
  None     — Unprocessed legacy row (treated as US in the default feed)
"""

from typing import Any

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
    "mumbai", "chennai", "noida", "gurgaon", "apac",
]

US_SIGNALS = [
    "united states", "usa", "u.s.", "new york", "san francisco",
    "seattle", "austin", "chicago", "boston", "los angeles",
    "denver", "atlanta", "dallas", "remote us", "remote - us",
    "us only", "must be located in the us",
]


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
        'US', 'EU', 'IN', or 'GLOBAL'
    """
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

    restriction = detect_geo_restriction(location_raw, description)

    if restriction:
        return restriction

    if work_type in ("remote", "fully_remote"):
        return "GLOBAL"

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

        if any(sig in combined for sig in ["india", "bangalore", "hyderabad", "pune"]):
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
