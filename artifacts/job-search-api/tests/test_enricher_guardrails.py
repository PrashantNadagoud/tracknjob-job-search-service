"""
Session 16 acceptance tests: Three Data-Integrity Guardrails.

G1 — Conditional enriched_at (Guardrail 1)
  G1a: All sources fail → enriched_at is None, enrichment_source == [].
  G1b: At least one source succeeds → enriched_at is set.

G2 — Field-level bounds validation via _apply_validated() (Guardrail 2)
  G2a: founded_year outside [1800, current year] is rejected;
       a valid year (2009) is accepted.
  G2b: salary_min_usd ≤ 0 or ≥ 10 000 000 is rejected.
  G2c: num_employees_range with a non-canonical string is rejected;
       a canonical value is accepted.
  G2d: culture_score as a letter grade (e.g. "A+") is rejected because
       it is not parseable as a float.

G3 — Additive-only writes (Guardrail 3)
  G3a: An existing non-null field value is never overwritten by a new
       incoming value.
  G3b: None / "" incoming values do not overwrite existing non-null fields
       and do not overwrite None fields (nothing changes).
"""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from app.enrichment.enricher import CompanyEnricher, CompanyRecord, _VALID_EMPLOYEE_RANGES
from app.enrichment.wikipedia import WikipediaResult


# ── Helpers ───────────────────────────────────────────────────────────────────


def _fresh_record(**kwargs) -> CompanyRecord:
    """Return a CompanyRecord with slug/name defaults plus any overrides."""
    defaults = {"slug": "test-guard-co", "name": "GuardCo"}
    defaults.update(kwargs)
    return CompanyRecord(**defaults)


def _enricher() -> CompanyEnricher:
    return CompanyEnricher()


# ── G1: Conditional enriched_at ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_g1a_all_sources_fail_enriched_at_is_none():
    """G1a: When every source raises an exception, enriched_at stays None."""
    enricher = _enricher()
    err = ConnectionError("network down")

    with (
        patch("app.enrichment.enricher.enrich_from_wikipedia", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.enrich_from_linkedin", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.enrich_from_comparably", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.enrich_from_builtin", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.enrich_salary_from_glassdoor", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.asyncio.sleep", new_callable=AsyncMock),
    ):
        record = await enricher.enrich("test-g1a-co", "G1aCo", "Engineer", "Remote")

    assert record.enriched_at is None, (
        "Guardrail 1: enriched_at must be None when enrichment_source is empty"
    )
    assert record.enrichment_source == []


@pytest.mark.asyncio
async def test_g1b_one_source_succeeds_enriched_at_is_set():
    """G1b: When at least one source returns real data, enriched_at is set."""
    enricher = _enricher()
    err = ConnectionError("network down")
    wiki_ok = WikipediaResult(founded_year=2009, sources=["wikipedia"])

    with (
        patch(
            "app.enrichment.enricher.enrich_from_wikipedia",
            AsyncMock(return_value=wiki_ok),
        ),
        patch("app.enrichment.enricher.enrich_from_linkedin", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.enrich_from_comparably", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.enrich_from_builtin", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.enrich_salary_from_glassdoor", AsyncMock(side_effect=err)),
        patch("app.enrichment.enricher.asyncio.sleep", new_callable=AsyncMock),
    ):
        record = await enricher.enrich("test-g1b-co", "G1bCo", "Engineer", "Remote")

    assert record.enriched_at is not None, (
        "Guardrail 1: enriched_at must be set when at least one source succeeded"
    )
    assert isinstance(record.enriched_at, datetime)
    assert "wikipedia" in record.enrichment_source


# ── G2: Field-level bounds validation ─────────────────────────────────────────


def test_g2a_invalid_founded_year_rejected():
    """G2a: founded_year outside [1800, current_year] is rejected; 2009 is accepted."""
    enricher = _enricher()
    current_year = datetime.now(timezone.utc).year

    # Too old — before 1800
    record = _fresh_record()
    enricher._apply_validated(record, {"founded_year": 1700})
    assert record.founded_year is None, f"1700 should have been rejected, got {record.founded_year}"

    # Future year
    record = _fresh_record()
    enricher._apply_validated(record, {"founded_year": current_year + 1})
    assert record.founded_year is None, f"future year should be rejected, got {record.founded_year}"

    # Valid year
    record = _fresh_record()
    enricher._apply_validated(record, {"founded_year": 2009})
    assert record.founded_year == 2009, f"2009 should be accepted, got {record.founded_year}"


def test_g2b_invalid_salary_min_usd_rejected():
    """G2b: salary_min_usd ≤ 0 or ≥ 10 000 000 is rejected; a mid-range value is accepted."""
    enricher = _enricher()

    # Negative
    record = _fresh_record()
    enricher._apply_validated(record, {"salary_min_usd": -1000})
    assert record.salary_min_usd is None, "Negative salary should be rejected"

    # Zero
    record = _fresh_record()
    enricher._apply_validated(record, {"salary_min_usd": 0})
    assert record.salary_min_usd is None, "Zero salary should be rejected"

    # Exceeds cap (≥ 10 000 000)
    record = _fresh_record()
    enricher._apply_validated(record, {"salary_min_usd": 10_000_000})
    assert record.salary_min_usd is None, "Salary ≥ 10 000 000 should be rejected"

    # Valid
    record = _fresh_record()
    enricher._apply_validated(record, {"salary_min_usd": 85_000})
    assert record.salary_min_usd == 85_000, f"85 000 should be accepted, got {record.salary_min_usd}"


def test_g2c_invalid_employee_range_rejected():
    """G2c: Non-canonical num_employees_range is rejected; canonical values are accepted."""
    enricher = _enricher()

    # Non-canonical (old format)
    for bad_range in ("1-49", "50-199", "200-499", "5001+", "unknown", ""):
        record = _fresh_record()
        if bad_range == "":
            enricher._apply_validated(record, {"num_employees_range": bad_range})
            assert record.num_employees_range is None, (
                f"Empty string should be skipped, got {record.num_employees_range}"
            )
        else:
            enricher._apply_validated(record, {"num_employees_range": bad_range})
            assert record.num_employees_range is None, (
                f"Non-canonical '{bad_range}' should be rejected, got {record.num_employees_range}"
            )

    # All canonical values must be accepted
    for good_range in sorted(_VALID_EMPLOYEE_RANGES):
        record = _fresh_record()
        enricher._apply_validated(record, {"num_employees_range": good_range})
        assert record.num_employees_range == good_range, (
            f"Canonical '{good_range}' should be accepted, got {record.num_employees_range}"
        )


def test_g2d_culture_score_letter_grade_rejected():
    """G2d: culture_score as a letter grade (non-float) is rejected by G2."""
    enricher = _enricher()

    for letter_grade in ("A+", "A", "B+", "B", "C", "F"):
        record = _fresh_record()
        enricher._apply_validated(record, {"culture_score": letter_grade})
        assert record.culture_score is None, (
            f"Letter grade '{letter_grade}' should be rejected (not parseable as float)"
        )

    # Numeric string should be accepted (float-parseable and within bounds)
    record = _fresh_record()
    enricher._apply_validated(record, {"culture_score": "4.2"})
    # culture_score field is typed as str, but _apply_validated accepts it only if float-parseable
    # The raw string "4.2" parses as 4.2 which is in [0.0, 5.0]
    assert record.culture_score == "4.2", (
        f"Numeric string '4.2' should be accepted, got {record.culture_score}"
    )


# ── G3: Additive-only writes ──────────────────────────────────────────────────


def test_g3a_existing_non_null_field_not_overwritten():
    """G3a: If a record field already has a value, a new incoming value is skipped."""
    enricher = _enricher()

    record = _fresh_record(founded_year=2009)
    enricher._apply_validated(record, {"founded_year": 2015})
    assert record.founded_year == 2009, (
        f"G3 violation: existing founded_year 2009 overwritten with 2015"
    )

    record = _fresh_record(num_employees_range="201-500")
    enricher._apply_validated(record, {"num_employees_range": "1001-5000"})
    assert record.num_employees_range == "201-500", (
        "G3 violation: existing num_employees_range overwritten"
    )

    record = _fresh_record(salary_min_usd=60_000)
    enricher._apply_validated(record, {"salary_min_usd": 120_000})
    assert record.salary_min_usd == 60_000, (
        "G3 violation: existing salary_min_usd overwritten"
    )


def test_g3b_none_and_empty_incoming_values_are_skipped():
    """G3b: None and '' incoming values never touch existing or unset record fields."""
    enricher = _enricher()

    # Unset field — None incoming must leave it None (not write None over None)
    record = _fresh_record()
    assert record.founded_year is None
    enricher._apply_validated(record, {"founded_year": None})
    assert record.founded_year is None, "None incoming should leave an unset field alone"

    # Unset field — empty string incoming must leave it None
    record = _fresh_record()
    enricher._apply_validated(record, {"remote_policy": ""})
    assert record.remote_policy is None, "Empty string incoming should leave an unset field alone"

    # Already-set field — None incoming must not erase it
    record = _fresh_record(founded_year=2005)
    enricher._apply_validated(record, {"founded_year": None})
    assert record.founded_year == 2005, "None incoming must not erase an existing non-null value"

    # Already-set field — empty string incoming must not erase it
    record = _fresh_record(remote_policy="hybrid")
    enricher._apply_validated(record, {"remote_policy": ""})
    assert record.remote_policy == "hybrid", "'' incoming must not erase an existing value"
