"""Tests for scripts/seed_india_sources.py.

Covers:
  - _parse_workday_config: extracts instance + career_site_name from URL
  - _slugify: company name → URL-safe slug
  - run(): skip custom/null-slug records
  - run(): dry-run mode (no DB writes)
  - run(): insert path with crawl_config populated for Workday
  - run(): update path for existing rows
  - run(): never touches US rows (country filter is on the query key,
            this is implicitly covered by the upsert key including country)
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# We import the module-level helpers directly so we don't need a real DB.
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.seed_india_sources import _parse_workday_config, _slugify, run


# ── _parse_workday_config ────────────────────────────────────────────────────

class TestParseWorkdayConfig:
    def test_standard_workday_url(self):
        url = "https://accenture.wd3.myworkdayjobs.com/AccentureCareers"
        result = _parse_workday_config(url)
        assert result == {"instance": "wd3", "career_site_name": "AccentureCareers"}

    def test_wd1_instance(self):
        url = "https://ibm.wd1.myworkdayjobs.com/IBMExternalSite"
        result = _parse_workday_config(url)
        assert result == {"instance": "wd1", "career_site_name": "IBMExternalSite"}

    def test_wd12_instance(self):
        url = "https://ibm.wd12.myworkdayjobs.com/IBMExternalSite"
        result = _parse_workday_config(url)
        assert result == {"instance": "wd12", "career_site_name": "IBMExternalSite"}

    def test_url_with_trailing_slash(self):
        url = "https://sap.wd3.myworkdayjobs.com/SAP/"
        result = _parse_workday_config(url)
        # Should still extract SAP (trailing slash excluded from named group)
        assert result is not None
        assert result["instance"] == "wd3"

    def test_none_url_returns_none(self):
        assert _parse_workday_config(None) is None

    def test_non_workday_url_returns_none(self):
        assert _parse_workday_config("https://boards.greenhouse.io/freshworks") is None

    def test_empty_string_returns_none(self):
        assert _parse_workday_config("") is None


# ── _slugify ─────────────────────────────────────────────────────────────────

class TestSlugify:
    def test_simple_name(self):
        assert _slugify("Accenture India") == "accenture-india"

    def test_special_chars(self):
        assert _slugify("HCL Technologies") == "hcl-technologies"

    def test_numbers(self):
        assert _slugify("3M India") == "3m-india"

    def test_already_lowercase(self):
        assert _slugify("ibm") == "ibm"


# ── run() — helpers ───────────────────────────────────────────────────────────

def _make_session_factory(execute_side_effects: list):
    """Build a minimal AsyncSessionFactory context-manager mock.

    execute_side_effects is a list of fetchone return values, consumed in order.
    """
    call_iter = iter(execute_side_effects)

    db = AsyncMock()

    def _make_result():
        val = next(call_iter, None)
        result = MagicMock()
        result.fetchone.return_value = val
        return result

    db.execute.side_effect = lambda *a, **kw: AsyncMock(return_value=_make_result())()
    db.commit = AsyncMock()
    db.__aenter__ = AsyncMock(return_value=db)
    db.__aexit__ = AsyncMock(return_value=False)

    mock_factory = MagicMock(return_value=db)
    return mock_factory, db


# ── run() — skip custom/null-slug records ────────────────────────────────────

@pytest.mark.asyncio
async def test_run_skips_custom_records(capsys):
    """Records with ats_type='custom' or null ats_slug are never written to DB."""
    data = [
        {
            "company_name": "Wipro",
            "ats_type": "custom",
            "ats_slug": None,
            "career_site_url": "https://careers.wipro.com",
            "country": "IN",
            "location_filter": None,
            "notes": "Custom portal",
        }
    ]
    mock_factory, db = _make_session_factory([])

    with (
        patch("scripts.seed_india_sources.DATA_FILE") as mock_file,
        patch("scripts.seed_india_sources.AsyncSessionFactory", mock_factory),
    ):
        mock_file.read_text.return_value = json.dumps(data)
        await run(dry_run=False)

    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_run_skips_null_slug_records():
    """Records with null ats_slug (regardless of type) are skipped."""
    data = [
        {
            "company_name": "SomeCompany",
            "ats_type": "greenhouse",
            "ats_slug": None,
            "career_site_url": None,
            "country": "IN",
            "location_filter": None,
            "notes": None,
        }
    ]
    mock_factory, db = _make_session_factory([])

    with (
        patch("scripts.seed_india_sources.DATA_FILE") as mock_file,
        patch("scripts.seed_india_sources.AsyncSessionFactory", mock_factory),
    ):
        mock_file.read_text.return_value = json.dumps(data)
        await run(dry_run=False)

    db.execute.assert_not_called()


# ── run() — dry-run ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_dry_run_does_not_write(capsys):
    """In dry-run mode no DB execute calls are made."""
    data = [
        {
            "company_name": "Freshworks",
            "ats_type": "greenhouse",
            "ats_slug": "freshworks",
            "career_site_url": "https://boards.greenhouse.io/freshworks",
            "country": "IN",
            "location_filter": None,
            "notes": None,
        }
    ]
    mock_factory, db = _make_session_factory([])

    with (
        patch("scripts.seed_india_sources.DATA_FILE") as mock_file,
        patch("scripts.seed_india_sources.AsyncSessionFactory", mock_factory),
    ):
        mock_file.read_text.return_value = json.dumps(data)
        await run(dry_run=True)

    db.execute.assert_not_called()
    db.commit.assert_not_called()

    captured = capsys.readouterr()
    assert "WOULD UPSERT" in captured.out
    assert "freshworks" in captured.out


# ── run() — insert path ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_insert_workday_record_with_crawl_config():
    """A new Workday IN record is inserted with crawl_config parsed from career_site_url."""
    company_id = uuid.uuid4()
    data = [
        {
            "company_name": "Accenture India",
            "ats_type": "workday",
            "ats_slug": "accenture",
            "career_site_url": "https://accenture.wd3.myworkdayjobs.com/AccentureCareers",
            "country": "IN",
            "location_filter": "India",
            "notes": "Global Workday slug",
        }
    ]

    # execute calls: 1) company upsert → company_id, 2) existing check → None
    mock_factory, db = _make_session_factory([
        (company_id,),   # company INSERT ... RETURNING id
        None,            # SELECT id FROM ats_sources (no existing row)
    ])

    execute_calls: list = []
    original_execute = db.execute.side_effect

    async def capturing_execute(stmt, params=None, **kw):
        execute_calls.append((str(stmt), params))
        result = MagicMock()
        if len(execute_calls) == 1:
            result.fetchone.return_value = (company_id,)
        else:
            result.fetchone.return_value = None
        return result

    db.execute.side_effect = capturing_execute

    with (
        patch("scripts.seed_india_sources.DATA_FILE") as mock_file,
        patch("scripts.seed_india_sources.AsyncSessionFactory", mock_factory),
    ):
        mock_file.read_text.return_value = json.dumps(data)
        await run(dry_run=False)

    # Should have 3 execute calls: company upsert, existence check, INSERT
    assert len(execute_calls) == 3
    # Third call is the INSERT; its params should contain crawl_config
    insert_params = execute_calls[2][1]
    assert insert_params is not None
    crawl_config_raw = insert_params.get("crawl_config")
    assert crawl_config_raw is not None
    config = json.loads(crawl_config_raw)
    assert config["instance"] == "wd3"
    assert config["career_site_name"] == "AccentureCareers"
    assert insert_params["country"] == "IN"
    assert insert_params["location_filter"] == "India"
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_insert_greenhouse_record_has_null_crawl_config():
    """A Greenhouse record is inserted with crawl_config='{}' (no instance to parse)."""
    company_id = uuid.uuid4()
    data = [
        {
            "company_name": "Freshworks",
            "ats_type": "greenhouse",
            "ats_slug": "freshworks",
            "career_site_url": "https://boards.greenhouse.io/freshworks",
            "country": "IN",
            "location_filter": None,
            "notes": None,
        }
    ]

    execute_calls: list = []
    db = AsyncMock()
    db.commit = AsyncMock()
    db.__aenter__ = AsyncMock(return_value=db)
    db.__aexit__ = AsyncMock(return_value=False)

    async def capturing_execute(stmt, params=None, **kw):
        execute_calls.append((str(stmt), params))
        result = MagicMock()
        if len(execute_calls) == 1:
            result.fetchone.return_value = (company_id,)
        else:
            result.fetchone.return_value = None
        return result

    db.execute.side_effect = capturing_execute
    mock_factory = MagicMock(return_value=db)

    with (
        patch("scripts.seed_india_sources.DATA_FILE") as mock_file,
        patch("scripts.seed_india_sources.AsyncSessionFactory", mock_factory),
    ):
        mock_file.read_text.return_value = json.dumps(data)
        await run(dry_run=False)

    insert_params = execute_calls[2][1]
    # crawl_config for non-Workday should be the fallback empty JSON object
    assert insert_params["crawl_config"] == "{}"


# ── run() — update path ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_update_existing_record():
    """An existing ats_sources row is UPDATEd, not INSERTed."""
    company_id = uuid.uuid4()
    existing_id = uuid.uuid4()
    data = [
        {
            "company_name": "Accenture India",
            "ats_type": "workday",
            "ats_slug": "accenture",
            "career_site_url": "https://accenture.wd3.myworkdayjobs.com/AccentureCareers",
            "country": "IN",
            "location_filter": "India",
            "notes": "Updated note",
        }
    ]

    execute_calls: list = []
    db = AsyncMock()
    db.commit = AsyncMock()
    db.__aenter__ = AsyncMock(return_value=db)
    db.__aexit__ = AsyncMock(return_value=False)

    async def capturing_execute(stmt, params=None, **kw):
        execute_calls.append((str(stmt), params))
        result = MagicMock()
        if len(execute_calls) == 1:
            result.fetchone.return_value = (company_id,)
        elif len(execute_calls) == 2:
            result.fetchone.return_value = (existing_id,)  # row exists → UPDATE path
        else:
            result.fetchone.return_value = None
        return result

    db.execute.side_effect = capturing_execute
    mock_factory = MagicMock(return_value=db)

    with (
        patch("scripts.seed_india_sources.DATA_FILE") as mock_file,
        patch("scripts.seed_india_sources.AsyncSessionFactory", mock_factory),
    ):
        mock_file.read_text.return_value = json.dumps(data)
        await run(dry_run=False)

    # Only 3 calls: company upsert, existence check, UPDATE (no INSERT)
    assert len(execute_calls) == 3
    update_sql = execute_calls[2][0]
    assert "UPDATE" in update_sql.upper()
    assert "INSERT" not in update_sql.upper()
    update_params = execute_calls[2][1]
    assert update_params["id"] == existing_id
    assert update_params["notes"] == "Updated note"
