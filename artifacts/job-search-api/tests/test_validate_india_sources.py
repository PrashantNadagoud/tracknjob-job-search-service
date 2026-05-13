"""Tests for scripts/validate_india_sources.py.

Covers:
  - _parse_workday_url: extracts (instance, career_site_name) from URL
  - _try_workday_cxs: single probe success/failure
  - _probe_workday_sync: uses career_site_url first, then brute-forces
  - _probe_greenhouse_sync: 200 → active, non-200 → inactive
  - _probe_lever_sync: 200 list → active, error → inactive
  - _probe: dispatches by ats_type
  - run(): dry-run no DB writes, probe routing, DB update on success/failure
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.validate_india_sources import (
    _parse_workday_url,
    _probe,
    _probe_greenhouse_sync,
    _probe_lever_sync,
    _probe_workday_sync,
    _try_workday_cxs,
    run,
)


# ── _parse_workday_url ────────────────────────────────────────────────────────

class TestParseWorkdayUrl:
    def test_standard_url(self):
        url = "https://accenture.wd3.myworkdayjobs.com/AccentureCareers"
        assert _parse_workday_url(url) == ("wd3", "AccentureCareers")

    def test_wd12_instance(self):
        url = "https://ibm.wd12.myworkdayjobs.com/IBMExternalSite"
        assert _parse_workday_url(url) == ("wd12", "IBMExternalSite")

    def test_none_returns_none(self):
        assert _parse_workday_url(None) is None

    def test_non_workday_url_returns_none(self):
        assert _parse_workday_url("https://boards.greenhouse.io/freshworks") is None

    def test_empty_string_returns_none(self):
        assert _parse_workday_url("") is None


# ── _try_workday_cxs ─────────────────────────────────────────────────────────

class TestTryWorkdayCxs:
    def _mock_post(self, status: int, body: dict | None = None):
        resp = MagicMock()
        resp.status_code = status
        resp.json.return_value = body or {}
        return resp

    def test_success_returns_dict(self):
        resp = self._mock_post(200, {"jobPostings": [{"title": "SWE"}]})
        with patch("scripts.validate_india_sources.httpx.post", return_value=resp):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _try_workday_cxs("acme", "wd5", "External", "India")
        assert result is not None
        assert result["active"] is True
        assert result["crawl_config"] == {"instance": "wd5", "career_site_name": "External"}
        assert "career_site_url" in result

    def test_non_200_returns_none(self):
        resp = self._mock_post(404)
        with patch("scripts.validate_india_sources.httpx.post", return_value=resp):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _try_workday_cxs("acme", "wd5", "External", "India")
        assert result is None

    def test_empty_postings_returns_none(self):
        resp = self._mock_post(200, {"jobPostings": None})
        with patch("scripts.validate_india_sources.httpx.post", return_value=resp):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _try_workday_cxs("acme", "wd5", "External", None)
        assert result is None

    def test_network_error_returns_none(self):
        with patch(
            "scripts.validate_india_sources.httpx.post",
            side_effect=Exception("Connection refused"),
        ):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _try_workday_cxs("acme", "wd5", "External", "India")
        assert result is None

    def test_location_filter_included_in_body(self):
        posted_bodies: list[dict] = []

        def fake_post(url, json=None, **kw):
            posted_bodies.append(json or {})
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"jobPostings": []}
            return resp

        with patch("scripts.validate_india_sources.httpx.post", side_effect=fake_post):
            with patch("scripts.validate_india_sources.time.sleep"):
                _try_workday_cxs("acme", "wd5", "External", "India")

        assert posted_bodies[0].get("appliedFacets", {}).get("Location") == ["India"]

    def test_no_location_filter_omits_facets(self):
        posted_bodies: list[dict] = []

        def fake_post(url, json=None, **kw):
            posted_bodies.append(json or {})
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"jobPostings": []}
            return resp

        with patch("scripts.validate_india_sources.httpx.post", side_effect=fake_post):
            with patch("scripts.validate_india_sources.time.sleep"):
                _try_workday_cxs("acme", "wd5", "External", None)

        assert "appliedFacets" not in posted_bodies[0]


# ── _probe_workday_sync ───────────────────────────────────────────────────────

class TestProbeWorkdaySync:
    def test_uses_career_site_url_first(self):
        """Should try the parsed URL before brute-forcing."""
        call_log: list[tuple] = []

        def fake_try(slug, instance, site_name, location_filter):
            call_log.append((instance, site_name))
            if (instance, site_name) == ("wd3", "AccentureCareers"):
                return {
                    "active": True,
                    "career_site_url": "https://accenture.wd3.myworkdayjobs.com/AccentureCareers",
                    "crawl_config": {"instance": "wd3", "career_site_name": "AccentureCareers"},
                }
            return None

        with patch("scripts.validate_india_sources._try_workday_cxs", side_effect=fake_try):
            result = _probe_workday_sync(
                "accenture",
                "India",
                "https://accenture.wd3.myworkdayjobs.com/AccentureCareers",
            )

        assert result["active"] is True
        # The wd3/AccentureCareers probe must be the FIRST attempted
        assert call_log[0] == ("wd3", "AccentureCareers")

    def test_falls_back_to_brute_force_on_failure(self):
        """If career_site_url probe fails, brute-force must be attempted."""
        call_log: list[tuple] = []

        def fake_try(slug, instance, site_name, location_filter):
            call_log.append((instance, site_name))
            # Only succeed on wd1/External
            if (instance, site_name) == ("wd1", "External"):
                return {
                    "active": True,
                    "career_site_url": "https://slug.wd1.myworkdayjobs.com/External",
                    "crawl_config": {"instance": "wd1", "career_site_name": "External"},
                }
            return None

        with patch("scripts.validate_india_sources._try_workday_cxs", side_effect=fake_try):
            result = _probe_workday_sync(
                "slug",
                "India",
                "https://slug.wd3.myworkdayjobs.com/Careers",  # this will fail
            )

        assert result["active"] is True
        assert result["crawl_config"] == {"instance": "wd1", "career_site_name": "External"}
        # First attempt was wd3/Careers (from URL), then brute-force found wd1/External
        assert call_log[0] == ("wd3", "Careers")
        assert ("wd1", "External") in call_log

    def test_all_fail_returns_inactive(self):
        with patch("scripts.validate_india_sources._try_workday_cxs", return_value=None):
            result = _probe_workday_sync("acme", "India", None)
        assert result["active"] is False
        assert result["crawl_config"] is None

    def test_no_career_site_url_goes_directly_to_brute_force(self):
        call_log: list[tuple] = []

        def fake_try(slug, instance, site_name, location_filter):
            call_log.append((instance, site_name))
            return None

        with patch("scripts.validate_india_sources._try_workday_cxs", side_effect=fake_try):
            _probe_workday_sync("acme", "India", None)

        # Without a URL to parse, should try all instance×site_name combinations
        instances_tried = {c[0] for c in call_log}
        assert len(instances_tried) >= 2


# ── _probe_greenhouse_sync ────────────────────────────────────────────────────

class TestProbeGreenhouseSync:
    def _mock_get(self, status: int, body: dict | None = None):
        resp = MagicMock()
        resp.status_code = status
        resp.json.return_value = body or {}
        return resp

    def test_success_200_returns_active(self):
        resp = self._mock_get(200, {"jobs": [{"id": 1, "title": "SWE"}]})
        with patch("scripts.validate_india_sources.httpx.get", return_value=resp):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _probe_greenhouse_sync("freshworks")
        assert result["active"] is True
        assert "freshworks" in result["career_site_url"]
        assert result["crawl_config"] is None

    def test_404_returns_inactive(self):
        resp = self._mock_get(404)
        with patch("scripts.validate_india_sources.httpx.get", return_value=resp):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _probe_greenhouse_sync("unknown-slug")
        assert result["active"] is False

    def test_network_error_returns_inactive(self):
        with patch(
            "scripts.validate_india_sources.httpx.get",
            side_effect=Exception("timeout"),
        ):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _probe_greenhouse_sync("freshworks")
        assert result["active"] is False


# ── _probe_lever_sync ─────────────────────────────────────────────────────────

class TestProbeLeverSync:
    def test_success_200_returns_active(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = [{"id": "abc", "text": "SWE"}]
        with patch("scripts.validate_india_sources.httpx.get", return_value=resp):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _probe_lever_sync("razorpay")
        assert result["active"] is True
        assert "razorpay" in result["career_site_url"]

    def test_non_list_body_returns_inactive(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"error": "not found"}
        with patch("scripts.validate_india_sources.httpx.get", return_value=resp):
            with patch("scripts.validate_india_sources.time.sleep"):
                result = _probe_lever_sync("razorpay")
        assert result["active"] is False


# ── _probe dispatcher ─────────────────────────────────────────────────────────

class TestProbeDispatch:
    def test_dispatches_workday(self):
        with patch(
            "scripts.validate_india_sources._probe_workday_sync",
            return_value={"active": True, "career_site_url": "x", "crawl_config": {}},
        ) as mock_wd:
            result = _probe("workday", "acme", "India", "https://acme.wd5.myworkdayjobs.com/Ext")
        mock_wd.assert_called_once_with("acme", "India", "https://acme.wd5.myworkdayjobs.com/Ext")
        assert result["active"] is True

    def test_dispatches_greenhouse(self):
        with patch(
            "scripts.validate_india_sources._probe_greenhouse_sync",
            return_value={"active": True, "career_site_url": "y", "crawl_config": None},
        ) as mock_gh:
            result = _probe("greenhouse", "freshworks", None, None)
        mock_gh.assert_called_once_with("freshworks")

    def test_dispatches_lever(self):
        with patch(
            "scripts.validate_india_sources._probe_lever_sync",
            return_value={"active": False, "career_site_url": None, "crawl_config": None},
        ) as mock_lv:
            _probe("lever", "slug", None, None)
        mock_lv.assert_called_once_with("slug")

    def test_unknown_type_returns_inactive(self):
        result = _probe("unknown_ats", "slug", None, None)
        assert result["active"] is False


# ── run() — dry-run ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_dry_run_no_db_writes(capsys):
    """Dry-run prints intentions but never executes any DB writes."""
    source_rows = [
        (uuid.uuid4(), "workday", "accenture", "India", uuid.uuid4(),
         "https://accenture.wd3.myworkdayjobs.com/AccentureCareers"),
    ]

    db = AsyncMock()
    result_mock = MagicMock()
    result_mock.fetchall.return_value = source_rows
    db.execute.return_value = result_mock
    db.__aenter__ = AsyncMock(return_value=db)
    db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=db)

    with patch("scripts.validate_india_sources.AsyncSessionFactory", mock_factory):
        await run(dry_run=True, limit=None)

    # Only the SELECT query is called (1 call), never the UPDATE
    assert db.execute.call_count == 1
    db.commit.assert_not_called()

    captured = capsys.readouterr()
    assert "WOULD PROBE" in captured.out
    assert "accenture" in captured.out


@pytest.mark.asyncio
async def test_run_no_rows_exits_early(capsys):
    """If no pending India sources, run() exits without probing."""
    db = AsyncMock()
    result_mock = MagicMock()
    result_mock.fetchall.return_value = []
    db.execute.return_value = result_mock
    db.__aenter__ = AsyncMock(return_value=db)
    db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=db)

    with patch("scripts.validate_india_sources.AsyncSessionFactory", mock_factory):
        await run(dry_run=False, limit=None)

    captured = capsys.readouterr()
    assert "No pending" in captured.out


# ── run() — live probe + DB update ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_successful_probe_activates_source():
    """A successful probe sets is_active=true and writes crawl_config back."""
    source_id = uuid.uuid4()
    source_rows = [
        (source_id, "workday", "accenture", "India", uuid.uuid4(),
         "https://accenture.wd3.myworkdayjobs.com/AccentureCareers"),
    ]

    # First factory call = SELECT, subsequent = UPDATE
    select_db = AsyncMock()
    select_result = MagicMock()
    select_result.fetchall.return_value = source_rows
    select_db.execute.return_value = select_result
    select_db.__aenter__ = AsyncMock(return_value=select_db)
    select_db.__aexit__ = AsyncMock(return_value=False)

    update_db = AsyncMock()
    update_db.__aenter__ = AsyncMock(return_value=update_db)
    update_db.__aexit__ = AsyncMock(return_value=False)

    call_count = 0

    def factory_side_effect():
        nonlocal call_count
        call_count += 1
        return select_db if call_count == 1 else update_db

    mock_factory = MagicMock(side_effect=factory_side_effect)

    fake_probe_result = {
        "active": True,
        "career_site_url": "https://accenture.wd3.myworkdayjobs.com/AccentureCareers",
        "crawl_config": {"instance": "wd3", "career_site_name": "AccentureCareers"},
    }

    with (
        patch("scripts.validate_india_sources.AsyncSessionFactory", mock_factory),
        patch("scripts.validate_india_sources._probe", return_value=fake_probe_result),
    ):
        await run(dry_run=False, limit=None)

    # UPDATE was called on the second DB session
    update_db.execute.assert_awaited_once()
    update_db.commit.assert_awaited_once()

    update_params = update_db.execute.call_args[0][1]
    assert update_params["active"] is True
    assert update_params["status"] == "validated"
    config_raw = update_params["crawl_config"]
    assert config_raw is not None
    config = json.loads(config_raw)
    assert config["instance"] == "wd3"
    assert config["career_site_name"] == "AccentureCareers"


@pytest.mark.asyncio
async def test_run_failed_probe_marks_validation_failed():
    """A failed probe sets is_active=false and last_crawl_status='validation_failed'."""
    source_id = uuid.uuid4()
    source_rows = [
        (source_id, "greenhouse", "bad-slug", None, uuid.uuid4(), None),
    ]

    select_db = AsyncMock()
    select_result = MagicMock()
    select_result.fetchall.return_value = source_rows
    select_db.execute.return_value = select_result
    select_db.__aenter__ = AsyncMock(return_value=select_db)
    select_db.__aexit__ = AsyncMock(return_value=False)

    update_db = AsyncMock()
    update_db.__aenter__ = AsyncMock(return_value=update_db)
    update_db.__aexit__ = AsyncMock(return_value=False)

    call_count = 0

    def factory_side_effect():
        nonlocal call_count
        call_count += 1
        return select_db if call_count == 1 else update_db

    mock_factory = MagicMock(side_effect=factory_side_effect)

    fake_probe_result = {"active": False, "career_site_url": None, "crawl_config": None}

    with (
        patch("scripts.validate_india_sources.AsyncSessionFactory", mock_factory),
        patch("scripts.validate_india_sources._probe", return_value=fake_probe_result),
    ):
        await run(dry_run=False, limit=None)

    update_params = update_db.execute.call_args[0][1]
    assert update_params["active"] is False
    assert update_params["status"] == "validation_failed"


@pytest.mark.asyncio
async def test_run_limit_applied_in_query():
    """The LIMIT clause is appended when limit param is provided."""
    select_db = AsyncMock()
    select_result = MagicMock()
    select_result.fetchall.return_value = []
    select_db.execute.return_value = select_result
    select_db.__aenter__ = AsyncMock(return_value=select_db)
    select_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=select_db)

    with patch("scripts.validate_india_sources.AsyncSessionFactory", mock_factory):
        await run(dry_run=False, limit=5)

    # The SQL text passed to execute should contain LIMIT 5
    sql_str = str(select_db.execute.call_args[0][0])
    assert "LIMIT 5" in sql_str
