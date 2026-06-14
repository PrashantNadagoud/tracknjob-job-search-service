"""Tests for the discovery queue processor (_process_discovery_item).

All DB and network calls are mocked so no real network or DB access occurs.
Focus areas:
- ATSProber.probe() is called (not CrawlDispatcher)
- detect_ats_from_careers_page() is tried first (stage 1)
- Company + AtsSource created ONLY on a confirmed ATS match
- Item marked 'resolved' on match, 'rejected' on no match
- Items with status != 'pending' are skipped
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models import AtsSource, Company, CompanyDiscoveryQueue


def _make_queue_item(
    *,
    status: str = "pending",
    attempt_count: int = 0,
    suspected_ats: str | None = None,
    suspected_slug: str | None = None,
    company_name: str = "Acme Corp",
    market: str = "US",
):
    item = MagicMock(spec=CompanyDiscoveryQueue)
    item.id = uuid.uuid4()
    item.status = status
    item.attempt_count = attempt_count
    item.suspected_ats = suspected_ats
    item.suspected_slug = suspected_slug
    item.company_name = company_name
    item.website = "https://acme.example.com"
    item.market = market
    item.source = "test"
    return item


def _make_company(company_name: str = "Acme Corp", slug: str | None = None) -> MagicMock:
    co = MagicMock(spec=Company)
    co.id = uuid.uuid4()
    co.slug = slug or company_name.lower().replace(" ", "-")
    co.name = company_name
    return co


def _make_ats_source(company_id: uuid.UUID, ats_type: str = "greenhouse") -> MagicMock:
    ats = MagicMock(spec=AtsSource)
    ats.id = uuid.uuid4()
    ats.company_id = company_id
    ats.ats_type = ats_type
    ats.is_active = True
    return ats


# Keep old names as aliases so unchanged tests continue to work
_make_probe_company = _make_company
_make_probe_ats = _make_ats_source


class TestProcessDiscoveryItem:
    """Unit tests for _process_discovery_item."""

    @pytest.mark.asyncio
    async def test_skips_non_pending_item(self):
        """Items with status != 'pending' are skipped immediately."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item(status="resolved")
        item_id = item.id

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=item)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        mock_session_maker = MagicMock()
        mock_session_maker.return_value = mock_session

        result = await _process_discovery_item(item_id, mock_session_maker)
        assert result == "skipped"

    @pytest.mark.asyncio
    async def test_skips_missing_item(self):
        """A deleted or missing queue item returns 'skipped'."""
        from app.crawler.tasks import _process_discovery_item

        item_id = uuid.uuid4()

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=None)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        mock_session_maker = MagicMock()
        mock_session_maker.return_value = mock_session

        result = await _process_discovery_item(item_id, mock_session_maker)
        assert result == "skipped"

    @pytest.mark.asyncio
    async def test_success_via_ats_prober_returns_resolved(self):
        """ATSProber returning a match results in 'resolved'."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item()
        item_id = item.id

        probe_match = {"ats_type": "greenhouse", "ats_slug": "acme-corp", "crawl_url": None}

        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()

            if call_count == 0:
                # Step 1: read item, mark attempted
                s.get = AsyncMock(return_value=item)
            else:
                # Step 2: write match — no existing company, no existing ats
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=None)
                s.execute = AsyncMock(return_value=execute_result)
                new_company = _make_company()
                s.flush = AsyncMock()
                s.add = MagicMock()
                q_item_copy = MagicMock(spec=CompanyDiscoveryQueue)
                q_item_copy.status = "pending"
                s.get = AsyncMock(return_value=q_item_copy)

            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch("app.discovery.ats_prober.detect_ats_from_careers_page", new=AsyncMock(return_value=None)):
            with patch("app.discovery.ats_prober.ATSProber.probe", new=AsyncMock(return_value=probe_match)):
                result = await _process_discovery_item(item_id, mock_session_maker)

        assert result == "resolved"

    @pytest.mark.asyncio
    async def test_no_match_returns_rejected(self):
        """When both fingerprinting and ATSProber find nothing, item is 'rejected'."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item(attempt_count=0)
        item_id = item.id

        q_item_mutable = MagicMock(spec=CompanyDiscoveryQueue)
        q_item_mutable.status = "pending"
        q_item_mutable.attempt_count = 0

        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()

            if call_count == 0:
                s.get = AsyncMock(return_value=item)
            else:
                s.get = AsyncMock(return_value=q_item_mutable)

            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch("app.discovery.ats_prober.detect_ats_from_careers_page", new=AsyncMock(return_value=None)):
            with patch("app.discovery.ats_prober.ATSProber.probe", new=AsyncMock(return_value=None)):
                result = await _process_discovery_item(item_id, mock_session_maker)

        assert result == "rejected"

    @pytest.mark.asyncio
    async def test_no_match_marks_queue_item_rejected(self):
        """When no ATS is found, queue item status is set to 'rejected'."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item(attempt_count=0)
        item_id = item.id

        q_item_mutable = MagicMock(spec=CompanyDiscoveryQueue)
        q_item_mutable.status = "pending"
        q_item_mutable.attempt_count = 0

        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()
            s.get = AsyncMock(return_value=item if call_count == 0 else q_item_mutable)
            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch("app.discovery.ats_prober.detect_ats_from_careers_page", new=AsyncMock(return_value=None)):
            with patch("app.discovery.ats_prober.ATSProber.probe", new=AsyncMock(return_value=None)):
                result = await _process_discovery_item(item_id, mock_session_maker)

        assert result == "rejected"
        assert q_item_mutable.status == "rejected"

    @pytest.mark.asyncio
    async def test_fingerprint_stage_tried_before_ats_prober(self):
        """detect_ats_from_careers_page is called as stage 1 before ATSProber."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item()
        item_id = item.id

        fingerprint_calls: list[str] = []
        prober_calls: list[str] = []

        async def mock_fingerprint(website):
            fingerprint_calls.append(website)
            return None  # fingerprint misses, fall through to prober

        async def mock_probe(self, company_dict):
            prober_calls.append(company_dict.get("name"))
            return None

        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()
            if call_count == 0:
                s.get = AsyncMock(return_value=item)
            else:
                q_item_m = MagicMock(spec=CompanyDiscoveryQueue)
                q_item_m.status = "pending"
                s.get = AsyncMock(return_value=q_item_m)
            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch("app.discovery.ats_prober.detect_ats_from_careers_page", new=mock_fingerprint):
            with patch("app.discovery.ats_prober.ATSProber.probe", new=mock_probe):
                await _process_discovery_item(item_id, mock_session_maker)

        # Stage 1 fingerprint was called with the company website
        assert len(fingerprint_calls) == 1
        assert "acme.example.com" in fingerprint_calls[0]
        # Stage 2 prober was called as fallback
        assert len(prober_calls) == 1

    @pytest.mark.asyncio
    async def test_fingerprint_match_skips_ats_prober(self):
        """When stage-1 fingerprinting succeeds, ATSProber.probe() is NOT called."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item()
        item_id = item.id

        fingerprint_result = {"ats_type": "greenhouse", "ats_slug": "acme"}
        prober_calls: list = []

        async def mock_probe(self, company_dict):
            prober_calls.append(company_dict)
            return None

        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()
            if call_count == 0:
                s.get = AsyncMock(return_value=item)
            else:
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=None)
                s.execute = AsyncMock(return_value=execute_result)
                s.flush = AsyncMock()
                s.add = MagicMock()
                q_item_copy = MagicMock(spec=CompanyDiscoveryQueue)
                q_item_copy.status = "pending"
                s.get = AsyncMock(return_value=q_item_copy)
            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch("app.discovery.ats_prober.detect_ats_from_careers_page", new=AsyncMock(return_value=fingerprint_result)):
            with patch("app.discovery.ats_prober.ATSProber.probe", new=mock_probe):
                result = await _process_discovery_item(item_id, mock_session_maker)

        assert result == "resolved"
        # ATSProber was NOT called because fingerprinting succeeded
        assert len(prober_calls) == 0
