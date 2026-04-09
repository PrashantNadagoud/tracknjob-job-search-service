"""Tests for the discovery queue processor (_process_discovery_item).

All DB and dispatcher calls are mocked so no real network or DB access occurs.
Focus areas:
- Dispatcher is called with the probe AtsSource ID (not raw crawler)
- Company + AtsSource created ONLY on successful probe
- Probe entities activated and renamed on success
- Attempt count incremented on failure
- Rejection at >= 3 attempts deletes probe entities
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
    suspected_ats: str = "workday",
    suspected_slug: str = "acme",
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


def _make_probe_company(company_name: str = "Acme Corp") -> MagicMock:
    co = MagicMock(spec=Company)
    co.id = uuid.uuid4()
    co.slug = company_name.lower() + "-probe"
    co.name = company_name
    return co


def _make_probe_ats(company_id: uuid.UUID, ats_type: str = "workday") -> MagicMock:
    ats = MagicMock(spec=AtsSource)
    ats.id = uuid.uuid4()
    ats.company_id = company_id
    ats.ats_type = ats_type
    ats.is_active = False
    return ats


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
    async def test_success_via_dispatcher_returns_resolved(self):
        """A successful probe returns 'resolved'."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item()
        probe_company = _make_probe_company()
        probe_ats = _make_probe_ats(probe_company.id)
        item_id = item.id

        dummy_jobs = [
            {"title": "SWE", "source_url": "http://example.com/1", "company": "Acme Corp"}
        ]

        # Session sequence: step1 (get item, probe company, probe ats),
        # step2 (dispatch), step3 (promote), step4 (upsert_ats_jobs)
        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()

            if call_count == 0:
                # Step 1: read item + get/create probe entities
                execute_result_item = MagicMock()
                execute_result_item.scalar_one_or_none = MagicMock(
                    side_effect=[probe_company, probe_ats]
                )
                s.execute = AsyncMock(return_value=execute_result_item)
                s.get = AsyncMock(return_value=item)
            elif call_count == 1:
                # Step 2: dispatch — dispatcher looks up probe ats
                s.get = AsyncMock(return_value=probe_ats)
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=probe_company)
                s.execute = AsyncMock(return_value=execute_result)
            elif call_count == 2:
                # Step 3: promote — look up real company (not found), rename probe
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=None)
                s.execute = AsyncMock(return_value=execute_result)
                q_item_copy = MagicMock(spec=CompanyDiscoveryQueue)
                q_item_copy.status = "pending"
                s.get = AsyncMock(side_effect=[probe_company, probe_ats, q_item_copy])
            else:
                # Step 4+: upsert
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=None)
                s.execute = AsyncMock(return_value=execute_result)
                s.get = AsyncMock(return_value=None)

            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch(
            "app.crawler.dispatcher.CrawlDispatcher",
            return_value=MagicMock(
                dispatch=AsyncMock(return_value=dummy_jobs)
            ),
        ):
            with patch("app.crawler.tasks._upsert_ats_jobs", new=AsyncMock(return_value=[])):
                result = await _process_discovery_item(item_id, mock_session_maker)

        assert result == "resolved"

    @pytest.mark.asyncio
    async def test_failure_increments_attempt_count(self):
        """A failed probe (no jobs returned) increments attempt_count."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item(attempt_count=0)
        probe_company = _make_probe_company()
        probe_ats = _make_probe_ats(probe_company.id)
        item_id = item.id

        q_item_mutable = MagicMock(spec=CompanyDiscoveryQueue)
        q_item_mutable.status = "pending"
        q_item_mutable.attempt_count = 0

        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()

            if call_count == 0:
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(
                    side_effect=[probe_company, probe_ats]
                )
                s.execute = AsyncMock(return_value=execute_result)
                s.get = AsyncMock(return_value=item)
            elif call_count == 1:
                # dispatch session
                s.get = AsyncMock(return_value=probe_ats)
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=probe_company)
                s.execute = AsyncMock(return_value=execute_result)
            else:
                # Step 3: update queue item
                s.get = AsyncMock(return_value=q_item_mutable)
                s.execute = AsyncMock()

            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch(
            "app.crawler.dispatcher.CrawlDispatcher",
            return_value=MagicMock(dispatch=AsyncMock(return_value=[])),
        ):
            result = await _process_discovery_item(item_id, mock_session_maker)

        assert result == "failed"
        assert q_item_mutable.attempt_count == 1

    @pytest.mark.asyncio
    async def test_rejection_at_3_attempts_deletes_probe_entities(self):
        """At >= 3 attempts with no jobs, status='rejected' and probe rows deleted."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item(attempt_count=2)  # this run is the 3rd
        probe_company = _make_probe_company()
        probe_ats = _make_probe_ats(probe_company.id)
        item_id = item.id

        q_item_mutable = MagicMock(spec=CompanyDiscoveryQueue)
        q_item_mutable.status = "pending"
        q_item_mutable.attempt_count = 2
        deleted_objects: list = []

        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()

            async def _delete(obj):
                deleted_objects.append(obj)

            s.delete = _delete

            if call_count == 0:
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(
                    side_effect=[probe_company, probe_ats]
                )
                s.execute = AsyncMock(return_value=execute_result)
                s.get = AsyncMock(return_value=item)
            elif call_count == 1:
                s.get = AsyncMock(return_value=probe_ats)
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=probe_company)
                s.execute = AsyncMock(return_value=execute_result)
            else:
                # Rejection session: get queue item, probe ats, probe company by slug
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=probe_company)
                s.execute = AsyncMock(return_value=execute_result)
                s.get = AsyncMock(side_effect=[q_item_mutable, probe_ats])

            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch(
            "app.crawler.dispatcher.CrawlDispatcher",
            return_value=MagicMock(dispatch=AsyncMock(return_value=[])),
        ):
            result = await _process_discovery_item(item_id, mock_session_maker)

        assert result == "rejected"
        assert q_item_mutable.status == "rejected"
        assert q_item_mutable.attempt_count == 3
        # Probe entities were passed to session.delete()
        assert len(deleted_objects) >= 1

    @pytest.mark.asyncio
    async def test_dispatcher_is_called_with_probe_ats_source_id(self):
        """CrawlDispatcher.dispatch() is called (not raw CRAWLER_MAP)."""
        from app.crawler.tasks import _process_discovery_item

        item = _make_queue_item()
        probe_company = _make_probe_company()
        probe_ats = _make_probe_ats(probe_company.id)
        item_id = item.id

        dispatched_ids: list[uuid.UUID] = []

        async def mock_dispatch(ats_source_id, db):
            dispatched_ids.append(ats_source_id)
            return []

        call_count = 0

        def session_factory():
            nonlocal call_count
            s = AsyncMock()
            if call_count == 0:
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(
                    side_effect=[probe_company, probe_ats]
                )
                s.execute = AsyncMock(return_value=execute_result)
                s.get = AsyncMock(return_value=item)
            elif call_count == 1:
                s.get = AsyncMock(return_value=probe_ats)
                execute_result = MagicMock()
                execute_result.scalar_one_or_none = MagicMock(return_value=probe_company)
                s.execute = AsyncMock(return_value=execute_result)
            else:
                q_item_m = MagicMock(spec=CompanyDiscoveryQueue)
                q_item_m.attempt_count = 0
                s.get = AsyncMock(return_value=q_item_m)
                s.execute = AsyncMock()
            call_count += 1
            s.__aenter__ = AsyncMock(return_value=s)
            s.__aexit__ = AsyncMock(return_value=None)
            return s

        mock_session_maker = MagicMock(side_effect=session_factory)

        with patch(
            "app.crawler.dispatcher.CrawlDispatcher",
            return_value=MagicMock(dispatch=AsyncMock(side_effect=mock_dispatch)),
        ):
            await _process_discovery_item(item_id, mock_session_maker)

        # Dispatcher must have been called with the probe AtsSource's ID
        assert len(dispatched_ids) == 1
        assert dispatched_ids[0] == probe_ats.id
