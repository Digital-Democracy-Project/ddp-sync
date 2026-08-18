"""Tests for scripts/backfill_bill_versions.py (SYNC-26).

This script writes directly to ddp-broker-py's BillVersion ledger, unlike
this repo's other one-off scripts (e.g. backfill_legislator_party.py, which
only PATCHes Webflow CMS fields if they differ) -- covered with real tests
rather than left as an untested standalone tool, given the higher stakes of
a computed-diff write to a production-critical ledger table.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

pytestmark = pytest.mark.asyncio

_SCRIPT_PATH = Path(__file__).parent.parent / "scripts" / "backfill_bill_versions.py"


def _load_script():
    """scripts/ isn't a package -- import backfill_bill_versions.py directly
    by path, matching how it's actually invoked (as a standalone script)."""
    spec = importlib.util.spec_from_file_location("backfill_bill_versions", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["backfill_bill_versions"] = module
    spec.loader.exec_module(module)
    return module


backfill_bill_versions = _load_script()


_CANDIDATES = [
    {"gov_id": "HB 1", "bill_openstates_id": "id-1", "session_code": "2026E", "live_url_fallback": ""},
    {"gov_id": "HB 2", "bill_openstates_id": "id-2", "session_code": "2026E", "live_url_fallback": ""},
]


def _patch_candidates(candidates):
    return patch(
        "ddp_sync.services.local_openstates_client.list_current_session_bill_candidates",
        new=AsyncMock(return_value=candidates),
    )


def _patch_versions(versions_by_id):
    async def _get(bill_openstates_id):
        return versions_by_id.get(bill_openstates_id)

    return patch(
        "ddp_sync.services.local_openstates_client.get_all_versions",
        new=AsyncMock(side_effect=_get),
    )


async def test_dry_run_lists_every_version_of_every_multi_version_bill_without_writing():
    mock_write = AsyncMock()
    versions_by_id = {
        "id-1": [
            {"date": "2026-08-01", "note": "Filed", "links": []},
            {"date": "2026-08-10", "note": "er", "links": []},
        ],
        "id-2": [{"date": "2026-08-01", "note": "Filed", "links": []}],  # single version
    }

    with (
        _patch_candidates(_CANDIDATES),
        _patch_versions(versions_by_id),
        patch("ddp_sync.services.broker_client.write_bill_version", new=mock_write),
    ):
        result = await backfill_bill_versions.run(
            jurisdiction="FL", session="2026E", dry_run=True, limit=None
        )

    mock_write.assert_not_called()
    assert len(result.attempted) == 2  # HB 1's Filed + er
    assert {n for _, _, n in result.attempted} == {"Filed", "er"}
    assert result.skipped_single_version == ["HB 2"]


async def test_write_mode_splits_created_vs_already_present():
    async def _write_side_effect(**kwargs):
        if kwargs["version_note"] == "Filed":
            return {"id": 1, "created": False}  # already recorded
        return {"id": 2, "created": True}

    mock_write = AsyncMock(side_effect=_write_side_effect)
    versions_by_id = {
        "id-1": [
            {"date": "2026-08-01", "note": "Filed", "links": []},
            {"date": "2026-08-10", "note": "er", "links": [{"url": "https://example.gov/er.pdf", "media_type": "application/pdf"}]},
        ],
    }

    with (
        _patch_candidates([_CANDIDATES[0]]),
        _patch_versions(versions_by_id),
        patch("ddp_sync.services.broker_client.write_bill_version", new=mock_write),
    ):
        result = await backfill_bill_versions.run(
            jurisdiction="FL", session="2026E", dry_run=False, limit=None
        )

    assert mock_write.await_count == 2
    assert len(result.created) == 1
    assert result.created[0][2] == "er"
    assert result.already_present == 1
    # text_url/media_type passed through for the version that has a link.
    er_call = next(c for c in mock_write.await_args_list if c.kwargs["version_note"] == "er")
    assert er_call.kwargs["text_url"] == "https://example.gov/er.pdf"
    assert er_call.kwargs["media_type"] == "application/pdf"


async def test_bill_with_no_versions_in_api_v3_is_skipped_not_failed():
    mock_write = AsyncMock()
    versions_by_id = {"id-1": None, "id-2": None}

    with (
        _patch_candidates(_CANDIDATES),
        _patch_versions(versions_by_id),
        patch("ddp_sync.services.broker_client.write_bill_version", new=mock_write),
    ):
        result = await backfill_bill_versions.run(
            jurisdiction="FL", session="2026E", dry_run=True, limit=None
        )

    mock_write.assert_not_called()
    assert set(result.skipped_no_versions) == {"HB 1", "HB 2"}
    assert result.failed == []


async def test_write_failure_for_one_bill_does_not_stop_the_others():
    async def _write_side_effect(**kwargs):
        if kwargs["bill_openstates_id"] == "id-1":
            from ddp_sync.services.broker_client import BrokerClientError
            raise BrokerClientError("ddp-broker-py rejected the write")
        return {"id": 1, "created": True}

    mock_write = AsyncMock(side_effect=_write_side_effect)
    versions_by_id = {
        "id-1": [
            {"date": "2026-08-01", "note": "Filed", "links": []},
            {"date": "2026-08-10", "note": "er", "links": []},
        ],
        "id-2": [
            {"date": "2026-08-01", "note": "Filed", "links": []},
            {"date": "2026-08-10", "note": "er", "links": []},
        ],
    }

    with (
        _patch_candidates(_CANDIDATES),
        _patch_versions(versions_by_id),
        patch("ddp_sync.services.broker_client.write_bill_version", new=mock_write),
    ):
        result = await backfill_bill_versions.run(
            jurisdiction="FL", session="2026E", dry_run=False, limit=None
        )

    assert len(result.failed) == 2  # both of HB 1's versions failed
    assert all(gov_id == "HB 1" for gov_id, _ in result.failed)
    # HB 2 still got both its versions attempted despite HB 1's failures.
    assert len(result.created) == 2
    assert all(gov_id == "HB 2" for gov_id, _, _ in result.created)


async def test_limit_defaults_to_500_and_is_passed_through():
    mock_candidates = AsyncMock(return_value=[])

    with (
        patch("ddp_sync.services.local_openstates_client.list_current_session_bill_candidates", new=mock_candidates),
    ):
        await backfill_bill_versions.run(jurisdiction="FL", session="2026E", dry_run=True, limit=None)

    assert mock_candidates.await_args.kwargs["limit"] == 500

    mock_candidates.reset_mock()
    with (
        patch("ddp_sync.services.local_openstates_client.list_current_session_bill_candidates", new=mock_candidates),
    ):
        await backfill_bill_versions.run(jurisdiction="FL", session="2026E", dry_run=True, limit=2000)

    assert mock_candidates.await_args.kwargs["limit"] == 2000
