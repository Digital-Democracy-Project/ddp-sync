"""The archive that follows a state's scrape (OPEN-291; SYNC-152 moved it into run_state_pipeline; SYNC-153 made it the
ONLY scheduled archive, with a retry, under the state's pipeline lock, and removed the 30-minute debounce).

Covers here: the usa -> us code mapping, the gates of `archive_with_retry` (archive disabled; the manifest's
`archive.enabled` is the one list, no yaml list, no fallback), `_run_scrape` itself never archiving, and that with the
debounce gone two archives in a row both run. The retry, the alert and the lock are in
tests/test_sync_153_archive_after_scrape_only.py. A tiny manifest in a temp dir stands in for ddp-open-states.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import yaml

from ddp_sync.pipelines.openstates_archive import (
    _archive_jurisdiction_code,
    _run_archive_with_hook,
    archive_with_retry,
)
from ddp_sync.pipelines.openstates_scrape import _run_scrape
from ddp_sync.services import jurisdiction_enrollment as je

ARCHIVE_CFG = {"enabled": True}


@pytest.fixture(autouse=True)
def manifest(tmp_path, monkeypatch):
    je._cache.clear()
    je._logged.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({
        "mi": {"status": "live", "archive": {"enabled": True}},
        "us": {"status": "live", "archive": {"enabled": True}},
        "al": {"status": "paused", "archive": {"enabled": False}},
    }))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    return tmp_path


# ── _archive_jurisdiction_code ──────────────────────────────────────────────────────


def test_usa_maps_to_us():
    assert _archive_jurisdiction_code("usa") == "us"


@pytest.mark.parametrize("jurisdiction", ["mi", "va", "ma", "ut", "az", "nc", "fl", "wa"])
def test_every_other_jurisdiction_is_unchanged(jurisdiction):
    assert _archive_jurisdiction_code(jurisdiction) == jurisdiction


# ── archive_with_retry's gates ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_disabled_archive_config_archives_nothing():
    with patch("ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock()) as run:
        assert await archive_with_retry("mi", {"enabled": False}) is None
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_state_whose_manifest_archive_is_off_is_not_archived_whatever_a_yaml_list_says():
    with patch("ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock()) as run:
        assert await archive_with_retry("al", {"enabled": True, "jurisdictions": ["al"]}) is None
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unreadable_manifest_archives_nothing_rather_than_guessing(manifest, monkeypatch):
    monkeypatch.setenv("OPENSTATES_ROOT", str(manifest / "nowhere"))
    with patch("ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock()) as run:
        assert await archive_with_retry("mi", ARCHIVE_CFG) is None
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_enrolled_state_is_archived():
    with patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock(return_value={"success": True})
    ) as run:
        assert await archive_with_retry("mi", ARCHIVE_CFG) is True
    run.assert_awaited_once_with("mi", ARCHIVE_CFG)


@pytest.mark.asyncio
async def test_usa_scrape_archives_us():
    with patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock(return_value={"success": True})
    ) as run:
        assert await archive_with_retry("usa", ARCHIVE_CFG) is True
    run.assert_awaited_once_with("us", ARCHIVE_CFG)


@pytest.mark.asyncio
async def test_no_config_loads_the_file():
    with patch(
        "ddp_sync.pipelines.openstates_archive._load_archive_config_for_scrape_trigger", return_value={}
    ) as load, patch("ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock()) as run:
        assert await archive_with_retry("mi") is None  # the loader's failure return ({}) means "disabled"
    load.assert_called_once()
    run.assert_not_awaited()


# ── _run_scrape no longer archives (SYNC-152: run_state_pipeline archives once, after every session) ──


@pytest.mark.asyncio
async def test_run_scrape_itself_never_archives():
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        new=AsyncMock(return_value={"success": True, "jurisdiction": "mi"}),
    ), patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=AsyncMock()) as mock_archive:
        result = await _run_scrape("mi", None, "/fake/root")

    assert result["success"] is True
    mock_archive.assert_not_awaited()


# ── no debounce (SYNC-153): the state's lock is what stops overlap, so a retry is never skipped ──


@pytest.mark.asyncio
async def test_two_archives_in_a_row_both_run():
    with patch(
        "ddp_sync.pipelines.openstates_archive._run_archive",
        new=AsyncMock(return_value={"success": True, "jurisdiction": "mi"}),
    ) as mock_run_archive, patch(
        "ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive", new=AsyncMock(),
    ), patch("ddp_sync.pipelines.openstates_archive._maybe_refresh_bill_search", new=AsyncMock()), patch(
        "ddp_sync.pipelines.openstates_archive._maybe_embed_knowledge_base", new=AsyncMock()
    ):
        first = await _run_archive_with_hook("mi", "/fake/root", config={})
        second = await _run_archive_with_hook("mi", "/fake/root", config={})

    assert first["success"] is True and second["success"] is True
    assert "skipped" not in second
    assert mock_run_archive.await_count == 2
