"""Tests for openstates_scrape.py's ScrapeBot cookie-mint fallback (PLAN-scrapebot.md §3.7).

_scrapebot_eligible/_maybe_seed_scrapebot_cookies are opt-in-per-jurisdiction and
best-effort: a jurisdiction not listed in secondary.scrapebot_fallback.jurisdictions
(or the feature disabled entirely) must see zero behavior change, and a dispatch
failure must never propagate up into the scrape job's own result.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    _maybe_seed_scrapebot_cookies,
    _scrapebot_eligible,
    run_single_scrape_job,
)
from ddp_sync.services.scrapebot_client import ScrapeBotDispatchError


def _config(*, enabled=True, jurisdictions=("mi",)):
    return {
        "secondary": {
            "scrapebot_fallback": {"enabled": enabled, "jurisdictions": list(jurisdictions)}
        }
    }


def test_eligible_true_when_enabled_and_listed():
    assert _scrapebot_eligible("mi", _config()) is True


def test_eligible_false_when_disabled():
    assert _scrapebot_eligible("mi", _config(enabled=False)) is False


def test_eligible_false_when_jurisdiction_not_listed():
    assert _scrapebot_eligible("va", _config()) is False


def test_eligible_false_when_config_missing_entirely():
    assert _scrapebot_eligible("mi", None) is False
    assert _scrapebot_eligible("mi", {}) is False


@pytest.mark.asyncio
async def test_seed_only_fires_for_waf_block_failures():
    jurisdictions = ["mi", "va"]
    results = [
        {"success": False, "failure_reason": "waf_block"},
        {"success": False, "failure_reason": "timeout"},
    ]
    config = _config(jurisdictions=("mi", "va"))
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value={
            "cookies": [{"name": "x", "value": "y", "expires": 0}],
            "user_agent": "ua",
        },
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.write_cookie_cache"
    ) as mock_write, patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.cache_path_for",
        return_value="/fake/_cache/mi_waf_cookies.json",
    ):
        await _maybe_seed_scrapebot_cookies(jurisdictions, results, config, "/fake/root")

    # va failed with "timeout", not "waf_block" -- only mi should dispatch.
    mock_dispatch.assert_awaited_once_with("mi")
    mock_write.assert_called_once()


@pytest.mark.asyncio
async def test_seed_skips_jurisdiction_not_opted_in():
    jurisdictions = ["mi"]
    results = [{"success": False, "failure_reason": "waf_block"}]
    config = _config(jurisdictions=("va",))  # mi not opted in
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        await _maybe_seed_scrapebot_cookies(jurisdictions, results, config, "/fake/root")

    mock_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_skips_successful_runs():
    jurisdictions = ["mi"]
    results = [{"success": True}]
    config = _config()
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        await _maybe_seed_scrapebot_cookies(jurisdictions, results, config, "/fake/root")

    mock_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_never_raises_when_dispatch_fails():
    jurisdictions = ["mi"]
    results = [{"success": False, "failure_reason": "waf_block"}]
    config = _config()
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        side_effect=ScrapeBotDispatchError("mint failed"),
    ):
        # Must not raise -- this is a best-effort cache seed (PLAN §3.7), never
        # something the caller's own job outcome should depend on.
        await _maybe_seed_scrapebot_cookies(jurisdictions, results, config, "/fake/root")


@pytest.mark.asyncio
async def test_run_single_scrape_job_seeds_scrapebot_on_waf_block():
    """run_single_scrape_job() (the manual /trigger/openstates-scrape/mi path) used
    to have no ScrapeBot fallback at all -- only run_secondary_scrapes_job() did.
    A jurisdiction triggered standalone must get the same fallback."""
    config = _config()
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape",
        new_callable=AsyncMock,
        return_value={"success": False, "failure_reason": "waf_block"},
    ), patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value={
            "cookies": [{"name": "x", "value": "y", "expires": 0}],
            "user_agent": "ua",
        },
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.write_cookie_cache"
    ) as mock_write, patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.cache_path_for",
        return_value="/fake/_cache/mi_waf_cookies.json",
    ):
        result = await run_single_scrape_job("mi", config)

    mock_dispatch.assert_awaited_once_with("mi")
    mock_write.assert_called_once()
    assert result["success"] is False  # the seed call doesn't change the job's own outcome


@pytest.mark.asyncio
async def test_run_single_scrape_job_skips_scrapebot_when_not_opted_in():
    config = _config(jurisdictions=("va",))  # mi not opted in
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape",
        new_callable=AsyncMock,
        return_value={"success": False, "failure_reason": "waf_block"},
    ), patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        await run_single_scrape_job("mi", config)

    mock_dispatch.assert_not_awaited()
