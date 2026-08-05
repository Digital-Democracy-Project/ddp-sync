"""Tests for openstates_scrape.py's ScrapeBot cookie pre-seed (PLAN-scrapebot.md §3.7).

Revised 2026-08-05: pre-seeding is now proactive (mint before every scrape attempt
for an opted-in jurisdiction), not reactive (mint only after a waf_block-classified
failure). The reactive design never actually fired against a real production run --
classify_failure_reason() can only see run-scrape.sh's own external stdout/stderr,
but the detailed WafBlockDetected error only ever reaches scraper.log, so a real MI
WAF block always classified as nonzero_exit_other, never waf_block.

_scrapebot_eligible/_maybe_preseed_scrapebot_cookies are opt-in-per-jurisdiction and
best-effort: a jurisdiction not listed in secondary.scrapebot_fallback.jurisdictions
(or the feature disabled entirely) must see zero behavior change, and a dispatch
failure must never propagate up into the scrape job's own result.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    _maybe_preseed_scrapebot_cookies,
    _run_scrape,
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
async def test_preseed_dispatches_for_opted_in_jurisdiction():
    config = _config()
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
        await _maybe_preseed_scrapebot_cookies("mi", config, "/fake/root")

    mock_dispatch.assert_awaited_once_with("mi")
    mock_write.assert_called_once()


@pytest.mark.asyncio
async def test_preseed_skips_jurisdiction_not_opted_in():
    config = _config(jurisdictions=("va",))  # mi not opted in
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        await _maybe_preseed_scrapebot_cookies("mi", config, "/fake/root")

    mock_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_preseed_skips_when_config_missing_entirely():
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        await _maybe_preseed_scrapebot_cookies("mi", None, "/fake/root")

    mock_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_preseed_never_raises_when_dispatch_fails():
    config = _config()
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        side_effect=ScrapeBotDispatchError("mint failed"),
    ):
        # Must not raise -- this is a best-effort pre-seed (PLAN §3.7), never
        # something the caller's own scrape attempt should depend on.
        await _maybe_preseed_scrapebot_cookies("mi", config, "/fake/root")


@pytest.mark.asyncio
async def test_run_scrape_preseeds_before_launching_the_subprocess():
    """The pre-seed must happen before run-scrape.sh is even invoked -- that's the
    whole point (start every attempt with fresh cookies already in place, not react
    to a failure afterward)."""
    config = _config()
    call_order = []

    async def fake_preseed(jurisdiction, cfg, root):
        call_order.append("preseed")

    def fake_run_with_group_kill(cmd, env, timeout):
        call_order.append("subprocess")
        return 0, b"", b"", False

    with patch(
        "ddp_sync.pipelines.openstates_scrape._maybe_preseed_scrapebot_cookies",
        side_effect=fake_preseed,
    ), patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        side_effect=fake_run_with_group_kill,
    ):
        result = await _run_scrape("mi", None, "/fake/root", config=config)

    assert call_order == ["preseed", "subprocess"]
    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_scrape_is_a_noop_preseed_when_config_omitted():
    """Every existing _run_scrape caller that doesn't pass config (or passes a
    config where this jurisdiction isn't opted in) must see zero behavior change."""
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        return_value=(0, b"", b"", False),
    ):
        result = await _run_scrape("fl", "session=2026", "/fake/root")

    mock_dispatch.assert_not_awaited()
    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_single_scrape_job_preseeds_via_run_scrape():
    """run_single_scrape_job() (the manual /trigger/openstates-scrape/mi path) used
    to have no ScrapeBot involvement at all. It gets the pre-seed for free now,
    via _run_scrape() itself, as long as it passes config through."""
    config = _config()
    with patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value={
            "cookies": [{"name": "x", "value": "y", "expires": 0}],
            "user_agent": "ua",
        },
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.write_cookie_cache"
    ), patch(
        "ddp_sync.pipelines.openstates_scrape.scrapebot_client.cache_path_for",
        return_value="/fake/_cache/mi_waf_cookies.json",
    ), patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        return_value=(0, b"", b"", False),
    ):
        result = await run_single_scrape_job("mi", config)

    mock_dispatch.assert_awaited_once_with("mi")
    assert result["success"] is True
