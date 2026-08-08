"""Tests for openstates_archive.py's ScrapeBot cookie pre-seed (added 2026-08-08).

Mirrors tests/test_openstates_scrape_scrapebot_fallback.py: the archive pipeline
gets the same proactive pre-seed the scrape pipeline has had since 2026-08-04,
which it never received when archiving was split out of run-scrape.sh on
2026-07-31. One structural difference: the archive pipeline's config IS the
openstates_archive: subtree of sync_schedule.yaml, so scrapebot_fallback sits at
its top level -- there is no "secondary" nesting here.

Same guarantees as the scrape version: opt-in per jurisdiction (a jurisdiction
not listed, or the feature disabled/absent, must see zero behavior change), and
best-effort (a dispatch failure must never propagate into the archive job's own
result).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.pipelines.openstates_archive import (
    _maybe_preseed_scrapebot_cookies,
    _run_archive,
    _scrapebot_eligible,
    run_single_archive_job,
)
from ddp_sync.services.scrapebot_client import ScrapeBotDispatchError


def _config(*, enabled=True, jurisdictions=("mi",)):
    return {
        "scrapebot_fallback": {"enabled": enabled, "jurisdictions": list(jurisdictions)}
    }


def _completed_process():
    proc = MagicMock()
    proc.returncode = 0
    proc.stdout = b""
    proc.stderr = b""
    return proc


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
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value={
            "cookies": [{"name": "x", "value": "y", "expires": 0}],
            "user_agent": "ua",
        },
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.write_cookie_cache"
    ) as mock_write, patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.cache_path_for",
        return_value="/fake/_cache/mi_waf_cookies.json",
    ):
        await _maybe_preseed_scrapebot_cookies("mi", config, "/fake/root")

    mock_dispatch.assert_awaited_once_with("mi")
    mock_write.assert_called_once()


@pytest.mark.asyncio
async def test_preseed_skips_jurisdiction_not_opted_in():
    config = _config(jurisdictions=("va",))  # mi not opted in
    with patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        await _maybe_preseed_scrapebot_cookies("mi", config, "/fake/root")

    mock_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_preseed_skips_when_config_missing_entirely():
    with patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        await _maybe_preseed_scrapebot_cookies("mi", None, "/fake/root")

    mock_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_preseed_never_raises_when_dispatch_fails():
    config = _config()
    with patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        side_effect=ScrapeBotDispatchError("mint failed"),
    ):
        # Must not raise -- this is a best-effort pre-seed, never something the
        # archive run itself should depend on.
        await _maybe_preseed_scrapebot_cookies("mi", config, "/fake/root")


@pytest.mark.asyncio
async def test_run_archive_preseeds_before_launching_the_subprocess():
    """The pre-seed must happen before run-archive.sh is even invoked -- start the
    run with fresh cookies already in the cache file, don't react to a failure
    afterward."""
    config = _config()
    call_order = []

    async def fake_preseed(jurisdiction, cfg, root):
        call_order.append("preseed")

    def fake_subprocess_run(*args, **kwargs):
        call_order.append("subprocess")
        return _completed_process()

    with patch(
        "ddp_sync.pipelines.openstates_archive._maybe_preseed_scrapebot_cookies",
        side_effect=fake_preseed,
    ), patch(
        "ddp_sync.pipelines.openstates_archive.subprocess.run",
        side_effect=fake_subprocess_run,
    ):
        result = await _run_archive("mi", "/fake/root", config=config)

    assert call_order == ["preseed", "subprocess"]
    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_archive_is_a_noop_preseed_when_config_omitted():
    """Every existing _run_archive caller that doesn't pass config (or passes a
    config where this jurisdiction isn't opted in) must see zero behavior change."""
    with patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.openstates_archive.subprocess.run",
        return_value=_completed_process(),
    ):
        result = await _run_archive("fl", "/fake/root")

    mock_dispatch.assert_not_awaited()
    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_single_archive_job_preseeds_via_run_archive():
    """run_single_archive_job() (the manual /trigger path) gets the pre-seed for
    free via _run_archive() itself, as long as it passes config through."""
    config = _config()
    with patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value={
            "cookies": [{"name": "x", "value": "y", "expires": 0}],
            "user_agent": "ua",
        },
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.write_cookie_cache"
    ), patch(
        "ddp_sync.pipelines.openstates_archive.scrapebot_client.cache_path_for",
        return_value="/fake/_cache/mi_waf_cookies.json",
    ), patch(
        "ddp_sync.pipelines.openstates_archive.subprocess.run",
        return_value=_completed_process(),
    ):
        result = await run_single_archive_job("mi", config)

    mock_dispatch.assert_awaited_once_with("mi")
    assert result["success"] is True
