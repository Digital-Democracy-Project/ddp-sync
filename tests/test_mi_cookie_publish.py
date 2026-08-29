"""Tests for mi_cookie_publish.py (OPEN-188).

Mirrors test_openstates_archive_scrapebot_preseed.py's mocking style. The acceptance
question is not "does it call ScrapeBot" -- it is:

  * a missing SCRAPER_MEMORY_PREFIX refuses outright rather than publishing under an
    unnamespaced key (the same OPEN-159/172 discipline every other publisher in this
    project follows).
  * a mint failure or a publish failure is best-effort: logged, returned as a
    structured failure, never raised -- a bad tick must not crash the scheduler.
  * the S3 key this job publishes to matches scraper_memory_cache_key's own formula
    exactly, since cloud_collector.py's S3Memory.cache_key() and scraper-memory.sh's
    scraper_memory_cache_key() both have to agree on it independently.
"""

from __future__ import annotations

import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.pipelines.mi_cookie_publish import _publish_key, run_mi_cookie_publish_job
from ddp_sync.services.scrapebot_client import ScrapeBotDispatchError

_MINT_RESULT = {"cookies": [{"name": "x", "value": "y", "expires": 0}], "user_agent": "ua"}


def _completed_process(returncode=0, stderr=""):
    proc = MagicMock(spec=subprocess.CompletedProcess)
    proc.returncode = returncode
    proc.stderr = stderr
    return proc


def test_publish_key_matches_scraper_memory_cache_key_shape():
    assert _publish_key("prod") == "prod/mi/_cache/mi_waf_cookies.json"


@pytest.mark.asyncio
async def test_refuses_when_scraper_memory_prefix_is_not_set(monkeypatch):
    monkeypatch.delenv("SCRAPER_MEMORY_PREFIX", raising=False)
    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        result = await run_mi_cookie_publish_job()

    mock_dispatch.assert_not_awaited()
    assert result == {"success": False, "reason": "missing_scraper_memory_prefix"}


@pytest.mark.asyncio
async def test_publishes_on_a_successful_mint(monkeypatch):
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    monkeypatch.setenv("SCRAPER_MEMORY_S3_CMD", "fake-s3-wrapper")
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return _completed_process(returncode=0)

    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value=_MINT_RESULT,
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.write_cookie_cache"
    ) as mock_write, patch(
        "ddp_sync.pipelines.mi_cookie_publish.subprocess.run", side_effect=fake_run
    ):
        result = await run_mi_cookie_publish_job()

    mock_dispatch.assert_awaited_once_with("mi")
    mock_write.assert_called_once()
    assert captured["cmd"][0] == "fake-s3-wrapper"
    assert captured["cmd"][1] == "put"
    assert captured["cmd"][3] == "prod/mi/_cache/mi_waf_cookies.json"
    assert result == {"success": True, "key": "prod/mi/_cache/mi_waf_cookies.json"}


@pytest.mark.asyncio
async def test_never_raises_when_mint_fails(monkeypatch):
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        side_effect=ScrapeBotDispatchError("mint failed"),
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.subprocess.run"
    ) as mock_run:
        result = await run_mi_cookie_publish_job()

    mock_run.assert_not_called()
    assert result["success"] is False
    assert result["reason"] == "mint_failed"


@pytest.mark.asyncio
async def test_never_raises_when_publish_fails(monkeypatch):
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value=_MINT_RESULT,
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.write_cookie_cache"
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.subprocess.run",
        return_value=_completed_process(returncode=1, stderr="AccessDenied"),
    ):
        result = await run_mi_cookie_publish_job()

    assert result["success"] is False
    assert result["reason"] == "publish_failed"
