"""Tests for the ScrapeBot dispatch client (ddp-agents PLAN-scrapebot.md Phase 2)."""

from __future__ import annotations

import json
import time as time_module
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.services.scrapebot_client import (
    ScrapeBotDispatchError,
    cache_path_for,
    dispatch_mint_cookies,
    write_cookie_cache,
)


@dataclass
class _FakeSettings:
    cams_base_url: str = "http://localhost:8000"
    cams_api_token: str = "test-token"
    cams_artifacts_dir: str = ""


def _mock_client(*, statuses, task_id="mi-task-1"):
    """Build a mock httpx.AsyncClient whose GET calls return `statuses` in
    order, then keep returning the last one."""
    client = AsyncMock()
    post_response = MagicMock()
    post_response.json.return_value = {"task_id": task_id}
    post_response.raise_for_status.return_value = None
    client.post = AsyncMock(return_value=post_response)

    remaining = list(statuses)

    async def _get(*args, **kwargs):
        status = remaining.pop(0) if remaining else statuses[-1]
        resp = MagicMock()
        resp.json.return_value = {"status": status}
        resp.raise_for_status.return_value = None
        return resp

    client.get = AsyncMock(side_effect=_get)
    return client


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.scrapebot_client.httpx.AsyncClient", return_value=cm)


@pytest.mark.asyncio
async def test_missing_artifacts_dir_raises_immediately():
    with patch(
        "ddp_sync.services.scrapebot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=""),
    ):
        with pytest.raises(ScrapeBotDispatchError, match="CAMS_ARTIFACTS_DIR"):
            await dispatch_mint_cookies("mi")


@pytest.mark.asyncio
async def test_happy_path_returns_cookies_and_user_agent(tmp_path):
    task_id = "mi-task-1"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    cookies = [{"name": "x-bni-fpc", "value": "abc", "expires": 0}]
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"cookies": cookies, "user_agent": "Mozilla/5.0 fake-ua"})
    )

    mock_client = _mock_client(statuses=["queued", "running", "completed"], task_id=task_id)
    with patch(
        "ddp_sync.services.scrapebot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.scrapebot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        result = await dispatch_mint_cookies("mi")

    assert result == {"cookies": cookies, "user_agent": "Mozilla/5.0 fake-ua"}
    post_call = mock_client.post.await_args
    assert post_call.kwargs["json"]["bot"] == "scrapebot"
    assert post_call.kwargs["json"]["task_type"] == "mint_scrape_cookies"
    assert post_call.kwargs["json"]["payload"] == {"jurisdiction": "mi"}


@pytest.mark.asyncio
async def test_missing_cookies_in_result_raises(tmp_path):
    task_id = "mi-task-2"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"cookies": [], "user_agent": "Mozilla/5.0 fake-ua"})
    )

    mock_client = _mock_client(statuses=["queued", "completed"], task_id=task_id)
    with patch(
        "ddp_sync.services.scrapebot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.scrapebot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        with pytest.raises(ScrapeBotDispatchError, match="missing cookies/user_agent"):
            await dispatch_mint_cookies("mi")


@pytest.mark.asyncio
async def test_missing_user_agent_in_result_raises(tmp_path):
    task_id = "mi-task-3"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"cookies": [{"name": "x", "value": "y", "expires": 0}], "user_agent": ""})
    )

    mock_client = _mock_client(statuses=["queued", "completed"], task_id=task_id)
    with patch(
        "ddp_sync.services.scrapebot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.scrapebot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        with pytest.raises(ScrapeBotDispatchError, match="missing cookies/user_agent"):
            await dispatch_mint_cookies("mi")


@pytest.mark.asyncio
async def test_failed_task_raises(tmp_path):
    mock_client = _mock_client(statuses=["queued", "failed"])
    with patch(
        "ddp_sync.services.scrapebot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(tmp_path)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.scrapebot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        with pytest.raises(ScrapeBotDispatchError, match="status=failed"):
            await dispatch_mint_cookies("mi")


@pytest.mark.asyncio
async def test_timeout_raises_without_hanging_forever(tmp_path):
    mock_client = _mock_client(statuses=["queued", "running", "running", "running"])
    with patch(
        "ddp_sync.services.scrapebot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(tmp_path)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.scrapebot_client.asyncio.sleep", new_callable=AsyncMock
    ), patch(
        "ddp_sync.services.scrapebot_client.time.monotonic",
        side_effect=[0.0, 0.0, 200.0],  # exceeds the default 90s timeout on the 2nd poll check
    ):
        with pytest.raises(ScrapeBotDispatchError, match="did not finish within"):
            await dispatch_mint_cookies("mi")


def test_cache_path_for_matches_cookieprovider_layout():
    path = cache_path_for("mi", "/Users/agentsmith/Developer/repos/ddp-open-states")
    assert path == (
        "/Users/agentsmith/Developer/repos/ddp-open-states/openstates-scrapers/_cache/mi_waf_cookies.json"
    )


def test_write_cookie_cache_session_cookie_gets_ttl_fallback(tmp_path):
    cache_path = str(tmp_path / "mi_waf_cookies.json")
    before = time_module.time()
    write_cookie_cache(
        cache_path,
        cookies=[{"name": "x-bni-fpc", "value": "abc", "expires": 0}],
        user_agent="Mozilla/5.0 fake-ua",
    )
    data = json.loads(open(cache_path).read())
    assert data["_meta"]["user_agent"] == "Mozilla/5.0 fake-ua"
    # Session-scoped cookie (expires <= 0) falls back to CookieProvider's own
    # _DEFAULT_SESSION_COOKIE_TTL_SECONDS (3600) -- not a smaller/arbitrary guess.
    assert data["x-bni-fpc"]["expires"] > before + 3500
    assert data["x-bni-fpc"]["expires"] < before + 3700


def test_write_cookie_cache_duplicate_name_last_one_wins(tmp_path):
    cache_path = str(tmp_path / "mi_waf_cookies.json")
    write_cookie_cache(
        cache_path,
        cookies=[
            {"name": "x-bni-fpc", "value": "first", "expires": 1234567890},
            {"name": "x-bni-fpc", "value": "second", "expires": 1234567999},
        ],
        user_agent="Mozilla/5.0 fake-ua",
    )
    data = json.loads(open(cache_path).read())
    # Matches CookieProvider._warm_up_and_cache()'s own identical last-one-wins
    # behavior on a name collision -- not new dedup/disambiguation logic.
    assert data["x-bni-fpc"]["value"] == "second"
