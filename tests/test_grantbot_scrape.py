"""Tests for pipelines/grantbot_scrape.py (SYNC-36) -- the monthly trigger
that replaces CAMS's own removed internal cron (AGENTS-54) for GrantBot's
funder-scrape + Kindora enrichment job.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.pipelines.grantbot_scrape import run_grantbot_scrape_job


@dataclass
class _FakeSettings:
    cams_base_url: str = "http://localhost:8000"
    cams_api_token: str = "test-token"


def _mock_client(mock_post: AsyncMock) -> MagicMock:
    mock_client = MagicMock()
    mock_client.post = mock_post
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


@pytest.mark.asyncio
async def test_cams_not_configured_returns_error_without_calling_http():
    with patch(
        "ddp_sync.pipelines.grantbot_scrape.get_settings",
        return_value=_FakeSettings(cams_api_token=""),
    ), patch("ddp_sync.pipelines.grantbot_scrape.httpx.AsyncClient") as mock_cls:
        result = await run_grantbot_scrape_job()

    assert result == {"success": False, "error": "cams_not_configured"}
    mock_cls.assert_not_called()


@pytest.mark.asyncio
async def test_success_posts_to_admin_scrape_funders_with_bearer_token():
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"status": "started", "detail": "running"}
    mock_post = AsyncMock(return_value=mock_response)
    cm = _mock_client(mock_post)

    with patch(
        "ddp_sync.pipelines.grantbot_scrape.get_settings",
        return_value=_FakeSettings(),
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.httpx.AsyncClient", return_value=cm,
    ):
        result = await run_grantbot_scrape_job(trigger="manual")

    assert result == {"success": True, "status": "started", "detail": "running"}
    call = mock_post.await_args
    assert call.args[0] == "http://localhost:8000/api/v1/admin/scrape-funders"
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"


def _http_status_error(status_code: int, text: str = "error") -> httpx.HTTPStatusError:
    mock_error_response = MagicMock(status_code=status_code, text=text)
    return httpx.HTTPStatusError(str(status_code), request=MagicMock(), response=mock_error_response)


@pytest.mark.asyncio
async def test_4xx_error_returns_immediately_without_retrying():
    """A 4xx (e.g. an auth misconfiguration) would fail identically on
    every attempt, so it must not be retried."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock(
        side_effect=_http_status_error(401, "Invalid token")
    )
    mock_post = AsyncMock(return_value=mock_response)
    cm = _mock_client(mock_post)

    with patch(
        "ddp_sync.pipelines.grantbot_scrape.get_settings",
        return_value=_FakeSettings(),
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.httpx.AsyncClient", return_value=cm,
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.asyncio.sleep", new=AsyncMock(),
    ) as mock_sleep:
        result = await run_grantbot_scrape_job()

    assert result == {"success": False, "error": "cams_error", "status_code": 401}
    assert mock_post.await_count == 1
    mock_sleep.assert_not_called()


@pytest.mark.asyncio
async def test_5xx_error_retries_then_fails_after_max_attempts():
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock(
        side_effect=_http_status_error(503, "Notion scraper unavailable")
    )
    mock_post = AsyncMock(return_value=mock_response)
    cm = _mock_client(mock_post)

    with patch(
        "ddp_sync.pipelines.grantbot_scrape.get_settings",
        return_value=_FakeSettings(),
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.httpx.AsyncClient", return_value=cm,
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.asyncio.sleep", new=AsyncMock(),
    ) as mock_sleep:
        result = await run_grantbot_scrape_job()

    assert result == {"success": False, "error": "cams_error", "status_code": 503}
    assert mock_post.await_count == 3
    assert mock_sleep.await_count == 2


@pytest.mark.asyncio
async def test_request_error_retries_then_succeeds():
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"status": "started"}
    mock_post = AsyncMock(
        side_effect=[
            httpx.ConnectError("connection refused"),
            mock_response,
        ]
    )
    cm = _mock_client(mock_post)

    with patch(
        "ddp_sync.pipelines.grantbot_scrape.get_settings",
        return_value=_FakeSettings(),
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.httpx.AsyncClient", return_value=cm,
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.asyncio.sleep", new=AsyncMock(),
    ) as mock_sleep:
        result = await run_grantbot_scrape_job()

    assert result == {"success": True, "status": "started"}
    assert mock_post.await_count == 2
    assert mock_sleep.await_count == 1


@pytest.mark.asyncio
async def test_request_error_all_attempts_fail_returns_cams_unreachable():
    mock_post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
    cm = _mock_client(mock_post)

    with patch(
        "ddp_sync.pipelines.grantbot_scrape.get_settings",
        return_value=_FakeSettings(),
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.httpx.AsyncClient", return_value=cm,
    ), patch(
        "ddp_sync.pipelines.grantbot_scrape.asyncio.sleep", new=AsyncMock(),
    ):
        result = await run_grantbot_scrape_job()

    assert result["success"] is False
    assert result["error"] == "cams_unreachable"
    assert mock_post.await_count == 3
