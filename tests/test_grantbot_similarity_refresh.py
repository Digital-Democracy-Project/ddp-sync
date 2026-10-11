"""The twice-yearly grantee-similarity refresh trigger and its on-demand endpoint (AGENTS-128).
Mirrors test_grantbot_scrape.py and test_trigger_grantbot_scrape_funders.py."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.pipelines.grantbot_scrape import (
    run_grantbot_scrape_job,
    run_grantbot_similarity_refresh_job,
)


@dataclass
class _FakeSettings:
    cams_base_url: str = "http://localhost:8000"
    cams_api_token: str = "test-token"


def _client_cm(post: AsyncMock) -> MagicMock:
    client = MagicMock()
    client.post = post
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _ok(body: dict) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = body
    return resp


def _status_error(code: int) -> httpx.HTTPStatusError:
    return httpx.HTTPStatusError(str(code), request=MagicMock(), response=MagicMock(status_code=code, text="x"))


def _patched(post: AsyncMock):
    return (
        patch("ddp_sync.pipelines.grantbot_scrape.get_settings", return_value=_FakeSettings()),
        patch("ddp_sync.pipelines.grantbot_scrape.httpx.AsyncClient", return_value=_client_cm(post)),
        patch("ddp_sync.pipelines.grantbot_scrape.asyncio.sleep", new=AsyncMock()),
    )


@pytest.mark.asyncio
async def test_it_posts_to_the_similarity_refresh_endpoint_with_the_bearer_token():
    post = AsyncMock(return_value=_ok({"status": "started", "detail": "running"}))
    a, b, c = _patched(post)
    with a, b, c:
        result = await run_grantbot_similarity_refresh_job(trigger="manual")
    assert result == {"success": True, "status": "started", "detail": "running"}
    assert post.await_args.args[0] == "http://localhost:8000/api/v1/admin/refresh-grantee-similarity"
    assert post.await_args.kwargs["headers"]["Authorization"] == "Bearer test-token"


@pytest.mark.asyncio
async def test_the_funder_scrape_job_still_posts_to_its_own_endpoint():
    post = AsyncMock(return_value=_ok({"status": "started"}))
    a, b, c = _patched(post)
    with a, b, c:
        await run_grantbot_scrape_job()
    assert post.await_args.args[0].endswith("/api/v1/admin/scrape-funders")


@pytest.mark.asyncio
async def test_a_409_means_a_refresh_is_already_running_and_is_not_an_error_or_retried():
    post = AsyncMock(return_value=MagicMock(raise_for_status=MagicMock(side_effect=_status_error(409))))
    a, b, c = _patched(post)
    with a, b, c:
        result = await run_grantbot_similarity_refresh_job()
    assert result == {"success": True, "status": "already_running"}
    assert post.await_count == 1


@pytest.mark.asyncio
async def test_other_4xx_is_an_error_and_not_retried_but_5xx_is_retried():
    post = AsyncMock(return_value=MagicMock(raise_for_status=MagicMock(side_effect=_status_error(401))))
    a, b, c = _patched(post)
    with a, b, c:
        result = await run_grantbot_similarity_refresh_job()
    assert result["error"] == "cams_error" and result["status_code"] == 401 and post.await_count == 1

    post = AsyncMock(side_effect=[
        MagicMock(raise_for_status=MagicMock(side_effect=_status_error(503))),
        _ok({"status": "started"}),
    ])
    a, b, c = _patched(post)
    with a, b, c:
        result = await run_grantbot_similarity_refresh_job()
    assert result["success"] is True and post.await_count == 2


@pytest.mark.asyncio
async def test_without_a_cams_token_it_does_nothing():
    with patch("ddp_sync.pipelines.grantbot_scrape.get_settings", return_value=_FakeSettings(cams_api_token="")), \
         patch("ddp_sync.pipelines.grantbot_scrape.httpx.AsyncClient") as cls:
        result = await run_grantbot_similarity_refresh_job()
    assert result == {"success": False, "error": "cams_not_configured"}
    cls.assert_not_called()


def _app() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return TestClient(app)


def test_the_trigger_endpoint_returns_the_result_and_runs_as_manual():
    with patch("ddp_sync.pipelines.grantbot_scrape.run_grantbot_similarity_refresh_job",
               new=AsyncMock(return_value={"success": True, "status": "already_running"})) as run:
        resp = _app().post("/trigger/grantbot-similarity-refresh")
    assert resp.status_code == 200 and resp.json()["status"] == "already_running"
    assert run.await_args.kwargs["trigger"] == "manual"


def test_the_trigger_endpoint_reports_a_missing_token_as_503_and_a_cams_failure_as_502():
    with patch("ddp_sync.pipelines.grantbot_scrape.run_grantbot_similarity_refresh_job",
               new=AsyncMock(return_value={"success": False, "error": "cams_not_configured"})):
        assert _app().post("/trigger/grantbot-similarity-refresh").status_code == 503
    with patch("ddp_sync.pipelines.grantbot_scrape.run_grantbot_similarity_refresh_job",
               new=AsyncMock(return_value={"success": False, "error": "cams_unreachable"})):
        assert _app().post("/trigger/grantbot-similarity-refresh").status_code == 502
