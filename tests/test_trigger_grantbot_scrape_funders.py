"""Tests for the /trigger/grantbot-scrape-funders endpoint (SYNC-36).

Mirrors test_trigger_vote_person_backfill.py's TestClient shape.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router


def _make_app():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return app


def test_success_returns_200_with_the_underlying_result():
    app = _make_app()
    client = TestClient(app)
    with patch(
        "ddp_sync.pipelines.grantbot_scrape.run_grantbot_scrape_job",
        new=AsyncMock(return_value={"success": True, "status": "started"}),
    ) as mock_run:
        resp = client.post("/trigger/grantbot-scrape-funders")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"success": True, "status": "started"}
    assert mock_run.await_args.kwargs["trigger"] == "manual"


def test_cams_not_configured_returns_503():
    app = _make_app()
    client = TestClient(app)
    with patch(
        "ddp_sync.pipelines.grantbot_scrape.run_grantbot_scrape_job",
        new=AsyncMock(return_value={"success": False, "error": "cams_not_configured"}),
    ):
        resp = client.post("/trigger/grantbot-scrape-funders")

    assert resp.status_code == 503


def test_cams_error_returns_502():
    app = _make_app()
    client = TestClient(app)
    with patch(
        "ddp_sync.pipelines.grantbot_scrape.run_grantbot_scrape_job",
        new=AsyncMock(
            return_value={"success": False, "error": "cams_error", "status_code": 503}
        ),
    ):
        resp = client.post("/trigger/grantbot-scrape-funders")

    assert resp.status_code == 502


def test_cams_unreachable_returns_502():
    app = _make_app()
    client = TestClient(app)
    with patch(
        "ddp_sync.pipelines.grantbot_scrape.run_grantbot_scrape_job",
        new=AsyncMock(
            return_value={"success": False, "error": "cams_unreachable", "detail": "x"}
        ),
    ):
        resp = client.post("/trigger/grantbot-scrape-funders")

    assert resp.status_code == 502


def test_requires_api_key_auth():
    """Proves the dependency is actually wired to the route -- a route that
    forgot to declare `token: str = Depends(api_key_auth)` would succeed here
    the same way it does in the tests above, which override it."""
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    resp = client.post("/trigger/grantbot-scrape-funders")

    assert resp.status_code == 401
