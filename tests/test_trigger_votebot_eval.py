"""HTTP-level tests for POST /trigger/votebot-eval (plan §3.4).

The endpoint is intentionally thin — input validation + return-code
translation only. The underlying logic lives in ``run_votebot_eval`` and
has its own unit coverage in test_votebot_eval.py. These tests pin the
HTTP contract: 200 / 400 / 409 / 503 status codes per the plan's
``Status codes`` doc block.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router


def _make_app(yaml_config: dict | None = None):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return app, yaml_config


def _patch_scheduler(yaml_config: dict | None):
    """Helper — patch get_scheduler() to return a stub with _sync_config set."""
    fake_scheduler = MagicMock()
    fake_scheduler._sync_config = {
        "votebot_eval": yaml_config
    } if yaml_config is not None else {}
    return patch(
        "ddp_sync.scheduler.get_scheduler", return_value=fake_scheduler
    )


def test_days_below_minimum_returns_400():
    app, _ = _make_app({"max_days": 30})
    client = TestClient(app)
    with _patch_scheduler({"max_days": 30}):
        response = client.post("/trigger/votebot-eval?days=0")
    assert response.status_code == 400
    assert "days must be in" in response.json()["detail"]


def test_days_above_max_returns_400():
    """Plan §3.4 — days > max_days returns 400 (cheap guardrail against
    accidental --days=365)."""
    app, _ = _make_app({"max_days": 30})
    client = TestClient(app)
    with _patch_scheduler({"max_days": 30}):
        response = client.post("/trigger/votebot-eval?days=365")
    assert response.status_code == 400
    assert "days must be in" in response.json()["detail"]


def test_lock_held_returns_409_with_current_run_id():
    """Plan §3.4 — when ``run_votebot_eval`` returns ``error="already_running"``,
    the endpoint translates to 409 + current_run_id."""
    app, _ = _make_app({"max_days": 30})
    client = TestClient(app)

    async def fake_run(**kwargs):
        return {
            "success": False,
            "error": "already_running",
            "current_run_id": "abc123",
        }

    with _patch_scheduler({"max_days": 30}), patch(
        "ddp_sync.pipelines.votebot_eval.run_votebot_eval",
        side_effect=fake_run,
    ):
        response = client.post("/trigger/votebot-eval?days=7")

    assert response.status_code == 409
    body = response.json()["detail"]
    assert body["error"] == "already_running"
    assert body["current_run_id"] == "abc123"


def test_path_invalid_returns_503():
    """Plan §3.4 — live path validation. votebot_path going stale at run time
    must surface as 503 (Service Unavailable) so operators can distinguish
    config issues from in-flight runs."""
    app, _ = _make_app({"max_days": 30})
    client = TestClient(app)

    async def fake_run(**kwargs):
        return {
            "success": False,
            "error": "votebot_path_invalid",
            "detail": "venv python not found",
        }

    with _patch_scheduler({"max_days": 30}), patch(
        "ddp_sync.pipelines.votebot_eval.run_votebot_eval",
        side_effect=fake_run,
    ):
        response = client.post("/trigger/votebot-eval?days=7")

    assert response.status_code == 503
    body = response.json()["detail"]
    assert body["error"] == "votebot_path_invalid"
    assert "venv" in body["message"]


def test_success_returns_200_with_headline():
    """Happy path — successful run returns 200 with the headline + regressions."""
    app, _ = _make_app({"max_days": 30})
    client = TestClient(app)

    async def fake_run(**kwargs):
        return {
            "success": True,
            "run_id": "run-1",
            "headline": {
                "n_query_processed": 100,
                "citation_rate": 0.27,
                "pass_rate": 0.5,
                "bill_history_leak_count": 0,
            },
            "regressions": [],
            "report_path": "/home/ubuntu/votebot/eval_reports/x.json",
            "duration_seconds": 90.0,
        }

    with _patch_scheduler({"max_days": 30}), patch(
        "ddp_sync.pipelines.votebot_eval.run_votebot_eval",
        side_effect=fake_run,
    ):
        response = client.post("/trigger/votebot-eval?days=7")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["headline"]["citation_rate"] == 0.27


def test_other_failure_returns_500():
    """Subprocess timeouts, parse errors, etc. → 500 with the result detail."""
    app, _ = _make_app({"max_days": 30})
    client = TestClient(app)

    async def fake_run(**kwargs):
        return {"success": False, "error": "subprocess_timeout"}

    with _patch_scheduler({"max_days": 30}), patch(
        "ddp_sync.pipelines.votebot_eval.run_votebot_eval",
        side_effect=fake_run,
    ):
        response = client.post("/trigger/votebot-eval?days=7")

    assert response.status_code == 500


def test_max_days_default_when_no_config():
    """If no votebot_eval YAML block exists, max_days falls back to default 30
    so the endpoint still validates days correctly."""
    app, _ = _make_app(None)
    client = TestClient(app)

    with _patch_scheduler(None):
        # 31 should be over the default cap of 30
        response = client.post("/trigger/votebot-eval?days=31")

    assert response.status_code == 400
