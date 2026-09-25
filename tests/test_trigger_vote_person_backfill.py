"""Tests for the /trigger/vote-person-backfill and /trigger/open304-lis-identifiers endpoints
(SYNC-74, OPEN-304).

Mirrors test_trigger_openstates_backfill.py's TestClient shape. Both endpoints share
run_fargate_script_job (vote_person_backfill.py) -- see that pipeline module's own tests for
its job-routing/RDS/Fargate-launch coverage; these tests only check each endpoint's own
validation and that it dispatches with the right job key.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _make_app():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return app


def _fake_scheduler():
    scheduler = MagicMock()
    scheduler._sync_config = {
        "openstates_archive": {"cloud_path": {"fargate": {"cluster": "ddp-scrapers"}}}
    }
    return scheduler


# ── /trigger/vote-person-backfill ────────────────────────────────────────────────────────────


def test_defaults_to_dry_run_and_returns_202_with_a_run_id():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.vote_person_backfill.run_fargate_script_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post("/trigger/vote-person-backfill")

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "started"
    assert body["mode"] == "dry-run"
    assert body["run_id"].startswith("vote-person-backfill-dry-run-")
    mock_run.assert_awaited_once()
    assert mock_run.call_args.args[0] == "vote-person-backfill"
    assert mock_run.call_args.args[1] == "dry-run"
    # The same run_id returned to the caller must be the one the job itself receives, or the
    # correlation this response promises would be a lie (same invariant as
    # test_trigger_openstates_backfill.py's identical check).
    assert mock_run.call_args.kwargs["run_id"] == body["run_id"]


def test_explicit_commit_mode_passed_through():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.vote_person_backfill.run_fargate_script_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post("/trigger/vote-person-backfill?mode=commit")

    assert resp.status_code == 202, resp.text
    assert resp.json()["mode"] == "commit"
    assert mock_run.call_args.args[0] == "vote-person-backfill"
    assert mock_run.call_args.args[1] == "commit"


def test_unknown_mode_404s_before_touching_the_pipeline():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.vote_person_backfill.run_fargate_script_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post("/trigger/vote-person-backfill?mode=delete-everything")

    assert resp.status_code == 404
    mock_run.assert_not_called()


def test_requires_api_key_auth():
    """Unlike the fixtures above (which override api_key_auth to always succeed), this proves
    the dependency is actually wired to the route at all -- a route that forgot to declare
    `token: str = Depends(api_key_auth)` would 202 here same as it does above."""
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    resp = client.post("/trigger/vote-person-backfill")

    assert resp.status_code == 401


# ── /trigger/open304-lis-identifiers ─────────────────────────────────────────────────────────


def test_open304_defaults_to_dry_run_and_dispatches_its_own_job_key():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.vote_person_backfill.run_fargate_script_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post("/trigger/open304-lis-identifiers")

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "started"
    assert body["mode"] == "dry-run"
    assert body["run_id"].startswith("open304-lis-identifiers-dry-run-")
    mock_run.assert_awaited_once()
    assert mock_run.call_args.args[0] == "open304-lis-identifiers"
    assert mock_run.call_args.args[1] == "dry-run"
    assert mock_run.call_args.kwargs["run_id"] == body["run_id"]


def test_open304_explicit_commit_mode_passed_through():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.vote_person_backfill.run_fargate_script_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post("/trigger/open304-lis-identifiers?mode=commit")

    assert resp.status_code == 202, resp.text
    assert resp.json()["mode"] == "commit"
    assert mock_run.call_args.args[0] == "open304-lis-identifiers"
    assert mock_run.call_args.args[1] == "commit"


def test_open304_unknown_mode_404s_before_touching_the_pipeline():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.vote_person_backfill.run_fargate_script_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post("/trigger/open304-lis-identifiers?mode=delete-everything")

    assert resp.status_code == 404
    mock_run.assert_not_called()


def test_open304_requires_api_key_auth():
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    resp = client.post("/trigger/open304-lis-identifiers")

    assert resp.status_code == 401
