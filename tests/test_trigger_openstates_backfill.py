"""Tests for the /trigger/openstates-backfill/{jurisdiction} endpoint (OPEN-268).

Mirrors test_trigger_openstates_archive.py's TestClient shape.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router


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


def test_valid_subcommand_and_mode_returns_202_with_a_run_id():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.openstates_backfill.run_backfill_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post(
            "/trigger/openstates-backfill/fl?subcommand=recompute-diff-order&mode=dry-run"
        )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "started"
    assert body["jurisdiction"] == "fl"
    assert body["subcommand"] == "recompute-diff-order"
    assert body["mode"] == "dry-run"
    assert body["run_id"].startswith("fl-recompute-diff-order-dry-run-")
    mock_run.assert_awaited_once()
    # The same run_id returned to the caller must be the one the job itself receives, or the
    # correlation this response promises would be a lie.
    assert mock_run.call_args.kwargs["run_id"] == body["run_id"]


def test_defaults_mode_to_dry_run_when_omitted():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.openstates_backfill.run_backfill_job", new=AsyncMock()
    ):
        resp = client.post("/trigger/openstates-backfill/ut?subcommand=refresh-extraction")

    assert resp.status_code == 202, resp.text
    assert resp.json()["mode"] == "dry-run"


def test_session_query_param_passed_through():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.openstates_backfill.run_backfill_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post(
            "/trigger/openstates-backfill/ut"
            "?subcommand=refresh-extraction&mode=commit&session=2025S2"
        )

    assert resp.status_code == 202, resp.text
    assert mock_run.call_args.kwargs["session"] == "2025S2"


def test_unknown_subcommand_404s_before_touching_the_pipeline():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.openstates_backfill.run_backfill_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post("/trigger/openstates-backfill/fl?subcommand=archive")

    assert resp.status_code == 404
    assert "archive" in resp.json()["detail"]
    mock_run.assert_not_called()


def test_unknown_mode_404s_before_touching_the_pipeline():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.openstates_backfill.run_backfill_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post(
            "/trigger/openstates-backfill/fl"
            "?subcommand=recompute-diff-order&mode=delete-everything"
        )

    assert resp.status_code == 404
    mock_run.assert_not_called()


def test_missing_subcommand_query_param_is_a_422_not_a_500():
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()):
        resp = client.post("/trigger/openstates-backfill/fl")

    assert resp.status_code == 422


def test_no_jurisdiction_allowlist_unlike_the_archive_trigger():
    """Deliberate difference from /trigger/openstates-archive/{target}: this endpoint does not
    validate `jurisdiction` against any pre-declared set (see the route's own docstring for
    why) -- an unrecognized jurisdiction string is accepted and handed to the pipeline, not
    404ed here."""
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=_fake_scheduler()), patch(
        "ddp_sync.pipelines.openstates_backfill.run_backfill_job", new=AsyncMock()
    ) as mock_run:
        resp = client.post(
            "/trigger/openstates-backfill/not-a-real-state?subcommand=recompute-diff-order"
        )

    assert resp.status_code == 202, resp.text
    mock_run.assert_awaited_once()
