"""Tests for the /trigger/openstates-archive/{target} endpoint.

Pins the 2026-08-10 fix: the endpoint used to validate `target` against its own
hardcoded `_OPENSTATES_ARCHIVE_JURISDICTIONS` set, which silently went stale
when ma/al/us were added to openstates_archive.jurisdictions in
sync_schedule.yaml -- the scheduler and run_archive_jobs' own default both
picked up the new jurisdictions, but this endpoint kept 404ing on them. Now
reads the valid set from config (falling back to the shared
DEFAULT_ARCHIVE_JURISDICTIONS constant when no scheduler is registered).
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


def _fake_scheduler(jurisdictions):
    scheduler = MagicMock()
    scheduler._sync_config = {"openstates_archive": {"jurisdictions": jurisdictions}}
    return scheduler


def test_us_target_accepted_when_scheduler_config_includes_it():
    app = _make_app()
    client = TestClient(app)
    scheduler = _fake_scheduler(["fl", "ut", "az", "wa", "va", "mi", "ma", "al", "us"])
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job",
        new=AsyncMock(),
    ) as mock_run:
        resp = client.post("/trigger/openstates-archive/us")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"status": "started", "target": "us"}
    mock_run.assert_awaited_once()
    assert mock_run.call_args.args[0] == "us"


def test_ma_and_al_targets_also_accepted():
    app = _make_app()
    client = TestClient(app)
    scheduler = _fake_scheduler(["fl", "ut", "az", "wa", "va", "mi", "ma", "al", "us"])
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job",
        new=AsyncMock(),
    ):
        for target in ("ma", "al"):
            resp = client.post(f"/trigger/openstates-archive/{target}")
            assert resp.status_code == 200, resp.text


def test_unknown_target_still_404():
    app = _make_app()
    client = TestClient(app)
    scheduler = _fake_scheduler(["fl", "ut", "az", "wa", "va", "mi", "ma", "al", "us"])
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler):
        resp = client.post("/trigger/openstates-archive/nowhere")

    assert resp.status_code == 404
    assert "us" in resp.json()["detail"]  # available list includes the new jurisdictions


def test_falls_back_to_default_jurisdictions_when_no_scheduler_registered():
    """No scheduler (e.g. before startup finishes) -> still accepts a jurisdiction
    from the shared DEFAULT_ARCHIVE_JURISDICTIONS constant, not just the old
    six-jurisdiction set."""
    app = _make_app()
    client = TestClient(app)
    with patch("ddp_sync.scheduler.get_scheduler", return_value=None), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job",
        new=AsyncMock(),
    ) as mock_run:
        resp = client.post("/trigger/openstates-archive/us")

    assert resp.status_code == 200, resp.text
    mock_run.assert_awaited_once()


def test_all_target_dispatches_run_archive_jobs():
    app = _make_app()
    client = TestClient(app)
    scheduler = _fake_scheduler(["fl", "ut", "az", "wa", "va", "mi", "ma", "al", "us"])
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), patch(
        "ddp_sync.pipelines.openstates_archive.run_archive_jobs",
        new=AsyncMock(),
    ) as mock_run:
        resp = client.post("/trigger/openstates-archive/all")

    assert resp.status_code == 200, resp.text
    mock_run.assert_awaited_once()
