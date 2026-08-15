"""HTTP-level tests for POST /trigger/bill-artifact-generation (SYNC-9).

run_legbot_pipeline's first real production caller. The endpoint is
intentionally thin -- auth + a limit ceiling + status-code translation only.
The underlying logic has its own unit coverage in
test_session_pipeline_runner.py.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router

_VALID_PAYLOAD = {
    "jurisdiction_iso2": "fl",
    "session_code": "2026F",
    "artifact_types": ["bill_summary", "bill_pros_cons"],
    "include_org_research": False,
    "limit": 10,
}


def _make_app():
    app = FastAPI()
    app.include_router(router)
    return app


def _make_authed_client():
    app = _make_app()
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return TestClient(app)


def test_no_auth_returns_401():
    app = _make_app()
    client = TestClient(app)

    response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 401


def test_missing_required_field_returns_422():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD)
    del payload["artifact_types"]

    response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 422


def test_limit_above_former_ceiling_now_passes_through_uncapped():
    """The old hard cap of 25 was removed 2026-08-15 -- run_legbot_pipeline
    dispatches sequentially regardless, and real MLX concurrency protection
    lives in CAMS's own semaphore (ddp-agents), not here. A large limit
    should reach the pipeline unmodified, not get rejected."""
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, limit=500)

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 500, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 200
    mock_run.assert_awaited_once_with(
        "fl", "2026F", ["bill_summary", "bill_pros_cons"], False, 500, dry_run=False
    )


def test_limit_zero_returns_400_without_calling_pipeline():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, limit=0)

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 400
    mock_run.assert_not_awaited()


def test_valid_payload_returns_200_and_calls_pipeline_with_exact_args():
    client = _make_authed_client()

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 200
    assert response.json() == {"bills_considered": 3, "results": []}
    mock_run.assert_awaited_once_with(
        "fl", "2026F", ["bill_summary", "bill_pros_cons"], False, 10, dry_run=False
    )


def test_dry_run_flag_is_passed_through():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, dry_run=True)

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["dry_run"] is True


def test_value_error_from_pipeline_returns_400():
    client = _make_authed_client()

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(side_effect=ValueError("Unrecognized artifact_types: ['bogus']")),
    ):
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 400
    assert "Unrecognized artifact_types" in response.json()["detail"]


def test_unexpected_error_from_pipeline_returns_500():
    client = _make_authed_client()

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(side_effect=RuntimeError("broker unreachable")),
    ):
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 500
