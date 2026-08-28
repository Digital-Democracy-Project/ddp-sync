"""HTTP-level tests for POST /trigger/legbot-analyze-bill-full (SYNC-15).

The single-parameter "run everything for this bill" counterpart to
/trigger/legbot-analyze-bill (SYNC-10) -- one call for all 8 artifact types
plus org position research, instead of 8+ separate calls. Mirrors
test_trigger_legbot_analyze_bill.py's thin-endpoint style: the underlying
dispatch/coverage-skip logic has its own unit coverage in
test_session_pipeline_runner.py, so these tests only exercise the
endpoint's own validation, host guard, and dev/prod broker-target routing.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.config import SyncSettings

_VALID_PAYLOAD = {
    "bill_openstates_id": "8d71a94e-0000-0000-0000-000000000001",
    "jurisdiction": "FL",
    "session_code": "2026F",
    "gov_id": "SB123",
    "bill_source": "https://flsenate.gov/Session/Bill/2026/123/BillText/Filed/PDF",
    "include_org_research": False,
    "include_concept_statements": False,
    "retry_failed": False,
}

_RUN_RESULT = {
    "gov_id": "SB123",
    "artifacts_generated": ["bill_summary"],
    "artifacts_skipped_present": [],
    "artifacts_failed": [],
    "artifacts_skipped_failed_previously": [],
    "artifact_durations_seconds": {"bill_summary": 1.2},
    "org_research_dispatched": False,
    "org_research_skipped_reason": None,
    "org_research_duration_seconds": None,
    "duration_seconds": 1.2,
    "error": None,
    "run_id": "11111111-1111-1111-1111-111111111111",
}


def _make_app():
    app = FastAPI()
    app.include_router(router)
    return app


def _make_authed_client():
    app = _make_app()
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return TestClient(app)


def _configured_settings(**overrides) -> SyncSettings:
    base = dict(
        cams_base_url="http://localhost:8000",
        cams_artifacts_dir="/tmp/artifacts",
        ondemand_broker_api_base_dev="http://localhost:8080",
        ondemand_broker_api_token_dev="dev-token",
        ondemand_broker_api_base_prod="",
        ondemand_broker_api_token_prod="",
    )
    base.update(overrides)
    return SyncSettings(**base)


def _patch_run(result=None, side_effect=None):
    kwargs = {"side_effect": side_effect} if side_effect else {"return_value": result or _RUN_RESULT}
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_single_bill_full",
        new=AsyncMock(**kwargs),
    )


def test_no_auth_returns_401():
    app = _make_app()
    client = TestClient(app)

    response = client.post(
        "/trigger/legbot-analyze-bill-full",
        json=_VALID_PAYLOAD,
        headers={"X-DDP-Environment": "dev"},
    )

    assert response.status_code == 401


def test_missing_required_field_returns_422():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD)
    del payload["include_org_research"]

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()):
        response = client.post(
            "/trigger/legbot-analyze-bill-full", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 422


def test_cams_not_configured_returns_503():
    client = _make_authed_client()
    unconfigured = _configured_settings(cams_artifacts_dir="")

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=unconfigured):
        response = client.post(
            "/trigger/legbot-analyze-bill-full",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 503
    assert "CAMS_BASE_URL" in response.json()["detail"]


def test_missing_environment_header_returns_400_without_running():
    client = _make_authed_client()

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_run() as mock_run:
        response = client.post("/trigger/legbot-analyze-bill-full", json=_VALID_PAYLOAD)

    assert response.status_code == 400
    assert "X-DDP-Environment" in response.json()["detail"]
    mock_run.assert_not_awaited()


def test_unrecognized_artifact_type_returns_400():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, artifact_types=["not_a_real_type"])

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_run(side_effect=ValueError("Unrecognized artifact_types: ['not_a_real_type']")):
        response = client.post(
            "/trigger/legbot-analyze-bill-full", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 400
    assert "Unrecognized artifact_types" in response.json()["detail"]


def test_omitted_artifact_types_defaults_to_none_meaning_all():
    """The endpoint itself doesn't resolve the default -- it passes None
    straight through to run_single_bill_full, which owns that decision
    (see test_session_pipeline_runner.py)."""
    client = _make_authed_client()
    payload = {k: v for k, v in _VALID_PAYLOAD.items() if k != "artifact_types"}

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_run() as mock_run:
        response = client.post(
            "/trigger/legbot-analyze-bill-full", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["artifact_types"] is None


def test_dev_environment_returns_full_result_synchronously():
    """Unlike /trigger/legbot-analyze-bill's 202/pending shape, this
    endpoint is synchronous -- returns the real result payload directly."""
    client = _make_authed_client()

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_run() as mock_run:
        response = client.post(
            "/trigger/legbot-analyze-bill-full",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["artifacts_generated"] == ["bill_summary"]
    assert body["run_id"] == _RUN_RESULT["run_id"]
    assert body["environment"] == "dev"

    call_kwargs = mock_run.await_args.kwargs
    assert call_kwargs["bill_openstates_id"] == _VALID_PAYLOAD["bill_openstates_id"]
    assert call_kwargs["jurisdiction_iso2"] == _VALID_PAYLOAD["jurisdiction"]
    assert call_kwargs["gov_id"] == _VALID_PAYLOAD["gov_id"]
    assert call_kwargs["include_org_research"] is False
    assert call_kwargs["broker_api_base"] == "http://localhost:8080"
    assert call_kwargs["broker_api_token"] == "dev-token"


def test_prod_environment_without_prod_broker_configured_returns_503():
    client = _make_authed_client()

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()):
        response = client.post(
            "/trigger/legbot-analyze-bill-full",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "prod"},
        )

    assert response.status_code == 503
    assert "ONDEMAND_BROKER_API_BASE_PROD" in response.json()["detail"]


def test_prod_environment_dispatches_with_prod_broker_target_when_configured():
    client = _make_authed_client()
    settings = _configured_settings(
        ondemand_broker_api_base_prod="http://10.0.0.11:8080",
        ondemand_broker_api_token_prod="prod-token",
    )

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=settings), \
            _patch_run() as mock_run:
        response = client.post(
            "/trigger/legbot-analyze-bill-full",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "prod"},
        )

    assert response.status_code == 200
    assert response.json()["environment"] == "prod"

    call_kwargs = mock_run.await_args.kwargs
    assert call_kwargs["broker_api_base"] == "http://10.0.0.11:8080"
    assert call_kwargs["broker_api_token"] == "prod-token"


def test_explicit_artifact_types_subset_is_passed_through():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, artifact_types=["bill_summary", "bill_pros_cons"])

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_run() as mock_run:
        response = client.post(
            "/trigger/legbot-analyze-bill-full", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["artifact_types"] == ["bill_summary", "bill_pros_cons"]


def test_dry_run_is_passed_through():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, dry_run=True)

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_run() as mock_run:
        response = client.post(
            "/trigger/legbot-analyze-bill-full", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["dry_run"] is True


def test_include_org_research_true_is_passed_through():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, include_org_research=True)

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_run() as mock_run:
        response = client.post(
            "/trigger/legbot-analyze-bill-full", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["include_org_research"] is True


def test_include_concept_statements_true_is_passed_through():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, include_concept_statements=True)

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_run() as mock_run:
        response = client.post(
            "/trigger/legbot-analyze-bill-full", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["include_concept_statements"] is True
