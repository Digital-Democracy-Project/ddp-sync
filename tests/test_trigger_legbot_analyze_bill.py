"""HTTP-level tests for POST /trigger/legbot-analyze-bill (SYNC-10).

The on-demand single-bill counterpart to /trigger/bill-artifact-generation
(SYNC-9) -- ddp-next's interactive "explain this bill" UX. Mirrors
test_trigger_bill_artifact_generation.py's thin-endpoint style: the
underlying dispatch/write logic has its own unit coverage in
test_bill_artifact_generation.py, so these tests only exercise the
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
    "bill_source": "https://flsenate.gov/Session/Bill/2026/123/BillText/Filed/PDF",
    "artifact_type": "bill_summary",
}

_VERSION_IDENTITY = {
    "version_date": "2026-01-05",
    "version_note": "Introduced",
    "bill_title": "Save our Homes from Excessive Property Taxes",
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


def _patch_version_identity(result=_VERSION_IDENTITY):
    return patch(
        "ddp_sync.services.local_openstates_client.get_current_version_identity",
        new=AsyncMock(return_value=result),
    )


def _patch_dispatch():
    return patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_and_record_bill_artifact",
        new=AsyncMock(),
    )


def test_no_auth_returns_401():
    app = _make_app()
    client = TestClient(app)

    response = client.post(
        "/trigger/legbot-analyze-bill",
        json=_VALID_PAYLOAD,
        headers={"X-DDP-Environment": "dev"},
    )

    assert response.status_code == 401


def test_missing_required_field_returns_422():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD)
    del payload["artifact_type"]

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()):
        response = client.post(
            "/trigger/legbot-analyze-bill", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 422


def test_missing_environment_header_returns_400_without_dispatching():
    client = _make_authed_client()

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_dispatch() as mock_dispatch:
        response = client.post("/trigger/legbot-analyze-bill", json=_VALID_PAYLOAD)

    assert response.status_code == 400
    assert "X-DDP-Environment" in response.json()["detail"]
    mock_dispatch.assert_not_awaited()


def test_invalid_environment_header_value_returns_400():
    client = _make_authed_client()

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()):
        response = client.post(
            "/trigger/legbot-analyze-bill",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "staging"},
        )

    assert response.status_code == 400


def test_cams_not_configured_returns_503():
    client = _make_authed_client()
    unconfigured = _configured_settings(cams_artifacts_dir="")

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=unconfigured):
        response = client.post(
            "/trigger/legbot-analyze-bill",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 503
    assert "CAMS_BASE_URL" in response.json()["detail"]


def test_unrecognized_artifact_type_returns_400():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, artifact_type="qa_report")

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()):
        response = client.post(
            "/trigger/legbot-analyze-bill", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 400
    assert "Unrecognized artifact_type" in response.json()["detail"]


def test_unresolvable_bill_version_returns_404():
    client = _make_authed_client()

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_version_identity(result=None):
        response = client.post(
            "/trigger/legbot-analyze-bill",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 404


def test_prod_environment_without_prod_broker_configured_returns_503():
    client = _make_authed_client()

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()):
        response = client.post(
            "/trigger/legbot-analyze-bill",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "prod"},
        )

    assert response.status_code == 503
    assert "ONDEMAND_BROKER_API_BASE_PROD" in response.json()["detail"]


def test_dev_environment_dispatches_with_dev_broker_target():
    client = _make_authed_client()

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_version_identity(), _patch_dispatch() as mock_dispatch:
        response = client.post(
            "/trigger/legbot-analyze-bill",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert body["environment"] == "dev"

    call_kwargs = mock_dispatch.await_args.kwargs
    assert call_kwargs["bill_openstates_id"] == _VALID_PAYLOAD["bill_openstates_id"]
    assert call_kwargs["artifact_type"] == "bill_summary"
    assert call_kwargs["version_date"] == _VERSION_IDENTITY["version_date"]
    assert call_kwargs["version_note"] == _VERSION_IDENTITY["version_note"]
    assert call_kwargs["broker_api_base"] == "http://localhost:8080"
    assert call_kwargs["broker_api_token"] == "dev-token"


def test_prod_environment_dispatches_with_prod_broker_target_when_configured():
    client = _make_authed_client()
    settings = _configured_settings(
        ondemand_broker_api_base_prod="http://10.0.0.11:8080",
        ondemand_broker_api_token_prod="prod-token",
    )

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=settings), \
            _patch_version_identity(), _patch_dispatch() as mock_dispatch:
        response = client.post(
            "/trigger/legbot-analyze-bill",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "prod"},
        )

    assert response.status_code == 202
    assert response.json()["environment"] == "prod"

    call_kwargs = mock_dispatch.await_args.kwargs
    assert call_kwargs["broker_api_base"] == "http://10.0.0.11:8080"
    assert call_kwargs["broker_api_token"] == "prod-token"


def test_bill_changelog_artifact_type_is_accepted():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, artifact_type="bill_changelog")

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_version_identity(), _patch_dispatch() as mock_dispatch:
        response = client.post(
            "/trigger/legbot-analyze-bill", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 202
    assert mock_dispatch.await_args.kwargs["artifact_type"] == "bill_changelog"


def test_bill_topics_artifact_type_is_accepted():
    """SYNC-1: bill_topics must be dispatchable through this second real
    caller too, not just run_legbot_pipeline -- both import the same
    ALL_ARTIFACT_TYPES recognition gate."""
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, artifact_type="bill_topics")

    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings()), \
            _patch_version_identity(), _patch_dispatch() as mock_dispatch:
        response = client.post(
            "/trigger/legbot-analyze-bill", json=payload, headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 202
    assert mock_dispatch.await_args.kwargs["artifact_type"] == "bill_topics"
