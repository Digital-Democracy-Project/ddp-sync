"""HTTP-level tests for POST /trigger/scraper-session-legbot (SYNC-59).

The remote entry point OPEN-193's EC2-broker ddp-sync instance calls, over
WireGuard, to trigger `trigger_scraper_session_pipeline` (SYNC-48's
overlap-safe, independently-gated automated-caller wrapper) on the Mac
Studio's own ddp-sync -- the only instance with CAMS/LegBot access. The
endpoint is intentionally thin: auth + environment-header resolution +
delegating every cost-relevant dispatch parameter to this instance's OWN
settings, never to the caller's request body. The underlying pipeline
logic has its own unit coverage in test_scraper_completion_hook.py.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.config import SyncSettings

_VALID_PAYLOAD = {"jurisdiction_iso2": "va", "session_code": "2026S1"}


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
        ondemand_broker_api_base_dev="http://localhost:8080",
        ondemand_broker_api_token_dev="dev-token",
        ondemand_broker_api_base_prod="",
        ondemand_broker_api_token_prod="",
        legbot_scrape_completion_trigger_artifact_types=["bill_summary", "bill_changelog"],
        legbot_scrape_completion_trigger_limit=10000,
        legbot_scrape_completion_trigger_include_concept_statements=True,
    )
    base.update(overrides)
    return SyncSettings(**base)


def test_no_auth_returns_401():
    app = _make_app()
    client = TestClient(app)

    response = client.post("/trigger/scraper-session-legbot", json=_VALID_PAYLOAD)

    assert response.status_code == 401


def test_missing_required_field_returns_422():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD)
    del payload["session_code"]

    response = client.post("/trigger/scraper-session-legbot", json=payload)

    assert response.status_code == 422


def test_valid_payload_calls_the_pipeline_with_settings_derived_args_not_the_callers():
    """The whole point: a remote automated caller supplies only
    jurisdiction_iso2/session_code -- every cost-relevant parameter
    (artifact_types, limit, include_concept_statements, include_org_research)
    comes from THIS instance's own settings, not the request body, matching
    the in-process hook's own policy exactly."""
    client = _make_authed_client()

    with patch(
        "ddp_sync.api.routes.triggers.get_settings",
        return_value=_configured_settings(),
    ), patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
        new=AsyncMock(return_value={"success": True, "run_id": "abc123"}),
    ) as mock_trigger:
        response = client.post("/trigger/scraper-session-legbot", json=_VALID_PAYLOAD)

    assert response.status_code == 200
    assert response.json() == {"success": True, "run_id": "abc123"}
    mock_trigger.assert_awaited_once_with(
        "va", "2026S1", ["bill_summary", "bill_changelog"], False, 10000,
        include_concept_statements=True,
        broker_api_base=None, broker_api_token=None,
    )


def test_a_non_success_pipeline_result_still_returns_200():
    """trigger_scraper_session_pipeline never raises -- its own "success":
    False outcomes (trigger_disabled, redis_unavailable, already_running,
    pipeline_error) are not this endpoint's error to report; the caller
    inspects the body itself."""
    client = _make_authed_client()

    with patch(
        "ddp_sync.api.routes.triggers.get_settings",
        return_value=_configured_settings(),
    ), patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
        new=AsyncMock(return_value={"success": False, "error": "already_running", "current_run_id": "xyz"}),
    ):
        response = client.post("/trigger/scraper-session-legbot", json=_VALID_PAYLOAD)

    assert response.status_code == 200
    assert response.json() == {
        "success": False, "error": "already_running", "current_run_id": "xyz",
    }


def test_ddp_environment_prod_header_resolves_the_prod_broker_target():
    client = _make_authed_client()

    with patch(
        "ddp_sync.api.routes.triggers.get_settings",
        return_value=_configured_settings(
            ondemand_broker_api_base_prod="http://prod-broker.example",
            ondemand_broker_api_token_prod="prod-token",
        ),
    ), patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
        new=AsyncMock(return_value={"success": True, "run_id": "abc123"}),
    ) as mock_trigger:
        response = client.post(
            "/trigger/scraper-session-legbot",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "prod"},
        )

    assert response.status_code == 200
    assert mock_trigger.await_args.kwargs["broker_api_base"] == "http://prod-broker.example"
    assert mock_trigger.await_args.kwargs["broker_api_token"] == "prod-token"


def test_ddp_environment_dev_header_resolves_the_dev_broker_target():
    client = _make_authed_client()

    with patch(
        "ddp_sync.api.routes.triggers.get_settings",
        return_value=_configured_settings(),
    ), patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
        new=AsyncMock(return_value={"success": True, "run_id": "abc123"}),
    ) as mock_trigger:
        response = client.post(
            "/trigger/scraper-session-legbot",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 200
    assert mock_trigger.await_args.kwargs["broker_api_base"] == "http://localhost:8080"
    assert mock_trigger.await_args.kwargs["broker_api_token"] == "dev-token"


def test_invalid_ddp_environment_header_returns_400():
    client = _make_authed_client()

    with patch(
        "ddp_sync.api.routes.triggers.get_settings",
        return_value=_configured_settings(),
    ), patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
        new=AsyncMock(),
    ) as mock_trigger:
        response = client.post(
            "/trigger/scraper-session-legbot",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "staging"},
        )

    assert response.status_code == 400
    mock_trigger.assert_not_awaited()


def test_include_org_research_is_always_false():
    """Gate 1 item 4 (PLAN-legbot.md §32): a deliberate operator decision,
    not a tunable default, same as the in-process hook -- no request field
    exists to override this."""
    client = _make_authed_client()

    with patch(
        "ddp_sync.api.routes.triggers.get_settings",
        return_value=_configured_settings(),
    ), patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
        new=AsyncMock(return_value={"success": True, "run_id": "abc123"}),
    ) as mock_trigger:
        client.post("/trigger/scraper-session-legbot", json=_VALID_PAYLOAD)

    assert mock_trigger.await_args.args[3] is False
