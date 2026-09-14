"""HTTP-level tests for POST /trigger/bill-artifact-generation (SYNC-9).

OPEN-290: this endpoint now dispatches through pipelines.scraper_triggered_
legbot.trigger_scraper_session_pipeline (require_trigger_enabled=False)
instead of calling run_legbot_pipeline directly, so it shares the same
Redis overlap lock /trigger/scraper-session-legbot used to have exclusively
-- that endpoint is gone, consolidated into this one. Every test that
reaches a real dispatch therefore needs a working fake Redis store (the
lock must actually acquire/release), same pattern
test_scraper_triggered_legbot.py's own FakeRedisClient already established.

The underlying pipeline logic has its own unit coverage in
test_session_pipeline_runner.py; the lock/enable-flag mechanics have their
own unit coverage in test_scraper_triggered_legbot.py. This file covers
only this endpoint's own thin layer: auth, request validation, status-code
translation, and (new) that a manual call here is never blocked by the
automated-only enable flag but IS blocked by a real overlap.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.scraper_triggered_legbot import _lock_key

_VALID_PAYLOAD = {
    "jurisdiction_iso2": "fl",
    "session_code": "2026F",
    "artifact_types": ["bill_summary", "bill_pros_cons"],
    "include_org_research": False,
    "include_concept_statements": False,
    "retry_failed": False,
    "limit": 10,
}


class FakeRedisClient:
    """Mirrors test_scraper_triggered_legbot.py's own FakeRedisClient -- same
    minimal SET-NX-EX/GET/DELETE surface, duplicated here rather than shared
    since the original isn't exported from a conftest fixture either."""

    def __init__(self):
        self.store: dict[str, bytes] = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return False
        self.store[key] = value if isinstance(value, bytes) else str(value).encode()
        return True

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0


def _fake_redis_store(prefilled: dict[str, bytes] | None = None):
    rs = MagicMock()
    rs._client = FakeRedisClient()
    if prefilled:
        rs._client.store.update(prefilled)
    return rs


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
    )
    base.update(overrides)
    return SyncSettings(**base)


def _patch_redis(fake_store=None):
    """Patches the real get_redis_store the route now reaches through
    trigger_scraper_session_pipeline -- without this every request 503s
    with redis_unavailable, since no real Redis exists in the test env."""
    return patch(
        "ddp_sync.services.redis_store.get_redis_store",
        return_value=fake_store or _fake_redis_store(),
    )


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

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 500, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 200
    assert response.json()["bills_considered"] == 500
    mock_run.assert_awaited_once_with(
        "fl", "2026F", ["bill_summary", "bill_pros_cons"], False, 500,
        include_concept_statements=False, retry_failed=False, dry_run=False,
        broker_api_base=None, broker_api_token=None, bill_candidates=None,
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

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 200
    body = response.json()
    assert body["bills_considered"] == 3
    assert body["results"] == []
    assert body["success"] is True
    assert body["run_id"]
    mock_run.assert_awaited_once_with(
        "fl", "2026F", ["bill_summary", "bill_pros_cons"], False, 10,
        include_concept_statements=False, retry_failed=False, dry_run=False,
        broker_api_base=None, broker_api_token=None, bill_candidates=None,
    )


def test_dry_run_flag_is_passed_through():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, dry_run=True)

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["dry_run"] is True


def test_include_concept_statements_true_is_passed_through():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, include_concept_statements=True)

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["include_concept_statements"] is True


def test_value_error_from_pipeline_returns_400():
    client = _make_authed_client()

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(side_effect=ValueError("Unrecognized artifact_types: ['bogus']")),
    ):
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 400
    assert "Unrecognized artifact_types" in response.json()["detail"]


def test_unexpected_error_from_pipeline_returns_500():
    client = _make_authed_client()

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(side_effect=RuntimeError("broker unreachable")),
    ):
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 500


def test_no_x_ddp_environment_header_preserves_default_broker_target():
    """No X-DDP-Environment header (the default for every existing caller --
    the scheduled session_pipeline_batch job, direct operator calls) must
    keep this endpoint's original behavior: (None, None), letting
    run_legbot_pipeline fall through to whatever DDP_BROKER_API_BASE is
    globally configured -- covered directly by the exact-args assertions
    above, but stated as its own test for clarity."""
    client = _make_authed_client()

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["broker_api_base"] is None
    assert mock_run.await_args.kwargs["broker_api_token"] is None


def test_x_ddp_environment_dev_routes_to_dev_broker():
    client = _make_authed_client()

    with _patch_redis(), patch(
        "ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings(),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        response = client.post(
            "/trigger/bill-artifact-generation",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "dev"},
        )

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["broker_api_base"] == "http://localhost:8080"
    assert mock_run.await_args.kwargs["broker_api_token"] == "dev-token"


def test_x_ddp_environment_prod_routes_to_prod_broker():
    client = _make_authed_client()
    settings = _configured_settings(
        ondemand_broker_api_base_prod="https://api.digitaldemocracyproject.org/broker",
        ondemand_broker_api_token_prod="prod-token",
    )

    with _patch_redis(), patch(
        "ddp_sync.api.routes.triggers.get_settings", return_value=settings,
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        response = client.post(
            "/trigger/bill-artifact-generation",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "prod"},
        )

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["broker_api_base"] == "https://api.digitaldemocracyproject.org/broker"
    assert mock_run.await_args.kwargs["broker_api_token"] == "prod-token"


def test_invalid_x_ddp_environment_returns_400_without_calling_pipeline():
    client = _make_authed_client()

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline", new=AsyncMock(),
    ) as mock_run:
        response = client.post(
            "/trigger/bill-artifact-generation",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "staging"},
        )

    assert response.status_code == 400
    assert "X-DDP-Environment" in response.json()["detail"]
    mock_run.assert_not_awaited()


def test_x_ddp_environment_prod_without_prod_configured_returns_503():
    """Mirrors _resolve_ondemand_broker_target's own guard for the on-demand
    endpoints -- an instance with no ONDEMAND_BROKER_API_BASE_PROD set
    (e.g. a dev-only ddp-sync deployment) must fail clearly rather than
    silently writing to an empty base URL."""
    client = _make_authed_client()

    with patch(
        "ddp_sync.api.routes.triggers.get_settings", return_value=_configured_settings(),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline", new=AsyncMock(),
    ) as mock_run:
        response = client.post(
            "/trigger/bill-artifact-generation",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Environment": "prod"},
        )

    assert response.status_code == 503
    mock_run.assert_not_awaited()


def test_retry_failed_is_required_not_defaulted():
    """SYNC-42/AC1. Retrying spends real inference and rewrites real rows, so
    it is a conscious choice per call -- omitting it is a 422, not a silent
    False. This is the same discipline every other cost-relevant field on
    this model already follows."""
    client = _make_authed_client()
    payload = {k: v for k, v in _VALID_PAYLOAD.items() if k != "retry_failed"}

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 422
    mock_run.assert_not_awaited()
    assert any(
        err["loc"][-1] == "retry_failed" for err in response.json()["detail"]
    ), response.json()


def test_retry_failed_true_is_passed_through():
    client = _make_authed_client()
    payload = dict(_VALID_PAYLOAD, retry_failed=True)

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 0, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["retry_failed"] is True


# --- bill_candidates (SYNC-63) ----------------------------------------------

def test_bill_candidates_omitted_defaults_to_none_unchanged_behavior():
    """No field on this request forces a caller to think about
    bill_candidates -- omitting it entirely must reach run_legbot_pipeline
    as bill_candidates=None, exactly like before this ticket."""
    client = _make_authed_client()

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["bill_candidates"] is None


def test_bill_candidates_list_is_passed_through_as_plain_dicts():
    client = _make_authed_client()
    payload = dict(
        _VALID_PAYLOAD,
        bill_candidates=[
            {"gov_id": "HB 1", "bill_openstates_id": "id-1"},
            {
                "gov_id": "HB 2", "bill_openstates_id": "id-2",
                "live_url_fallback": "https://example.com/hb2.pdf",
            },
        ],
    )

    with _patch_redis(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 2, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 200
    assert mock_run.await_args.kwargs["bill_candidates"] == [
        {"gov_id": "HB 1", "bill_openstates_id": "id-1", "live_url_fallback": ""},
        {
            "gov_id": "HB 2", "bill_openstates_id": "id-2",
            "live_url_fallback": "https://example.com/hb2.pdf",
        },
    ]


def test_bill_candidates_entry_missing_required_field_returns_422():
    """Caught by FastAPI/Pydantic request validation before the pipeline is
    ever called -- same "fail loudly and early" posture as
    run_legbot_pipeline's own bill_candidates validation, just one layer
    further out for an HTTP caller."""
    client = _make_authed_client()
    payload = dict(
        _VALID_PAYLOAD,
        bill_candidates=[{"gov_id": "HB 1"}],  # missing bill_openstates_id
    )

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 422
    mock_run.assert_not_awaited()


def test_bill_candidates_entry_empty_string_field_returns_422():
    """/pm-review: an empty gov_id/bill_openstates_id must be rejected at
    the HTTP layer too, not just accepted as a technically-present string."""
    client = _make_authed_client()
    payload = dict(
        _VALID_PAYLOAD,
        bill_candidates=[{"gov_id": "", "bill_openstates_id": "id-1"}],
    )

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=payload)

    assert response.status_code == 422
    mock_run.assert_not_awaited()


# --- OPEN-290: shared overlap lock with the (now-removed) scraper-session- --
# --- legbot endpoint / the archive-completion hook --------------------------

def test_overlap_with_an_in_flight_automated_trigger_returns_409():
    """The exact scenario behind the real 2026-09-13 FL/2026E double-dispatch
    incident: an automated trigger (archive-completion hook, or the WireGuard
    hop from EC2) already holds the lock for this jurisdiction+session when a
    manual call arrives here -- it must be rejected, not run concurrently."""
    client = _make_authed_client()
    lock_key = _lock_key("fl", "2026F")

    with _patch_redis(_fake_redis_store({lock_key: b"some-other-run-id"})), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "already_running"
    assert response.json()["detail"]["current_run_id"] == "some-other-run-id"
    mock_run.assert_not_awaited()


def test_overlap_is_case_insensitive_matching_the_archive_hooks_own_casing():
    """pm-review: the archive-completion hook always locks with
    jurisdiction.upper() (openstates_archive.py's _maybe_trigger_legbot_for_
    archive), while this endpoint's caller can type any case -- _VALID_
    PAYLOAD itself uses lowercase 'fl'. Before the lock-key normalization
    fix, an automated 'FL' lock and a manual 'fl' call would never contend,
    silently defeating this whole ticket's fix for exactly the mismatched-
    casing shape the real callers actually use."""
    client = _make_authed_client()
    # Prefilled directly with the raw, uppercase key the archive hook would
    # produce -- not via _lock_key -- so this test doesn't just confirm
    # _lock_key is consistent with itself.
    from ddp_sync.pipelines.scraper_triggered_legbot import _LOCK_KEY_PREFIX
    raw_uppercase_key = f"{_LOCK_KEY_PREFIX}FL:2026F"

    with _patch_redis(_fake_redis_store({raw_uppercase_key: b"archive-hook-run-id"})), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 409
    assert response.json()["detail"]["current_run_id"] == "archive-hook-run-id"
    mock_run.assert_not_awaited()


def test_redis_unavailable_returns_503_without_calling_pipeline():
    client = _make_authed_client()
    unavailable = MagicMock()
    unavailable._client = None

    with patch(
        "ddp_sync.services.redis_store.get_redis_store", return_value=unavailable,
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 503
    mock_run.assert_not_awaited()


def test_automated_trigger_disabled_flag_does_not_block_this_endpoint():
    """LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED defaults to False (SYNC-48) and
    exists to pause only the automated path -- this manual endpoint must keep
    working exactly as before regardless of that flag's value, per
    trigger_scraper_session_pipeline's own require_trigger_enabled contract."""
    client = _make_authed_client()

    with _patch_redis(), patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        return_value=SyncSettings(legbot_scrape_completion_trigger_enabled=False),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        response = client.post("/trigger/bill-artifact-generation", json=_VALID_PAYLOAD)

    assert response.status_code == 200
    mock_run.assert_awaited_once()


# --- OPEN-290 (pm-review): X-DDP-Automated-Trigger restores the Mac-side --
# --- kill switch for the one real automated caller (the WireGuard hop) ---

def test_automated_trigger_header_is_still_paused_by_the_disabled_flag():
    """The one thing that must NOT change post-consolidation: the archive-
    completion hook's WireGuard call (which sends this header) is still
    paused by THIS instance's own LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED,
    exactly like the removed /trigger/scraper-session-legbot used to be --
    unlike a plain manual call (the test above), which never was."""
    client = _make_authed_client()

    with patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        return_value=SyncSettings(legbot_scrape_completion_trigger_enabled=False),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        response = client.post(
            "/trigger/bill-artifact-generation",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Automated-Trigger": "true"},
        )

    assert response.status_code == 500
    mock_run.assert_not_awaited()


def test_automated_trigger_header_still_dispatches_when_enabled():
    client = _make_authed_client()

    with _patch_redis(), patch(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        return_value=SyncSettings(legbot_scrape_completion_trigger_enabled=True),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        response = client.post(
            "/trigger/bill-artifact-generation",
            json=_VALID_PAYLOAD,
            headers={"X-DDP-Automated-Trigger": "true"},
        )

    assert response.status_code == 200
    mock_run.assert_awaited_once()
