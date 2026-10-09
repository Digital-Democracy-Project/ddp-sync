"""Tests for SYNC-100: verifying and storing organization-position citations from an external source.

The verify-and-write mechanics have their own coverage in test_bill_organization_position_research.py
(it runs through the same verify_and_store_position). These cover what is new: skipping what the
broker already settled, the rate-limit path, dry run, the limit, and the trigger's guards.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.bill_organization_position_research import verify_and_store_position
from ddp_sync.pipelines.verify_org_citations import MAX_CONSECUTIVE_RETRYABLE, verify_org_citations
from ddp_sync.services.broker_client import BrokerClientError

_RESEARCH = "ddp_sync.pipelines.bill_organization_position_research"
_BATCH = "ddp_sync.pipelines.verify_org_citations"

_VERSION = {"version_date": "2026-01-05", "version_note": "Introduced", "bill_title": "An act relating to tests"}


def _item(org="Sierra Club", position="support", url="https://a.invalid", bill="bill-1"):
    return {
        "bill_openstates_id": bill,
        "jurisdiction": "FL",
        "session_code": "2026",
        "gov_id": "HB123",
        "org_name": org,
        "position": position,
        "citation_url": url,
    }


def _row(org="Sierra Club", position="support", url="https://a.invalid", status="complete", verdict="confirmed"):
    return {
        "org_name": org,
        "position": position,
        "citation_url": url,
        "status": status,
        "verification_verdict": verdict,
    }


def _confirmed():
    return {
        "answer": {"verdict": "confirmed", "insufficient_information": False, "explanation": "says so"},
        "backend": "claude",
    }


def _rate_limited():
    return {
        "answer": {"insufficient_information": True, "reason": "backend error: Error code: 429 rate_limit_error"},
        "backend": "claude",
    }


def _patches(*, existing=None, version=_VERSION, verify=None, write=None):
    return (
        patch(f"{_BATCH}.get_bill_organization_positions_existing", new=AsyncMock(return_value=existing or [])),
        patch(f"{_BATCH}.get_current_version_identity", new=AsyncMock(return_value=version)),
        patch(f"{_RESEARCH}.dispatch_bill_position_verification", new=verify or AsyncMock(return_value=_confirmed())),
        patch(f"{_RESEARCH}.write_bill_organization_position", new=write or AsyncMock(return_value={"id": 7})),
    )


async def _run(items, *, dry_run=False, limit=None, **patch_kwargs):
    p_existing, p_version, p_verify, p_write = _patches(**patch_kwargs)
    with p_existing, p_version, p_verify as verify, p_write as write:
        summary = await verify_org_citations(items, source="slack-legislation-zapier", dry_run=dry_run, limit=limit)
    return summary, verify, write


@pytest.mark.asyncio
async def test_verifies_and_writes_with_the_external_source_as_provenance():
    summary, verify, write = await _run([_item()])

    assert summary["counts"] == {"written": 1}
    verify.assert_awaited_once()
    assert verify.await_args.args[0] == "https://a.invalid"
    kwargs = write.await_args.kwargs
    assert kwargs["find_model_name"] == "slack-legislation-zapier"
    assert kwargs["verification_verdict"] == "confirmed"
    assert kwargs["version_date"] == "2026-01-05"
    assert kwargs["citation_excerpt"] == ""


@pytest.mark.asyncio
async def test_second_run_over_the_same_batch_writes_nothing():
    """Acceptance criterion 2: the broker's settled rows are what make a rerun a no-op."""
    summary, verify, write = await _run([_item()], existing=[_row()])

    assert summary["counts"] == {"already_settled": 1}
    verify.assert_not_awaited()
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_match_ignores_org_name_case_and_whitespace():
    summary, verify, _ = await _run([_item(org="  sierra club ")], existing=[_row(org="Sierra Club")])
    assert summary["counts"] == {"already_settled": 1}


@pytest.mark.asyncio
async def test_a_not_confirmed_row_is_settled_too():
    """A verdict was reached, so there is nothing to retry."""
    summary, verify, _ = await _run([_item()], existing=[_row(verdict="not_confirmed")])
    assert summary["counts"] == {"already_settled": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_row",
    [
        _row(verdict="pending"),  # could not be checked
        _row(status="failed", verdict="pending"),
        _row(position="oppose"),  # same org and page, other stance
        _row(url="https://other.invalid"),  # same org and stance, other page
    ],
)
async def test_unsettled_or_different_rows_are_retried(existing_row):
    summary, verify, _ = await _run([_item()], existing=[existing_row])
    assert summary["counts"] == {"written": 1}
    verify.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_dispatch_failure_is_a_failed_row_for_that_item_only():
    """Acceptance criterion 3: the other items are unaffected."""
    from ddp_sync.services.legbot_client import LegBotDispatchError

    verify = AsyncMock(side_effect=[LegBotDispatchError("boom"), _confirmed()])
    summary, _, write = await _run([_item(org="A"), _item(org="B")], verify=verify)

    assert [r["outcome"] for r in summary["results"]] == ["verification_failed", "written"]
    first = write.await_args_list[0].kwargs
    assert first["status"] == "failed"
    assert first["failure_stage"] == "verification"


@pytest.mark.asyncio
async def test_a_rate_limited_item_is_retryable_and_writes_no_row():
    verify = AsyncMock(side_effect=[_rate_limited(), _confirmed()])
    summary, _, write = await _run([_item(org="A"), _item(org="B")], verify=verify)

    assert [r["outcome"] for r in summary["results"]] == ["retryable", "written"]
    assert write.await_count == 1  # only B


@pytest.mark.asyncio
async def test_stops_after_consecutive_rate_limited_answers():
    n = MAX_CONSECUTIVE_RETRYABLE + 2
    items = [_item(org=f"Org {i}") for i in range(n)]
    verify = AsyncMock(return_value=_rate_limited())
    summary, _, write = await _run(items, verify=verify)

    assert summary["stopped_early"] is True
    assert verify.await_count == MAX_CONSECUTIVE_RETRYABLE
    assert summary["counts"] == {"retryable": MAX_CONSECUTIVE_RETRYABLE, "not_attempted": 2}
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_success_resets_the_rate_limit_streak():
    verify = AsyncMock(
        side_effect=[_rate_limited(), _rate_limited(), _confirmed(), _rate_limited(), _rate_limited()]
    )
    summary, _, _ = await _run([_item(org=f"Org {i}") for i in range(5)], verify=verify)
    assert summary["stopped_early"] is False
    assert verify.await_count == 5


@pytest.mark.asyncio
async def test_dry_run_dispatches_and_writes_nothing():
    summary, verify, write = await _run([_item(org="A"), _item(org="B")], dry_run=True, existing=[_row(org="B")])

    assert summary["counts"] == {"dry_run": 1, "already_settled": 1}
    verify.assert_not_awaited()
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_limit_counts_dispatches_not_skipped_items():
    items = [_item(org="Settled"), _item(org="A"), _item(org="B"), _item(org="C")]
    summary, verify, _ = await _run(items, limit=2, existing=[_row(org="Settled")])

    assert verify.await_count == 2
    assert summary["counts"] == {"already_settled": 1, "written": 2, "not_attempted": 1}


@pytest.mark.asyncio
async def test_a_bill_with_no_current_version_is_skipped_not_fatal():
    summary, verify, _ = await _run([_item()], version=None)
    assert summary["counts"] == {"no_current_version": 1}
    verify.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_broker_read_skips_that_bill_and_continues():
    existing = AsyncMock(side_effect=[BrokerClientError("down"), []])
    p_existing, p_version, p_verify, p_write = _patches()
    with patch(f"{_BATCH}.get_bill_organization_positions_existing", new=existing), p_version, p_verify, p_write:
        summary = await verify_org_citations(
            [_item(bill="bill-1"), _item(bill="bill-2")], source="s", dry_run=False
        )
    assert [r["outcome"] for r in summary["results"]] == ["broker_read_failed", "written"]


@pytest.mark.asyncio
async def test_the_bills_existing_rows_are_read_once_per_bill():
    existing = AsyncMock(return_value=[])
    _, p_version, p_verify, p_write = _patches()
    with patch(f"{_BATCH}.get_bill_organization_positions_existing", new=existing), p_version, p_verify, p_write:
        await verify_org_citations([_item(org="A"), _item(org="B"), _item(org="C")], source="s", dry_run=False)
    existing.assert_awaited_once()


# -- the flag that makes a rate-limited answer retryable, on the shared function ---------------


def _common_kwargs(**overrides):
    kwargs = dict(
        bill_openstates_id="bill-1", jurisdiction="FL", session_code="2026", version_date="2026-01-05",
        version_note="Introduced", gov_id="HB123", bill_title="An act", invocation_id="inv-1",
        org_name="Sierra Club", position="support", citation_url="https://a.invalid",
    )
    kwargs.update(overrides)
    return kwargs


@pytest.mark.asyncio
async def test_a_rate_limited_answer_is_still_written_when_the_flag_is_off():
    """find_bill_positions research must keep its existing behavior."""
    with patch(f"{_RESEARCH}.dispatch_bill_position_verification", new=AsyncMock(return_value=_rate_limited())), \
            patch(f"{_RESEARCH}.write_bill_organization_position", new=AsyncMock(return_value={"id": 1})) as write:
        result = await verify_and_store_position(**_common_kwargs())
    assert result["outcome"] == "written"
    write.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_degraded_answer_that_is_not_rate_limiting_is_still_written_with_the_flag_on():
    """A >100-page PDF is a permanent property of the page, not something a retry fixes."""
    degraded = {"answer": {"insufficient_information": True, "reason": "backend error: 100 PDF pages"}, "backend": "claude"}
    with patch(f"{_RESEARCH}.dispatch_bill_position_verification", new=AsyncMock(return_value=degraded)), \
            patch(f"{_RESEARCH}.write_bill_organization_position", new=AsyncMock(return_value={"id": 1})) as write:
        result = await verify_and_store_position(**_common_kwargs(), skip_write_on_rate_limit=True)
    assert result["outcome"] == "written"
    assert write.await_args.kwargs["verification_verdict"] == "pending"


# -- the trigger -------------------------------------------------------------------------------

_BODY = {"source": "slack-legislation-zapier", "items": [_item()], "limit": 5}


def _client():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return TestClient(app)


def _settings(**overrides):
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


def test_trigger_requires_auth():
    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).post("/trigger/verify-org-citations", json=_BODY, headers={"X-DDP-Environment": "dev"})
    assert response.status_code == 401


def test_trigger_without_cams_returns_503():
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings(cams_artifacts_dir="")):
        response = _client().post("/trigger/verify-org-citations", json=_BODY, headers={"X-DDP-Environment": "dev"})
    assert response.status_code == 503


def test_trigger_without_an_environment_header_returns_400_and_runs_nothing():
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()), \
            patch(f"{_BATCH}.verify_org_citations", new=AsyncMock()) as run:
        response = _client().post("/trigger/verify-org-citations", json=_BODY)
    assert response.status_code == 400
    run.assert_not_awaited()


@pytest.mark.parametrize("bad_item", [{"position": "neutral"}, {"org_name": ""}, {"citation_url": ""}])
def test_trigger_rejects_a_malformed_item(bad_item):
    body = {**_BODY, "items": [{**_item(), **bad_item}]}
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()):
        response = _client().post("/trigger/verify-org-citations", json=body, headers={"X-DDP-Environment": "dev"})
    assert response.status_code == 422


def test_trigger_defaults_to_a_dry_run_on_the_dev_broker_and_returns_202():
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()), \
            patch(f"{_BATCH}.verify_org_citations", new=AsyncMock()) as run:
        response = _client().post("/trigger/verify-org-citations", json=_BODY, headers={"X-DDP-Environment": "dev"})

    assert response.status_code == 202, response.text
    body = response.json()
    assert body["dry_run"] is True
    assert body["run_id"].startswith("org-citation-verify-dry-")
    run.assert_awaited_once()
    assert run.await_args.kwargs["run_id"] == body["run_id"]
    assert run.await_args.kwargs["dry_run"] is True
    assert run.await_args.kwargs["broker_api_base"] == "http://localhost:8080"
    assert run.await_args.kwargs["broker_api_token"] == "dev-token"
