"""Tests for SYNC-100: verifying a cited organization position and storing the result.

The verify-and-write mechanics have their own coverage in test_bill_organization_position_research.py
(it runs through the same verify_and_store_position). These cover what is new: the batch loop, what
it reports back for a created / updated / unchanged row, the rate-limit path, and the trigger's guards.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import VERIFY_ORG_CITATIONS_MAX_ITEMS, router
from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.bill_organization_position_research import verify_and_store_position
from ddp_sync.pipelines.verify_org_citations import verify_org_citations
from ddp_sync.services.legbot_client import LegBotDispatchError

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


def _confirmed():
    return {
        "answer": {"verdict": "confirmed", "insufficient_information": False, "explanation": "says so"},
        "backend": "claude",
    }


def _not_confirmed():
    return {
        "answer": {"verdict": "not_confirmed", "insufficient_information": False, "explanation": "does not"},
        "backend": "claude",
    }


def _rate_limited():
    return {
        "answer": {"insufficient_information": True, "reason": "backend error: Error code: 429 rate_limit_error"},
        "backend": "claude",
    }


def _stored(result="created", verdict="confirmed", position_id=7):
    return {"id": position_id, "result": result, "verification_verdict": verdict}


async def _run(items, *, version=_VERSION, verify=None, write=None):
    with patch(f"{_BATCH}.get_current_version_identity", new=AsyncMock(return_value=version)) as version_lookup, \
            patch(f"{_RESEARCH}.dispatch_bill_position_verification",
                  new=verify or AsyncMock(return_value=_confirmed())) as verify_mock, \
            patch(f"{_RESEARCH}.write_bill_organization_position",
                  new=write or AsyncMock(return_value=_stored())) as write_mock:
        summary = await verify_org_citations(items, source="slack-legislation-zapier")
    return summary, verify_mock, write_mock, version_lookup


@pytest.mark.asyncio
async def test_verifies_the_cited_page_and_writes_with_the_source_as_provenance():
    summary, verify, write, _ = await _run([_item()])

    assert summary["counts"] == {"written": 1}
    assert verify.await_args.args[0] == "https://a.invalid"
    kwargs = write.await_args.kwargs
    assert kwargs["find_model_name"] == "slack-legislation-zapier"
    assert kwargs["verification_verdict"] == "confirmed"
    assert kwargs["version_date"] == "2026-01-05"
    assert kwargs["citation_excerpt"] == ""


@pytest.mark.asyncio
async def test_a_new_row_is_reported_as_created_with_its_verdict():
    summary, _, _, _ = await _run([_item()])
    row = summary["results"][0]
    assert (row["outcome"], row["result"], row["verification_verdict"], row["position_id"]) == (
        "written", "created", "confirmed", 7,
    )
    assert row["attempted_verdict"] == "confirmed"


@pytest.mark.asyncio
async def test_a_recheck_that_demotes_a_confirmed_row_says_so():
    """Use case: a visitor flagged a position and the double-check found the page does not support it.
    The caller needs to see that the stored verdict changed."""
    write = AsyncMock(return_value=_stored(result="updated", verdict="not_confirmed", position_id=3))
    summary, _, _, _ = await _run([_item()], verify=AsyncMock(return_value=_not_confirmed()), write=write)

    row = summary["results"][0]
    assert (row["result"], row["verification_verdict"], row["position_id"]) == ("updated", "not_confirmed", 3)
    assert row["attempted_verdict"] == "not_confirmed"


@pytest.mark.asyncio
async def test_an_inconclusive_recheck_reports_the_settled_verdict_it_kept():
    degraded = {"answer": {"insufficient_information": True, "reason": "backend error: could not read"}, "backend": "claude"}
    write = AsyncMock(return_value=_stored(result="unchanged", verdict="confirmed", position_id=3))
    summary, _, _, _ = await _run([_item()], verify=AsyncMock(return_value=degraded), write=write)

    row = summary["results"][0]
    assert (row["result"], row["verification_verdict"]) == ("unchanged", "confirmed")
    # what this check concluded is not the stored verdict: the caller must not read the stored
    # "confirmed" as the result of the new check
    assert row["attempted_verdict"] == "pending"
    # the inconclusive answer is what was sent; the broker is what refuses to overwrite with it
    assert write.await_args.kwargs["verification_verdict"] == "pending"


@pytest.mark.asyncio
async def test_a_dispatch_failure_is_a_failed_row_for_that_item_only():
    verify = AsyncMock(side_effect=[LegBotDispatchError("boom"), _confirmed()])
    summary, _, write, _ = await _run([_item(org="A"), _item(org="B")], verify=verify)

    assert [r["outcome"] for r in summary["results"]] == ["verification_failed", "written"]
    first = write.await_args_list[0].kwargs
    assert first["status"] == "failed"
    assert first["failure_stage"] == "verification"


@pytest.mark.asyncio
async def test_a_rate_limited_item_is_retryable_writes_nothing_and_the_rest_continue():
    verify = AsyncMock(side_effect=[_rate_limited(), _confirmed()])
    summary, _, write, _ = await _run([_item(org="A"), _item(org="B")], verify=verify)

    assert [r["outcome"] for r in summary["results"]] == ["retryable", "written"]
    assert write.await_count == 1  # only B
    assert summary["results"][0]["result"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", [
    {"insufficient_information": True},  # no reason at all
    {"insufficient_information": True, "reason": None},
    {"insufficient_information": True, "reason": "backend error: could not read the page"},
])
async def test_a_degraded_answer_without_a_rate_limit_reason_is_written_not_retryable(answer):
    """A missing or unrelated reason follows the existing missing-verdict policy and never raises."""
    verify = AsyncMock(return_value={"answer": answer, "backend": "claude"})
    summary, _, write, _ = await _run([_item()], verify=verify)

    assert summary["counts"] == {"written": 1}
    assert write.await_args.kwargs["verification_verdict"] == "pending"


@pytest.mark.asyncio
async def test_the_same_citation_twice_in_one_request_is_verified_once():
    """Verification is the paid step, so a repeat must not pay twice (the broker would only update)."""
    items = [_item(org="Sierra Club"), _item(org="  sierra club "), _item(org="Other")]
    summary, verify, write, _ = await _run(items)

    assert [r["outcome"] for r in summary["results"]] == ["written", "duplicate_in_request", "written"]
    assert verify.await_count == 2
    assert write.await_count == 2


@pytest.mark.asyncio
async def test_a_different_page_or_stance_for_the_same_organization_is_its_own_item():
    items = [_item(), _item(url="https://b.invalid"), _item(position="oppose")]
    summary, verify, _, _ = await _run(items)
    assert summary["counts"] == {"written": 3}
    assert verify.await_count == 3


@pytest.mark.asyncio
async def test_a_bill_with_no_current_version_is_reported_and_nothing_is_dispatched():
    summary, verify, write, _ = await _run([_item()], version=None)
    assert summary["counts"] == {"no_current_version": 1}
    verify.assert_not_awaited()
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_bills_current_version_is_looked_up_once_per_bill():
    items = [_item(org="A"), _item(org="B"), _item(org="C", bill="bill-2")]
    _, _, _, version_lookup = await _run(items)
    assert version_lookup.await_count == 2


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
async def test_a_permanent_degradation_is_still_written_with_the_flag_on():
    """A >100-page PDF is a property of the page, not something a retry fixes."""
    degraded = {"answer": {"insufficient_information": True, "reason": "backend error: 100 PDF pages"}, "backend": "claude"}
    with patch(f"{_RESEARCH}.dispatch_bill_position_verification", new=AsyncMock(return_value=degraded)), \
            patch(f"{_RESEARCH}.write_bill_organization_position", new=AsyncMock(return_value={"id": 1})) as write:
        result = await verify_and_store_position(**_common_kwargs(), skip_write_on_rate_limit=True)
    assert result["outcome"] == "written"
    assert write.await_args.kwargs["verification_verdict"] == "pending"


# -- the trigger -------------------------------------------------------------------------------

_BODY = {"source": "slack-legislation-zapier", "items": [_item()]}


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


def test_trigger_rejects_more_items_than_fit_inside_the_proxy_timeout():
    body = {**_BODY, "items": [_item(org=f"Org {i}") for i in range(VERIFY_ORG_CITATIONS_MAX_ITEMS + 1)]}
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()), \
            patch(f"{_BATCH}.verify_org_citations", new=AsyncMock()) as run:
        response = _client().post("/trigger/verify-org-citations", json=body, headers={"X-DDP-Environment": "dev"})
    assert response.status_code == 422
    run.assert_not_awaited()


def test_trigger_returns_the_results_synchronously_against_the_dev_broker():
    fake = {"run_id": "r1", "source": "slack-legislation-zapier", "counts": {"written": 1}, "results": []}
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()), \
            patch(f"{_BATCH}.verify_org_citations", new=AsyncMock(return_value=fake)) as run:
        response = _client().post("/trigger/verify-org-citations", json=_BODY, headers={"X-DDP-Environment": "dev"})

    assert response.status_code == 200, response.text
    assert response.json() == {**fake, "environment": "dev"}
    assert run.await_args.kwargs["source"] == "slack-legislation-zapier"
    assert run.await_args.kwargs["broker_api_base"] == "http://localhost:8080"
    assert run.await_args.kwargs["broker_api_token"] == "dev-token"
