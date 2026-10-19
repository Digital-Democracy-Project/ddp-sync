"""Tests for SYNC-100: verifying a cited organization position and storing the result.

The verify-and-write mechanics have their own coverage in test_bill_organization_position_research.py
(it runs through the same verify_and_store_position). These cover what is new: the batch loop, what
it reports back for a created / updated / unchanged row, the rate-limit path, and the trigger's guards.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
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


def test_trigger_accepts_a_long_list_of_items():
    body = {**_BODY, "items": [_item(org=f"Org {i}") for i in range(50)]}
    fake = {"run_id": "r1", "source": body["source"], "counts": {"written": 50}, "results": []}
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()), \
            patch(f"{_BATCH}.verify_org_citations", new=AsyncMock(return_value=fake)) as run:
        response = _client().post("/trigger/verify-org-citations", json=body, headers={"X-DDP-Environment": "dev"})
    assert response.status_code == 200, response.text
    assert len(run.await_args.args[0]) == 50


def test_trigger_rejects_an_empty_list():
    body = {**_BODY, "items": []}
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()):
        response = _client().post("/trigger/verify-org-citations", json=body, headers={"X-DDP-Environment": "dev"})
    assert response.status_code == 422


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


# ---- SYNC-106: background runs --------------------------------------------------------------------


class _FakeStore:
    """In-memory stand-in for the Redis run records."""

    def __init__(self):
        self.runs: dict[str, dict] = {}

    async def set_verify_run(self, run_id, data):
        self.runs[run_id] = dict(data)

    async def get_verify_run(self, run_id):
        return self.runs.get(run_id)


@pytest.fixture
def store():
    fake = _FakeStore()
    with patch("ddp_sync.pipelines.verify_org_citations.get_redis_store", return_value=fake), \
            patch("ddp_sync.services.redis_store.get_redis_store", return_value=fake):
        yield fake


@pytest.mark.asyncio
async def test_progress_is_reported_after_every_item_including_a_duplicate():
    seen = []

    async def progress(processed, counts):
        seen.append((processed, dict(counts)))

    items = [_item(org="A"), _item(org="A"), _item(org="B")]
    with patch(f"{_BATCH}.get_current_version_identity", new=AsyncMock(return_value=_VERSION)), \
            patch(f"{_RESEARCH}.dispatch_bill_position_verification", new=AsyncMock(return_value=_confirmed())), \
            patch(f"{_RESEARCH}.write_bill_organization_position", new=AsyncMock(return_value=_stored())):
        await verify_org_citations(items, source="s", progress=progress)

    assert [p for p, _ in seen] == [1, 2, 3]
    assert seen[-1][1] == {"written": 2, "duplicate_in_request": 1}


def test_a_background_request_answers_202_and_the_run_finishes_with_its_counts(store):
    body = {**_BODY, "background": True, "items": [_item(org="A"), _item(org="B")]}
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()), \
            patch(f"{_BATCH}.get_current_version_identity", new=AsyncMock(return_value=_VERSION)), \
            patch(f"{_RESEARCH}.dispatch_bill_position_verification", new=AsyncMock(return_value=_confirmed())), \
            patch(f"{_RESEARCH}.write_bill_organization_position", new=AsyncMock(return_value=_stored())) as store_call:
        response = _client().post("/trigger/verify-org-citations", json=body, headers={"X-DDP-Environment": "dev"})
        run_id = response.json()["run_id"]
        read = _client().get(f"/trigger/verify-org-citations/{run_id}")

    assert response.status_code == 202
    assert response.json() == {"status": "started", "run_id": run_id, "total": 2, "environment": "dev"}
    assert store_call.await_count == 2
    run = read.json()
    assert (run["status"], run["total"], run["processed"], run["counts"]) == ("finished", 2, 2, {"written": 2})
    assert run["environment"] == "dev" and run["finished_at"] and run["error"] is None


def test_a_background_request_keeps_the_guards_of_the_synchronous_one(store):
    body = {**_BODY, "background": True}
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()):
        response = _client().post("/trigger/verify-org-citations", json=body)  # no environment header
    assert response.status_code == 400
    assert store.runs == {}


def test_a_background_run_that_raises_is_recorded_as_failed(store):
    body = {**_BODY, "background": True}
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()), \
            patch(f"{_BATCH}.get_current_version_identity", new=AsyncMock(side_effect=RuntimeError("api-v3 down"))):
        response = _client().post("/trigger/verify-org-citations", json=body, headers={"X-DDP-Environment": "dev"})
        run = _client().get(f"/trigger/verify-org-citations/{response.json()['run_id']}").json()

    assert run["status"] == "failed"
    assert "api-v3 down" in run["error"]


def test_reading_an_unknown_run_is_a_404(store):
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=_settings()):
        assert _client().get("/trigger/verify-org-citations/nope").status_code == 404


@pytest.mark.asyncio
async def test_a_second_background_run_waits_for_the_first(store):
    import asyncio

    from ddp_sync.pipelines.verify_org_citations import run_verify_org_citations_in_background

    release = asyncio.Event()
    started: list[str] = []

    async def fake_verify(items, *, run_id, **_):
        started.append(run_id)
        await release.wait()

    kwargs = dict(source="s", environment="dev", broker_api_base="b", broker_api_token="t")
    with patch(f"{_BATCH}.verify_org_citations", new=fake_verify):
        first = asyncio.create_task(run_verify_org_citations_in_background([_item()], run_id="r1", **kwargs))
        await asyncio.sleep(0.01)
        second = asyncio.create_task(run_verify_org_citations_in_background([_item()], run_id="r2", **kwargs))
        await asyncio.sleep(0.01)
        assert started == ["r1"]
        assert (store.runs["r1"]["status"], store.runs["r2"]["status"]) == ("running", "queued")
        release.set()
        await asyncio.gather(first, second)

    assert started == ["r1", "r2"]
    assert (store.runs["r1"]["status"], store.runs["r2"]["status"]) == ("finished", "finished")


# ── SYNC-127: the matcher and About steps run after a citation's write, as in research ─────────────


def _step_settings(mode="act", content=False):
    return SimpleNamespace(organization_resolution_mode=mode, organization_content_enabled=content)


async def _run_with_steps(*, mode="act", content=False, stored=None, resolve=None, about=None):
    stored = stored or {**_stored(), "organization_id": 55, "organization_needs_content": True}
    resolve = resolve or AsyncMock(return_value={
        "position_id": 7, "attempt_id": 3, "outcome": "acted", "choice": 10, "organization_id": 10,
    })
    about = about or AsyncMock(return_value={"organization_id": 55, "outcome": "written", "written": ["description"]})
    with patch(f"{_RESEARCH}.get_settings", return_value=_step_settings(mode, content)), \
            patch(f"{_RESEARCH}.resolve_organization_for_position", new=resolve), \
            patch(f"{_RESEARCH}.generate_and_store_organization_content", new=about):
        summary, *_ = await _run([_item(url="https://cited.invalid")], write=AsyncMock(return_value=stored))
    return summary, resolve, about


@pytest.mark.asyncio
async def test_a_new_confirmed_citation_runs_the_matcher_with_the_cited_page():
    summary, resolve, _ = await _run_with_steps()

    resolve.assert_awaited_once()
    kwargs = resolve.await_args.kwargs
    assert (kwargs["position_id"], kwargs["org_name"], kwargs["citation_url"], kwargs["citation_excerpt"],
            kwargs["mode"]) == (7, "Sierra Club", "https://cited.invalid", "", "act")
    assert summary["results"][0]["resolution"]["outcome"] == "acted"


@pytest.mark.asyncio
async def test_with_the_matcher_off_nothing_is_resolved_and_the_result_has_no_resolution():
    summary, resolve, _ = await _run_with_steps(mode="off")

    resolve.assert_not_awaited()
    assert "resolution" not in summary["results"][0]


@pytest.mark.asyncio
async def test_an_updated_row_is_not_resolved_again():
    _, resolve, _ = await _run_with_steps(stored={**_stored(result="updated"), "organization_id": 55})

    resolve.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_stub_the_matcher_left_alone_gets_its_about_content():
    no_match = AsyncMock(return_value={
        "position_id": 7, "attempt_id": 3, "outcome": "null_answer", "choice": None, "organization_id": None,
    })
    summary, _, about = await _run_with_steps(content=True, resolve=no_match)

    about.assert_awaited_once()
    assert about.await_args.kwargs["organization_id"] == 55
    assert summary["results"][0]["about_content"]["outcome"] == "written"


@pytest.mark.asyncio
async def test_a_position_moved_to_an_existing_organization_gets_no_about_content_for_the_stub():
    _, _, about = await _run_with_steps(content=True)

    about.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_matcher_that_raises_leaves_the_written_citation_recorded():
    summary, _, _ = await _run_with_steps(resolve=AsyncMock(side_effect=RuntimeError("boom")))

    assert summary["counts"] == {"written": 1}
    assert summary["results"][0]["result"] == "created"
    assert summary["results"][0]["resolution"]["outcome"] == "failed"
