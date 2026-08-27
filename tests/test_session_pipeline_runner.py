"""Tests for the session-targeted batch runner (ddp-infra's
PLAN-bill-document-provenance.md, "Step 1, scoped version", approved
2026-08-01 after 3 rounds of /pm-review).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.session_pipeline_runner import (
    ALL_ARTIFACT_TYPES,
    run_scheduled_session_pipeline,
    run_legbot_pipeline,
    run_single_bill_full,
)
from ddp_sync.services.broker_client import BrokerClientError

_CANDIDATE = {
    "gov_id": "SJR 2F",
    "bill_openstates_id": "a3afb726-0000-0000-0000-000000000001",
    "session_code": "2026F",
    "live_url_fallback": "https://flsenate.gov/bill/2f.pdf",
}

_VERSION_IDENTITY = {
    "version_date": "2026-06-01",
    "version_note": "c1",
    "bill_title": "Save our Homes from Excessive Property Taxes",
    "chamber_classification": "lower",
    "jurisdiction_classification": "state",
}


def _patch_lister(candidates):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.list_current_session_bill_candidates",
        new=AsyncMock(return_value=candidates),
    )


def _patch_coverage(result):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_artifacts",
        new=AsyncMock(return_value=result),
    )


def _patch_org_status(result):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_organization_positions_status",
        new=AsyncMock(return_value=result),
    )


def _patch_concept_set(result=None, side_effect=None):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_concept_statement_set",
        new=AsyncMock(return_value=result, side_effect=side_effect),
    )


def _patch_version(result=_VERSION_IDENTITY):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_current_version_identity",
        new=AsyncMock(return_value=result),
    )


def _patch_archived_text(result=None):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_archived_bill_text",
        new=AsyncMock(return_value=result),
    )


def _patch_ensure(result=None, side_effect=None):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.ensure_bill_exists",
        new=AsyncMock(
            return_value=result if result is not None else {"bill_id": 1, "created": True},
            side_effect=side_effect,
        ),
    )


@pytest.fixture(autouse=True)
def _default_no_archived_text(monkeypatch):
    """SYNC-21: every pre-existing test in this file predates the new
    get_archived_bill_text-gated ensure_bill_exists call in _process_bill.
    Default every test here to "no archived text" (the common case for
    every jurisdiction but FL today) so `ensure` is never attempted unless a
    test explicitly overrides this via _patch_archived_text(...) -- keeps
    every pre-existing test's dispatch behavior unchanged, since a bill
    without archived text never triggers `ensure` per this ticket's own
    design (PLAN-local-openstates-migration.md §3.6)."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.session_pipeline_runner.get_archived_bill_text",
        AsyncMock(return_value=None),
    )


# --- input validation --------------------------------------------------

@pytest.mark.asyncio
async def test_rejects_empty_jurisdiction():
    with pytest.raises(ValueError, match="jurisdiction_iso2"):
        await run_legbot_pipeline("", "2026F", ["bill_summary"], False, 10, include_concept_statements=False)


@pytest.mark.asyncio
async def test_rejects_empty_session_code():
    with pytest.raises(ValueError, match="session_code"):
        await run_legbot_pipeline("fl", "", ["bill_summary"], False, 10, include_concept_statements=False)


@pytest.mark.asyncio
async def test_rejects_non_positive_limit():
    with pytest.raises(ValueError, match="limit"):
        await run_legbot_pipeline("fl", "2026F", ["bill_summary"], False, 0, include_concept_statements=False)


@pytest.mark.asyncio
async def test_rejects_empty_artifact_types():
    with pytest.raises(ValueError, match="artifact_types"):
        await run_legbot_pipeline("fl", "2026F", [], False, 10, include_concept_statements=False)


@pytest.mark.asyncio
async def test_rejects_unrecognized_artifact_type():
    with pytest.raises(ValueError, match="Unrecognized"):
        await run_legbot_pipeline("fl", "2026F", ["not_a_real_type"], False, 10, include_concept_statements=False)


@pytest.mark.asyncio
async def test_bill_topics_is_a_recognized_artifact_type():
    """SYNC-1: bill_topics must be dispatchable through this real caller,
    not just accepted by generate_and_store_bill_artifact directly -- the
    gap the first implementation attempt (PR #39) missed. dry_run=True
    proves ALL_ARTIFACT_TYPES's own recognition check passes without
    ever needing a real LegBot/broker call."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_topics"], False, limit=10, dry_run=True,
            include_concept_statements=False,
        )

    mock_artifact.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == ["bill_topics"]


@pytest.mark.asyncio
async def test_none_of_the_required_args_have_defaults():
    """Round 2 of /pm-review's own point: every cost-relevant argument is a
    conscious choice, not just artifact_types/include_org_research but
    limit too -- and, since SYNC-31, include_concept_statements, the same
    real-dispatch-cost category as include_org_research."""
    import inspect
    sig = inspect.signature(run_legbot_pipeline)
    for name in (
        "artifact_types", "include_org_research", "include_concept_statements", "limit",
    ):
        assert sig.parameters[name].default is inspect.Parameter.empty


# --- truncation ----------------------------------------------------------

@pytest.mark.asyncio
async def test_truncated_is_true_when_more_candidates_exist_than_limit():
    candidates = [dict(_CANDIDATE, gov_id=f"HB {n}") for n in range(3)]
    with _patch_lister(candidates), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=2,
            include_concept_statements=False,
        )

    assert result["truncated"] is True
    assert result["bills_considered"] == 2
    assert result["bills_processed"] == 2


@pytest.mark.asyncio
async def test_truncated_is_false_when_exactly_limit_candidates_exist():
    candidates = [dict(_CANDIDATE, gov_id=f"HB {n}") for n in range(2)]
    with _patch_lister(candidates), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=2,
            include_concept_statements=False,
        )

    assert result["truncated"] is False
    assert result["bills_considered"] == 2


@pytest.mark.asyncio
async def test_lister_called_with_limit_plus_one():
    with _patch_lister([]) as mock_lister:
        await run_legbot_pipeline("fl", "2026F", ["bill_summary"], False, limit=5, include_concept_statements=False)

    mock_lister.assert_awaited_once_with("fl", session_code="2026F", limit=6)


# --- dry_run ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_dispatches_nothing():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact, patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_changelog",
        new=AsyncMock(),
    ) as mock_changelog, _patch_org_status({"has_rows": False, "row_count": 0}), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_organization_positions",
        new=AsyncMock(),
    ) as mock_org:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary", "bill_changelog"], True, limit=10, dry_run=True,
            include_concept_statements=False,
        )

    mock_artifact.assert_not_awaited()
    mock_changelog.assert_not_awaited()
    mock_org.assert_not_awaited()
    bill_result = result["results"][0]
    assert set(bill_result["artifacts_generated"]) == {"bill_summary", "bill_changelog"}
    assert bill_result["org_research_dispatched"] is True


# --- coverage-based skip/dispatch decisions ---------------------------------

@pytest.mark.asyncio
async def test_present_complete_row_is_skipped_not_regenerated():
    with _patch_lister([_CANDIDATE]), _patch_coverage(
        {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "complete"}}}
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact, _patch_org_status({"has_rows": True, "row_count": 3}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_artifact.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["artifacts_skipped_present"] == ["bill_summary"]
    assert bill_result["artifacts_generated"] == []


@pytest.mark.asyncio
async def test_previously_failed_row_is_skipped_not_retried():
    with _patch_lister([_CANDIDATE]), _patch_coverage(
        {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "failed"}}}
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact, _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_artifact.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["artifacts_skipped_failed_previously"] == ["bill_summary"]
    assert bill_result["artifacts_skipped_present"] == []


@pytest.mark.asyncio
async def test_missing_row_is_dispatched():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ) as mock_artifact, _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_artifact.assert_awaited_once()
    call_kwargs = mock_artifact.await_args.kwargs
    assert call_kwargs["artifact_type"] == "bill_summary"
    assert call_kwargs["version_date"] == "2026-06-01"
    assert call_kwargs["version_note"] == "c1"
    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_bill_changelog_dispatches_via_the_changelog_function_not_the_artifact_one():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact, patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_changelog",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ) as mock_changelog, _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_changelog"], True, limit=10,
            include_concept_statements=False,
        )

    mock_artifact.assert_not_awaited()
    mock_changelog.assert_awaited_once()
    assert "bill_source" not in mock_changelog.await_args.kwargs
    assert result["results"][0]["artifacts_generated"] == ["bill_changelog"]


# --- failure isolation -------------------------------------------------

@pytest.mark.asyncio
async def test_one_artifact_type_failure_does_not_abort_the_bill_or_batch():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(side_effect=[Exception("dispatch failed"), {"id": 2, "status": "complete"}]),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary", "bill_pros_cons"], True, limit=10,
            include_concept_statements=False,
        )

    bill_result = result["results"][0]
    assert bill_result["artifacts_failed"] == ["bill_summary"]
    assert bill_result["artifacts_generated"] == ["bill_pros_cons"]
    assert result["bills_processed"] == 1


@pytest.mark.asyncio
async def test_a_version_mismatch_exception_is_caught_the_same_as_any_other():
    """No bill_changelog-specific handling -- ArchivedVersionMismatchError is
    just one more exception the generic per-artifact isolation catches."""
    from ddp_sync.pipelines.bill_artifact_generation import ArchivedVersionMismatchError

    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_changelog",
        new=AsyncMock(side_effect=ArchivedVersionMismatchError("stale version")),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_changelog"], True, limit=10,
            include_concept_statements=False,
        )

    assert result["results"][0]["artifacts_failed"] == ["bill_changelog"]


@pytest.mark.asyncio
async def test_coverage_check_failure_is_isolated_to_that_bill():
    candidates = [_CANDIDATE, dict(_CANDIDATE, gov_id="HB 999", bill_openstates_id="other-uuid")]
    with _patch_lister(candidates), patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_artifacts",
        new=AsyncMock(side_effect=[BrokerClientError("unreachable"), None]),
    ), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    assert result["bills_processed"] == 2
    assert result["results"][0]["error"] is not None
    assert result["results"][1]["error"] is None
    assert result["results"][1]["artifacts_generated"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_missing_version_identity_fails_the_needed_artifacts_not_the_whole_bill():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(None), _patch_org_status(
        {"has_rows": True, "row_count": 0}
    ):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    bill_result = result["results"][0]
    assert bill_result["artifacts_failed"] == ["bill_summary"]
    assert bill_result["error"] is None


# --- SYNC-24: a returned status="failed" row is a failure, not a success ---

@pytest.mark.asyncio
async def test_a_normally_returned_failed_status_is_not_counted_as_generated():
    """generate_and_store_bill_artifact returns normally (no exception) for a
    legitimate LegBot decline (e.g. insufficient_information) -- it still
    writes a real status="failed" BillArtifact row. Before SYNC-24,
    _process_bill only checked whether an exception was raised, so this
    silently landed in artifacts_generated. This is the exact SB2500E
    scenario from the ticket: real archived text, no exception, but the
    model correctly declined."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={
            "id": 1, "status": "failed", "failure_reason": "insufficient_information",
        }),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == []
    assert bill_result["artifacts_failed"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_a_normally_returned_complete_status_is_still_counted_as_generated():
    """The success path must keep working exactly as before -- a real
    status="complete" return still lands in artifacts_generated."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == ["bill_summary"]
    assert bill_result["artifacts_failed"] == []


@pytest.mark.asyncio
async def test_bill_changelog_normally_returned_failed_status_is_not_counted_as_generated():
    """Same status-inspection fix applies to the bill_changelog branch, which
    dispatches via a separate function than every other artifact_type."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_changelog",
        new=AsyncMock(return_value={
            "id": 1, "status": "failed", "failure_reason": "insufficient_information",
        }),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_changelog"], True, limit=10,
            include_concept_statements=False,
        )

    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == []
    assert bill_result["artifacts_failed"] == ["bill_changelog"]


@pytest.mark.asyncio
async def test_bill_changelog_normally_returned_complete_status_is_still_counted_as_generated():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_changelog",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_changelog"], True, limit=10,
            include_concept_statements=False,
        )

    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == ["bill_changelog"]
    assert bill_result["artifacts_failed"] == []


@pytest.mark.asyncio
async def test_a_raised_exception_and_a_normal_failed_status_are_both_captured_independently():
    """A thrown exception (a real dispatch outage) and a normally-returned
    status="failed" (a legitimate decline) both end up in artifacts_failed,
    but via entirely independent code paths -- proven here by triggering
    both for two different artifact_types on the same bill in one call."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(side_effect=[
            Exception("LegBot unreachable"),
            {"id": 2, "status": "failed", "failure_reason": "insufficient_information"},
        ]),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary", "bill_pros_cons"], True, limit=10,
            include_concept_statements=False,
        )

    bill_result = result["results"][0]
    assert set(bill_result["artifacts_failed"]) == {"bill_summary", "bill_pros_cons"}
    assert bill_result["artifacts_generated"] == []


@pytest.mark.asyncio
async def test_run_legbot_pipeline_artifacts_generated_excludes_declined_bills():
    """Pipeline-level regression for AC #6: across a two-bill run, one bill's
    dispatch completes and the other's legitimately declines (no exception)
    -- run_legbot_pipeline's own per-bill results must not conflate the two,
    matching the ticket's own real dispatch (16 "generated", one of which
    -- SB2500E -- had actually declined)."""
    candidates = [_CANDIDATE, dict(_CANDIDATE, gov_id="SB2500E", bill_openstates_id="other-uuid")]
    with _patch_lister(candidates), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(side_effect=[
            {"id": 1, "status": "complete"},
            {"id": 2, "status": "failed", "failure_reason": "insufficient_information"},
        ]),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    completed_bill, declined_bill = result["results"]
    assert completed_bill["gov_id"] == "SJR 2F"
    assert completed_bill["artifacts_generated"] == ["bill_summary"]
    assert completed_bill["artifacts_failed"] == []
    assert declined_bill["gov_id"] == "SB2500E"
    assert declined_bill["artifacts_generated"] == []
    assert declined_bill["artifacts_failed"] == ["bill_summary"]


# --- SYNC-21: ensure_bill_exists gating (PLAN-local-openstates-migration.md §3.6) ---

@pytest.mark.asyncio
async def test_ensure_called_when_archived_text_exists_and_dispatch_proceeds():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
         _patch_archived_text("the actual bill text"), _patch_ensure() as mock_ensure, \
         patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_ensure.assert_awaited_once()
    call_kwargs = mock_ensure.await_args.kwargs
    assert call_kwargs["jurisdiction"] == "fl"
    assert call_kwargs["session_code"] == "2026F"
    assert call_kwargs["gov_id"] == "SJR 2F"
    assert call_kwargs["title"] == "Save our Homes from Excessive Property Taxes"
    assert call_kwargs["chamber_classification"] == "lower"
    assert call_kwargs["jurisdiction_classification"] == "state"
    assert call_kwargs["bill_openstates_id"] == "a3afb726-0000-0000-0000-000000000001"
    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == ["bill_summary"]
    assert bill_result["error"] is None


@pytest.mark.asyncio
async def test_ensure_called_with_each_bills_own_openstates_id_not_a_stale_one():
    """SYNC-22 (PLAN-local-openstates-migration.md §3.6, BROKER-81's paired
    fix): ensure_bill_exists must be called with EACH candidate's own
    bill_openstates_id, never a hardcoded, stale, or leaked-from-a-previous-
    iteration value -- without this, ddp-broker-py has no way to backfill
    primary_openstates_id on the stub it creates/matches, so a later Voatz/
    Webflow curation pass can't match its own OpenStates lookup back to the
    stub and creates a duplicate Bill row instead of promoting this one.
    Two candidates in the same run (rather than one) is what actually rules
    out a loop-variable/closure bug that a single-candidate case can't
    catch."""
    candidate_a = {**_CANDIDATE, "gov_id": "SJR 2F", "bill_openstates_id": "aaaaaaaa-1111-1111-1111-111111111111"}
    candidate_b = {**_CANDIDATE, "gov_id": "HB 100", "bill_openstates_id": "bbbbbbbb-2222-2222-2222-222222222222"}
    with _patch_lister([candidate_a, candidate_b]), _patch_coverage(None), _patch_version(), \
         _patch_archived_text("the actual bill text"), _patch_ensure() as mock_ensure, \
         patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        await run_legbot_pipeline("fl", "2026F", ["bill_summary"], True, limit=10, include_concept_statements=False)

    assert mock_ensure.await_count == 2
    # Pair by gov_id (also passed on the same call) rather than just
    # comparing the two id sets -- this is what actually catches a swap
    # (each id correct in isolation, but attached to the wrong bill).
    ids_by_gov_id = {call.kwargs["gov_id"]: call.kwargs["bill_openstates_id"] for call in mock_ensure.await_args_list}
    assert ids_by_gov_id == {
        "SJR 2F": "aaaaaaaa-1111-1111-1111-111111111111",
        "HB 100": "bbbbbbbb-2222-2222-2222-222222222222",
    }


@pytest.mark.asyncio
async def test_ensure_not_called_when_no_archived_text_yet():
    """The sharpest version of the "don't stub a skipped bill" AC: version
    metadata resolves fine (the common case for every jurisdiction but FL
    today has this), but there's no archived text yet -- `ensure` must never
    be attempted, and dispatch must still proceed normally via the existing
    live-fetch fallback (this new gate doesn't block artifact dispatch
    itself, only the new `ensure` call)."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
         _patch_archived_text(None), _patch_ensure() as mock_ensure, \
         patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_ensure.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == ["bill_summary"]
    assert bill_result["error"] is None


@pytest.mark.asyncio
async def test_ensure_not_called_when_bill_is_already_fully_covered():
    """A bill this run skips for any other ordinary reason (already fully
    covered, nothing needs dispatch, org research not requested) must never
    trigger `ensure` either -- version is never even resolved in that case,
    confirming this gate's placement doesn't create rows for bills the
    pipeline was never going to process anyway."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(
        {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "complete"}}}
    ), _patch_archived_text("the actual bill text"), _patch_ensure() as mock_ensure:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=False,
        )

    mock_ensure.assert_not_awaited()
    assert result["results"][0]["artifacts_skipped_present"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_ensure_not_called_when_version_identity_fails():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(None), \
         _patch_archived_text("the actual bill text"), _patch_ensure() as mock_ensure, \
         _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_ensure.assert_not_awaited()
    assert result["results"][0]["artifacts_failed"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_ensure_never_called_under_dry_run():
    """dry_run's whole contract is zero side effects -- creating a real Bill
    row is a side effect, so `ensure` must be skipped under dry_run exactly
    like every other dispatch already is, even when archived text exists."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
         _patch_archived_text("the actual bill text"), _patch_ensure() as mock_ensure, \
         _patch_org_status({"has_rows": False, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10, dry_run=True,
            include_concept_statements=False,
        )

    mock_ensure.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_ensure_failure_prevents_dispatch_and_org_research_with_its_own_error_category():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
         _patch_archived_text("the actual bill text"), \
         _patch_ensure(side_effect=BrokerClientError("broker down")), \
         patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact, patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_organization_positions",
        new=AsyncMock(),
    ) as mock_org, _patch_org_status({"has_rows": False, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_artifact.assert_not_awaited()
    mock_org.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["error"] == "ensure_failed: broker down"
    # Distinct from artifacts_failed -- a systemic ensure failure (the
    # broker endpoint down) must read differently from a per-artifact-type
    # dispatch failure.
    assert bill_result["artifacts_failed"] == []
    assert bill_result["artifacts_generated"] == []


# --- org research ------------------------------------------------------

@pytest.mark.asyncio
async def test_org_research_skipped_when_already_researched():
    with _patch_lister([_CANDIDATE]), _patch_coverage(
        {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "complete"}}}
    ), _patch_org_status({"has_rows": True, "row_count": 7}), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_organization_positions",
        new=AsyncMock(),
    ) as mock_org:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_org.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["org_research_dispatched"] is False
    assert bill_result["org_research_skipped_reason"] == "already_researched"


@pytest.mark.asyncio
async def test_org_research_dispatched_when_not_yet_researched():
    with _patch_lister([_CANDIDATE]), _patch_coverage(
        {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "complete"}}}
    ), _patch_version(), _patch_org_status({"has_rows": False, "row_count": 0}), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_organization_positions",
        new=AsyncMock(return_value=[{"org_name": "Sierra Club", "outcome": "written"}]),
    ) as mock_org:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10,
            include_concept_statements=False,
        )

    mock_org.assert_awaited_once()
    call_kwargs = mock_org.await_args.kwargs
    assert call_kwargs["bill_title"] == _VERSION_IDENTITY["bill_title"]
    bill_result = result["results"][0]
    assert bill_result["org_research_dispatched"] is True
    assert bill_result["org_research_skipped_reason"] is None


@pytest.mark.asyncio
async def test_org_research_not_requested_is_never_checked():
    with _patch_lister([_CANDIDATE]), _patch_coverage(
        {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "complete"}}}
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_organization_positions_status",
        new=AsyncMock(),
    ) as mock_status:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=False,
        )

    mock_status.assert_not_awaited()
    assert result["results"][0]["org_research_dispatched"] is False
    assert result["results"][0]["org_research_skipped_reason"] is None


# --- concept statements (SYNC-31) ---------------------------------------

@pytest.mark.asyncio
async def test_concept_statements_not_requested_is_never_checked():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_concept_statement_set",
        new=AsyncMock(),
    ) as mock_get_concept_set:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=False,
        )

    mock_get_concept_set.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["concept_statements_dispatched"] is False
    assert bill_result["concept_statements_skipped_reason"] is None


@pytest.mark.asyncio
async def test_concept_statements_skipped_when_already_published():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
         _patch_concept_set({"id": 7, "status": "published"}), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.dispatch_and_store_concept_statements",
        new=AsyncMock(),
    ) as mock_dispatch:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=True,
        )

    mock_dispatch.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["concept_statements_dispatched"] is False
    assert bill_result["concept_statements_skipped_reason"] == "already_published"


@pytest.mark.asyncio
async def test_concept_statements_dispatched_when_not_yet_published():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
         _patch_concept_set(None), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value={"id": 9, "status": "pending"}),
    ) as mock_dispatch:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=True,
        )

    mock_dispatch.assert_awaited_once()
    call_kwargs = mock_dispatch.await_args.kwargs
    assert call_kwargs["gov_id"] == "SJR 2F"
    assert call_kwargs["bill_openstates_id"] == "a3afb726-0000-0000-0000-000000000001"
    assert call_kwargs["bill_source"] == _CANDIDATE["live_url_fallback"]
    bill_result = result["results"][0]
    assert bill_result["concept_statements_dispatched"] is True
    assert bill_result["concept_statements_skipped_reason"] is None


_ALREADY_COVERED = {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "complete"}}}


@pytest.mark.asyncio
async def test_concept_statements_no_row_created_is_not_a_failure():
    """insufficient_information (or no archived text at all) means
    dispatch_and_store_concept_statements returns None -- there is no
    "failed" ConceptStatementSet status to record, unlike every
    BillArtifact-backed artifact_type, so this must never land in
    artifacts_failed."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(_ALREADY_COVERED), \
         _patch_concept_set(None), patch(
        "ddp_sync.pipelines.session_pipeline_runner.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value=None),
    ):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=True,
        )

    bill_result = result["results"][0]
    assert bill_result["concept_statements_dispatched"] is False
    assert bill_result["concept_statements_skipped_reason"] == "nothing_to_publish"
    assert bill_result["artifacts_failed"] == []


@pytest.mark.asyncio
async def test_concept_statements_dispatch_failure_is_isolated():
    with _patch_lister([_CANDIDATE]), _patch_coverage(_ALREADY_COVERED), \
         _patch_concept_set(None), patch(
        "ddp_sync.pipelines.session_pipeline_runner.dispatch_and_store_concept_statements",
        new=AsyncMock(side_effect=Exception("legbot dispatch failed")),
    ):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=True,
        )

    bill_result = result["results"][0]
    assert bill_result["concept_statements_dispatched"] is False
    assert "dispatch_failed" in bill_result["concept_statements_skipped_reason"]
    assert bill_result["error"] is None


@pytest.mark.asyncio
async def test_concept_statements_status_check_failure_is_isolated():
    with _patch_lister([_CANDIDATE]), _patch_coverage(_ALREADY_COVERED), \
         _patch_concept_set(side_effect=BrokerClientError("unreachable")):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=True,
        )

    bill_result = result["results"][0]
    assert bill_result["concept_statements_dispatched"] is False
    assert "status_check_failed" in bill_result["concept_statements_skipped_reason"]


@pytest.mark.asyncio
async def test_concept_statements_dry_run_dispatches_nothing():
    with _patch_lister([_CANDIDATE]), _patch_coverage(_ALREADY_COVERED), \
         _patch_concept_set(None), patch(
        "ddp_sync.pipelines.session_pipeline_runner.dispatch_and_store_concept_statements",
        new=AsyncMock(),
    ) as mock_dispatch:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=True, dry_run=True,
        )

    mock_dispatch.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["concept_statements_dispatched"] is True


@pytest.mark.asyncio
async def test_concept_statements_does_not_need_version_identity():
    """Unlike every BillArtifact-backed type/org research, ConceptStatementSet
    has no BillVersion FK at all -- dispatch must succeed even when
    get_current_version_identity resolves nothing (here, bill_summary's own
    dispatch fails for exactly that reason, while concept_statements
    dispatches successfully regardless)."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(None), \
         _patch_concept_set(None), patch(
        "ddp_sync.pipelines.session_pipeline_runner.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value={"id": 9}),
    ) as mock_dispatch:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=True,
        )

    mock_dispatch.assert_awaited_once()
    bill_result = result["results"][0]
    assert bill_result["artifacts_failed"] == ["bill_summary"]
    assert bill_result["concept_statements_dispatched"] is True


@pytest.mark.asyncio
async def test_concept_statements_broker_override_threads_through():
    with _patch_lister([_CANDIDATE]), _patch_coverage(_ALREADY_COVERED), patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_concept_statement_set",
        new=AsyncMock(return_value=None),
    ) as mock_get_concept_set, patch(
        "ddp_sync.pipelines.session_pipeline_runner.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value={"id": 9}),
    ) as mock_dispatch:
        await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10,
            include_concept_statements=True,
            broker_api_base="http://dev-broker:8080",
            broker_api_token="dev-token",
        )

    assert mock_get_concept_set.await_args.kwargs["broker_api_base"] == "http://dev-broker:8080"
    assert mock_get_concept_set.await_args.kwargs["broker_api_token"] == "dev-token"
    assert mock_dispatch.await_args.kwargs["broker_api_base"] == "http://dev-broker:8080"
    assert mock_dispatch.await_args.kwargs["broker_api_token"] == "dev-token"


# --- run_scheduled_session_pipeline (SYNC-9 scheduler wrapper) -------------

_VALID_BATCH_CONFIG = {
    "jurisdiction_iso2": "FL",
    "session_code": "2026F",
    "artifact_types": ["bill_summary", "bill_pros_cons"],
    "limit": 10,
}


@pytest.mark.asyncio
async def test_scheduled_wrapper_maps_full_config_to_run_legbot_pipeline():
    config = dict(_VALID_BATCH_CONFIG, include_org_research=True, dry_run=True)
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 2}),
    ) as mock_run:
        result = await run_scheduled_session_pipeline(config)

    mock_run.assert_awaited_once_with(
        "FL", "2026F", ["bill_summary", "bill_pros_cons"], True, 10,
        include_concept_statements=False, dry_run=True,
    )
    assert result == {"bills_considered": 2}


@pytest.mark.asyncio
async def test_scheduled_wrapper_defaults_org_research_and_dry_run_when_absent():
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={}),
    ) as mock_run:
        await run_scheduled_session_pipeline(_VALID_BATCH_CONFIG)

    mock_run.assert_awaited_once_with(
        "FL", "2026F", ["bill_summary", "bill_pros_cons"], False, 10,
        include_concept_statements=False, dry_run=False,
    )


@pytest.mark.asyncio
async def test_scheduled_wrapper_maps_include_concept_statements_when_present():
    config = dict(_VALID_BATCH_CONFIG, include_concept_statements=True)
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={}),
    ) as mock_run:
        await run_scheduled_session_pipeline(config)

    mock_run.assert_awaited_once_with(
        "FL", "2026F", ["bill_summary", "bill_pros_cons"], False, 10,
        include_concept_statements=True, dry_run=False,
    )


@pytest.mark.asyncio
async def test_scheduled_wrapper_missing_required_key_returns_invalid_config():
    config = {k: v for k, v in _VALID_BATCH_CONFIG.items() if k != "artifact_types"}
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        result = await run_scheduled_session_pipeline(config)

    mock_run.assert_not_awaited()
    assert result["success"] is False
    assert result["error"] == "invalid_config"
    assert "artifact_types" in result["missing_keys"]


@pytest.mark.asyncio
async def test_scheduled_wrapper_zero_limit_treated_as_missing():
    config = dict(_VALID_BATCH_CONFIG, limit=0)
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        result = await run_scheduled_session_pipeline(config)

    mock_run.assert_not_awaited()
    assert result["error"] == "invalid_config"
    assert "limit" in result["missing_keys"]


@pytest.mark.asyncio
async def test_scheduled_wrapper_value_error_from_pipeline_is_caught():
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(side_effect=ValueError("Unrecognized artifact_types: ['bogus']")),
    ):
        result = await run_scheduled_session_pipeline(_VALID_BATCH_CONFIG)

    assert result["success"] is False
    assert result["error"] == "invalid_config"
    assert "Unrecognized artifact_types" in result["detail"]


# --- run_single_bill_full (SYNC-15) ----------------------------------------
#
# The single-bill counterpart to run_legbot_pipeline -- one caller-specified
# bill instead of a jurisdiction/session-wide candidate listing. Reuses
# _process_bill directly, so its coverage-skip/dispatch/failure-isolation
# behavior is already covered above via run_legbot_pipeline's own tests;
# these tests focus on what's actually new: no candidate listing, the
# artifact_types=None default-to-all behavior, required-field validation,
# and broker_api_base/token threading.

_SINGLE_BILL_KWARGS = dict(
    bill_openstates_id=_CANDIDATE["bill_openstates_id"],
    jurisdiction_iso2="fl",
    session_code="2026F",
    gov_id=_CANDIDATE["gov_id"],
    bill_source=_CANDIDATE["live_url_fallback"],
    include_org_research=False,
    include_concept_statements=False,
)


@pytest.mark.asyncio
async def test_single_bill_rejects_empty_bill_openstates_id():
    with pytest.raises(ValueError, match="bill_openstates_id"):
        await run_single_bill_full(**{**_SINGLE_BILL_KWARGS, "bill_openstates_id": ""}, artifact_types=["bill_summary"])


@pytest.mark.asyncio
async def test_single_bill_rejects_empty_jurisdiction():
    with pytest.raises(ValueError, match="jurisdiction_iso2"):
        await run_single_bill_full(**{**_SINGLE_BILL_KWARGS, "jurisdiction_iso2": ""}, artifact_types=["bill_summary"])


@pytest.mark.asyncio
async def test_single_bill_rejects_empty_session_code():
    with pytest.raises(ValueError, match="session_code"):
        await run_single_bill_full(**{**_SINGLE_BILL_KWARGS, "session_code": ""}, artifact_types=["bill_summary"])


@pytest.mark.asyncio
async def test_single_bill_rejects_empty_gov_id():
    with pytest.raises(ValueError, match="gov_id"):
        await run_single_bill_full(**{**_SINGLE_BILL_KWARGS, "gov_id": ""}, artifact_types=["bill_summary"])


@pytest.mark.asyncio
async def test_single_bill_rejects_empty_bill_source():
    with pytest.raises(ValueError, match="bill_source"):
        await run_single_bill_full(**{**_SINGLE_BILL_KWARGS, "bill_source": ""}, artifact_types=["bill_summary"])


@pytest.mark.asyncio
async def test_single_bill_rejects_empty_artifact_types_list():
    """An explicit empty list is still rejected -- only None means 'all'."""
    with pytest.raises(ValueError, match="artifact_types"):
        await run_single_bill_full(**_SINGLE_BILL_KWARGS, artifact_types=[])


@pytest.mark.asyncio
async def test_single_bill_rejects_unrecognized_artifact_type():
    with pytest.raises(ValueError, match="Unrecognized"):
        await run_single_bill_full(**_SINGLE_BILL_KWARGS, artifact_types=["not_a_real_type"])


@pytest.mark.asyncio
async def test_single_bill_none_artifact_types_defaults_to_all_8():
    """The one real default in this module -- omitting artifact_types (None)
    means 'run everything', which is this function's whole reason to
    exist."""
    with _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_changelog",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ):
        result = await run_single_bill_full(**_SINGLE_BILL_KWARGS, artifact_types=None, dry_run=True)

    assert set(result["artifacts_generated"]) == ALL_ARTIFACT_TYPES


@pytest.mark.asyncio
async def test_single_bill_never_lists_candidates():
    """The defining difference from run_legbot_pipeline: no
    jurisdiction/session-wide listing call at all -- the caller already
    knows exactly which bill they want."""
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.list_current_session_bill_candidates",
        new=AsyncMock(),
    ) as mock_lister, _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ):
        await run_single_bill_full(**_SINGLE_BILL_KWARGS, artifact_types=["bill_summary"])

    mock_lister.assert_not_awaited()


@pytest.mark.asyncio
async def test_single_bill_dry_run_dispatches_nothing():
    with _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact:
        result = await run_single_bill_full(
            **_SINGLE_BILL_KWARGS, artifact_types=["bill_summary"], dry_run=True
        )

    mock_artifact.assert_not_awaited()
    assert result["artifacts_generated"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_single_bill_already_present_artifact_is_skipped_not_regenerated():
    """This is not a force-regenerate mode -- reuses _process_bill's own
    coverage-check skip logic unchanged."""
    with _patch_coverage(
        {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "complete"}}}
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact:
        result = await run_single_bill_full(**_SINGLE_BILL_KWARGS, artifact_types=["bill_summary"])

    mock_artifact.assert_not_awaited()
    assert result["artifacts_skipped_present"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_single_bill_includes_org_research_when_requested():
    with _patch_coverage(None), _patch_version(), _patch_org_status(
        {"has_rows": False, "row_count": 0}
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_organization_positions",
        new=AsyncMock(return_value=[{"org_name": "Sierra Club", "outcome": "written"}]),
    ) as mock_org:
        result = await run_single_bill_full(
            **{**_SINGLE_BILL_KWARGS, "include_org_research": True},
            artifact_types=["bill_summary"],
        )

    mock_org.assert_awaited_once()
    assert result["org_research_dispatched"] is True


@pytest.mark.asyncio
async def test_single_bill_includes_concept_statements_when_requested():
    with _patch_coverage(
        {"bill_version_id": 2, "artifacts": {"bill_summary": {"status": "complete"}}}
    ), _patch_concept_set(None), patch(
        "ddp_sync.pipelines.session_pipeline_runner.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value={"id": 9}),
    ) as mock_dispatch:
        result = await run_single_bill_full(
            **{**_SINGLE_BILL_KWARGS, "include_concept_statements": True},
            artifact_types=["bill_summary"],
        )

    mock_dispatch.assert_awaited_once()
    assert result["concept_statements_dispatched"] is True


@pytest.mark.asyncio
async def test_single_bill_result_includes_run_id():
    with _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ):
        result = await run_single_bill_full(**_SINGLE_BILL_KWARGS, artifact_types=["bill_summary"])

    assert result["run_id"]
    assert isinstance(result["run_id"], str)


@pytest.mark.asyncio
async def test_single_bill_threads_broker_override_to_coverage_check_and_dispatch():
    """The whole point of adding broker_api_base/token here -- a dev-tagged
    caller's coverage check and its dispatch must land on the same broker
    instance, not the shared default."""
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_artifacts",
        new=AsyncMock(return_value=None),
    ) as mock_coverage, _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "status": "complete"}),
    ) as mock_artifact:
        await run_single_bill_full(
            **_SINGLE_BILL_KWARGS,
            artifact_types=["bill_summary"],
            broker_api_base="http://dev-broker:8080",
            broker_api_token="dev-token",
        )

    assert mock_coverage.await_args.kwargs["broker_api_base"] == "http://dev-broker:8080"
    assert mock_coverage.await_args.kwargs["broker_api_token"] == "dev-token"
    assert mock_artifact.await_args.kwargs["broker_api_base"] == "http://dev-broker:8080"
    assert mock_artifact.await_args.kwargs["broker_api_token"] == "dev-token"


@pytest.mark.asyncio
async def test_single_bill_broker_override_defaults_to_none_preserving_batch_behavior():
    """run_legbot_pipeline's own existing caller (SYNC-9) never passes these
    -- confirm the default stays None, i.e. unchanged behavior, when a
    caller doesn't opt in."""
    import inspect
    sig = inspect.signature(run_legbot_pipeline)
    assert sig.parameters["broker_api_base"].default is None
    assert sig.parameters["broker_api_token"].default is None


# --- bounded concurrency (2026-08-18) ---------------------------------------
#
# ddp-agents' AGENTS-33/34 shipped a demand-based, memory-gated MLX-LM/
# MLX-VLM instance pool specifically so CAMS can scale up under real
# concurrent load -- but a live run against FL's 2026F session confirmed
# run_legbot_pipeline drove exactly one _process_bill call at a time no
# matter how many bills or how much spare memory existed, so that pool could
# never actually be exercised by this real caller. These tests use a
# tracking coverage-check stub (the first await inside _process_bill) rather
# than trying to patch _process_bill itself, since it's a closure-captured
# call inside run_legbot_pipeline's own local _process_bill_bounded helper.

def _make_candidates(n: int) -> list[dict]:
    return [
        dict(_CANDIDATE, gov_id=f"HB {i}", bill_openstates_id=f"id-{i}")
        for i in range(n)
    ]


def _patch_settings(concurrency: int):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_settings",
        return_value=SimpleNamespace(session_pipeline_concurrency=concurrency),
    )


@pytest.mark.asyncio
async def test_bills_are_processed_with_real_concurrency_not_strictly_one_at_a_time():
    candidates = _make_candidates(4)
    in_flight = {"count": 0}
    max_seen = {"value": 0}

    async def _tracking_coverage(*, jurisdiction, session_code, gov_id, broker_api_base=None, broker_api_token=None):
        in_flight["count"] += 1
        max_seen["value"] = max(max_seen["value"], in_flight["count"])
        await asyncio.sleep(0.05)
        in_flight["count"] -= 1
        return None

    with _patch_lister(candidates), patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_artifacts",
        new=AsyncMock(side_effect=_tracking_coverage),
    ), _patch_version(), _patch_settings(4):
        await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10, dry_run=True,
            include_concept_statements=False,
        )

    assert max_seen["value"] > 1


@pytest.mark.asyncio
async def test_concurrency_is_capped_at_session_pipeline_concurrency():
    """6 candidates against a cap of 2 must never let more than 2
    _process_bill calls have their own coverage check in flight at once --
    proving the semaphore is a real cap, not just decoration."""
    candidates = _make_candidates(6)
    in_flight = {"count": 0}
    max_seen = {"value": 0}

    async def _tracking_coverage(*, jurisdiction, session_code, gov_id, broker_api_base=None, broker_api_token=None):
        in_flight["count"] += 1
        max_seen["value"] = max(max_seen["value"], in_flight["count"])
        await asyncio.sleep(0.05)
        in_flight["count"] -= 1
        return None

    with _patch_lister(candidates), patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_artifacts",
        new=AsyncMock(side_effect=_tracking_coverage),
    ), _patch_version(), _patch_settings(2):
        await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10, dry_run=True,
            include_concept_statements=False,
        )

    assert max_seen["value"] == 2


@pytest.mark.asyncio
async def test_results_preserve_candidate_order_even_when_completion_order_differs():
    """asyncio.gather preserves input order regardless of completion order --
    give the FIRST candidate the LONGEST delay and the LAST candidate no
    delay at all, so a bug that returned results in completion order instead
    of candidate order would show up as a reversed list here."""
    candidates = _make_candidates(4)
    delays = {"HB 0": 0.09, "HB 1": 0.06, "HB 2": 0.03, "HB 3": 0.0}

    async def _staggered_coverage(*, jurisdiction, session_code, gov_id, broker_api_base=None, broker_api_token=None):
        await asyncio.sleep(delays[gov_id])
        return None

    with _patch_lister(candidates), patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_artifacts",
        new=AsyncMock(side_effect=_staggered_coverage),
    ), _patch_version(), _patch_settings(4):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10, dry_run=True,
            include_concept_statements=False,
        )

    assert [r["gov_id"] for r in result["results"]] == ["HB 0", "HB 1", "HB 2", "HB 3"]


@pytest.mark.asyncio
async def test_concurrency_of_one_is_effectively_sequential():
    """A cap of 1 is the pre-2026-08-18 behavior this change replaced --
    confirm it still works (falls back to strictly sequential) rather than
    the semaphore silently becoming a no-op at the boundary value."""
    candidates = _make_candidates(3)
    in_flight = {"count": 0}
    max_seen = {"value": 0}

    async def _tracking_coverage(*, jurisdiction, session_code, gov_id, broker_api_base=None, broker_api_token=None):
        in_flight["count"] += 1
        max_seen["value"] = max(max_seen["value"], in_flight["count"])
        await asyncio.sleep(0.01)
        in_flight["count"] -= 1
        return None

    with _patch_lister(candidates), patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_bill_artifacts",
        new=AsyncMock(side_effect=_tracking_coverage),
    ), _patch_version(), _patch_settings(1):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=10, dry_run=True,
            include_concept_statements=False,
        )

    assert max_seen["value"] == 1
    assert len(result["results"]) == 3


def test_session_pipeline_concurrency_defaults_to_one_not_four():
    """AGENTS-37 (2026-08-18): the first real dispatch under this setting's
    original default of 4 found LEGBOT_MLX_MAX_INSTANCES configured at 3
    (oversubscribing the pool from the first request) AND real concurrent
    MLX-LM throughput on that hardware degrading 20-50x under 3-way
    contention rather than parallelizing -- confirmed via dev ddp-broker-py
    itself landing zero new BillArtifact rows from that run. The safe
    out-of-the-box default is 1 (genuinely sequential, the pre-2026-08-18
    behavior) until real concurrent MLX-LM throughput is actually
    benchmarked and shown safe -- operators can still opt into a higher
    value via SESSION_PIPELINE_CONCURRENCY once that validation happens.
    """
    assert SyncSettings().session_pipeline_concurrency == 1


# --- SYNC-37: organisation research runs alongside the artifact set ------

def _patch_org_dispatch(side_effect=None, return_value=None):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner"
        ".generate_and_store_bill_organization_positions",
        new=AsyncMock(side_effect=side_effect, return_value=return_value),
    )


def _patch_artifact(side_effect=None, return_value=None):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(
            side_effect=side_effect,
            return_value=return_value if return_value is not None
            else {"id": 1, "status": "complete"},
        ),
    )


class TestConcurrentOrgResearch:
    """Organisation research used to sit between a bill's artifacts and its
    concept statements, blocking both.

    Traced live on the WA 2025-2026 run of 2026-08-27: eight artifacts five
    seconds apart, every one a warm cache hit, then ~45 seconds of nothing
    while the bill waited on a network call, then concept_statements paying a
    full re-read because the idle MLX worker had -- correctly -- been given to
    another bill. Six of seven full-artifact bills paid 2-3 prefills.
    """

    @pytest.mark.asyncio
    async def test_org_research_starts_before_the_artifact_loop_finishes(self):
        """The point of the ticket.

        A test that merely observed both finishing would pass on the old
        sequential code too, so the artifact dispatch below refuses to return
        until org research has demonstrably begun. On the old code this can
        never happen, and wait_for turns that into a failure rather than a
        hung suite.
        """
        org_started = asyncio.Event()

        async def _org(**kwargs):
            org_started.set()
            return None

        async def _artifact(**kwargs):
            await asyncio.wait_for(org_started.wait(), timeout=2)
            return {"id": 1, "status": "complete"}

        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_artifact(side_effect=_artifact), \
             _patch_org_status({"has_rows": False, "row_count": 0}), \
             _patch_org_dispatch(side_effect=_org):
            result = await run_legbot_pipeline(
                "fl", "2026F", ["bill_summary"], True, limit=10,
                include_concept_statements=False,
            )

        bill_result = result["results"][0]
        assert bill_result["artifacts_generated"] == ["bill_summary"]
        assert bill_result["org_research_dispatched"] is True

    @pytest.mark.asyncio
    async def test_the_result_dict_keeps_its_shape_on_every_org_outcome(self):
        """AC3. Those three fields are assembled by a task now instead of
        inline, and downstream readers get them by name."""
        # Every branch _run_org_research can return through, not only the
        # common ones -- /pm-review's point being that a task assembling
        # these fields is only equivalent to inline assignment if *all* of
        # them still come out the same.
        cases = [
            ("already_researched",
             {"has_rows": True, "row_count": 3}, None, None, False,
             {"org_research_dispatched": False,
              "org_research_skipped_reason": "already_researched",
              "org_research_duration_seconds": None}),
            ("dispatched",
             {"has_rows": False, "row_count": 0}, None, None, False,
             {"org_research_dispatched": True, "org_research_skipped_reason": None}),
            ("dispatch_failed",
             {"has_rows": False, "row_count": 0}, RuntimeError("upstream exploded"),
             None, False,
             {"org_research_dispatched": False}),
            ("status_check_failed",
             BrokerClientError("broker down"), None, None, False,
             {"org_research_dispatched": False,
              "org_research_duration_seconds": None}),
            ("no_current_version_resolved",
             {"has_rows": False, "row_count": 0}, None, None, True,
             {"org_research_dispatched": False,
              "org_research_skipped_reason": "no_current_version_resolved",
              "org_research_duration_seconds": None}),
        ]
        for label, org_status, org_exc, _unused, no_version, expected in cases:
            status_patch = (
                patch("ddp_sync.pipelines.session_pipeline_runner"
                      ".get_bill_organization_positions_status",
                      new=AsyncMock(side_effect=org_status))
                if isinstance(org_status, Exception) else _patch_org_status(org_status)
            )
            with _patch_lister([_CANDIDATE]), _patch_coverage(None), \
                 _patch_version(None if no_version else _VERSION_IDENTITY), \
                 _patch_artifact(), status_patch, \
                 _patch_org_dispatch(side_effect=org_exc):
                result = await run_legbot_pipeline(
                    "fl", "2026F", ["bill_summary"], True, limit=10,
                    include_concept_statements=False,
                )

            bill_result = result["results"][0]
            for key, value in expected.items():
                assert bill_result[key] == value, f"{key} in the {label} case"
            if label == "dispatch_failed":
                assert "upstream exploded" in bill_result["org_research_skipped_reason"]
            if label == "status_check_failed":
                assert "broker down" in bill_result["org_research_skipped_reason"]
            # A bill is never killed by its own organisation research.
            assert bill_result["error"] is None, f"error set in the {label} case"

    @pytest.mark.asyncio
    async def test_dry_run_reports_dispatched_without_dispatching(self):
        """The dry_run branch returns before touching anything real, and it
        is the one branch that reports dispatched=True having done nothing."""
        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_org_status({"has_rows": False, "row_count": 0}), \
             _patch_org_dispatch() as mock_org:
            result = await run_legbot_pipeline(
                "fl", "2026F", ["bill_summary"], True, limit=10,
                include_concept_statements=False, dry_run=True,
            )

        mock_org.assert_not_awaited()
        bill_result = result["results"][0]
        assert bill_result["org_research_dispatched"] is True
        assert bill_result["org_research_skipped_reason"] is None
        assert bill_result["org_research_duration_seconds"] is None

    @pytest.mark.asyncio
    async def test_the_org_task_is_awaited_even_when_the_rest_of_the_bill_raises(self):
        """AC2, and the real hazard in making this a task.

        A task nobody awaits becomes an "exception was never retrieved"
        warning at collection time and a silently discarded result. Here the
        concept-statement step raises something its own handler does not
        catch, so the exception leaves _process_bill entirely -- and the org
        task still has to be cleaned up on the way out.
        """
        observed = []
        org_in_flight = asyncio.Event()

        async def _org(**kwargs):
            org_in_flight.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                observed.append("cancelled")
                raise

        async def _artifact(**kwargs):
            # Makes the interleaving deterministic: the org task is genuinely
            # in flight, not merely created, by the time the bill blows up.
            await asyncio.wait_for(org_in_flight.wait(), timeout=2)
            return {"id": 1, "status": "complete"}

        before = asyncio.all_tasks()
        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_artifact(side_effect=_artifact), \
             _patch_org_status({"has_rows": False, "row_count": 0}), \
             _patch_org_dispatch(side_effect=_org), \
             _patch_concept_set(side_effect=RuntimeError("not a BrokerClientError")):
            with pytest.raises(RuntimeError, match="not a BrokerClientError"):
                await run_legbot_pipeline(
                    "fl", "2026F", ["bill_summary"], True, limit=10,
                    include_concept_statements=True,
                )

        assert observed == ["cancelled"], "the org task outlived its bill"
        assert not (asyncio.all_tasks() - before), "a task was left running"

    @pytest.mark.asyncio
    async def test_cancelling_the_run_mid_bill_does_not_orphan_the_org_task(self):
        """AC2's other half. A run has been stopped mid-bill for real, twice,
        under memory pressure."""
        observed = []
        artifact_running = asyncio.Event()

        async def _org(**kwargs):
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                observed.append("cancelled")
                raise

        async def _artifact(**kwargs):
            artifact_running.set()
            await asyncio.sleep(30)
            return {"id": 1, "status": "complete"}

        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_artifact(side_effect=_artifact), \
             _patch_org_status({"has_rows": False, "row_count": 0}), \
             _patch_org_dispatch(side_effect=_org):
            run = asyncio.create_task(run_legbot_pipeline(
                "fl", "2026F", ["bill_summary"], True, limit=10,
                include_concept_statements=False,
            ))
            await asyncio.wait_for(artifact_running.wait(), timeout=2)
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

        assert observed == ["cancelled"], "the org task survived the cancelled run"

    @pytest.mark.asyncio
    async def test_an_already_failed_org_task_still_has_its_exception_retrieved(self):
        """The case /pm-review caught, and the subtlest one here.

        `_run_org_research` catches BrokerClientError around its status
        lookup and broad Exception around the dispatch -- but an unexpected
        error from the status lookup itself (a transport error, a timeout,
        a malformed response) escapes. That was true of the old inline code
        too and still fails the bill the same way; what is new is that the
        failure now lands in a *task*.

        A task that has already finished with an exception is `done()`. A
        cleanup that only touches pending tasks skips it, nobody ever
        retrieves the exception, and asyncio reports "Task exception was
        never retrieved" at collection time with the result thrown away.
        """
        org_failed = asyncio.Event()

        async def _status(**kwargs):
            org_failed.set()
            raise TimeoutError("status lookup timed out")

        async def _artifact(**kwargs):
            # Guarantees the org task is already done-with-exception by the
            # time the bill's own failure happens.
            await asyncio.wait_for(org_failed.wait(), timeout=2)
            return {"id": 1, "status": "complete"}

        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_artifact(side_effect=_artifact), patch(
                 "ddp_sync.pipelines.session_pipeline_runner"
                 ".get_bill_organization_positions_status",
                 new=AsyncMock(side_effect=_status),
             ), _patch_concept_set(
                 side_effect=RuntimeError("the bill's own failure")
             ), patch(
                 "ddp_sync.pipelines.session_pipeline_runner.logger.warning"
             ) as mock_warning:
            # The bill's own failure is what propagates. The org task's
            # exception must not displace it.
            with pytest.raises(RuntimeError, match="the bill's own failure"):
                await run_legbot_pipeline(
                    "fl", "2026F", ["bill_summary"], True, limit=10,
                    include_concept_statements=True,
                )

        # ...and it must not vanish either. Retrieving the exception is what
        # this log line proves happened: it is emitted from the same `await`
        # that consumes the task, so it cannot be present unless the already-
        # done task was actually awaited.
        events = [c.args[0] for c in mock_warning.call_args_list if c.args]
        assert "session_pipeline_org_research_task_failed" in events, (
            f"the org task's exception was never retrieved; warnings: {events}"
        )

    @pytest.mark.asyncio
    async def test_nothing_is_started_when_org_research_is_not_requested(self):
        """include_org_research=False must not create a task to clean up."""
        status = AsyncMock()
        org = AsyncMock()
        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_artifact(), patch(
                 "ddp_sync.pipelines.session_pipeline_runner"
                 ".get_bill_organization_positions_status", new=status,
             ), patch(
                 "ddp_sync.pipelines.session_pipeline_runner"
                 ".generate_and_store_bill_organization_positions", new=org,
             ):
            result = await run_legbot_pipeline(
                "fl", "2026F", ["bill_summary"], False, limit=10,
                include_concept_statements=False,
            )

        status.assert_not_awaited()
        org.assert_not_awaited()
        assert result["results"][0]["org_research_dispatched"] is False


# --- SYNC-38: dispatch order is the pipeline's, not the caller's ---------

class TestCanonicalDispatchOrder:
    """Before this, ALL_ARTIFACT_TYPES was a frozenset and _process_bill
    dispatched in whatever order the caller's artifact_types list happened to
    use. That decided which artifact ran last, and therefore which bill's KV
    cache survived -- measured on VA 2026S1: concept_statements warm 3 times
    out of 20, against 90-100% for every type that ran before bill_changelog.
    """

    @staticmethod
    def _order_from(mock_artifact, mock_changelog):
        calls = [c.kwargs.get("artifact_type") for c in mock_artifact.await_args_list]
        # the changelog dispatches through its own function, so it carries no
        # artifact_type kwarg -- its position is what matters, and it is last
        # by construction if it was awaited at all.
        return calls, mock_changelog.await_count

    @pytest.mark.asyncio
    async def test_same_order_regardless_of_how_the_caller_sorts(self):
        """The point of the ticket."""
        seen = []
        for types in (
            ["bill_summary", "bill_changelog", "bill_topics", "bill_pros_cons"],
            ["bill_changelog", "bill_pros_cons", "bill_topics", "bill_summary"],
            ["bill_topics", "bill_summary", "bill_pros_cons", "bill_changelog"],
        ):
            with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
                 _patch_artifact(), _patch_org_status({"has_rows": True, "row_count": 1}), \
                 patch("ddp_sync.pipelines.session_pipeline_runner"
                       ".generate_and_store_bill_changelog",
                       new=AsyncMock(return_value={"id": 1, "status": "complete"})):
                from ddp_sync.pipelines import session_pipeline_runner as spr
                with patch.object(spr, "generate_and_store_bill_artifact",
                                  new=AsyncMock(return_value={"id": 1, "status": "complete"})) as ma:
                    await run_legbot_pipeline(
                        "fl", "2026F", types, True, limit=10,
                        include_concept_statements=False,
                    )
                    seen.append([c.kwargs.get("artifact_type") for c in ma.await_args_list])

        assert seen[0] == seen[1] == seen[2], f"order followed the caller: {seen}"
        assert seen[0] == ["bill_summary", "bill_pros_cons", "bill_topics"], seen[0]

    @pytest.mark.asyncio
    async def test_changelog_is_dispatched_after_concept_statements(self):
        """The actual cache fix.

        bill_changelog builds a two-input prompt and so takes its own cache
        key. Running it before concept statements evicts the bill's cache and
        makes them pay a full prefill -- which is what the run measured.
        """
        order = []
        async def _changelog(**kw):
            order.append("changelog"); return {"id": 1, "status": "complete"}
        async def _concepts(**kw):
            order.append("concepts"); return {"id": 1}

        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_artifact(), _patch_org_status({"has_rows": True, "row_count": 1}), \
             _patch_concept_set(None), \
             patch("ddp_sync.pipelines.session_pipeline_runner"
                   ".generate_and_store_bill_changelog", new=AsyncMock(side_effect=_changelog)), \
             patch("ddp_sync.pipelines.session_pipeline_runner"
                   ".dispatch_and_store_concept_statements", new=AsyncMock(side_effect=_concepts)):
            await run_legbot_pipeline(
                "fl", "2026F", ["bill_changelog", "bill_summary"], True, limit=10,
                include_concept_statements=True,
            )

        assert order == ["concepts", "changelog"], (
            f"changelog must run after concept statements, got {order}"
        )

    def test_the_order_and_the_validation_set_cannot_drift(self):
        """A type recognised but never ordered would silently never dispatch,
        and .index() would raise on a type ordered but not recognised."""
        from ddp_sync.pipelines.session_pipeline_runner import (
            ALL_ARTIFACT_TYPES, ARTIFACT_DISPATCH_ORDER,
        )
        assert set(ARTIFACT_DISPATCH_ORDER) == ALL_ARTIFACT_TYPES
        assert len(ARTIFACT_DISPATCH_ORDER) == len(ALL_ARTIFACT_TYPES), "duplicate entry"
        assert ARTIFACT_DISPATCH_ORDER[-1] == "bill_changelog"

    @pytest.mark.asyncio
    async def test_a_caller_omitting_changelog_still_works(self):
        """own_cache_key is empty then -- phase 2 must simply do nothing."""
        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_artifact(), _patch_org_status({"has_rows": True, "row_count": 1}):
            result = await run_legbot_pipeline(
                "fl", "2026F", ["bill_summary", "bill_topics"], True, limit=10,
                include_concept_statements=False,
            )
        assert result["results"][0]["artifacts_generated"] == ["bill_summary", "bill_topics"]

    @pytest.mark.asyncio
    async def test_a_contained_concept_failure_still_dispatches_changelog(self):
        """The failure-isolation question /pm-review raised.

        Moving changelog after the concept block couples the two. For every
        failure that block actually contains, phase 2 must still run. (The one
        escaping case ends the whole run -- see the comment on phase 2 for why
        that one does not get a try/finally.)
        """
        async def _run(concept_ctx, dispatch_ctx):
            ran = []
            async def _changelog(**kw):
                ran.append("changelog"); return {"id": 1, "status": "complete"}
            with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
                 _patch_artifact(), _patch_org_status({"has_rows": True, "row_count": 1}), \
                 concept_ctx, dispatch_ctx, \
                 patch("ddp_sync.pipelines.session_pipeline_runner"
                       ".generate_and_store_bill_changelog",
                       new=AsyncMock(side_effect=_changelog)):
                result = await run_legbot_pipeline(
                    "fl", "2026F", ["bill_changelog", "bill_summary"], True, limit=10,
                    include_concept_statements=True,
                )
            return ran, result["results"][0]

        # (a) the status check fails with the error the block catches
        ran, bill = await _run(
            _patch_concept_set(side_effect=BrokerClientError("broker down")),
            patch("ddp_sync.pipelines.session_pipeline_runner"
                  ".dispatch_and_store_concept_statements", new=AsyncMock()),
        )
        assert ran == ["changelog"], "status_check_failed skipped changelog"
        assert "status_check_failed" in bill["concept_statements_skipped_reason"]

        # (b) the dispatch itself raises -- caught by the block's broad except
        ran, bill = await _run(
            _patch_concept_set(None),
            patch("ddp_sync.pipelines.session_pipeline_runner"
                  ".dispatch_and_store_concept_statements",
                  new=AsyncMock(side_effect=RuntimeError("dispatch blew up"))),
        )
        assert ran == ["changelog"], "a failed concept dispatch skipped changelog"
        assert "dispatch_failed" in bill["concept_statements_skipped_reason"]

    @pytest.mark.asyncio
    async def test_the_full_interleaving_in_one_assertion(self):
        """The phase contract, end to end, in the order a reader cares about."""
        order = []
        async def _artifact(**kw):
            order.append(kw["artifact_type"]); return {"id": 1, "status": "complete"}
        async def _changelog(**kw):
            order.append("bill_changelog"); return {"id": 1, "status": "complete"}
        async def _concepts(**kw):
            order.append("concept_statements"); return {"id": 1}

        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_org_status({"has_rows": True, "row_count": 1}), _patch_concept_set(None), \
             patch("ddp_sync.pipelines.session_pipeline_runner"
                   ".generate_and_store_bill_artifact", new=AsyncMock(side_effect=_artifact)), \
             patch("ddp_sync.pipelines.session_pipeline_runner"
                   ".generate_and_store_bill_changelog", new=AsyncMock(side_effect=_changelog)), \
             patch("ddp_sync.pipelines.session_pipeline_runner"
                   ".dispatch_and_store_concept_statements", new=AsyncMock(side_effect=_concepts)):
            await run_legbot_pipeline(
                # deliberately worst-case caller order: changelog first
                "fl", "2026F",
                ["bill_changelog", "bill_topics", "bill_summary", "bill_impact_analysis"],
                True, limit=10, include_concept_statements=True,
            )

        assert order == [
            "bill_summary", "bill_impact_analysis", "bill_topics",   # phase 1
            "concept_statements",                                     # between
            "bill_changelog",                                         # phase 2
        ], order

    @pytest.mark.asyncio
    async def test_dry_run_and_missing_version_behave_as_before(self):
        """continue -> return inside the closure: each invocation is one loop
        iteration, so both early-exit branches must record what they always
        did and move on to the next type."""
        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
             _patch_artifact(), _patch_org_status({"has_rows": True, "row_count": 1}):
            dry = await run_legbot_pipeline(
                "fl", "2026F", ["bill_changelog", "bill_summary", "bill_topics"], True,
                limit=10, include_concept_statements=False, dry_run=True,
            )
        assert dry["results"][0]["artifacts_generated"] == [
            "bill_summary", "bill_topics", "bill_changelog"
        ], "dry_run must still record every type, in canonical order"

        with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(None), \
             _patch_artifact(), _patch_org_status({"has_rows": True, "row_count": 1}):
            noversion = await run_legbot_pipeline(
                "fl", "2026F", ["bill_changelog", "bill_summary"], True,
                limit=10, include_concept_statements=False,
            )
        r = noversion["results"][0]
        assert r["artifacts_generated"] == []
        assert sorted(r["artifacts_failed"]) == ["bill_changelog", "bill_summary"]
