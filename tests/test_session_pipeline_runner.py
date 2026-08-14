"""Tests for the session-targeted batch runner (ddp-infra's
PLAN-bill-document-provenance.md, "Step 1, scoped version", approved
2026-08-01 after 3 rounds of /pm-review).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.session_pipeline_runner import run_legbot_pipeline
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


def _patch_version(result=_VERSION_IDENTITY):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_current_version_identity",
        new=AsyncMock(return_value=result),
    )


# --- input validation --------------------------------------------------

@pytest.mark.asyncio
async def test_rejects_empty_jurisdiction():
    with pytest.raises(ValueError, match="jurisdiction_iso2"):
        await run_legbot_pipeline("", "2026F", ["bill_summary"], False, 10)


@pytest.mark.asyncio
async def test_rejects_empty_session_code():
    with pytest.raises(ValueError, match="session_code"):
        await run_legbot_pipeline("fl", "", ["bill_summary"], False, 10)


@pytest.mark.asyncio
async def test_rejects_non_positive_limit():
    with pytest.raises(ValueError, match="limit"):
        await run_legbot_pipeline("fl", "2026F", ["bill_summary"], False, 0)


@pytest.mark.asyncio
async def test_rejects_empty_artifact_types():
    with pytest.raises(ValueError, match="artifact_types"):
        await run_legbot_pipeline("fl", "2026F", [], False, 10)


@pytest.mark.asyncio
async def test_rejects_unrecognized_artifact_type():
    with pytest.raises(ValueError, match="Unrecognized"):
        await run_legbot_pipeline("fl", "2026F", ["not_a_real_type"], False, 10)


@pytest.mark.asyncio
async def test_none_of_the_three_required_args_have_defaults():
    """Round 2 of /pm-review's own point: every cost-relevant argument is a
    conscious choice, not just artifact_types/include_org_research but
    limit too."""
    import inspect
    sig = inspect.signature(run_legbot_pipeline)
    for name in ("artifact_types", "include_org_research", "limit"):
        assert sig.parameters[name].default is inspect.Parameter.empty


# --- truncation ----------------------------------------------------------

@pytest.mark.asyncio
async def test_truncated_is_true_when_more_candidates_exist_than_limit():
    candidates = [dict(_CANDIDATE, gov_id=f"HB {n}") for n in range(3)]
    with _patch_lister(candidates), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1}),
    ):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=2
        )

    assert result["truncated"] is True
    assert result["bills_considered"] == 2
    assert result["bills_processed"] == 2


@pytest.mark.asyncio
async def test_truncated_is_false_when_exactly_limit_candidates_exist():
    candidates = [dict(_CANDIDATE, gov_id=f"HB {n}") for n in range(2)]
    with _patch_lister(candidates), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1}),
    ):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], False, limit=2
        )

    assert result["truncated"] is False
    assert result["bills_considered"] == 2


@pytest.mark.asyncio
async def test_lister_called_with_limit_plus_one():
    with _patch_lister([]) as mock_lister:
        await run_legbot_pipeline("fl", "2026F", ["bill_summary"], False, limit=5)

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
            "fl", "2026F", ["bill_summary", "bill_changelog"], True, limit=10, dry_run=True
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
            "fl", "2026F", ["bill_summary"], True, limit=10
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
            "fl", "2026F", ["bill_summary"], True, limit=10
        )

    mock_artifact.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["artifacts_skipped_failed_previously"] == ["bill_summary"]
    assert bill_result["artifacts_skipped_present"] == []


@pytest.mark.asyncio
async def test_missing_row_is_dispatched():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_artifact, _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10
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
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_changelog, _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_changelog"], True, limit=10
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
        new=AsyncMock(side_effect=[Exception("dispatch failed"), {"id": 2}]),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary", "bill_pros_cons"], True, limit=10
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
            "fl", "2026F", ["bill_changelog"], True, limit=10
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
        new=AsyncMock(return_value={"id": 1}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10
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
            "fl", "2026F", ["bill_summary"], True, limit=10
        )

    bill_result = result["results"][0]
    assert bill_result["artifacts_failed"] == ["bill_summary"]
    assert bill_result["error"] is None


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
            "fl", "2026F", ["bill_summary"], True, limit=10
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
            "fl", "2026F", ["bill_summary"], True, limit=10
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
            "fl", "2026F", ["bill_summary"], False, limit=10
        )

    mock_status.assert_not_awaited()
    assert result["results"][0]["org_research_dispatched"] is False
    assert result["results"][0]["org_research_skipped_reason"] is None
