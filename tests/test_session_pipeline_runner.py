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
    ALL_8_ARTIFACT_TYPES,
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
async def test_bill_topics_is_a_recognized_artifact_type():
    """SYNC-1: bill_topics must be dispatchable through this real caller,
    not just accepted by generate_and_store_bill_artifact directly -- the
    gap the first implementation attempt (PR #39) missed. dry_run=True
    proves ALL_8_ARTIFACT_TYPES's own recognition check passes without
    ever needing a real LegBot/broker call."""
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact:
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_topics"], False, limit=10, dry_run=True
        )

    mock_artifact.assert_not_awaited()
    bill_result = result["results"][0]
    assert bill_result["artifacts_generated"] == ["bill_topics"]


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


# --- SYNC-21: ensure_bill_exists gating (PLAN-local-openstates-migration.md §3.6) ---

@pytest.mark.asyncio
async def test_ensure_called_when_archived_text_exists_and_dispatch_proceeds():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(), \
         _patch_archived_text("the actual bill text"), _patch_ensure() as mock_ensure, \
         patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10
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
        new=AsyncMock(return_value={"id": 1}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        await run_legbot_pipeline("fl", "2026F", ["bill_summary"], True, limit=10)

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
        new=AsyncMock(return_value={"id": 1}),
    ), _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10
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
            "fl", "2026F", ["bill_summary"], False, limit=10
        )

    mock_ensure.assert_not_awaited()
    assert result["results"][0]["artifacts_skipped_present"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_ensure_not_called_when_version_identity_fails():
    with _patch_lister([_CANDIDATE]), _patch_coverage(None), _patch_version(None), \
         _patch_archived_text("the actual bill text"), _patch_ensure() as mock_ensure, \
         _patch_org_status({"has_rows": True, "row_count": 0}):
        result = await run_legbot_pipeline(
            "fl", "2026F", ["bill_summary"], True, limit=10
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
            "fl", "2026F", ["bill_summary"], True, limit=10, dry_run=True
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
            "fl", "2026F", ["bill_summary"], True, limit=10
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
        "FL", "2026F", ["bill_summary", "bill_pros_cons"], True, 10, dry_run=True
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
        "FL", "2026F", ["bill_summary", "bill_pros_cons"], False, 10, dry_run=False
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
        new=AsyncMock(return_value={"id": 1}),
    ), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_changelog",
        new=AsyncMock(return_value={"id": 1}),
    ):
        result = await run_single_bill_full(**_SINGLE_BILL_KWARGS, artifact_types=None, dry_run=True)

    assert set(result["artifacts_generated"]) == ALL_8_ARTIFACT_TYPES


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
        new=AsyncMock(return_value={"id": 1}),
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
        new=AsyncMock(return_value={"id": 1}),
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
async def test_single_bill_result_includes_run_id():
    with _patch_coverage(None), _patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1}),
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
        new=AsyncMock(return_value={"id": 1}),
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
            "fl", "2026F", ["bill_summary"], False, limit=10, dry_run=True
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
            "fl", "2026F", ["bill_summary"], False, limit=10, dry_run=True
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
            "fl", "2026F", ["bill_summary"], False, limit=10, dry_run=True
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
            "fl", "2026F", ["bill_summary"], False, limit=10, dry_run=True
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
