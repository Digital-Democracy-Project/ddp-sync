"""Tests for the concept-statement dispatch pipeline (ddp-infra
PLAN-bill-concept-polling.md §0.4): wiring LegBot's concept_statements
question type to the new ConceptStatementSet create endpoint, plus the
scheduled batch job that decides which bills to run it against.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.bill_artifact_generation import (
    _ARTIFACT_TYPE_TO_QUESTION_TYPE,
    generate_and_store_bill_artifact,
)
from ddp_sync.pipelines.concept_statement_dispatch import (
    dispatch_and_store_concept_statements,
    run_concept_statement_batch_job,
)
from ddp_sync.services.broker_client import BrokerClientError
from ddp_sync.services.legbot_client import LegBotDispatchError

_COMMON_KWARGS = dict(
    gov_id="HB 123",
    bill_openstates_id="8d71a94e-0000-0000-0000-000000000001",
    jurisdiction_iso2="FL",
    session_code="2026",
    bill_source="https://flsenate.gov/Session/Bill/2026/123/BillText/Filed/PDF",
)


@pytest.fixture(autouse=True)
def no_archived_text_by_default():
    """Every test in this file gets the "not archived" fallback path by
    default -- matches test_bill_artifact_generation.py's own fixture.
    dispatch_and_store_concept_statements reuses _resolve_bill_source
    (and, through it, get_archived_bill_text) directly rather than
    reimplementing it, so patching it at its one real definition site
    (bill_artifact_generation) is what actually governs both callers.
    """
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_bill_text",
        new=AsyncMock(return_value=None),
    ) as mock_lookup:
        yield mock_lookup


# ---------------------------------------------------------------------------
# dispatch_and_store_concept_statements
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_happy_path_creates_row_via_new_endpoint():
    dispatch_result = {
        "answer": {
            "statements": ["The state should require X.", "The state should fund Y."],
            "insufficient_information": False,
        },
        "backend": "mlx",
    }
    created_row = {
        "id": 42,
        "gov_id": _COMMON_KWARGS["gov_id"],
        "jurisdiction_iso2": "FL",
        "session_code": "2026",
        "statements": dispatch_result["answer"]["statements"],
        "status": "pending",
        "generated_at": "2026-07-30T00:00:00Z",
    }
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
        new=AsyncMock(return_value=created_row),
    ) as mock_create:
        result = await dispatch_and_store_concept_statements(**_COMMON_KWARGS)

    assert result == created_row
    mock_dispatch.assert_awaited_once_with(_COMMON_KWARGS["bill_source"], "concept_statements")
    mock_create.assert_awaited_once_with(
        gov_id=_COMMON_KWARGS["gov_id"],
        jurisdiction_iso2="FL",
        session_code="2026",
        statements=dispatch_result["answer"]["statements"],
        source_document_url=_COMMON_KWARGS["bill_source"],
        model_name="mlx",
    )


@pytest.mark.asyncio
async def test_insufficient_information_no_create_call_and_returns_none():
    dispatch_result = {
        "answer": {"statements": [], "insufficient_information": True},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
        new=AsyncMock(),
    ) as mock_create:
        result = await dispatch_and_store_concept_statements(**_COMMON_KWARGS)

    assert result is None
    mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_archived_text_found_is_used_instead_of_live_url(no_archived_text_by_default):
    no_archived_text_by_default.return_value = "ARCHIVED FULL BILL TEXT"
    dispatch_result = {
        "answer": {"statements": ["A statement."], "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
        new=AsyncMock(return_value={"id": 1}),
    ):
        result = await dispatch_and_store_concept_statements(**_COMMON_KWARGS)

    assert result == {"id": 1}
    no_archived_text_by_default.assert_awaited_once_with(_COMMON_KWARGS["bill_openstates_id"])
    mock_dispatch.assert_awaited_once_with("ARCHIVED FULL BILL TEXT", "concept_statements")


@pytest.mark.asyncio
async def test_archived_text_not_found_falls_back_to_live_url(no_archived_text_by_default):
    dispatch_result = {
        "answer": {"statements": ["A statement."], "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
        new=AsyncMock(return_value={"id": 2}),
    ):
        result = await dispatch_and_store_concept_statements(**_COMMON_KWARGS)

    assert result == {"id": 2}
    no_archived_text_by_default.assert_awaited_once_with(_COMMON_KWARGS["bill_openstates_id"])
    mock_dispatch.assert_awaited_once_with(_COMMON_KWARGS["bill_source"], "concept_statements")


@pytest.mark.asyncio
async def test_legbot_dispatch_error_propagates():
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
        new=AsyncMock(side_effect=LegBotDispatchError("timed out")),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
        new=AsyncMock(),
    ) as mock_create:
        with pytest.raises(LegBotDispatchError):
            await dispatch_and_store_concept_statements(**_COMMON_KWARGS)

    mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_broker_client_error_propagates():
    dispatch_result = {
        "answer": {"statements": ["A statement."], "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
        new=AsyncMock(side_effect=BrokerClientError("broker rejected")),
    ):
        with pytest.raises(BrokerClientError):
            await dispatch_and_store_concept_statements(**_COMMON_KWARGS)


# ---------------------------------------------------------------------------
# run_concept_statement_batch_job
# ---------------------------------------------------------------------------

def _candidate(n: int) -> dict:
    return {
        "gov_id": f"gov-id-{n}",
        "bill_openstates_id": f"bill-openstates-id-{n}",
        "session_code": "2026",
        "live_url_fallback": f"https://example.com/bill-{n}.pdf",
    }


@pytest.mark.asyncio
async def test_batch_job_respects_max_bills_per_run():
    """10 candidates available, cap is 3 -- only 3 are considered/dispatched."""
    candidates = [_candidate(i) for i in range(10)]
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.list_current_session_bill_candidates",
        new=AsyncMock(return_value=candidates),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.get_concept_statement_set",
        new=AsyncMock(return_value=None),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_dispatch_store:
        result = await run_concept_statement_batch_job(
            {"max_bills_per_run": 3}, jurisdictions=["FL"],
        )

    assert result["considered"] == 3
    assert result["dispatched"] == 3
    assert result["created"] == 3
    assert mock_dispatch_store.await_count == 3


@pytest.mark.asyncio
async def test_batch_job_skips_bills_with_existing_published_set():
    candidates = [_candidate(1), _candidate(2)]

    async def _fake_existing(*, gov_id, jurisdiction_iso2, session_code):
        # Bill 1 already has a published set; bill 2 does not.
        return {"id": 99} if gov_id == "gov-id-1" else None

    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.list_current_session_bill_candidates",
        new=AsyncMock(return_value=candidates),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.get_concept_statement_set",
        new=AsyncMock(side_effect=_fake_existing),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_dispatch_store:
        result = await run_concept_statement_batch_job(
            {"max_bills_per_run": 10}, jurisdictions=["FL"],
        )

    assert result["considered"] == 2
    assert result["skipped_existing_published"] == 1
    assert result["dispatched"] == 1
    assert mock_dispatch_store.await_count == 1


@pytest.mark.asyncio
async def test_batch_job_counts_insufficient_information_separately_from_created():
    candidates = [_candidate(1), _candidate(2)]

    async def _fake_dispatch(*, gov_id, bill_openstates_id, jurisdiction_iso2, session_code, bill_source):
        return None if gov_id == "gov-id-1" else {"id": 5}

    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.list_current_session_bill_candidates",
        new=AsyncMock(return_value=candidates),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.get_concept_statement_set",
        new=AsyncMock(return_value=None),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_and_store_concept_statements",
        new=AsyncMock(side_effect=_fake_dispatch),
    ):
        result = await run_concept_statement_batch_job(
            {"max_bills_per_run": 10}, jurisdictions=["FL"],
        )

    assert result["created"] == 1
    assert result["insufficient_information"] == 1


@pytest.mark.asyncio
async def test_batch_job_records_errors_without_aborting_the_run():
    candidates = [_candidate(1), _candidate(2)]

    async def _fake_dispatch(*, gov_id, bill_openstates_id, jurisdiction_iso2, session_code, bill_source):
        if gov_id == "gov-id-1":
            raise LegBotDispatchError("timed out")
        return {"id": 6}

    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.list_current_session_bill_candidates",
        new=AsyncMock(return_value=candidates),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.get_concept_statement_set",
        new=AsyncMock(return_value=None),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_and_store_concept_statements",
        new=AsyncMock(side_effect=_fake_dispatch),
    ):
        result = await run_concept_statement_batch_job(
            {"max_bills_per_run": 10}, jurisdictions=["FL"],
        )

    assert result["failed"] == 1
    assert result["created"] == 1
    assert len(result["errors"]) == 1
    assert "gov-id-1" in result["errors"][0]


@pytest.mark.asyncio
async def test_batch_job_passes_gov_id_and_bill_openstates_id_as_distinct_values_regression():
    """Regression test for the 2026-07-30 live-testing bug: the batch job
    must forward both identities from each candidate as two separate,
    non-interchangeable arguments -- gov_id (short identifier, stored) and
    bill_openstates_id (UUID, archive-lookup only). Passing the UUID as
    gov_id made every real dispatch fail ConceptStatementSet.gov_id's
    max_length=20 check, 100% of the time."""
    candidates = [_candidate(1)]
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.list_current_session_bill_candidates",
        new=AsyncMock(return_value=candidates),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.get_concept_statement_set",
        new=AsyncMock(return_value=None),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_dispatch_store:
        await run_concept_statement_batch_job({"max_bills_per_run": 10}, jurisdictions=["FL"])

    call_kwargs = mock_dispatch_store.await_args.kwargs
    assert call_kwargs["gov_id"] == "gov-id-1"
    assert call_kwargs["bill_openstates_id"] == "bill-openstates-id-1"
    assert call_kwargs["gov_id"] != call_kwargs["bill_openstates_id"]


@pytest.mark.asyncio
async def test_batch_job_defaults_max_bills_per_run_to_25_when_absent():
    candidates = [_candidate(i) for i in range(30)]
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.list_current_session_bill_candidates",
        new=AsyncMock(return_value=candidates),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.get_concept_statement_set",
        new=AsyncMock(return_value=None),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_and_store_concept_statements",
        new=AsyncMock(return_value={"id": 1}),
    ):
        result = await run_concept_statement_batch_job({}, jurisdictions=["FL"])

    assert result["considered"] == 25


# ---------------------------------------------------------------------------
# Regression: this new module must not touch bill_artifact_generation's own
# 8 existing dispatch types.
# ---------------------------------------------------------------------------

def test_bill_artifact_generation_dispatch_map_is_unchanged():
    """Importing/using concept_statement_dispatch must not have mutated or
    shrunk/grown bill_artifact_generation's own artifact_type -> question_type
    map -- the two modules write to entirely independent models and must
    stay independently wired."""
    assert _ARTIFACT_TYPE_TO_QUESTION_TYPE == {
        "bill_summary": "summary_500char",
        "bill_pros_cons": "pros_cons",
        "bill_vote_yes_frame": "vote_yes_frame",
        "bill_vote_no_frame": "vote_no_frame",
        "bill_supporting_orgs": "supporting_orgs",
        "bill_opposing_orgs": "opposing_orgs",
        "bill_impact_analysis": "impact_analysis",
    }


@pytest.mark.asyncio
async def test_bill_artifact_generation_bill_summary_dispatch_still_works_unchanged():
    """A real end-to-end smoke test of one of the 8 existing BillArtifact
    types, run from this test module (which also imports
    concept_statement_dispatch) -- confirms the new module's import/reuse
    of _resolve_bill_source doesn't change BillArtifact's own write path
    or its artifact_type -> question_type wiring."""
    dispatch_result = {
        "answer": {"text": "A plain-language summary.", "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            bill_openstates_id="8d71a94e-0000-0000-0000-000000000001",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            bill_source="https://flsenate.gov/Session/Bill/2026/123/BillText/Filed/PDF",
            artifact_type="bill_summary",
        )

    assert result == {"id": 1, "created": True}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "A plain-language summary."
    assert write_kwargs["status"] == "complete"
