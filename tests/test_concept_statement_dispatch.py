"""Tests for the concept-statement dispatch pipeline (ddp-infra
PLAN-bill-concept-polling.md §0.4): wiring LegBot's concept_statements
question type to the new ConceptStatementSet create endpoint.

SYNC-32: this module's own scheduled batch job (run_concept_statement_
batch_job, "which bills to run it against") was removed along with its
tests -- see tests/test_session_pipeline_runner.py's own "concept
statements (SYNC-31)" section for the consolidated path's coverage of that
same candidate-enumeration/dedup/dispatch behavior instead.
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


# _resolve_bill_source has no live-URL fallback anymore -- every test gets
# real archived text by default so it reaches dispatch at all.
_ARCHIVED_TEXT = "ARCHIVED BILL TEXT FOR TESTING"


@pytest.fixture(autouse=True)
def archived_text_by_default():
    """Every test in this file gets real archived bill text by default --
    matches test_bill_artifact_generation.py's own fixture.
    dispatch_and_store_concept_statements reuses _resolve_bill_source
    (and, through it, get_archived_bill_text) directly rather than
    reimplementing it, so patching it at its one real definition site
    (bill_artifact_generation) is what actually governs both callers.
    """
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_bill_text",
        new=AsyncMock(return_value=_ARCHIVED_TEXT),
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
    mock_dispatch.assert_awaited_once_with(_ARCHIVED_TEXT, "concept_statements")
    mock_create.assert_awaited_once_with(
        gov_id=_COMMON_KWARGS["gov_id"],
        jurisdiction_iso2="FL",
        session_code="2026",
        statements=dispatch_result["answer"]["statements"],
        source_document_url=_COMMON_KWARGS["bill_source"],
        model_name="mlx",
        broker_api_base=None,
        broker_api_token=None,
    )


@pytest.mark.asyncio
async def test_broker_override_threads_through_to_create_call():
    """SYNC-31: session_pipeline_runner.py's consolidated path needs this
    write to land on the same dev/prod broker instance its other artifact
    types do -- None (the default, exercised above) preserves the original
    caller's behavior unchanged."""
    dispatch_result = {
        "answer": {"statements": ["The state should require X."], "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_create:
        await dispatch_and_store_concept_statements(
            **_COMMON_KWARGS,
            broker_api_base="http://dev-broker:8080",
            broker_api_token="dev-token",
        )

    assert mock_create.await_args.kwargs["broker_api_base"] == "http://dev-broker:8080"
    assert mock_create.await_args.kwargs["broker_api_token"] == "dev-token"


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
async def test_archived_text_found_is_used_for_dispatch(archived_text_by_default):
    archived_text_by_default.return_value = "ARCHIVED FULL BILL TEXT"
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
    archived_text_by_default.assert_awaited_once_with(_COMMON_KWARGS["bill_openstates_id"])
    mock_dispatch.assert_awaited_once_with("ARCHIVED FULL BILL TEXT", "concept_statements")


@pytest.mark.asyncio
async def test_no_archived_text_skips_dispatch_and_returns_none(archived_text_by_default):
    """No live-URL fallback anymore -- when nothing is archived,
    concept_statements is never dispatched at all, matching
    generate_and_store_bill_artifact's own no-archived-text posture."""
    archived_text_by_default.return_value = None
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
        new=AsyncMock(),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
        new=AsyncMock(),
    ) as mock_create:
        result = await dispatch_and_store_concept_statements(**_COMMON_KWARGS)

    assert result is None
    archived_text_by_default.assert_awaited_once_with(_COMMON_KWARGS["bill_openstates_id"])
    mock_dispatch.assert_not_awaited()
    mock_create.assert_not_called()


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
        "bill_topics": "bill_topics",
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
            artifact_type="bill_summary",
        )

    assert result == {"id": 1, "created": True}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "A plain-language summary."
    assert write_kwargs["status"] == "complete"
