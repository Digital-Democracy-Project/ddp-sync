"""Tests for the Phase 8 write path (ddp-infra PLAN-bill-document-provenance.md):
connecting LegBot dispatch to the BillArtifact ledger + Pinecone.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.bill_artifact_generation import generate_and_store_bill_artifact

_COMMON_KWARGS = dict(
    bill_openstates_id="8d71a94e-0000-0000-0000-000000000001",
    jurisdiction="FL",
    session_code="2026",
    version_date="2026-01-05",
    version_note="Introduced",
    bill_source="https://flsenate.gov/Session/Bill/2026/123/BillText/Filed/PDF",
)


@pytest.mark.asyncio
async def test_rejects_unsupported_artifact_type():
    with pytest.raises(ValueError, match="Unsupported artifact_type"):
        await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_impact_analysis"
        )


@pytest.mark.asyncio
async def test_happy_path_writes_broker_and_pinecone():
    dispatch_result = {
        "answer": {"text": "A plain-language summary.", "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ) as mock_write:
        mock_pipeline_cls.return_value.ingest_document = AsyncMock()

        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result == {"id": 1, "created": True}
    mock_pipeline_cls.return_value.ingest_document.assert_awaited_once()
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "A plain-language summary."
    assert write_kwargs["status"] == "complete"
    assert write_kwargs["model_name"] == "mlx"
    assert write_kwargs["pinecone_synced_at"] is not None


@pytest.mark.asyncio
async def test_pros_cons_content_is_json_and_still_writes_on_pinecone_failure():
    dispatch_result = {
        "answer": {
            "pros": ["Expands access."],
            "cons": ["Costly to implement."],
            "insufficient_information": False,
        },
        "backend": "openai",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 2, "created": True}),
    ) as mock_write:
        mock_pipeline_cls.return_value.ingest_document = AsyncMock(
            side_effect=RuntimeError("pinecone is down")
        )

        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_pros_cons"
        )

    assert result == {"id": 2, "created": True}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == '{"pros": ["Expands access."], "cons": ["Costly to implement."]}'
    assert write_kwargs["status"] == "complete"
    # Pinecone failed -- must not be silently treated as synced.
    assert write_kwargs["pinecone_synced_at"] is None


@pytest.mark.asyncio
async def test_insufficient_information_is_recorded_as_a_failed_artifact_no_pinecone_attempt():
    dispatch_result = {
        "answer": {"text": "", "insufficient_information": True},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 3, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result == {"id": 3, "created": True}
    mock_pipeline_cls.return_value.ingest_document.assert_not_called()
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["status"] == "failed"
    assert write_kwargs["failure_stage"] == "generation"
    assert write_kwargs["failure_reason"] == "insufficient_information"
