"""Tests for the Phase 8 write path (ddp-infra PLAN-bill-document-provenance.md):
connecting LegBot dispatch to the BillArtifact ledger + Pinecone.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.bill_artifact_generation import (
    ArchivedVersionMismatchError,
    generate_and_store_bill_artifact,
    generate_and_store_bill_changelog,
)
from ddp_sync.services.broker_client import BrokerClientError

_COMMON_KWARGS = dict(
    bill_openstates_id="8d71a94e-0000-0000-0000-000000000001",
    jurisdiction="FL",
    session_code="2026",
    version_date="2026-01-05",
    version_note="Introduced",
    bill_source="https://flsenate.gov/Session/Bill/2026/123/BillText/Filed/PDF",
)


@pytest.fixture(autouse=True)
def no_archived_text_by_default():
    """Every test in this file gets the "not archived" fallback path by
    default (get_archived_bill_text returns None), matching what today's
    behavior looks like for every non-FL bill / not-yet-archived FL bill --
    the common case. Tests exercising the archived-text-found path override
    this explicitly. Never makes a real HTTP call either way.
    """
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_bill_text",
        new=AsyncMock(return_value=None),
    ) as mock_lookup:
        yield mock_lookup


@pytest.mark.asyncio
async def test_rejects_unsupported_artifact_type():
    with pytest.raises(ValueError, match="Unsupported artifact_type"):
        await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="qa_report"
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


@pytest.mark.parametrize(
    ("artifact_type", "question_type"),
    [("bill_vote_yes_frame", "vote_yes_frame"), ("bill_vote_no_frame", "vote_no_frame")],
)
@pytest.mark.asyncio
async def test_vote_frame_happy_path_writes_broker_and_pinecone(artifact_type, question_type):
    dispatch_result = {
        "answer": {"text": "Vote yes if you want...", "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 4, "created": True}),
    ) as mock_write:
        mock_pipeline_cls.return_value.ingest_document = AsyncMock()

        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type=artifact_type
        )

    assert result == {"id": 4, "created": True}
    mock_dispatch.assert_awaited_once_with(_COMMON_KWARGS["bill_source"], question_type)
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "Vote yes if you want..."
    assert write_kwargs["status"] == "complete"


@pytest.mark.parametrize(
    ("artifact_type", "question_type"),
    [("bill_supporting_orgs", "supporting_orgs"), ("bill_opposing_orgs", "opposing_orgs")],
)
@pytest.mark.asyncio
async def test_org_types_happy_path_writes_broker_and_pinecone(artifact_type, question_type):
    dispatch_result = {
        "answer": {
            "org_types": [{"type": "environmental advocacy groups", "reason": "Title II's emissions provisions"}],
            "insufficient_information": False,
        },
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 5, "created": True}),
    ) as mock_write:
        mock_pipeline_cls.return_value.ingest_document = AsyncMock()

        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type=artifact_type
        )

    assert result == {"id": 5, "created": True}
    mock_dispatch.assert_awaited_once_with(_COMMON_KWARGS["bill_source"], question_type)
    write_kwargs = mock_write.await_args.kwargs
    assert json.loads(write_kwargs["content"]) == {
        "org_types": [{"type": "environmental advocacy groups", "reason": "Title II's emissions provisions"}]
    }
    assert write_kwargs["status"] == "complete"


@pytest.mark.asyncio
async def test_impact_analysis_happy_path_writes_broker_and_pinecone():
    dispatch_result = {
        "answer": {
            "affected_parties": [{"party": "small businesses", "effect": "new licensing fee"}],
            "fiscal_or_programmatic_effects": "Appropriates $2M for enforcement.",
            "effective_date": "2027-01-01",
            "insufficient_information": False,
        },
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 6, "created": True}),
    ) as mock_write:
        mock_pipeline_cls.return_value.ingest_document = AsyncMock()

        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_impact_analysis"
        )

    assert result == {"id": 6, "created": True}
    mock_dispatch.assert_awaited_once_with(_COMMON_KWARGS["bill_source"], "impact_analysis")
    write_kwargs = mock_write.await_args.kwargs
    assert json.loads(write_kwargs["content"]) == {
        "affected_parties": [{"party": "small businesses", "effect": "new licensing fee"}],
        "fiscal_or_programmatic_effects": "Appropriates $2M for enforcement.",
        "effective_date": "2027-01-01",
    }
    assert write_kwargs["status"] == "complete"


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


# ---------------------------------------------------------------------------
# bill_source resolution (ddp-infra "Real gap found 2026-07-29/30" -- prefer
# ddp-open-states' archived text over a live-fetch URL, OPEN-13)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_archived_text_found_is_used_instead_of_live_url(no_archived_text_by_default):
    """When the local api-v3 instance already has archived text for this
    bill's latest version, LegBot is dispatched with that text directly --
    never the live URL bill_source would otherwise have been.
    """
    no_archived_text_by_default.return_value = "ARCHIVED FULL BILL TEXT"
    dispatch_result = {
        "answer": {"text": "A plain-language summary.", "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 7, "created": True}),
    ):
        mock_pipeline_cls.return_value.ingest_document = AsyncMock()

        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result == {"id": 7, "created": True}
    no_archived_text_by_default.assert_awaited_once_with(_COMMON_KWARGS["bill_openstates_id"])
    mock_dispatch.assert_awaited_once_with("ARCHIVED FULL BILL TEXT", "summary_500char")


@pytest.mark.asyncio
async def test_archived_text_not_found_falls_back_to_live_url_bill_source(
    no_archived_text_by_default,
):
    """When the local api-v3 instance has no archived text (the common case
    today -- a non-FL bill, or an FL bill not yet archived), LegBot is
    dispatched with the caller-supplied live URL exactly as before this
    change -- fallback behavior, not a regression.
    """
    dispatch_result = {
        "answer": {"text": "A plain-language summary.", "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 8, "created": True}),
    ):
        mock_pipeline_cls.return_value.ingest_document = AsyncMock()

        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result == {"id": 8, "created": True}
    no_archived_text_by_default.assert_awaited_once_with(_COMMON_KWARGS["bill_openstates_id"])
    mock_dispatch.assert_awaited_once_with(_COMMON_KWARGS["bill_source"], "summary_500char")


_CHANGELOG_KWARGS = dict(
    bill_openstates_id="8d71a94e-0000-0000-0000-000000000001",
    jurisdiction="FL",
    session_code="2026",
    version_date="2026-02-01",
    version_note="Engrossed",
)

_ARCHIVED = {
    "old_bill_source": "Archived introduced text.",
    "diff_source": "--- Introduced\n+++ Engrossed\n@@ -1 +1 @@\n-old\n+new\n",
    "old_version_date": "2026-01-01",
    "old_version_note": "Introduced",
    "latest_version_date": "2026-02-01",
    "latest_version_note": "Engrossed",
}


@pytest.mark.asyncio
async def test_changelog_no_archived_inputs_writes_failed_row():
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=None),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result == {"id": 1, "created": True}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["status"] == "failed"
    assert write_kwargs["failure_reason"] == "no_archived_changelog_inputs"
    assert write_kwargs["content"] == ""
    assert "compare_version_date" not in write_kwargs


@pytest.mark.asyncio
async def test_changelog_version_mismatch_raises_and_writes_nothing():
    """The most important test in this design (round 4's catch): a stale or
    mismatched caller must never write anything at all -- not a failed row,
    not a successful one -- since write_bill_artifact's own upsert key
    resolves from this call's own (stale) version_date/version_note and
    could otherwise silently overwrite an already-correct row for that
    version.
    """
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=_ARCHIVED),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(),
    ) as mock_write:
        with pytest.raises(ArchivedVersionMismatchError):
            await generate_and_store_bill_changelog(
                **{**_CHANGELOG_KWARGS, "version_date": "1999-01-01", "version_note": "Stale"}
            )

    mock_dispatch.assert_not_called()
    mock_pipeline_cls.assert_not_called()
    mock_write.assert_not_called()


@pytest.mark.asyncio
async def test_changelog_happy_path_writes_broker_and_pinecone_with_compare_version():
    dispatch_result = {
        "answer": {
            "insufficient_information": False,
            "sections_added": ["A new section on fees."],
            "sections_removed": [],
            "sections_modified": ["Section 3 reworded."],
            "policy_implications": "Increases administrative cost.",
        },
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=_ARCHIVED),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 2, "created": True}),
    ) as mock_write:
        mock_pipeline_cls.return_value.ingest_document = AsyncMock()

        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result == {"id": 2, "created": True}
    mock_dispatch.assert_awaited_once_with(
        old_bill_source="Archived introduced text.",
        diff_source="--- Introduced\n+++ Engrossed\n@@ -1 +1 @@\n-old\n+new\n",
    )
    mock_pipeline_cls.return_value.ingest_document.assert_awaited_once()
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["status"] == "complete"
    assert write_kwargs["model_name"] == "mlx"
    assert write_kwargs["pinecone_synced_at"] is not None
    assert write_kwargs["compare_version_date"] == "2026-01-01"
    assert write_kwargs["compare_version_note"] == "Introduced"
    content = write_kwargs["content"]
    assert "## What Changed" not in content  # no bill_title param -- see design note
    assert "**From:** Introduced" in content
    assert "**To:** Engrossed" in content
    assert "### Sections Added" in content
    assert "- A new section on fees." in content
    assert "### Sections Removed" in content
    assert "- None" in content
    assert "### Sections Modified" in content
    assert "### Key Policy Implications" in content
    assert "Increases administrative cost." in content


@pytest.mark.asyncio
async def test_changelog_insufficient_information_writes_failed_row_with_compare_version():
    dispatch_result = {
        "answer": {"insufficient_information": True, "reason": "diff_too_ambiguous"},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=_ARCHIVED),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 3, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result == {"id": 3, "created": True}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["status"] == "failed"
    assert write_kwargs["failure_reason"] == "diff_too_ambiguous"
    assert write_kwargs["content"] == ""
    # compare_version is still real, known provenance even on this failure
    # path -- only the "nothing archived at all" and version-mismatch cases
    # write with no compare_version.
    assert write_kwargs["compare_version_date"] == "2026-01-01"
    assert write_kwargs["compare_version_note"] == "Introduced"


@pytest.mark.asyncio
async def test_changelog_broker_write_failure_after_pinecone_success_propagates():
    dispatch_result = {
        "answer": {
            "insufficient_information": False,
            "sections_added": [],
            "sections_removed": [],
            "sections_modified": [],
            "policy_implications": "",
        },
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=_ARCHIVED),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.IngestionPipeline"
    ) as mock_pipeline_cls, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(side_effect=BrokerClientError("compare_version FK resolution failed")),
    ):
        mock_pipeline_cls.return_value.ingest_document = AsyncMock()

        with pytest.raises(BrokerClientError):
            await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    mock_pipeline_cls.return_value.ingest_document.assert_awaited_once()
