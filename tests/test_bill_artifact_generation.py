"""Tests for the Phase 8 write path (ddp-infra PLAN-bill-document-provenance.md):
connecting LegBot dispatch to the BillArtifact ledger. Does not touch
Pinecone -- decoupled 2026-08-10, see bill_artifact_generation.py's own
module docstring.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.bill_artifact_generation import (
    ArchivedVersionMismatchError,
    dispatch_and_record_bill_artifact,
    generate_and_store_bill_artifact,
    generate_and_store_bill_changelog,
)
from ddp_sync.services.broker_client import BrokerClientError
from ddp_sync.services.legbot_client import LegBotDispatchError

_COMMON_KWARGS = dict(
    bill_openstates_id="8d71a94e-0000-0000-0000-000000000001",
    jurisdiction="FL",
    session_code="2026",
    version_date="2026-01-05",
    version_note="Introduced",
)

# Only used by dispatch_and_record_bill_artifact tests (_ONDEMAND_KWARGS
# below) -- that wrapper still accepts bill_source for API-shape stability,
# even though it no longer passes it through. generate_and_store_bill_artifact
# itself has no bill_source parameter anymore.
_BILL_SOURCE_URL = "https://flsenate.gov/Session/Bill/2026/123/BillText/Filed/PDF"

# What get_archived_bill_text returns by default (see the fixture below) --
# _resolve_bill_source has no live-URL fallback anymore, so every happy-path
# test needs real archived text to reach dispatch at all.
_ARCHIVED_TEXT = "ARCHIVED BILL TEXT FOR TESTING"


@pytest.fixture(autouse=True)
def archived_text_by_default():
    """Every test in this file gets real archived bill text by default
    (get_archived_bill_text returns _ARCHIVED_TEXT) -- _resolve_bill_source
    no longer falls back to a live URL when nothing is archived, so this is
    the only way a test reaches a dispatch call at all. Tests exercising the
    "nothing archived" skip/failed-row path override this to return None.
    Never makes a real HTTP call either way.
    """
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_bill_text",
        new=AsyncMock(return_value=_ARCHIVED_TEXT),
    ) as mock_lookup:
        yield mock_lookup


@pytest.mark.asyncio
async def test_rejects_unsupported_artifact_type():
    with pytest.raises(ValueError, match="Unsupported artifact_type"):
        await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="qa_report"
        )


@pytest.mark.asyncio
async def test_happy_path_writes_broker():
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
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result == {"id": 1, "created": True, "status": "complete"}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "A plain-language summary."
    assert write_kwargs["status"] == "complete"
    assert write_kwargs["model_name"] == "mlx"
    assert "pinecone_synced_at" not in write_kwargs


@pytest.mark.asyncio
async def test_pros_cons_content_is_json():
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
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 2, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_pros_cons"
        )

    assert result == {"id": 2, "created": True, "status": "complete"}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == '{"pros": ["Expands access."], "cons": ["Costly to implement."]}'
    assert write_kwargs["status"] == "complete"


@pytest.mark.parametrize(
    ("artifact_type", "question_type"),
    [("bill_vote_yes_frame", "vote_yes_frame"), ("bill_vote_no_frame", "vote_no_frame")],
)
@pytest.mark.asyncio
async def test_vote_frame_happy_path_writes_broker(artifact_type, question_type):
    dispatch_result = {
        "answer": {"text": "Vote yes if you want...", "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 4, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type=artifact_type
        )

    assert result == {"id": 4, "created": True, "status": "complete"}
    mock_dispatch.assert_awaited_once_with(_ARCHIVED_TEXT, question_type)
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "Vote yes if you want..."
    assert write_kwargs["status"] == "complete"


@pytest.mark.parametrize(
    ("artifact_type", "question_type"),
    [("bill_supporting_orgs", "supporting_orgs"), ("bill_opposing_orgs", "opposing_orgs")],
)
@pytest.mark.asyncio
async def test_org_types_happy_path_writes_broker(artifact_type, question_type):
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
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 5, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type=artifact_type
        )

    assert result == {"id": 5, "created": True, "status": "complete"}
    mock_dispatch.assert_awaited_once_with(_ARCHIVED_TEXT, question_type)
    write_kwargs = mock_write.await_args.kwargs
    assert json.loads(write_kwargs["content"]) == {
        "org_types": [{"type": "environmental advocacy groups", "reason": "Title II's emissions provisions"}]
    }
    assert write_kwargs["status"] == "complete"


@pytest.mark.asyncio
async def test_impact_analysis_happy_path_writes_broker():
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
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 6, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_impact_analysis"
        )

    assert result == {"id": 6, "created": True, "status": "complete"}
    mock_dispatch.assert_awaited_once_with(_ARCHIVED_TEXT, "impact_analysis")
    write_kwargs = mock_write.await_args.kwargs
    assert json.loads(write_kwargs["content"]) == {
        "affected_parties": [{"party": "small businesses", "effect": "new licensing fee"}],
        "fiscal_or_programmatic_effects": "Appropriates $2M for enforcement.",
        "effective_date": "2027-01-01",
    }
    assert write_kwargs["status"] == "complete"


@pytest.mark.asyncio
async def test_insufficient_information_is_recorded_as_a_failed_artifact():
    dispatch_result = {
        "answer": {"text": "", "insufficient_information": True},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 3, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result == {"id": 3, "created": True, "status": "failed"}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["status"] == "failed"
    assert write_kwargs["failure_stage"] == "generation"
    assert write_kwargs["failure_reason"] == "insufficient_information"


@pytest.mark.asyncio
async def test_local_status_overrides_a_conflicting_broker_response_status():
    """SYNC-24: the broker's own write response is merged with this
    function's own locally-known status LAST, so it always wins -- even if
    ddp-broker-py's serializer ever started echoing back its own `status`
    field (it doesn't today, just `id`/`created`), a stale or differently-
    named value there could never silently override the authoritative
    outcome _process_bill relies on to classify artifacts_generated vs
    artifacts_failed."""
    dispatch_result = {
        "answer": {"text": "", "insufficient_information": True},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 3, "created": True, "status": "pending"}),
    ):
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# bill_topics (ddp-infra's PLAN-legbot.md §27) -- deterministic filter/flatten
# of LegBot's primary_topic/topics answer shape against the 33-name taxonomy.
# ---------------------------------------------------------------------------

def _bill_topics_dispatch_result(*, primary_topic, topics):
    return {
        "answer": {
            "primary_topic": primary_topic,
            "topics": topics,
            "insufficient_information": False,
        },
        "backend": "mlx",
    }


@pytest.mark.asyncio
async def test_bill_topics_happy_path_writes_broker():
    dispatch_result = _bill_topics_dispatch_result(
        primary_topic="Criminal Justice", topics=["Criminal Justice", "Drugs"]
    )
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 8, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_topics"
        )

    assert result == {"id": 8, "created": True, "status": "complete"}
    mock_dispatch.assert_awaited_once_with(_ARCHIVED_TEXT, "bill_topics")
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "**Primary:** Criminal Justice\n\n- Criminal Justice\n- Drugs"
    assert write_kwargs["status"] == "complete"


@pytest.mark.asyncio
async def test_bill_topics_drops_invalid_topic():
    dispatch_result = _bill_topics_dispatch_result(
        primary_topic="Criminal Justice",
        topics=["Criminal Justice", "Not A Real Topic", "Drugs"],
    )
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 9, "created": True}),
    ) as mock_write:
        await generate_and_store_bill_artifact(**_COMMON_KWARGS, artifact_type="bill_topics")

    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "**Primary:** Criminal Justice\n\n- Criminal Justice\n- Drugs"
    assert write_kwargs["status"] == "complete"


@pytest.mark.asyncio
async def test_bill_topics_prepends_valid_primary_missing_from_topics():
    dispatch_result = _bill_topics_dispatch_result(
        primary_topic="Education", topics=["Drugs", "Guns"]
    )
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 10, "created": True}),
    ) as mock_write:
        await generate_and_store_bill_artifact(**_COMMON_KWARGS, artifact_type="bill_topics")

    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "**Primary:** Education\n\n- Education\n- Drugs\n- Guns"
    assert write_kwargs["status"] == "complete"


@pytest.mark.asyncio
async def test_bill_topics_promotes_first_surviving_topic_when_primary_invalid():
    dispatch_result = _bill_topics_dispatch_result(
        primary_topic="Not A Real Topic", topics=["Drugs", "Guns"]
    )
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 11, "created": True}),
    ) as mock_write:
        await generate_and_store_bill_artifact(**_COMMON_KWARGS, artifact_type="bill_topics")

    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == "**Primary:** Drugs\n\n- Drugs\n- Guns"
    assert write_kwargs["status"] == "complete"


@pytest.mark.asyncio
async def test_bill_topics_over_cap_truncates_but_keeps_primary():
    dispatch_result = _bill_topics_dispatch_result(
        primary_topic="Guns",
        topics=["Animals", "Arts", "Business", "Civil Rights", "Guns"],
    )
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 12, "created": True}),
    ) as mock_write:
        await generate_and_store_bill_artifact(**_COMMON_KWARGS, artifact_type="bill_topics")

    write_kwargs = mock_write.await_args.kwargs
    content = write_kwargs["content"]
    assert content.startswith("**Primary:** Guns\n\n")
    bullets = content.split("\n\n", 1)[1].splitlines()
    assert len(bullets) == 4
    assert "- Guns" in bullets
    assert "- Civil Rights" not in bullets
    assert write_kwargs["status"] == "complete"


@pytest.mark.asyncio
async def test_bill_topics_zero_survivors_is_recorded_as_a_failed_artifact():
    dispatch_result = _bill_topics_dispatch_result(
        primary_topic="Not A Real Topic", topics=["Also Not Real", "Nope"]
    )
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 13, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_topics"
        )

    assert result == {"id": 13, "created": True, "status": "failed"}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == ""
    assert write_kwargs["status"] == "failed"
    assert write_kwargs["failure_stage"] == "generation"
    assert write_kwargs["failure_reason"] == "no_valid_topics"


# ---------------------------------------------------------------------------
# bill_source resolution (ddp-infra "Real gap found 2026-07-29/30", later
# hardened to drop the live-URL fallback entirely -- ddp-open-states'
# archived text is the only source LegBot ever sees, OPEN-13 / OPEN-48)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_archived_text_found_is_used_for_dispatch(archived_text_by_default):
    """LegBot is dispatched with the bill's own archived text -- there is no
    live-URL fallback for _resolve_bill_source to ever fall back to.
    """
    archived_text_by_default.return_value = "ARCHIVED FULL BILL TEXT"
    dispatch_result = {
        "answer": {"text": "A plain-language summary.", "insufficient_information": False},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 7, "created": True}),
    ):
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result == {"id": 7, "created": True, "status": "complete"}
    archived_text_by_default.assert_awaited_once_with(_COMMON_KWARGS["bill_openstates_id"])
    mock_dispatch.assert_awaited_once_with("ARCHIVED FULL BILL TEXT", "summary_500char")


@pytest.mark.asyncio
async def test_no_archived_text_skips_dispatch_and_writes_failed_row(archived_text_by_default):
    """When ddp-open-states has nothing archived for this bill, LegBot is
    never dispatched at all -- no live-URL fallback exists anymore (removed
    specifically so a missing archive can never silently undo OPEN-48's
    data-quality work) -- and a failed BillArtifact row is written instead.
    """
    archived_text_by_default.return_value = None
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 8, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert result == {"id": 8, "created": True, "status": "failed"}
    archived_text_by_default.assert_awaited_once_with(_COMMON_KWARGS["bill_openstates_id"])
    mock_dispatch.assert_not_awaited()
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["content"] == ""
    assert write_kwargs["status"] == "failed"
    assert write_kwargs["failure_stage"] == "generation"
    assert write_kwargs["failure_reason"] == "no_archived_bill_text"


_CHANGELOG_KWARGS = dict(
    bill_openstates_id="8d71a94e-0000-0000-0000-000000000001",
    jurisdiction="FL",
    session_code="2026",
    version_date="2026-02-01",
    version_note="Engrossed",
)

# SYNC-44: get_archived_version_transitions' return shape -- a list of
# transitions (oldest-first) plus the bill's complete raw version list.
# One transition here (Introduced -> Engrossed) is the same single-hop shape
# the old get_archived_changelog_inputs-based design always produced; the
# multi-transition tests below add a second, earlier hop (Filed ->
# Introduced) on top of this same "Filed" entry already present in
# `versions`.
_ONE_TRANSITION = {
    "old_bill_source": "Archived introduced text.",
    "diff_source": "--- Introduced\n+++ Engrossed\n@@ -1 +1 @@\n-old\n+new\n",
    "old_version_date": "2026-01-01",
    "old_version_note": "Introduced",
    "new_version_date": "2026-02-01",
    "new_version_note": "Engrossed",
}

_RESOLVED_ONE_TRANSITION = {
    "transitions": [_ONE_TRANSITION],
    # SYNC-30: a real bill can have more than the versions any single
    # transition's own diff resolution needs -- "Filed" here is older than
    # either, matching FL SB 2506E's real 3-version shape (Filed -> e1 -> er)
    # that motivated backfilling the bill's full archived history.
    "versions": [
        {"date": "2025-12-01", "note": "Filed"},
        {"date": "2026-01-01", "note": "Introduced"},
        {"date": "2026-02-01", "note": "Engrossed"},
    ],
}


@pytest.mark.asyncio
async def test_changelog_no_transitions_returns_not_applicable_and_writes_nothing():
    """SYNC-44/AC4: a bill with no version transition ready yet (its
    earliest version, or a diff not archived yet) is not a failure -- it
    writes nothing at all, replacing the old blanket `failed`/
    "no_archived_changelog_inputs" row (actively harmful once retry_failed
    existed: it looked identical to a real, retryable failure)."""
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=None),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(),
    ) as mock_write:
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result == {"status": "not_applicable"}
    mock_write.assert_not_called()


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
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(),
    ) as mock_write:
        with pytest.raises(ArchivedVersionMismatchError):
            await generate_and_store_bill_changelog(
                **{**_CHANGELOG_KWARGS, "version_date": "1999-01-01", "version_note": "Stale"}
            )

    mock_dispatch.assert_not_called()
    mock_write.assert_not_called()


@pytest.mark.asyncio
async def test_changelog_happy_path_writes_broker_with_compare_version():
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
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 2, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result == {"id": 2, "created": True, "status": "complete"}
    mock_dispatch.assert_awaited_once_with(
        old_bill_source="Archived introduced text.",
        diff_source="--- Introduced\n+++ Engrossed\n@@ -1 +1 @@\n-old\n+new\n",
    )
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["status"] == "complete"
    assert write_kwargs["model_name"] == "mlx"
    assert "pinecone_synced_at" not in write_kwargs
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
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 3, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result == {"id": 3, "created": True, "status": "failed"}
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
async def test_changelog_local_status_overrides_a_conflicting_broker_response_status():
    """Same merge-order guarantee as generate_and_store_bill_artifact's own
    equivalent test -- this function's own known status always wins over
    whatever the broker write response itself contains."""
    dispatch_result = {
        "answer": {"insufficient_information": True, "reason": "diff_too_ambiguous"},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 3, "created": True, "status": "pending"}),
    ):
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result["status"] == "failed"


@pytest.mark.asyncio
async def test_changelog_broker_write_failure_propagates():
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
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(side_effect=BrokerClientError("compare_version FK resolution failed")),
    ):
        with pytest.raises(BrokerClientError):
            await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)


# ---------------------------------------------------------------------------
# SYNC-44: the full-history walk itself -- a bill with more than one
# transition ready must generate every one of them, oldest-first, not just
# the one immediately before its current latest version.
# ---------------------------------------------------------------------------

_EARLIER_TRANSITION = {
    "old_bill_source": "Archived filed text.",
    "diff_source": "--- Filed\n+++ Introduced\n@@ -1 +1 @@\n-a\n+b\n",
    "old_version_date": "2025-12-01",
    "old_version_note": "Filed",
    "new_version_date": "2026-01-01",
    "new_version_note": "Introduced",
}

_RESOLVED_TWO_TRANSITIONS = {
    "transitions": [_EARLIER_TRANSITION, _ONE_TRANSITION],
    "versions": _RESOLVED_ONE_TRANSITION["versions"],
}

_CHANGELOG_DISPATCH_RESULT = {
    "answer": {
        "insufficient_information": False,
        "sections_added": [],
        "sections_removed": [],
        "sections_modified": [],
        "policy_implications": "",
    },
    "backend": "mlx",
}


@pytest.mark.asyncio
async def test_changelog_generates_every_transition_oldest_first():
    """SYNC-44's whole point: a bill with two ready transitions gets a
    bill_changelog for EACH one, in oldest-first order -- not just the last
    hop, which is what produced whitespace-only changelogs on FL 2026E."""
    dispatch_calls = []

    async def _fake_dispatch(*, old_bill_source, diff_source):
        dispatch_calls.append((old_bill_source, diff_source))
        return _CHANGELOG_DISPATCH_RESULT

    write_calls = []

    async def _fake_write(**kwargs):
        write_calls.append(kwargs)
        return {"id": len(write_calls), "created": True}

    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_TWO_TRANSITIONS),
    ), patch(
        "ddp_sync.pipelines.bill_version.BillVersionSyncService._backfill_missing_versions",
        new=AsyncMock(return_value=1),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=_fake_dispatch,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=_fake_write,
    ):
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert dispatch_calls == [
        ("Archived filed text.", "--- Filed\n+++ Introduced\n@@ -1 +1 @@\n-a\n+b\n"),
        (
            "Archived introduced text.",
            "--- Introduced\n+++ Engrossed\n@@ -1 +1 @@\n-old\n+new\n",
        ),
    ]
    assert [w["version_note"] for w in write_calls] == ["Introduced", "Engrossed"]
    assert [w["compare_version_note"] for w in write_calls] == ["Filed", "Introduced"]
    # The return value reflects the LAST transition processed -- both
    # succeeded here, so this is the same as reporting the first failure
    # (there isn't one). See test_changelog_reports_the_first_failed_
    # transition_not_the_last below for the mixed-result case.
    assert result == {"id": 2, "created": True, "status": "complete"}


@pytest.mark.asyncio
async def test_changelog_processes_every_transition_uncapped():
    """/pm-review caught a real bug in an earlier version of this function: a
    hardcoded cap kept only the most recent N transitions, but since this
    function is only ever reached again once the bill's CURRENT LATEST
    version lacks a changelog, writing the latest transition (always inside
    the kept window) meant the outer coverage gate would never call this
    function for this bill again -- silently and PERMANENTLY stranding
    whatever older transitions the cap dropped, with no continuation
    mechanism to ever pick them back up. There is no cap: a long version
    history costs more sequential LegBot dispatches, not lost history."""
    many_transitions = [
        {
            "old_bill_source": f"text {i}",
            "diff_source": f"diff {i}",
            "old_version_date": f"2026-01-{i:02d}",
            "old_version_note": f"v{i}",
            "new_version_date": f"2026-01-{i + 1:02d}",
            "new_version_note": f"v{i + 1}",
        }
        for i in range(15)
    ]
    resolved = {
        "transitions": many_transitions,
        "versions": [
            {"date": t["new_version_date"], "note": t["new_version_note"]}
            for t in many_transitions
        ],
    }

    dispatch_calls = []

    async def _fake_dispatch(*, old_bill_source, diff_source):
        dispatch_calls.append(old_bill_source)
        return _CHANGELOG_DISPATCH_RESULT

    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=resolved),
    ), patch(
        "ddp_sync.pipelines.bill_version.BillVersionSyncService._backfill_missing_versions",
        new=AsyncMock(return_value=0),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=_fake_dispatch,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ):
        await generate_and_store_bill_changelog(
            **{
                **_CHANGELOG_KWARGS,
                "version_date": many_transitions[-1]["new_version_date"],
                "version_note": many_transitions[-1]["new_version_note"],
            }
        )

    assert dispatch_calls == [t["old_bill_source"] for t in many_transitions]


@pytest.mark.asyncio
async def test_changelog_reports_the_first_failed_transition_not_the_last():
    """/pm-review's other real catch: if only the LAST transition's result
    were returned, an earlier transition failing (e.g. LegBot reports
    insufficient_information) while a later one succeeds would report the
    whole call as "complete" to session_pipeline_runner.py -- hiding a real
    failed row that nothing ever revisits (the outer coverage gate only
    looks at the bill's current latest version, and the later transition
    just made that look fully covered). Both transitions are still written
    for real regardless -- only the REPORTED status changes."""
    write_calls = []

    async def _fake_write(**kwargs):
        write_calls.append(kwargs)
        return {"id": len(write_calls), "created": True}

    async def _fake_dispatch(*, old_bill_source, diff_source):
        if old_bill_source == "Archived filed text.":
            return {
                "answer": {"insufficient_information": True, "reason": "diff_too_ambiguous"},
                "backend": "mlx",
            }
        return _CHANGELOG_DISPATCH_RESULT

    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_TWO_TRANSITIONS),
    ), patch(
        "ddp_sync.pipelines.bill_version.BillVersionSyncService._backfill_missing_versions",
        new=AsyncMock(return_value=1),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=_fake_dispatch,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=_fake_write,
    ):
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    # Both transitions were written for real...
    assert [w["version_note"] for w in write_calls] == ["Introduced", "Engrossed"]
    assert [w["status"] for w in write_calls] == ["failed", "complete"]
    # ...but the overall call reports the failure, not the later success.
    assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# AC2/BROKER-130 (2026-08-29): a subsequent run must skip transitions that
# already have a bill_changelog, gated on gov_id being supplied. This is the
# gap /pm-review's SECOND pass found live: calling generate_and_store_
# bill_changelog twice against the same bill produced 4 LegBot dispatches
# for 2 real transitions, because nothing filtered out the already-complete
# ones once the bill gained a new version and the outer (latest-version
# -only) coverage gate opened again.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_changelog_skips_a_transition_already_covered_when_gov_id_given():
    """The AC2 fix itself: with gov_id supplied, a transition whose target
    already has a bill_changelog (any status) of its own is never
    re-dispatched -- only the genuinely new one is."""
    coverage = {
        "versions": [
            {
                "bill_version_id": 19, "version_note": "Introduced",
                "artifacts": {"bill_changelog": {"status": "complete", "compare_version_id": 380}},
            },
            {"bill_version_id": 18, "version_note": "Engrossed", "artifacts": {}},
        ],
        "unclassified_versions": [],
    }
    dispatch_calls = []

    async def _fake_dispatch(*, old_bill_source, diff_source):
        dispatch_calls.append(old_bill_source)
        return _CHANGELOG_DISPATCH_RESULT

    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_TWO_TRANSITIONS),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_bill_artifact_coverage_all_versions",
        new=AsyncMock(return_value=coverage),
    ) as mock_coverage, patch(
        "ddp_sync.pipelines.bill_version.BillVersionSyncService._backfill_missing_versions",
        new=AsyncMock(return_value=0),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=_fake_dispatch,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ):
        result = await generate_and_store_bill_changelog(
            **_CHANGELOG_KWARGS, gov_id="SJR 2F",
        )

    mock_coverage.assert_awaited_once_with(
        jurisdiction=_CHANGELOG_KWARGS["jurisdiction"],
        session_code=_CHANGELOG_KWARGS["session_code"],
        gov_id="SJR 2F",
        broker_api_base=None,
        broker_api_token=None,
    )
    # Only the Introduced -> Engrossed hop dispatched -- Filed -> Introduced
    # already has a changelog on "Introduced" and is skipped.
    assert dispatch_calls == ["Archived introduced text."]
    assert result["status"] == "complete"


@pytest.mark.asyncio
async def test_changelog_returns_not_applicable_when_every_transition_already_covered():
    coverage = {
        "versions": [
            {"bill_version_id": 19, "version_note": "Introduced",
             "artifacts": {"bill_changelog": {"status": "complete", "compare_version_id": 380}}},
            {"bill_version_id": 18, "version_note": "Engrossed",
             "artifacts": {"bill_changelog": {"status": "complete", "compare_version_id": 19}}},
        ],
        "unclassified_versions": [],
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_TWO_TRANSITIONS),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_bill_artifact_coverage_all_versions",
        new=AsyncMock(return_value=coverage),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(),
    ) as mock_write:
        result = await generate_and_store_bill_changelog(
            **_CHANGELOG_KWARGS, gov_id="SJR 2F",
        )

    mock_dispatch.assert_not_called()
    mock_write.assert_not_called()
    assert result == {"status": "not_applicable"}


@pytest.mark.asyncio
async def test_changelog_without_gov_id_never_calls_the_coverage_read():
    """SYNC-10's on-demand endpoint has no gov_id in its request body at all
    -- omitting it must be a pure no-op for the coverage filter, not an
    error, and every transition dispatches exactly as it did before this
    fix (the pre-existing multi-transition test above already covers this;
    this test only asserts the coverage read itself is never attempted)."""
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_bill_artifact_coverage_all_versions",
        new=AsyncMock(),
    ) as mock_coverage, patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=_CHANGELOG_DISPATCH_RESULT),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ):
        await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    mock_coverage.assert_not_called()


@pytest.mark.asyncio
async def test_changelog_propagates_when_broker_predates_broker_130():
    """get_bill_artifact_coverage_all_versions raises BrokerClientError when
    the target broker doesn't support ?versions=all yet -- this must
    propagate uncaught (same convention as every other BrokerClientError
    here), not be swallowed as "nothing covered" (which would silently
    regenerate everything, the exact bug this fix exists to prevent)."""
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_bill_artifact_coverage_all_versions",
        new=AsyncMock(side_effect=BrokerClientError("predates BROKER-130")),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(),
    ) as mock_dispatch:
        with pytest.raises(BrokerClientError, match="predates BROKER-130"):
            await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS, gov_id="SJR 2F")

    mock_dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_changelog_coverage_filter_matches_unclassified_versions_too():
    coverage = {
        "versions": [
            {"bill_version_id": 18, "version_note": "Engrossed", "artifacts": {}},
        ],
        # "Introduced" -- the Filed->Introduced transition's own target --
        # shows up here, not in `versions`, to prove the filter checks both
        # lists rather than only the lineage one.
        "unclassified_versions": [
            {
                "bill_version_id": 19, "version_note": "Introduced",
                "artifacts": {"bill_changelog": {"status": "failed", "compare_version_id": None}},
            },
        ],
    }
    dispatch_calls = []

    async def _fake_dispatch(*, old_bill_source, diff_source):
        dispatch_calls.append(old_bill_source)
        return _CHANGELOG_DISPATCH_RESULT

    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_TWO_TRANSITIONS),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_bill_artifact_coverage_all_versions",
        new=AsyncMock(return_value=coverage),
    ), patch(
        "ddp_sync.pipelines.bill_version.BillVersionSyncService._backfill_missing_versions",
        new=AsyncMock(return_value=0),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=_fake_dispatch,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ):
        await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS, gov_id="SJR 2F")

    # "Introduced" already has a bill_changelog row (a failed one -- still
    # "any status", per this fix's own never-regenerate posture) even
    # though it's unclassified rather than in the lineage -- still skipped,
    # so only the Introduced -> Engrossed hop dispatches.
    assert dispatch_calls == ["Archived introduced text."]


@pytest.mark.asyncio
async def test_changelog_coverage_filter_tolerates_a_malformed_response():
    """/pm-review's second-pass catch: a coverage response missing a key
    this code didn't itself guarantee (e.g. no `unclassified_versions`, or
    an entry with no `artifacts`) must degrade to "no extra coverage found
    there", not crash this bill's dispatch with a raw KeyError. The one
    shape violation that IS fatal -- no `versions` key at all -- is a
    separate, already-tested BrokerClientError raised inside
    get_bill_artifact_coverage_all_versions itself."""
    incomplete_coverage = {
        "versions": [{"bill_version_id": 18, "version_note": "Engrossed"}],
        # unclassified_versions deliberately omitted.
    }

    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_bill_artifact_coverage_all_versions",
        new=AsyncMock(return_value=incomplete_coverage),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=_CHANGELOG_DISPATCH_RESULT),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ):
        result = await generate_and_store_bill_changelog(
            **_CHANGELOG_KWARGS, gov_id="SJR 2F",
        )

    assert result["status"] == "complete"


# ---------------------------------------------------------------------------
# SYNC-26 follow-up: run_legbot_pipeline's own compare_version backfill.
# check_and_reingest_version (the daily sync job) already backfills missing
# older BillVersion rows on a bill's first sighting -- run_legbot_pipeline
# never went through that code path, so it could still hit the same
# compare_version-FK-resolution 400 SYNC-26 fixed for the other caller.
#
# SYNC-28: the original fix gated this backfill on "does ddp-broker-py's
# ledger have any row at all for this bill," skipping entirely once one
# existed. That gate is gone -- the backfill call is now unconditional,
# because a SIBLING artifact type in the SAME run_legbot_pipeline batch
# reliably creates a `latest`-version ledger row (via ddp-broker-py's own
# BillArtifact-write auto-vivification) before bill_changelog itself ever
# runs, which made the old gate see "already has a row" and skip backfilling
# the actually-missing OLDER (compare_version) row every single time.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_changelog_always_backfills_compare_version_regardless_of_ledger_state():
    """SYNC-28: the backfill call is now unconditional -- this function no
    longer has any awareness of ledger state (no more gate to distinguish
    "first sighting" from "a sibling artifact type already wrote a `latest`
    row for this bill earlier in the same batch," SYNC-28's own real bug
    scenario), so both cases exercise this exact same code path and this one
    test covers both. The actual safety property this depends on --
    _backfill_missing_versions never re-writing `latest_version`'s own
    natural key, and write_bill_version being a true idempotent no-op for a
    version that already exists -- is verified separately, at that
    function's own level, by test_bill_version_history.py's
    test_backfill_excludes_latest_by_natural_key_not_object_identity and
    test_backfill_return_count_excludes_already_present_versions; this test
    only needs to prove THIS caller invokes it unconditionally."""
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_version.BillVersionSyncService._backfill_missing_versions",
        new=AsyncMock(return_value=1),
    ) as mock_backfill, patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=_CHANGELOG_DISPATCH_RESULT),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 2, "created": True}),
    ):
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result == {"id": 2, "created": True, "status": "complete"}
    mock_backfill.assert_awaited_once_with(
        bill_openstates_id=_CHANGELOG_KWARGS["bill_openstates_id"],
        jurisdiction_code=_CHANGELOG_KWARGS["jurisdiction"],
        session_code=_CHANGELOG_KWARGS["session_code"],
        versions=_RESOLVED_ONE_TRANSITION["versions"],
        latest_version={"date": "2026-02-01", "note": "Engrossed"},
        broker_api_base=None,
        broker_api_token=None,
    )


@pytest.mark.asyncio
async def test_changelog_backfill_covers_full_version_history_not_just_old_and_new():
    """SYNC-30: a bill with 3+ real versions must have EVERY older version
    passed to _backfill_missing_versions, not just the one immediately-
    previous compare_version this function's own diff needs -- SYNC-28's
    fix only ever covered that one version, silently never backfilling
    anything older (confirmed live: FL SB 2506E's "Filed" version never
    got a BillVersion row, while "e1"/"er" did)."""
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_version.BillVersionSyncService._backfill_missing_versions",
        new=AsyncMock(return_value=1),
    ) as mock_backfill, patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=_CHANGELOG_DISPATCH_RESULT),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 2, "created": True}),
    ):
        await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    passed_versions = mock_backfill.await_args.kwargs["versions"]
    passed_notes = {v["note"] for v in passed_versions}
    assert passed_notes == {"Filed", "Introduced", "Engrossed"}


@pytest.mark.asyncio
async def test_changelog_backfill_threads_broker_target_override():
    """The X-DDP-Environment-resolved broker target must reach the backfill
    call -- not silently fall back to this process's own global config."""
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_version.BillVersionSyncService._backfill_missing_versions",
        new=AsyncMock(return_value=1),
    ) as mock_backfill, patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=_CHANGELOG_DISPATCH_RESULT),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 2, "created": True}),
    ):
        await generate_and_store_bill_changelog(
            **_CHANGELOG_KWARGS,
            broker_api_base="http://localhost:8080",
            broker_api_token="dev-token",
        )

    mock_backfill.assert_awaited_once_with(
        bill_openstates_id=_CHANGELOG_KWARGS["bill_openstates_id"],
        jurisdiction_code=_CHANGELOG_KWARGS["jurisdiction"],
        session_code=_CHANGELOG_KWARGS["session_code"],
        versions=_RESOLVED_ONE_TRANSITION["versions"],
        latest_version={"date": "2026-02-01", "note": "Engrossed"},
        broker_api_base="http://localhost:8080",
        broker_api_token="dev-token",
    )


# ---------------------------------------------------------------------------
# dispatch_and_record_bill_artifact -- on-demand single-bill endpoint (SYNC-10)
# ---------------------------------------------------------------------------

_ONDEMAND_KWARGS = dict(
    **_COMMON_KWARGS,
    bill_source=_BILL_SOURCE_URL,
    broker_api_base="http://localhost:8080",
    broker_api_token="dev-token",
)


@pytest.mark.asyncio
async def test_writes_pending_row_before_dispatching():
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ) as mock_write, patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": False}),
    ) as mock_generate:
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_summary")

    pending_kwargs = mock_write.await_args.kwargs
    assert pending_kwargs["status"] == "pending"
    assert pending_kwargs["content"] == ""
    assert pending_kwargs["broker_api_base"] == "http://localhost:8080"
    assert pending_kwargs["broker_api_token"] == "dev-token"
    mock_generate.assert_awaited_once()


@pytest.mark.asyncio
async def test_happy_path_delegates_to_generate_and_store_bill_artifact_with_broker_target():
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": False}),
    ) as mock_generate:
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_pros_cons")

    call_kwargs = mock_generate.await_args.kwargs
    assert call_kwargs["artifact_type"] == "bill_pros_cons"
    assert "bill_source" not in call_kwargs
    assert call_kwargs["broker_api_base"] == "http://localhost:8080"
    assert call_kwargs["broker_api_token"] == "dev-token"


@pytest.mark.asyncio
async def test_bill_changelog_routes_to_changelog_function_not_artifact_one():
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_artifact, patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_changelog",
        new=AsyncMock(return_value={"id": 1, "created": False}),
    ) as mock_changelog:
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_changelog")

    mock_artifact.assert_not_awaited()
    mock_changelog.assert_awaited_once()
    call_kwargs = mock_changelog.await_args.kwargs
    assert "bill_source" not in call_kwargs
    assert call_kwargs["broker_api_base"] == "http://localhost:8080"
    assert call_kwargs["broker_api_token"] == "dev-token"


@pytest.mark.asyncio
async def test_bill_changelog_not_applicable_resolves_the_pending_row_to_failed():
    """SYNC-44: generate_and_store_bill_changelog now returns status=
    "not_applicable" without writing anything for a bill with no version
    transition ready yet. session_pipeline_runner.py's batch caller is fine
    with that (nothing to skip past). This caller already wrote a `pending`
    row above before dispatching, though, and ddp-next polls it -- left
    untouched, it would hang forever. There's no broker-side
    "not_applicable" status, so this resolves the placeholder to `failed`
    with a reason string distinct from a real generation failure."""
    mock_write = AsyncMock(side_effect=[{"id": 1, "created": True}, {"id": 1, "created": False}])
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=mock_write,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_changelog",
        new=AsyncMock(return_value={"status": "not_applicable"}),
    ):
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_changelog")

    assert mock_write.await_count == 2
    resolved_kwargs = mock_write.await_args_list[1].kwargs
    assert resolved_kwargs["status"] == "failed"
    assert resolved_kwargs["failure_reason"] == "no_version_transition_available"


@pytest.mark.asyncio
async def test_pending_write_failure_never_dispatches():
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(side_effect=BrokerClientError("broker unreachable")),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_artifact",
        new=AsyncMock(),
    ) as mock_generate:
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_summary")

    mock_generate.assert_not_awaited()


@pytest.mark.asyncio
async def test_legbot_dispatch_error_writes_a_failed_row():
    mock_write = AsyncMock(side_effect=[{"id": 1, "created": True}, {"id": 1, "created": False}])
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=mock_write,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_artifact",
        new=AsyncMock(side_effect=LegBotDispatchError("LegBot task timed out")),
    ):
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_summary")

    assert mock_write.await_count == 2
    failed_kwargs = mock_write.await_args_list[1].kwargs
    assert failed_kwargs["status"] == "failed"
    assert failed_kwargs["failure_stage"] == "dispatch_error"
    assert "LegBot task timed out" in failed_kwargs["failure_reason"]
    assert failed_kwargs["broker_api_base"] == "http://localhost:8080"


@pytest.mark.asyncio
async def test_broker_error_during_dispatch_writes_a_failed_row():
    """generate_and_store_bill_artifact's own internal write can fail
    (BrokerClientError) after LegBot already answered -- this must still
    resolve the pending row, not leave it stuck."""
    mock_write = AsyncMock(side_effect=[{"id": 1, "created": True}, {"id": 1, "created": False}])
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=mock_write,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_artifact",
        new=AsyncMock(side_effect=BrokerClientError("ddp-broker-py rejected the write")),
    ):
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_summary")

    assert mock_write.await_count == 2
    assert mock_write.await_args_list[1].kwargs["status"] == "failed"


@pytest.mark.asyncio
async def test_archived_version_mismatch_writes_a_failed_row_unlike_generate_and_store():
    """Unlike generate_and_store_bill_changelog's own ArchivedVersionMismatchError
    handling (writes nothing, to protect a possibly-good pre-existing row),
    this wrapper resolves its own just-written pending row instead -- the
    only row under this natural key at this point is the pending placeholder
    this same call created, not an arbitrary pre-existing one."""
    mock_write = AsyncMock(side_effect=[{"id": 1, "created": True}, {"id": 1, "created": False}])
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=mock_write,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_changelog",
        new=AsyncMock(side_effect=ArchivedVersionMismatchError("stale version")),
    ):
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_changelog")

    assert mock_write.await_count == 2
    assert mock_write.await_args_list[1].kwargs["status"] == "failed"


@pytest.mark.asyncio
async def test_failed_row_write_itself_failing_is_swallowed_not_raised():
    """Never raises, even in the worst case -- this runs as a fire-and-forget
    BackgroundTasks callback with no caller left to catch anything."""
    mock_write = AsyncMock(
        side_effect=[
            {"id": 1, "created": True},
            BrokerClientError("ddp-broker-py unreachable"),
        ]
    )
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=mock_write,
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.generate_and_store_bill_artifact",
        new=AsyncMock(side_effect=LegBotDispatchError("timed out")),
    ):
        await dispatch_and_record_bill_artifact(**_ONDEMAND_KWARGS, artifact_type="bill_summary")

    assert mock_write.await_count == 2


# ---------------------------------------------------------------------------
# SYNC-43: source_support reaches write_bill_artifact from both success paths
#
# The mapping itself is unit-tested in test_broker_client.py. What is tested
# here is the wiring -- `answer.get("source_support")` at the two call sites
# that write a *complete* artifact. /pm-review pointed out this was the
# load-bearing gap: a regression there stores inferred artifacts unmarked,
# which is the exact failure SYNC-43 exists to prevent, and no test in either
# file would have caught it.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_source_support_reaches_the_artifact_write():
    dispatch_result = {
        "answer": {
            "text": "A plain-language summary.",
            "source_support": "inferred",
            "insufficient_information": False,
        },
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ) as mock_write:
        await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert mock_write.await_args.kwargs["source_support"] == "inferred"


@pytest.mark.asyncio
async def test_source_support_reaches_the_changelog_write():
    dispatch_result = {
        "answer": {
            "sections_added": ["A new section."],
            "sections_modified": [],
            "sections_removed": [],
            "policy_implications": "Broadens eligibility.",
            "source_support": "inferred",
            "insufficient_information": False,
        },
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_version_transitions",
        new=AsyncMock(return_value=_RESOLVED_ONE_TRANSITION),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 3, "created": True}),
    ) as mock_write:
        await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert mock_write.await_args.kwargs["source_support"] == "inferred"


@pytest.mark.asyncio
async def test_a_direct_answer_passes_direct_through_unchanged():
    """The other half: the wiring must not hard-code "inferred"."""
    dispatch_result = {
        "answer": {
            "text": "A plain-language summary.",
            "source_support": "direct",
            "insufficient_information": False,
        },
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ) as mock_write:
        await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    assert mock_write.await_args.kwargs["source_support"] == "direct"


@pytest.mark.asyncio
async def test_a_failed_artifact_carries_no_source_support():
    """A withheld answer has no grounding claim to record, so the failure
    paths deliberately pass nothing."""
    dispatch_result = {
        "answer": {"insufficient_information": True, "source_support": "inferred"},
        "backend": "mlx",
    }
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_question",
        new=AsyncMock(return_value=dispatch_result),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 1, "created": True}),
    ) as mock_write:
        await generate_and_store_bill_artifact(
            **_COMMON_KWARGS, artifact_type="bill_summary"
        )

    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["status"] == "failed"
    assert "source_support" not in write_kwargs
