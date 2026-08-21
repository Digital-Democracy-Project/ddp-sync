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

    assert result == {"id": 1, "created": True}
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

    assert result == {"id": 2, "created": True}
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

    assert result == {"id": 4, "created": True}
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

    assert result == {"id": 5, "created": True}
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

    assert result == {"id": 6, "created": True}
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

    assert result == {"id": 3, "created": True}
    write_kwargs = mock_write.await_args.kwargs
    assert write_kwargs["status"] == "failed"
    assert write_kwargs["failure_stage"] == "generation"
    assert write_kwargs["failure_reason"] == "insufficient_information"


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

    assert result == {"id": 8, "created": True}
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

    assert result == {"id": 13, "created": True}
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

    assert result == {"id": 7, "created": True}
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

    assert result == {"id": 8, "created": True}
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
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=_ARCHIVED),
    ), patch(
        "ddp_sync.pipelines.bill_artifact_generation.dispatch_bill_changelog",
        new=AsyncMock(return_value=dispatch_result),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_artifact_generation.write_bill_artifact",
        new=AsyncMock(return_value={"id": 2, "created": True}),
    ) as mock_write:
        result = await generate_and_store_bill_changelog(**_CHANGELOG_KWARGS)

    assert result == {"id": 2, "created": True}
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
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=_ARCHIVED),
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
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=_ARCHIVED),
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

    assert result == {"id": 2, "created": True}
    mock_backfill.assert_awaited_once_with(
        bill_openstates_id=_CHANGELOG_KWARGS["bill_openstates_id"],
        jurisdiction_code=_CHANGELOG_KWARGS["jurisdiction"],
        session_code=_CHANGELOG_KWARGS["session_code"],
        versions=[
            {"date": "2026-01-01", "note": "Introduced"},
            {"date": "2026-02-01", "note": "Engrossed"},
        ],
        latest_version={"date": "2026-02-01", "note": "Engrossed"},
        broker_api_base=None,
        broker_api_token=None,
    )


@pytest.mark.asyncio
async def test_changelog_backfill_threads_broker_target_override():
    """The X-DDP-Environment-resolved broker target must reach the backfill
    call -- not silently fall back to this process's own global config."""
    with patch(
        "ddp_sync.pipelines.bill_artifact_generation.get_archived_changelog_inputs",
        new=AsyncMock(return_value=_ARCHIVED),
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
        versions=[
            {"date": "2026-01-01", "note": "Introduced"},
            {"date": "2026-02-01", "note": "Engrossed"},
        ],
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
