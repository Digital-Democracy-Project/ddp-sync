"""Tests for the Organization Position Research pipeline (ddp-infra's
PLAN-bill-document-provenance.md Phase 8, approved 2026-08-01 after 4 rounds
of /pm-review): find_bill_positions -> per-org verify_bill_position -> write.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.bill_organization_position_research import (
    generate_and_store_bill_organization_positions,
    generate_and_store_organization_content,
)
from ddp_sync.services.broker_client import BrokerClientError
from ddp_sync.services.legbot_client import LegBotDispatchError

_COMMON_KWARGS = dict(
    bill_openstates_id="8d71a94e-0000-0000-0000-000000000001",
    jurisdiction="FL",
    session_code="2026",
    version_date="2026-01-05",
    version_note="Introduced",
    gov_id="HB123",
    bill_title="An act relating to test fixtures",
)

# _resolve_bill_source has no live-URL fallback anymore -- it either returns
# ddp-open-states' own archived text or None. Every test in this file gets
# archived text by default so it reaches dispatch; tests exercising the
# "nothing archived" path override this to return None instead.
_ARCHIVED_TEXT = "ARCHIVED BILL TEXT FOR TESTING"


@pytest.fixture(autouse=True)
def archived_text_by_default():
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research._resolve_bill_source",
        new=AsyncMock(return_value=_ARCHIVED_TEXT),
    ) as mock_resolve:
        yield mock_resolve


@pytest.fixture(autouse=True)
def mock_research_run_write():
    """PLAN-bill-document-provenance.md's "Step 1, scoped version" (approved
    2026-08-01): every test in this file gets this mocked by default so
    existing tests don't need to know about it -- specific tests below
    assert on its call args directly."""
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_research_run",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_run:
        yield mock_run


def _find_result(positions, insufficient_information=False):
    return {
        "answer": {"positions": positions, "insufficient_information": insufficient_information},
        "backend": "openai",
    }


def _verify_result(verdict="confirmed", insufficient_information=False, content_incomplete=False):
    return {
        "answer": {
            "verdict": verdict,
            "insufficient_information": insufficient_information,
            "content_looks_incomplete": content_incomplete,
            "explanation": "explanation text",
        },
        "backend": "openai",
    }


@pytest.mark.asyncio
async def test_no_positions_found_writes_nothing(mock_research_run_write):
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result([])),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert result == []
    mock_write.assert_not_awaited()
    # The tracking record IS still written -- this is the whole point of
    # BillOrganizationResearchRun: distinguish "researched, found nothing"
    # from "never touched."
    mock_research_run_write.assert_awaited_once()
    assert mock_research_run_write.await_args.kwargs["positions_found_count"] == 0


@pytest.mark.asyncio
async def test_insufficient_information_writes_nothing(mock_research_run_write):
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(
            return_value=_find_result(
                [{"org_name": "Sierra Club", "position": "support", "citation_url": "https://x.invalid"}],
                insufficient_information=True,
            )
        ),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert result == []
    mock_write.assert_not_awaited()
    mock_research_run_write.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_archived_text_skips_entirely_no_dispatch(
    archived_text_by_default, mock_research_run_write
):
    """When ddp-open-states has nothing archived for this bill,
    find_bill_positions is never dispatched at all -- no live-URL fallback,
    no tracking row, no organization rows. Matches
    generate_and_store_bill_artifact's own no-archived-text posture."""
    archived_text_by_default.return_value = None
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(),
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert result == []
    mock_dispatch.assert_not_awaited()
    mock_write.assert_not_awaited()
    mock_research_run_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_failure_never_writes_research_run(mock_research_run_write):
    """A transient dispatch failure (LegBotDispatchError) must never be
    mistaken for "researched, found nothing" -- no BillOrganizationResearchRun
    row, so the bill stays correctly eligible for a retry."""
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(side_effect=LegBotDispatchError("timed out")),
    ):
        with pytest.raises(LegBotDispatchError):
            await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    mock_research_run_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_happy_path_records_positions_found_count(mock_research_run_write):
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
        {"org_name": "Chamber of Commerce", "position": "oppose", "citation_url": "https://b.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_verify_result()),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(side_effect=[{"id": 1}, {"id": 2}]),
    ):
        await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    mock_research_run_write.assert_awaited_once()
    assert mock_research_run_write.await_args.kwargs["positions_found_count"] == 2
    assert mock_research_run_write.await_args.kwargs["bill_openstates_id"] == _COMMON_KWARGS["bill_openstates_id"]


@pytest.mark.asyncio
async def test_research_run_write_failure_does_not_abort_the_rest(mock_research_run_write):
    """A failure recording the tracking record is isolated, not fatal -- the
    real organization findings below still get researched and written."""
    mock_research_run_write.side_effect = BrokerClientError("unreachable")
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_verify_result()),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 1}),
    ):
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert len(result) == 1
    assert result[0]["outcome"] == "written"


@pytest.mark.asyncio
async def test_happy_path_verifies_and_writes_each_organization():
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid", "citation_excerpt": "excerpt a"},
        {"org_name": "Chamber of Commerce", "position": "oppose", "citation_url": "https://b.invalid", "citation_excerpt": "excerpt b"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_verify_result()),
    ) as mock_verify, patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(side_effect=[{"id": 1}, {"id": 2}]),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert len(result) == 2
    assert all(r["outcome"] == "written" for r in result)
    assert [r["position_id"] for r in result] == [1, 2]

    assert mock_verify.await_count == 2
    first_call_args = mock_verify.await_args_list[0].args
    assert first_call_args[0] == "https://a.invalid"
    assert "Sierra Club" in first_call_args[1]
    assert "HB123" in first_call_args[1]

    first_write_kwargs = mock_write.await_args_list[0].kwargs
    assert first_write_kwargs["org_name"] == "Sierra Club"
    assert first_write_kwargs["verification_verdict"] == "confirmed"
    assert first_write_kwargs["status"] == "complete"
    # Same invocation_id stamped on both rows.
    assert mock_write.await_args_list[0].kwargs["invocation_id"] == mock_write.await_args_list[1].kwargs["invocation_id"]


@pytest.mark.asyncio
async def test_broker_override_threaded_to_both_write_calls(mock_research_run_write):
    """SYNC-15: a dev-tagged single-bill full-run call must have its org
    research land on the same dev/prod broker instance as the other
    artifact types, not the shared default."""
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_verify_result()),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_write:
        await generate_and_store_bill_organization_positions(
            **_COMMON_KWARGS,
            broker_api_base="http://dev-broker:8080",
            broker_api_token="dev-token",
        )

    assert mock_research_run_write.await_args.kwargs["broker_api_base"] == "http://dev-broker:8080"
    assert mock_research_run_write.await_args.kwargs["broker_api_token"] == "dev-token"
    assert mock_write.await_args.kwargs["broker_api_base"] == "http://dev-broker:8080"
    assert mock_write.await_args.kwargs["broker_api_token"] == "dev-token"


@pytest.mark.asyncio
async def test_truncates_to_cap_and_logs():
    """The cap is settings-driven (org_research_max_organizations, default
    500 -- raised from a hardcoded 20 after a real report that some bills
    draw hundreds of real supporting/opposing organizations), not a fixed
    module constant -- patch a small value here so the test doesn't need to
    construct hundreds of fake positions."""
    test_cap = 20
    many_positions = [
        {"org_name": f"Org {i}", "position": "support", "citation_url": f"https://{i}.invalid"}
        for i in range(test_cap + 5)
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.get_settings",
        return_value=SimpleNamespace(org_research_max_organizations=test_cap, organization_resolution_mode="off", organization_content_enabled=False),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(many_positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_verify_result()),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert len(result) == test_cap
    assert mock_write.await_count == test_cap


@pytest.mark.asyncio
async def test_verification_failure_isolated_to_one_organization():
    """One organization's verify_bill_position dispatch failing must not
    abort the run -- the loop continues, and that row is written with
    status=failed, verification_verdict left at ddp-broker-py's own pending
    default (never overwritten by this pipeline itself)."""
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
        {"org_name": "Chamber of Commerce", "position": "oppose", "citation_url": "https://b.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(side_effect=[LegBotDispatchError("timed out"), _verify_result()]),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(side_effect=[{"id": 1}, {"id": 2}]),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert len(result) == 2
    assert result[0]["outcome"] == "verification_failed"
    assert result[1]["outcome"] == "written"

    first_write_kwargs = mock_write.await_args_list[0].kwargs
    assert first_write_kwargs["status"] == "failed"
    assert first_write_kwargs["failure_stage"] == "verification"
    # verification_verdict is deliberately absent from write_kwargs on
    # failure -- left at ddp-broker-py's own "pending" default, not
    # re-asserted by this pipeline.
    assert "verification_verdict" not in first_write_kwargs


@pytest.mark.asyncio
async def test_broker_write_failure_isolated_to_one_organization():
    """A broker-write failure for one organization must not abort the run
    either -- the loop continues to the next organization."""
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
        {"org_name": "Chamber of Commerce", "position": "oppose", "citation_url": "https://b.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_verify_result()),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(side_effect=[BrokerClientError("unreachable"), {"id": 2}]),
    ):
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert len(result) == 2
    assert result[0]["outcome"] == "broker_write_failed"
    assert result[0]["position_id"] is None
    assert result[1]["outcome"] == "written"


@pytest.mark.asyncio
async def test_find_bill_positions_has_no_arbitrary_timeout_override(
    mock_research_run_write,
):
    """AGENTS-74 / SYNC-37: no per-call timeout_seconds on this dispatch.

    The old value was 240.0, raised from a then-120s default after a real FL
    SJR 2F run finished at ~119s and tripped a spurious client-side timeout.
    That default is now legbot_dispatch_timeout_seconds (1200s), so the
    override stopped raising the ceiling and started lowering it.

    The defect was never the duration, though. When the cap fires, the call
    returns fewer organisations than exist -- and nothing downstream can tell
    "found 3 organisations" from "found 3 before we cut it off", so a
    truncated research pass is indistinguishable from a thorough one and the
    missing organisations look like real absence.

    This is not the same as unbounded: legbot_dispatch_timeout_seconds still
    applies, and LEGBOT_STATE_TIMEOUT_S bounds the CAMS side. Asserting the
    absence of the argument is the point -- a future "just put a cap back"
    edit should have to come past this test and its reasoning.
    """
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result([])),
    ) as mock_dispatch:
        await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    find_call = mock_dispatch.await_args_list[0]
    assert find_call.args[1] == "find_bill_positions"
    assert "timeout_seconds" not in find_call.kwargs

    # And the value it falls back to, not just the absence of the argument.
    # /pm-review's concern was that omitting the override could *lower* the
    # effective timeout if the default were still the old 120s. It is not,
    # and that default lives in this same repo -- there is no cross-repo
    # version to coordinate. Asserting it here means a change to that
    # default has to consider this call site.
    from ddp_sync.config import SyncSettings
    assert SyncSettings().legbot_dispatch_timeout_seconds == 1200.0


# ---------------------------------------------------------------------------
# SYNC-41: LegBot answers that omit "verdict"
#
# LegBot degrades to {"insufficient_information": True, "reason": ...} --
# with no "verdict" and no "explanation" -- whenever it fails before reaching
# the model (backend error, unparseable JSON, unresolved URL). That is its
# documented, intended behavior. A bare verify_answer["verdict"] subscript
# raised KeyError on it, and because the caller wraps this whole function in
# `except Exception`, one unverifiable citation cost the bill its entire
# org-research pass (observed live on FL 2026E HB 5205E: a >100-page PDF the
# Anthropic API rejected on page count).
# ---------------------------------------------------------------------------

def _degraded_verify_result(reason="backend error: a maximum of 100 PDF pages"):
    """LegBot's pre-model degradation shape -- ddp-agents' legbot/handlers.py
    builds exactly this in four places."""
    return {
        "answer": {"insufficient_information": True, "reason": reason},
        "backend": "openai",
    }


@pytest.mark.asyncio
async def test_verify_answer_without_verdict_still_writes_the_row():
    """Acceptance criteria 2 and 5: no "verdict" key must write the row, not
    raise."""
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_degraded_verify_result()),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert len(result) == 1
    assert result[0]["outcome"] == "written"

    write_kwargs = mock_write.await_args_list[0].kwargs
    # "pending", never "" -- ddp-broker-py serializes verification_verdict as
    # a ChoiceField over pending/confirmed/not_confirmed, so a blank string
    # would be a 400. This asserts the value, not just the absence of a raise.
    assert write_kwargs["verification_verdict"] == "pending"
    assert write_kwargs["verification_insufficient_information"] is True
    assert write_kwargs["verification_content_incomplete"] is False


@pytest.mark.asyncio
async def test_verify_answer_reason_is_carried_into_the_explanation():
    """Acceptance criterion 4: the reason must reach somewhere an operator
    can read it. A degraded answer carries "reason", not "explanation" --
    reading only "explanation" would drop the one field saying why the page
    could not be read, conflating it with "the page does not support the
    claim" (AGENTS-65)."""
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_degraded_verify_result(reason="backend error: 100 PDF pages")),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_write:
        await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert mock_write.await_args_list[0].kwargs["verification_explanation"] == (
        "backend error: 100 PDF pages"
    )


@pytest.mark.asyncio
async def test_model_explanation_still_wins_when_both_are_present():
    """The reason fallback must not shadow a real model explanation."""
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
    ]
    verify = _verify_result()
    verify["answer"]["reason"] = "should not be used"
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=verify),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_write:
        await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    write_kwargs = mock_write.await_args_list[0].kwargs
    assert write_kwargs["verification_explanation"] == "explanation text"
    assert write_kwargs["verification_verdict"] == "confirmed"


@pytest.mark.asyncio
async def test_one_unverifiable_citation_does_not_cost_the_other_organizations():
    """Acceptance criterion 3: this is the actual damage the bug did. The
    caller wraps this function in `except Exception`, so the KeyError did not
    just lose one citation -- it lost org research for the whole bill."""
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
        {"org_name": "Chamber of Commerce", "position": "oppose", "citation_url": "https://b.invalid"},
        {"org_name": "Audubon", "position": "support", "citation_url": "https://c.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(side_effect=[
            _degraded_verify_result(),
            _verify_result(),
            _verify_result(verdict="not_confirmed"),
        ]),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(side_effect=[{"id": 1}, {"id": 2}, {"id": 3}]),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert len(result) == 3
    assert [r["outcome"] for r in result] == ["written", "written", "written"]
    assert mock_write.await_count == 3
    verdicts = [c.kwargs["verification_verdict"] for c in mock_write.await_args_list]
    assert verdicts == ["pending", "confirmed", "not_confirmed"]


@pytest.mark.asyncio
async def test_malformed_finding_is_skipped_not_fatal():
    """The wider check SYNC-41 asked for: every field of a finding is
    model-produced too, so a bare finding["citation_url"] was the same latent
    bug one level up. A finding missing a required field skips itself and the
    other organizations are still written."""
    positions = [
        {"org_name": "Sierra Club", "position": "support"},  # no citation_url
        {"org_name": "Chamber of Commerce", "position": "oppose", "citation_url": "https://b.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_verify_result()),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 2}),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert [r["outcome"] for r in result] == ["malformed_finding", "written"]
    assert mock_write.await_count == 1
    assert mock_write.await_args_list[0].kwargs["org_name"] == "Chamber of Commerce"


# --- /pm-review round 1 follow-ups -----------------------------------------

@pytest.mark.asyncio
async def test_non_object_finding_is_skipped_not_fatal():
    """/pm-review caught this: `positions` is a JSON list straight out of the
    model, so an entry can be a null or a bare string, not just a dict with a
    missing key. Those reach .get() and raise AttributeError -- the same
    whole-bill loss the KeyError caused, through a different door."""
    positions = [
        None,
        "Sierra Club supports this bill",
        {"org_name": "Chamber of Commerce", "position": "oppose", "citation_url": "https://b.invalid"},
    ]
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=_verify_result()),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 3}),
    ) as mock_write:
        result = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    assert [r["outcome"] for r in result] == [
        "malformed_finding", "malformed_finding", "written",
    ]
    assert mock_write.await_count == 1
    assert mock_write.await_args_list[0].kwargs["org_name"] == "Chamber of Commerce"


@pytest.mark.asyncio
async def test_null_verdict_is_treated_as_no_verdict():
    """A present-but-null "verdict" is a broker 400 rather than a KeyError,
    so `.get("verdict", "pending")` alone would not have covered it
    (/pm-review). Same for a blank string."""
    positions = [
        {"org_name": "Sierra Club", "position": "support", "citation_url": "https://a.invalid"},
    ]
    verify = {
        "answer": {
            "verdict": None,
            "insufficient_information": True,
            "explanation": "",
            "reason": "backend error: could not read the page",
        },
        "backend": "openai",
    }
    with patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_question",
        new=AsyncMock(return_value=_find_result(positions)),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.dispatch_bill_position_verification",
        new=AsyncMock(return_value=verify),
    ), patch(
        "ddp_sync.pipelines.bill_organization_position_research.write_bill_organization_position",
        new=AsyncMock(return_value={"id": 1}),
    ) as mock_write:
        await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)

    write_kwargs = mock_write.await_args_list[0].kwargs
    assert write_kwargs["verification_verdict"] == "pending"
    # And a blank explanation falls through to reason, rather than writing
    # the empty string -- pinning the `or` rather than fallback-on-missing.
    assert write_kwargs["verification_explanation"] == "backend error: could not read the page"


# ---------------------------------------------------------------------------
# SYNC-107: "About" content for an organization a research run confirmed a position for
# ---------------------------------------------------------------------------

_P = "ddp_sync.pipelines.bill_organization_position_research"


def _position_write(position_id, organization_id, needs_content, verdict="confirmed"):
    return {
        "id": position_id, "result": "created", "verification_verdict": verdict,
        "organization_id": organization_id, "organization_needs_content": needs_content,
    }


def _about_result(**fields):
    return {"answer": fields, "backend": "claude"}


async def _run(positions, writes, *, enabled=True, about=None, content_write=None):
    about = about or AsyncMock(return_value=_about_result(description="A group.", org_type="Advocacy nonprofit", sources=["https://a.invalid"]))
    content_write = content_write or AsyncMock(
        return_value={"id": 7, "written": ["description", "org_type"], "skipped": []}
    )
    with patch(
        f"{_P}.get_settings",
        return_value=SimpleNamespace(org_research_max_organizations=500, organization_resolution_mode="off", organization_content_enabled=enabled),
    ), patch(f"{_P}.dispatch_bill_question", new=AsyncMock(return_value=_find_result(positions))), patch(
        f"{_P}.dispatch_bill_position_verification", new=AsyncMock(return_value=_verify_result())
    ), patch(f"{_P}.write_bill_organization_position", new=AsyncMock(side_effect=writes)), patch(
        f"{_P}.dispatch_describe_organization", new=about
    ), patch(f"{_P}.write_organization_content", new=content_write):
        results = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)
    return results, about, content_write


_ONE = [{"org_name": "New Group", "position": "support", "citation_url": "https://a.invalid"}]


@pytest.mark.asyncio
async def test_a_confirmed_position_for_an_organization_without_content_asks_for_and_saves_its_about_text():
    results, about, content_write = await _run(_ONE, [_position_write(1, 7, True)])

    about.assert_awaited_once_with("New Group")
    kwargs = content_write.await_args.kwargs
    assert kwargs["organization_id"] == 7
    assert kwargs["content"]["description"] == "A group."
    assert kwargs["content"]["org_type"] == "Advocacy nonprofit"  # LegBot answers in the broker's own field names
    assert "sources" not in kwargs["content"]  # LegBot's `sources` is not a broker field
    assert kwargs["source"] == "legbot:describe_organization:claude"
    assert results[0]["about_content"] == {
        "organization_id": 7, "outcome": "written", "written": ["description", "org_type"],
    }


@pytest.mark.asyncio
async def test_nothing_is_asked_when_the_switch_is_off():
    results, about, content_write = await _run(_ONE, [_position_write(1, 7, True)], enabled=False)

    about.assert_not_awaited()
    content_write.assert_not_awaited()
    assert "about_content" not in results[0]


@pytest.mark.asyncio
async def test_an_organization_that_already_has_a_description_is_left_alone():
    _, about, content_write = await _run(_ONE, [_position_write(1, 7, False)])

    about.assert_not_awaited()
    content_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unconfirmed_position_does_not_trigger_content():
    _, about, _ = await _run(_ONE, [_position_write(1, 7, True, verdict="not_confirmed")])

    about.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_organization_named_in_two_findings_is_asked_once():
    two = [
        {"org_name": "New Group", "position": "support", "citation_url": "https://a.invalid"},
        {"org_name": "new group", "position": "support", "citation_url": "https://b.invalid"},
    ]
    results, about, _ = await _run(two, [_position_write(1, 7, True), _position_write(2, 7, True)])

    about.assert_awaited_once()
    assert "about_content" in results[0] and "about_content" not in results[1]


@pytest.mark.asyncio
async def test_a_failed_about_dispatch_keeps_the_position_and_the_rest_of_the_run():
    two = [
        {"org_name": "First Group", "position": "support", "citation_url": "https://a.invalid"},
        {"org_name": "Second Group", "position": "oppose", "citation_url": "https://b.invalid"},
    ]
    about = AsyncMock(side_effect=[LegBotDispatchError("timed out"), _about_result(description="Second.")])
    results, about, content_write = await _run(
        two, [_position_write(1, 7, True), _position_write(2, 8, True)], about=about
    )

    assert [r["outcome"] for r in results] == ["written", "written"]
    assert results[0]["about_content"]["outcome"] == "dispatch_failed"
    assert results[1]["about_content"]["outcome"] == "written"
    assert content_write.await_count == 1


@pytest.mark.asyncio
async def test_a_broker_rejection_of_the_content_is_reported_not_raised():
    results, _, _ = await _run(
        _ONE, [_position_write(1, 7, True)],
        content_write=AsyncMock(side_effect=BrokerClientError("400")),
    )

    assert results[0]["outcome"] == "written"
    assert results[0]["about_content"]["outcome"] == "broker_write_failed"


@pytest.mark.asyncio
async def test_legbot_having_nothing_writes_nothing():
    for answer in ({"insufficient_information": True, "reason": "no results"}, {}, {"description": "  "}):
        _, _, content_write = await _run(
            _ONE, [_position_write(1, 7, True)], about=AsyncMock(return_value={"answer": answer, "backend": "claude"})
        )
        content_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_answers_are_joined_and_the_broker_is_told_which_fields_it_skipped():
    about = AsyncMock(return_value=_about_result(policy_focus_areas=["Housing", "Zoning"], website="https://g.example.org"))
    content_write = AsyncMock(return_value={"id": 7, "written": ["website"], "skipped": ["policy_focus_areas"]})
    outcome = None
    with patch(f"{_P}.dispatch_describe_organization", new=about), patch(
        f"{_P}.write_organization_content", new=content_write
    ):
        outcome = await generate_and_store_organization_content(organization_id=7, org_name="G")

    assert content_write.await_args.kwargs["content"]["policy_focus_areas"] == "Housing; Zoning"
    assert outcome == {"organization_id": 7, "outcome": "written", "written": ["website"]}


@pytest.mark.asyncio
async def test_a_broker_that_does_not_return_the_new_fields_triggers_nothing():
    """Mixed rollout: ddp-sync ahead of BROKER-204 means no organization_id, so nothing is asked."""
    old_broker_answer = {"id": 1, "result": "created", "verification_verdict": "confirmed"}
    results, about, content_write = await _run(_ONE, [old_broker_answer])

    about.assert_not_awaited()
    content_write.assert_not_awaited()
    assert results[0]["organization_id"] is None and results[0]["organization_needs_content"] is False


@pytest.mark.asyncio
async def test_all_eight_broker_fields_pass_through_under_their_own_names():
    fields = {
        "description": "d", "org_type": "t", "policy_focus_areas": "p", "funding": "f",
        "affiliates": "a", "website": "https://w.example.org", "email": "e@w.example.org",
        "contact_page_url": "https://w.example.org/contact",
    }
    content_write = AsyncMock(return_value={"id": 7, "written": list(fields), "skipped": []})
    with patch(
        f"{_P}.dispatch_describe_organization",
        new=AsyncMock(return_value=_about_result(**fields, sources=["https://s.invalid"])),
    ), patch(f"{_P}.write_organization_content", new=content_write):
        await generate_and_store_organization_content(organization_id=7, org_name="G")

    assert content_write.await_args.kwargs["content"] == fields


# ---------------------------------------------------------------------------
# SYNC-97: the resolve step
# ---------------------------------------------------------------------------

_CANDIDATES = [
    {"id": 41, "name": "Example Teachers Association", "aliases": [], "website": "", "parent": None, "chapters": []},
    {"id": 42, "name": "Example Teachers Association of Ohio", "aliases": [], "website": "", "parent": 41, "chapters": []},
]


def _shortlist(attempt_id=900, candidates=None):
    return {"attempt_id": attempt_id, "candidates": _CANDIDATES if candidates is None else candidates}


def _answer(choice, reason="same organization", backend="claude", **extra):
    return {"answer": {"choice": choice, "reason": reason, **extra}, "backend": backend}


async def _run_resolve(
    *, mode="suggest", shortlist=None, answer=None, act=None, writes=None, about_enabled=False,
    positions=None, report=None,
):
    shortlist = shortlist or AsyncMock(return_value=_shortlist())
    answer = answer or AsyncMock(return_value=_answer(None))
    report = report or AsyncMock(return_value={})
    act = act or AsyncMock(return_value={"acted": False, "reason": "act mode is off", "organization_id": None})
    about = AsyncMock(return_value=_about_result(description="A group."))
    with patch(
        f"{_P}.get_settings",
        return_value=SimpleNamespace(
            org_research_max_organizations=500, organization_resolution_mode=mode,
            organization_content_enabled=about_enabled,
        ),
    ), patch(
        f"{_P}.dispatch_bill_question", new=AsyncMock(return_value=_find_result(positions or _ONE))
    ), patch(
        f"{_P}.dispatch_bill_position_verification", new=AsyncMock(return_value=_verify_result())
    ), patch(
        f"{_P}.write_bill_organization_position",
        new=AsyncMock(side_effect=writes or [_position_write(1, 7, True)]),
    ), patch(f"{_P}.get_organization_resolution_shortlist", new=shortlist), patch(
        f"{_P}.dispatch_resolve_organization", new=answer
    ), patch(f"{_P}.report_organization_resolution_answer", new=report), patch(
        f"{_P}.request_organization_resolution_act", new=act
    ), patch(f"{_P}.dispatch_describe_organization", new=about), patch(
        f"{_P}.write_organization_content", new=AsyncMock(return_value={"id": 7, "written": ["description"], "skipped": []})
    ):
        results = await generate_and_store_bill_organization_positions(**_COMMON_KWARGS)
    return results, shortlist, answer, report, act, about


@pytest.mark.asyncio
async def test_suggest_mode_records_the_answer_once_and_never_calls_the_act_endpoint():
    results, shortlist, answer, report, act, _ = await _run_resolve(
        mode="suggest", answer=AsyncMock(return_value=_answer(41, prompt_version="v1"))
    )

    shortlist.assert_awaited_once()
    assert shortlist.await_args.kwargs["position_id"] == 1
    assert answer.await_args.args == ("New Group", _CANDIDATES)
    assert answer.await_args.kwargs["citation_url"] == "https://a.invalid"
    report.assert_awaited_once()
    assert report.await_args.kwargs == {
        "attempt_id": 900, "model": "claude", "resolver_prompt_rev": "v1", "choice": 41,
        "reason": "same organization", "broker_api_base": None, "broker_api_token": None,
    }
    act.assert_not_awaited()
    assert results[0]["resolution"]["outcome"] == "suggested"
    assert results[0]["position_id"] == 1  # the position is still there, as published


@pytest.mark.asyncio
async def test_act_mode_asks_the_broker_to_act_by_attempt_id_for_a_non_null_answer():
    act = AsyncMock(return_value={"acted": True, "reason": "", "organization_id": 41})
    results, _, _, report, act, about = await _run_resolve(
        mode="act", answer=AsyncMock(return_value=_answer(41)), act=act, about_enabled=True
    )

    report.assert_awaited_once()
    act.assert_awaited_once()
    assert act.await_args.kwargs["attempt_id"] == 900
    assert results[0]["resolution"]["outcome"] == "acted"
    assert results[0]["organization_id"] == 41
    about.assert_not_awaited()  # the position now belongs to a reviewed organization


@pytest.mark.asyncio
async def test_act_mode_does_not_act_on_a_null_answer():
    results, _, _, report, act, _ = await _run_resolve(mode="act", answer=AsyncMock(return_value=_answer(None)))

    report.assert_awaited_once()
    act.assert_not_awaited()
    assert results[0]["resolution"]["outcome"] == "null_answer"


@pytest.mark.asyncio
async def test_a_refused_act_request_is_a_normal_outcome():
    results, *_ = await _run_resolve(mode="act", answer=AsyncMock(return_value=_answer(41)))

    assert results[0]["resolution"]["outcome"] == "act_refused"
    assert results[0]["organization_id"] == 7  # unchanged


@pytest.mark.asyncio
async def test_a_choice_outside_the_shortlist_is_reported_as_null_and_never_acted_on():
    results, _, _, report, act, _ = await _run_resolve(mode="act", answer=AsyncMock(return_value=_answer(999)))

    assert report.await_args.kwargs["choice"] is None
    assert "999" in report.await_args.kwargs["reason"]
    act.assert_not_awaited()
    assert results[0]["resolution"]["outcome"] == "null_answer"


@pytest.mark.asyncio
async def test_an_ineligible_position_asks_nothing():
    results, _, answer, report, act, _ = await _run_resolve(
        shortlist=AsyncMock(return_value={"attempt_id": None, "candidates": []})
    )

    answer.assert_not_awaited()
    report.assert_not_awaited()
    act.assert_not_awaited()
    assert results[0]["resolution"]["outcome"] == "not_eligible"


@pytest.mark.asyncio
async def test_an_empty_shortlist_completes_the_attempt_without_asking_the_model():
    results, _, answer, report, _, _ = await _run_resolve(shortlist=AsyncMock(return_value=_shortlist(candidates=[])))

    answer.assert_not_awaited()
    assert report.await_args.kwargs["choice"] is None
    assert results[0]["resolution"]["outcome"] == "no_candidates"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs, outcome, reported",
    [
        ({"shortlist": AsyncMock(side_effect=BrokerClientError("404"))}, "shortlist_failed", False),
        ({"answer": AsyncMock(side_effect=LegBotDispatchError("timeout"))}, "dispatch_failed", False),
        ({"answer": AsyncMock(return_value=_answer(41)), "report": AsyncMock(side_effect=BrokerClientError("409"))},
         "report_failed", True),
    ],
)
async def test_a_failure_leaves_the_stub_and_the_position_is_still_returned(kwargs, outcome, reported):
    results, _, _, report, act, _ = await _run_resolve(mode="act", **kwargs)

    assert results[0]["resolution"]["outcome"] == outcome
    assert results[0]["position_id"] == 1 and results[0]["organization_id"] == 7
    act.assert_not_awaited()
    assert report.await_count == (1 if reported else 0)


@pytest.mark.asyncio
async def test_a_failed_act_request_leaves_the_stub():
    results, *_ = await _run_resolve(
        mode="act", answer=AsyncMock(return_value=_answer(41)),
        act=AsyncMock(side_effect=BrokerClientError("500")),
    )

    assert results[0]["resolution"]["outcome"] == "act_failed"
    assert results[0]["organization_id"] == 7


@pytest.mark.asyncio
async def test_an_unexpected_error_in_the_step_does_not_fail_the_bill():
    results, *_ = await _run_resolve(shortlist=AsyncMock(side_effect=RuntimeError("boom")))

    assert results[0]["position_id"] == 1
    assert results[0]["resolution"]["outcome"] == "failed"


@pytest.mark.asyncio
async def test_the_step_is_off_by_default_and_asks_nothing():
    results, shortlist, answer, report, act, _ = await _run_resolve(mode="off")

    for mock in (shortlist, answer, report, act):
        mock.assert_not_awaited()
    assert "resolution" not in results[0]


@pytest.mark.asyncio
async def test_only_newly_created_confirmed_positions_are_resolved():
    unconfirmed = _position_write(1, 7, True, verdict="not_confirmed")
    existing = {**_position_write(2, 8, True), "result": "updated"}
    for write in (unconfirmed, existing):
        results, shortlist, *_ = await _run_resolve(writes=[write])
        shortlist.assert_not_awaited()
        assert "resolution" not in results[0]


@pytest.mark.asyncio
async def test_a_retry_makes_a_new_attempt():
    shortlist = AsyncMock(side_effect=[_shortlist(attempt_id=900), _shortlist(attempt_id=901)])
    _, _, _, report1, *_ = await _run_resolve(shortlist=shortlist)
    _, _, _, report2, *_ = await _run_resolve(shortlist=shortlist)

    assert report1.await_args.kwargs["attempt_id"] == 900
    assert report2.await_args.kwargs["attempt_id"] == 901
