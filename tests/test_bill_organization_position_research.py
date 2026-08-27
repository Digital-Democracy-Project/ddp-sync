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
        return_value=SimpleNamespace(org_research_max_organizations=test_cap),
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
