"""Tests for the ddp-broker-py read/write client (ddp-infra Phases 4 and 8)."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.services.broker_client import (
    BrokerClientError,
    create_concept_statement_set,
    ensure_bill_exists,
    get_bill_artifacts,
    get_bill_organization_positions_status,
    get_concept_statement_set,
    get_latest_bill_version,
    write_bill_artifact,
    write_bill_organization_position,
    write_bill_organization_research_run,
    write_bill_version,
)


@dataclass
class _FakeSettings:
    ddp_broker_api_base: str = "http://localhost:8080"
    ddp_broker_api_token: str = "test-token"


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.broker_client.httpx.AsyncClient", return_value=cm)


@pytest.mark.asyncio
async def test_missing_base_url_raises_immediately():
    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(ddp_broker_api_base=""),
    ):
        with pytest.raises(BrokerClientError, match="DDP_BROKER_API_BASE"):
            await write_bill_artifact(
                bill_openstates_id="abc",
                jurisdiction="FL",
                session_code="2026",
                version_date="2026-01-05",
                version_note="Introduced",
                artifact_type="bill_summary",
                content="text",
            )


@pytest.mark.asyncio
async def test_happy_path_posts_expected_payload_and_returns_result():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 7, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await write_bill_artifact(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            artifact_type="bill_summary",
            content="A summary.",
            model_name="mlx",
        )

    assert result == {"id": 7, "created": True}
    call = mock_client.post.await_args
    assert call.args[0] == "http://localhost:8080/api/bill-artifacts/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"
    assert call.kwargs["json"]["bill_openstates_id"] == "abc"
    assert call.kwargs["json"]["artifact_type"] == "bill_summary"
    assert call.kwargs["json"]["model_name"] == "mlx"
    assert call.kwargs["json"]["compare_version_date"] is None
    assert call.kwargs["json"]["compare_version_note"] is None


@pytest.mark.asyncio
async def test_compare_version_fields_pass_through_the_payload():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 9, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        await write_bill_artifact(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-02-01",
            version_note="Engrossed",
            artifact_type="bill_changelog",
            content="## What changed...",
            compare_version_date="2026-01-01",
            compare_version_note="Introduced",
        )

    call = mock_client.post.await_args
    assert call.kwargs["json"]["compare_version_date"] == "2026-01-01"
    assert call.kwargs["json"]["compare_version_note"] == "Introduced"


# ---------------------------------------------------------------------------
# SYNC-29: failure_reason truncation -- a verbose failure_reason (e.g. a
# backend-error repr) must never cause ddp-broker-py to reject the whole
# write and lose the BillArtifact row entirely.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_overlong_failure_reason_is_truncated_before_posting():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 11, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    overlong = "backend error: " + "x" * 200
    assert len(overlong) > 100

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        await write_bill_artifact(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            artifact_type="bill_changelog",
            content="",
            status="failed",
            failure_stage="generation",
            failure_reason=overlong,
        )

    sent_reason = mock_client.post.await_args.kwargs["json"]["failure_reason"]
    assert len(sent_reason) == 100
    assert sent_reason == overlong[:100]


@pytest.mark.asyncio
async def test_overlong_failure_reason_logs_a_warning():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 14, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.broker_client.logger",
    ) as mock_logger:
        await write_bill_artifact(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            artifact_type="bill_changelog",
            content="",
            status="failed",
            failure_reason="x" * 150,
        )

    mock_logger.warning.assert_called_once()
    assert mock_logger.warning.call_args.kwargs["original_length"] == 150


@pytest.mark.asyncio
async def test_exactly_100_char_failure_reason_is_not_truncated_or_warned():
    """The boundary case: exactly at the limit must pass through unchanged
    and must not log a truncation warning that never actually happened."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 15, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    exactly_100 = "x" * 100

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.broker_client.logger",
    ) as mock_logger:
        await write_bill_artifact(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            artifact_type="bill_summary",
            content="",
            status="failed",
            failure_reason=exactly_100,
        )

    sent_reason = mock_client.post.await_args.kwargs["json"]["failure_reason"]
    assert sent_reason == exactly_100
    mock_logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_failure_reason_within_limit_is_unchanged():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 12, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        await write_bill_artifact(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            artifact_type="bill_summary",
            content="",
            status="failed",
            failure_reason="insufficient_information",
        )

    sent_reason = mock_client.post.await_args.kwargs["json"]["failure_reason"]
    assert sent_reason == "insufficient_information"


@pytest.mark.asyncio
async def test_none_failure_reason_stays_none():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 13, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        await write_bill_artifact(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            artifact_type="bill_summary",
            content="A summary.",
        )

    assert mock_client.post.await_args.kwargs["json"]["failure_reason"] is None


@pytest.mark.asyncio
async def test_write_bill_organization_position_posts_expected_payload():
    """PLAN-bill-document-provenance.md's Organization Position Research
    addition (approved 2026-08-01)."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 42}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await write_bill_organization_position(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            invocation_id="11111111-1111-1111-1111-111111111111",
            org_name="Sierra Club",
            position="support",
            citation_url="https://example.invalid/statement",
            citation_excerpt="We support this bill.",
            find_model_name="openai",
            verification_verdict="confirmed",
            verify_model_name="openai",
            status="complete",
        )

    assert result == {"id": 42}
    call = mock_client.post.await_args
    assert call.args[0] == "http://localhost:8080/api/bill-organization-positions/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"
    payload = call.kwargs["json"]
    assert payload["org_name"] == "Sierra Club"
    assert payload["position"] == "support"
    assert payload["verification_verdict"] == "confirmed"
    assert payload["invocation_id"] == "11111111-1111-1111-1111-111111111111"


@pytest.mark.asyncio
async def test_write_bill_organization_position_error_status_raises():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"org_name": ["This field is required."]}'
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError):
            await write_bill_organization_position(
                bill_openstates_id="abc",
                jurisdiction="FL",
                session_code="2026",
                version_date="2026-01-05",
                version_note="Introduced",
                invocation_id="11111111-1111-1111-1111-111111111111",
                org_name="",
                position="support",
                citation_url="https://example.invalid/statement",
            )


@pytest.mark.asyncio
async def test_error_status_raises_broker_write_error():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"artifact_type": ["not a valid choice"]}'
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="400"):
            await write_bill_artifact(
                bill_openstates_id="abc",
                jurisdiction="FL",
                session_code="2026",
                version_date="2026-01-05",
                version_note="Introduced",
                artifact_type="bill_summary",
                content="A summary.",
            )


@pytest.mark.asyncio
async def test_unreachable_broker_raises_broker_write_error():
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="unreachable"):
            await write_bill_artifact(
                bill_openstates_id="abc",
                jurisdiction="FL",
                session_code="2026",
                version_date="2026-01-05",
                version_note="Introduced",
                artifact_type="bill_summary",
                content="A summary.",
            )


@pytest.mark.asyncio
async def test_ensure_bill_exists_missing_base_url_raises_immediately():
    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(ddp_broker_api_base=""),
    ):
        with pytest.raises(BrokerClientError, match="DDP_BROKER_API_BASE"):
            await ensure_bill_exists(
                jurisdiction="FL",
                session_code="2026",
                gov_id="SJR 2F",
                title="A bill",
                chamber_classification="lower",
                jurisdiction_classification="state",
                bill_openstates_id="11111111-1111-1111-1111-111111111111",
            )


@pytest.mark.asyncio
async def test_ensure_bill_exists_posts_expected_payload_and_returns_result():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"bill_id": 42, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await ensure_bill_exists(
            jurisdiction="FL",
            session_code="2026",
            gov_id="SJR 2F",
            title="A bill",
            chamber_classification="lower",
            jurisdiction_classification="state",
            bill_openstates_id="11111111-1111-1111-1111-111111111111",
        )

    assert result == {"bill_id": 42, "created": True}
    call = mock_client.post.await_args
    assert call.args[0] == "http://localhost:8080/api/bills/ensure/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"
    assert call.kwargs["json"] == {
        "jurisdiction": "FL",
        "session_code": "2026",
        "gov_id": "SJR 2F",
        "title": "A bill",
        "chamber_classification": "lower",
        "jurisdiction_classification": "state",
        "bill_openstates_id": "11111111-1111-1111-1111-111111111111",
    }


@pytest.mark.asyncio
async def test_ensure_bill_exists_error_status_raises_broker_client_error():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"error": "chamber_classification/jurisdiction_classification did not resolve to a known Chamber"}'
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="400"):
            await ensure_bill_exists(
                jurisdiction="FL",
                session_code="2026",
                gov_id="SJR 2F",
                title="A bill",
                chamber_classification="bogus",
                jurisdiction_classification="state",
                bill_openstates_id="11111111-1111-1111-1111-111111111111",
            )


@pytest.mark.asyncio
async def test_ensure_bill_exists_unreachable_broker_raises_broker_client_error():
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="unreachable"):
            await ensure_bill_exists(
                jurisdiction="FL",
                session_code="2026",
                gov_id="SJR 2F",
                title="A bill",
                chamber_classification="lower",
                jurisdiction_classification="state",
                bill_openstates_id="11111111-1111-1111-1111-111111111111",
            )


@pytest.mark.asyncio
async def test_get_latest_bill_version_returns_none_when_not_found():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"found": False}
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_latest_bill_version("abc")

    assert result is None
    call = mock_client.get.await_args
    assert call.args[0] == "http://localhost:8080/api/bill-versions/latest/"
    assert call.kwargs["params"] == {"bill_openstates_id": "abc"}
    # SYNC-40: prod reaches ddp-broker-py through ddp-api, which
    # authenticates every path -- this read 401s without the header.
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"


@pytest.mark.asyncio
async def test_get_latest_bill_version_returns_the_version_when_found():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "found": True,
        "version_date": "2026-01-05",
        "version_note": "Introduced",
        "text_url": "https://example.com/bill.pdf",
        "media_type": "application/pdf",
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_latest_bill_version("abc")

    assert result["version_note"] == "Introduced"
    assert result["text_url"] == "https://example.com/bill.pdf"


@pytest.mark.asyncio
async def test_get_latest_bill_version_raises_on_error_status():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"detail": "bill_openstates_id must be a valid UUID"}'
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="400"):
            await get_latest_bill_version("not-a-uuid")


@pytest.mark.asyncio
async def test_write_bill_version_posts_expected_payload():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 3, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await write_bill_version(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            text_url="https://example.com/bill.pdf",
            media_type="application/pdf",
            chunk_count=5,
            pinecone_ingested=True,
        )

    assert result == {"id": 3, "created": True}
    call = mock_client.post.await_args
    assert call.args[0] == "http://localhost:8080/api/bill-versions/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"
    assert call.kwargs["json"]["version_note"] == "Introduced"
    assert call.kwargs["json"]["chunk_count"] == 5
    assert call.kwargs["json"]["pinecone_ingested"] is True


@pytest.mark.asyncio
async def test_write_bill_version_raises_on_error_status():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"bill_openstates_id": ["No Bill with openstates_id=... exists yet"]}'
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="400"):
            await write_bill_version(
                bill_openstates_id="unknown",
                jurisdiction="FL",
                session_code="2026",
                version_date="2026-01-05",
                version_note="Introduced",
            )


# ---------------------------------------------------------------------------
# ConceptStatementSet read/write (ddp-infra PLAN-bill-concept-polling.md §0.4)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_concept_statement_set_returns_none_when_not_found():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"found": False}
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_concept_statement_set(
            gov_id="abc", jurisdiction_iso2="FL", session_code="2026",
        )

    assert result is None
    call = mock_client.get.await_args
    assert call.args[0] == "http://localhost:8080/api/concept-statements/"
    assert call.kwargs["params"] == {
        "gov_id": "abc", "jurisdiction": "FL", "session": "2026",
    }
    # SYNC-40: this read shipped with no Authorization header. The local
    # dev broker serves it unauthenticated, so nothing here caught it;
    # prod sits behind ddp-api and 401'd every bill of every run.
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"


@pytest.mark.asyncio
async def test_get_concept_statement_set_returns_the_set_when_found():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "found": True,
        "id": 12,
        "statements": ["A statement."],
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_concept_statement_set(
            gov_id="abc", jurisdiction_iso2="FL", session_code="2026",
        )

    assert result["id"] == 12
    assert result["statements"] == ["A statement."]


@pytest.mark.asyncio
async def test_get_concept_statement_set_raises_on_error_status():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"detail": "bad request"}'
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="400"):
            await get_concept_statement_set(
                gov_id="abc", jurisdiction_iso2="FL", session_code="2026",
            )


@pytest.mark.asyncio
async def test_create_concept_statement_set_posts_expected_payload():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {
        "id": 12,
        "gov_id": "abc",
        "jurisdiction_iso2": "FL",
        "session_code": "2026",
        "statements": ["A statement.", "Another statement."],
        "status": "pending",
        "generated_at": "2026-07-30T00:00:00Z",
    }
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await create_concept_statement_set(
            gov_id="abc",
            jurisdiction_iso2="FL",
            session_code="2026",
            statements=["A statement.", "Another statement."],
            source_document_url="https://example.com/bill.pdf",
            model_name="mlx",
        )

    assert result["id"] == 12
    assert result["status"] == "pending"
    call = mock_client.post.await_args
    assert call.args[0] == "http://localhost:8080/api/concept-statement-sets/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"
    assert call.kwargs["json"]["gov_id"] == "abc"
    assert call.kwargs["json"]["jurisdiction_iso2"] == "FL"
    assert call.kwargs["json"]["session_code"] == "2026"
    assert call.kwargs["json"]["statements"] == ["A statement.", "Another statement."]
    assert call.kwargs["json"]["source_document_url"] == "https://example.com/bill.pdf"
    assert call.kwargs["json"]["model_name"] == "mlx"


@pytest.mark.asyncio
async def test_create_concept_statement_set_raises_on_error_status():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"statements": ["This field is required."]}'
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="400"):
            await create_concept_statement_set(
                gov_id="abc",
                jurisdiction_iso2="FL",
                session_code="2026",
                statements=[],
            )


@pytest.mark.asyncio
async def test_create_concept_statement_set_raises_when_unreachable():
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="unreachable"):
            await create_concept_statement_set(
                gov_id="abc",
                jurisdiction_iso2="FL",
                session_code="2026",
                statements=["A statement."],
            )


@pytest.mark.asyncio
async def test_get_bill_artifacts_returns_none_when_not_found():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"found": False}
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_bill_artifacts(jurisdiction="FL", session_code="2026F", gov_id="SJR 2F")

    assert result is None
    call = mock_client.get.await_args
    assert call.args[0] == "http://localhost:8080/api/bill-artifacts/status/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"
    assert call.kwargs["params"] == {
        "jurisdiction": "FL", "session": "2026F", "gov_id": "SJR 2F",
    }


@pytest.mark.asyncio
async def test_get_bill_artifacts_returns_status_including_failed_rows():
    """The whole reason this function calls .../status/ and not .../current/
    -- a failed row must be visible here."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "found": True,
        "bill_version_id": 2,
        "artifacts": {
            "bill_summary": {"status": "complete"},
            "bill_pros_cons": {"status": "failed"},
        },
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_bill_artifacts(jurisdiction="FL", session_code="2026F", gov_id="SJR 2F")

    assert result["artifacts"]["bill_pros_cons"]["status"] == "failed"


@pytest.mark.asyncio
async def test_get_bill_artifacts_raises_on_error_status():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 401
    response.text = '{"detail": "Authentication credentials were not provided."}'
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="401"):
            await get_bill_artifacts(jurisdiction="FL", session_code="2026F", gov_id="SJR 2F")


@pytest.mark.asyncio
async def test_get_bill_organization_positions_status_posts_bill_openstates_id_only():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"has_rows": True, "row_count": 7}
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_bill_organization_positions_status(bill_openstates_id="abc")

    assert result == {"has_rows": True, "row_count": 7}
    call = mock_client.get.await_args
    assert call.args[0] == "http://localhost:8080/api/bill-organization-positions/status/"
    assert call.kwargs["params"] == {"bill_openstates_id": "abc"}


@pytest.mark.asyncio
async def test_get_bill_organization_positions_status_raises_on_error():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"detail": "bill_openstates_id is required"}'
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="400"):
            await get_bill_organization_positions_status(bill_openstates_id="abc")


@pytest.mark.asyncio
async def test_write_bill_organization_research_run_posts_expected_payload():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 9}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await write_bill_organization_research_run(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026F",
            invocation_id="11111111-0000-0000-0000-000000000001",
            positions_found_count=0,
        )

    assert result == {"id": 9}
    call = mock_client.post.await_args
    assert call.args[0] == "http://localhost:8080/api/bill-organization-research-runs/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-token"
    assert call.kwargs["json"]["positions_found_count"] == 0


@pytest.mark.asyncio
async def test_write_bill_organization_research_run_raises_on_error_status():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 400
    response.text = '{"detail": "invalid payload"}'
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        with pytest.raises(BrokerClientError, match="400"):
            await write_bill_organization_research_run(
                bill_openstates_id="abc",
                jurisdiction="FL",
                session_code="2026F",
                invocation_id="11111111-0000-0000-0000-000000000001",
                positions_found_count=0,
            )


# --- broker_api_base/broker_api_token per-call overrides (SYNC-15) --------
#
# get_bill_artifacts, get_bill_organization_positions_status,
# write_bill_organization_position, and write_bill_organization_research_run
# didn't support this before -- only write_bill_artifact (SYNC-10) did.
# SYNC-15's single-bill full-run endpoint needs every one of these to land
# on the same dev/prod broker instance its writes do, so all four gained
# the same optional-override shape write_bill_artifact already established.
# One test each confirms the override wins over settings; the "None
# preserves default" half is already covered by every existing test above,
# which all call these functions without passing an override at all.

@pytest.mark.asyncio
async def test_get_bill_artifacts_override_wins_over_settings():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"found": False}
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(ddp_broker_api_base="http://prod-broker:8080", ddp_broker_api_token="prod-token"),
    ), _patch_async_client(mock_client):
        await get_bill_artifacts(
            jurisdiction="FL", session_code="2026F", gov_id="SJR 2F",
            broker_api_base="http://dev-broker:8080", broker_api_token="dev-token",
        )

    call = mock_client.get.await_args
    assert call.args[0] == "http://dev-broker:8080/api/bill-artifacts/status/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer dev-token"


@pytest.mark.asyncio
async def test_get_concept_statement_set_override_wins_over_settings():
    # SYNC-40: this function always accepted broker_api_token and
    # session_pipeline_runner always passed it -- it was simply never used.
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"found": False}
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(ddp_broker_api_base="http://prod-broker:8080", ddp_broker_api_token="prod-token"),
    ), _patch_async_client(mock_client):
        await get_concept_statement_set(
            gov_id="SJR 2F", jurisdiction_iso2="FL", session_code="2026F",
            broker_api_base="http://dev-broker:8080", broker_api_token="dev-token",
        )

    call = mock_client.get.await_args
    assert call.args[0] == "http://dev-broker:8080/api/concept-statements/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer dev-token"


@pytest.mark.asyncio
async def test_get_bill_organization_positions_status_override_wins_over_settings():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"has_rows": False, "row_count": 0}
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(ddp_broker_api_base="http://prod-broker:8080", ddp_broker_api_token="prod-token"),
    ), _patch_async_client(mock_client):
        await get_bill_organization_positions_status(
            bill_openstates_id="abc",
            broker_api_base="http://dev-broker:8080", broker_api_token="dev-token",
        )

    call = mock_client.get.await_args
    assert call.args[0] == "http://dev-broker:8080/api/bill-organization-positions/status/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer dev-token"


@pytest.mark.asyncio
async def test_write_bill_organization_position_override_wins_over_settings():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 42}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(ddp_broker_api_base="http://prod-broker:8080", ddp_broker_api_token="prod-token"),
    ), _patch_async_client(mock_client):
        await write_bill_organization_position(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026",
            version_date="2026-01-05",
            version_note="Introduced",
            invocation_id="11111111-1111-1111-1111-111111111111",
            org_name="Sierra Club",
            position="support",
            citation_url="https://example.invalid/statement",
            broker_api_base="http://dev-broker:8080", broker_api_token="dev-token",
        )

    call = mock_client.post.await_args
    assert call.args[0] == "http://dev-broker:8080/api/bill-organization-positions/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer dev-token"


@pytest.mark.asyncio
async def test_write_bill_organization_research_run_override_wins_over_settings():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 9}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(ddp_broker_api_base="http://prod-broker:8080", ddp_broker_api_token="prod-token"),
    ), _patch_async_client(mock_client):
        await write_bill_organization_research_run(
            bill_openstates_id="abc",
            jurisdiction="FL",
            session_code="2026F",
            invocation_id="11111111-0000-0000-0000-000000000001",
            positions_found_count=0,
            broker_api_base="http://dev-broker:8080", broker_api_token="dev-token",
        )

    call = mock_client.post.await_args
    assert call.args[0] == "http://dev-broker:8080/api/bill-organization-research-runs/"
    assert call.kwargs["headers"]["Authorization"] == "Bearer dev-token"


# ---------------------------------------------------------------------------
# SYNC-43: LegBot's source_support (AGENTS-80) recorded as a review marker
#
# AGENTS-80 lets LegBot return a weakly-grounded answer instead of
# withholding it, tagged source_support="inferred". Without this, ddp-sync
# dropped the value and stored an inferred artifact looking identical to a
# directly-supported one -- content recovered, but no way for a reader to
# tell a quoted figure from a characterisation the model supplied.
# ---------------------------------------------------------------------------

def _artifact_write_kwargs(**overrides):
    base = dict(
        bill_openstates_id="abc",
        jurisdiction="FL",
        session_code="2026E",
        version_date="2026-01-05",
        version_note="Introduced",
        artifact_type="bill_summary",
        content="A summary.",
    )
    base.update(overrides)
    return base


async def _write_and_capture(**overrides):
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"id": 1, "created": True}
    mock_client.post = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.broker_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        await write_bill_artifact(**_artifact_write_kwargs(**overrides))

    return mock_client.post.await_args.kwargs["json"]


@pytest.mark.asyncio
async def test_inferred_source_support_is_recorded_in_validation_notes():
    """AC1. The marker has to actually reach the wire, not just the docstring."""
    payload = await _write_and_capture(source_support="inferred")

    assert payload["validation_notes"].startswith("source_support=inferred")
    # review_status is NOT sent: it already defaults to pending_review for
    # every artifact, so it cannot discriminate, and it is deliberately not
    # caller-writable -- a generating service must not mark its own output
    # approved.
    assert "review_status" not in payload


@pytest.mark.asyncio
async def test_direct_source_support_writes_no_marker():
    """AC2: byte-identical to a row written before this existed."""
    payload = await _write_and_capture(source_support="direct")
    assert payload["validation_notes"] == ""


@pytest.mark.asyncio
async def test_absent_source_support_behaves_as_direct():
    """AC3, absent half. This is every artifact until AGENTS-80 merges, so it
    must be the quiet path, not a warning on every write."""
    payload = await _write_and_capture()
    assert payload["validation_notes"] == ""


@pytest.mark.asyncio
async def test_unrecognised_source_support_behaves_as_direct_and_warns():
    """AC3, unrecognised half. A producer/consumer mismatch is a real anomaly,
    unlike a merely absent value, so this one is loud."""
    with patch("ddp_sync.services.broker_client.logger") as mock_logger:
        payload = await _write_and_capture(source_support="probably_fine")

    assert payload["validation_notes"] == ""
    assert mock_logger.warning.called
    assert "source_support_unusable" in mock_logger.warning.call_args.args[0]


def test_the_marker_prefix_is_the_queryable_contract():
    """AC4 depends on this prefix being stable: an operator lists a session's
    artifacts awaiting review with
    validation_notes__startswith="source_support=inferred". Reformatting the
    note breaks that query, so the prefix is pinned separately from the prose."""
    from ddp_sync.services.broker_client import _SOURCE_SUPPORT_INFERRED_NOTE

    assert _SOURCE_SUPPORT_INFERRED_NOTE.startswith("source_support=inferred")
