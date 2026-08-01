"""Tests for the ddp-broker-py read/write client (ddp-infra Phases 4 and 8)."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.services.broker_client import (
    BrokerClientError,
    create_concept_statement_set,
    get_concept_statement_set,
    get_latest_bill_version,
    write_bill_artifact,
    write_bill_organization_position,
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
