"""Tests for the ddp-broker-py read/write client (ddp-infra Phases 4 and 8)."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.services.broker_client import (
    BrokerClientError,
    get_latest_bill_version,
    write_bill_artifact,
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
