"""Tests for the ddp-broker-py BillArtifact write client (ddp-infra Phase 8)."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.services.broker_client import BrokerWriteError, write_bill_artifact


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
        with pytest.raises(BrokerWriteError, match="DDP_BROKER_API_BASE"):
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
        with pytest.raises(BrokerWriteError, match="400"):
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
        with pytest.raises(BrokerWriteError, match="unreachable"):
            await write_bill_artifact(
                bill_openstates_id="abc",
                jurisdiction="FL",
                session_code="2026",
                version_date="2026-01-05",
                version_note="Introduced",
                artifact_type="bill_summary",
                content="A summary.",
            )
