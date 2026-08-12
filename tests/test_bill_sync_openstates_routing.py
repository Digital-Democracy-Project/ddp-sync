"""Tests for SYNC-6's jurisdiction-based OpenStates routing.

Covers config parsing (config.py's openstates_api_base / ddp_openstates_jurisdictions)
and BillSyncService.fetch_bill_from_openstates()'s routing between the public
v3.openstates.org API and the local OpenStates replica, mirroring
ddp-broker-py's OpenStatesService._get_client_for_jurisdiction().
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings, _load_from_env
from ddp_sync.pipelines.bill_sync import BillSyncService


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.pipelines.bill_sync.httpx.AsyncClient", return_value=cm)


def _make_service(**settings_overrides) -> BillSyncService:
    settings = SyncSettings(
        openai_api_key="test-openai-key",  # EmbeddingService constructs AsyncOpenAI eagerly
        openstates_api_key="public-key",
        openstates_api_base="https://v3.openstates.org",
        local_openstates_api_base="http://localhost:8002",
        local_openstates_api_key="local-key",
        **settings_overrides,
    )
    return BillSyncService(settings)


def _mock_response(bill_data: dict) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = bill_data
    response.raise_for_status.return_value = None
    return response


# --- Config parsing -------------------------------------------------------


def test_openstates_api_base_env_var_override(monkeypatch):
    monkeypatch.setenv("OPENSTATES_API_BASE", "https://openstates.example.test")
    raw = _load_from_env()
    assert raw["openstates_api_base"] == "https://openstates.example.test"


def test_openstates_api_base_defaults_to_public_api(monkeypatch):
    monkeypatch.delenv("OPENSTATES_API_BASE", raising=False)
    raw = _load_from_env()
    assert raw["openstates_api_base"] == "https://v3.openstates.org"


def test_ddp_openstates_jurisdictions_parses_comma_separated_list(monkeypatch):
    monkeypatch.setenv("DDP_OPENSTATES_JURISDICTIONS", "mi, ut,US , va")
    raw = _load_from_env()
    assert raw["ddp_openstates_jurisdictions"] == ["MI", "UT", "US", "VA"]


def test_ddp_openstates_jurisdictions_defaults_to_empty_list(monkeypatch):
    monkeypatch.delenv("DDP_OPENSTATES_JURISDICTIONS", raising=False)
    raw = _load_from_env()
    assert raw["ddp_openstates_jurisdictions"] == []


# --- Routing ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_routes_flipped_jurisdiction_to_local_replica():
    service = _make_service(ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"id": "ocd-bill/123"})

    with _patch_async_client(mock_client):
        result = await service.fetch_bill_from_openstates("us", "119", "HR1")

    assert result == {"id": "ocd-bill/123"}
    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "http://localhost:8002/bills/us/119/HR1"
    # Local api-v3's apikey_auth is a query param, not the public API's
    # x-api-key header (matches local_openstates_client.py's convention).
    assert "x-api-key" not in called_kwargs["headers"]
    assert ("apikey", "local-key") in called_kwargs["params"]


@pytest.mark.asyncio
async def test_routing_is_case_insensitive():
    service = _make_service(ddp_openstates_jurisdictions=["us"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"id": "ocd-bill/123"})

    with _patch_async_client(mock_client):
        await service.fetch_bill_from_openstates("US", "119", "HR1")

    called_url = mock_client.get.call_args[0][0]
    assert called_url == "http://localhost:8002/bills/US/119/HR1"


@pytest.mark.asyncio
async def test_routes_non_flipped_jurisdiction_to_public_api():
    service = _make_service(ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"id": "ocd-bill/456"})

    with _patch_async_client(mock_client):
        result = await service.fetch_bill_from_openstates("fl", "2025", "HB123")

    assert result == {"id": "ocd-bill/456"}
    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/bills/fl/2025/HB123"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


@pytest.mark.asyncio
async def test_empty_jurisdiction_list_preserves_always_public_api_behavior():
    """Regression guard: default (unset) config must not change today's behavior."""
    service = _make_service(ddp_openstates_jurisdictions=[])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"id": "ocd-bill/789"})

    with _patch_async_client(mock_client):
        await service.fetch_bill_from_openstates("us", "119", "HR1")

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/bills/us/119/HR1"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


def test_get_api_base_and_key_helper_directly():
    service = _make_service(ddp_openstates_jurisdictions=["VA"])
    assert service._get_api_base_and_key("va") == ("http://localhost:8002", "local-key", True)
    assert service._get_api_base_and_key("fl") == ("https://v3.openstates.org", "public-key", False)
