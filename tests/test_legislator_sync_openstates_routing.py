"""Tests for SYNC-8's jurisdiction-based OpenStates routing in LegislatorSyncService.

Mirrors tests/test_bill_sync_openstates_routing.py's structure (SYNC-6), adapted to
LegislatorSyncService's call sites: fetch_sponsored_bills/fetch_legislator_votes
(jurisdiction known -> can route) vs _get_sponsor_name (opaque-ID lookup, no
jurisdiction -> always public API).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.legislator_sync import LegislatorSyncService


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.pipelines.legislator_sync.httpx.AsyncClient", return_value=cm)


def _make_service(**settings_overrides) -> LegislatorSyncService:
    settings = SyncSettings(
        openai_api_key="test-openai-key",  # EmbeddingService constructs AsyncOpenAI eagerly
        openstates_api_key="public-key",
        openstates_api_base="https://v3.openstates.org",
        local_openstates_api_base="http://localhost:8002",
        local_openstates_api_key="local-key",
        **settings_overrides,
    )
    return LegislatorSyncService(settings)


def _mock_response(data: dict) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = data
    response.raise_for_status.return_value = None
    return response


# --- fetch_sponsored_bills ----------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_sponsored_bills_routes_flipped_jurisdiction_to_local_replica():
    service = _make_service(ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": [], "pagination": {"max_page": 1}})

    with _patch_async_client(mock_client):
        await service.fetch_sponsored_bills(
            "ocd-person/123", "va", sponsor_name="Smith"
        )

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "http://localhost:8002/bills"
    assert "x-api-key" not in called_kwargs["headers"]
    assert called_kwargs["params"]["apikey"] == "local-key"


@pytest.mark.asyncio
async def test_fetch_sponsored_bills_routes_non_flipped_jurisdiction_to_public_api():
    service = _make_service(ddp_openstates_jurisdictions=["US"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": [], "pagination": {"max_page": 1}})

    with _patch_async_client(mock_client):
        await service.fetch_sponsored_bills(
            "ocd-person/123", "fl", sponsor_name="Smith"
        )

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/bills"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


@pytest.mark.asyncio
async def test_fetch_sponsored_bills_empty_jurisdiction_list_preserves_public_api_behavior():
    """Regression guard: default (unset) config must not change today's behavior."""
    service = _make_service(ddp_openstates_jurisdictions=[])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": [], "pagination": {"max_page": 1}})

    with _patch_async_client(mock_client):
        await service.fetch_sponsored_bills(
            "ocd-person/123", "va", sponsor_name="Smith"
        )

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/bills"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


# --- fetch_legislator_votes (list + per-bill detail) --------------------------


@pytest.mark.asyncio
async def test_fetch_legislator_votes_routes_list_and_detail_calls_to_local_replica():
    service = _make_service(ddp_openstates_jurisdictions=["VA"])
    mock_client = AsyncMock()
    list_response = _mock_response(
        {"results": [{"id": "ocd-bill/1"}], "pagination": {"max_page": 1}}
    )
    detail_response = _mock_response({"id": "ocd-bill/1", "identifier": "HB1", "votes": []})
    mock_client.get.side_effect = [list_response, detail_response]

    with _patch_async_client(mock_client):
        await service.fetch_legislator_votes("ocd-person/123", "va", max_bills=1)

    list_call, detail_call = mock_client.get.call_args_list
    assert list_call[0][0] == "http://localhost:8002/bills"
    assert detail_call[0][0] == "http://localhost:8002/bills/ocd-bill/1"
    assert "x-api-key" not in list_call[1]["headers"]
    assert list_call[1]["params"]["apikey"] == "local-key"
    assert ("apikey", "local-key") in detail_call[1]["params"]


@pytest.mark.asyncio
async def test_fetch_legislator_votes_routes_non_flipped_jurisdiction_to_public_api():
    service = _make_service(ddp_openstates_jurisdictions=["US"])
    mock_client = AsyncMock()
    list_response = _mock_response(
        {"results": [{"id": "ocd-bill/1"}], "pagination": {"max_page": 1}}
    )
    detail_response = _mock_response({"id": "ocd-bill/1", "identifier": "HB1", "votes": []})
    mock_client.get.side_effect = [list_response, detail_response]

    with _patch_async_client(mock_client):
        await service.fetch_legislator_votes("ocd-person/123", "fl", max_bills=1)

    list_call, detail_call = mock_client.get.call_args_list
    assert list_call[0][0] == "https://v3.openstates.org/bills"
    assert detail_call[0][0] == "https://v3.openstates.org/bills/ocd-bill/1"
    assert list_call[1]["headers"]["x-api-key"] == "public-key"


# --- _get_sponsor_name: always public (no jurisdiction) -----------------------


@pytest.mark.asyncio
async def test_get_sponsor_name_always_uses_public_api_even_when_jurisdictions_flipped():
    service = _make_service(ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {"results": [{"family_name": "Smith", "name": "Jane Smith"}]}
    )

    with _patch_async_client(mock_client):
        name = await service._get_sponsor_name("ocd-person/123")

    assert name == "Smith"
    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


# --- _get_api_base_and_key helper directly -----------------------------------


def test_get_api_base_and_key_helper_is_case_insensitive():
    service = _make_service(ddp_openstates_jurisdictions=["va"])
    assert service._get_api_base_and_key("VA") == ("http://localhost:8002", "local-key", True)
    assert service._get_api_base_and_key("fl") == ("https://v3.openstates.org", "public-key", False)


def test_get_api_base_and_key_helper_empty_list_always_public():
    service = _make_service(ddp_openstates_jurisdictions=[])
    assert service._get_api_base_and_key("us") == ("https://v3.openstates.org", "public-key", False)
