"""Tests for SYNC-8's jurisdiction-based OpenStates routing in LegislatorSyncService.

Mirrors tests/test_bill_sync_openstates_routing.py's structure (SYNC-6), adapted to
LegislatorSyncService's call sites: fetch_sponsored_bills/fetch_legislator_votes, and
(as of SYNC-78) _get_sponsor_name() too when its caller passes a jurisdiction through --
previously _get_sponsor_name() always used the public API regardless, which meant a
legislator present in the RDS replica but not yet in the public API (e.g. someone seated
after the replica became the source of truth for their jurisdiction) could never resolve.
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


# --- _get_sponsor_name: with no jurisdiction argument, unchanged public-API fallback --


@pytest.mark.asyncio
async def test_get_sponsor_name_without_jurisdiction_arg_falls_back_to_public_api():
    """No jurisdiction passed at all (jurisdiction=None, the default) -- preserves
    the exact pre-SYNC-78 behavior for any caller that doesn't have one."""
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


# --- _get_sponsor_name: with a jurisdiction argument (SYNC-78) -----------------


@pytest.mark.asyncio
async def test_get_sponsor_name_with_flipped_jurisdiction_routes_to_local_replica():
    service = _make_service(ddp_openstates_jurisdictions=["US"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {"results": [{"family_name": "Gallagher"}]}
    )

    with _patch_async_client(mock_client):
        name = await service._get_sponsor_name("ocd-person/123", jurisdiction="us")

    assert name == "Gallagher"
    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "http://localhost:8002/people"
    assert "x-api-key" not in called_kwargs["headers"]
    assert ("apikey", "local-key") in called_kwargs["params"]


@pytest.mark.asyncio
async def test_get_sponsor_name_with_non_flipped_jurisdiction_routes_to_public_api():
    service = _make_service(ddp_openstates_jurisdictions=["US"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {"results": [{"family_name": "Smith"}]}
    )

    with _patch_async_client(mock_client):
        name = await service._get_sponsor_name("ocd-person/123", jurisdiction="fl")

    assert name == "Smith"
    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


@pytest.mark.asyncio
async def test_fetch_sponsored_bills_routes_its_own_sponsor_name_lookup_to_local_replica():
    """Integration-level check: fetch_sponsored_bills (no sponsor_name given) must
    pass its own jurisdiction through to _get_sponsor_name, not just to the bills
    fetch itself -- this is the actual SYNC-78 bug (Gallagher/Graham's sponsor-name
    lookup failing on the public API despite the replica already having them)."""
    service = _make_service(ddp_openstates_jurisdictions=["US"])
    mock_client = AsyncMock()
    sponsor_response = _mock_response({"results": [{"family_name": "Gallagher"}]})
    bills_response = _mock_response({"results": [], "pagination": {"max_page": 1}})
    mock_client.get.side_effect = [sponsor_response, bills_response]

    with _patch_async_client(mock_client):
        await service.fetch_sponsored_bills("ocd-person/123", "us")

    sponsor_call, bills_call = mock_client.get.call_args_list
    assert sponsor_call[0][0] == "http://localhost:8002/people"
    assert bills_call[0][0] == "http://localhost:8002/bills"


# --- _get_api_base_and_key helper directly -----------------------------------


def test_get_api_base_and_key_helper_is_case_insensitive():
    service = _make_service(ddp_openstates_jurisdictions=["va"])
    assert service._get_api_base_and_key("VA") == ("http://localhost:8002", "local-key", True)
    assert service._get_api_base_and_key("fl") == ("https://v3.openstates.org", "public-key", False)


def test_get_api_base_and_key_helper_empty_list_always_public():
    service = _make_service(ddp_openstates_jurisdictions=[])
    assert service._get_api_base_and_key("us") == ("https://v3.openstates.org", "public-key", False)
