"""Tests for SYNC-8's jurisdiction-based OpenStates routing in OpenStatesSource.

Mirrors tests/test_bill_sync_openstates_routing.py's structure (SYNC-6), adapted to
OpenStatesSource's call sites: fetch_jurisdiction/fetch/fetch_legislators (jurisdiction
known -> can route) vs fetch_bill/fetch_legislator_by_id (opaque-ID lookup, no
jurisdiction -> always public API).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.ingestion.sources.openstates import OpenStatesSource


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.ingestion.sources.openstates.httpx.AsyncClient", return_value=cm)


def _make_source(**settings_overrides) -> OpenStatesSource:
    settings = SyncSettings(
        openstates_api_key="public-key",
        openstates_api_base="https://v3.openstates.org",
        local_openstates_api_base="http://localhost:8002",
        local_openstates_api_key="local-key",
        **settings_overrides,
    )
    return OpenStatesSource(settings)


def _mock_response(data: dict) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = data
    response.raise_for_status.return_value = None
    return response


_JURISDICTION_BODY = {
    "id": "ocd-jurisdiction/country:us/state:va/government",
    "name": "Virginia",
    "classification": "state",
    "url": "https://virginiageneralassembly.gov",
    "latest_bill_update": None,
    "latest_people_update": None,
    "legislative_sessions": [],
    "organizations": [],
}


# --- fetch_jurisdiction ------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_jurisdiction_routes_flipped_jurisdiction_to_local_replica():
    source = _make_source(ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_JURISDICTION_BODY)

    with _patch_async_client(mock_client):
        result = await source.fetch_jurisdiction("va")

    assert result is not None
    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "http://localhost:8002/jurisdictions/va"
    assert "X-API-Key" not in called_kwargs["headers"]
    assert ("apikey", "local-key") in called_kwargs["params"]


@pytest.mark.asyncio
async def test_fetch_jurisdiction_routes_non_flipped_jurisdiction_to_public_api():
    source = _make_source(ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_JURISDICTION_BODY)

    with _patch_async_client(mock_client):
        await source.fetch_jurisdiction("fl")

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/jurisdictions/fl"
    assert called_kwargs["headers"]["X-API-Key"] == "public-key"


@pytest.mark.asyncio
async def test_fetch_jurisdiction_empty_jurisdiction_list_preserves_public_api_behavior():
    """Regression guard: default (unset) config must not change today's behavior."""
    source = _make_source(ddp_openstates_jurisdictions=[])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_JURISDICTION_BODY)

    with _patch_async_client(mock_client):
        await source.fetch_jurisdiction("va")

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/jurisdictions/va"
    assert called_kwargs["headers"]["X-API-Key"] == "public-key"


# --- fetch_legislators --------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_legislators_routes_flipped_jurisdiction_to_local_replica():
    source = _make_source(ddp_openstates_jurisdictions=["va"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": []})

    with _patch_async_client(mock_client):
        async for _ in source.fetch_legislators("VA"):
            pass

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "http://localhost:8002/people"
    assert "X-API-Key" not in called_kwargs["headers"]
    assert ("apikey", "local-key") in called_kwargs["params"]


@pytest.mark.asyncio
async def test_fetch_legislators_routes_non_flipped_jurisdiction_to_public_api():
    source = _make_source(ddp_openstates_jurisdictions=["us"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": []})

    with _patch_async_client(mock_client):
        async for _ in source.fetch_legislators("fl"):
            pass

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["X-API-Key"] == "public-key"


# --- fetch (bills list + per-bill detail) ------------------------------------


@pytest.mark.asyncio
async def test_fetch_with_jurisdiction_routes_list_and_detail_calls_to_local_replica():
    source = _make_source(ddp_openstates_jurisdictions=["VA"])
    mock_client = AsyncMock()
    list_response = _mock_response({"results": [{"id": "ocd-bill/123"}]})
    detail_response = _mock_response({"id": "ocd-bill/123", "title": "A bill"})
    mock_client.get.side_effect = [list_response, detail_response]

    with _patch_async_client(mock_client):
        docs = [doc async for doc in source.fetch(jurisdiction="va")]

    assert len(mock_client.get.call_args_list) == 2
    list_call, detail_call = mock_client.get.call_args_list
    assert list_call[0][0] == "http://localhost:8002/bills"
    assert detail_call[0][0] == "http://localhost:8002/bills/ocd-bill/123"
    # Both calls use the local replica's query-param auth, not the public
    # header scheme.
    assert "X-API-Key" not in list_call[1]["headers"]
    assert list_call[1]["params"]["apikey"] == "local-key"
    assert detail_call[1]["params"] == {"apikey": "local-key"}
    assert docs  # bill had a title, so content extraction succeeded


@pytest.mark.asyncio
async def test_fetch_without_jurisdiction_always_uses_public_api():
    """No jurisdiction given (multi-jurisdiction crawl) -- nothing to route on."""
    source = _make_source(ddp_openstates_jurisdictions=["VA", "US"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": []})

    with _patch_async_client(mock_client):
        async for _ in source.fetch():
            pass

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/bills"
    assert called_kwargs["headers"]["X-API-Key"] == "public-key"


# --- fetch_bill / fetch_legislator_by_id: always public (no jurisdiction) ---


@pytest.mark.asyncio
async def test_fetch_bill_always_uses_public_api_even_when_jurisdictions_flipped():
    source = _make_source(ddp_openstates_jurisdictions=["VA", "US"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {"id": "ocd-bill/123", "title": "A bill", "jurisdiction": {"id": "va"}}
    )

    with _patch_async_client(mock_client):
        await source.fetch_bill("ocd-bill/123")

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/bills/ocd-bill/123"
    assert called_kwargs["headers"]["X-API-Key"] == "public-key"


@pytest.mark.asyncio
async def test_fetch_legislator_by_id_always_uses_public_api_even_when_jurisdictions_flipped():
    source = _make_source(ddp_openstates_jurisdictions=["VA", "US"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {"results": [{"id": "ocd-person/123", "name": "Someone"}]}
    )

    with _patch_async_client(mock_client):
        await source.fetch_legislator_by_id("ocd-person/123")

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["X-API-Key"] == "public-key"


# --- _get_api_base_and_key helper directly -----------------------------------


def test_get_api_base_and_key_helper_is_case_insensitive():
    source = _make_source(ddp_openstates_jurisdictions=["va"])
    assert source._get_api_base_and_key("VA") == ("http://localhost:8002", "local-key", True)
    assert source._get_api_base_and_key("fl") == ("https://v3.openstates.org", "public-key", False)


def test_get_api_base_and_key_helper_empty_list_always_public():
    source = _make_source(ddp_openstates_jurisdictions=[])
    assert source._get_api_base_and_key("us") == ("https://v3.openstates.org", "public-key", False)
