"""Tests for SYNC-8's jurisdiction-based OpenStates routing in OpenStatesPeopleClient.

Mirrors tests/test_bill_sync_openstates_routing.py's structure (SYNC-6), adapted to
OpenStatesPeopleClient's call sites: iter_jurisdiction (jurisdiction known -> can
route) vs fetch_by_id (opaque-ID lookup, no jurisdiction -> always public API).

Unlike the other three files, this client takes explicit constructor kwargs
instead of a `Settings` object (see its module docstring) -- tests exercise both
the routing-enabled construction and the backward-compatible defaults.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.services.openstates_people import OpenStatesPeopleClient


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.openstates_people.httpx.AsyncClient", return_value=cm)


def _make_client(ddp_openstates_jurisdictions=None) -> OpenStatesPeopleClient:
    return OpenStatesPeopleClient(
        api_key="public-key",
        openstates_api_base="https://v3.openstates.org",
        local_openstates_api_base="http://localhost:8002",
        local_openstates_api_key="local-key",
        ddp_openstates_jurisdictions=ddp_openstates_jurisdictions,
    )


def _mock_response(data: dict, status_code: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.headers = {}
    response.json.return_value = data
    return response


# --- iter_jurisdiction ---------------------------------------------------


@pytest.mark.asyncio
async def test_iter_jurisdiction_routes_flipped_jurisdiction_to_local_replica():
    client = _make_client(ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": []})

    with _patch_async_client(mock_client):
        async for _ in client.iter_jurisdiction("va"):
            pass

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "http://localhost:8002/people"
    assert "x-api-key" not in called_kwargs["headers"]
    assert ("apikey", "local-key") in called_kwargs["params"]


@pytest.mark.asyncio
async def test_iter_jurisdiction_routes_non_flipped_jurisdiction_to_public_api():
    client = _make_client(ddp_openstates_jurisdictions=["us"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": []})

    with _patch_async_client(mock_client):
        async for _ in client.iter_jurisdiction("fl"):
            pass

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


@pytest.mark.asyncio
async def test_iter_jurisdiction_empty_jurisdiction_list_preserves_public_api_behavior():
    """Regression guard: default (unset) config must not change today's behavior."""
    client = _make_client(ddp_openstates_jurisdictions=[])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": []})

    with _patch_async_client(mock_client):
        async for _ in client.iter_jurisdiction("va"):
            pass

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


# --- fetch_by_id: always public (no jurisdiction) ------------------------


@pytest.mark.asyncio
async def test_fetch_by_id_always_uses_public_api_even_when_jurisdictions_flipped():
    client = _make_client(ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {"results": [{"id": "ocd-person/123", "name": "Someone", "current_role": {}}]}
    )

    with _patch_async_client(mock_client):
        person = await client.fetch_by_id("ocd-person/123")

    assert person is not None
    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["x-api-key"] == "public-key"


# --- constructor backward compatibility -----------------------------------


@pytest.mark.asyncio
async def test_constructor_without_routing_kwargs_defaults_to_class_constant_and_no_routing():
    """No settings-derived kwargs passed (as all pre-SYNC-8 callers do) ->
    unchanged behavior: BASE_URL, public header auth, no routing possible."""
    client = OpenStatesPeopleClient(api_key="only-key")
    assert client.openstates_api_base == OpenStatesPeopleClient.BASE_URL
    assert client.ddp_openstates_jurisdictions == []

    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response({"results": []})

    with _patch_async_client(mock_client):
        async for _ in client.iter_jurisdiction("va"):
            pass

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == f"{OpenStatesPeopleClient.BASE_URL}/people"
    assert called_kwargs["headers"]["x-api-key"] == "only-key"


# --- _get_api_base_and_key helper directly ---------------------------------


def test_get_api_base_and_key_helper_is_case_insensitive():
    client = _make_client(ddp_openstates_jurisdictions=["va"])
    assert client._get_api_base_and_key("VA") == ("http://localhost:8002", "local-key", True)
    assert client._get_api_base_and_key("fl") == ("https://v3.openstates.org", "public-key", False)


def test_get_api_base_and_key_helper_empty_list_always_public():
    client = _make_client(ddp_openstates_jurisdictions=[])
    assert client._get_api_base_and_key("us") == ("https://v3.openstates.org", "public-key", False)
