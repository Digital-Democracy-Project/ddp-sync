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
    assert called_kwargs["headers"]["X-API-Key"] == "local-key"  # SYNC-68: a header, never the url
    assert "apikey" not in dict(called_kwargs["params"])


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


# --- SYNC-93: hosts with no CAMS access route replica jurisdictions to the RDS-backed api-v3 ---


def _make_ec2_source(cams_api_token: str = "", **settings_overrides) -> OpenStatesSource:
    """The EC2 broker host: no CAMS token, a reachable RDS-backed api-v3, and the code-default
    localhost:8002 'local' base that nothing answers there."""
    settings = SyncSettings(
        openstates_api_key="public-key",
        openstates_api_base="https://v3.openstates.org",
        local_openstates_api_base="http://localhost:8002",
        local_openstates_api_key="",
        rds_openstates_api_base="http://10.0.0.11:8002",
        rds_openstates_api_key="rds-key",
        cams_api_token=cams_api_token,
        **settings_overrides,
    )
    return OpenStatesSource(settings)


@pytest.mark.asyncio
async def test_fetch_jurisdiction_on_a_non_mac_host_uses_the_rds_api_with_header_auth():
    source = _make_ec2_source(ddp_openstates_jurisdictions=["UT", "US"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_JURISDICTION_BODY)

    with _patch_async_client(mock_client):
        result = await source.fetch_jurisdiction("ut")

    assert result is not None
    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "http://10.0.0.11:8002/jurisdictions/ut"
    # The RDS-backed api-v3 is called with the X-API-Key header, never the `apikey` query
    # parameter the Mac's local api-v3 uses.
    assert called_kwargs["headers"]["X-API-Key"] == "rds-key"
    assert all(name != "apikey" for name, _ in called_kwargs["params"])


@pytest.mark.asyncio
async def test_fetch_jurisdiction_on_the_mac_still_uses_the_local_replica():
    """A host with a CAMS token is the Mac: unchanged, even when an RDS base is configured."""
    source = _make_ec2_source(ddp_openstates_jurisdictions=["UT"], cams_api_token="cams-token")
    source.settings.local_openstates_api_key = "local-key"
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_JURISDICTION_BODY)

    with _patch_async_client(mock_client):
        await source.fetch_jurisdiction("ut")

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "http://localhost:8002/jurisdictions/ut"
    assert called_kwargs["headers"]["X-API-Key"] == "local-key"  # SYNC-68: a header, never the url
    assert "apikey" not in dict(called_kwargs["params"])


@pytest.mark.asyncio
async def test_fetch_jurisdiction_non_mac_host_without_an_rds_base_keeps_the_local_replica():
    """No RDS base configured (dev, CI): nothing to route to, behavior unchanged."""
    source = _make_ec2_source(ddp_openstates_jurisdictions=["UT"])
    source.settings.rds_openstates_api_base = ""
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_JURISDICTION_BODY)

    with _patch_async_client(mock_client):
        await source.fetch_jurisdiction("ut")

    called_url, _ = mock_client.get.call_args
    assert called_url[0] == "http://localhost:8002/jurisdictions/ut"


@pytest.mark.asyncio
async def test_fetch_jurisdiction_non_mac_host_still_sends_unlisted_jurisdictions_to_the_public_api():
    source = _make_ec2_source(ddp_openstates_jurisdictions=["UT"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_JURISDICTION_BODY)

    with _patch_async_client(mock_client):
        await source.fetch_jurisdiction("fl")

    called_url, called_kwargs = mock_client.get.call_args
    assert called_url[0] == "https://v3.openstates.org/jurisdictions/fl"
    assert called_kwargs["headers"]["X-API-Key"] == "public-key"


def test_get_api_base_and_key_flag_follows_the_instance_chosen():
    ec2 = _make_ec2_source(ddp_openstates_jurisdictions=["UT"])
    assert ec2._get_api_base_and_key("UT") == ("http://10.0.0.11:8002", "rds-key", False)
    mac = _make_ec2_source(ddp_openstates_jurisdictions=["UT"], cams_api_token="t")
    assert mac._get_api_base_and_key("UT") == ("http://localhost:8002", "", True)


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
    assert called_kwargs["headers"]["X-API-Key"] == "local-key"  # SYNC-68: a header, never the url
    assert "apikey" not in dict(called_kwargs["params"])


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
    assert list_call[1]["headers"]["X-API-Key"] == "local-key"  # SYNC-68: a header, never the url
    assert detail_call[1]["headers"]["X-API-Key"] == "local-key"
    assert "apikey" not in list_call[1]["params"]
    assert not detail_call[1].get("params")  # the detail call carries no query string at all
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
