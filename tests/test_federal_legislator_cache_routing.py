"""Tests for SYNC-8's jurisdiction-based OpenStates routing in FederalLegislatorCache.

Mirrors tests/test_bill_sync_openstates_routing.py's structure (SYNC-6), adapted to
FederalLegislatorCache.refresh()/_fetch_chamber_members(), which always fetch
jurisdiction "us" -- so routing here is a single per-refresh() decision rather
than one per call site.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.sync import federal_legislator_cache as flc_module
from ddp_sync.sync.federal_legislator_cache import FederalLegislatorCache


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.sync.federal_legislator_cache.httpx.AsyncClient", return_value=cm)


def _make_cache(tmp_path, monkeypatch, **settings_overrides) -> FederalLegislatorCache:
    # refresh() writes a JSON cache file at the module-level CACHE_FILE path --
    # redirect it under tmp_path so tests never touch the real repo tree.
    cache_dir = tmp_path / "cache"
    monkeypatch.setattr(flc_module, "CACHE_DIR", cache_dir)
    monkeypatch.setattr(flc_module, "CACHE_FILE", cache_dir / "federal_legislators.json")

    defaults = {
        "openstates_api_key": "public-key",
        "openstates_api_base": "https://v3.openstates.org",
        "local_openstates_api_base": "http://localhost:8002",
        "local_openstates_api_key": "local-key",
    }
    defaults.update(settings_overrides)
    settings = SyncSettings(**defaults)
    return FederalLegislatorCache(settings)


def _mock_response(data: dict) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = data
    response.raise_for_status.return_value = None
    return response


_EMPTY_PAGE = {"results": []}


# --- refresh() / _fetch_chamber_members --------------------------------------


@pytest.mark.asyncio
async def test_refresh_routes_to_local_replica_when_us_flipped(tmp_path, monkeypatch):
    cache = _make_cache(tmp_path, monkeypatch, ddp_openstates_jurisdictions=["US", "VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_EMPTY_PAGE)

    with _patch_async_client(mock_client):
        result = await cache.refresh()

    assert result["success"] is True
    # Two calls: Senate (upper) then House (lower), both against the replica.
    assert len(mock_client.get.call_args_list) == 2
    for call in mock_client.get.call_args_list:
        called_url, called_kwargs = call
        assert called_url[0] == "http://localhost:8002/people"
        assert "X-API-KEY" not in called_kwargs["headers"]
        assert called_kwargs["params"]["apikey"] == "local-key"


@pytest.mark.asyncio
async def test_refresh_routes_to_public_api_when_us_not_flipped(tmp_path, monkeypatch):
    cache = _make_cache(tmp_path, monkeypatch, ddp_openstates_jurisdictions=["VA"])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_EMPTY_PAGE)

    with _patch_async_client(mock_client):
        result = await cache.refresh()

    assert result["success"] is True
    called_url, called_kwargs = mock_client.get.call_args_list[0]
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["X-API-KEY"] == "public-key"


@pytest.mark.asyncio
async def test_refresh_empty_jurisdiction_list_preserves_public_api_behavior(tmp_path, monkeypatch):
    """Regression guard: default (unset) config must not change today's behavior."""
    cache = _make_cache(tmp_path, monkeypatch, ddp_openstates_jurisdictions=[])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(_EMPTY_PAGE)

    with _patch_async_client(mock_client):
        result = await cache.refresh()

    assert result["success"] is True
    called_url, called_kwargs = mock_client.get.call_args_list[0]
    assert called_url[0] == "https://v3.openstates.org/people"
    assert called_kwargs["headers"]["X-API-KEY"] == "public-key"


@pytest.mark.asyncio
async def test_refresh_fails_gracefully_when_routed_local_key_missing(tmp_path, monkeypatch):
    """US flipped to the replica, but no local key configured -- should
    report the *local* key as missing, not silently fall back to the (also
    present) public key."""
    cache = _make_cache(
        tmp_path,
        monkeypatch,
        ddp_openstates_jurisdictions=["US"],
        local_openstates_api_key="",
    )
    result = await cache.refresh()
    assert result["success"] is False
    assert "Local OpenStates replica API key" in result["error"]


@pytest.mark.asyncio
async def test_refresh_fails_gracefully_when_public_key_missing(tmp_path, monkeypatch):
    cache = _make_cache(
        tmp_path,
        monkeypatch,
        ddp_openstates_jurisdictions=[],
        openstates_api_key="",
    )
    result = await cache.refresh()
    assert result["success"] is False
    assert result["error"] == "OpenStates API key not configured"


# --- _get_api_base_and_key helper directly -----------------------------------


def test_get_api_base_and_key_helper_is_case_insensitive(tmp_path, monkeypatch):
    cache = _make_cache(tmp_path, monkeypatch, ddp_openstates_jurisdictions=["us"])
    assert cache._get_api_base_and_key("US") == ("http://localhost:8002", "local-key", True)
    assert cache._get_api_base_and_key("va") == ("https://v3.openstates.org", "public-key", False)


def test_get_api_base_and_key_helper_empty_list_always_public(tmp_path, monkeypatch):
    cache = _make_cache(tmp_path, monkeypatch, ddp_openstates_jurisdictions=[])
    assert cache._get_api_base_and_key("us") == ("https://v3.openstates.org", "public-key", False)
