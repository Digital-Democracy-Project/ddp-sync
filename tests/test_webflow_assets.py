"""Unit tests for the WebflowAssetService two-step upload flow."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.services.webflow_assets import (
    AssetReference,
    WebflowAssetError,
    WebflowAssetService,
)


@pytest.mark.asyncio
async def test_upload_from_url_two_step_flow():
    """Happy path: fetch image → POST /assets → PUT bytes → returns
    AssetReference with the field-shape value."""
    service = WebflowAssetService(
        api_token="test-token", site_id="test-site",
    )

    image_bytes = b"\x89PNG\r\n\x1a\nfake-png-bytes"
    fetch_resp = MagicMock(
        status_code=200, content=image_bytes,
        headers={"content-type": "image/png"},
    )
    create_resp = MagicMock(
        status_code=200,
        json=lambda: {
            "id": "asset-abc",
            "hostedUrl": "https://cdn.webflow.com/asset-abc.png",
            "uploadUrl": "https://s3.amazonaws.com/webflow-uploads/signed",
            "uploadDetails": {"key": "abc.png", "signature": "sig"},
        },
    )
    put_resp = MagicMock(status_code=204, headers={}, content=b"")

    # Three sequential httpx.AsyncClient instantiations: fetch, create, put.
    # Each returns its own context manager.
    def make_ctx(get_resp=None, post_resp=None):
        cli = MagicMock()
        cli.get = AsyncMock(return_value=get_resp) if get_resp else AsyncMock()
        cli.post = AsyncMock(return_value=post_resp) if post_resp else AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=cli)
        ctx.__aexit__ = AsyncMock(return_value=None)
        return ctx

    contexts = [
        make_ctx(get_resp=fetch_resp),       # _fetch_image
        make_ctx(post_resp=create_resp),     # _create_asset
        make_ctx(post_resp=put_resp),        # _put_to_signed_url
    ]
    with patch(
        "ddp_sync.services.webflow_assets.httpx.AsyncClient",
        side_effect=contexts,
    ):
        ref = await service.upload_from_url(
            "https://www.flhouse.gov/photo.png",
            alt_text="Jane Rep",
        )

    assert isinstance(ref, AssetReference)
    assert ref.asset_id == "asset-abc"
    assert ref.hosted_url == "https://cdn.webflow.com/asset-abc.png"
    assert ref.alt_text == "Jane Rep"
    assert ref.to_image_field_value() == {
        "fileId": "asset-abc",
        "url": "https://cdn.webflow.com/asset-abc.png",
        "alt": "Jane Rep",
    }


@pytest.mark.asyncio
async def test_upload_from_url_caches_by_source_url():
    """Second call with the same source URL returns the cached
    AssetReference without re-fetching or re-uploading."""
    service = WebflowAssetService(
        api_token="test-token", site_id="test-site",
    )
    # Pre-populate the cache directly to bypass the upload flow
    service._cache["https://example.com/photo.jpg"] = AssetReference(
        asset_id="asset-cached",
        hosted_url="https://cdn.webflow.com/cached.jpg",
        alt_text="Cached",
    )

    # Patch httpx to fail loudly if any HTTP call is attempted
    with patch(
        "ddp_sync.services.webflow_assets.httpx.AsyncClient",
        side_effect=AssertionError("should not hit HTTP on cached source_url"),
    ):
        ref = await service.upload_from_url(
            "https://example.com/photo.jpg",
            alt_text="Updated alt",
        )

    assert ref.asset_id == "asset-cached"
    # The fresh alt_text overrides the cached one
    assert ref.alt_text == "Updated alt"


@pytest.mark.asyncio
async def test_upload_from_url_create_asset_failure_raises_with_body():
    """When POST /assets fails, WebflowAssetError carries the response
    body in str() for diagnostic visibility (round-17 lesson)."""
    service = WebflowAssetService(
        api_token="test-token", site_id="test-site",
    )

    fetch_resp = MagicMock(
        status_code=200, content=b"fake-bytes",
        headers={"content-type": "image/jpeg"},
    )
    create_fail_resp = MagicMock(
        status_code=400,
        text='{"message":"Validation Error","details":[{"param":"fileHash"}]}',
    )

    def make_ctx(get_resp=None, post_resp=None):
        cli = MagicMock()
        cli.get = AsyncMock(return_value=get_resp) if get_resp else AsyncMock()
        cli.post = AsyncMock(return_value=post_resp) if post_resp else AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=cli)
        ctx.__aexit__ = AsyncMock(return_value=None)
        return ctx

    contexts = [
        make_ctx(get_resp=fetch_resp),
        make_ctx(post_resp=create_fail_resp),
    ]
    with patch(
        "ddp_sync.services.webflow_assets.httpx.AsyncClient",
        side_effect=contexts,
    ):
        with pytest.raises(WebflowAssetError) as exc_info:
            await service.upload_from_url(
                "https://example.com/photo.jpg", alt_text="X",
            )

    err = exc_info.value
    assert err.status_code == 400
    # Body is surfaced in str()
    assert "Validation Error" in str(err)
    assert "fileHash" in str(err)


@pytest.mark.asyncio
async def test_upload_from_url_image_too_large_rejected():
    """Source images exceeding MAX_IMAGE_BYTES are rejected before
    any Webflow API calls."""
    service = WebflowAssetService(
        api_token="test-token", site_id="test-site",
    )
    huge_bytes = b"x" * (11 * 1024 * 1024)  # 11 MB
    fetch_resp = MagicMock(
        status_code=200, content=huge_bytes,
        headers={"content-type": "image/jpeg"},
    )

    def make_ctx(get_resp=None):
        cli = MagicMock()
        cli.get = AsyncMock(return_value=get_resp) if get_resp else AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=cli)
        ctx.__aexit__ = AsyncMock(return_value=None)
        return ctx

    with patch(
        "ddp_sync.services.webflow_assets.httpx.AsyncClient",
        return_value=make_ctx(get_resp=fetch_resp),
    ):
        with pytest.raises(WebflowAssetError) as exc_info:
            await service.upload_from_url(
                "https://example.com/big.jpg", alt_text="X",
            )

    assert "exceeds" in str(exc_info.value)


def test_constructor_validates_required_inputs():
    with pytest.raises(ValueError, match="api_token"):
        WebflowAssetService(api_token="", site_id="x")
    with pytest.raises(ValueError, match="site_id"):
        WebflowAssetService(api_token="x", site_id="")


def test_derive_filename_from_url_path():
    """Best-effort filename extraction from URL path."""
    f = WebflowAssetService._derive_filename
    assert f("https://x.com/photo.jpg", "image/jpeg") == "photo.jpg"
    assert f("https://x.com/dir/img.png", "image/png") == "img.png"
    # No extension in path → use content-type fallback
    assert f("https://x.com/photo", "image/png") == "legislator-photo.png"
    # No path → use content-type fallback
    assert f("https://x.com", "image/jpeg") == "legislator-photo.jpg"
