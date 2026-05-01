"""Webflow Assets v2 API client for the legislator photo upload pipeline.

Phase 3 V1: state-leg photos come from per-state CDNs (FL House CMS,
MA legislature CDN, AZ Apptegy, etc.) with varying stability. The bio
sync's ``photo-source-url`` Link field stores the source URL for the
website to hotlink, but Phase 3 also uploads the image into Webflow's
asset library so the website can render a stable, Webflow-hosted
version via the ``legislator-image`` Image field.

The Webflow Assets v2 API is a two-step upload:
  1. POST /v2/sites/{site_id}/assets with {fileName, fileHash, parentFolder?}
     → returns {id, hostedUrl, uploadUrl, uploadDetails, ...}
  2. POST (S3-compatible multipart form-data) to uploadUrl with
     uploadDetails fields + the file bytes. NOTE: Webflow uses an S3
     POST policy, not PUT — the response from step 1 is a presigned
     URL that expects multipart/form-data, NOT a PUT-with-bytes body.
The asset id can then be set on an Image-typed field via the standard
items PATCH endpoint.

NOTE: Several contracts in this implementation are unverified against
the live Webflow Assets v2 API as of Phase 3 V1 (2026-04-30). The
operator runbook in §Phase 3 of plans/PLAN-legislator-bio-sync.md
covers verification before flipping ``upload_photos`` default-on:
  - fileHash format: MD5 hex (Webflow docs are ambiguous between hex
    and base64; defensive choice). If the live API rejects, switch to
    hashlib.md5(image_bytes).digest() base64-encoded.
  - Image field PATCH payload shape: ``{fileId, url, alt}``. If the
    live API rejects, the alternative is just the asset_id string.
  - Signed-URL upload: POST multipart form-data (this implementation).
    If the live API rejects with 405, switch to PUT raw bytes with
    Content-Type from the source response.

This service:
  - Fetches the image bytes from a source URL (handles redirects, common
    image content-types).
  - Hashes the bytes with MD5 (Webflow's expected hash format) for the
    upload + as the dedup key.
  - Uploads via the two-step flow.
  - Caches source_url → AssetReference in-memory for the lifetime of
    the service instance (one bio-sync run). Redis-backed cross-run
    cache is a Phase 4 follow-up.
  - Surfaces errors as ``WebflowAssetError`` so callers can per-record-
    isolate without aborting the run.

Errors propagate the response body (round-17 lesson — surface 4xx body
on exception str so diagnostics aren't lost).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx
import structlog

logger = structlog.get_logger(__name__)


WEBFLOW_API_BASE = "https://api.webflow.com/v2"

# Cap source-image fetches at 10 MB. Webflow's per-asset limit is 4 MB
# for free / lower tiers and 10 MB for higher; we use 10 MB as a soft
# upper bound for the source-fetch step. If a state-leg portrait
# exceeds this, we skip + log; the source-URL Link field still stores
# the original URL so the website can fall back to hotlinking.
MAX_IMAGE_BYTES = 10 * 1024 * 1024


class WebflowAssetError(Exception):
    """Raised on any non-success during the upload flow.

    Carries the response (when applicable) and surfaces a truncated
    body in ``__str__`` for diagnostic visibility — same pattern as
    WebflowError.
    """

    def __init__(
        self,
        message: str,
        *,
        response: httpx.Response | None = None,
    ):
        self.response = response
        self.status_code = (
            response.status_code if response is not None else None
        )
        self.error_detail = (
            response.text[:500] if response is not None else None
        )
        if self.error_detail:
            message = f"{message} body={self.error_detail!r}"
        super().__init__(message)


@dataclass
class AssetReference:
    """Reference to a Webflow asset.

    Shape matches what the Image field expects in a CMS items PATCH:
      ``{"fileId": <asset_id>, "url": <hosted_url>, "alt": <alt_text>}``
    """

    asset_id: str
    hosted_url: str
    alt_text: str = ""

    def to_image_field_value(self) -> dict[str, str]:
        return {
            "fileId": self.asset_id,
            "url": self.hosted_url,
            "alt": self.alt_text,
        }


class WebflowAssetService:
    """Two-step upload of source images into Webflow's asset library."""

    DEFAULT_TIMEOUT_SECONDS = 60.0

    def __init__(
        self,
        api_token: str,
        site_id: str,
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_image_bytes: int = MAX_IMAGE_BYTES,
    ):
        if not api_token:
            raise ValueError("api_token is required")
        if not site_id:
            raise ValueError("site_id is required")
        self.api_token = api_token
        self.site_id = site_id
        self.timeout_seconds = timeout_seconds
        # Configurable upper bound on source-image fetch size. Defaults
        # to MAX_IMAGE_BYTES (10 MB — Webflow Pro+ tier limit). Lower
        # tiers can override via constructor; future per-state photo
        # CDNs that serve oversize originals can be capped explicitly.
        self.max_image_bytes = max_image_bytes
        # source_url → AssetReference, in-memory for the run's lifetime
        self._cache: dict[str, AssetReference] = {}

    async def upload_from_url(
        self,
        source_url: str,
        *,
        alt_text: str = "",
        dry_run: bool = False,
    ) -> AssetReference | None:
        """Upload an image fetched from ``source_url`` into Webflow.

        Cached on the service instance — second call with the same
        source_url returns the cached AssetReference without any HTTP
        traffic.

        ``dry_run=True`` performs the source-image fetch + size check +
        hash computation but skips both the Webflow ``POST /assets``
        call and the signed-URL upload, returning ``None``. Lets
        operators smoke-test source-CDN reachability without consuming
        Webflow's asset rate limit or storage on an unverified run.

        Raises WebflowAssetError on any step's failure. Caller is
        expected to per-record-isolate (catch + log + continue) so a
        single bad source URL doesn't abort the whole sync.
        """
        if not source_url:
            raise ValueError("source_url is required")
        cached = self._cache.get(source_url)
        if cached is not None:
            return AssetReference(
                asset_id=cached.asset_id,
                hosted_url=cached.hosted_url,
                alt_text=alt_text or cached.alt_text,
            )

        image_bytes, content_type = await self._fetch_image(source_url)
        file_hash = hashlib.md5(image_bytes).hexdigest()  # noqa: S324
        file_name = self._derive_filename(source_url, content_type)

        if dry_run:
            logger.info(
                "Webflow asset upload skipped (dry_run)",
                metric="webflow_assets.upload_dry_run",
                source_url=source_url,
                bytes=len(image_bytes),
                file_hash=file_hash,
            )
            return None

        asset_meta = await self._create_asset(file_name, file_hash)
        await self._put_to_signed_url(
            asset_meta["uploadUrl"],
            asset_meta.get("uploadDetails") or {},
            image_bytes,
            content_type,
            file_name,
        )

        ref = AssetReference(
            asset_id=asset_meta["id"],
            hosted_url=asset_meta.get("hostedUrl", ""),
            alt_text=alt_text,
        )
        self._cache[source_url] = ref
        logger.info(
            "Webflow asset uploaded",
            metric="webflow_assets.upload_complete",
            source_url=source_url,
            asset_id=ref.asset_id,
            bytes=len(image_bytes),
        )
        return ref

    async def _fetch_image(
        self, source_url: str,
    ) -> tuple[bytes, str]:
        """Fetch the image bytes + content-type, with size + redirect handling."""
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds, follow_redirects=True,
        ) as client:
            try:
                resp = await client.get(source_url)
            except httpx.HTTPError as e:
                raise WebflowAssetError(
                    f"Failed to fetch source image: {source_url} — {e}"
                ) from e
        if resp.status_code != 200:
            raise WebflowAssetError(
                f"Source image returned {resp.status_code}: {source_url}",
                response=resp,
            )
        if len(resp.content) > self.max_image_bytes:
            raise WebflowAssetError(
                f"Source image exceeds {self.max_image_bytes} bytes: "
                f"{source_url} ({len(resp.content)} bytes)"
            )
        content_type = (
            resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
        )
        return resp.content, content_type

    async def _create_asset(
        self, file_name: str, file_hash: str,
    ) -> dict[str, Any]:
        """Step 1 of the two-step upload — get a signed URL from Webflow."""
        url = f"{WEBFLOW_API_BASE}/sites/{self.site_id}/assets"
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "accept-version": "2.0.0",
            "Content-Type": "application/json",
        }
        payload = {"fileName": file_name, "fileHash": file_hash}
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            try:
                resp = await client.post(url, headers=headers, json=payload)
            except httpx.HTTPError as e:
                raise WebflowAssetError(
                    f"Failed to create Webflow asset metadata: {e}"
                ) from e
        if not (200 <= resp.status_code < 300):
            raise WebflowAssetError(
                f"Webflow asset create failed: status={resp.status_code}",
                response=resp,
            )
        body = resp.json()
        # Required fields the next step depends on
        for k in ("id", "uploadUrl"):
            if not body.get(k):
                raise WebflowAssetError(
                    f"Webflow asset create response missing {k!r}: "
                    f"{body!r}"
                )
        return body

    async def _put_to_signed_url(
        self,
        upload_url: str,
        upload_details: dict[str, Any],
        image_bytes: bytes,
        content_type: str,
        file_name: str,
    ) -> None:
        """Step 2 — POST the image bytes to the signed S3 URL with
        the form fields Webflow returned in step 1.

        Webflow's signed-URL flow uses S3-compatible multipart form
        upload. ``upload_details`` carries policy/signature/etc. fields
        that must be sent verbatim alongside the file.
        """
        files: dict[str, Any] = {
            **{
                k: (None, str(v))
                for k, v in (upload_details or {}).items()
            },
            "file": (file_name, image_bytes, content_type),
        }
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds,
        ) as client:
            try:
                resp = await client.post(upload_url, files=files)
            except httpx.HTTPError as e:
                raise WebflowAssetError(
                    f"Failed to PUT bytes to signed url: {e}"
                ) from e
        if not (200 <= resp.status_code < 300):
            raise WebflowAssetError(
                f"Signed-URL upload failed: status={resp.status_code}",
                response=resp,
            )

    @staticmethod
    def _derive_filename(source_url: str, content_type: str) -> str:
        """Best-effort filename from URL path; fall back to content-type."""
        path = urlparse(source_url).path
        base = path.rsplit("/", 1)[-1] if path else ""
        if base and "." in base:
            return base
        ext_map = {
            "image/jpeg": "jpg", "image/jpg": "jpg",
            "image/png": "png", "image/gif": "gif",
            "image/webp": "webp",
        }
        ext = ext_map.get(content_type, "jpg")
        return f"legislator-photo.{ext}"
