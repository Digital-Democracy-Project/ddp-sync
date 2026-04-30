"""Webflow CMS write operations for sync pipelines.

Despite the historical "Lookup" name, this module is the **write** service.
Legacy reads happen elsewhere (`ingestion/sources/webflow.py`); this file
holds PATCH/POST against the CMS.

Provides:
- update_bill_fields(): legacy bool-return PATCH for bill CMS items.
  Used by pipelines/bill_version.py (Flow 1 status sync). Bool contract
  preserved — no caller migration required.
- update_bill_gov_url(): thin wrapper for backward compat.
- update_legislator_fields(): bio-sync PATCH with field-existence tolerance
  (drops unknown fields with a warning, supporting incremental schema
  rollout). Raises on failure for callers that need to surface errors in
  a run report.
- create_legislator_draft(): POST a new Legislators item with isDraft=True.
- _patch_with_backoff() / _post_with_backoff(): shared 429-aware HTTP helpers.

All writes from the same instance share a single in-process RateLimiter
to enforce the per-process Webflow budget. Default 60 req/min on the
120 req/min plan tier, leaving 60 req/min headroom for unified-sync API
calls and ad-hoc triggers. See PLAN-legislator-bio-sync.md "Single-worker
assumption" for the cross-process implications.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import AsyncIterator

import httpx
import structlog

from ddp_sync.config import Settings, get_settings
from ddp_sync.services.rate_limiter import RateLimitConfig, RateLimiter

logger = structlog.get_logger()


# How long a successfully-fetched collection schema stays "fresh" before we
# attempt a refresh. On refresh failure we reuse the stale cached value as
# long as we have one (round-6 fix). 1 hour gives editors near-instant
# pickup of new fields after they're added in Webflow Designer, while
# providing protection against transient /collections/{id} 5xx incidents.
SCHEMA_CACHE_TTL_SECONDS = 60 * 60

# Same TTL semantics applied to the Jurisdictions CMS collection mapping
# (round-9 fix). Editors rarely add new jurisdiction entries, but a 1-hour
# refresh window lets a long-running worker pick up additions (e.g. DC,
# territories) without a process restart. On refresh failure we reuse a
# stale non-empty mapping with a structured metric breadcrumb.
JURISDICTION_CACHE_TTL_SECONDS = 60 * 60


# ---------- Error types & result objects ----------


class WebflowError(Exception):
    """Base class for any non-success Webflow API response.

    Carries the underlying httpx.Response (when available) so callers
    can introspect status code and body without re-fetching.
    """

    def __init__(self, message: str, *, response: httpx.Response | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code if response is not None else None
        self.error_detail = response.text[:500] if response is not None else None


class WebflowRateLimitError(WebflowError):
    """Raised when Webflow returns 429 after the configured retry budget.

    Distinct from WebflowError so callers that want to retry the whole
    operation later (rather than skip the item) can do so selectively.
    """


@dataclass
class WebflowPatchResult:
    """Outcome of an HTTP-2xx PATCH that may have partial issues.

    success=False is reserved for ``HTTP-2xx-but-not-OK`` cases. Hard
    failures raise WebflowError instead — never silently masked.

    dropped_fields captures field names removed from the payload because
    they don't exist in the collection schema yet (incremental rollout).
    """

    success: bool
    webflow_id: str
    dropped_fields: set[str] = field(default_factory=set)
    status_code: int = 200
    error_detail: str | None = None


@dataclass
class WebflowCreateResult:
    """Outcome of a successful Legislators draft creation."""

    webflow_id: str
    dropped_fields: set[str] = field(default_factory=set)
    status_code: int = 200


# ---------- Service class ----------


class WebflowLookupService:
    """Webflow CMS write service shared across all ddp-sync pipelines.

    Two write contracts coexist intentionally:

    Legacy bool-return (bill sync):
      - update_bill_fields()
      - update_bill_gov_url()
      Used by pipelines/bill_version.py. Returns True on 2xx, False on any
      failure (including persistent 429). Existing callers already wrap in
      try/except + bool check — semantically equivalent to a raising
      contract, no migration needed.

    Raising (bio sync):
      - update_legislator_fields()
      - create_legislator_draft()
      Raise WebflowRateLimitError on persistent 429, WebflowError on other
      non-2xx. Callers in pipelines/legislator_bio.py append to
      BioSyncReport.errors so the per-record failure surfaces in the
      run-summary alert.

    All methods share the same RateLimiter and field-slug cache, so two
    pipelines using the same instance share their write budget naturally.
    """

    BASE_URL = "https://api.webflow.com/v2"
    DEFAULT_MAX_REQUESTS_PER_MINUTE = 60

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        max_requests_per_minute: int = DEFAULT_MAX_REQUESTS_PER_MINUTE,
    ):
        self.settings = settings or get_settings()
        self.api_key = (
            self.settings.webflow_scheduler_api_key
            or self.settings.webflow_votebot_api_key
        )
        self.bills_collection_id = self.settings.webflow_bills_collection_id
        self.legislators_collection_id = (
            self.settings.webflow_legislators_collection_id
        )
        # Shared limiter — protects all pipelines using the same instance
        self._limiter = RateLimiter(
            RateLimitConfig(
                requests_per_minute=max_requests_per_minute,
                delay_between_requests_ms=0,  # rely on requests_per_minute
            )
        )
        # Cache of {collection_id -> (timestamp, set of known field slugs)}.
        # On expiry we attempt a refresh; if the refresh fails AND we have a
        # stale entry, we reuse it (round-6 fix: a transient 503 on
        # /collections/{id} during a Webflow incident must not dead-stop
        # every legislator PATCH). If the refresh fails AND we have nothing,
        # the WebflowError propagates per the fail-closed contract.
        self._field_slug_cache: dict[str, tuple[float, set[str]]] = {}
        # Cache of (timestamp, {jurisdiction_ref_id -> two-letter state code}).
        # Populated lazily on the first ``get_jurisdiction_mapping()`` call.
        # 1-hour TTL (round-9 fix) — editors rarely add new jurisdiction
        # entries but new states/territories arriving mid-run shouldn't
        # require a process restart. On refresh failure with a stale
        # non-empty entry, we reuse it with a structured-metric breadcrumb.
        self._jurisdiction_mapping: tuple[float, dict[str, str]] | None = None
        # Lock around the refresh path (round-10 fix). Concurrent coroutines
        # hitting get_jurisdiction_mapping() just after TTL expiry would
        # otherwise each fire a fresh fetch. Single-worker safe today, but
        # the lock matches the schema-cache pattern and protects against
        # future high-parallelism callers.
        self._jurisdiction_refresh_lock = asyncio.Lock()

    # ---------- Shared HTTP helpers ----------

    async def _patch_with_backoff(
        self,
        client: httpx.AsyncClient,
        url: str,
        headers: dict,
        payload: dict,
    ) -> httpx.Response:
        """Rate-limited PATCH with 429 retry honoring Retry-After.

        Raises WebflowRateLimitError if 429 persists after 3 retries.
        Returns the final response for any other status; the caller decides
        whether non-2xx is an error in their context.
        """
        return await self._send_with_backoff(client, "PATCH", url, headers, payload)

    async def _post_with_backoff(
        self,
        client: httpx.AsyncClient,
        url: str,
        headers: dict,
        payload: dict,
    ) -> httpx.Response:
        """Rate-limited POST. Same semantics as _patch_with_backoff."""
        return await self._send_with_backoff(client, "POST", url, headers, payload)

    async def _send_with_backoff(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        headers: dict,
        payload: dict,
    ) -> httpx.Response:
        last_resp = None
        for attempt in range(3):
            await self._limiter.apply()
            last_resp = await client.request(
                method, url, headers=headers, json=payload
            )
            if last_resp.status_code != 429:
                return last_resp
            wait = float(last_resp.headers.get("Retry-After", 2 ** attempt))
            jittered = wait + random.uniform(0, 0.5)
            logger.warning(
                "Webflow 429 — backing off",
                method=method,
                attempt=attempt + 1,
                wait_seconds=round(jittered, 2),
                url=url,
            )
            await asyncio.sleep(jittered)
        raise WebflowRateLimitError(
            f"Webflow returned 429 after 3 retries: {method} {url}",
            response=last_resp,
        )

    # ---------- Field-existence cache ----------

    async def _get_field_slugs(
        self,
        client: httpx.AsyncClient,
        headers: dict,
        collection_id: str,
    ) -> set[str]:
        """Return the set of field slugs in a collection.

        Cached per collection_id with a TTL (default 1 hour). On TTL expiry
        we try to refresh; on refresh failure we reuse the stale entry if
        available (round-6 fix) so a transient `/collections/{id}` 5xx
        during a Webflow incident doesn't dead-stop every legislator PATCH.
        Only when we have NO cached entry at all does a fetch failure
        propagate as WebflowError.
        """
        cached = self._field_slug_cache.get(collection_id)
        if cached and (time.time() - cached[0]) < SCHEMA_CACHE_TTL_SECONDS:
            return cached[1]

        url = f"{self.BASE_URL}/collections/{collection_id}"
        await self._limiter.apply()
        try:
            resp = await client.get(url, headers=headers)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            if cached is not None:
                logger.warning(
                    "Schema refresh transport error — reusing stale cache",
                    collection_id=collection_id,
                    error=str(e),
                    metric="webflow.schema_stale_reuse",
                )
                return cached[1]
            raise WebflowError(
                f"Failed to fetch collection schema (no cache available): "
                f"{collection_id} — {e}"
            ) from e
        if resp.status_code != 200:
            if cached is not None:
                logger.warning(
                    "Schema refresh non-200 — reusing stale cache",
                    collection_id=collection_id,
                    status_code=resp.status_code,
                    metric="webflow.schema_stale_reuse",
                )
                return cached[1]
            raise WebflowError(
                f"Failed to fetch collection schema: {collection_id}",
                response=resp,
            )
        data = resp.json()
        slugs = {
            f.get("slug") for f in data.get("fields", []) if f.get("slug")
        }
        self._field_slug_cache[collection_id] = (time.time(), slugs)
        logger.info(
            "Cached Webflow collection field slugs",
            collection_id=collection_id,
            field_count=len(slugs),
        )
        return slugs

    def _filter_known_fields(
        self,
        field_data: dict,
        known_slugs: set[str],
    ) -> tuple[dict, set[str]]:
        """Split ``field_data`` into (kept, dropped) by known-slug membership."""
        kept = {k: v for k, v in field_data.items() if k in known_slugs}
        dropped = set(field_data.keys()) - set(kept.keys())
        return kept, dropped

    # ---------- Jurisdiction-mapping cache ----------

    async def get_jurisdiction_mapping(self) -> dict[str, str]:
        """Return ``{jurisdiction_ref_id: two-letter-state-code}``.

        Fetches the Jurisdictions CMS collection and caches the result on
        the service instance with a 1-hour TTL (round-9 fix). On expiry we
        attempt a refresh; if it fails AND we have a non-empty stale entry,
        we reuse it with a ``metric=webflow.jurisdiction_stale_reuse``
        breadcrumb. If the fresh fetch returns empty AND we have no usable
        cache, we emit ``metric=webflow.jurisdiction_mapping_empty`` so
        infra alerting can surface the silent-degraded state.

        The refresh path is guarded by an ``asyncio.Lock`` (round-10 fix)
        so concurrent coroutines arriving just after TTL expiry don't each
        fire a fresh fetch. Single-worker safe today; the lock protects
        against future high-parallelism callers.

        Never raises — callers always get a dict (possibly empty).
        """
        cached = self._jurisdiction_mapping
        if cached and (time.time() - cached[0]) < JURISDICTION_CACHE_TTL_SECONDS:
            return cached[1]

        async with self._jurisdiction_refresh_lock:
            # Re-check inside the lock — a concurrent caller may have
            # refreshed while we were waiting.
            cached = self._jurisdiction_mapping
            if cached and (
                time.time() - cached[0]
            ) < JURISDICTION_CACHE_TTL_SECONDS:
                return cached[1]

            fresh = await self._fetch_jurisdiction_mapping_fresh()
            if fresh:
                self._jurisdiction_mapping = (time.time(), fresh)
                logger.info(
                    "Built jurisdiction mapping",
                    entries=len(fresh),
                )
                return fresh

            # Fresh fetch returned empty
            if cached and cached[1]:
                # Round-11 fix: bump the timestamp so we don't re-fire the
                # fetch on every subsequent call during a sustained Webflow
                # outage. We still retry once per TTL window — the cache
                # never grows older than TTL relative to "last attempt" —
                # so editors who fix the underlying issue see updates within
                # an hour, rather than holding stale data indefinitely OR
                # hammering the Webflow endpoint on every call.
                logger.warning(
                    "Jurisdiction refresh returned empty; reusing stale cache",
                    metric="webflow.jurisdiction_stale_reuse",
                    stale_age_seconds=round(time.time() - cached[0], 1),
                )
                self._jurisdiction_mapping = (time.time(), cached[1])
                return cached[1]

            # No usable cache — emit metric breadcrumb so infra alerts can fire
            logger.warning(
                "Jurisdiction mapping is empty; "
                "audits and state-code resolution are disabled",
                metric="webflow.jurisdiction_mapping_empty",
            )
            self._jurisdiction_mapping = (time.time(), {})
            return {}

    async def _fetch_jurisdiction_mapping_fresh(self) -> dict[str, str]:
        """Single-pass fetch of the Jurisdictions collection.

        Returns the mapping (possibly empty on config-missing or HTTP
        errors). Never raises — caller decides whether empty means
        "use stale" or "alert".
        """
        coll = self.settings.webflow_jurisdiction_collection_id
        if not coll:
            return {}
        key = (
            self.settings.webflow_votebot_api_key
            or self.settings.webflow_scheduler_api_key
        )
        if not key:
            return {}
        headers = {
            "Authorization": f"Bearer {key}",
            "accept": "application/json",
        }
        mapping: dict[str, str] = {}
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                offset = 0
                page_size = 100
                page = 0
                max_pages = 50  # Jurisdictions is small (50 states + US + DC + territories)
                while page < max_pages:
                    await self._limiter.apply()
                    resp = await client.get(
                        f"{self.BASE_URL}/collections/{coll}/items",
                        headers=headers,
                        params={"limit": page_size, "offset": offset},
                    )
                    if resp.status_code != 200:
                        logger.warning(
                            "Jurisdictions fetch returned non-200; "
                            "partial mapping",
                            status_code=resp.status_code,
                        )
                        break
                    data = resp.json()
                    items = data.get("items") or []
                    if not items:
                        break
                    for item in items:
                        item_id = item.get("id") or ""
                        fields = item.get("fieldData") or {}
                        candidate = (
                            fields.get("state-code")
                            or fields.get("code")
                            or fields.get("abbreviation")
                            or (fields.get("name") or "")[:2]
                        )
                        normalized = self._normalize_state_code(candidate)
                        if item_id and normalized:
                            mapping[item_id] = normalized
                    pagination = data.get("pagination") or {}
                    total = pagination.get("total", 0)
                    offset += len(items)
                    if offset >= total or len(items) < page_size:
                        break
                    page += 1
        except Exception as e:  # noqa: BLE001 — never raise
            logger.warning(
                "Jurisdiction mapping fetch failed; returning partial",
                error=str(e),
            )
        return mapping

    @staticmethod
    def _normalize_state_code(value) -> str | None:
        """Coerce ``value`` to exactly two uppercase ASCII letters or None.

        Round-8 reviewer fix: previous logic accepted ≥2 chars which let
        full state names (e.g. "Florida" → "FL"... but also "FLORIDA"
        in the worst case) leak through and break exact-match jurisdiction
        filters. Only true 2-letter codes pass this gate.
        """
        if not value or not isinstance(value, str):
            return None
        cleaned = value.strip().upper()
        if len(cleaned) != 2:
            return None
        if not cleaned.isalpha():
            return None
        return cleaned

    @staticmethod
    def resolve_jurisdiction_ref(
        ref,
        mapping: dict[str, str],
    ) -> str | None:
        """Resolve a Legislators ``jurisdiction`` field value to a state code.

        Accepts the upstream shapes seen in the wild:
        - None / "" — unset (returns None)
        - list[str] — multi-reference field. First non-empty element wins.
        - str — single ref ID OR an already-2-letter state code.

        Returns a normalized two-letter state code or None if the ref
        cannot be resolved. **Returns None for federal/US-Congress
        jurisdiction** — the orchestrator detects federal members via the
        ``chamber`` heuristic and doesn't need this method's output.
        """
        if not ref:
            return None
        if isinstance(ref, list):
            for r in ref:
                resolved = WebflowLookupService.resolve_jurisdiction_ref(
                    r, mapping
                )
                if resolved:
                    return resolved
            return None
        if not isinstance(ref, str):
            return None
        # Already a 2-letter code?
        direct = WebflowLookupService._normalize_state_code(ref)
        if direct and direct != "US":
            return direct
        # Otherwise assume it's a ref ID and look it up
        mapped = mapping.get(ref)
        if not mapped:
            return None
        normalized = WebflowLookupService._normalize_state_code(mapped)
        if normalized == "US":
            return None
        return normalized

    # ---------- Read API ----------

    async def iter_legislator_items(
        self,
        *,
        per_page: int = 100,
        max_pages: int = 200,
    ) -> AsyncIterator[dict]:
        """Iterate every Legislators CMS item as a raw upstream dict.

        Each yielded value is the full ``items`` element from the v2 API:
        ``{"id": "...", "fieldData": {...}, "isDraft": bool, ...}``.
        Use this when the orchestrator needs to diff against the current
        CMS state (which is most callers).

        Pagination is capped at ``max_pages`` as a safety valve.

        Raises:
            WebflowError: any non-2xx response (including persistent 429 via
              the retry budget on the underlying limiter — though list reads
              don't go through the 429-aware helper today).
        """
        if not self.legislators_collection_id:
            raise WebflowError(
                "legislators_collection_id is not configured"
            )
        url = (
            f"{self.BASE_URL}/collections/{self.legislators_collection_id}/items"
        )
        # Read scope is enough; prefer the votebot key if available (it's
        # typically the read-scoped key). Fall back to the scheduler key.
        key = (
            self.settings.webflow_votebot_api_key
            or self.settings.webflow_scheduler_api_key
        )
        if not key:
            raise WebflowError("No Webflow API key available")
        headers = {
            "Authorization": f"Bearer {key}",
            "accept": "application/json",
        }

        offset = 0
        page = 0
        async with httpx.AsyncClient(timeout=60.0) as client:
            while page < max_pages:
                await self._limiter.apply()
                resp = await client.get(
                    url,
                    headers=headers,
                    params={"limit": per_page, "offset": offset},
                )
                if resp.status_code != 200:
                    raise WebflowError(
                        f"Webflow legislator read failed: status="
                        f"{resp.status_code}",
                        response=resp,
                    )
                data = resp.json()
                items = data.get("items") or []
                if not items:
                    return
                for item in items:
                    yield item
                pagination = data.get("pagination") or {}
                total = pagination.get("total", 0)
                offset += len(items)
                if offset >= total or len(items) < per_page:
                    return
                page += 1

    # ---------- Bill methods (legacy bool contract) ----------

    async def update_bill_fields(
        self,
        webflow_id: str,
        field_data: dict[str, str],
        api_key: str | None = None,
    ) -> bool:
        """Update arbitrary fields for a bill in Webflow CMS.

        Uses PATCH /v2/collections/{id}/items/{item_id}/live (publishes
        immediately). Routes through the shared rate-limiter and 429-retry
        helper so this method now inherits both improvements without any
        caller-side change.

        Returns True on 2xx, False on any failure including persistent 429.
        bill_version.py:_update_webflow_status and update_bill_status depend
        on this contract.
        """
        if not webflow_id or not field_data:
            logger.warning("Missing webflow_id or field_data for bill update")
            return False

        key = api_key or self.api_key
        url = (
            f"{self.BASE_URL}/collections/{self.bills_collection_id}"
            f"/items/{webflow_id}/live"
        )
        headers = {
            "Authorization": f"Bearer {key}",
            "accept": "application/json",
            "content-type": "application/json",
        }
        payload = {"fieldData": field_data}

        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                response = await self._patch_with_backoff(
                    client, url, headers, payload
                )
            except WebflowRateLimitError:
                logger.error(
                    "Bill PATCH gave up after 429 retries",
                    webflow_id=webflow_id,
                    fields=list(field_data.keys()),
                )
                return False
            except Exception as e:
                logger.error(
                    "Error updating bill fields in Webflow CMS",
                    webflow_id=webflow_id,
                    error=str(e),
                )
                return False

            if 200 <= response.status_code < 300:
                logger.info(
                    "Updated bill fields in Webflow CMS",
                    webflow_id=webflow_id,
                    fields=list(field_data.keys()),
                )
                return True
            logger.error(
                "Failed to update bill fields in Webflow CMS",
                webflow_id=webflow_id,
                fields=list(field_data.keys()),
                status_code=response.status_code,
                response_text=response.text[:200],
            )
            return False

    async def update_bill_gov_url(
        self,
        webflow_id: str,
        new_url: str,
        api_key: str | None = None,
    ) -> bool:
        """Update the gov-url field for a bill in Webflow CMS."""
        if not new_url:
            logger.warning("Missing new_url for gov-url update")
            return False
        return await self.update_bill_fields(
            webflow_id, {"gov-url": new_url}, api_key
        )

    # ---------- Legislator methods (raising contract) ----------

    async def update_legislator_fields(
        self,
        webflow_id: str,
        field_data: dict,
        *,
        publish: bool = True,
        api_key: str | None = None,
    ) -> WebflowPatchResult:
        """PATCH a Legislators CMS item with new field values.

        Filters out fields not present in the collection schema (supports
        incremental rollout — see PLAN "Webflow CMS schema additions").

        Args:
            webflow_id: Existing Legislators item ID.
            field_data: Dict of field slugs to values. Missing slugs are
              dropped and reported in the result, not failed.
            publish: True (default) writes via /live so the change is
              published immediately. False writes without /live, so the
              change stays staged until an editor publishes the item.
            api_key: Override the default key (e.g. the scheduler key).

        Returns:
            WebflowPatchResult with success=True and dropped_fields populated
            when applicable.

        Raises:
            WebflowRateLimitError: 429 persists after the retry budget.
            WebflowError: any other non-2xx, missing collection id, or
              missing API key.
        """
        if not self.legislators_collection_id:
            raise WebflowError("legislators_collection_id is not configured")
        if not webflow_id:
            raise WebflowError("webflow_id is required")
        if not field_data:
            return WebflowPatchResult(success=True, webflow_id=webflow_id)

        key = api_key or self.api_key
        if not key:
            raise WebflowError("No Webflow API key available")

        suffix = "/live" if publish else ""
        url = (
            f"{self.BASE_URL}/collections/{self.legislators_collection_id}"
            f"/items/{webflow_id}{suffix}"
        )
        headers = {
            "Authorization": f"Bearer {key}",
            "accept": "application/json",
            "content-type": "application/json",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            kept, dropped = await self._partition_payload(
                client, headers, field_data, self.legislators_collection_id
            )
            if dropped:
                logger.warning(
                    "Dropping unknown Webflow fields from legislator PATCH",
                    webflow_id=webflow_id,
                    dropped=sorted(dropped),
                    note="Add the field in the Webflow Designer to enable sync",
                )
            if not kept:
                # Nothing to write after filtering — still a successful no-op
                return WebflowPatchResult(
                    success=True,
                    webflow_id=webflow_id,
                    dropped_fields=dropped,
                )

            payload = {"fieldData": kept}
            response = await self._patch_with_backoff(
                client, url, headers, payload
            )
            if 200 <= response.status_code < 300:
                logger.info(
                    "Updated legislator in Webflow CMS",
                    webflow_id=webflow_id,
                    fields=list(kept.keys()),
                    dropped=sorted(dropped) if dropped else None,
                )
                return WebflowPatchResult(
                    success=True,
                    webflow_id=webflow_id,
                    dropped_fields=dropped,
                    status_code=response.status_code,
                )
            raise WebflowError(
                f"Webflow PATCH failed: status={response.status_code}",
                response=response,
            )

    async def create_legislator_draft(
        self,
        field_data: dict,
        *,
        api_key: str | None = None,
    ) -> WebflowCreateResult:
        """Create a new Legislators CMS item with isDraft=True.

        Drafts do not appear on the live site until an editor publishes them.
        Used by the bio-sync auto-create path with editor review as the gate.

        Filters unknown fields the same way update_legislator_fields does.

        Raises:
            WebflowRateLimitError: 429 persists after the retry budget.
            WebflowError: any other non-2xx, missing collection id, missing
              API key, or successful response without an item id.
        """
        if not self.legislators_collection_id:
            raise WebflowError("legislators_collection_id is not configured")
        if not field_data:
            raise WebflowError("field_data is required to create a draft")

        key = api_key or self.api_key
        if not key:
            raise WebflowError("No Webflow API key available")

        url = (
            f"{self.BASE_URL}/collections/{self.legislators_collection_id}"
            f"/items"
        )
        headers = {
            "Authorization": f"Bearer {key}",
            "accept": "application/json",
            "content-type": "application/json",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            kept, dropped = await self._partition_payload(
                client, headers, field_data, self.legislators_collection_id
            )
            if dropped:
                logger.warning(
                    "Dropping unknown Webflow fields from legislator create",
                    dropped=sorted(dropped),
                )

            payload = {"isDraft": True, "fieldData": kept}
            response = await self._post_with_backoff(
                client, url, headers, payload
            )
            if 200 <= response.status_code < 300:
                data = response.json()
                new_id = data.get("id") or data.get("_id")
                if not new_id:
                    raise WebflowError(
                        "Webflow create returned 2xx but no item id",
                        response=response,
                    )
                logger.info(
                    "Created legislator draft in Webflow CMS",
                    webflow_id=new_id,
                    fields=list(kept.keys()),
                    dropped=sorted(dropped) if dropped else None,
                )
                return WebflowCreateResult(
                    webflow_id=new_id,
                    dropped_fields=dropped,
                    status_code=response.status_code,
                )
            raise WebflowError(
                f"Webflow create failed: status={response.status_code}",
                response=response,
            )

    async def _partition_payload(
        self,
        client: httpx.AsyncClient,
        headers: dict,
        field_data: dict,
        collection_id: str,
    ) -> tuple[dict, set[str]]:
        """Filter ``field_data`` against the collection schema.

        Fails closed on schema-fetch failure (round-5 fix): a transient 5xx
        on `/collections/{id}` propagates as `WebflowError` rather than
        silently passing the unfiltered payload through. Reasoning: if the
        schema is unreachable we cannot guarantee the payload is safe;
        better to surface the upstream issue in `BioSyncReport.errors` and
        let the next run retry than to risk every PATCH 4xx-ing on unknown
        slugs and masking the root cause.

        The schema-fetch result is cached on the service instance, so once
        it succeeds for a collection, subsequent partitions reuse the
        cached set without an extra HTTP call.
        """
        known = await self._get_field_slugs(client, headers, collection_id)
        return self._filter_known_fields(field_data, known)
