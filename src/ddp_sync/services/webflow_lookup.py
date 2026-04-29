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
from dataclasses import dataclass, field

import httpx
import structlog

from ddp_sync.config import Settings, get_settings
from ddp_sync.services.rate_limiter import RateLimitConfig, RateLimiter

logger = structlog.get_logger()


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
        # Cache of {collection_id -> set of known field slugs}
        self._field_slug_cache: dict[str, set[str]] = {}

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
        """Return the set of field slugs in a collection. Cached per id.

        Used to filter PATCH/POST payloads to only include known fields,
        supporting incremental schema rollout — sync code can reference new
        fields before editors create them in Webflow without breaking the
        whole sync.
        """
        if collection_id in self._field_slug_cache:
            return self._field_slug_cache[collection_id]
        url = f"{self.BASE_URL}/collections/{collection_id}"
        await self._limiter.apply()
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise WebflowError(
                f"Failed to fetch collection schema: {collection_id}",
                response=resp,
            )
        data = resp.json()
        slugs = {
            f.get("slug") for f in data.get("fields", []) if f.get("slug")
        }
        self._field_slug_cache[collection_id] = slugs
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
        """Try to filter ``field_data`` against the collection schema.

        On schema-fetch failure, fall back to passing everything through so
        a transient error doesn't manifest as silent field drops. Webflow
        itself will surface unknown fields as a 4xx in that case.
        """
        try:
            known = await self._get_field_slugs(client, headers, collection_id)
        except WebflowError as e:
            logger.warning(
                "Field-slug schema fetch failed; passing payload as-is",
                collection_id=collection_id,
                error=str(e),
            )
            return field_data, set()
        return self._filter_known_fields(field_data, known)
