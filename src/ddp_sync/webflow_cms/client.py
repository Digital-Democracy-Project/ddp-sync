"""Webflow CMS API client.

Consolidates all duplicated HTTP logic from the original 6 scripts into a
single class with retry, pagination, and structured error handling.
"""

from __future__ import annotations

import logging
import time

import requests

from ddp_sync.webflow_cms.constants import (
    API_BASE,
    API_VERSION,
    MAX_BACKOFF_SECONDS,
    PAGE_LIMIT,
    TRANSIENT_STATUS_CODES,
)
from ddp_sync.webflow_cms.exceptions import (
    WebflowAPIError,
    WebflowConflictError,
)

logger = logging.getLogger(__name__)


class WebflowClient:
    """Low-level client for the Webflow v2 CMS API."""

    def __init__(self, token: str):
        self._token = token

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept-Version": API_VERSION,
            "Accept": "application/json",
        }

    def _write_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        headers: dict | None = None,
        params: dict | None = None,
        json_body: dict | None = None,
        allow_404: bool = False,
    ) -> requests.Response | None:
        """Execute an HTTP request with exponential backoff on transient errors.

        Args:
            method: HTTP method (GET, PATCH, DELETE).
            url: Full URL.
            headers: Request headers.
            params: Query parameters.
            json_body: JSON body for PATCH/POST.
            allow_404: If True, return None on 404 instead of raising.

        Returns:
            The Response object, or None if allow_404 and 404 received.

        Raises:
            WebflowConflictError: On 409 with conflict details.
            WebflowAPIError: On any other non-transient error.
        """
        backoff = 1
        while True:
            logger.debug("%s %s", method, url)
            resp = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
            )
            logger.debug("Response %s: %s", resp.status_code, resp.text[:200])

            if resp.status_code in TRANSIENT_STATUS_CODES:
                logger.debug("Transient %s, sleeping %ss", resp.status_code, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
                continue

            if allow_404 and resp.status_code == 404:
                return None

            # Handle 409 Conflict (item has references)
            if resp.status_code == 409:
                try:
                    error_data = resp.json()
                    if error_data.get("code") == "conflict" and "details" in error_data:
                        conflicts = error_data["details"][0].get("conflicts", [])
                        raise WebflowConflictError(
                            error_data.get("message", "Conflict"),
                            references=conflicts,
                        )
                except (ValueError, KeyError, IndexError):
                    pass
                raise WebflowAPIError(
                    f"Conflict: {resp.text}",
                    status_code=409,
                    response_text=resp.text,
                )

            if resp.status_code in (200, 204):
                return resp

            # Any other error
            try:
                resp.raise_for_status()
            except requests.HTTPError:
                raise WebflowAPIError(
                    f"API error {resp.status_code}: {resp.text}",
                    status_code=resp.status_code,
                    response_text=resp.text,
                )

            return resp

    def _collection_item_url(self, collection_id: str, item_id: str) -> str:
        return f"{API_BASE}/collections/{collection_id}/items/{item_id}/live"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_all_live_items(self, collection_id: str) -> list[dict]:
        """Fetch all published (live) items from a collection using offset pagination."""
        headers = self._read_headers()
        items: list[dict] = []
        offset = 0
        while True:
            params = {"limit": PAGE_LIMIT, "offset": offset}
            resp = self._request_with_retry(
                "GET",
                f"{API_BASE}/collections/{collection_id}/items/live",
                headers=headers,
                params=params,
            )
            data = resp.json()
            batch = data.get("items", [])
            items.extend(batch)
            if len(batch) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT
        return items

    def fetch_item(self, collection_id: str, item_id: str) -> dict | None:
        """Fetch a single live item.  Returns None if not found."""
        resp = self._request_with_retry(
            "GET",
            self._collection_item_url(collection_id, item_id),
            headers=self._read_headers(),
            allow_404=True,
        )
        if resp is None:
            return None
        return resp.json()

    def update_item_fields(
        self,
        collection_id: str,
        item: dict,
        fields: dict,
    ) -> dict:
        """Update one or more fields on a live CMS item.

        Args:
            collection_id: The collection containing the item.
            item: The full item dict (needs 'id', 'isArchived', 'isDraft').
            fields: Mapping of field slug -> new value.

        Returns:
            The updated item dict from the API.
        """
        payload = {
            "isArchived": item.get("isArchived", False),
            "isDraft": item.get("isDraft", False),
            "fieldData": fields,
        }
        resp = self._request_with_retry(
            "PATCH",
            self._collection_item_url(collection_id, item["id"]),
            headers=self._write_headers(),
            json_body=payload,
        )
        return resp.json()

    def update_item_field(
        self,
        collection_id: str,
        item_id: str,
        field_name: str,
        field_value: object,
    ) -> dict:
        """Update a single field on a live CMS item (by item ID only).

        Unlike update_item_fields, this does not require the full item dict.
        """
        payload = {"fieldData": {field_name: field_value}}
        resp = self._request_with_retry(
            "PATCH",
            self._collection_item_url(collection_id, item_id),
            headers=self._write_headers(),
            json_body=payload,
        )
        return resp.json()

    def delete_item(self, collection_id: str, item_id: str) -> bool:
        """Delete a live CMS item.

        Returns True on success.

        Raises:
            WebflowConflictError: If the item has incoming references.
            WebflowAPIError: On other failures.
        """
        resp = self._request_with_retry(
            "DELETE",
            self._collection_item_url(collection_id, item_id),
            headers=self._write_headers(),
        )
        return True

    def remove_reference(
        self,
        collection_id: str,
        item_id: str,
        target_id: str,
    ) -> bool:
        """Remove all occurrences of target_id from any multi-reference field on item_id.

        Returns True if the reference was removed (or was not present).
        """
        item = self.fetch_item(collection_id, item_id)
        if not item:
            return False

        field_data = item.get("fieldData", {})
        fields_to_update = {}
        for field_name, field_value in field_data.items():
            if isinstance(field_value, list) and target_id in field_value:
                fields_to_update[field_name] = [x for x in field_value if x != target_id]

        if not fields_to_update:
            logger.debug("target_id %s not found in any field of item %s", target_id, item_id)
            return True

        payload = {"fieldData": fields_to_update}
        self._request_with_retry(
            "PATCH",
            self._collection_item_url(collection_id, item_id),
            headers=self._write_headers(),
            json_body=payload,
        )
        return True

    def find_item_collection(
        self,
        item_id: str,
        candidate_collection_ids: list[str],
    ) -> str | None:
        """Search candidate collections for an item.  Returns the collection ID or None."""
        for cid in candidate_collection_ids:
            item = self.fetch_item(cid, item_id)
            if item is not None:
                return cid
        return None
