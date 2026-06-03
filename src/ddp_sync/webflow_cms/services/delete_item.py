"""Service for deleting CMS items with multi-reference conflict handling."""

from __future__ import annotations

import logging
import time

from ddp_sync.webflow_cms.client import WebflowClient
from ddp_sync.webflow_cms.exceptions import WebflowConflictError
from ddp_sync.webflow_cms.models import DeleteResult

logger = logging.getLogger(__name__)


class DeleteItemService:
    """Delete a CMS item, handling 409 conflicts by removing references first."""

    def __init__(self, client: WebflowClient):
        self._client = client

    def delete(
        self,
        collection_id: str,
        item_id: str,
        *,
        ref_collection_ids: list[str] | None = None,
        force_remove_references: bool = False,
    ) -> DeleteResult:
        """Delete a CMS item.

        Args:
            collection_id: The item's collection.
            item_id: The item to delete.
            ref_collection_ids: Fallback collection IDs to search when a
                conflict response is missing collectionId for a reference.
            force_remove_references: If True, automatically remove all
                incoming references before deleting (no prompts).

        Returns:
            DeleteResult with outcome details.
        """
        ref_collection_ids = ref_collection_ids or []
        item = self._client.fetch_item(collection_id, item_id)
        item_name = "(not found)"
        if item:
            fd = item.get("fieldData") or {}
            item_name = fd.get("name") or fd.get("slug") or item_id

        try:
            self._client.delete_item(collection_id, item_id)
            logger.info("Deleted '%s' (%s)", item_name, item_id)
            return DeleteResult(item_id=item_id, item_name=item_name, deleted=True)
        except WebflowConflictError as exc:
            if not force_remove_references:
                return DeleteResult(
                    item_id=item_id,
                    item_name=item_name,
                    deleted=False,
                    error=f"Item has {len(exc.references)} incoming reference(s). "
                          "Set force_remove_references=True to auto-remove.",
                )
            return self._remove_refs_and_delete(
                collection_id, item_id, item_name,
                exc.references, ref_collection_ids,
            )

    def _remove_refs_and_delete(
        self,
        collection_id: str,
        item_id: str,
        item_name: str,
        references: list[dict],
        ref_collection_ids: list[str],
    ) -> DeleteResult:
        """Remove incoming references then retry deletion."""
        removed = 0
        failed = 0

        for ref in references:
            ref_info = ref.get("ref", {})
            ref_id = ref_info.get("id")
            ref_coll = ref_info.get("collectionId")

            if not ref_id:
                failed += 1
                continue

            # Find collection if not provided in conflict response
            if not ref_coll:
                candidates = [collection_id] + [c for c in ref_collection_ids if c != collection_id]
                ref_coll = self._client.find_item_collection(ref_id, candidates)
                if not ref_coll:
                    logger.warning("Could not find collection for ref %s", ref_id)
                    failed += 1
                    continue

            try:
                self._client.remove_reference(ref_coll, ref_id, item_id)
                removed += 1
            except Exception as exc:
                logger.error("Failed to remove ref from %s: %s", ref_id, exc)
                failed += 1
            time.sleep(0.3)

        # Retry delete
        try:
            self._client.delete_item(collection_id, item_id)
            logger.info("Deleted '%s' after removing %d refs", item_name, removed)
            return DeleteResult(
                item_id=item_id,
                item_name=item_name,
                deleted=True,
                references_removed=removed,
                references_failed=failed,
            )
        except Exception as exc:
            return DeleteResult(
                item_id=item_id,
                item_name=item_name,
                deleted=False,
                references_removed=removed,
                references_failed=failed,
                error=str(exc),
            )
