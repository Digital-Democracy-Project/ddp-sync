"""Service for filling missing gov-url fields on CMS items."""

from __future__ import annotations

import logging
import time

from ddp_sync.webflow_cms.client import WebflowClient
from ddp_sync.webflow_cms.constants import THROTTLE_SECONDS
from ddp_sync.webflow_cms.models import FillResult, UpdateResult
from ddp_sync.webflow_cms.utils import item_label

logger = logging.getLogger(__name__)


class GovUrlService:
    """Fill missing gov-url fields on bill CMS items."""

    def __init__(self, client: WebflowClient):
        self._client = client

    def find_missing(self, collection_id: str) -> tuple[list[dict], list[dict]]:
        """Return (items_missing_gov_url, all_items)."""
        items = self._client.fetch_all_live_items(collection_id)
        missing = [
            item for item in items
            if not (item.get("fieldData") or {}).get("gov-url")
        ]
        return missing, items

    def fill_item(
        self,
        collection_id: str,
        item: dict,
        gov_url: str,
    ) -> UpdateResult:
        """Set the gov-url field on a single item."""
        name = item_label(item)
        try:
            self._client.update_item_fields(collection_id, item, {"gov-url": gov_url})
            logger.info("Updated gov-url for '%s'", name)
            return UpdateResult(
                item_id=item["id"],
                item_name=name,
                fields_updated={"gov-url": gov_url},
                success=True,
            )
        except Exception as exc:
            logger.error("Failed to update gov-url for '%s': %s", name, exc)
            return UpdateResult(
                item_id=item["id"],
                item_name=name,
                fields_updated={},
                success=False,
                error=str(exc),
            )

    def fill_batch(
        self,
        collection_id: str,
        items_and_urls: list[tuple[dict, str]],
    ) -> FillResult:
        """Fill gov-url for a batch of (item, url) pairs."""
        result = FillResult(total_items=len(items_and_urls))
        for item, url in items_and_urls:
            update = self.fill_item(collection_id, item, url)
            result.updates.append(update)
            if update.success:
                result.items_updated += 1
            else:
                result.items_failed += 1
            time.sleep(THROTTLE_SECONDS)
        return result
