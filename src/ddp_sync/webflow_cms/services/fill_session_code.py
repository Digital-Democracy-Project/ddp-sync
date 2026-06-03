"""Service for filling session-code, bill-prefix, and bill-number from open-states-url-2."""

from __future__ import annotations

import logging
import time

from ddp_sync.webflow_cms.client import WebflowClient
from ddp_sync.webflow_cms.constants import THROTTLE_SECONDS
from ddp_sync.webflow_cms.models import FillResult, UpdateResult
from ddp_sync.webflow_cms.utils import item_label, parse_open_states_url

logger = logging.getLogger(__name__)


class SessionCodeService:
    """Parse open-states-url-2 and fill session-code, bill-prefix, bill-number."""

    def __init__(self, client: WebflowClient):
        self._client = client

    def fill(self, collection_id: str, *, dry_run: bool = False) -> FillResult:
        """Scan all items and fill missing session-code / bill-prefix / bill-number.

        Args:
            collection_id: The bills collection.
            dry_run: If True, report what would change without writing.

        Returns:
            FillResult with per-item details.
        """
        items = self._client.fetch_all_live_items(collection_id)
        result = FillResult(total_items=len(items))

        for item in items:
            data = item.get("fieldData") or {}
            open_states_url = data.get("open-states-url-2")
            name = item_label(item)

            if not open_states_url:
                result.items_skipped += 1
                continue

            # Already filled?
            if data.get("session-code") and data.get("bill-prefix") and data.get("bill-number"):
                result.items_already_filled += 1
                logger.debug("SKIP '%s': already filled", name)
                continue

            parsed = parse_open_states_url(open_states_url)
            if not parsed:
                result.items_skipped += 1
                logger.warning("Could not parse URL for '%s': %s", name, open_states_url)
                continue

            # Build update dict for only the missing fields
            fields: dict[str, str] = {}
            if not data.get("session-code"):
                fields["session-code"] = parsed["session_code"]
            if not data.get("bill-prefix"):
                fields["bill-prefix"] = parsed["bill_prefix"]
            if not data.get("bill-number"):
                fields["bill-number"] = parsed["bill_number"]

            if not fields:
                result.items_already_filled += 1
                continue

            if dry_run:
                result.updates.append(UpdateResult(
                    item_id=item["id"],
                    item_name=name,
                    fields_updated=fields,
                    success=True,
                ))
                result.items_updated += 1
                continue

            try:
                self._client.update_item_fields(collection_id, item, fields)
                logger.info("Updated '%s': %s", name, fields)
                result.updates.append(UpdateResult(
                    item_id=item["id"],
                    item_name=name,
                    fields_updated=fields,
                    success=True,
                ))
                result.items_updated += 1
            except Exception as exc:
                logger.error("Failed to update '%s': %s", name, exc)
                result.updates.append(UpdateResult(
                    item_id=item["id"],
                    item_name=name,
                    fields_updated={},
                    success=False,
                    error=str(exc),
                ))
                result.items_failed += 1

            time.sleep(THROTTLE_SECONDS)

        return result
