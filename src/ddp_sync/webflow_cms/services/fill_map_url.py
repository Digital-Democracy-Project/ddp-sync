"""Service for filling map-url and setting bill visibility."""

from __future__ import annotations

import logging
import time

from ddp_sync.webflow_cms.client import WebflowClient
from ddp_sync.webflow_cms.constants import THROTTLE_SECONDS
from ddp_sync.webflow_cms.models import FillResult, UpdateResult
from ddp_sync.webflow_cms.utils import (
    build_map_url,
    is_compliant_map_url,
    item_label,
    parse_open_states_url,
)

logger = logging.getLogger(__name__)


class MapUrlService:
    """Build and fill the map-url field, and set bill visibility."""

    def __init__(self, client: WebflowClient):
        self._client = client

    def fill(self, collection_id: str, *, dry_run: bool = False) -> FillResult:
        """Scan all items, fill/fix map-url, and make visible.

        Processes three categories:
        1. Items missing map-url entirely (auto-fill + make visible)
        2. Items with non-compliant map-url (auto-fix + make visible)
        3. Items with compliant map-url but currently hidden (make visible)

        Only items with a voatzid and parseable open-states-url-2 are eligible.

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
            name = item_label(item)

            # Must have voatzId
            if not data.get("voatzid"):
                result.items_skipped += 1
                continue

            open_states_url = data.get("open-states-url-2")
            if not open_states_url:
                result.items_skipped += 1
                logger.debug("SKIP '%s': has voatzid but no open-states-url-2", name)
                continue

            parsed = parse_open_states_url(open_states_url)
            if not parsed:
                result.items_skipped += 1
                logger.warning("Could not parse URL for '%s': %s", name, open_states_url)
                continue

            expected_map_url = build_map_url(parsed)
            existing_map_url = data.get("map-url")
            # public=True means hidden in UI; public=False means visible
            is_hidden = data.get("public", False)

            fields: dict[str, object] = {}

            if not existing_map_url:
                fields["map-url"] = expected_map_url
                fields["public"] = False  # make visible
            elif not is_compliant_map_url(existing_map_url, expected_map_url):
                fields["map-url"] = expected_map_url
                fields["public"] = False
            elif is_hidden:
                fields["public"] = False
            else:
                # Already compliant and visible
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
                logger.info("Updated '%s': %s", name, list(fields.keys()))
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
