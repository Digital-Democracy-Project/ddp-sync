"""Service for detecting and resolving duplicate/companion bills."""

from __future__ import annotations

import logging
import re
import time
from collections import defaultdict

from ddp_sync.webflow_cms.client import WebflowClient
from ddp_sync.webflow_cms.constants import THROTTLE_SECONDS
from ddp_sync.webflow_cms.exceptions import WebflowConflictError
from ddp_sync.webflow_cms.models import DeleteResult, DuplicateGroup
from ddp_sync.webflow_cms.utils import (
    analyze_field_completeness,
    has_random_slug_suffix,
    item_label,
    normalize_title,
)

logger = logging.getLogger(__name__)


class UnionFind:
    """Union-Find (disjoint set) data structure with path compression and union by rank."""

    def __init__(self):
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: str, y: str) -> None:
        px, py = self.find(x), self.find(y)
        if px == py:
            return
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1


class DuplicateBillsService:
    """Detect duplicate and companion bills; migrate content and delete anomalous copies."""

    def __init__(self, client: WebflowClient):
        self._client = client

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def find_duplicates(self, collection_id: str) -> list[DuplicateGroup]:
        """Scan a bills collection and return groups of duplicates and companions.

        TRUE DUPLICATES are identified by:
          1. Same normalized title
          2. Same open-states-url-2

        POTENTIAL COMPANIONS are identified by:
          - Same gov-url but different open-states-url-2
        """
        items = self._client.fetch_all_live_items(collection_id)
        uf = UnionFind()

        item_data: dict[str, dict] = {}
        by_normalized_title: dict[str, list[str]] = defaultdict(list)
        by_open_states_url: dict[str, list[str]] = defaultdict(list)
        by_gov_url: dict[str, list[str]] = defaultdict(list)

        for item in items:
            data = item.get("fieldData") or {}
            name = data.get("name")
            item_id = item.get("id")
            if not name:
                continue

            slug = data.get("slug")
            open_states_url = data.get("open-states-url-2")
            gov_url = data.get("gov-url")

            item_data[item_id] = {
                "id": item_id,
                "name": name,
                "slug": slug,
                "open_states_url": open_states_url,
                "gov_url": gov_url,
                "is_hidden": data.get("public", False),
                "has_random_suffix": has_random_slug_suffix(slug),
                "field_data": data,
                "completeness": analyze_field_completeness(data),
                "original_item": item,
            }

            normalized = normalize_title(name)
            if normalized:
                by_normalized_title[normalized].append(item_id)
            if open_states_url and open_states_url.strip():
                by_open_states_url[open_states_url.strip()].append(item_id)
            if gov_url and gov_url.strip():
                by_gov_url[gov_url.strip()].append(item_id)

        # Union by normalized title and by open-states-url (true duplicates)
        for ids in by_normalized_title.values():
            if len(ids) > 1:
                for i in range(1, len(ids)):
                    uf.union(ids[0], ids[i])

        for ids in by_open_states_url.values():
            if len(ids) > 1:
                for i in range(1, len(ids)):
                    uf.union(ids[0], ids[i])

        # Build duplicate groups
        groups: dict[str, list[dict]] = defaultdict(list)
        for item_id in item_data:
            root = uf.find(item_id)
            groups[root].append(item_data[item_id])

        duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}
        items_in_dup = {it["id"] for its in duplicate_groups.values() for it in its}

        # Find companion groups (same gov-url, different open-states-url)
        companion_groups: dict[str, list[dict]] = {}
        for gov_url, ids in by_gov_url.items():
            candidates = [i for i in ids if i not in items_in_dup]
            if len(candidates) < 2:
                continue
            os_urls = {
                item_data[i].get("open_states_url", "").strip()
                for i in candidates
                if item_data[i].get("open_states_url")
            }
            if len(os_urls) >= 2:
                companion_groups[gov_url] = [item_data[i] for i in candidates]

        # Label and build DuplicateGroup objects
        result: list[DuplicateGroup] = []

        for root_id, items_list in duplicate_groups.items():
            sorted_by = sorted(items_list, key=lambda x: x["completeness"]["populated_count"], reverse=True)
            label = normalize_title(sorted_by[0]["name"]) or f"group-{root_id}"

            # Determine match reasons
            match_reasons: list[str] = []
            titles = {normalize_title(it["name"]) for it in items_list if it["name"]}
            if len(titles) <= 1:
                match_reasons.append("same title")
            else:
                match_reasons.append(f"different titles: {', '.join(sorted(titles))}")
            os_urls = {it["open_states_url"] for it in items_list if it.get("open_states_url")}
            if len(os_urls) == 1:
                match_reasons.append("shared open-states-url")

            for it in items_list:
                it["match_reasons"] = match_reasons
                it["group_type"] = "duplicate"

            result.append(DuplicateGroup(
                label=label,
                group_type="duplicate",
                match_reasons=match_reasons,
                items=items_list,
            ))

        for gov_url, items_list in companion_groups.items():
            sorted_by = sorted(items_list, key=lambda x: x["completeness"]["populated_count"], reverse=True)
            label = normalize_title(sorted_by[0]["name"]) or f"companions-{gov_url[:20]}"
            match_reasons = ["shared gov-url", "different open-states-urls (likely House/Senate companions)"]
            for it in items_list:
                it["match_reasons"] = match_reasons
                it["group_type"] = "companion"
            result.append(DuplicateGroup(
                label=label,
                group_type="companion",
                match_reasons=match_reasons,
                items=items_list,
            ))

        return result

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve_group(
        self,
        collection_id: str,
        correct_item_id: str,
        anomalous_item_ids: list[str],
        *,
        migrate_content: bool = True,
        delete_anomalous: bool = True,
        orgs_collection_id: str | None = None,
    ) -> list[DeleteResult]:
        """Resolve a duplicate group: migrate content and delete anomalous items.

        Args:
            collection_id: Bills collection ID.
            correct_item_id: The item to keep.
            anomalous_item_ids: Items to migrate from and delete.
            migrate_content: Whether to copy populated fields from anomalous -> correct.
            delete_anomalous: Whether to delete anomalous items after migration.
            orgs_collection_id: If provided, used for org name lookups during migration.

        Returns:
            List of DeleteResult for each anomalous item.
        """
        correct_item = self._client.fetch_item(collection_id, correct_item_id)
        if not correct_item:
            raise ValueError(f"Correct item {correct_item_id} not found")

        results: list[DeleteResult] = []
        correct_data = correct_item.get("fieldData") or {}

        for anom_id in anomalous_item_ids:
            anom_item = self._client.fetch_item(collection_id, anom_id)
            if not anom_item:
                results.append(DeleteResult(
                    item_id=anom_id,
                    item_name="(not found)",
                    deleted=False,
                    error=f"Item {anom_id} not found",
                ))
                continue

            anom_name = item_label(anom_item)
            anom_data = anom_item.get("fieldData") or {}

            # Migrate content
            if migrate_content:
                self._migrate_fields(collection_id, anom_data, correct_data, correct_item_id)

            # Delete
            if delete_anomalous:
                dr = self._delete_with_reference_handling(collection_id, anom_id, anom_name)
                results.append(dr)
            else:
                results.append(DeleteResult(item_id=anom_id, item_name=anom_name, deleted=False))

        return results

    def _migrate_fields(
        self,
        collection_id: str,
        source_data: dict,
        target_data: dict,
        target_item_id: str,
    ) -> None:
        """Copy populated fields from source to target where target is empty."""
        skip = {"name", "slug", "id", "_id", "_archived", "_draft", "created-on", "updated-on", "published-on"}
        multi_ref = {"member-organizations", "organizations-oppose", "bills-support", "bills-oppose"}
        fields_to_update: dict = {}

        for field_name, src_val in source_data.items():
            if field_name in skip or not src_val:
                continue
            if isinstance(src_val, list) and not src_val:
                continue

            tgt_val = target_data.get(field_name)

            if field_name in multi_ref and isinstance(src_val, list):
                tgt_list = tgt_val if isinstance(tgt_val, list) else []
                to_add = [x for x in src_val if x not in tgt_list]
                if to_add:
                    fields_to_update[field_name] = tgt_list + to_add
            else:
                is_empty = tgt_val is None or tgt_val == "" or (isinstance(tgt_val, list) and not tgt_val)
                if is_empty:
                    fields_to_update[field_name] = src_val

        if fields_to_update:
            self._client.update_item_field(
                collection_id, target_item_id,
                list(fields_to_update.keys())[0],
                list(fields_to_update.values())[0],
            )
            # For multiple fields, batch them
            if len(fields_to_update) > 1:
                payload = {"fieldData": fields_to_update}
                # Use raw PATCH via the retry mechanism
                from ddp_sync.webflow_cms.constants import API_BASE
                url = f"{API_BASE}/collections/{collection_id}/items/{target_item_id}/live"
                self._client._request_with_retry(
                    "PATCH", url,
                    headers=self._client._write_headers(),
                    json_body=payload,
                )
            logger.info("Migrated %d field(s) to %s", len(fields_to_update), target_item_id)

    def _delete_with_reference_handling(
        self,
        collection_id: str,
        item_id: str,
        item_name: str,
    ) -> DeleteResult:
        """Delete an item, removing references first if needed."""
        try:
            self._client.delete_item(collection_id, item_id)
            logger.info("Deleted '%s' (%s)", item_name, item_id)
            return DeleteResult(item_id=item_id, item_name=item_name, deleted=True)
        except WebflowConflictError as exc:
            # Remove references then retry
            removed = 0
            failed = 0
            for ref in exc.references:
                ref_info = ref.get("ref", {})
                ref_id = ref_info.get("id")
                ref_coll = ref_info.get("collectionId")
                if not ref_id or not ref_coll:
                    failed += 1
                    continue
                try:
                    self._client.remove_reference(ref_coll, ref_id, item_id)
                    removed += 1
                except Exception:
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
            except Exception as exc2:
                return DeleteResult(
                    item_id=item_id,
                    item_name=item_name,
                    deleted=False,
                    references_removed=removed,
                    references_failed=failed,
                    error=str(exc2),
                )
        except Exception as exc:
            return DeleteResult(
                item_id=item_id,
                item_name=item_name,
                deleted=False,
                error=str(exc),
            )
