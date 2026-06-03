"""Service for merging duplicate organizations in the Webflow CMS.

Merge steps for each (duplicate -> canonical) pair:
  1. Union bills-support / bills-oppose from duplicate into canonical.
  2. Copy any populated scalar fields from duplicate to canonical where
     canonical has no value (same fill-forward logic as _migrate_fields).
  3. Re-point bill references: for every bill that lists the duplicate org
     in member-organizations or organizations-oppose, swap in the canonical
     org ID.
  4. Delete the duplicate org.

Bills are identified from the duplicate org's bills-support / bills-oppose
lists rather than a full collection scan, so the operation is O(bills on
that org) rather than O(all bills). Running sync_bill_org_references after
a merge pass will catch any stragglers where the bidirectional sync had not
yet run.
"""

from __future__ import annotations

import logging
import time

from ddp_sync.webflow_cms.client import WebflowClient
from ddp_sync.webflow_cms.constants import THROTTLE_SECONDS
from ddp_sync.webflow_cms.exceptions import WebflowConflictError
from ddp_sync.webflow_cms.models import DeleteResult, MergeResult
from ddp_sync.webflow_cms.utils import item_label

logger = logging.getLogger(__name__)

_SKIP_FIELDS = {"name", "slug", "id", "_id", "_archived", "_draft",
                "created-on", "updated-on", "published-on"}
_ORG_MULTI_REF = {"bills-support", "bills-oppose"}
_BILL_MULTI_REF = {"member-organizations", "organizations-oppose"}

# Which org field maps to which bill field when re-pointing references
_ORG_TO_BILL_FIELD = {
    "bills-support": "member-organizations",
    "bills-oppose": "organizations-oppose",
}


def _normalize_for_dedup(name: str) -> str:
    """Normalize an org name to a canonical key for duplicate detection."""
    import re
    if not isinstance(name, str):
        return ""
    n = name.lower().strip()
    n = n.replace("&", "and")
    n = re.sub(r"['’‘]", "'", n)
    n = re.sub(r"[–—]", "-", n)
    n = re.sub(r"[|,()]", " ", n)
    n = re.sub(r"^the\s+", "", n)
    for suffix in [
        r",?\s+inc\.?$", r",?\s+llc\.?$", r",?\s+corp\.?$",
        r",?\s+aca$", r",?\s+association$", r",?\s+assoc\.?$",
        r",?\s+foundation$", r",?\s+fund$", r",?\s+institute$",
    ]:
        n = re.sub(suffix, "", n)
    return " ".join(n.split())


def _richness_score(item: dict) -> int:
    """Score an org by data richness — used to pick the canonical record."""
    fd = item.get("fieldData") or {}
    bill_refs = len(fd.get("bills-support") or []) + len(fd.get("bills-oppose") or [])
    populated = sum(1 for v in fd.values() if v and v != [] and v != "")
    return bill_refs * 10 + populated


class OrgMergeService:
    """Merge duplicate organizations, preserving all bill references."""

    def __init__(self, client: WebflowClient):
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_and_merge_exact_duplicates(
        self,
        orgs_collection_id: str,
        bills_collection_id: str,
    ) -> list[MergeResult]:
        """Detect exact-name duplicates across the org collection and merge them.

        Two orgs are considered exact duplicates when their names are identical
        after normalization (lowercasing, stripping "The", "&" -> "and", common
        suffixes, etc.).  For each duplicate group the record with the most bill
        references + populated fields is kept as canonical; all others are merged
        into it and deleted.

        Returns:
            List of MergeResult, one per duplicate that was processed.
        """
        from collections import defaultdict

        logger.info("Fetching all orgs to detect exact duplicates...")
        orgs = self._client.fetch_all_live_items(orgs_collection_id)
        logger.info("Fetched %d orgs", len(orgs))

        # Group by normalized name
        groups: dict[str, list[dict]] = defaultdict(list)
        for org in orgs:
            key = _normalize_for_dedup((org.get("fieldData") or {}).get("name", ""))
            if key:
                groups[key].append(org)

        dup_groups = {k: v for k, v in groups.items() if len(v) > 1}
        logger.info("Found %d exact-duplicate group(s) (%d orgs involved)",
                    len(dup_groups), sum(len(v) for v in dup_groups.values()))

        results: list[MergeResult] = []
        for norm_name, members in dup_groups.items():
            # Pick canonical: highest richness score
            members_sorted = sorted(members, key=_richness_score, reverse=True)
            canonical = members_sorted[0]
            duplicates = members_sorted[1:]
            canonical_id = canonical["id"]

            logger.info(
                "Group '%s': keeping %s, merging %d duplicate(s)",
                norm_name, canonical_id, len(duplicates),
            )

            for dup in duplicates:
                result = self.merge(
                    dup["id"], canonical_id,
                    orgs_collection_id, bills_collection_id,
                )
                results.append(result)
                time.sleep(THROTTLE_SECONDS)

        return results

    def merge(
        self,
        duplicate_id: str,
        canonical_id: str,
        orgs_collection_id: str,
        bills_collection_id: str,
    ) -> MergeResult:
        """Merge one duplicate org into the canonical org and delete it.

        Args:
            duplicate_id: CMS ID of the org to remove.
            canonical_id: CMS ID of the org to keep.
            orgs_collection_id: Member Organizations collection ID.
            bills_collection_id: Bills collection ID.

        Returns:
            MergeResult with counts and status.
        """
        duplicate = self._client.fetch_item(orgs_collection_id, duplicate_id)
        canonical = self._client.fetch_item(orgs_collection_id, canonical_id)

        if not duplicate:
            return MergeResult(
                duplicate_id=duplicate_id, canonical_id=canonical_id,
                duplicate_name="(not found)", canonical_name="",
                error=f"Duplicate org {duplicate_id} not found",
            )
        if not canonical:
            return MergeResult(
                duplicate_id=duplicate_id, canonical_id=canonical_id,
                duplicate_name=item_label(duplicate), canonical_name="(not found)",
                error=f"Canonical org {canonical_id} not found",
            )

        dup_name = item_label(duplicate)
        can_name = item_label(canonical)
        result = MergeResult(
            duplicate_id=duplicate_id, canonical_id=canonical_id,
            duplicate_name=dup_name, canonical_name=can_name,
        )

        dup_fd = duplicate.get("fieldData") or {}
        can_fd = canonical.get("fieldData") or {}

        # Step 1 + 2: merge org-side fields into canonical
        fields_to_update = self._build_canonical_update(dup_fd, can_fd)
        if fields_to_update:
            try:
                self._client.update_item_fields(
                    orgs_collection_id, canonical, fields_to_update,
                )
                result.fields_migrated = list(fields_to_update.keys())
                logger.info(
                    "Merged fields %s from '%s' into '%s'",
                    list(fields_to_update.keys()), dup_name, can_name,
                )
            except Exception as exc:
                result.error = f"Failed to update canonical org: {exc}"
                logger.error("Failed to update canonical org '%s': %s", can_name, exc)
                return result
            time.sleep(THROTTLE_SECONDS)

        # Step 3: re-point bill references
        bill_refs_repointed = self._repoint_bill_references(
            dup_fd, duplicate_id, canonical_id, bills_collection_id,
        )
        result.bill_refs_repointed = bill_refs_repointed

        # Step 4: delete the duplicate
        delete_result = self._delete_org(orgs_collection_id, duplicate_id, dup_name)
        result.deleted = delete_result.deleted
        if not delete_result.deleted:
            result.error = delete_result.error

        return result

    def merge_many(
        self,
        pairs: list[tuple[str, str]],
        orgs_collection_id: str,
        bills_collection_id: str,
    ) -> list[MergeResult]:
        """Merge multiple (duplicate_id, canonical_id) pairs.

        Args:
            pairs: List of (duplicate_id, canonical_id) tuples.
            orgs_collection_id: Member Organizations collection ID.
            bills_collection_id: Bills collection ID.

        Returns:
            List of MergeResult, one per pair.
        """
        results = []
        for duplicate_id, canonical_id in pairs:
            logger.info("Merging %s -> %s", duplicate_id, canonical_id)
            result = self.merge(
                duplicate_id, canonical_id,
                orgs_collection_id, bills_collection_id,
            )
            results.append(result)
            time.sleep(THROTTLE_SECONDS)
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_canonical_update(self, dup_fd: dict, can_fd: dict) -> dict:
        """Build a field-update payload to apply to the canonical org."""
        update: dict = {}

        for field_name, src_val in dup_fd.items():
            if field_name in _SKIP_FIELDS or not src_val:
                continue

            tgt_val = can_fd.get(field_name)

            if field_name in _ORG_MULTI_REF and isinstance(src_val, list):
                # Union: add any bill IDs the canonical doesn't already have
                tgt_list = tgt_val if isinstance(tgt_val, list) else []
                to_add = [x for x in src_val if x not in tgt_list]
                if to_add:
                    update[field_name] = tgt_list + to_add
            else:
                # Scalar: only fill if canonical field is empty
                is_empty = (
                    tgt_val is None
                    or tgt_val == ""
                    or (isinstance(tgt_val, list) and not tgt_val)
                )
                if is_empty:
                    update[field_name] = src_val

        return update

    def _repoint_bill_references(
        self,
        dup_fd: dict,
        duplicate_id: str,
        canonical_id: str,
        bills_collection_id: str,
    ) -> int:
        """Swap duplicate_id -> canonical_id in each bill that references the duplicate.

        Uses the duplicate org's bills-support / bills-oppose lists to identify
        which bills to patch, avoiding a full collection scan.

        Returns:
            Number of bill fields re-pointed.
        """
        repointed = 0

        for org_field, bill_field in _ORG_TO_BILL_FIELD.items():
            bill_ids = dup_fd.get(org_field) or []
            for bill_id in bill_ids:
                bill = self._client.fetch_item(bills_collection_id, bill_id)
                if not bill:
                    logger.warning("Bill %s not found — skipping ref repoint", bill_id)
                    continue

                bill_fd = bill.get("fieldData") or {}
                current = bill_fd.get(bill_field) or []

                if duplicate_id not in current:
                    logger.debug(
                        "Bill %s does not reference duplicate %s in %s — skipping",
                        bill_id, duplicate_id, bill_field,
                    )
                    continue

                # Swap duplicate_id for canonical_id, preserving order, deduping
                seen: set[str] = set()
                new_list: list[str] = []
                for ref in current:
                    replacement = canonical_id if ref == duplicate_id else ref
                    if replacement not in seen:
                        seen.add(replacement)
                        new_list.append(replacement)

                try:
                    self._client.update_item_fields(
                        bills_collection_id, bill, {bill_field: new_list},
                    )
                    repointed += 1
                    logger.info(
                        "Re-pointed bill %s %s: %s -> %s",
                        bill_id, bill_field, duplicate_id, canonical_id,
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to re-point bill %s %s: %s", bill_id, bill_field, exc,
                    )
                time.sleep(THROTTLE_SECONDS)

        return repointed

    def _delete_org(
        self,
        orgs_collection_id: str,
        item_id: str,
        item_name: str,
    ) -> DeleteResult:
        """Delete the duplicate org, stripping any remaining references first."""
        try:
            self._client.delete_item(orgs_collection_id, item_id)
            logger.info("Deleted duplicate org '%s' (%s)", item_name, item_id)
            return DeleteResult(item_id=item_id, item_name=item_name, deleted=True)
        except WebflowConflictError as exc:
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
                    logger.info("Removed stale ref to '%s' from %s/%s", item_name, ref_coll, ref_id)
                except Exception:
                    failed += 1
                time.sleep(0.3)

            try:
                self._client.delete_item(orgs_collection_id, item_id)
                logger.info(
                    "Deleted duplicate org '%s' after removing %d stale ref(s)",
                    item_name, removed,
                )
                return DeleteResult(
                    item_id=item_id, item_name=item_name, deleted=True,
                    references_removed=removed, references_failed=failed,
                )
            except Exception as exc2:
                return DeleteResult(
                    item_id=item_id, item_name=item_name, deleted=False,
                    references_removed=removed, references_failed=failed,
                    error=str(exc2),
                )
        except Exception as exc:
            return DeleteResult(
                item_id=item_id, item_name=item_name, deleted=False, error=str(exc),
            )
