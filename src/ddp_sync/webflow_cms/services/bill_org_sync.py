"""Service for synchronizing bills <-> organizations and parsing about-organization."""

from __future__ import annotations

import logging
import time

import requests as http_requests

from ddp_sync.webflow_cms.client import WebflowClient
from ddp_sync.webflow_cms.constants import ABOUT_FIELD_MAP, THROTTLE_SECONDS, ZAPIER_WEBHOOK_URL
from ddp_sync.webflow_cms.models import SyncResult
from ddp_sync.webflow_cms.utils import item_label, parse_about_organization

logger = logging.getLogger(__name__)


class BillOrgSyncService:
    """Synchronize bill<->org references, parse about-org, check missing fields."""

    def __init__(self, client: WebflowClient):
        self._client = client

    # ------------------------------------------------------------------
    # Missing-field checks + Zapier hooks
    # ------------------------------------------------------------------

    def check_missing_fields(
        self,
        orgs_collection_id: str,
        fields_to_check: list[str],
        *,
        send_zapier_hooks: bool = True,
        zapier_url: str = ZAPIER_WEBHOOK_URL,
    ) -> list[dict]:
        """Check orgs for missing fields and optionally send Zapier hooks.

        Args:
            orgs_collection_id: The organizations collection.
            fields_to_check: Field slugs to check (e.g. ["website", "email"]).
            send_zapier_hooks: Whether to POST to Zapier for each org with gaps.
            zapier_url: Override the default Zapier webhook URL.

        Returns:
            List of dicts with org_id, org_name, missing_fields for each org
            that has at least one missing field.
        """
        orgs = self._client.fetch_all_live_items(orgs_collection_id)
        results: list[dict] = []
        hooks_sent = 0

        for org in orgs:
            fd = org.get("fieldData") or {}
            missing = [f for f in fields_to_check if not fd.get(f)]
            if not missing:
                continue

            org_id = org.get("id")
            org_name = item_label(org)
            results.append({
                "org_id": org_id,
                "org_name": org_name,
                "missing_fields": missing,
            })

            if send_zapier_hooks:
                try:
                    resp = http_requests.post(
                        zapier_url,
                        json={
                            "member-cms": org_id,
                            "organization": org_name,
                            "missing-fields": ", ".join(missing),
                        },
                    )
                    if resp.status_code == 200:
                        hooks_sent += 1
                        logger.info("Zapier hook sent for '%s' (%s)", org_name, org_id)
                    else:
                        logger.warning(
                            "Zapier hook %s for '%s': %s",
                            resp.status_code, org_name, resp.text,
                        )
                except Exception as exc:
                    logger.error("Zapier hook failed for '%s': %s", org_name, exc)
                time.sleep(1)

        logger.info(
            "Missing-field check: %d org(s) with gaps, %d hooks sent",
            len(results), hooks_sent,
        )
        return results

    # ------------------------------------------------------------------
    # Parse about-organization into sub-fields
    # ------------------------------------------------------------------

    def parse_about_fields(self, orgs_collection_id: str) -> int:
        """Parse about-organization and fill sub-fields for orgs missing them.

        Skips orgs that already have description-4 populated.

        Returns:
            Number of orgs updated.
        """
        orgs = self._client.fetch_all_live_items(orgs_collection_id)
        updated = 0

        for org in orgs:
            fd = org.get("fieldData") or {}
            about = fd.get("about-organization")
            if not about:
                continue
            # Skip if already parsed
            if fd.get("description-4"):
                continue

            parsed = parse_about_organization(about)
            if not parsed:
                continue

            org_name = item_label(org)
            for section, content in parsed.items():
                slug = ABOUT_FIELD_MAP.get(section)
                if not slug:
                    logger.debug("No CMS mapping for section '%s' on '%s'", section, org_name)
                    continue
                logger.info("Parsing '%s' -> '%s' for '%s'", section, slug, org_name)
                self._client.update_item_fields(orgs_collection_id, org, {slug: content})

            updated += 1
            time.sleep(1)

        return updated

    # ------------------------------------------------------------------
    # Bill <-> Org reference sync
    # ------------------------------------------------------------------

    def sync_bill_org_references(
        self,
        bills_collection_id: str,
        orgs_collection_id: str,
    ) -> SyncResult:
        """Bidirectional sync of bill <-> org references.

        Pass 1 (bills -> orgs): For each bill, reads member-organizations and
        organizations-oppose. For each referenced org, appends the bill ID to
        the org's bills-support or bills-oppose list if not already present.

        Pass 2 (orgs -> bills): For each org, reads bills-support and
        bills-oppose. For each referenced bill, appends the org ID to the
        bill's member-organizations or organizations-oppose list if not
        already present.

        Returns:
            SyncResult with counts.
        """
        bills = self._client.fetch_all_live_items(bills_collection_id)
        orgs = self._client.fetch_all_live_items(orgs_collection_id)
        logger.info("Fetched %d bills and %d organizations", len(bills), len(orgs))

        result = SyncResult(bills_processed=len(bills))
        org_map = {o["id"]: o for o in orgs}
        bill_map = {b["id"]: b for b in bills}

        # Pass 1: bills -> orgs
        logger.info("Pass 1: syncing bill references to orgs...")
        for bill in bills:
            bill_id = bill.get("id")
            data = bill.get("fieldData") or {}

            # Supporting orgs
            for org_id in data.get("member-organizations") or []:
                self._sync_ref_to_org(
                    org_map, org_id, bill_id, orgs_collection_id,
                    "bills-support", result,
                )

            # Opposing orgs
            for org_id in data.get("organizations-oppose") or []:
                self._sync_ref_to_org(
                    org_map, org_id, bill_id, orgs_collection_id,
                    "bills-oppose", result,
                )

        # Pass 2: orgs -> bills
        logger.info("Pass 2: syncing org references to bills...")
        for org in orgs:
            org_id = org.get("id")
            org_data = org.get("fieldData") or {}

            # Bills this org supports
            for bill_id in org_data.get("bills-support") or []:
                self._sync_ref_to_bill(
                    bill_map, bill_id, org_id, bills_collection_id,
                    "member-organizations", result,
                )

            # Bills this org opposes
            for bill_id in org_data.get("bills-oppose") or []:
                self._sync_ref_to_bill(
                    bill_map, bill_id, org_id, bills_collection_id,
                    "organizations-oppose", result,
                )

        return result

    def _sync_ref_to_org(
        self,
        org_map: dict,
        org_id: str,
        bill_id: str,
        orgs_collection_id: str,
        field_name: str,
        result: SyncResult,
    ) -> None:
        org = org_map.get(org_id)
        if not org:
            result.errors.append(f"Org {org_id} not found")
            return
        org_data = org.get("fieldData") or {}
        current: list = org_data.get(field_name) or []
        if bill_id in current:
            return
        new_list = current + [bill_id]
        org_name = item_label(org)
        try:
            self._client.update_item_fields(
                orgs_collection_id, org, {field_name: new_list},
            )
            # Reflect locally so later bills append correctly
            org_map[org_id]["fieldData"][field_name] = new_list
            result.references_added += 1
            result.orgs_updated += 1
            logger.info("Updated org '%s' %s: +%s", org_name, field_name, bill_id)
        except Exception as exc:
            result.errors.append(f"Failed to update org {org_name}: {exc}")
            logger.error("Failed to update org '%s' %s: %s", org_name, field_name, exc)

    def _sync_ref_to_bill(
        self,
        bill_map: dict,
        bill_id: str,
        org_id: str,
        bills_collection_id: str,
        field_name: str,
        result: SyncResult,
    ) -> None:
        bill = bill_map.get(bill_id)
        if not bill:
            result.errors.append(f"Bill {bill_id} not found")
            return
        bill_data = bill.get("fieldData") or {}
        current: list = bill_data.get(field_name) or []
        if org_id in current:
            return
        new_list = current + [org_id]
        bill_name = item_label(bill)
        try:
            self._client.update_item_fields(
                bills_collection_id, bill, {field_name: new_list},
            )
            # Reflect locally so later orgs append correctly
            bill_map[bill_id]["fieldData"][field_name] = new_list
            result.references_added += 1
            result.bills_updated += 1
            logger.info("Updated bill '%s' %s: +%s", bill_name, field_name, org_id)
        except Exception as exc:
            result.errors.append(f"Failed to update bill {bill_name}: {exc}")
            logger.error("Failed to update bill '%s' %s: %s", bill_name, field_name, exc)
