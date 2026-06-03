"""
Webflow CMS batch operations.

Moved from DDP-API scheduler.py. Weekly maintenance jobs that fill
missing CMS fields, sync references, and detect duplicates.
"""

import logging
import os

from ddp_sync.config import get_settings

logger = logging.getLogger(__name__)


def _get_webflow_client():
    """Instantiate a WebflowClient from config/env."""
    try:
        settings = get_settings()
        token = settings.webflow_api_token
        bills_cid = settings.webflow_bills_collection_id
        orgs_cid = settings.webflow_organizations_collection_id
    except Exception:
        token = os.getenv("WEBFLOW_API_TOKEN", "")
        bills_cid = os.getenv("WEBFLOW_BILLS_COLLECTION_ID", "")
        orgs_cid = os.getenv("WEBFLOW_ORGS_COLLECTION_ID", "")

    if not token:
        logger.error("WEBFLOW_API_TOKEN not configured — skipping Webflow job")
        return None, None, None

    from webflow_cms import WebflowClient
    return WebflowClient(token), bills_cid, orgs_cid


def run_webflow_fill_session_code():
    """Scheduled job: fill session-code/bill-prefix/bill-number."""
    logger.info("Starting Webflow fill-session-code job")
    try:
        client, bills_cid, _ = _get_webflow_client()
        if not client or not bills_cid:
            return
        from webflow_cms.services.fill_session_code import SessionCodeService
        result = SessionCodeService(client).fill(bills_cid)
        logger.info(f"Fill session-code: {result.items_updated} updated, {result.items_failed} failed")
    except Exception as e:
        logger.error(f"Webflow fill-session-code job failed: {e}")


def run_webflow_fill_map_url():
    """Scheduled job: fill map-url and set visibility."""
    logger.info("Starting Webflow fill-map-url job")
    try:
        client, bills_cid, _ = _get_webflow_client()
        if not client or not bills_cid:
            return
        from webflow_cms.services.fill_map_url import MapUrlService
        result = MapUrlService(client).fill(bills_cid)
        logger.info(f"Fill map-url: {result.items_updated} updated, {result.items_failed} failed")
    except Exception as e:
        logger.error(f"Webflow fill-map-url job failed: {e}")


def run_webflow_bill_org_sync():
    """Scheduled job: sync bill-org references."""
    logger.info("Starting Webflow bill-org sync job")
    try:
        client, bills_cid, orgs_cid = _get_webflow_client()
        if not client or not bills_cid or not orgs_cid:
            return
        from webflow_cms.services.bill_org_sync import BillOrgSyncService
        result = BillOrgSyncService(client).sync_bill_org_references(bills_cid, orgs_cid)
        logger.info(f"Bill-org sync: {result.references_added} refs added, {len(result.errors)} errors")
    except Exception as e:
        logger.error(f"Webflow bill-org sync job failed: {e}")


def run_webflow_org_about_parse():
    """Scheduled job: parse about-organization into sub-fields."""
    logger.info("Starting Webflow org-about-parse job")
    try:
        client, _, orgs_cid = _get_webflow_client()
        if not client or not orgs_cid:
            return
        from webflow_cms.services.bill_org_sync import BillOrgSyncService
        updated = BillOrgSyncService(client).parse_about_fields(orgs_cid)
        logger.info(f"Org about-parse: {updated} orgs updated")
    except Exception as e:
        logger.error(f"Webflow org-about-parse job failed: {e}")


def run_webflow_check_org_missing():
    """Scheduled job: check for missing org fields, send Zapier hooks."""
    logger.info("Starting Webflow check-org-missing job")
    try:
        client, _, orgs_cid = _get_webflow_client()
        if not client or not orgs_cid:
            return
        from webflow_cms.services.bill_org_sync import BillOrgSyncService
        fields = ["about-organization", "website", "email", "contact-form", "description-4"]
        results = BillOrgSyncService(client).check_missing_fields(orgs_cid, fields)
        logger.info(f"Check org missing: {len(results)} orgs with gaps")
    except Exception as e:
        logger.error(f"Webflow check-org-missing job failed: {e}")


def run_webflow_find_duplicates():
    """Scheduled job: find duplicate bills (report only, no resolution)."""
    logger.info("Starting Webflow find-duplicates job")
    try:
        client, bills_cid, _ = _get_webflow_client()
        if not client or not bills_cid:
            return
        from webflow_cms.services.duplicate_bills import DuplicateBillsService
        groups = DuplicateBillsService(client).find_duplicates(bills_cid)
        dups = [g for g in groups if g.group_type == "duplicate"]
        comps = [g for g in groups if g.group_type == "companion"]
        logger.info(f"Find duplicates: {len(dups)} duplicate groups, {len(comps)} companion groups")
    except Exception as e:
        logger.error(f"Webflow find-duplicates job failed: {e}")


def run_webflow_merge_duplicate_orgs():
    """Scheduled job: detect and merge exact-duplicate organizations."""
    logger.info("Starting Webflow merge-duplicate-orgs job")
    try:
        client, bills_cid, orgs_cid = _get_webflow_client()
        if not client or not bills_cid or not orgs_cid:
            return
        from webflow_cms.services.org_merge import OrgMergeService
        results = OrgMergeService(client).find_and_merge_exact_duplicates(orgs_cid, bills_cid)
        succeeded = [r for r in results if r.deleted]
        failed = [r for r in results if not r.deleted]
        logger.info(
            f"Merge duplicate orgs: {len(results)} processed, "
            f"{len(succeeded)} merged, {len(failed)} failed"
        )
        for r in succeeded:
            logger.info(
                f"  Merged '{r.duplicate_name}' ({r.duplicate_id}) -> "
                f"'{r.canonical_name}' | fields: {r.fields_migrated} | "
                f"bill refs repointed: {r.bill_refs_repointed}"
            )
        for r in failed:
            logger.error(
                f"  Failed to merge '{r.duplicate_name}' ({r.duplicate_id}): {r.error}"
            )
    except Exception as e:
        logger.error(f"Webflow merge-duplicate-orgs job failed: {e}")
