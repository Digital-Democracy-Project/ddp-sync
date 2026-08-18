"""Bill version sync service — detects newer bill text versions and re-ingests.

Replaces the daily bill-history/bill-votes sync with a targeted check:
1. For each current-session bill, fetch OpenStates `versions` array
2. Compare latest version against ddp-broker-py's BillVersion table
   (PLAN-bill-document-provenance.md Phase 4 — replaces the Redis version
   cache this used to read/write; Redis is still used for VoteBot's
   separate webflow_id -> slug lookup cache, see check_and_reingest_version)
3. If newer: download bill text (PDF or HTML), re-ingest into Pinecone,
   update Webflow CMS gov-url
4. If unchanged: no-op (VoteBot's slug cache is still refreshed either way)
"""

import asyncio
import difflib
import gc
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import structlog
import yaml

from ddp_sync.config import Settings, get_settings

logger = structlog.get_logger()

DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent.parent / "config" / "sync_schedule.yaml"


@dataclass
class VersionCheckResult:
    """Result of checking a single bill's version."""

    webflow_id: str
    bill_title: str
    jurisdiction: str
    status: str  # "updated", "unchanged", "partial", "no_versions", "error", "skipped"
    version_note: str = ""
    version_date: str = ""
    text_url: str = ""
    chunks_created: int = 0
    history_chunks_created: int = 0
    changelog_chunks_created: int = 0
    changelog_skipped: bool = False
    changelog_skip_reason: str = ""
    surplus_chunks_deleted: int = 0
    webflow_updated: bool = False
    status_updated: bool = False
    webflow_patch_skipped: bool = False
    error: str | None = None


@dataclass
class VersionSyncBatchResult:
    """Aggregate result of a batch version sync run."""

    total_bills: int = 0
    checked: int = 0
    updated: int = 0
    unchanged: int = 0
    no_versions: int = 0
    skipped: int = 0
    failed: int = 0
    chunks_created: int = 0
    history_chunks_created: int = 0
    changelog_chunks_created: int = 0
    changelogs_skipped: int = 0
    surplus_chunks_deleted: int = 0
    webflow_updates: int = 0
    status_updates: int = 0
    webflow_skipped: int = 0
    webflow_patch_failures: int = 0
    no_latest_action: int = 0
    skipped_no_url: int = 0
    skipped_not_current: int = 0
    skipped_jurisdiction: int = 0
    errors: list[str] = field(default_factory=list)


class BillVersionSyncService:
    """Service for detecting and syncing newer bill text versions.

    Delegates to existing services:
    - BillSyncService: OpenStates URL parsing, API fetching, session detection, rate limiting
    - WebflowSource: PDF/HTML text extraction and document creation
    - IngestionPipeline: Chunking and Pinecone upsert
    - WebflowLookupService: CMS gov-url update
    - RedisStore: Version cache
    """

    def __init__(self, settings: Settings | None = None, config_path: Path | None = None):
        self.settings = settings or get_settings()
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        self._config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        """Load bill_version_check config from sync_schedule.yaml."""
        if not self.config_path.exists():
            return {}
        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f) or {}
            return config.get("bill_version_check", {})
        except Exception as e:
            logger.error("Failed to load bill version sync config", error=str(e))
            return {}

    @staticmethod
    def _get_latest_version(versions: list[dict]) -> dict | None:
        """Get the latest version from OpenStates versions array.

        Sorts by date descending, returns the first entry.
        OpenStates versions have: date, note, links[{url, media_type}].
        """
        if not versions:
            return None
        # Sort by date descending — versions without dates go last
        sorted_versions = sorted(
            versions,
            key=lambda v: v.get("date") or "",
            reverse=True,
        )
        return sorted_versions[0]

    @staticmethod
    def _get_best_text_url(version: dict) -> tuple[str, str] | None:
        """Extract best URL + media_type from a version's links.

        Priority: application/pdf first, then text/html.

        Returns:
            (url, media_type) tuple, or None if no usable links
        """
        links = version.get("links", [])
        if not links:
            return None

        # Prefer PDF over HTML
        pdf_link = None
        html_link = None

        for link in links:
            url = link.get("url", "")
            media_type = (link.get("media_type") or "").lower()

            if not url:
                continue

            if "application/pdf" in media_type:
                pdf_link = (url, "application/pdf")
            elif "text/html" in media_type:
                html_link = (url, "text/html")
            elif not pdf_link and not html_link:
                # Unknown media type — keep as fallback
                html_link = (url, media_type or "unknown")

        return pdf_link or html_link

    @staticmethod
    def _extract_bill_openstates_id(bill_data: dict) -> str | None:
        """Bare UUID (no `ocd-bill/` prefix) from an OpenStates bill response's
        `id` field, matching ddp-broker-py's Bill.openstates_id convention
        (PLAN-bill-document-provenance.md Phase 4). None if bill_data has no
        id — shouldn't happen for a real OpenStates API response, but this
        function doesn't assume it.
        """
        raw_id = bill_data.get("id") or ""
        if not raw_id:
            return None
        return raw_id.rsplit("/", 1)[-1]

    @staticmethod
    def _is_newer_version(latest_version: dict, cached: dict | None) -> bool:
        """Determine if the latest version is newer than cached.

        Returns True if:
        - No cache exists (first run)
        - Date is newer
        - Same date but different version note (e.g., "Engrossed" vs "Introduced")
        - URL changed
        """
        if cached is None:
            return True

        latest_date = latest_version.get("date") or ""
        cached_date = cached.get("version_date") or ""

        # Newer date
        if latest_date > cached_date:
            return True

        # Same date, different note
        latest_note = latest_version.get("note") or ""
        cached_note = cached.get("version_note") or ""
        if latest_date == cached_date and latest_note != cached_note:
            return True

        # URL changed
        best_url = BillVersionSyncService._get_best_text_url(latest_version)
        if best_url:
            url, _ = best_url
            if url != cached.get("text_url", ""):
                return True

        return False

    @staticmethod
    def _extract_latest_action(bill_data: dict) -> tuple[str | None, str | None, str | None]:
        """Extract the latest action description, date, and chamber from OpenStates bill data.

        OpenStates v3 API returns `latest_action_description` and
        `latest_action_date` as top-level fields. The chamber is derived
        from the last entry in the `actions` array, which includes an
        `organization.name` field (e.g. "House", "Senate").

        The date is YYYY-MM-DD; we convert it to ISO 8601 for Webflow's
        timestamp field type.

        Returns:
            (description, iso_date, chamber) tuple — any may be None
        """
        description = bill_data.get("latest_action_description") or None
        action_date = bill_data.get("latest_action_date")
        # Convert to Webflow-compatible ISO 8601 timestamp.
        # OpenStates returns either "YYYY-MM-DD" or full ISO like
        # "2026-02-25T17:37:53+00:00" — only append time suffix if
        # the date doesn't already contain one.
        if action_date and "T" not in action_date:
            iso_date = f"{action_date}T00:00:00.000Z"
        elif action_date:
            # Already has time component; normalize to Webflow format
            from datetime import datetime, timezone
            try:
                dt = datetime.fromisoformat(action_date)
                iso_date = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            except (ValueError, TypeError):
                iso_date = action_date
        else:
            iso_date = None

        # Extract chamber from the last action in the actions array.
        # Each action has organization.name (e.g. "House", "Senate").
        chamber = None
        actions = bill_data.get("actions") or []
        if actions:
            last_action = actions[-1]
            org = last_action.get("organization") or {}
            chamber = org.get("name") or None

        return description, iso_date, chamber

    @staticmethod
    def _dates_match(cms_date: str | None, openstates_date: str | None) -> bool:
        """Compare a Webflow CMS date with an OpenStates-derived date.

        Webflow may return dates with varying precision (e.g.
        ``2026-02-25T00:00:00.000Z`` vs ``2026-02-25T00:00:00Z``).
        We normalise both to ``YYYY-MM-DD`` before comparing.
        """
        if not cms_date and not openstates_date:
            return True
        if not cms_date or not openstates_date:
            return False
        return cms_date[:10] == openstates_date[:10]

    async def _update_webflow_status(
        self,
        webflow_id: str,
        bill_title: str,
        new_status: str,
        status_date: str | None = None,
        status_chamber: str | None = None,
    ) -> bool:
        """Update the status, status-date, and status-chamber fields for a bill in Webflow CMS.

        Args:
            webflow_id: Webflow item ID
            bill_title: For logging
            new_status: Latest action description from OpenStates
            status_date: ISO 8601 timestamp for the action date
            status_chamber: Chamber where the latest action occurred (e.g. "House", "Senate")

        Returns:
            True on success, False on failure
        """
        try:
            from ddp_sync.services.webflow_lookup import WebflowLookupService

            lookup = WebflowLookupService(self.settings)
            scheduler_key = self.settings.webflow_scheduler_api_key
            field_data: dict[str, str] = {"status": new_status}
            if status_date:
                field_data["status-date"] = status_date
            if status_chamber:
                field_data["status-chamber"] = status_chamber
            return await lookup.update_bill_fields(
                webflow_id,
                field_data,
                api_key=scheduler_key or None,
            )
        except Exception as e:
            logger.warning(
                "Failed to update Webflow status",
                webflow_id=webflow_id,
                bill_title=bill_title,
                error=str(e),
            )
            return False

    async def update_bill_status(
        self,
        webflow_id: str,
        bill_title: str,
        jurisdiction_code: str,
        bill_data: dict,
        fields: dict,
    ) -> dict:
        """Flow 1 write path: update Webflow CMS status fields from OpenStates data.

        Extracts status, status-date, status-chamber, and gov-url from the
        already-fetched bill_data and PATCHes Webflow if any values differ
        from the current CMS fields.

        Args:
            webflow_id: Webflow item ID
            bill_title: Human-readable bill title
            jurisdiction_code: Two-letter state code
            bill_data: Full bill data dict from OpenStates API
            fields: Current CMS field data from Webflow

        Returns:
            Dict with keys: status_updated, webflow_updated, patch_skipped,
            latest_action, action_date, action_chamber, gov_url
        """
        from ddp_sync.services.redis_store import get_redis_store

        result = {
            "status_updated": False,
            "webflow_updated": False,
            "patch_skipped": False,
            "latest_action": None,
            "action_date": None,
            "action_chamber": None,
            "gov_url": None,
        }

        latest_action, action_date, action_chamber = self._extract_latest_action(bill_data)
        result["latest_action"] = latest_action
        result["action_date"] = action_date
        result["action_chamber"] = action_chamber

        # Extract gov-url from the latest version's text link
        versions = bill_data.get("versions", [])
        latest_version = self._get_latest_version(versions)
        text_url = None
        if latest_version:
            url_info = self._get_best_text_url(latest_version)
            if url_info:
                text_url = url_info[0]
        result["gov_url"] = text_url

        if not latest_action:
            logger.warning(
                "No latest_action from OpenStates — status not updated",
                bill_title=bill_title,
                webflow_id=webflow_id,
                jurisdiction=jurisdiction_code,
                cms_status=fields.get("status", ""),
            )
            return result

        skip_webflow = self._config.get("skip_webflow_update", False)
        if skip_webflow:
            return result

        # Build PATCH payload — only include fields that differ from CMS
        field_data: dict[str, str] = {}
        if text_url and fields.get("gov-url") != text_url:
            field_data["gov-url"] = text_url
        if fields.get("status") != latest_action:
            field_data["status"] = latest_action
        if action_date and not self._dates_match(fields.get("status-date"), action_date):
            field_data["status-date"] = action_date
        if action_chamber and fields.get("status-chamber") != action_chamber:
            field_data["status-chamber"] = action_chamber

        if not field_data:
            result["patch_skipped"] = True
            logger.debug(
                "Skipping Webflow PATCH — status already matches",
                bill_title=bill_title,
                status=latest_action,
            )
        else:
            try:
                from ddp_sync.services.webflow_lookup import WebflowLookupService

                lookup = WebflowLookupService(self.settings)
                scheduler_key = self.settings.webflow_scheduler_api_key
                success = await lookup.update_bill_fields(
                    webflow_id,
                    field_data,
                    api_key=scheduler_key or None,
                )
                if success:
                    result["webflow_updated"] = True
                    if "status" in field_data:
                        result["status_updated"] = True
                    logger.info(
                        "Bill status updated in Webflow",
                        bill_title=bill_title,
                        fields_updated=list(field_data.keys()),
                        status=latest_action,
                        status_date=action_date,
                        status_chamber=action_chamber,
                    )
                else:
                    logger.warning(
                        "Webflow status PATCH failed",
                        bill_title=bill_title,
                        webflow_id=webflow_id,
                        attempted_fields=list(field_data.keys()),
                    )
            except Exception as e:
                logger.warning(
                    "Failed to update Webflow fields",
                    webflow_id=webflow_id,
                    error=str(e),
                )

        # Write per-bill status cache to Redis
        redis_store = get_redis_store()
        await redis_store.set_bill_status(webflow_id, {
            "status": latest_action,
            "status_date": action_date or "",
            "status_chamber": action_chamber or "",
            "gov_url": text_url or "",
            "last_synced": datetime.utcnow().isoformat(),
        })

        return result

    async def check_and_reingest_version(
        self,
        webflow_id: str,
        bill_title: str,
        jurisdiction_code: str,
        bill_data: dict,
        bill_slug: str,
        fields: dict,
    ) -> dict:
        """Flow 2 write path: check for new bill version and re-ingest to Pinecone.

        Compares the latest version from bill_data against ddp-broker-py's
        BillVersion table (PLAN-bill-document-provenance.md Phase 4 — Redis is
        no longer the source of truth for this check, per the 2026-07-26
        decision to drop it from this job entirely, not just add a fallback).
        If newer, downloads the bill text and ingests it into Pinecone.

        Redis is still written to at the end (unchanged) purely to keep
        VoteBot's webflow_id -> slug lookup cache fresh (services/button_cache.py
        reconciliation) — a separate concern from version tracking that this
        redesign doesn't touch.

        Args:
            webflow_id: Webflow item ID
            bill_title: Human-readable bill title
            jurisdiction_code: Two-letter state code
            bill_data: Full bill data dict from OpenStates API
            bill_slug: Webflow slug for DDP linking
            fields: Current CMS field data from Webflow

        Returns:
            Dict with keys: is_newer, chunks_created, version_note,
            version_date, text_url, error
        """
        from ddp_sync.services.broker_client import (
            BrokerClientError,
            get_latest_bill_version,
            write_bill_version,
        )
        from ddp_sync.services.redis_store import get_redis_store

        result = {
            "is_newer": False,
            "chunks_created": 0,
            "history_chunks_created": 0,
            "changelog_chunks_created": 0,
            "changelog_skipped": False,
            "changelog_skip_reason": "",
            "surplus_chunks_deleted": 0,
            "version_note": "",
            "version_date": "",
            "text_url": "",
            "error": None,
        }

        versions = bill_data.get("versions", [])
        latest_version = self._get_latest_version(versions)
        if not latest_version:
            return result

        result["version_note"] = latest_version.get("note", "")
        result["version_date"] = latest_version.get("date", "")

        url_info = self._get_best_text_url(latest_version)
        if not url_info:
            return result

        text_url, media_type = url_info
        result["text_url"] = text_url

        # Compare against ddp-broker-py's BillVersion (Phase 4) instead of
        # Redis. bill_openstates_id comes straight from the OpenStates
        # response already in hand (bill_data) -- no extra fetch needed.
        bill_openstates_id = self._extract_bill_openstates_id(bill_data)
        cached: dict | None = None
        if bill_openstates_id:
            try:
                cached = await get_latest_bill_version(bill_openstates_id)
                if cached is None and len(versions) > 1:
                    # SYNC-26: this is the first time ddp-sync has ever seen
                    # this bill, but it already has more than one version --
                    # a fast-moving bill can outrun this job's poll cadence
                    # and arrive already several stages ahead. Without this,
                    # only `latest_version` (written at the end of this
                    # function) ever gets a ledger row -- every earlier
                    # version silently never does, which later breaks
                    # bill_changelog's compare_version FK resolution even
                    # though api-v3 itself has every version archived
                    # correctly (SYNC-26's own root-cause finding).
                    await self._backfill_missing_versions(
                        bill_openstates_id=bill_openstates_id,
                        jurisdiction_code=jurisdiction_code,
                        session_code=fields.get("session-code", ""),
                        versions=versions,
                        latest_version=latest_version,
                    )
            except BrokerClientError as e:
                # Can't tell if this is genuinely new -- skip this bill this
                # run rather than risk re-processing (and re-billing OpenAI/
                # Pinecone) on a false "new" during a ddp-broker-py outage.
                # The next scheduled run will catch it once the read works.
                logger.error(
                    "Failed to read BillVersion from ddp-broker-py -- "
                    "skipping this bill's version check this run",
                    webflow_id=webflow_id,
                    bill_title=bill_title,
                    error=str(e),
                )
                await self._refresh_slug_cache(webflow_id, bill_slug, latest_version)
                return result
        else:
            logger.warning(
                "OpenStates bill_data has no 'id' -- cannot check BillVersion, "
                "treating as never-seen",
                webflow_id=webflow_id,
                bill_title=bill_title,
            )

        if not self._is_newer_version(latest_version, cached):
            # Keep VoteBot's webflow_id -> slug cache fresh regardless of
            # whether the version changed -- separate concern from the
            # version-tracking check above (see docstring).
            await self._refresh_slug_cache(webflow_id, bill_slug, latest_version)
            return result

        # Newer version detected
        result["is_newer"] = True
        logger.info(
            "New bill version detected",
            bill_title=bill_title,
            webflow_id=webflow_id,
            version_note=result["version_note"],
            version_date=result["version_date"],
            text_url=text_url,
            media_type=media_type,
        )

        try:
            chunks_created, extracted_content = await self._ingest_bill_text(
                webflow_id=webflow_id,
                bill_title=bill_title,
                bill_slug=bill_slug,
                text_url=text_url,
                media_type=media_type,
                fields=fields,
            )
            result["chunks_created"] = chunks_created

            if chunks_created > 0:
                # Delete surplus chunks from the previous version (upsert already live).
                old_chunk_count = cached.get("chunk_count") if cached else None
                surplus_deleted = await self._delete_surplus_chunks(
                    document_id=f"bill-pdf-{webflow_id}",
                    old_chunk_count=old_chunk_count,
                    new_chunk_count=chunks_created,
                )
                result["surplus_chunks_deleted"] = surplus_deleted

                # Store permanent history record for this version.
                history_chunks = await self._ingest_bill_history(
                    webflow_id=webflow_id,
                    bill_title=bill_title,
                    bill_slug=bill_slug,
                    text_url=text_url,
                    media_type=media_type,
                    version_date=result["version_date"],
                    version_note=result["version_note"],
                    jurisdiction=fields.get("jurisdiction", ""),
                    content=extracted_content,
                )
                result["history_chunks_created"] = history_chunks

                # Generate changelog if a previous version exists.
                if cached:
                    cl_chunks, cl_skipped, cl_reason = await self._generate_and_ingest_changelog(
                        webflow_id=webflow_id,
                        bill_title=bill_title,
                        bill_slug=bill_slug,
                        jurisdiction=fields.get("jurisdiction", ""),
                        old_version=cached,
                        new_version_date=result["version_date"],
                        new_version_note=result["version_note"],
                        new_content=extracted_content,
                        bill_openstates_id=bill_openstates_id,
                    )
                    result["changelog_chunks_created"] = cl_chunks
                    result["changelog_skipped"] = cl_skipped
                    result["changelog_skip_reason"] = cl_reason
                    if cl_skipped:
                        logger.warning(
                            "Changelog generation skipped",
                            webflow_id=webflow_id,
                            bill_title=bill_title,
                            reason=cl_reason,
                        )

        except Exception as e:
            result["error"] = str(e)
            logger.error(
                "Failed to ingest bill text",
                webflow_id=webflow_id,
                bill_title=bill_title,
                error=str(e),
            )

        # Record this version in ddp-broker-py's BillVersion (Phase 4) --
        # regardless of ingestion success, matching the old Redis behavior:
        # a bill whose ingest failed still gets marked "seen" so a permanently
        # broken document doesn't get re-fetched and re-failed forever. This
        # is the durable "have we seen this" ledger now; Redis no longer is.
        if bill_openstates_id:
            try:
                await write_bill_version(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction_code,
                    session_code=fields.get("session-code", ""),
                    version_date=result["version_date"],
                    version_note=result["version_note"],
                    text_url=text_url,
                    media_type=media_type,
                    chunk_count=result["chunks_created"],
                    pinecone_ingested=result["chunks_created"] > 0,
                )
            except BrokerClientError as e:
                # Not recorded -> this version may look "new" again next run
                # and get re-processed. Logged loudly rather than silently
                # swallowed, but doesn't fail this run -- the actual content
                # work above already succeeded or already recorded its own
                # error in result["error"].
                logger.error(
                    "Failed to record BillVersion in ddp-broker-py -- this "
                    "version may be re-processed next run",
                    webflow_id=webflow_id,
                    bill_title=bill_title,
                    error=str(e),
                )
        else:
            logger.warning(
                "Cannot record BillVersion -- OpenStates bill_data had no 'id'",
                webflow_id=webflow_id,
                bill_title=bill_title,
            )

        # Keep VoteBot's webflow_id -> slug cache fresh -- separate concern
        # from the version-tracking write above (see docstring).
        await self._refresh_slug_cache(webflow_id, bill_slug, latest_version)

        # Publish cache invalidation for VoteBot's button cache, but only if
        # ingestion actually produced new chunks. Without that guard, a no-op
        # version-check run would invalidate cached responses unnecessarily.
        # Subscribers (votebot main.py) call ButtonCache.invalidate_bill(slug)
        # so subsequent button taps regenerate from fresh Pinecone content.
        # See plans/PLAN-quick-action-buttons.md (Phase 5 / Fix invalidation).
        if result.get("chunks_created", 0) > 0 and bill_slug:
            try:
                redis_store = get_redis_store()
                payload = json.dumps({
                    "slug": bill_slug,
                    "reason": "bill_version_change",
                    "version_note": latest_version.get("note", ""),
                })
                subscribers = await redis_store.publish(
                    "votebot:cache:invalidate",
                    payload,
                )
                logger.info(
                    "Published button cache invalidation",
                    slug=bill_slug,
                    version_note=latest_version.get("note", ""),
                    subscribers=subscribers,
                )
            except Exception as e:
                logger.warning(
                    "Failed to publish cache invalidation",
                    slug=bill_slug,
                    error=str(e),
                )

        return result

    @staticmethod
    async def _backfill_missing_versions(
        *,
        bill_openstates_id: str,
        jurisdiction_code: str,
        session_code: str,
        versions: list[dict],
        latest_version: dict,
    ) -> int:
        """SYNC-26: record a ledger-only BillVersion row for every version
        in `versions` other than `latest_version` (which the caller already
        writes in full, with real text_url/chunk_count/pinecone_ingested,
        once ingestion finishes).

        Ledger-only on purpose -- chunk_count=0, pinecone_ingested=False:
        this does not download or ingest the older version's text into
        Pinecone, it only ensures write_bill_version's natural key
        (bill + version_date + version_note) exists so ddp-broker-py's
        BillArtifact write can resolve compare_version against it. Safe to
        call repeatedly -- write_bill_version is idempotent, so a version
        already recorded here (or previously) is just a no-op create=False.

        Never raises: best-effort, one bad version shouldn't block the
        caller's own handling of `latest_version` -- logs and continues.

        Returns the number of versions actually newly created (matches
        write_bill_version's own create=True/False, not just attempted).
        """
        from ddp_sync.services.broker_client import write_bill_version

        # Compared by natural key (date, note) -- the same key
        # write_bill_version itself upserts on -- rather than object
        # identity. _get_latest_version's sorted()/[0] happens to return the
        # same object reference today, but that's an implementation detail
        # of a different function this one shouldn't have to assume holds.
        latest_key = (latest_version.get("date", ""), latest_version.get("note", ""))

        backfilled = 0
        for version in versions:
            if (version.get("date", ""), version.get("note", "")) == latest_key:
                continue
            version_note = version.get("note", "")
            if not version_note:
                continue
            try:
                url_info = BillVersionSyncService._get_best_text_url(version)
                text_url, media_type = url_info if url_info else ("", "")
                write_result = await write_bill_version(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction_code,
                    session_code=session_code,
                    version_date=version.get("date", ""),
                    version_note=version_note,
                    text_url=text_url,
                    media_type=media_type,
                    # Explicit, not relied-on-as-defaults -- self-documents
                    # the ledger-only contract this method's own docstring
                    # describes, rather than depending on write_bill_version's
                    # signature never changing its own defaults.
                    chunk_count=0,
                    pinecone_ingested=False,
                )
                if write_result.get("created"):
                    backfilled += 1
            except Exception as e:
                # Broad on purpose, matching this method's own "never raises"
                # contract -- an unexpected error here (not just a
                # BrokerClientError) must not propagate up through
                # check_and_reingest_version's own BrokerClientError-only
                # except and block the latest version's own handling below.
                logger.warning(
                    "SYNC-26: failed to backfill an older BillVersion -- "
                    "changelog generation may still fail for this version pair",
                    bill_openstates_id=bill_openstates_id,
                    version_date=version.get("date", ""),
                    version_note=version_note,
                    error=str(e),
                )

        logger.info(
            "SYNC-26: first-sighting version backfill complete",
            bill_openstates_id=bill_openstates_id,
            versions_seen=len(versions),
            versions_backfilled=backfilled,
        )
        return backfilled

    @staticmethod
    async def _refresh_slug_cache(webflow_id: str, bill_slug: str, latest_version: dict) -> None:
        """Keep Redis's webflow_id -> slug mapping fresh for VoteBot's
        startup reconciliation (services/button_cache.py) -- unrelated to
        Phase 4's version-tracking redesign above, which uses ddp-broker-py's
        BillVersion instead of this same Redis key for its own "have we seen
        this" check. Never raises -- a failure here shouldn't fail the
        surrounding version check, same tolerance the old inline write had.
        """
        from ddp_sync.services.redis_store import get_redis_store

        try:
            redis_store = get_redis_store()
            await redis_store.set_bill_version(webflow_id, {
                "version_date": latest_version.get("date", ""),
                "version_note": latest_version.get("note", ""),
                "bill_slug": bill_slug or "",
            })
        except Exception as e:
            logger.warning(
                "Failed to refresh VoteBot slug cache",
                webflow_id=webflow_id,
                bill_slug=bill_slug,
                error=str(e),
            )

    async def check_and_update_bill(
        self,
        webflow_id: str,
        bill_title: str,
        jurisdiction_code: str,
        openstates_url: str,
        bill_slug: str,
        fields: dict,
    ) -> VersionCheckResult:
        """Check a single bill for version updates and re-ingest if newer.

        Combined entry point that fetches bill data from OpenStates once,
        then routes to both write paths: update_bill_status (Flow 1) and
        check_and_reingest_version (Flow 2).

        Args:
            webflow_id: Webflow item ID
            bill_title: Human-readable bill title
            jurisdiction_code: Two-letter state code
            openstates_url: OpenStates URL for the bill
            bill_slug: Webflow slug for DDP linking
            fields: Full CMS field data from Webflow

        Returns:
            VersionCheckResult with outcome details
        """
        from ddp_sync.pipelines.bill_sync import BillSyncService

        sync_service = BillSyncService(self.settings)

        # 1. Parse OpenStates URL
        parsed = sync_service.parse_openstates_url(openstates_url)
        if not parsed:
            return VersionCheckResult(
                webflow_id=webflow_id,
                bill_title=bill_title,
                jurisdiction=jurisdiction_code,
                status="error",
                error=f"Could not parse OpenStates URL: {openstates_url}",
            )

        # 2. Fetch bill from OpenStates (reuses retry/rate-limit logic)
        bill_data = await sync_service.fetch_bill_from_openstates(
            parsed.jurisdiction, parsed.session, parsed.bill_id
        )
        if not bill_data:
            return VersionCheckResult(
                webflow_id=webflow_id,
                bill_title=bill_title,
                jurisdiction=jurisdiction_code,
                status="error",
                error=f"Failed to fetch bill from OpenStates: {openstates_url}",
            )

        # 3. Flow 2: Check version and re-ingest if newer
        version_result = await self.check_and_reingest_version(
            webflow_id=webflow_id,
            bill_title=bill_title,
            jurisdiction_code=jurisdiction_code,
            bill_data=bill_data,
            bill_slug=bill_slug,
            fields=fields,
        )

        # 4. Flow 1: Update Webflow CMS status fields
        status_result = await self.update_bill_status(
            webflow_id=webflow_id,
            bill_title=bill_title,
            jurisdiction_code=jurisdiction_code,
            bill_data=bill_data,
            fields=fields,
        )

        # 5. Build combined result
        if version_result["is_newer"]:
            return VersionCheckResult(
                webflow_id=webflow_id,
                bill_title=bill_title,
                jurisdiction=jurisdiction_code,
                status="updated" if not version_result["error"] else "partial",
                version_note=version_result["version_note"],
                version_date=version_result["version_date"],
                text_url=version_result["text_url"],
                chunks_created=version_result["chunks_created"],
                history_chunks_created=version_result["history_chunks_created"],
                changelog_chunks_created=version_result["changelog_chunks_created"],
                changelog_skipped=version_result["changelog_skipped"],
                changelog_skip_reason=version_result["changelog_skip_reason"],
                surplus_chunks_deleted=version_result["surplus_chunks_deleted"],
                webflow_updated=status_result["webflow_updated"],
                status_updated=status_result["status_updated"],
                webflow_patch_skipped=status_result["patch_skipped"],
                error=f"Ingestion failed: {version_result['error']}" if version_result["error"] else None,
            )
        else:
            return VersionCheckResult(
                webflow_id=webflow_id,
                bill_title=bill_title,
                jurisdiction=jurisdiction_code,
                status="unchanged",
                version_note=version_result["version_note"],
                version_date=version_result["version_date"],
                text_url=version_result["text_url"],
                status_updated=status_result["status_updated"],
                webflow_patch_skipped=status_result["patch_skipped"],
            )

    async def _ingest_bill_text(
        self,
        webflow_id: str,
        bill_title: str,
        bill_slug: str,
        text_url: str,
        media_type: str,
        fields: dict,
    ) -> tuple[int, str]:
        """Download bill text and ingest into Pinecone.

        Routes based on media_type:
        - "application/pdf" → WebflowSource._process_bill_pdf()
        - "text/html" → WebflowSource._process_bill_html()
        - Unknown → detect via _get_url_content_type(), then route

        Returns:
            (chunks_created, extracted_content) — content passed to callers
            so history/changelog can reuse it without re-downloading.
        """
        from ddp_sync.ingestion.pipeline import IngestionPipeline
        from ddp_sync.ingestion.sources.webflow import WebflowSource

        webflow_source = WebflowSource(self.settings)
        pipeline = IngestionPipeline(self.settings)

        # Determine content type
        if "pdf" in media_type.lower():
            doc = await webflow_source._process_bill_pdf(text_url, fields, webflow_id)
        elif "html" in media_type.lower():
            doc = await webflow_source._process_bill_html(text_url, fields, webflow_id)
        else:
            # Unknown media type — detect
            detected = await webflow_source._get_url_content_type(text_url)
            if detected == "pdf":
                doc = await webflow_source._process_bill_pdf(text_url, fields, webflow_id)
            elif detected == "html":
                doc = await webflow_source._process_bill_html(text_url, fields, webflow_id)
            else:
                logger.warning(
                    "Cannot determine content type for bill text URL",
                    url=text_url,
                    media_type=media_type,
                    detected=detected,
                )
                return 0, ""

        if not doc:
            logger.warning(
                "No document produced from bill text extraction",
                webflow_id=webflow_id,
                url=text_url,
            )
            return 0, ""

        # Ingest with skip_duplicates=False to force overwrite
        result = await pipeline.ingest_document(
            content=doc.content,
            metadata=doc.metadata,
            skip_duplicates=False,
        )

        logger.info(
            "Bill text re-ingested",
            webflow_id=webflow_id,
            bill_title=bill_title,
            chunks_created=result.chunks_created,
            chunks_upserted=result.chunks_upserted,
        )

        return result.chunks_created, doc.content

    async def _delete_surplus_chunks(
        self,
        document_id: str,
        old_chunk_count: int | None,
        new_chunk_count: int,
    ) -> int:
        """Delete chunk IDs from new_chunk_count up to old_chunk_count - 1.

        Uses exact ID deletion (no metadata scan). Guards against a missing or
        implausibly large cached value to avoid wiping valid new chunks.

        Returns number of chunks deleted (0 if no-op).
        """
        if not old_chunk_count:
            return 0
        if old_chunk_count > new_chunk_count * 4:
            logger.warning(
                "Skipping surplus chunk deletion — old_chunk_count implausibly large",
                document_id=document_id,
                old_chunk_count=old_chunk_count,
                new_chunk_count=new_chunk_count,
            )
            return 0
        if old_chunk_count <= new_chunk_count:
            return 0

        from ddp_sync.services.vector_store import VectorStoreService

        vector_store = VectorStoreService(self.settings)
        ids_to_delete = [
            f"{document_id}-chunk-{i}"
            for i in range(new_chunk_count, old_chunk_count)
        ]
        await vector_store.delete(ids=ids_to_delete)
        logger.info(
            "Surplus bill chunks deleted",
            document_id=document_id,
            deleted_count=len(ids_to_delete),
            old_chunk_count=old_chunk_count,
            new_chunk_count=new_chunk_count,
        )
        return len(ids_to_delete)

    async def _ingest_bill_history(
        self,
        webflow_id: str,
        bill_title: str,
        bill_slug: str,
        text_url: str,
        media_type: str,
        version_date: str,
        version_note: str,
        jurisdiction: str,
        content: str,
    ) -> int:
        """Ingest bill text as a permanent versioned history record.

        Uses already-extracted content so the PDF/HTML is not re-downloaded.

        Returns number of chunks created.
        """
        from ddp_sync.ingestion.metadata import DocumentMetadata
        from ddp_sync.ingestion.pipeline import IngestionPipeline

        if not content:
            return 0

        pipeline = IngestionPipeline(self.settings)
        ddp_url = f"https://digitaldemocracyproject.org/bills/{bill_slug}" if bill_slug else None

        metadata = DocumentMetadata(
            document_id=f"bill-text-history-{webflow_id}-{version_date}",
            document_type="bill-text-history",
            source="OpenStates / Government Source",
            title=f"{bill_title} - {version_note}" if version_note else bill_title,
            jurisdiction=jurisdiction or None,
            url=ddp_url,
            extra={
                "webflow_id": webflow_id,
                "bill_slug": bill_slug or "",
                "version_date": version_date,
                "version_note": version_note,
                "text_url": text_url,
                "media_type": media_type,
            },
        )

        result = await pipeline.ingest_document(
            content=content,
            metadata=metadata,
            skip_duplicates=False,
        )

        logger.info(
            "Bill version history stored",
            webflow_id=webflow_id,
            version_date=version_date,
            version_note=version_note,
            chunks_created=result.chunks_created,
        )
        return result.chunks_created

    async def _generate_and_ingest_changelog(
        self,
        webflow_id: str,
        bill_title: str,
        bill_slug: str,
        jurisdiction: str,
        old_version: dict,
        new_version_date: str,
        new_version_note: str,
        new_content: str,
        bill_openstates_id: str | None = None,
    ) -> tuple[int, bool, str]:
        """Generate a changelog comparing old and new bill versions via LegBot.

        Prefers ddp-open-states' already-archived changelog inputs (diff_from_previous_version
        + the prior version's own raw_text, both precomputed at scrape time by
        archive_bill_versions() and surfaced by api-v3 -- ddp-infra's bill_changelog diff-
        endpoint fix, 2026-07-30) over re-downloading old_version["text_url"] and re-deriving
        the diff locally. Falls back to that live re-fetch-and-diff exactly as before when
        bill_openstates_id is unavailable or nothing is archived for this bill yet. Fails
        gracefully on any error either way — the surrounding ingest is never blocked.

        Returns:
            (chunks_created, skipped, skip_reason)
            skipped=True means no changelog was produced; ingest still succeeded.
        """
        import httpx

        from ddp_sync.ingestion.metadata import DocumentMetadata
        from ddp_sync.ingestion.pipeline import IngestionPipeline
        from ddp_sync.ingestion.sources.webflow import WebflowSource
        from ddp_sync.services.local_openstates_client import get_archived_changelog_inputs

        old_url = old_version.get("text_url", "")
        old_media_type = old_version.get("media_type", "")
        old_version_date = old_version.get("version_date", "")
        old_version_note = old_version.get("version_note", "")

        old_bill_source: str | None = None
        diff_text: str | None = None

        if bill_openstates_id:
            archived = await get_archived_changelog_inputs(bill_openstates_id)
            if archived:
                old_bill_source = archived["old_bill_source"]
                diff_text = archived["diff_source"]
                logger.info(
                    "Using ddp-open-states' archived changelog inputs -- skipping live "
                    "refetch and local diff",
                    bill_openstates_id=bill_openstates_id,
                )

        if diff_text is None:
            if not old_url:
                return 0, True, "no_old_url"

            # Re-download and extract old bill text.
            try:
                webflow_source = WebflowSource(self.settings)
                fields: dict = {}  # Old URL only — no CMS fields needed for extraction
                if "pdf" in old_media_type.lower():
                    old_doc = await webflow_source._process_bill_pdf(old_url, fields, webflow_id)
                elif "html" in old_media_type.lower():
                    old_doc = await webflow_source._process_bill_html(old_url, fields, webflow_id)
                else:
                    detected = await webflow_source._get_url_content_type(old_url)
                    if detected == "pdf":
                        old_doc = await webflow_source._process_bill_pdf(old_url, fields, webflow_id)
                    elif detected == "html":
                        old_doc = await webflow_source._process_bill_html(old_url, fields, webflow_id)
                    else:
                        return 0, True, "old_url_unknown_content_type"

                if not old_doc or not old_doc.content:
                    return 0, True, "old_url_no_content"

                old_content = old_doc.content

            except (httpx.HTTPError, httpx.TimeoutException) as e:
                return 0, True, f"old_url_fetch_failed: {type(e).__name__}"
            except Exception as e:
                return 0, True, f"old_text_extraction_failed: {type(e).__name__}"

            if len(old_content) < 500:
                return 0, True, "old_content_too_short"

            # Generate structured changelog via LegBot (ddp-infra Phase 4 /
            # PLAN-legbot.md's bill_changelog capability) -- replaces the old
            # direct gpt-4o-mini call. ddp-sync computes the diff (LegBot is
            # handed a precomputed diff, not two full documents to re-diff
            # itself). old_bill_source is the already-fetched/extracted text,
            # not old_url -- LegBot no longer fetches URLs itself (PLAN §24,
            # 2026-07-30), so handing it a URL here would just skip with
            # old_bill_source_is_unresolved_url.
            old_bill_source = old_content
            diff_text = "\n".join(
                difflib.unified_diff(
                    old_content.splitlines(),
                    new_content.splitlines(),
                    fromfile=old_version_note or "previous version",
                    tofile=new_version_note or "this version",
                    lineterm="",
                )
            )
            if not diff_text.strip():
                return 0, True, "no_diff_produced"

        from ddp_sync.services.legbot_client import (
            LegBotDispatchError,
            dispatch_bill_changelog,
        )

        try:
            dispatch_result = await dispatch_bill_changelog(
                old_bill_source=old_bill_source,
                diff_source=diff_text,
            )
        except LegBotDispatchError as e:
            return 0, True, f"legbot_dispatch_failed: {type(e).__name__}"

        answer = dispatch_result["answer"]
        if answer.get("insufficient_information"):
            # Covers both handle_ingest's four skip reasons (no prior
            # version archived, diff unavailable, unsupported diff format,
            # malformed diff) and handle_analyze's own graceful skip (bill
            # too short/vague to answer confidently) -- same "answer" shape
            # either way, per config/legbot_questions.yaml.
            return 0, True, answer.get("reason", "legbot_insufficient_information")

        def _bullet_list(items: list) -> str:
            return "\n".join(f"- {item}" for item in items) if items else "- None"

        changelog_text = f"""## What Changed: {bill_title}
**From:** {old_version_note} ({old_version_date})
**To:** {new_version_note} ({new_version_date})

### Sections Added
{_bullet_list(answer.get("sections_added") or [])}

### Sections Removed
{_bullet_list(answer.get("sections_removed") or [])}

### Sections Modified
{_bullet_list(answer.get("sections_modified") or [])}

### Key Policy Implications
{answer.get("policy_implications") or "None noted."}"""

        if not changelog_text.strip():
            return 0, True, "legbot_empty_response"

        # Ingest changelog as a permanent document.
        pipeline = IngestionPipeline(self.settings)
        ddp_url = f"https://digitaldemocracyproject.org/bills/{bill_slug}" if bill_slug else None

        metadata = DocumentMetadata(
            document_id=f"bill-changelog-{webflow_id}-{new_version_date}",
            document_type="bill-changelog",
            source="Digital Democracy Project",
            title=f"{bill_title} — Changelog ({old_version_note} → {new_version_note})",
            jurisdiction=jurisdiction or None,
            url=ddp_url,
            extra={
                "webflow_id": webflow_id,
                "bill_slug": bill_slug or "",
                "version_from_date": old_version_date,
                "version_from_note": old_version_note,
                "version_to_date": new_version_date,
                "version_to_note": new_version_note,
            },
        )

        result = await pipeline.ingest_document(
            content=changelog_text,
            metadata=metadata,
            skip_duplicates=False,
        )

        logger.info(
            "Bill changelog generated and stored",
            webflow_id=webflow_id,
            version_from=old_version_note,
            version_to=new_version_note,
            chunks_created=result.chunks_created,
        )
        return result.chunks_created, False, ""

    async def sync_bill_versions(
        self,
        bills: list[dict[str, Any]],
        heartbeat_callback: Any | None = None,
    ) -> VersionSyncBatchResult:
        """Batch entry point: check all current-session bills for version updates.

        Filters to current-session bills, applies rate limiting between
        OpenStates API calls. Optionally caps re-ingestions via max_updates_per_run
        config (0 = unlimited).

        Args:
            bills: List of bill dicts from Webflow CMS (raw items with fieldData)

        Returns:
            VersionSyncBatchResult with aggregate stats
        """
        from ddp_sync.services.redis_store import get_redis_store
        from ddp_sync.pipelines.bill_sync import BillSyncService

        sync_service = BillSyncService(self.settings)
        max_updates = self._config.get("max_updates_per_run", 0)
        skip_webflow = self._config.get("skip_webflow_update", False)

        logger.info(
            "Starting bill version sync batch",
            total_bills=len(bills),
            max_updates_per_run=max_updates,
            skip_webflow_update=skip_webflow,
        )

        result = VersionSyncBatchResult(total_bills=len(bills))

        # Warm legislative calendar with live OpenStates session data
        jurisdiction_codes = set()
        for bill in bills:
            fields = bill.get("fieldData", {})
            jurisdiction_id = fields.get("jurisdiction", "")
            openstates_url = fields.get("open-states-url-2", "")
            code = sync_service.resolve_jurisdiction_code(jurisdiction_id, openstates_url)
            if code:
                jurisdiction_codes.add(code)

        jurisdiction_data = {}
        for code in jurisdiction_codes:
            try:
                info = await sync_service.get_jurisdiction_info(code)
                if info:
                    jurisdiction_data[code] = info
            except Exception as e:
                logger.warning("Failed to fetch jurisdiction for calendar warm", state=code, error=str(e))

        if jurisdiction_data:
            sync_service.calendar.warm_cache(jurisdiction_data)

        # Track active jurisdictions in Redis
        if jurisdiction_codes:
            redis_store = get_redis_store()
            for code in jurisdiction_codes:
                await redis_store.add_active_jurisdiction(code)

        updates_this_run = 0

        for bill in bills:
            fields = bill.get("fieldData", {})
            webflow_id = bill.get("id", "")
            title = fields.get("name", "Unknown")
            openstates_url = fields.get("open-states-url-2", "")
            session_year = str(fields.get("bill-session", ""))
            session_code = fields.get("session-code", "")
            jurisdiction_id = fields.get("jurisdiction", "")
            slug = fields.get("slug", "")

            jurisdiction_code = sync_service.resolve_jurisdiction_code(
                jurisdiction_id, openstates_url
            )

            # Skip bills without OpenStates URL
            if not openstates_url:
                result.skipped += 1
                result.skipped_no_url += 1
                logger.info(
                    "Skipping bill (no OpenStates URL)",
                    bill=title,
                    webflow_id=webflow_id,
                    slug=slug,
                    cms_status=fields.get("status", ""),
                )
                continue

            # Check if current session (async version checks OpenStates API
            # for non-standard session identifiers like AZ "57th-2nd-regular")
            if not await sync_service.is_current_session_async(session_year, session_code, jurisdiction_code):
                result.skipped += 1
                result.skipped_not_current += 1
                logger.info(
                    "Skipping bill (not current session)",
                    bill=title,
                    webflow_id=webflow_id,
                    jurisdiction=jurisdiction_code,
                    session_year=session_year,
                    session_code=session_code,
                )
                continue

            # Check if we should sync this jurisdiction today
            if not sync_service.should_sync_jurisdiction(jurisdiction_code):
                result.skipped += 1
                result.skipped_jurisdiction += 1
                logger.info(
                    "Skipping bill (jurisdiction not scheduled today)",
                    bill=title,
                    jurisdiction=jurisdiction_code,
                )
                continue

            # Respect max_updates_per_run
            if max_updates > 0 and updates_this_run >= max_updates:
                result.skipped += 1
                logger.info("Skipping bill (max updates reached)", bill=title, max_updates=max_updates)
                continue

            # Apply rate limiting
            await sync_service._apply_rate_limit()

            # Check and update
            try:
                check_result = await self.check_and_update_bill(
                    webflow_id=webflow_id,
                    bill_title=title,
                    jurisdiction_code=jurisdiction_code,
                    openstates_url=openstates_url,
                    bill_slug=slug,
                    fields=fields,
                )

                result.checked += 1

                if check_result.status == "updated":
                    result.updated += 1
                    result.chunks_created += check_result.chunks_created
                    result.history_chunks_created += check_result.history_chunks_created
                    result.changelog_chunks_created += check_result.changelog_chunks_created
                    if check_result.changelog_skipped:
                        result.changelogs_skipped += 1
                    result.surplus_chunks_deleted += check_result.surplus_chunks_deleted
                    if check_result.webflow_updated:
                        result.webflow_updates += 1
                    if check_result.status_updated:
                        result.status_updates += 1
                    if check_result.webflow_patch_skipped:
                        result.webflow_skipped += 1
                    if not check_result.webflow_updated and not check_result.webflow_patch_skipped:
                        result.webflow_patch_failures += 1
                    updates_this_run += 1
                    logger.info(
                        "Bill version updated",
                        bill=title,
                        webflow_id=webflow_id,
                        version=check_result.version_note,
                        date=check_result.version_date,
                        chunks=check_result.chunks_created,
                        history_chunks=check_result.history_chunks_created,
                        changelog_chunks=check_result.changelog_chunks_created,
                        changelog_skipped=check_result.changelog_skipped,
                        changelog_skip_reason=check_result.changelog_skip_reason,
                        surplus_deleted=check_result.surplus_chunks_deleted,
                        webflow_updated=check_result.webflow_updated,
                        status_updated=check_result.status_updated,
                    )
                elif check_result.status == "unchanged":
                    result.unchanged += 1
                    if check_result.status_updated:
                        result.status_updates += 1
                    if check_result.webflow_patch_skipped:
                        result.webflow_skipped += 1
                    # Detect silent failures: not updated AND not skipped = PATCH failed or no action
                    if not check_result.status_updated and not check_result.webflow_patch_skipped:
                        result.webflow_patch_failures += 1
                    logger.info(
                        "Bill version unchanged",
                        bill=title,
                        webflow_id=webflow_id,
                        version=check_result.version_note,
                        status_updated=check_result.status_updated,
                        patch_skipped=check_result.webflow_patch_skipped,
                    )
                elif check_result.status == "partial":
                    # Ingestion failed but Webflow status was still updated
                    result.updated += 1
                    if check_result.webflow_updated:
                        result.webflow_updates += 1
                    if check_result.status_updated:
                        result.status_updates += 1
                    if check_result.error:
                        result.errors.append(f"{title}: {check_result.error}")
                    logger.warning(
                        "Bill ingestion failed but status updated",
                        bill=title,
                        webflow_id=webflow_id,
                        status_updated=check_result.status_updated,
                        error=check_result.error,
                    )
                elif check_result.status == "no_versions":
                    result.no_versions += 1
                    logger.warning(
                        "Bill has no versions in OpenStates",
                        bill=title,
                        webflow_id=webflow_id,
                    )
                elif check_result.status == "error":
                    result.failed += 1
                    if check_result.error:
                        result.errors.append(f"{title}: {check_result.error}")
                    logger.warning(
                        "Bill version check failed",
                        bill=title,
                        webflow_id=webflow_id,
                        error=check_result.error,
                    )

            except Exception as e:
                result.failed += 1
                result.errors.append(f"{title}: {e}")
                logger.error(
                    "Unexpected error checking bill version",
                    bill=title,
                    webflow_id=webflow_id,
                    error=str(e),
                )

            # Keep heartbeat alive during long-running version sync phase
            if heartbeat_callback and result.checked % 10 == 0:
                await heartbeat_callback()

            # Reclaim memory between bills (PDF objects, embedding vectors, etc.)
            gc.collect()

        logger.info(
            "Bill version sync batch complete",
            total=result.total_bills,
            checked=result.checked,
            updated=result.updated,
            unchanged=result.unchanged,
            no_versions=result.no_versions,
            skipped=result.skipped,
            skipped_no_url=result.skipped_no_url,
            skipped_not_current=result.skipped_not_current,
            skipped_jurisdiction=result.skipped_jurisdiction,
            failed=result.failed,
            chunks_created=result.chunks_created,
            history_chunks_created=result.history_chunks_created,
            changelog_chunks_created=result.changelog_chunks_created,
            changelogs_skipped=result.changelogs_skipped,
            surplus_chunks_deleted=result.surplus_chunks_deleted,
            webflow_updates=result.webflow_updates,
            status_updates=result.status_updates,
            webflow_skipped=result.webflow_skipped,
            webflow_patch_failures=result.webflow_patch_failures,
            no_latest_action=result.no_latest_action,
            errors=result.errors[:10] if result.errors else [],
        )

        return result

    async def sync_bill_statuses(
        self,
        bills: list[dict[str, Any]],
        all_sessions: bool = False,
        jurisdiction: str | None = None,
        heartbeat_callback: Any | None = None,
    ) -> VersionSyncBatchResult:
        """Flow 1 standalone: sync OpenStates → Webflow CMS status fields only.

        Lightweight alternative to sync_bill_versions — fetches from OpenStates
        with ?include=actions only (no versions, votes, documents), then calls
        update_bill_status() for each bill. No Pinecone interaction.

        Args:
            bills: List of bill dicts from Webflow CMS (raw items with fieldData)
            all_sessions: If True, skip session/jurisdiction filters (backfill mode)
            jurisdiction: Optional jurisdiction filter (e.g. "FL")
            heartbeat_callback: Optional async callback for zombie watchdog

        Returns:
            VersionSyncBatchResult with aggregate stats
        """
        from ddp_sync.pipelines.bill_sync import BillSyncService

        sync_service = BillSyncService(self.settings)

        logger.info(
            "Starting bill status sync (Flow 1 only)",
            total_bills=len(bills),
            all_sessions=all_sessions,
            jurisdiction=jurisdiction,
        )

        result = VersionSyncBatchResult(total_bills=len(bills))

        # Warm legislative calendar (only needed if filtering by session)
        if not all_sessions:
            jurisdiction_codes = set()
            for bill in bills:
                fields = bill.get("fieldData", {})
                jurisdiction_id = fields.get("jurisdiction", "")
                openstates_url = fields.get("open-states-url-2", "")
                code = sync_service.resolve_jurisdiction_code(jurisdiction_id, openstates_url)
                if code:
                    jurisdiction_codes.add(code)

            jurisdiction_data = {}
            for code in jurisdiction_codes:
                try:
                    info = await sync_service.get_jurisdiction_info(code)
                    if info:
                        jurisdiction_data[code] = info
                except Exception as e:
                    logger.warning("Failed to fetch jurisdiction for calendar warm", state=code, error=str(e))

            if jurisdiction_data:
                sync_service.calendar.warm_cache(jurisdiction_data)

        for bill in bills:
            fields = bill.get("fieldData", {})
            webflow_id = bill.get("id", "")
            title = fields.get("name", "Unknown")
            openstates_url = fields.get("open-states-url-2", "")
            session_year = str(fields.get("bill-session", ""))
            session_code = fields.get("session-code", "")
            jurisdiction_id = fields.get("jurisdiction", "")

            jurisdiction_code = sync_service.resolve_jurisdiction_code(
                jurisdiction_id, openstates_url
            )

            # Skip bills without OpenStates URL
            if not openstates_url:
                result.skipped += 1
                result.skipped_no_url += 1
                continue

            # Apply jurisdiction filter if specified
            if jurisdiction and jurisdiction_code.upper() != jurisdiction.upper():
                result.skipped += 1
                result.skipped_jurisdiction += 1
                continue

            # Apply session filter unless all_sessions=True
            if not all_sessions:
                if not await sync_service.is_current_session_async(session_year, session_code, jurisdiction_code):
                    result.skipped += 1
                    result.skipped_not_current += 1
                    continue

                if not sync_service.should_sync_jurisdiction(jurisdiction_code):
                    result.skipped += 1
                    result.skipped_jurisdiction += 1
                    continue

            # Rate limiting
            await sync_service._apply_rate_limit()

            try:
                # Parse URL and fetch from OpenStates with lightweight query
                parsed = sync_service.parse_openstates_url(openstates_url)
                if not parsed:
                    result.failed += 1
                    result.errors.append(f"{title}: Could not parse OpenStates URL")
                    continue

                # Fetch with actions only (lighter than full 10-include)
                bill_data = await sync_service.fetch_bill_from_openstates(
                    parsed.jurisdiction, parsed.session, parsed.bill_id
                )
                if not bill_data:
                    result.failed += 1
                    result.errors.append(f"{title}: Failed to fetch from OpenStates")
                    continue

                result.checked += 1

                # Flow 1 write path
                status_result = await self.update_bill_status(
                    webflow_id=webflow_id,
                    bill_title=title,
                    jurisdiction_code=jurisdiction_code,
                    bill_data=bill_data,
                    fields=fields,
                )

                if status_result["status_updated"]:
                    result.status_updates += 1
                if status_result["webflow_updated"]:
                    result.webflow_updates += 1
                if status_result["patch_skipped"]:
                    result.webflow_skipped += 1
                if not status_result["webflow_updated"] and not status_result["patch_skipped"]:
                    if status_result["latest_action"]:
                        result.webflow_patch_failures += 1
                    else:
                        result.no_latest_action += 1

                result.unchanged += 1  # No version check, so always "unchanged"

            except Exception as e:
                result.failed += 1
                result.errors.append(f"{title}: {e}")
                logger.error(
                    "Error syncing bill status",
                    bill=title,
                    webflow_id=webflow_id,
                    error=str(e),
                )

            if heartbeat_callback and result.checked % 10 == 0:
                await heartbeat_callback()

        logger.info(
            "Bill status sync complete (Flow 1 only)",
            total=result.total_bills,
            checked=result.checked,
            skipped=result.skipped,
            status_updates=result.status_updates,
            webflow_updates=result.webflow_updates,
            webflow_skipped=result.webflow_skipped,
            webflow_patch_failures=result.webflow_patch_failures,
            failed=result.failed,
        )

        return result
