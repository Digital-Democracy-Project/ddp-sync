"""Scheduler for OpenStates bill sync and data pipeline jobs."""

import asyncio
from datetime import datetime, time
from pathlib import Path
from typing import Any, Callable

import structlog
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ddp_sync.config import Settings, get_settings
from ddp_sync.services.legislative_calendar import StateLegislativeCalendar

logger = structlog.get_logger()

# Default config path — try package-relative first (editable install),
# then CWD-relative (non-editable install / systemd WorkingDirectory)
_pkg_config = Path(__file__).parent.parent.parent / "config" / "sync_schedule.yaml"
_cwd_config = Path.cwd() / "config" / "sync_schedule.yaml"
DEFAULT_CONFIG_PATH = _pkg_config if _pkg_config.exists() else _cwd_config


class UpdateScheduler:
    """
    Scheduler for periodic content updates.

    Handles:
    - Daily OpenStates bill sync (based on legislative calendar)
    - Legislator and organization syncs
    - Voatz/Brevo user syncs
    - Webflow CMS batch jobs
    - Graceful shutdown
    """

    def __init__(
        self,
        settings: Settings | None = None,
        config_path: Path | None = None,
    ):
        """
        Initialize the update scheduler.

        Args:
            settings: Application settings
            config_path: Path to sync_schedule.yaml config file
        """
        self.settings = settings or get_settings()
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        self.scheduler = AsyncIOScheduler()
        self.calendar = StateLegislativeCalendar()
        self._is_running = False
        self._update_callbacks: list[Callable] = []
        self._sync_config = self._load_sync_config()

    def _load_sync_config(self) -> dict[str, Any]:
        """Load sync schedule configuration from YAML file."""
        if not self.config_path.exists():
            logger.warning(f"Sync config not found at {self.config_path}, using defaults")
            return {
                "sync_time_utc": "04:00",
                "US": {
                    "enabled": True,
                    "frequency": "daily",
                    "congress_number": 119,
                },
            }

        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded sync config from {self.config_path}")
                return config or {}
        except Exception as e:
            logger.error(f"Failed to load sync config: {e}")
            return {}

    def start(self) -> None:
        """Start the scheduler."""
        if self._is_running:
            logger.warning("Scheduler already running")
            return

        # Parse sync time — prefer new bill_sync block, fall back to top-level
        bill_sync_config = self._sync_config.get("bill_sync", {})
        sync_time_str = bill_sync_config.get("sync_time_utc") or self._sync_config.get("sync_time_utc", "04:00")
        hour, minute = map(int, sync_time_str.split(":"))

        # Add daily bill sync job (shared fetch, independent write paths)
        self.scheduler.add_job(
            self._run_daily_bill_sync,
            trigger=CronTrigger(hour=hour, minute=minute),
            id="daily_bill_sync",
            name="Daily Bill Sync",
            replace_existing=True,
        )

        # Add legislator sync job (daily or weekly based on config)
        # Prefer new legislator_sync config, fall back to legacy legislator_bills
        leg_config = self._sync_config.get("legislator_sync", {})
        if not leg_config:
            leg_config = self._sync_config.get("legislator_bills", {})

        if leg_config.get("enabled", False):
            leg_sync_time = leg_config.get("sync_time_utc", "06:00")
            leg_hour, leg_minute = map(int, leg_sync_time.split(":"))
            frequency = leg_config.get("frequency", "weekly")

            # Map day name to cron day_of_week (0=Monday, 6=Sunday)
            day_map = {
                "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
                "friday": 4, "saturday": 5, "sunday": 6
            }

            if frequency == "daily":
                self.scheduler.add_job(
                    self._run_legislator_bills_sync,
                    trigger=CronTrigger(hour=leg_hour, minute=leg_minute),
                    id="daily_legislator_sync",
                    name="Daily Legislator Sync",
                    replace_existing=True,
                )
                logger.info(
                    "Legislator sync scheduled (daily)",
                    sync_time=leg_sync_time,
                )
            else:
                sync_day = leg_config.get("sync_day", "sunday")
                day_of_week = day_map.get(sync_day.lower(), 6)
                self.scheduler.add_job(
                    self._run_legislator_bills_sync,
                    trigger=CronTrigger(
                        day_of_week=day_of_week,
                        hour=leg_hour,
                        minute=leg_minute,
                    ),
                    id="weekly_legislator_sync",
                    name="Weekly Legislator Sync",
                    replace_existing=True,
                )
                logger.info(
                    "Legislator sync scheduled (weekly)",
                    sync_time=leg_sync_time,
                    sync_day=sync_day,
                )

        # Add legislator bio sync job (weekly or daily based on config)
        # Independent of legislator_sync — different pipeline, different
        # source data. See plans/PLAN-legislator-bio-sync.md.
        bio_config = self._sync_config.get("legislator_bio_sync", {})

        # Round-17 fix: clean up the OTHER frequency's job id before
        # registering this run's job. Without this, toggling frequency
        # daily↔weekly and reloading config would leave the previous
        # frequency's cron registered alongside the new one.
        for stale_id in (
            "daily_legislator_bio_sync", "weekly_legislator_bio_sync",
        ):
            try:
                self.scheduler.remove_job(stale_id)
            except Exception:  # noqa: BLE001 — JobLookupError variant
                pass

        if bio_config.get("enabled", False):
            # Phase-4 startup-time scope validation. If upload_photos is
            # set true in YAML but webflow_assets_read_write_key isn't
            # configured, the orchestrator's per-run fail-fast would
            # disable photo uploads with an error logged once per run —
            # but only after the cron fires. Surfacing it at startup
            # gives the operator a clear signal hours/days before the
            # first scheduled run rather than after-the-fact.
            if bio_config.get("upload_photos", False) and not (
                self.settings.webflow_assets_read_write_key
            ):
                logger.error(
                    "legislator_bio_sync.upload_photos: true but "
                    "webflow_assets_read_write_key is NOT configured. "
                    "The scheduled run will skip photo uploads with a "
                    "per-run error. Add a Webflow API token with "
                    "assets:read + assets:write scopes to the secret "
                    "as `webflow_assets_read_write_key`, or set "
                    "upload_photos: false until the key is configured.",
                    metric="legislator_bio_sync.startup_misconfig",
                )

            bio_sync_time = bio_config.get("sync_time_utc", "07:00")
            bio_hour, bio_minute = map(int, bio_sync_time.split(":"))
            bio_frequency = bio_config.get("frequency", "weekly")

            day_map = {
                "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
                "friday": 4, "saturday": 5, "sunday": 6,
            }

            if bio_frequency == "daily":
                self.scheduler.add_job(
                    self._run_legislator_bio_sync,
                    trigger=CronTrigger(hour=bio_hour, minute=bio_minute),
                    id="daily_legislator_bio_sync",
                    name="Daily Legislator Bio Sync",
                    replace_existing=True,
                )
                logger.info(
                    "Legislator bio sync scheduled (daily)",
                    sync_time=bio_sync_time,
                )
            else:
                bio_sync_day = bio_config.get("sync_day", "sunday")
                bio_day_of_week = day_map.get(bio_sync_day.lower(), 6)
                self.scheduler.add_job(
                    self._run_legislator_bio_sync,
                    trigger=CronTrigger(
                        day_of_week=bio_day_of_week,
                        hour=bio_hour,
                        minute=bio_minute,
                    ),
                    id="weekly_legislator_bio_sync",
                    name="Weekly Legislator Bio Sync",
                    replace_existing=True,
                )
                logger.info(
                    "Legislator bio sync scheduled (weekly)",
                    sync_time=bio_sync_time,
                    sync_day=bio_sync_day,
                )

        # Add organization sync job (monthly based on config)
        org_config = self._sync_config.get("organization_sync", {})
        if org_config.get("enabled", False):
            org_sync_time = org_config.get("sync_time_utc", "08:00")
            org_hour, org_minute = map(int, org_sync_time.split(":"))
            org_day_of_month = org_config.get("day_of_month", 1)

            self.scheduler.add_job(
                self._run_organization_sync,
                trigger=CronTrigger(
                    day=org_day_of_month,
                    hour=org_hour,
                    minute=org_minute,
                ),
                id="monthly_organization_sync",
                name="Monthly Organization Sync",
                replace_existing=True,
            )
            logger.info(
                "Organization sync scheduled (monthly)",
                sync_time=org_sync_time,
                day_of_month=org_day_of_month,
            )

        # --- DDP-API jobs (Voatz/Brevo + Webflow CMS) ---
        self._register_ddp_api_jobs()

        # --- VoteBot eval cron (plan §3) ---
        self._register_votebot_eval_job()

        self.scheduler.start()
        self._is_running = True

        logger.info(
            "Update scheduler started",
            openstates_sync_time=sync_time_str,
        )

    def _register_ddp_api_jobs(self) -> None:
        """Register jobs moved from DDP-API (Voatz/Brevo sync + Webflow CMS batch)."""
        from ddp_sync.pipelines.voatz_brevo import run_sync_job, run_full_sync_job
        from ddp_sync.pipelines.webflow_batch import (
            run_webflow_fill_session_code,
            run_webflow_fill_map_url,
            run_webflow_bill_org_sync,
            run_webflow_org_about_parse,
            run_webflow_check_org_missing,
            run_webflow_find_duplicates,
        )

        # Voatz -> Brevo user sync — every N minutes (default 30)
        interval = self.settings.sync_interval_minutes
        self.scheduler.add_job(
            run_sync_job,
            trigger=IntervalTrigger(minutes=interval),
            id="voatz_user_sync",
            name="Voatz -> Brevo user sync",
            replace_existing=True,
        )
        logger.info("Voatz user sync scheduled", interval_minutes=interval)

        # Voatz -> Brevo full-attribute sync — monthly 1st at 02:00 UTC
        self.scheduler.add_job(
            run_full_sync_job,
            trigger=CronTrigger(day=1, hour=2),
            id="voatz_full_sync",
            name="Voatz -> Brevo full-attribute sync",
            replace_existing=True,
        )
        logger.info("Voatz full-attribute sync scheduled (monthly, 1st at 02:00 UTC)")

        # Webflow CMS batch jobs — weekly Monday at 03:00 UTC
        webflow_trigger = CronTrigger(day_of_week="mon", hour=3)
        webflow_jobs = [
            ("webflow_fill_session_code", "Webflow: fill session-code", run_webflow_fill_session_code),
            ("webflow_fill_map_url", "Webflow: fill map-url", run_webflow_fill_map_url),
            ("webflow_bill_org_sync", "Webflow: bill-org reference sync", run_webflow_bill_org_sync),
            ("webflow_org_about_parse", "Webflow: org about-field parse", run_webflow_org_about_parse),
            ("webflow_check_org_missing", "Webflow: check org missing fields", run_webflow_check_org_missing),
            ("webflow_find_duplicates", "Webflow: find duplicate bills", run_webflow_find_duplicates),
        ]

        for job_id, name, func in webflow_jobs:
            self.scheduler.add_job(
                func,
                trigger=webflow_trigger,
                id=job_id,
                name=name,
                replace_existing=True,
            )
        logger.info("Webflow CMS batch jobs scheduled (weekly, Mon 03:00 UTC)", count=len(webflow_jobs))

    def _register_votebot_eval_job(self) -> None:
        """Register the weekly votebot eval cron (plan §3.3).

        - Reads YAML block ``votebot_eval`` from sync_schedule.yaml.
        - Validates required keys + types via ``validate_yaml_config``.
        - Validates the votebot path (env > YAML > default; loud-fail).
        - Skips registration on any validation failure (no silent no-op).
        - Job parameters: ``max_instances=1`` + ``coalesce=True`` +
          ``misfire_grace_time=3600`` for concurrency safety.
        """
        from ddp_sync.pipelines.votebot_eval import (
            resolve_votebot_path,
            validate_votebot_path,
            validate_yaml_config,
            run_votebot_eval,
        )

        config = self._sync_config.get("votebot_eval")
        if not config:
            logger.info("votebot_eval: not configured in sync_schedule.yaml — skipping")
            return

        validated, errors = validate_yaml_config(config)
        if errors:
            for err in errors:
                logger.error("votebot_eval: YAML validation failed", error=err)
            logger.error("votebot_eval: skipping registration due to config errors")
            return

        if not validated.get("enabled", False):
            logger.info("votebot_eval: disabled in config — skipping")
            return

        votebot_path = resolve_votebot_path(validated)
        is_valid, err = validate_votebot_path(votebot_path)
        if not is_valid:
            logger.error(
                "votebot_eval: path validation failed at registration",
                error=err,
                votebot_path=votebot_path,
            )
            return

        # Build the trigger.
        sync_time_str = validated.get("sync_time_utc", "12:00")
        hour, minute = map(int, sync_time_str.split(":"))
        frequency = validated.get("frequency", "weekly")

        if frequency == "daily":
            trigger = CronTrigger(hour=hour, minute=minute)
            cadence_str = f"daily at {sync_time_str} UTC"
        else:
            day_map = {
                "monday": "mon", "tuesday": "tue", "wednesday": "wed",
                "thursday": "thu", "friday": "fri", "saturday": "sat",
                "sunday": "sun",
            }
            sync_day = validated.get("sync_day", "sunday").lower()
            trigger = CronTrigger(
                day_of_week=day_map.get(sync_day, "sun"),
                hour=hour, minute=minute,
            )
            cadence_str = f"weekly {sync_day} {sync_time_str} UTC"

        days = validated.get("days", 7)
        # Closure binds `days` + `validated` so the wrapper has everything it needs.
        async def _votebot_eval_wrapper():
            return await run_votebot_eval(
                days=days,
                settings=self.settings,
                yaml_config=validated,
                trigger="scheduled",
            )

        self.scheduler.add_job(
            _votebot_eval_wrapper,
            trigger=trigger,
            id="weekly_votebot_eval",
            name="Weekly VoteBot eval",
            replace_existing=True,
            max_instances=1,            # primary mutex is the Redis lock; this is belt+suspenders
            coalesce=True,              # if multiple fires queue up, run once
            misfire_grace_time=3600,    # 1h grace if scheduler was paused
        )
        logger.info(
            "votebot_eval: registered",
            cadence=cadence_str,
            days=days,
            votebot_path=votebot_path,
        )

    def stop(self) -> None:
        """Stop the scheduler."""
        if not self._is_running:
            return

        self.scheduler.shutdown(wait=True)
        self._is_running = False

        logger.info("Update scheduler stopped")

    def add_callback(self, callback: Callable) -> None:
        """
        Add a callback to be called after updates complete.

        Args:
            callback: Async function to call with update results
        """
        self._update_callbacks.append(callback)

    async def _fetch_webflow_bills(self) -> list[dict]:
        """Fetch all bill items from Webflow CMS via paginated API calls."""
        import httpx

        bills = []
        async with httpx.AsyncClient(timeout=60.0) as client:
            headers = {
                "Authorization": f"Bearer {self.settings.webflow_votebot_api_key}",
                "accept": "application/json",
            }

            offset = 0
            while True:
                response = await client.get(
                    f"https://api.webflow.com/v2/collections/{self.settings.webflow_bills_collection_id}/items",
                    headers=headers,
                    params={"limit": 100, "offset": offset},
                )

                if response.status_code != 200:
                    break

                data = response.json()
                items = data.get("items", [])

                if not items:
                    break

                bills.extend(items)
                offset += 100

                if len(items) < 100:
                    break

        logger.info(f"Fetched {len(bills)} bills from Webflow CMS")
        return bills

    async def _run_daily_bill_sync(self) -> dict[str, Any]:
        """Daily bill sync: shared OpenStates fetch, independent write paths.

        1. Fetches all current-session bills from Webflow CMS
        2. For each bill, fetches latest data from OpenStates (one API call)
        3. Routes the fetched data to enabled write paths:
           - Flow 1: Update Webflow CMS status fields
           - Flow 2: Check for new bill text version, re-ingest to Pinecone if needed
        Each write path has independent error handling — a Flow 2 failure
        does not prevent Flow 1 from succeeding for the same bill.

        Returns:
            Dict with sync results
        """
        from ddp_sync.pipelines.bill_version import BillVersionSyncService
        from ddp_sync.services.redis_store import get_redis_store

        start_time = datetime.utcnow()

        # Read flow config — prefer new bill_sync block, fall back to legacy
        bill_sync_config = self._sync_config.get("bill_sync", {})
        flow1_enabled = bill_sync_config.get("webflow_status", {}).get("enabled", True)
        flow2_enabled = bill_sync_config.get("version_check", {}).get("enabled", True)

        logger.info(
            "Starting daily bill sync",
            flow1_enabled=flow1_enabled,
            flow2_enabled=flow2_enabled,
        )

        if not flow1_enabled and not flow2_enabled:
            return {"success": True, "skipped": "all flows disabled"}

        try:
            version_sync = BillVersionSyncService(self.settings)
            bills = await self._fetch_webflow_bills()

            if flow1_enabled and flow2_enabled:
                # Both flows: shared fetch via sync_bill_versions
                # (internally calls update_bill_status + check_and_reingest_version)
                result = await version_sync.sync_bill_versions(bills)
            elif flow1_enabled:
                # Flow 1 only: lightweight status sync
                result = await version_sync.sync_bill_statuses(bills)
            else:
                # Flow 2 only: version check with Webflow writes disabled
                # Temporarily set skip_webflow_update to suppress Flow 1
                version_sync._config["skip_webflow_update"] = True
                result = await version_sync.sync_bill_versions(bills)

            duration = (datetime.utcnow() - start_time).total_seconds()

            has_problems = result.failed > 0 or result.webflow_patch_failures > 0
            log_fn = logger.warning if has_problems else logger.info
            log_fn(
                "Daily bill sync completed",
                duration_seconds=round(duration, 1),
                flow1_enabled=flow1_enabled,
                flow2_enabled=flow2_enabled,
                total_bills=result.total_bills,
                checked=result.checked,
                updated=result.updated,
                unchanged=result.unchanged,
                no_versions=result.no_versions,
                skipped=result.skipped,
                failed=result.failed,
                chunks_created=result.chunks_created,
                webflow_updates=result.webflow_updates,
                status_updates=result.status_updates,
                webflow_skipped=result.webflow_skipped,
                webflow_patch_failures=result.webflow_patch_failures,
                errors=result.errors[:10] if result.errors else [],
            )

            # Record flow status in Redis
            redis_store = get_redis_store()
            await redis_store.set_flow_status("daily_bill_sync", {
                "flow": "daily_bill_sync",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": round(duration, 1),
                "status": "completed",
                "flow1_enabled": flow1_enabled,
                "flow2_enabled": flow2_enabled,
                "bills_checked": result.checked,
                "flow1_results": {
                    "bills_updated": result.webflow_updates,
                    "bills_skipped": result.webflow_skipped,
                    "bills_failed": result.webflow_patch_failures,
                },
                "flow2_results": {
                    "versions_checked": result.checked,
                    "versions_reingested": result.updated,
                    "bills_failed": result.failed,
                    "chunks_created": result.chunks_created,
                },
                "errors": result.errors[:10] if result.errors else [],
                "trigger": "scheduled",
            })

            # Call callbacks
            for callback in self._update_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback({"bill_version_sync": result})
                    else:
                        callback({"bill_version_sync": result})
                except Exception as e:
                    logger.error("Sync callback failed", error=str(e))

            return {
                "success": True,
                "flow1_enabled": flow1_enabled,
                "flow2_enabled": flow2_enabled,
                "duration_seconds": duration,
                "total_bills": result.total_bills,
                "checked": result.checked,
                "updated": result.updated,
                "unchanged": result.unchanged,
                "no_versions": result.no_versions,
                "skipped": result.skipped,
                "failed": result.failed,
                "chunks_created": result.chunks_created,
                "webflow_updates": result.webflow_updates,
                "status_updates": result.status_updates,
                "errors": result.errors[:10] if result.errors else [],
            }

        except Exception as e:
            logger.exception("Daily bill sync failed", error=str(e))
            # Record failure in Redis
            try:
                redis_store = get_redis_store()
                await redis_store.set_flow_status("daily_bill_sync", {
                    "flow": "daily_bill_sync",
                    "started_at": start_time.isoformat(),
                    "completed_at": datetime.utcnow().isoformat(),
                    "status": "failed",
                    "error": str(e),
                    "trigger": "scheduled",
                })
            except Exception:
                pass
            return {
                "success": False,
                "error": str(e),
            }

    async def trigger_openstates_sync(
        self,
        force_all: bool = False,
        webflow_only: bool = False,
    ) -> dict[str, Any]:
        """Manually trigger a bill sync.

        Args:
            force_all: If True, backload all bills (ignores session filtering).
            webflow_only: If True, run Flow 1 only (status sync, no Pinecone).

        Returns:
            Dict with sync results
        """
        logger.info(
            "Manual bill sync triggered",
            force_all=force_all,
            webflow_only=webflow_only,
        )

        if webflow_only:
            return await self.trigger_bill_status_sync(all_sessions=force_all)

        if force_all:
            from ddp_sync.pipelines.bill_sync import BillSyncService

            sync_service = BillSyncService(self.settings)
            bills = await self._fetch_webflow_bills()
            result = await sync_service.backload_all_bills(bills)

            return {
                "success": True,
                "mode": "backload_all",
                "total_bills": result.total_bills,
                "successful": result.successful,
                "failed": result.failed,
                "chunks_created": result.chunks_created,
                "errors": result.errors[:10] if result.errors else [],
            }

        return await self._run_daily_bill_sync()

    async def trigger_bill_status_sync(
        self,
        all_sessions: bool = False,
        jurisdiction: str | None = None,
    ) -> dict[str, Any]:
        """Manually trigger Flow 1: OpenStates → Webflow CMS status sync.

        Args:
            all_sessions: Bypass session filters for backfill
            jurisdiction: Filter to a single state code

        Returns:
            Dict with sync results
        """
        from ddp_sync.pipelines.bill_version import BillVersionSyncService
        from ddp_sync.services.redis_store import get_redis_store

        start_time = datetime.utcnow()
        logger.info(
            "Manual bill status sync triggered (Flow 1 only)",
            all_sessions=all_sessions,
            jurisdiction=jurisdiction,
        )

        try:
            version_sync = BillVersionSyncService(self.settings)
            bills = await self._fetch_webflow_bills()

            result = await version_sync.sync_bill_statuses(
                bills,
                all_sessions=all_sessions,
                jurisdiction=jurisdiction,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            # Record flow status in Redis
            redis_store = get_redis_store()
            await redis_store.set_flow_status("webflow_status", {
                "flow": "webflow_status",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": round(duration, 1),
                "status": "completed",
                "all_sessions": all_sessions,
                "jurisdiction": jurisdiction,
                "bills_checked": result.checked,
                "bills_updated": result.webflow_updates,
                "bills_skipped": result.webflow_skipped,
                "bills_failed": result.webflow_patch_failures,
                "trigger": "manual",
            })

            return {
                "success": True,
                "mode": "webflow_status_sync",
                "all_sessions": all_sessions,
                "jurisdiction": jurisdiction,
                "duration_seconds": duration,
                "total_bills": result.total_bills,
                "checked": result.checked,
                "status_updates": result.status_updates,
                "webflow_updates": result.webflow_updates,
                "webflow_skipped": result.webflow_skipped,
                "failed": result.failed,
                "errors": result.errors[:10] if result.errors else [],
            }

        except Exception as e:
            logger.exception("Bill status sync failed", error=str(e))
            return {
                "success": False,
                "error": str(e),
            }

    async def _run_legislator_bills_sync(self) -> dict[str, Any]:
        """
        Run the legislator sync (bills and optionally votes).

        Fetches sponsored bills and voting records for each legislator
        from OpenStates and creates documents for RAG.

        Returns:
            Dict with sync results
        """
        from ddp_sync.ingestion.sources.webflow import WebflowSource
        from ddp_sync.ingestion.metadata import MetadataExtractor
        from ddp_sync.pipelines.legislator_sync import LegislatorSyncService

        start_time = datetime.utcnow()

        try:
            # Get config - prefer new legislator_sync, fall back to legacy legislator_bills
            leg_config = self._sync_config.get("legislator_sync", {})
            if not leg_config:
                leg_config = self._sync_config.get("legislator_bills", {})

            delay_ms = leg_config.get("delay_between_legislators_ms", 500)
            max_per_run = leg_config.get("max_legislators_per_run", 200)

            # Vote sync settings
            sync_votes = leg_config.get("sync_votes", False)
            max_vote_bills = leg_config.get("max_vote_bills_per_legislator", 200)
            vote_session = leg_config.get("vote_session")  # None = current session

            sync_type = "bills + votes" if sync_votes else "bills only"
            logger.info(
                "Starting legislator sync",
                sync_type=sync_type,
                max_per_run=max_per_run,
            )

            # Initialize services
            sync_service = LegislatorSyncService(self.settings)
            webflow = WebflowSource(self.settings, MetadataExtractor())

            # Fetch legislators from Webflow
            legislators = []
            async for doc in webflow.fetch_legislators(limit=0):
                extra = doc.metadata.extra
                legislator = {
                    "openstates_id": doc.metadata.legislator_id,
                    "name": doc.metadata.title,
                    "slug": extra.get("slug", ""),
                    "jurisdiction": doc.metadata.jurisdiction or "us",
                    "party": extra.get("party", ""),
                    "chamber": extra.get("chamber", ""),
                }
                if legislator["openstates_id"]:
                    legislators.append(legislator)

            logger.info(f"Fetched {len(legislators)} legislators from Webflow")

            # Limit per run to avoid timeouts
            if max_per_run > 0 and len(legislators) > max_per_run:
                # Rotate which legislators get synced by using date-based offset
                day_of_year = datetime.utcnow().timetuple().tm_yday
                offset = (day_of_year * max_per_run) % len(legislators)
                legislators = legislators[offset:offset + max_per_run]
                if len(legislators) < max_per_run:
                    # Wrap around
                    legislators.extend(legislators[:max_per_run - len(legislators)])
                logger.info(f"Processing batch of {len(legislators)} legislators (offset {offset})")

            # Override rate limit for paced sync
            sync_service.rate_limit.delay_between_requests_ms = delay_ms

            # Run sync with votes if enabled
            result = await sync_service.sync_all_legislators(
                legislators,
                include_votes=sync_votes,
                vote_session=vote_session,
                max_vote_bills=max_vote_bills,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            logger.info(
                "Legislator sync completed",
                sync_type=sync_type,
                duration_seconds=duration,
                total_legislators=result.total_legislators,
                successful=result.successful,
                failed=result.failed,
                total_bills=result.total_bills_found,
                total_votes=result.total_votes_found,
                chunks_created=result.chunks_created,
            )

            # Call callbacks
            for callback in self._update_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback({"legislator_sync": result})
                    else:
                        callback({"legislator_sync": result})
                except Exception as e:
                    logger.error("Sync callback failed", error=str(e))

            return {
                "success": True,
                "sync_type": sync_type,
                "duration_seconds": duration,
                "total_legislators": result.total_legislators,
                "successful": result.successful,
                "failed": result.failed,
                "total_bills": result.total_bills_found,
                "total_votes": result.total_votes_found,
                "chunks_created": result.chunks_created,
                "errors": result.errors[:10] if result.errors else [],
            }

        except Exception as e:
            logger.exception("Legislator sync failed", error=str(e))
            return {
                "success": False,
                "error": str(e),
            }

    async def _run_legislator_bio_sync(self) -> dict[str, Any]:
        """Run the scheduled legislator bio + contact sync.

        Mirrors the trigger endpoint's payload-build path: instantiates
        ``LegislatorBioPipeline`` (which lazy-creates the underlying
        services and warms the unitedstates dataset cache idempotently —
        the 24h on-disk cache makes weekly runs cheap after the first).
        Reads ``legislator_bio_sync`` block from sync_schedule.yaml for
        runtime knobs. Errors are logged + re-surfaced in the return
        dict so APScheduler's job history captures them; the orchestrator
        itself wraps per-record errors and Zapier-alerts on its own
        try/finally so this method's catch is just a defensive shell.
        """
        from datetime import date, datetime
        from ddp_sync.pipelines.legislator_bio import (
            BioSyncOptions, LegislatorBioPipeline,
        )

        bio_config = self._sync_config.get("legislator_bio_sync", {})
        start_time = datetime.utcnow()

        try:
            historical_since_str = bio_config.get(
                "historical_since", "2023-01-01"
            )
            try:
                historical_since = date.fromisoformat(historical_since_str)
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid historical_since in config; using 2023-01-01",
                    raw_value=historical_since_str,
                )
                historical_since = date(2023, 1, 1)

            options = BioSyncOptions(
                target=bio_config.get("target", "all"),
                jurisdiction=bio_config.get("jurisdiction"),
                auto_create=bool(bio_config.get("auto_create", False)),
                dry_run=False,
                historical_since=historical_since,
                # Phase-4: scheduler-level wiring for the operator-driven
                # flags. Defaults preserve the "manual operator" behavior
                # (everything off) so a config without these keys keeps
                # the previous semantics. Operator flips upload_photos
                # to true once the dedicated webflow_assets_read_write_key
                # is configured AND a clean monitoring window has passed.
                upload_photos=bool(bio_config.get("upload_photos", False)),
                upload_photos_dry_run=bool(
                    bio_config.get("upload_photos_dry_run", False)
                ),
                strict_schema=bool(bio_config.get("strict_schema", False)),
            )
            logger.info(
                "Starting scheduled legislator bio sync",
                target=options.target,
                jurisdiction=options.jurisdiction,
                auto_create=options.auto_create,
                upload_photos=options.upload_photos,
                strict_schema=options.strict_schema,
            )

            pipeline = LegislatorBioPipeline()
            report = await pipeline.run(options)

            duration = (datetime.utcnow() - start_time).total_seconds()
            # Round-17 fix: explicit metric event for external monitoring
            # to scrape (separate from the orchestrator's per-record logs
            # and from the Zapier alert, which is on a different
            # routing path). success=False fires on aborted runs even
            # though the wrapper itself didn't raise.
            logger.info(
                "Scheduled legislator bio sync complete",
                metric="legislator_bio_sync.scheduled_run_completed",
                success=(not report.aborted) and len(report.errors) == 0,
                duration_seconds=duration,
                items_seen=report.cms_items_seen,
                patched=len(report.would_patch),
                created=len(report.would_create),
                errors=len(report.errors),
                aborted=report.aborted,
            )
            return {
                "success": not report.aborted,
                "items_seen": report.cms_items_seen,
                "patched": len(report.would_patch),
                "errors": len(report.errors),
                "aborted": report.aborted,
                "abort_reason": report.abort_reason,
                "duration_seconds": duration,
            }
        except Exception as e:
            logger.exception(
                "Scheduled legislator bio sync failed",
                metric="legislator_bio_sync.scheduled_run_completed",
                success=False,
            )
            return {"success": False, "error": str(e)}

    async def _run_organization_sync(self) -> dict[str, Any]:
        """
        Run the monthly organization sync.

        Fetches all organizations from Webflow CMS and re-ingests them
        into the vector store.

        Returns:
            Dict with sync results
        """
        from ddp_sync.sync.handlers.organization import OrganizationHandler
        from ddp_sync.sync.types import SyncOptions

        start_time = datetime.utcnow()
        logger.info("Starting monthly organization sync")

        try:
            handler = OrganizationHandler(self.settings)
            options = SyncOptions(include_openstates=False)
            result = await handler.sync_batch(options)

            duration = (datetime.utcnow() - start_time).total_seconds()

            logger.info(
                "Monthly organization sync completed",
                duration_seconds=duration,
                processed=result.items_processed,
                successful=result.items_successful,
                failed=result.items_failed,
                chunks_created=result.chunks_created,
            )

            # Call callbacks
            for callback in self._update_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback({"organization_sync": result})
                    else:
                        callback({"organization_sync": result})
                except Exception as e:
                    logger.error("Sync callback failed", error=str(e))

            return {
                "success": result.success,
                "duration_seconds": duration,
                "processed": result.items_processed,
                "successful": result.items_successful,
                "failed": result.items_failed,
                "chunks_created": result.chunks_created,
                "errors": result.errors[:10] if result.errors else [],
            }

        except Exception as e:
            logger.exception("Organization sync failed", error=str(e))
            return {
                "success": False,
                "error": str(e),
            }

    async def trigger_organization_sync(self) -> dict[str, Any]:
        """Manually trigger an organization sync."""
        logger.info("Manual organization sync triggered")
        return await self._run_organization_sync()

    async def trigger_legislator_bills_sync(
        self,
        limit: int = 0,
        jurisdiction: str | None = None,
        include_votes: bool | None = None,
        vote_session: str | None = None,
        max_vote_bills: int | None = None,
    ) -> dict[str, Any]:
        """
        Manually trigger a legislator sync (bills and optionally votes).

        Args:
            limit: Maximum legislators to process (0 = use config default)
            jurisdiction: Filter by jurisdiction code (e.g., 'fl', 'us')
            include_votes: Whether to sync votes (None = use config default)
            vote_session: Session filter for votes (None = use config default)
            max_vote_bills: Max bills to check for votes per legislator

        Returns:
            Dict with sync results
        """
        from ddp_sync.ingestion.sources.webflow import WebflowSource
        from ddp_sync.ingestion.metadata import MetadataExtractor
        from ddp_sync.pipelines.legislator_sync import LegislatorSyncService

        # Get config
        leg_config = self._sync_config.get("legislator_sync", {})
        if not leg_config:
            leg_config = self._sync_config.get("legislator_bills", {})

        delay_ms = leg_config.get("delay_between_legislators_ms", 500)

        # Use config defaults if not specified
        if include_votes is None:
            include_votes = leg_config.get("sync_votes", False)
        if vote_session is None:
            vote_session = leg_config.get("vote_session")
        if max_vote_bills is None:
            max_vote_bills = leg_config.get("max_vote_bills_per_legislator", 200)

        sync_type = "bills + votes" if include_votes else "bills only"

        logger.info(
            "Manual legislator sync triggered",
            limit=limit,
            jurisdiction=jurisdiction,
            sync_type=sync_type,
        )

        start_time = datetime.utcnow()

        try:
            # Initialize services
            sync_service = LegislatorSyncService(self.settings)
            webflow = WebflowSource(self.settings, MetadataExtractor())

            # Fetch legislators from Webflow
            legislators = []
            count = 0
            async for doc in webflow.fetch_legislators(limit=0):
                extra = doc.metadata.extra
                legislator = {
                    "openstates_id": doc.metadata.legislator_id,
                    "name": doc.metadata.title,
                    "slug": extra.get("slug", ""),
                    "jurisdiction": doc.metadata.jurisdiction or "us",
                    "party": extra.get("party", ""),
                    "chamber": extra.get("chamber", ""),
                }

                if not legislator["openstates_id"]:
                    continue

                # Filter by jurisdiction if specified
                if jurisdiction:
                    if legislator["jurisdiction"].lower() != jurisdiction.lower():
                        continue

                legislators.append(legislator)
                count += 1

                if limit > 0 and count >= limit:
                    break

            logger.info(f"Processing {len(legislators)} legislators")

            # Override rate limit for paced sync
            sync_service.rate_limit.delay_between_requests_ms = delay_ms

            # Run sync with votes if enabled
            result = await sync_service.sync_all_legislators(
                legislators,
                include_votes=include_votes,
                vote_session=vote_session,
                max_vote_bills=max_vote_bills,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            return {
                "success": True,
                "mode": "manual",
                "sync_type": sync_type,
                "duration_seconds": duration,
                "total_legislators": result.total_legislators,
                "successful": result.successful,
                "failed": result.failed,
                "total_bills": result.total_bills_found,
                "total_votes": result.total_votes_found,
                "chunks_created": result.chunks_created,
                "errors": result.errors[:10] if result.errors else [],
            }

        except Exception as e:
            logger.exception("Manual legislator sync failed", error=str(e))
            return {
                "success": False,
                "error": str(e),
            }

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._is_running

    def get_jobs(self) -> list[dict]:
        """Get information about scheduled jobs."""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            })
        return jobs


class UpdateSchedulerFactory:
    """Factory for creating update scheduler instances."""

    _instance: UpdateScheduler | None = None

    @classmethod
    def get_instance(cls, settings: Settings | None = None) -> UpdateScheduler:
        """Get or create a singleton scheduler instance."""
        if cls._instance is None:
            cls._instance = UpdateScheduler(settings)
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance."""
        if cls._instance and cls._instance.is_running:
            cls._instance.stop()
        cls._instance = None


def get_scheduler() -> UpdateScheduler | None:
    """Get the current scheduler instance, if any."""
    return UpdateSchedulerFactory._instance
