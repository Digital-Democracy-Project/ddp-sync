"""Scheduler for OpenStates bill sync and data pipeline jobs."""

import asyncio
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Callable

import structlog
import yaml
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ddp_sync.config import Settings, get_settings
from ddp_sync.services.legislative_calendar import StateLegislativeCalendar

logger = structlog.get_logger()

# The schedule config uses `sync_time_utc` fields, so triggers must fire in UTC. NOTE:
# APScheduler applies the scheduler's tz only to triggers IT builds (the add_job(..., "cron",
# **kw) form); a pre-built CronTrigger(...) object keeps its construction-default LOCAL tz. So
# each CronTrigger below must also pass timezone=_UTC explicitly, not just the scheduler tz.
_UTC = ZoneInfo("UTC")

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
        self.scheduler = AsyncIOScheduler(timezone=_UTC)
        self.calendar = StateLegislativeCalendar()
        self._is_running = False
        self._update_callbacks: list[Callable] = []
        self._sync_config = self._load_sync_config()
        # OPEN-140: the effective scrape cadence per jurisdiction as last registered.
        # Populated by _register_openstates_scrape_jobs and surfaced on /schedule,
        # because the floor rule puts the live cadence in Redis and so takes it out of
        # any git diff — leaving no other way to see it without a redis-cli.
        self._openstates_cadence: dict[str, str] = {}

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

        # Add daily bill sync job (shared fetch, independent write paths). SYNC-51: this had
        # no gate of any kind before -- bill_sync_enabled is a per-host .env opt-out, default
        # True (preserves every existing host's current behavior with no config change).
        if self.settings.bill_sync_enabled:
            self.scheduler.add_job(
                self._run_daily_bill_sync,
                trigger=CronTrigger(hour=hour, minute=minute),
                id="daily_bill_sync",
                name="Daily Bill Sync",
                replace_existing=True,
            )
        else:
            logger.info("bill_sync_enabled=false — skipping daily bill sync job")

        # Add legislator sync job (daily or weekly based on config)
        # Prefer new legislator_sync config, fall back to legacy legislator_bills
        leg_config = self._sync_config.get("legislator_sync", {})
        if not leg_config:
            leg_config = self._sync_config.get("legislator_bills", {})

        # SYNC-51: env flag ANDs with the existing (shared, checked-in) YAML gate -- a host
        # can opt out even while sync_schedule.yaml says enabled: true for every deployment.
        if self.settings.legislator_sync_enabled and leg_config.get("enabled", False):
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

        # SYNC-51: env flag ANDs with the existing (shared, checked-in) YAML gate.
        if self.settings.legislator_bio_sync_enabled and bio_config.get("enabled", False):
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
        # SYNC-51: env flag ANDs with the existing (shared, checked-in) YAML gate.
        if self.settings.organization_sync_enabled and org_config.get("enabled", False):
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

        # --- API health check ---
        self._register_api_health_check_job()

        # --- OpenStates jurisdiction scrapes ---
        self._register_openstates_scrape_jobs()

        # --- OpenStates bill-document archive (independent of scrape schedule) ---
        self._register_openstates_archive_jobs()

        # --- Michigan WAF cookie publish (OPEN-188, independent of scrape schedule) ---
        self._register_mi_cookie_publish_job()

        # Concept-statement dispatch's own standalone scheduled batch job
        # (ddp-infra PLAN-bill-concept-polling.md §0.4) retired here (SYNC-32)
        # -- ConceptStatementSet generation now runs through this same
        # session-targeted batch job below, via its include_concept_statements
        # option (SYNC-31).

        # --- Session-targeted BillArtifact batch job (SYNC-9) ---
        self._register_session_pipeline_batch_job()

        self.scheduler.start()
        self._is_running = True

        logger.info(
            "Update scheduler started",
            openstates_sync_time=sync_time_str,
        )

    def _register_ddp_api_jobs(self) -> None:
        """Register jobs moved from DDP-API (Voatz/Brevo sync + Webflow CMS batch).

        SYNC-51: neither half had any gate at all before -- voatz_sync_enabled and
        webflow_batch_enabled are independent per-host .env opt-outs (default True,
        preserving every existing host's current behavior with no config change), gated
        separately since a host may need one without the other.
        """
        if self.settings.voatz_sync_enabled:
            from ddp_sync.pipelines.voatz_brevo import run_sync_job, run_full_sync_job

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
        else:
            logger.info("voatz_sync_enabled=false — skipping Voatz -> Brevo sync jobs")

        if self.settings.webflow_batch_enabled:
            from ddp_sync.pipelines.webflow_batch import (
                run_webflow_bill_org_sync,
                run_webflow_check_org_missing,
                run_webflow_fill_map_url,
                run_webflow_fill_session_code,
                run_webflow_find_duplicates,
                run_webflow_merge_duplicate_orgs,
                run_webflow_org_about_parse,
            )

            # Webflow CMS batch jobs — weekly Monday at 03:00 UTC
            webflow_trigger = CronTrigger(day_of_week="mon", hour=3)
            webflow_jobs = [
                ("webflow_fill_session_code", "Webflow: fill session-code", run_webflow_fill_session_code),
                ("webflow_fill_map_url", "Webflow: fill map-url", run_webflow_fill_map_url),
                ("webflow_bill_org_sync", "Webflow: bill-org reference sync", run_webflow_bill_org_sync),
                ("webflow_org_about_parse", "Webflow: org about-field parse", run_webflow_org_about_parse),
                ("webflow_check_org_missing", "Webflow: check org missing fields", run_webflow_check_org_missing),
                ("webflow_find_duplicates", "Webflow: find duplicate bills", run_webflow_find_duplicates),
                ("webflow_merge_duplicate_orgs", "Webflow: merge duplicate orgs", run_webflow_merge_duplicate_orgs),
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
        else:
            logger.info("webflow_batch_enabled=false — skipping Webflow CMS batch jobs")

    def _register_votebot_eval_job(self) -> None:
        """Register the weekly votebot eval cron (plan §3.3).

        - Reads YAML block ``votebot_eval`` from sync_schedule.yaml.
        - Validates required keys + types via ``validate_yaml_config``.
        - Validates the votebot path (env > YAML > default; loud-fail).
        - Skips registration on any validation failure (no silent no-op).
        - Job parameters: ``max_instances=1`` + ``coalesce=True`` +
          ``misfire_grace_time=3600`` for concurrency safety.

        SYNC-51: votebot_eval_enabled is a per-host .env opt-out, ANDed with the existing
        (shared, checked-in) YAML gate below -- default True, preserves every existing host's
        current behavior with no config change.
        """
        if not self.settings.votebot_eval_enabled:
            logger.info("votebot_eval_enabled=false — skipping")
            return

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
        is_valid, err, _ = validate_votebot_path(votebot_path)
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

    def _register_openstates_scrape_jobs(self, cadences: dict[str, str] | None = None) -> None:
        """Register independent APScheduler jobs for each OpenStates jurisdiction.

        Replaces the sequential run-all-scrapes.sh launchd job. FL, WA, and
        USA run as separate daily jobs so a 12-hour FL scrape no longer delays
        WA or USA. Secondary states fan out concurrently inside their own
        Sunday job. The patch_refresh job runs at 01:00 UTC before all scrapes.

        OPEN-140: `cadences` is a per-jurisdiction effective cadence resolved from
        Redis and floored at the YAML value. Passing None -- which is what start()
        does -- registers from the YAML floors alone. That is deliberate and is
        what makes "Redis is down" structurally safe rather than merely handled:
        boot never consults Redis, so it cannot leave a jurisdiction unscheduled.
        Overrides arrive later, by the cadence-review job calling this again.

        Calling this repeatedly is the supported way to change cadence without a
        restart. Every add_job below passes replace_existing=True, and the
        secondary section removes the job ids its new layout no longer uses, so a
        jurisdiction that moves between the weekly batch and its own nightly job
        leaves exactly one cron behind either way.
        """
        # None means "startup" -- see the docstring. Captured before defaulting, because
        # only the startup pass schedules the catch-up review; re-registration triggered
        # BY a review must not schedule another one.
        is_startup = cadences is None
        cadences = dict(cadences or {})

        # What each jurisdiction actually ended up on, recorded for /schedule.
        # AC: the effective cadence has to be observable without reading Redis by
        # hand -- which matters more here than usual, because the floor rule takes
        # cadence out of git and a reader can no longer see it in a diff.
        effective: dict[str, str] = {}
        from ddp_sync.pipelines.openstates_scrape import (
            run_patch_refresh_job,
            run_fl_scrapes_job,
            run_wa_scrape_job,
            run_usa_scrapes_job,
            run_secondary_scrapes_job,
            run_people_refresh_job,
        )

        config = self._sync_config.get("openstates_scrape", {})
        # SYNC-51: env flag ANDs with the existing (shared, checked-in) YAML gate -- applies
        # to every caller of this method (startup and cadence-review re-registration alike).
        if not (self.settings.openstates_scrape_enabled and config.get("enabled", False)):
            logger.info("openstates_scrape: disabled — skipping")
            return

        # An excluded jurisdiction can never be escalated here, whatever the caller
        # says. cadence_review() already enforces this at the decision point, but that
        # only covers decisions this process made -- a value hand-written into Redis, or
        # a future caller passing a map straight to this method, would otherwise reach
        # APScheduler unchecked. MI is the one that matters: OPEN-53 established that
        # more traffic against a WAF worsens a block, which is why it is already barred
        # from scrape retries. A safety property is worth asserting twice.
        excluded_juris = set(
            config.get("dynamic_cadence", {}).get("jurisdictions_excluded", [])
        )
        for j in [j for j in cadences if j in excluded_juris and cadences[j] == "nightly"]:
            logger.error(
                "openstates_scrape: refusing to escalate an excluded jurisdiction "
                "— falling back to its configured cadence",
                jurisdiction=j,
            )
            cadences.pop(j)

        day_map = {
            "monday": "mon", "tuesday": "tue", "wednesday": "wed",
            "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun",
        }

        # --- patch refresh ---
        patch_cfg = config.get("patch_refresh", {})
        if patch_cfg.get("enabled", True):
            ph, pm = map(int, patch_cfg.get("sync_time_utc", "01:00").split(":"))

            async def _patch_refresh_wrapper():
                return await run_patch_refresh_job(config)

            self._add_job_replacing(
                _patch_refresh_wrapper,
                trigger=CronTrigger(hour=ph, minute=pm, timezone=_UTC),
                id="openstates_patch_refresh",
                name="OpenStates: apply local patches",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            logger.info(
                "openstates_patch_refresh: registered",
                sync_time=patch_cfg.get("sync_time_utc", "01:00"),
            )

        # --- primary jobs (daily, each with its own sync_time_utc) ---
        primary_cfg = config.get("primary", {})

        fl_cfg = primary_cfg.get("fl", {})
        if fl_cfg.get("enabled", True):
            fl_time = fl_cfg.get("sync_time_utc", "02:00")
            flh, flm = map(int, fl_time.split(":"))
            # Optional weekly cadence. FL has no bills to scrape while out of
            # session (new drafts aren't introduced until ~November), and a
            # daily FL scrape wipes _data/fl on startup — which collides with
            # any in-progress historical backfill sharing that dir. Set
            # sync_day to run weekly; absent → daily (in-session default).
            fl_day = fl_cfg.get("sync_day")
            fl_trigger_kwargs = {"hour": flh, "minute": flm, "timezone": _UTC}
            # OPEN-140: the YAML sync_day is a FLOOR. An escalation may lift FL to
            # nightly by dropping day_of_week; nothing may push it below weekly.
            # This is the case the ticket was filed for -- FL's sync_day carries a
            # "remove once the 2027 session opens" comment that depends on a human
            # remembering in November.
            fl_effective = cadences.get("fl", "weekly" if fl_day else "nightly")
            if fl_day and fl_effective != "nightly":
                fl_trigger_kwargs["day_of_week"] = day_map.get(fl_day.lower(), "sun")

            async def _fl_wrapper():
                return await run_fl_scrapes_job(config)

            self._add_job_replacing(
                _fl_wrapper,
                trigger=CronTrigger(**fl_trigger_kwargs),
                id="openstates_fl_scrape",
                name="OpenStates: FL scrape (all sessions)",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            logger.info(
                "openstates_fl_scrape: registered",
                sync_time=fl_time,
                sync_day=fl_day or "daily",
                effective_cadence=fl_effective,
            )
            effective["fl"] = fl_effective

        wa_cfg = primary_cfg.get("wa", {})
        if wa_cfg.get("enabled", True):
            wa_time = wa_cfg.get("sync_time_utc", "02:30")
            wah, wam = map(int, wa_time.split(":"))

            async def _wa_wrapper():
                return await run_wa_scrape_job(config)

            self._add_job_replacing(
                _wa_wrapper,
                trigger=CronTrigger(hour=wah, minute=wam, timezone=_UTC),
                id="openstates_wa_scrape",
                name="OpenStates: WA scrape",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            logger.info("openstates_wa_scrape: registered", sync_time=wa_time)

        usa_cfg = primary_cfg.get("usa", {})
        if usa_cfg.get("enabled", True):
            usa_time = usa_cfg.get("sync_time_utc", "03:00")
            usah, usam = map(int, usa_time.split(":"))

            async def _usa_wrapper():
                return await run_usa_scrapes_job(config)

            self._add_job_replacing(
                _usa_wrapper,
                trigger=CronTrigger(hour=usah, minute=usam, timezone=_UTC),
                id="openstates_usa_scrape",
                name="OpenStates: USA scrape (lower + upper)",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            logger.info("openstates_usa_scrape: registered", sync_time=usa_time)

        # --- secondary jobs (weekly batch, minus anything escalated to nightly) ---
        sec_cfg = config.get("secondary", {})
        if sec_cfg.get("enabled", True):
            sec_day = sec_cfg.get("sync_day", "sunday")
            sec_time = sec_cfg.get("sync_time_utc", "02:00")
            sh, sm = map(int, sec_time.split(":"))
            all_secondary: list[str] = list(sec_cfg.get("jurisdictions", []))

            # OPEN-140: the secondary states do not have one job each -- they share a
            # single weekly job that fans out over a list. So escalating one to nightly
            # is not a trigger edit, it is a move: the jurisdiction leaves the batch and
            # gets a job of its own. Leaving it in the batch as well would scrape it
            # twice on the batch's day, and each run wipes the other's _data dir.
            #
            # The batch keeps its own id and its own weekly day, so a jurisdiction that
            # is never escalated is registered byte-for-byte as it is today.
            nightly = [j for j in all_secondary if cadences.get(j) == "nightly"]
            weekly = [j for j in all_secondary if j not in nightly]
            for j in all_secondary:
                effective[j] = "nightly" if j in nightly else "weekly"

            # Drop the per-jurisdiction jobs this layout no longer uses, before adding
            # the ones it does. Without this a demotion -- or an operator lowering a
            # floor -- would leave the old nightly cron registered alongside the batch,
            # which is the stale-job-id hazard scheduler.py already solved once for
            # legislator_bio_sync and is solving again here for the same reason.
            for j in all_secondary:
                if j in nightly:
                    continue
                self._remove_job_if_present(f"openstates_secondary_scrape_{j}")

            for j in nightly:
                async def _single_secondary_wrapper(_j: str = j):
                    return await run_secondary_scrapes_job(
                        config,
                        jurisdictions=[_j],
                        flow_status_key=f"openstates_secondary_scrape_{_j}",
                    )

                self._add_job_replacing(
                    _single_secondary_wrapper,
                    trigger=CronTrigger(hour=sh, minute=sm, timezone=_UTC),
                    id=f"openstates_secondary_scrape_{j}",
                    name=f"OpenStates: {j} scrape (escalated to nightly)",
                    replace_existing=True,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=3600,
                )
                logger.info(
                    "openstates_secondary_scrape: registered nightly",
                    jurisdiction=j,
                    sync_time=sec_time,
                )

            if weekly:
                async def _secondary_wrapper(_weekly: list[str] = weekly):
                    # Pass the narrowed list explicitly rather than letting the job
                    # re-read config: config still names every secondary jurisdiction,
                    # including the escalated ones that now run on their own.
                    return await run_secondary_scrapes_job(config, jurisdictions=_weekly)

                self._add_job_replacing(
                    _secondary_wrapper,
                    trigger=CronTrigger(
                        day_of_week=day_map.get(sec_day.lower(), "sun"),
                        hour=sh,
                        minute=sm,
                        timezone=_UTC,
                    ),
                    id="openstates_secondary_scrapes",
                    name="OpenStates: secondary states",
                    replace_existing=True,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=3600,
                )
                logger.info(
                    "openstates_secondary_scrapes: registered",
                    jurisdictions=weekly,
                    sync_day=sec_day,
                    sync_time=sec_time,
                )
            else:
                # Every secondary jurisdiction escalated. An empty batch job would fire
                # weekly and scrape nothing, so remove it rather than register a no-op.
                self._remove_job_if_present("openstates_secondary_scrapes")
                logger.info(
                    "openstates_secondary_scrapes: not registered — every jurisdiction "
                    "is on its own nightly job",
                    jurisdictions=nightly,
                )

        # --- people refresh (weekly) ---
        people_cfg = config.get("people_refresh", {})
        if people_cfg.get("enabled", True):
            p_day = people_cfg.get("sync_day", "sunday")
            p_time = people_cfg.get("sync_time_utc", "10:00")
            pph, ppm = map(int, p_time.split(":"))

            async def _people_wrapper():
                return await run_people_refresh_job(config)

            self._add_job_replacing(
                _people_wrapper,
                trigger=CronTrigger(
                    day_of_week=day_map.get(p_day.lower(), "sun"),
                    hour=pph,
                    minute=ppm,
                    timezone=_UTC,
                ),
                id="openstates_people_refresh",
                name="OpenStates: people refresh",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            logger.info(
                "openstates_people_refresh: registered",
                sync_day=p_day,
                sync_time=p_time,
            )

        self._openstates_cadence = effective

        # --- cadence review (OPEN-140) ---
        self._register_cadence_review_job(config, startup=is_startup)

    def _remove_job_if_present(self, job_id: str) -> None:
        try:
            self.scheduler.remove_job(job_id)
        except Exception:  # noqa: BLE001 — JobLookupError variant
            pass

    def _add_job_replacing(self, func, **kwargs) -> None:
        """add_job that is idempotent whether or not the scheduler has started.

        `replace_existing=True` only replaces jobs in the JOBSTORE. Before start()
        APScheduler holds everything in `_pending_jobs`, and add_job appends there
        without consulting the flag -- so registering twice before start leaves two
        crons for the same id.

        Production does not hit that today (start() registers once, and the cadence
        review re-registers only while running), but "exactly one cron per job" has to
        hold in both states or it is a trap set for whoever calls this next.

        The remove is deliberately confined to the not-yet-started case. On a RUNNING
        scheduler, replace_existing=True already swaps the job in the jobstore in one
        step, and removing first would open a window where the job does not exist --
        so a failure between the two would leave a live jurisdiction unscheduled. There
        is no live schedule to damage before start(), which is the only place the
        remove is needed at all.
        """
        if not self.scheduler.running:
            self._remove_job_if_present(kwargs["id"])
        self.scheduler.add_job(func, **kwargs)

    def _register_cadence_review_job(self, config: dict, startup: bool = False) -> None:
        """Register the daily job that re-decides cadence and applies it live.

        Kept out of _register_openstates_scrape_jobs' body only because that method
        is what this job calls -- re-registering the review job from inside its own
        re-registration is legal here (replace_existing=True) but reads as a loop.
        """
        cadence_cfg = config.get("dynamic_cadence", {})
        if not cadence_cfg.get("enabled", False):
            # Ships dark. The decision logic needs OPEN-139's bills_new figures in the
            # run history before it can decide anything, and on weekly jobs that is
            # weeks of accumulation. Nothing is registered, so nothing can fire.
            logger.info("openstates_cadence_review: disabled in config — skipping")
            self._remove_job_if_present("openstates_cadence_review")
            return

        # 00:30 UTC: after the previous night's runs have recorded their history and
        # before the day's first scrape at 01:00 (patch refresh) / 02:00 (FL, secondary),
        # so a cadence decided today takes effect today rather than a day late.
        rh, rm = map(int, cadence_cfg.get("review_time_utc", "00:30").split(":"))

        job_kwargs: dict[str, Any] = {}
        if startup:
            # Catch-up review shortly after boot, then the normal daily cron.
            #
            # Startup registers from the YAML floors and never reads Redis, which is
            # what keeps a Redis outage from leaving anything unscheduled. The cost is
            # that a restart drops any stored escalation back to its floor -- and
            # pm-review was right that on a 00:30 cron, a 03:00 restart would leave a
            # jurisdiction under-scraped for nearly 24 hours. ddp-sync is deployed by a
            # launchctl kickstart, so restarts are routine, not rare.
            #
            # A few minutes of delay rather than immediately: the scheduler has to be
            # running and Redis reachable before the review can do anything useful, and
            # a review that fires into a half-warm process just fails and logs.
            delay = int(cadence_cfg.get("startup_review_delay_s", 300))
            job_kwargs["next_run_time"] = datetime.now(_UTC) + timedelta(seconds=delay)

        self._add_job_replacing(
            self._run_cadence_review,
            trigger=CronTrigger(hour=rh, minute=rm, timezone=_UTC),
            id="openstates_cadence_review",
            name="OpenStates: scrape cadence review",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
            **job_kwargs,
        )
        logger.info(
            "openstates_cadence_review: registered",
            review_time=cadence_cfg.get("review_time_utc", "00:30"),
            startup_catch_up=startup,
            escalate_window=cadence_cfg.get("escalate_window", 2),
            quiet_window=cadence_cfg.get("quiet_window", 4),
            excluded=cadence_cfg.get("jurisdictions_excluded", []),
        )

    async def _run_cadence_review(self) -> dict[str, Any]:
        """Re-decide every jurisdiction's cadence, persist escalations, apply them live.

        This is the actuation half of OPEN-140. The decision half already exists and is
        pure (services/scrape_cadence.py); this reads the inputs, writes the one output
        that persists, and then re-registers the scrape jobs so the change takes effect
        without a restart -- which was the ticket's only genuine gap, since scheduler.py
        loads its YAML once in __init__ and builds CronTriggers at start().

        Never raises. A review that cannot run must leave the existing schedule exactly
        as it is; the jobs registered from the YAML floors are always a safe state to
        stay in, and refusing to reschedule is strictly better than rescheduling from a
        half-read view of Redis.
        """
        from ddp_sync.services.redis_store import get_redis_store
        from ddp_sync.services.scrape_cadence import cadence_from_yaml, cadence_review

        config = self._sync_config.get("openstates_scrape", {})
        cadence_cfg = config.get("dynamic_cadence", {})
        if not cadence_cfg.get("enabled", False):
            return {"success": True, "skipped": "disabled"}

        excluded = cadence_cfg.get("jurisdictions_excluded", [])
        escalate_window = cadence_cfg.get("escalate_window", 2)
        quiet_window = cadence_cfg.get("quiet_window", 4)

        # Floors, straight from the committed config. Every jurisdiction whose cadence
        # this can move has to appear here, or resolve_cadence has no floor to floor it at.
        floors: dict[str, str] = {}
        fl_cfg = config.get("primary", {}).get("fl", {})
        if fl_cfg.get("enabled", True):
            floors["fl"] = cadence_from_yaml(fl_cfg)
        sec_cfg = config.get("secondary", {})
        if sec_cfg.get("enabled", True):
            # The batch's own sync_day is the floor for every jurisdiction in it.
            sec_floor = cadence_from_yaml(sec_cfg)
            for j in sec_cfg.get("jurisdictions", []):
                floors[j] = sec_floor

        # Read and decide for EVERY jurisdiction before writing anything. pm-review
        # found the ordering that matters: with read-decide-write interleaved per
        # jurisdiction, a read that throws on the fourth jurisdiction happens after the
        # first three escalations are already stored, leaving Redis saying nightly while
        # the live schedule stays weekly. Doing all the reads first means a read failure
        # aborts before any write exists to disagree with.
        try:
            redis_store = get_redis_store()
            cadences: dict[str, str] = {}
            verdicts: list[dict[str, Any]] = []

            for jurisdiction, floor in floors.items():
                override = await redis_store.get_scrape_cadence(jurisdiction)
                # History is recorded under the flow that ran the scrape. Escalated
                # jurisdictions keep writing to the secondary flow's key on purpose --
                # see run_secondary_scrapes_job -- so one lookup covers both cadences.
                flow = (
                    "openstates_fl_scrape" if jurisdiction == "fl"
                    else "openstates_secondary_scrapes"
                )
                history = await redis_store.get_run_history(flow, jurisdiction)

                verdict = cadence_review(
                    jurisdiction=jurisdiction,
                    floor=floor,
                    current_override=override,
                    history=history,
                    escalate_window=escalate_window,
                    quiet_window=quiet_window,
                    excluded=excluded,
                )
                verdicts.append(verdict)
                cadences[jurisdiction] = verdict["effective"]
        except Exception as e:  # noqa: BLE001 — a failed review must not touch the schedule
            logger.error(
                "openstates_cadence_review: could not read — leaving the current "
                "schedule alone",
                error=str(e),
            )
            return {"success": False, "error": str(e)}

        for verdict in verdicts:
            if verdict["action"] != "escalate":
                continue
            jurisdiction = verdict["jurisdiction"]
            try:
                stored = await redis_store.set_scrape_cadence(
                    jurisdiction, verdict["effective"],
                )
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "openstates_cadence_review: escalation write failed",
                    jurisdiction=jurisdiction,
                    error=str(e),
                )
                stored = False
            if not stored:
                # Do not schedule from a decision we could not record: a nightly job
                # with no override behind it would silently revert at the next restart,
                # and a schedule that disagrees with its own stored state is worse than
                # not escalating, because nothing would ever say so. Tomorrow's review
                # makes the same decision again.
                logger.error(
                    "openstates_cadence_review: could not persist an escalation "
                    "— leaving this jurisdiction on its floor",
                    jurisdiction=jurisdiction,
                    cadence=verdict["effective"],
                )
                cadences[jurisdiction] = verdict["previous"]
                verdict["action"] = "none"
                continue
            logger.info(
                "openstates_cadence_review: cadence changed",
                jurisdiction=jurisdiction,
                previous=verdict["previous"],
                effective=verdict["effective"],
                floor=verdict["floor"],
                reason="observed filing activity",
            )

        changed = [v for v in verdicts if v["action"] != "none"]
        # Always record what was resolved, even when nothing changed, so /schedule
        # reports live values rather than only post-change ones.
        self._openstates_cadence = cadences
        if changed:
            # The whole point: re-register so the new cadence is in force now, rather
            # than at the next process restart.
            try:
                self._register_openstates_scrape_jobs(cadences)
            except Exception as e:  # noqa: BLE001 — the docstring's promise, kept
                # Redis now says nightly while the schedule still says weekly. That is
                # the safe direction -- under-scraping, not scraping a WAF-guarded site
                # twice a day -- and it self-heals: the next review, and any restart,
                # re-read the stored override and apply it.
                logger.error(
                    "openstates_cadence_review: could not apply the new schedule; "
                    "the stored cadence will be re-applied at the next review",
                    error=str(e),
                    cadences=cadences,
                )
                return {"success": False, "error": str(e), "cadences": cadences}

        logger.info(
            "openstates_cadence_review: completed",
            reviewed=len(verdicts),
            changed=len(changed),
            rescheduled=bool(changed),
            cadences=cadences,
            demotion_advice=[v["jurisdiction"] for v in verdicts if v["demotion_advice"]],
        )
        return {
            "success": True,
            "cadences": cadences,
            "changed": [v["jurisdiction"] for v in changed],
            "verdicts": verdicts,
        }

    def _register_openstates_archive_jobs(self) -> None:
        """Register one weekly APScheduler job per OpenStates archive jurisdiction.

        Split out of the scrape jobs (2026-07-31, ddp-open-states
        PLAN-open-states.md's incremental-scraping section): archiving to
        DDP-HOT used to run as the last step of run-scrape.sh, gating that
        script's incremental-cutoff marker write on the archive step
        finishing too — a run whose archive step ran long or died left the
        cutoff stuck, making the next run slower and more likely to also
        miss its own archive window.

        Originally registered as a single daily job fanning out to every
        ARCHIVE_ENABLED_STATE jurisdiction concurrently. Changed 2026-08-10
        to one job per jurisdiction, each on its own weekly day (see the
        `schedule` map in openstates_archive's config block): `us` alone has
        two orders of magnitude more never-archived documents than any state
        jurisdiction, so running it daily alongside everything else let its
        fetch+extract volume dominate shared CPU/network/DDP-HOT I/O and
        starve the smaller jurisdictions' own runs.
        """
        from ddp_sync.pipelines.openstates_archive import run_single_archive_job

        config = self._sync_config.get("openstates_archive", {})

        # The old code registered a single "openstates_archive" job covering every
        # jurisdiction. Clean it up so a process that re-registers jobs from a reloaded
        # config doesn't end up with both the old batch job and the new per-jurisdiction
        # ones (same defensive shape as the legislator_bio_sync round-17 fix above).
        try:
            self.scheduler.remove_job("openstates_archive")
        except Exception:  # noqa: BLE001 — JobLookupError variant
            pass

        # SYNC-51: env flag ANDs with the existing (shared, checked-in) YAML gate.
        if not (self.settings.openstates_archive_enabled and config.get("enabled", False)):
            logger.info("openstates_archive: disabled — skipping")
            return

        day_map = {
            "monday": "mon", "tuesday": "tue", "wednesday": "wed",
            "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun",
        }

        archive_time = config.get("sync_time_utc", "05:00")
        ah, am = map(int, archive_time.split(":"))
        schedule_cfg = config.get("schedule", {})
        jurisdictions: list[str] = config.get("jurisdictions", [])

        for jurisdiction in jurisdictions:
            sync_day = (schedule_cfg.get(jurisdiction) or "sunday").lower()
            day_of_week = day_map.get(sync_day, "sun")

            async def _archive_wrapper(_jurisdiction: str = jurisdiction):
                return await run_single_archive_job(_jurisdiction, config)

            self.scheduler.add_job(
                _archive_wrapper,
                trigger=CronTrigger(day_of_week=day_of_week, hour=ah, minute=am, timezone=_UTC),
                id=f"openstates_archive_{jurisdiction}",
                name=f"OpenStates: bill-document archive ({jurisdiction})",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            logger.info(
                "openstates_archive: registered",
                jurisdiction=jurisdiction,
                sync_day=sync_day,
                sync_time=archive_time,
            )

    def _register_mi_cookie_publish_job(self) -> None:
        """OPEN-188: mint Michigan's WAF cookies and publish them to the shared S3 memory
        store on an independent schedule, decoupled from whether a Michigan scrape is
        actually running right now -- see mi_cookie_publish.py's own module docstring for
        why this is a separate schedule rather than reusing the scrape's own pre-seed.

        Config lives under openstates_scrape.mi_cookie_publish, disabled by default so
        nothing mints or publishes unless explicitly turned on.
        """
        from ddp_sync.pipelines.mi_cookie_publish import run_mi_cookie_publish_job

        config = self._sync_config.get("openstates_scrape", {})
        publish_cfg = config.get("mi_cookie_publish", {})
        # SYNC-51: env flag ANDs with the existing (shared, checked-in) YAML gate.
        if not (self.settings.mi_cookie_publish_enabled and publish_cfg.get("enabled", False)):
            logger.info("mi_cookie_publish: disabled — skipping")
            return

        interval_hours = publish_cfg.get("interval_hours", 6)

        async def _mi_cookie_publish_wrapper():
            return await run_mi_cookie_publish_job(config)

        self._add_job_replacing(
            _mi_cookie_publish_wrapper,
            trigger=IntervalTrigger(hours=interval_hours),
            id="mi_cookie_publish",
            name="OpenStates: publish Michigan WAF cookies",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        logger.info("mi_cookie_publish: registered", interval_hours=interval_hours)

    def _register_session_pipeline_batch_job(self) -> None:
        """Register the session-targeted BillArtifact batch job (SYNC-9).

        Reads YAML block ``session_pipeline_batch``, mirroring
        ``concept_statement_dispatch``'s own ``enabled``/``frequency``/
        ``sync_day``/``sync_time_utc`` shape. Shipped ``enabled: false`` in
        sync_schedule.yaml -- see that block's own comment for why
        (ddp-infra's Phase 8 concurrency cap/prioritization isn't live yet,
        verified 2026-08-14 directly against PLAN-legbot.md and
        PLAN-bill-document-provenance.md).

        Unlike ``concept_statement_dispatch``'s all-optional block, this
        job has genuinely required fields with no sensible default
        (jurisdiction_iso2, session_code, artifact_types, limit) --
        validated here, before registration, same gate style
        ``votebot_eval`` uses for its own required config. An ``enabled:
        true`` block missing one of these skips registration (logged as an
        error) rather than registering a job that would fail every time it
        fires.
        """
        from ddp_sync.pipelines.session_pipeline_runner import (
            _REQUIRED_BATCH_CONFIG_KEYS,
            run_scheduled_session_pipeline,
        )

        config = self._sync_config.get("session_pipeline_batch", {})

        if not config.get("enabled", False):
            logger.info("session_pipeline_batch: disabled in config — skipping")
            return

        missing = [key for key in _REQUIRED_BATCH_CONFIG_KEYS if not config.get(key)]
        if missing:
            logger.error(
                "session_pipeline_batch: missing required config keys — skipping registration",
                missing_keys=missing,
            )
            return

        sync_time_str = config.get("sync_time_utc", "13:00")
        hour, minute = map(int, sync_time_str.split(":"))
        frequency = config.get("frequency", "weekly")

        async def _session_pipeline_batch_wrapper():
            return await run_scheduled_session_pipeline(config)

        if frequency == "daily":
            trigger = CronTrigger(hour=hour, minute=minute, timezone=_UTC)
            cadence_str = f"daily at {sync_time_str} UTC"
        else:
            day_map = {
                "monday": "mon", "tuesday": "tue", "wednesday": "wed",
                "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun",
            }
            sync_day = config.get("sync_day", "sunday").lower()
            trigger = CronTrigger(
                day_of_week=day_map.get(sync_day, "sun"),
                hour=hour,
                minute=minute,
                timezone=_UTC,
            )
            cadence_str = f"weekly {sync_day} {sync_time_str} UTC"

        self.scheduler.add_job(
            _session_pipeline_batch_wrapper,
            trigger=trigger,
            id="session_pipeline_batch",
            name="Session-targeted BillArtifact batch (SYNC-9)",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        logger.info(
            "session_pipeline_batch: registered",
            cadence=cadence_str,
            jurisdiction_iso2=config.get("jurisdiction_iso2"),
            session_code=config.get("session_code"),
            artifact_types=config.get("artifact_types"),
            limit=config.get("limit"),
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
                # Phase-4 photo coverage metrics. Always present (zero
                # when upload_photos was disabled) for stable
                # dashboarding.
                photo_uploads_attempted=report.photo_uploads_attempted,
                photo_uploads_succeeded=report.photo_uploads_succeeded,
                photo_uploads_failed=report.photo_uploads_failed,
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

    def _register_api_health_check_job(self) -> None:
        """Register the nightly API health check job."""
        from ddp_sync.pipelines.api_health_check import run_api_health_check_job

        config = self._sync_config.get("api_health_check", {})
        # SYNC-51: env flag ANDs with the existing (shared, checked-in) YAML gate.
        if not (self.settings.api_health_check_enabled and config.get("enabled", False)):
            logger.info("api_health_check: disabled — skipping")
            return

        sync_time_str = config.get("sync_time_utc", "09:00")
        hour, minute = map(int, sync_time_str.split(":"))

        self.scheduler.add_job(
            run_api_health_check_job,
            trigger=CronTrigger(hour=hour, minute=minute),
            id="api_health_check",
            name="API health check",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        logger.info("api_health_check: registered", sync_time=sync_time_str)

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
