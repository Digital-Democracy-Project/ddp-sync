"""On-demand trigger endpoints for scheduled jobs."""

import asyncio
import logging
from dataclasses import asdict
from datetime import date

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request

from ddp_sync.api.auth import api_key_auth

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/trigger/user-sync")
async def trigger_user_sync(token: str = Depends(api_key_auth)):
    """Trigger incremental Voatz -> Brevo user sync."""
    try:
        from ddp_sync.pipelines.voatz_brevo import run_sync_job
        await asyncio.get_event_loop().run_in_executor(None, run_sync_job)
        return {"status": "completed", "job": "user_sync"}
    except Exception as e:
        logger.error(f"User sync trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/full-sync")
async def trigger_full_sync(token: str = Depends(api_key_auth)):
    """Trigger full-attribute Voatz -> Brevo sync."""
    try:
        from ddp_sync.pipelines.voatz_brevo import run_full_sync_job
        await asyncio.get_event_loop().run_in_executor(None, run_full_sync_job)
        return {"status": "completed", "job": "full_sync"}
    except Exception as e:
        logger.error(f"Full sync trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/bill-version-check")
async def trigger_bill_version_check(token: str = Depends(api_key_auth)):
    """Trigger the daily bill version check (status updates to Webflow CMS)."""
    try:
        from ddp_sync.scheduler import get_scheduler
        scheduler = get_scheduler()
        if not scheduler:
            raise HTTPException(status_code=503, detail="Scheduler not initialized")
        result = await scheduler.trigger_openstates_sync(force_all=False)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Bill version check trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/bill-status-sync")
async def trigger_bill_status_sync(
    all_sessions: bool = False,
    jurisdiction: str | None = None,
    token: str = Depends(api_key_auth),
):
    """Sync OpenStates → Webflow CMS status fields only (no Pinecone).

    Lightweight alternative to bill-version-check that only updates
    status, status-date, status-chamber, and gov-url in Webflow CMS.

    Query params:
        all_sessions: Bypass session filters for backfill (default false)
        jurisdiction: Filter to a single state code (e.g. FL)
    """
    try:
        from ddp_sync.scheduler import get_scheduler
        scheduler = get_scheduler()
        if not scheduler:
            raise HTTPException(status_code=503, detail="Scheduler not initialized")
        result = await scheduler.trigger_bill_status_sync(
            all_sessions=all_sessions,
            jurisdiction=jurisdiction,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Bill status sync trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/legislator-bio-sync")
async def trigger_legislator_bio_sync(
    request: Request,
    dry_run: bool = False,
    auto_create: bool | None = None,
    jurisdiction: str | None = None,
    target: str = "all",
    limit: int = 0,
    historical_since: str | None = None,
    audit_only: str | None = None,
    strict_schema: bool = False,
    upload_photos: bool = False,
    upload_photos_dry_run: bool = False,
    token: str = Depends(api_key_auth),
):
    """Trigger the legislator bio + contact sync.

    See plans/PLAN-legislator-bio-sync.md for the full design. Phase 1
    federal-only; state path is a clear stub.

    Query params:
        dry_run:          Preview the diff without writing.
        auto_create:      Create drafts for upstream-only members. Defaults
                          to the per-jurisdiction config (currently false).
        jurisdiction:     Filter to one state code ("FL", "WA", ..., "us"
                          for federal). Default: all configured.
        target:           "all" / "webflow" / "pinecone".
        limit:            Cap items processed. 0 = unlimited.
        historical_since: Federal historical backfill cutoff (YYYY-MM-DD).
                          Default: 2023-01-01.
        audit_only:       Skip the sync and return just an audit report.
                          Values: "A" (federal join-key coverage),
                          "B" (bulk-import readiness — every record has
                          openstatesid + no duplicates), "C" (pre-existing
                          state CMS records lacking openstatesid).
        strict_schema:    Phase-3 validation flag. When True, any payload
                          field that the schema cache would silently drop
                          (slug missing from the live CMS collection)
                          becomes a per-record error. Default False;
                          flip True for the first deploy after adding a
                          new write target so missing slugs surface
                          instead of silently no-op'ing.
        upload_photos:    Phase-3 photo-upload pipeline. When True, the
                          orchestrator fetches the source image from
                          photo-source-url and uploads to Webflow's
                          asset library, populating the legislator-image
                          (Image type) field. Default False; flip True
                          after editor verification of a sample. Skipped
                          when CMS already has a legislator-image set
                          (cardinal rule). Per-record upload failures
                          are logged + isolated, don't abort the run.
        upload_photos_dry_run: Phase-3 connectivity smoke. When True
                          (with upload_photos=True), the source image
                          is fetched + size-validated + hashed but the
                          Webflow asset-creation step is skipped.
                          Lets operators smoke-test source-CDN
                          reachability without consuming Webflow's
                          asset rate limit or storage. No-op when
                          upload_photos is False.

    Returns: BioSyncReport JSON.

    503 if the congress-legislators source isn't yet warmed at app
    startup — retry in ~60s. Set Retry-After header.
    """
    # ALB-timeout safety gate (round-7 fix). The startup pre-warm task
    # in app.py::lifespan usually finishes before any trigger arrives,
    # but a request can race the pre-warm on a freshly-scaled-up
    # container. Returning 503 with Retry-After is honest about the
    # actual state and avoids a silent 30s ALB idle timeout on the cold
    # path.
    source = getattr(request.app.state, "congress_legislators", None)
    if source is None or not source._warmed:
        logger.warning(
            "legislator-bio-sync trigger arrived before pre-warm complete; "
            "returning 503 Retry-After=60"
        )
        raise HTTPException(
            status_code=503,
            detail=(
                "Bio-sync source still warming up; retry in ~60s. "
                "(8.6 MB historical YAML parse runs once at app startup.)"
            ),
            headers={"Retry-After": "60"},
        )

    # Validate target
    if target not in ("all", "webflow", "pinecone"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid target '{target}'. Must be all/webflow/pinecone.",
        )

    # Audit-only short-circuits the sync (step 6: Audits A and C; Audit B
    # added before scheduler enable).
    if audit_only is not None:
        audit_code = audit_only.upper()
        if audit_code not in ("A", "B", "C"):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid audit_only='{audit_only}'. Use 'A' (federal "
                    "join-key), 'B' (bulk-import readiness — no missing or "
                    "duplicate openstatesid), or 'C' (pre-existing state "
                    "lacking openstatesid)."
                ),
            )
        from ddp_sync.pipelines.legislator_bio import LegislatorBioPipeline
        try:
            pipeline = LegislatorBioPipeline(congress=source)
            if audit_code == "A":
                report = await pipeline.audit_federal_join_keys()
            elif audit_code == "B":
                report = await pipeline.audit_bulk_import_readiness()
            else:  # "C"
                report = await pipeline.audit_state_join_keys(
                    jurisdiction=jurisdiction,
                )
            return asdict(report)
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("legislator-bio-sync audit failed")
            raise HTTPException(status_code=500, detail=str(e))

    # Parse historical_since
    try:
        if historical_since:
            since = date.fromisoformat(historical_since)
        else:
            since = date(2023, 1, 1)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid historical_since '{historical_since}': {e}",
        )

    # Build options + run
    from ddp_sync.pipelines.legislator_bio import (
        BioSyncOptions,
        LegislatorBioPipeline,
    )

    # auto_create defaults: currently always false until per-jurisdiction
    # config wiring lands (step 7). Editors can flip explicitly via the
    # query param.
    effective_auto_create = bool(auto_create) if auto_create is not None else False

    options = BioSyncOptions(
        target=target,  # type: ignore[arg-type]
        jurisdiction=jurisdiction,
        auto_create=effective_auto_create,
        dry_run=dry_run,
        limit=limit,
        historical_since=since,
        strict_schema=strict_schema,
        upload_photos=upload_photos,
        upload_photos_dry_run=upload_photos_dry_run,
    )

    try:
        # Reuse the pre-warmed source from app.state so we don't pay
        # the parse cost again.
        pipeline = LegislatorBioPipeline(congress=source)
        report = await pipeline.run(options)
        return asdict(report)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("legislator-bio-sync trigger failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/votebot-eval")
async def trigger_votebot_eval(
    days: int = 7,
    token: str = Depends(api_key_auth),
):
    """Trigger an on-demand votebot eval run.

    See plans/PLAN-eval-and-cache-hit-logging.md §3.4 for the full design.

    Query params:
        days: Window passed to evaluate_production.py --days. Bounded by
              the YAML config's ``max_days`` (default 30). Out-of-range
              returns 400.

    Status codes:
        200 — run completed successfully (returns headline + regressions).
        400 — days outside [1, max_days].
        409 — another run is currently in flight (returns current_run_id
              + lock TTL).
        500 — unexpected error.
        503 — votebot path is invalid (Phase 1 not deployed, EC2 path
              moved, etc.).
    """
    from ddp_sync.pipelines.votebot_eval import (
        run_votebot_eval,
        DEFAULT_MAX_DAYS,
    )
    from ddp_sync.scheduler import get_scheduler

    scheduler = get_scheduler()
    yaml_config = (
        scheduler._sync_config.get("votebot_eval") if scheduler else None
    )
    max_days = (yaml_config or {}).get("max_days", DEFAULT_MAX_DAYS)
    if not isinstance(days, int) or days < 1 or days > max_days:
        raise HTTPException(
            status_code=400,
            detail=f"days must be in [1, {max_days}], got {days}",
        )

    try:
        result = await run_votebot_eval(
            days=days,
            yaml_config=yaml_config,
            trigger="manual",
        )
    except Exception as e:
        logger.exception("votebot-eval trigger failed unexpectedly")
        raise HTTPException(status_code=500, detail=str(e))

    if result.get("success"):
        return result

    err = result.get("error")
    if err == "already_running":
        raise HTTPException(
            status_code=409,
            detail={
                "error": "already_running",
                "current_run_id": result.get("current_run_id"),
            },
        )
    if err == "votebot_path_invalid":
        raise HTTPException(
            status_code=503,
            detail={
                "error": "votebot_path_invalid",
                "message": result.get("detail"),
            },
        )
    # Other failures (timeout, subprocess_nonzero, redis_unavailable, parse_error)
    raise HTTPException(status_code=500, detail=result)


@router.post("/trigger/webflow/{job_name}")
async def trigger_webflow_job(job_name: str, token: str = Depends(api_key_auth)):
    """Trigger a specific Webflow CMS batch job."""
    from ddp_sync.pipelines import webflow_batch

    job_map = {
        "fill-session-code": webflow_batch.run_webflow_fill_session_code,
        "fill-map-url": webflow_batch.run_webflow_fill_map_url,
        "bill-org-sync": webflow_batch.run_webflow_bill_org_sync,
        "org-about-parse": webflow_batch.run_webflow_org_about_parse,
        "check-org-missing": webflow_batch.run_webflow_check_org_missing,
        "find-duplicates": webflow_batch.run_webflow_find_duplicates,
        "merge-duplicate-orgs": webflow_batch.run_webflow_merge_duplicate_orgs,
    }

    func = job_map.get(job_name)
    if not func:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown job: {job_name}. Available: {', '.join(job_map.keys())}"
        )

    try:
        await asyncio.get_event_loop().run_in_executor(None, func)
        return {"status": "completed", "job": f"webflow_{job_name}"}
    except Exception as e:
        logger.error(f"Webflow {job_name} trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# OpenStates scrape triggers
# ---------------------------------------------------------------------------

# Targets that map to a named job function.
_OPENSTATES_JOB_TARGETS = {"patches", "fl", "wa", "usa", "secondary", "people"}

# Individual secondary-state codes accepted as single-jurisdiction triggers.
_OPENSTATES_SINGLE_JURISDICTION = {"va", "mi", "ma", "ut", "az"}


@router.post("/trigger/openstates-scrape/{target}")
async def trigger_openstates_scrape(
    target: str,
    background_tasks: BackgroundTasks,
    token: str = Depends(api_key_auth),
):
    """Trigger an OpenStates scrape job immediately, without waiting for its cron.

    Returns 202 Accepted immediately; the job runs in the background and logs
    to ~/Developer/repos/ddp-open-states/logs/scraper.log plus ddp-sync's
    structured log. Flow status is written to Redis under ddp:flow:openstates_*.

    Targets:
        patches     — run apply-local-patches.sh (idempotent, ~30s)
        fl          — all FL sessions sequentially (2026, 2026D, 2026E, 2026F)
        wa          — WA scrape + import
        usa         — USA lower then upper sequentially
        secondary   — VA, MI, MA, UT, AZ concurrently
        people      — git pull people repo + os-people to-database for all states
        va|mi|ma|ut|az — single secondary-state scrape + import
    """
    from ddp_sync.pipelines.openstates_scrape import (
        run_patch_refresh_job,
        run_fl_scrapes_job,
        run_wa_scrape_job,
        run_usa_scrapes_job,
        run_secondary_scrapes_job,
        run_people_refresh_job,
        run_single_scrape_job,
    )
    from ddp_sync.scheduler import get_scheduler

    scheduler = get_scheduler()
    config = scheduler._sync_config.get("openstates_scrape", {}) if scheduler else {}

    if target in _OPENSTATES_JOB_TARGETS:
        job_map = {
            "patches": run_patch_refresh_job,
            "fl": run_fl_scrapes_job,
            "wa": run_wa_scrape_job,
            "usa": run_usa_scrapes_job,
            "secondary": run_secondary_scrapes_job,
            "people": run_people_refresh_job,
        }
        background_tasks.add_task(job_map[target], config)
        return {"status": "started", "target": target}

    if target in _OPENSTATES_SINGLE_JURISDICTION:
        background_tasks.add_task(run_single_scrape_job, target, config)
        return {"status": "started", "target": target}

    available = sorted(_OPENSTATES_JOB_TARGETS | _OPENSTATES_SINGLE_JURISDICTION)
    raise HTTPException(
        status_code=404,
        detail=f"Unknown target '{target}'. Available: {', '.join(available)}",
    )


# Jurisdictions with bill-document archiving enabled — same list as ddp-open-states's
# ARCHIVE_ENABLED_STATES (activate.sh) and openstates_archive.jurisdictions in
# sync_schedule.yaml. Keep in sync if that list ever changes.
_OPENSTATES_ARCHIVE_JURISDICTIONS = {"fl", "ut", "az", "wa", "va", "mi"}


@router.post("/trigger/openstates-archive/{target}")
async def trigger_openstates_archive(
    target: str,
    background_tasks: BackgroundTasks,
    token: str = Depends(api_key_auth),
):
    """Trigger OpenStates bill-document archiving immediately, without waiting for its cron.

    Split out from the scrape trigger (2026-07-31) — archiving is now fully independent of
    scraping (see openstates_archive in sync_schedule.yaml), so it gets its own trigger too.
    Returns 202 Accepted immediately; the job runs in the background and logs to
    ~/Developer/repos/ddp-open-states/logs/scraper.log plus ddp-sync's structured log. Flow
    status is written to Redis under ddp:flow:openstates_archive.

    Targets:
        all                        — every jurisdiction in ARCHIVE_ENABLED_STATES, concurrently
        fl|ut|az|wa|va|mi          — a single jurisdiction
    """
    from ddp_sync.pipelines.openstates_archive import run_archive_jobs, run_single_archive_job
    from ddp_sync.scheduler import get_scheduler

    scheduler = get_scheduler()
    config = scheduler._sync_config.get("openstates_archive", {}) if scheduler else {}

    if target == "all":
        background_tasks.add_task(run_archive_jobs, config)
        return {"status": "started", "target": target}

    if target in _OPENSTATES_ARCHIVE_JURISDICTIONS:
        background_tasks.add_task(run_single_archive_job, target, config)
        return {"status": "started", "target": target}

    available = sorted(_OPENSTATES_ARCHIVE_JURISDICTIONS | {"all"})
    raise HTTPException(
        status_code=404,
        detail=f"Unknown target '{target}'. Available: {', '.join(available)}",
    )
