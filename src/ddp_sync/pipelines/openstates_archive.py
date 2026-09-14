"""Pipeline that runs OpenStates bill-document archiving on its own schedule.

Split out of openstates_scrape.py (2026-07-31). Archiving to DDP-HOT used to
run as the last step inside run-scrape.sh, which meant the incremental cutoff
marker (logs/last-run/<key>.ts) didn't advance until archiving finished too.
That created a compounding failure mode: a run whose archive step ran long
or died left the cutoff stuck at its old value, so the next run had to treat
more bills as "changed since cutoff," making that run slower too and more
likely to also miss its own archive window — observed live 2026-07-30/31: a
WA run was still archiving 1h45m+ after scrape+import had already finished
cleanly.

run-scrape.sh no longer touches archiving at all (ddp-open-states
PLAN-open-states.md, incremental-scraping section). This module runs
run-archive.sh <state> for each ARCHIVE_ENABLED_STATE independently, on its
own cadence, with no relationship to when/whether a scrape ran. Safe to run
concurrently with a scrape for the same jurisdiction — os-text-extract's
natural-key skip check makes an already-archived version a cheap DB check,
not a re-fetch — or with any other scrape.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
import httpx
import requests
import structlog

from ddp_sync.config import SyncSettings, get_settings
from ddp_sync.pipelines.openstates_scrape import _run_with_group_kill
from ddp_sync.services import scrapebot_client
from ddp_sync.services.rds_credentials import resolve_rds_database_url

logger = structlog.get_logger()

DEFAULT_OPENSTATES_ROOT = "/Users/agentsmith/Developer/repos/ddp-open-states"

# Same jurisdiction list as ddp-open-states's ARCHIVE_ENABLED_STATES (activate.sh) and
# openstates_archive.jurisdictions in sync_schedule.yaml. Exported so callers (the manual
# trigger endpoint, run_archive_jobs' own default) share one definition instead of each
# hardcoding their own copy -- triggers.py's copy silently went stale when ma/al/us were
# added here 2026-08-10, exactly the class of bug a single shared constant prevents.
DEFAULT_ARCHIVE_JURISDICTIONS = ["fl", "ut", "az", "wa", "va", "mi", "ma", "al", "us"]

# Per-jurisdiction archive timeouts. Downloads + extracts every not-yet-captured
# document, so these scale with total historical bill count, not just what
# changed recently — sized generously, matching the scrape timeouts' shape.
# `us` has ~83k never-archived documents as of 2026-08-10 (vs. low
# thousands/tens-of-thousands for the state jurisdictions) — its first several
# weekly runs are a cold backfill, not steady-state, so it gets the longest
# runway here.
#
# SYNC-2: `mi` needs its own entry rather than falling through to `default`.
# MI's document backlog is ~8k docs at ~10s/doc via os-text-extract -- 20h+ for
# a healthy run -- so the 4h default kills a run that is making genuine
# progress every single day, not just on a bad day. Given `us`'s 24h for a
# comparable cold-backfill shape, mi gets the same runway rather than a
# number sized to one measurement that the next run then exceeds (the same
# trap openstates_scrape.py's SCRAPE_TIMEOUT_S comment describes for MA).
ARCHIVE_TIMEOUT_S: dict[str, int] = {
    "fl": 16 * 3600,
    "wa": 8 * 3600,
    "mi": 24 * 3600,
    "us": 24 * 3600,
    "default": 4 * 3600,
}


def _get_root(config: dict | None) -> str:
    return (config or {}).get("openstates_root", DEFAULT_OPENSTATES_ROOT)


def _alert_archive_failure(jurisdiction: str, error: str, duration_seconds: float) -> None:
    """Best-effort Slack + CAMS alert for an archive run that ddp-sync itself gave up on.

    Mirrors openstates_scrape.py's _alert_scrape_failure exactly (same channel/token
    convention, same CAMS payload shape) -- SYNC-2: the archive pipeline never got any of
    the scrape pipeline's operational hardening. run-archive.sh has its own ERR trap that
    posts to Slack #automation-errors and CAMS on an ordinary in-process failure, but that
    only fires from *inside* the script's own process. A ddp-sync group-kill on timeout, a
    signal delivered straight to run-archive.sh's own process, or an exception raised here
    before/while invoking the subprocess all happen outside that process entirely, so
    run-archive.sh never gets a chance to alert on any of them. Before this, all three were
    100% silent: logged at ERROR and written to a Redis flow-status key nothing surfaces.
    Real incident 2026-08-07: MI's 4h archive timeout killed the wrapper, its own alert
    never fired, and the orphaned os-text-extract archiver ran ~24h more with no one told.
    Never raises -- same convention as every other alerting call site in this codebase.
    """
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if token:
        channel = os.getenv("HEALTH_ALERT_SLACK_CHANNEL", "#automation-errors")
        text = (
            f":red_circle: *OpenStates archive failed: {jurisdiction}* — {error} "
            f"(after {duration_seconds:.0f}s) — check ddp-sync logs / os-text-extract logs"
        )
        try:
            resp = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {token}"},
                json={"channel": channel, "text": text},
                timeout=15,
            )
            if not (resp.ok and resp.json().get("ok")):
                logger.error("openstates_archive: Slack alert failed", response=resp.text[:200])
        except Exception as e:  # noqa: BLE001
            logger.error("openstates_archive: Slack alert error", error=str(e))
    else:
        logger.warning(
            "openstates_archive: SLACK_BOT_TOKEN not set — cannot alert on archive failure"
        )

    cams_token = os.getenv("CAMS_API_TOKEN", "")
    if cams_token:
        cams_url = os.getenv("CAMS_BASE_URL", "http://localhost:8000")
        payload = {
            "v": 1,
            "service": "ddp-sync",
            "error_type": "ArchiveTimeoutOrSubprocessError",
            "message": f"archive failed for {jurisdiction}: {error} (after {duration_seconds:.0f}s)",
            "metadata": {"jurisdiction": jurisdiction},
        }
        try:
            resp = requests.post(
                f"{cams_url}/api/v1/failures",
                headers={"Authorization": f"Bearer {cams_token}", "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10,
            )
            if not resp.ok:
                logger.error("openstates_archive: CAMS report failed", status=resp.status_code)
        except Exception as e:  # noqa: BLE001
            logger.error("openstates_archive: CAMS report error", error=str(e))


def _scrapebot_eligible(jurisdiction: str, config: dict | None) -> bool:
    """Is this jurisdiction opted into ScrapeBot cookie pre-seeding for archive runs?

    Config-gated per jurisdiction under openstates_archive.scrapebot_fallback in
    sync_schedule.yaml -- absent/disabled by default, so adding this never changes
    behavior for a jurisdiction that hasn't explicitly opted in. Same shape as the
    scrape pipeline's secondary.scrapebot_fallback block (openstates_scrape.py), but
    keyed inside this pipeline's own config subtree: run_archive_jobs() receives the
    openstates_archive: section directly, so there is no "secondary" level here.
    """
    fallback_cfg = (config or {}).get("scrapebot_fallback", {})
    if not fallback_cfg.get("enabled", False):
        return False
    return jurisdiction in fallback_cfg.get("jurisdictions", [])


async def _maybe_preseed_scrapebot_cookies(
    jurisdiction: str,
    config: dict | None,
    openstates_root: str,
) -> None:
    """Proactively mint fresh WAF-passing cookies via ScrapeBot before archiving a
    jurisdiction opted into scrapebot_fallback -- the same proactive pre-seed the
    scrape pipeline has done since 2026-08-04 (openstates_scrape.py,
    PLAN-scrapebot.md §3.7), which this pipeline never got when archiving was split
    out of run-scrape.sh on 2026-07-31.

    Without it, an archive run depends on whatever cookies the last scrape's
    pre-seed left in the cache file; once a mid-run WAF block invalidates that
    cache, CookieProvider falls back to its own local Playwright self-warm --
    observed 2026-08-07/08: a MI archive run ground through ~24h at a 30s warm-up
    timeout per document, with the WAF refusing to issue the required cookies to
    the local headless browser at all.

    Best-effort: a mint failure here must never block or fail the archive run that
    follows -- it just proceeds with whatever's already cached, or CookieProvider's
    own self-warm.
    """
    if not _scrapebot_eligible(jurisdiction, config):
        return
    try:
        mint_result = await scrapebot_client.dispatch_mint_cookies(jurisdiction)
        cache_path = scrapebot_client.cache_path_for(jurisdiction, openstates_root)
        scrapebot_client.write_cookie_cache(
            cache_path,
            cookies=mint_result["cookies"],
            user_agent=mint_result["user_agent"],
        )
        logger.info(
            "openstates_archive: ScrapeBot pre-seeded fresh cookies before archive",
            jurisdiction=jurisdiction,
            cache_path=cache_path,
        )
    except scrapebot_client.ScrapeBotDispatchError as e:
        logger.warning(
            "openstates_archive: ScrapeBot pre-seed mint failed, proceeding with "
            "existing/self-warmed cookies",
            jurisdiction=jurisdiction,
            error=str(e),
        )


async def _run_archive(
    jurisdiction: str,
    openstates_root: str,
    timeout_s: int | None = None,
    config: dict | None = None,
) -> dict[str, Any]:
    """Run run-archive.sh for one jurisdiction off the event loop.

    Pre-seeds ScrapeBot cookies first for any jurisdiction opted into
    scrapebot_fallback -- see _maybe_preseed_scrapebot_cookies()'s docstring. A
    no-op for every jurisdiction not opted in (config defaults to None, in which
    case it's always a no-op).

    SYNC-2: uses openstates_scrape's _run_with_group_kill rather than a bare
    subprocess.run(timeout=...). subprocess.run(timeout=...) only kills the direct
    child (run-archive.sh itself) on TimeoutExpired -- start_new_session=True makes
    the wrapper the leader of its own process group, but nothing then targets that
    group, so its grandchildren (os-text-extract archive <state> and its tee)
    survive, orphaned and unsupervised, in the detached session. Observed live
    2026-08-07: MI's archiver kept running headless for ~24h after its 4h wrapper
    timeout killed only the wrapper. _run_with_group_kill's timeout path instead
    killpg()s the whole group, reaching the grandchildren too. Only that half of
    the helper is used here -- progress_dir/stall_seconds are left unset, so no
    stall watchdog runs; OPEN-155's stall detection is out of this ticket's scope.
    """
    await _maybe_preseed_scrapebot_cookies(jurisdiction, config, openstates_root)

    script = os.path.join(openstates_root, "run-archive.sh")
    cmd = ["/bin/bash", script, jurisdiction]

    timeout = timeout_s or ARCHIVE_TIMEOUT_S.get(jurisdiction, ARCHIVE_TIMEOUT_S["default"])

    logger.info("openstates_archive: starting", jurisdiction=jurisdiction, timeout_s=timeout)

    start = time.monotonic()
    try:
        returncode, _stdout, stderr, timed_out, _stalled = await asyncio.to_thread(
            _run_with_group_kill, cmd, dict(os.environ), timeout
        )
        duration = round(time.monotonic() - start, 1)

        if timed_out:
            logger.error(
                "openstates_archive: timeout",
                jurisdiction=jurisdiction,
                timeout_s=timeout,
                duration_seconds=duration,
            )
            # Whole process group killed above, before run-archive.sh's own ERR trap ever got
            # a chance to run -- it never alerted on this one. We're the only ones who know it
            # happened, so we're the only ones who can alert.
            _alert_archive_failure(jurisdiction, f"timed out after {timeout}s", duration)
            return {
                "success": False,
                "error": "timeout",
                "jurisdiction": jurisdiction,
                "duration_seconds": duration,
            }

        if returncode != 0:
            stderr_tail = (stderr or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_archive: failed",
                jurisdiction=jurisdiction,
                returncode=returncode,
                stderr_tail=stderr_tail,
                duration_seconds=duration,
            )
            if returncode < 0:
                # Negative returncode = killed by a signal that didn't originate from our own
                # timeout handling above (OOM killer, an operator's `kill`, another supervisor).
                # run-archive.sh's own ERR trap only fires on an ordinary command failure inside
                # the script, not on the script's own process receiving a terminating signal --
                # so unlike a plain nonzero exit, this one was never self-alerted.
                _alert_archive_failure(jurisdiction, f"killed by signal {-returncode}", duration)
            # else (positive returncode): run-archive.sh's own ERR trap already fired its
            # Slack/CAMS alert from inside the process before exiting nonzero -- alerting again
            # here would double-page for the exact same failure.
            return {
                "success": False,
                "error": f"exit_code_{returncode}",
                "jurisdiction": jurisdiction,
                "duration_seconds": duration,
            }

        logger.info(
            "openstates_archive: done",
            jurisdiction=jurisdiction,
            duration_seconds=duration,
        )
        return {"success": True, "jurisdiction": jurisdiction, "duration_seconds": duration}

    except Exception as e:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "openstates_archive: subprocess error",
            jurisdiction=jurisdiction,
            error=str(e),
            duration_seconds=duration,
        )
        # Something failed before/while invoking the subprocess itself (e.g. the script or
        # openstates_root path doesn't exist) -- run-archive.sh never started, so it never had
        # a chance to alert either.
        _alert_archive_failure(jurisdiction, str(e), duration)
        return {
            "success": False,
            "error": str(e),
            "jurisdiction": jurisdiction,
            "duration_seconds": duration,
        }


def _launch_archive_fargate_task(
    jurisdiction: str,
    run_id: str,
    rds_url: str,
    fargate_cfg: dict,
    ecs_client,
) -> tuple[bool, str | None, str]:
    """Launch cloud_archiver.py for one jurisdiction as a Fargate task.

    Deliberately its own function rather than reusing cloud_scrape_trigger._launch_fargate_task
    -- that function's `environment` override is fixed to just RUN_ID, which is correct for
    cloud_collector.py (it reads its S3/DB config from the task definition itself) but not
    enough here: this container also needs RUNNER_SCRIPT (selects cloud_archiver.py over the
    image's default cloud_collector.py -- see docker-entrypoint.sh) and DATABASE_URL (RDS,
    cloud_archiver.py reads bill rows from Django/Postgres directly, unlike the scrape path).
    Changing the shared helper to carry archive-only environment keys risked the live scrape
    path picking up something it doesn't need; a second small function with its own environment
    list keeps the two genuinely independent.
    """
    container_name = fargate_cfg.get("container_name", "scraper")
    try:
        resp = ecs_client.run_task(
            cluster=fargate_cfg["cluster"],
            taskDefinition=fargate_cfg["task_definition"],
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": fargate_cfg["subnets"],
                    "securityGroups": fargate_cfg["security_groups"],
                    "assignPublicIp": fargate_cfg.get("assign_public_ip", "ENABLED"),
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": container_name,
                        "command": [jurisdiction],
                        "environment": [
                            {"name": "RUN_ID", "value": run_id},
                            {"name": "RUNNER_SCRIPT", "value": "cloud_archiver.py"},
                            {"name": "DATABASE_URL", "value": rds_url},
                            {
                                "name": "MEMORY_BUCKET",
                                "value": fargate_cfg.get(
                                    "memory_bucket", os.environ.get("MEMORY_BUCKET", "")
                                ),
                            },
                            {
                                "name": "MEMORY_PREFIX",
                                "value": fargate_cfg.get("memory_prefix", "prod"),
                            },
                        ],
                    }
                ]
            },
        )
    except Exception as e:  # noqa: BLE001 -- ClientError and friends, all "never started" alike
        return False, None, str(e)

    failures = resp.get("failures", [])
    tasks = resp.get("tasks", [])
    if failures or not tasks:
        detail = "; ".join(f.get("reason", "unknown") for f in failures) or "no task returned"
        return False, None, detail
    return True, tasks[0]["taskArn"], ""


async def _run_archive_fargate(
    jurisdiction: str,
    config: dict | None = None,
    ecs_client=None,
) -> dict[str, Any]:
    """OPEN-192 (reopened): run cloud_archiver.py for one jurisdiction as a real Fargate task,
    instead of run-archive.sh locally. Selected by run_archive_jobs()/run_single_archive_job()
    when openstates_archive.use_fargate is true; default is false, so enabling this file's own
    import changes no behavior until that flag is flipped.

    Deliberately skips _maybe_preseed_scrapebot_cookies -- that writes a WAF cookie to a LOCAL
    cache file path this process can reach, which a disposable Fargate task's own filesystem
    never sees. cloud_archiver.py already hydrates Michigan's WAF cookie itself, from the
    shared S3 memory store (see its own docstring) -- the two paths reach a fresh cookie by two
    different means, not one silently losing a capability the other has.

    Reuses cloud_scrape_trigger's config validation and task-completion wait -- both are
    generic over what the task actually runs, not scrape-specific -- rather than a second
    implementation of the same run_task-and-poll mechanics.
    """
    from ddp_sync.pipelines.cloud_scrape_trigger import _fargate_config, _wait_for_task_stop

    # OPEN-260: resolved live from Secrets Manager on every launch, not read from a cached env
    # var -- RDS's own automatic 7-day credential rotation goes stale under a cached value
    # regardless of how recently this process started (see rds_credentials.py's docstring for
    # the full incident this fixes, 2026-09-09).
    rds_url, rds_error = resolve_rds_database_url()
    if rds_error:
        error = f"cannot resolve an RDS target: {rds_error}"
        logger.error(
            "openstates_archive: fargate launch refused", jurisdiction=jurisdiction, error=error
        )
        return {
            "success": False,
            "error": error,
            "jurisdiction": jurisdiction,
            "duration_seconds": 0.0,
        }

    try:
        fargate_cfg = _fargate_config(config)
    except ValueError as e:
        logger.error(
            "openstates_archive: fargate config error", jurisdiction=jurisdiction, error=str(e)
        )
        return {
            "success": False,
            "error": f"config_error: {e}",
            "jurisdiction": jurisdiction,
            "duration_seconds": 0.0,
        }

    client = ecs_client or boto3.client("ecs")
    run_id = f"{jurisdiction}-archive-{uuid.uuid4().hex[:12]}"

    start = time.monotonic()
    started, task_arn, detail = _launch_archive_fargate_task(
        jurisdiction, run_id, rds_url, fargate_cfg, client
    )
    if not started:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "openstates_archive: fargate launch failed", jurisdiction=jurisdiction, detail=detail
        )
        _alert_archive_failure(jurisdiction, f"fargate run_task failed: {detail}", duration)
        return {
            "success": False,
            "error": f"run_task_failed: {detail}",
            "jurisdiction": jurisdiction,
            "duration_seconds": duration,
        }

    exit_code, wait_detail = await asyncio.to_thread(
        _wait_for_task_stop, task_arn, fargate_cfg, client
    )
    duration = round(time.monotonic() - start, 1)

    if exit_code != 0:
        error = "exit_code_none" if exit_code is None else f"exit_code_{exit_code}"
        logger.error(
            "openstates_archive: fargate task failed",
            jurisdiction=jurisdiction,
            exit_code=exit_code,
            detail=wait_detail,
            duration_seconds=duration,
        )
        _alert_archive_failure(jurisdiction, f"{error}: {wait_detail}", duration)
        return {
            "success": False,
            "error": error,
            "jurisdiction": jurisdiction,
            "duration_seconds": duration,
        }

    logger.info(
        "openstates_archive: fargate task done",
        jurisdiction=jurisdiction,
        duration_seconds=duration,
    )
    return {"success": True, "jurisdiction": jurisdiction, "duration_seconds": duration}


async def _write_flow_status(flow_key: str, status: dict) -> None:
    """Best-effort Redis flow_status write. Never raises."""
    try:
        from ddp_sync.services.redis_store import get_redis_store
        redis_store = get_redis_store()
        await redis_store.set_flow_status(flow_key, status)
    except Exception as e:
        logger.warning("openstates_archive: redis write failed", flow=flow_key, error=str(e))


_MAC_TRIGGER_TIMEOUT_SECONDS = 3600.0


def _mac_capable() -> bool:
    """SYNC-65 (real conflict found by the prod agent, 2026-09-13): does THIS process
    have real, local CAMS/LegBot access, or does it need to reach the Mac Studio over
    WireGuard instead?

    The first version of this hook assumed archiving only ever runs on the Mac's own
    ddp-sync instance -- true when written, false as of this fix: `OPENSTATES_ARCHIVE_
    ENABLED` is now also `true` on the EC2-broker instance (OPEN-192's `us` cutover),
    which has no local CAMS server at all (`CAMS_BASE_URL` unset there, confirmed by
    the prod agent directly). `cams_api_token` being configured is the right signal --
    it's the one thing that's true on the Mac and false everywhere else, unlike
    `OPENSTATES_ARCHIVE_ENABLED` itself, which is now true on both.
    """
    return bool(get_settings().cams_api_token)


async def _trigger_legbot_session_via_mac_wireguard(
    jurisdiction_iso2: str,
    session_code: str,
    settings: SyncSettings,
) -> None:
    """SYNC-65: the EC2-side counterpart to calling `trigger_scraper_session_pipeline`
    in-process -- reaches the Mac Studio's own ddp-sync over the existing WireGuard
    mesh instead, the same live pattern SYNC-59 built (and this same ticket's first
    version removed, on the mistaken assumption it was now dead). `MAC_DDP_SYNC_BASE_
    URL`/`MAC_DDP_SYNC_API_KEY` were left wired and verified working specifically for
    this contingency -- see the prod agent's own note, `notes/open285-real-status-and-
    sync65-conflict-20260913.md`.

    OPEN-290: posts to /trigger/bill-artifact-generation, not the removed
    /trigger/scraper-session-legbot -- that endpoint now shares the same
    overlap lock (trigger_scraper_session_pipeline, require_trigger_enabled=
    False) that this WireGuard hop itself relies on, so the two are no
    longer distinguishable at the HTTP layer. Every cost-relevant dispatch
    parameter is still resolved from THIS (the EC2 caller's own) instance's
    settings.legbot_scrape_completion_trigger_* values, exactly as before --
    bill-artifact-generation's request body just makes that explicit instead
    of the old endpoint resolving them itself on the Mac side.

    Never raises -- same log-and-continue contract as the in-process branch; a
    failure here must never affect the archive job's own already-successful result.
    """
    if not settings.mac_ddp_sync_base_url:
        logger.warning(
            "archiver_triggered_legbot_no_mac_target_configured",
            jurisdiction=jurisdiction_iso2,
            session_code=session_code,
        )
        return
    if not settings.mac_ddp_sync_api_key:
        # pm-review: catch this before ever sending a request that's guaranteed to
        # come back 401 -- a base URL with no key is a real, distinguishable
        # misconfiguration, not the same "nothing configured at all" case above.
        logger.warning(
            "archiver_triggered_legbot_no_mac_api_key_configured",
            jurisdiction=jurisdiction_iso2,
            session_code=session_code,
        )
        return

    headers = {
        "Authorization": f"Bearer {settings.mac_ddp_sync_api_key}",
        "X-DDP-Environment": "prod",
    }
    url = f"{settings.mac_ddp_sync_base_url.rstrip('/')}/ddp-sync/v1/trigger/bill-artifact-generation"

    # pm-review: the whole request/response cycle, INCLUDING interpreting the
    # response body, lives inside this one try -- the first version's `result.get(
    # "success")` sat outside the try block, so a 2xx response whose body wasn't a
    # JSON object (null, a list, a bare string) would raise `AttributeError` past
    # this function's own documented never-raise contract, aborting every session
    # still left in the caller's loop, not just this one.
    try:
        async with httpx.AsyncClient(timeout=_MAC_TRIGGER_TIMEOUT_SECONDS) as client:
            resp = await client.post(
                url,
                headers=headers,
                json={
                    "jurisdiction_iso2": jurisdiction_iso2,
                    "session_code": session_code,
                    "artifact_types": settings.legbot_scrape_completion_trigger_artifact_types,
                    "include_org_research": False,  # Gate 1 item 4, PLAN-legbot.md §32: a
                    # deliberate operator decision, not a tunable default.
                    "include_concept_statements": (
                        settings.legbot_scrape_completion_trigger_include_concept_statements
                    ),
                    "limit": settings.legbot_scrape_completion_trigger_limit,
                    "retry_failed": False,  # SYNC-42: this automated path never retries.
                    "dry_run": False,
                },
            )
            resp.raise_for_status()
            result = resp.json()
        success = bool(isinstance(result, dict) and result.get("success"))
    except Exception as e:  # noqa: BLE001 -- must never affect the archive job's own result
        logger.error(
            "archiver_triggered_legbot_wireguard_trigger_failed",
            jurisdiction=jurisdiction_iso2,
            session_code=session_code,
            error=str(e),
        )
        return

    log_fn = logger.info if success else logger.warning
    log_fn(
        "archiver_triggered_legbot_wireguard_result",
        jurisdiction=jurisdiction_iso2,
        session_code=session_code,
        result=result,
    )


async def _maybe_trigger_legbot_for_archive(
    jurisdiction: str,
    archive_started_at: datetime,
) -> None:
    """SYNC-65: the real archive-completion hook, replacing the scrape-completion
    hooks SYNC-50/SYNC-59 built (`_maybe_trigger_legbot_for_scrape` /
    `_maybe_trigger_legbot_for_cloud_scrape`, both removed by this same ticket).

    Root cause this exists to fix: `_resolve_bill_source()`
    (`bill_artifact_generation.py`) only ever reads already-archived,
    already-extracted text (`get_archived_bill_text`, OPEN-13) -- it has no
    live-fetch fallback. Archiving runs on its own separate, per-jurisdiction
    weekly schedule with no connection to the scrape schedule, so triggering
    off scrape completion routinely lost the race against archiving and
    produced a permanent `status="failed", failure_reason="no_archived_bill_text"`
    (`session_pipeline_runner.py` only retries a failed row when the caller
    passes `retry_failed=True`, which this automated path deliberately never
    does -- SYNC-42). Triggering off archive completion instead removes the
    race outright: the text this hook depends on already exists by construction
    by the time it fires.

    Called only after an archive run has already succeeded -- a failure here
    must never affect the archive job's own reported outcome, same never-raise,
    log-and-continue contract as SYNC-50's original hook.

    **Mac-vs-EC2 split (added after the prod agent caught a real conflict,
    2026-09-13):** this function's first version assumed archiving only ever runs
    on the Mac's own ddp-sync instance and always dispatched in-process. That's no
    longer true -- `OPENSTATES_ARCHIVE_ENABLED` is now also `true` on the EC2-broker
    instance (OPEN-192), which has no local CAMS server. `_mac_capable()` decides
    which of two paths this run takes:

    - Mac-capable: session resolution reads the Mac's own local api-v3
      (`local_openstates_api_base`), dispatch calls `trigger_scraper_session_pipeline`
      in-process. Unchanged from this ticket's first version.
    - Not Mac-capable (EC2): session resolution reads `rds_openstates_api_base`
      instead (the Mac's local replica reflects RDS-origin data via OPEN-280, but
      only the MAC can reach `local_openstates_api_base` at all -- from the EC2 host
      that address resolves to nothing), and dispatch reaches the Mac Studio's own
      ddp-sync over WireGuard (`_trigger_legbot_session_via_mac_wireguard`), mirroring
      SYNC-59's now-removed cloud-scrape hook.

    Uses `since_param="document_updated_since"` (api-v3 PR #11) in both cases, NOT the
    default `updated_since` SYNC-50's scrape hook used -- pm-review on this ticket's
    first version caught a real gap: `archive_bill_versions()` (openstates-core) never
    touches `Bill.updated_at`, only the `BillVersionDocument` row's own `updated_at`
    (confirmed by reading the actual archiver code, not assumed). Reusing
    `updated_since` here would have silently resolved zero sessions on every real
    archive run.
    """
    settings = get_settings()
    if not settings.legbot_scrape_completion_trigger_enabled:
        return

    mac_capable = _mac_capable()

    from ddp_sync.services.local_openstates_client import resolve_touched_sessions

    resolution_kwargs: dict = {}
    if not mac_capable:
        if not settings.rds_openstates_api_base:
            logger.warning(
                "archiver_triggered_legbot_rds_read_path_not_configured",
                jurisdiction=jurisdiction,
            )
            return
        resolution_kwargs = {
            "api_base": settings.rds_openstates_api_base,
            "api_key": settings.rds_openstates_api_key,
        }

    try:
        session_codes = await resolve_touched_sessions(
            jurisdiction.upper(),
            since=archive_started_at,
            max_bills_scanned=settings.legbot_scrape_completion_trigger_resolution_max_bills,
            since_param="document_updated_since",
            **resolution_kwargs,
        )
    except Exception as e:  # noqa: BLE001 -- must never affect the archive job's own result
        logger.error(
            "archiver_triggered_legbot_session_resolution_failed",
            jurisdiction=jurisdiction,
            error=str(e),
        )
        return

    if not session_codes:
        logger.info(
            "archiver_triggered_legbot_no_sessions_touched",
            jurisdiction=jurisdiction,
            since=archive_started_at.isoformat(),
        )
        return

    if not mac_capable:
        for session_code in session_codes:
            await _trigger_legbot_session_via_mac_wireguard(
                jurisdiction.upper(), session_code, settings
            )
        return

    from ddp_sync.pipelines.scraper_triggered_legbot import trigger_scraper_session_pipeline

    for session_code in session_codes:
        try:
            result = await trigger_scraper_session_pipeline(
                jurisdiction.upper(),
                session_code,
                settings.legbot_scrape_completion_trigger_artifact_types,
                False,
                settings.legbot_scrape_completion_trigger_limit,
                include_concept_statements=(
                    settings.legbot_scrape_completion_trigger_include_concept_statements
                ),
            )
        except Exception as e:  # noqa: BLE001
            logger.error(
                "archiver_triggered_legbot_trigger_failed",
                jurisdiction=jurisdiction,
                session_code=session_code,
                error=str(e),
            )
            continue
        logger.info(
            "archiver_triggered_legbot_result",
            jurisdiction=jurisdiction,
            session_code=session_code,
            result=result,
        )


async def _run_archive_with_hook(
    jurisdiction: str,
    openstates_root: str | None = None,
    config: dict | None = None,
) -> dict[str, Any]:
    """Thin wrapper adding the archive-completion LegBot hook (SYNC-65) uniformly
    over both archive branches (local run-archive.sh and Fargate cloud_archiver.py)
    -- neither run_archive_jobs() nor run_single_archive_job() needs its own copy.

    `archive_started_at` is captured here, before the archive itself runs, as a
    real wall-clock floor for `resolve_touched_sessions`' own `since` filter --
    same slack-tolerant contract SYNC-50's scrape-side capture already
    established (a few seconds early/late only widens or narrows the window
    slightly, never produces a wrong session).

    The whole post-success block is wrapped in one catch-all, not just the hook
    call, mirroring `_run_scrape`'s own fix for the same pm-review finding
    (SYNC-59 round 1): this wrapper's contract is "nothing after a successful
    archive may change that archive's result," not "nothing we currently believe
    can raise."
    """
    archive_started_at = datetime.now(timezone.utc)
    if (config or {}).get("use_fargate", False):
        result = await _run_archive_fargate(jurisdiction, config=config)
    else:
        result = await _run_archive(jurisdiction, openstates_root or _get_root(config), config=config)

    if result.get("success"):
        try:
            await _maybe_trigger_legbot_for_archive(jurisdiction, archive_started_at)
        except Exception as e:  # noqa: BLE001 -- must never affect the archive job's own result
            logger.error(
                "archiver_triggered_legbot_hook_failed",
                jurisdiction=jurisdiction,
                error=str(e),
            )
    return result


async def run_archive_jobs(config: dict | None = None) -> dict[str, Any]:
    """Archive every jurisdiction in ARCHIVE_ENABLED_STATES concurrently.

    Independent of the scrape schedule entirely — each jurisdiction's own
    natural-key skip check makes this safe to run at any cadence, on any
    subset of jurisdictions, without coordinating with when that
    jurisdiction's own scrape last ran.

    OPEN-192 (reopened): `use_fargate: true` in config switches the whole batch to
    _run_archive_fargate instead of the local run-archive.sh wrapper -- one flag for the
    entire batch, not per-jurisdiction, since a mixed batch would need its own reasoning
    about which jurisdictions are safe to run where, and nothing here needs that yet.
    """
    jurisdictions: list[str] = (config or {}).get(
        "jurisdictions", DEFAULT_ARCHIVE_JURISDICTIONS
    )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_archive: starting batch", jurisdictions=jurisdictions)

    openstates_root = None if (config or {}).get("use_fargate", False) else _get_root(config)
    results: list[dict[str, Any]] = await asyncio.gather(
        *[
            _run_archive_with_hook(j, openstates_root, config=config)
            for j in jurisdictions
        ]
    )

    duration = round(time.monotonic() - t, 1)
    failed = [r for r in results if not r["success"]]

    log_fn = logger.error if failed else logger.info
    log_fn(
        "openstates_archive: batch completed",
        jurisdictions=jurisdictions,
        total=len(results),
        failed=len(failed),
        duration_seconds=duration,
    )

    await _write_flow_status("openstates_archive", {
        "flow": "openstates_archive",
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if not failed else "completed_with_errors",
        "jurisdictions": jurisdictions,
        "total": len(results),
        "failed": len(failed),
        "results": list(results),
        "duration_seconds": duration,
    })
    return {
        "success": not failed,
        "jurisdictions": jurisdictions,
        "results": list(results),
        "failed": len(failed),
        "duration_seconds": duration,
    }


async def run_single_archive_job(
    jurisdiction: str,
    config: dict | None = None,
) -> dict[str, Any]:
    """Archive a single arbitrary jurisdiction. Used by the manual trigger endpoint.

    Same `use_fargate` dispatch (and same archive-completion LegBot hook) as
    run_archive_jobs() -- see _run_archive_with_hook's docstring.
    """
    return await _run_archive_with_hook(jurisdiction, config=config)
