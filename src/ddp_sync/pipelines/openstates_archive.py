"""Pipeline that runs OpenStates bill-document archiving.

SYNC-153: a state is archived only right after its own scrape (`archive_with_retry`, called once by
`run_state_pipeline` after all the state's sessions) or by the manual archive trigger (`run_archive_only`), both under
the state's pipeline lock, with a bounded retry and an alert. There is no recurring archive schedule any more (the
weekly per-state days are gone) and no yaml list: the manifest's `archive.enabled` says who is archived.

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
PLAN-open-states.md, incremental-scraping section). Safe to run
concurrently with a scrape for the same jurisdiction — os-text-extract's
natural-key skip check makes an already-archived version a cheap DB check,
not a re-fetch — or with any other scrape.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
import httpx
import requests
import structlog

from ddp_sync.config import SyncSettings, get_settings
from ddp_sync.pipelines import inflight_fargate_jobs
from ddp_sync.pipelines.openstates_scrape import _get_root as _scrape_root
from ddp_sync.pipelines.openstates_scrape import _run_with_group_kill
from ddp_sync.services.jurisdiction_enrollment import enrolled, is_enrolled
from ddp_sync.services import scrapebot_client
from ddp_sync.services.rds_credentials import resolve_rds_database_url
from ddp_sync.slack_alerts import post_alert

logger = structlog.get_logger()

DEFAULT_OPENSTATES_ROOT = "/Users/agentsmith/Developer/repos/ddp-open-states"

# OPEN-291: the scrape side's federal job is "usa" (openstates_scrape.timing.usa in
# sync_schedule.yaml); the archive side's is "us" (matching OpenStates' own jurisdiction
# code). Every other jurisdiction uses the same
# code on both sides. Kept as an explicit map rather than assumed identical strings --
# archive_with_retry() below is the one place this actually matters.
_SCRAPE_TO_ARCHIVE_JURISDICTION = {"usa": "us"}

# SYNC-153: a state's archive runs only after its own scrape (or a manual archive trigger), under the state's pipeline lock
# (services/redis_store.state_lock). A failed archive is retried after these delays, then alerted. Plan open question 3
# proposed two retries, 30 min and 2 h after a failure; built with those values, not yet confirmed by Ramon.
ARCHIVE_RETRY_DELAYS_S: tuple[int, ...] = (1800, 7200)
# SYNC-192: which retry attempt (1-based) is running, set by `_archive_with_retry` so `_run_archive_fargate` can put it in the
# in-flight record without a new parameter on the three functions between them.
_archive_attempt: contextvars.ContextVar[int] = contextvars.ContextVar("archive_attempt", default=1)

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
# SYNC-157: a state's own ceiling is the manifest's `archive.timeout_s` (fl 16h, wa 8h, mi 24h -- SYNC-2 --, us 24h on
# 2026-11-12); this module's copy (ARCHIVE_TIMEOUT_S) is deleted. A state the manifest cannot answer for gets the default.
DEFAULT_ARCHIVE_TIMEOUT_S = 4 * 3600


def archive_timeout_s(jurisdiction: str, config: dict | None = None) -> int:
    """The wall-clock ceiling of one archive run: the manifest's `archive.timeout_s`, else DEFAULT_ARCHIVE_TIMEOUT_S."""
    from ddp_sync.services.jurisdiction_enrollment import manifest_value

    value = manifest_value(jurisdiction, ("archive", "timeout_s"), _scrape_root(config))
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else DEFAULT_ARCHIVE_TIMEOUT_S


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
    text = (
        f":red_circle: *OpenStates archive failed: {jurisdiction}* — {error} "
        f"(after {duration_seconds:.0f}s) — check ddp-sync logs / os-text-extract logs"
    )
    post_alert(text, source="openstates_archive.archive_failure")

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
    scrape pipeline's openstates_scrape.scrapebot_fallback block (openstates_scrape.py), but
    keyed inside this pipeline's own config subtree: the archive functions receive the
    openstates_archive: section directly.
    """
    fallback_cfg = (config or {}).get("scrapebot_fallback", {})
    if not fallback_cfg.get("enabled", False):
        return False
    # SYNC-157: WHO is the manifest's waf.scrapebot_fallback (the same field the scrape reads)
    return is_enrolled("scrapebot_fallback_archive", jurisdiction, _scrape_root(config))


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

    timeout = timeout_s or archive_timeout_s(jurisdiction, config)

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
            await asyncio.to_thread(_alert_archive_failure, jurisdiction, f"timed out after {timeout}s", duration)
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
                await asyncio.to_thread(_alert_archive_failure, jurisdiction, f"killed by signal {-returncode}", duration)
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
        await asyncio.to_thread(_alert_archive_failure, jurisdiction, str(e), duration)
        return {
            "success": False,
            "error": str(e),
            "jurisdiction": jurisdiction,
            "duration_seconds": duration,
        }


_DSN_RE = re.compile(r"postgres(?:ql)?://\S+")


def redact_dsn(text: str) -> str:
    """OPEN-633: cut any connection string out of a message before it is logged or alerted. A launch error from ECS
    could echo the task's overrides, and until `rds_from_task_secret` is on those carry DATABASE_URL, password
    included."""
    return _DSN_RE.sub("postgresql://<redacted>", text or "")


def _launch_archive_fargate_task(
    jurisdiction: str,
    run_id: str,
    rds_url: str | None,
    fargate_cfg: dict,
    ecs_client,
    *,
    runner: str = "cloud_archiver.py",
    command: list[str] | None = None,
) -> tuple[bool, str | None, str]:
    """Launch cloud_archiver.py for one jurisdiction as a Fargate task.

    OPEN-633: also launches the cloud load (`runner="cloud_loader.py"`, `command=[state, run_id, session args...]`),
    so there is no fourth copy of `run_task`. `rds_url=None` passes no DATABASE_URL at all: the task builds it from
    its task definition's RDS secret (`cloud_path.fargate.rds_from_task_secret`; ddp-open-states
    `apply_rds_task_secret`). A launch failure's detail has any connection string cut out (`redact_dsn`).

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
    # OPEN-639: the load and archive tasks run from their own task definition, the only one that may carry the database
    # login; collection and probe tasks (`task_definition`) never get it. No fallback to `task_definition`: a missing
    # setting refuses the launch rather than running a database job on the collection definition.
    task_definition = fargate_cfg.get("task_definition_db")
    if not task_definition:
        return False, None, "cloud_path.fargate.task_definition_db is not set (OPEN-639: load and archive tasks run only on the database task definition)"
    container_name = fargate_cfg.get("container_name", "scraper")
    try:
        resp = ecs_client.run_task(
            cluster=fargate_cfg["cluster"],
            taskDefinition=task_definition,
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
                        "command": command or [jurisdiction],
                        "environment": [
                            {"name": "RUN_ID", "value": run_id},
                            {"name": "RUNNER_SCRIPT", "value": runner},
                            *([{"name": "DATABASE_URL", "value": rds_url}] if rds_url else []),
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
        return False, None, redact_dsn(str(e))

    failures = resp.get("failures", [])
    tasks = resp.get("tasks", [])
    if failures or not tasks:
        detail = "; ".join(f.get("reason", "unknown") for f in failures) or "no task returned"
        return False, None, redact_dsn(detail)
    return True, tasks[0]["taskArn"], ""


async def _run_archive_fargate(
    jurisdiction: str,
    config: dict | None = None,
    ecs_client=None,
    archive_started_at: datetime | None = None,
) -> dict[str, Any]:
    """OPEN-192 (reopened): run cloud_archiver.py for one jurisdiction as a real Fargate task,
    instead of run-archive.sh locally. Selected by run_single_archive_job()
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

    SYNC-192: once the task has launched its ARN goes into the in-flight record (`record_archive_started`), so a
    restarted ddp-sync waits for it (`resume_inflight_archive_job`). A success returns `archive_run_id` and LEAVES the
    record: `_run_archive_followups` clears it when the follow-ups are done. A failed task clears it here (its retry and
    alert are the caller's, as before). A cancelled wait (ddp-sync stopping) clears nothing: that is the record's purpose.
    """
    from ddp_sync.pipelines.cloud_scrape_trigger import _fargate_config, _wait_for_task_stop

    # OPEN-260: resolved live from Secrets Manager on every launch, not read from a cached env
    # var -- RDS's own automatic 7-day credential rotation goes stale under a cached value
    # regardless of how recently this process started (see rds_credentials.py's docstring for
    # the full incident this fixes, 2026-09-09).
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

    # OPEN-633 (D2): once the task definition carries the RDS secret, the task builds its own DATABASE_URL and none
    # is passed here (so the password is no longer in ECS's task start record).
    rds_url = None
    if not fargate_cfg.get("rds_from_task_secret"):
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
        await asyncio.to_thread(_alert_archive_failure, jurisdiction, f"fargate run_task failed: {detail}", duration)
        return {
            "success": False,
            "error": f"run_task_failed: {detail}",
            "jurisdiction": jurisdiction,
            "duration_seconds": duration,
        }

    # Between run_task above and this write a restart leaves a running task nobody waits for (as the load's gap, OPEN-633):
    # its follow-ups do not run, and the state's next archive meets the task's own `_archive_lock` and is retried.
    inflight_fargate_jobs.record_archive_started(
        run_id, jurisdiction, task_arn, fargate_cfg,
        (archive_started_at or datetime.now(timezone.utc)).isoformat(), _archive_attempt.get(),
    )
    # SYNC-167: cloud_archiver.py holds `_archive_lock` (holder = this run_id); a stop at max_wait releases it.
    exit_code, wait_detail = await asyncio.to_thread(
        _wait_for_task_stop, task_arn, fargate_cfg, client, run_lock=(jurisdiction, run_id, "_archive_lock")
    )
    duration = round(time.monotonic() - start, 1)

    if exit_code != 0:
        inflight_fargate_jobs.clear(run_id)
        error = "exit_code_none" if exit_code is None else f"exit_code_{exit_code}"
        logger.error(
            "openstates_archive: fargate task failed",
            jurisdiction=jurisdiction,
            exit_code=exit_code,
            detail=wait_detail,
            duration_seconds=duration,
        )
        await asyncio.to_thread(_alert_archive_failure, jurisdiction, f"{error}: {wait_detail}", duration)
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
    return {"success": True, "jurisdiction": jurisdiction, "duration_seconds": duration, "archive_run_id": run_id}


async def _write_flow_status(flow_key: str, status: dict) -> None:
    """Best-effort Redis flow_status write. Never raises."""
    try:
        from ddp_sync.services.redis_store import get_redis_store
        redis_store = get_redis_store()
        await redis_store.set_flow_status(flow_key, status)
    except Exception as e:
        logger.warning("openstates_archive: redis write failed", flow=flow_key, error=str(e))


# SYNC-162: the POST is answered at once (202 and a run id) by a Mac that knows `background`; an older Mac still answers
# only when the run ends, which is what the POST's own timeout is for. The run itself is then checked every
# _MAC_RUN_POLL_SECONDS, for at most _MAC_RUN_MAX_WAIT_SECONDS (Massachusetts' 11-15 pass took 88 min); the Mac counts as
# unreachable after _MAC_RUN_POLL_FAILURES_ALLOWED checks in a row fail.
_MAC_TRIGGER_TIMEOUT_SECONDS = 3600.0
_MAC_RUN_POLL_SECONDS = 60.0
_MAC_RUN_MAX_WAIT_SECONDS = 6 * 3600.0
_MAC_RUN_POLL_FAILURES_ALLOWED = 10
# refusals that are not a failure of LegBot: logged as a warning, no alert (another run of the session is going, or an
# operator paused the automated path on the Mac)
_MAC_RUN_QUIET_ERRORS = ("already_running", "trigger_disabled")


class _MacRunFailed(Exception):
    """The Mac run did not end well, or could not be followed; the message says why."""


def _describe(exc: BaseException) -> str:
    """An exception as text that is never empty (httpx's timeouts have an empty str(), which is how 11-15 logged
    `error=` with nothing after it)."""
    return f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__


async def _wait_for_mac_run(client: httpx.AsyncClient, url: str, headers: dict, run_id: str) -> dict:
    """Check GET <url>/<run_id> until the run's record is final (completed / failed / lost) and return it. Raises
    _MacRunFailed when it is still running after _MAC_RUN_MAX_WAIT_SECONDS, or after _MAC_RUN_POLL_FAILURES_ALLOWED
    checks in a row fail (no answer, an error status, or no record)."""
    waited = 0.0
    failures = 0
    last_problem = ""
    while waited < _MAC_RUN_MAX_WAIT_SECONDS:
        await asyncio.sleep(_MAC_RUN_POLL_SECONDS)
        waited += _MAC_RUN_POLL_SECONDS
        try:
            resp = await client.get(f"{url}/{run_id}", headers=headers, timeout=30.0)
            resp.raise_for_status()
            record = resp.json()
            if not isinstance(record, dict):
                raise ValueError(f"not a run record: {record!r}"[:200])
        except Exception as e:  # counted; a run is not given up on for one failed check
            failures += 1
            last_problem = _describe(e)
            if failures >= _MAC_RUN_POLL_FAILURES_ALLOWED:
                raise _MacRunFailed(f"the Mac did not answer {failures} checks in a row on run {run_id} "
                                    f"(last: {last_problem})") from e
            continue
        failures = 0
        if record.get("status") in ("completed", "failed", "lost"):
            return record
    raise _MacRunFailed(f"run {run_id} still not finished after {_MAC_RUN_MAX_WAIT_SECONDS / 3600:g} h")


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
    overlap lock (trigger_scraper_session_pipeline) that this WireGuard hop
    relied on before. The X-DDP-Automated-Trigger header (below) is what
    still identifies this specific call as the automated one, so the Mac's
    own LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED still gates it exactly like
    before -- a plain manual caller (the endpoint's original, only use case)
    omits that header and is never subject to that flag, same as always.
    Every cost-relevant dispatch parameter is still resolved from THIS (the
    EC2 caller's own) instance's settings.legbot_scrape_completion_trigger_*
    values, exactly as before -- bill-artifact-generation's request body
    just makes that explicit instead of the old endpoint resolving them
    itself on the Mac side. **This does mean EC2's own copy of those three
    settings must be configured to match the Mac's** (previously only the
    boolean enabled flag needed to agree across hosts; the dispatch-shape
    values were resolved purely on the Mac and EC2 never needed them) --
    flagged to the prod agent as a pre-deploy check, not something this
    code can verify for itself.

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
        # pm-review (OPEN-290): identifies this call to the now-shared endpoint as
        # the automated caller, so the Mac's own LEGBOT_SCRAPE_COMPLETION_TRIGGER_
        # ENABLED still gates it independently of this (EC2) host's own copy of that
        # flag -- restores the Mac-side kill switch the removed, automated-only
        # /trigger/scraper-session-legbot used to provide.
        "X-DDP-Automated-Trigger": "true",
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
                    "background": True,  # SYNC-162: answered at once; the run is followed below
                },
            )
            resp.raise_for_status()
            result = resp.json()
            if resp.status_code == 202:
                run_id = result.get("run_id") if isinstance(result, dict) else None
                if not run_id:
                    raise _MacRunFailed(f"the Mac accepted the run but sent no run_id: {result!r}"[:300])
                logger.info("archiver_triggered_legbot_wireguard_accepted", jurisdiction=jurisdiction_iso2,
                            session_code=session_code, run_id=run_id)
                record = await _wait_for_mac_run(client, url, headers, run_id)
                result = {**(record.get("result") or {}), "run_id": run_id, "status": record.get("status"),
                          "error": record.get("error")}
                result["success"] = record.get("status") == "completed"
        success = bool(isinstance(result, dict) and result.get("success"))
    except Exception as e:  # noqa: BLE001 -- must never affect the archive job's own result
        await _wireguard_trigger_failed(jurisdiction_iso2, session_code, _describe(e))
        return

    error = result.get("error") if isinstance(result, dict) else None
    if success:
        logger.info("archiver_triggered_legbot_wireguard_result", jurisdiction=jurisdiction_iso2,
                    session_code=session_code, result=result)
    elif error in _MAC_RUN_QUIET_ERRORS:
        logger.warning("archiver_triggered_legbot_wireguard_result", jurisdiction=jurisdiction_iso2,
                       session_code=session_code, result=result)
    else:
        await _wireguard_trigger_failed(jurisdiction_iso2, session_code,
                                        f"the Mac's LegBot run ended without success: {error or result!r}"[:500])


async def _wireguard_trigger_failed(jurisdiction_iso2: str, session_code: str, error: str) -> None:
    """A real failure of the EC2 -> Mac LegBot run (Mac unreachable, the run failed or was lost, or it never finished):
    one error line and one operator alert (SYNC-162). Never raises."""
    logger.error("archiver_triggered_legbot_wireguard_trigger_failed", jurisdiction=jurisdiction_iso2,
                 session_code=session_code, error=error)
    try:
        await asyncio.to_thread(
            post_alert,
            f":warning: LegBot after the {jurisdiction_iso2} archive (session {session_code}) did not complete: {error}",
            source="openstates_archive.legbot_trigger",
            dedupe_key=f"legbot-trigger-{jurisdiction_iso2}-{session_code}",
        )
    except Exception as e:  # noqa: BLE001 -- the alert must not hide the outcome
        logger.error("archiver_triggered_legbot_alert_failed", jurisdiction=jurisdiction_iso2, error=str(e))


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

    # SYNC-186: no hand-off at all for a state whose manifest `legbot_replica` is false (ND and KY started real runs on
    # 2026-11-22 that only the per-bill check stopped). The same reader as that per-bill check (SYNC-142), no second list;
    # an unreadable manifest answers "none" there, so nothing is handed off either.
    from ddp_sync.pipelines.session_pipeline_runner import _legbot_allowed_codes

    if jurisdiction.upper() not in _legbot_allowed_codes(settings):
        logger.info("archiver_triggered_legbot_skipped_legbot_replica_off", jurisdiction=jurisdiction.upper())
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
        # resolve_touched_sessions itself never raises (see its own docstring) -- this
        # remains a defense-in-depth backstop for the unexpected, not the path SYNC-66
        # actually hit. That one comes back as session_codes is None below.
        logger.error(
            "archiver_triggered_legbot_session_resolution_failed",
            jurisdiction=jurisdiction,
            error=str(e),
        )
        return

    if session_codes is None:
        # SYNC-66: a real production connection hiccup made resolve_touched_sessions
        # return the same empty result as "genuinely nothing touched," at ERROR-worthy
        # INFO severity, and a whole archive run's new content never reached LegBot.
        # None here means resolution itself failed -- log it as the real failure it is,
        # distinct from the "nothing to do" case right below.
        logger.error(
            "archiver_triggered_legbot_session_resolution_failed",
            jurisdiction=jurisdiction,
            since=archive_started_at.isoformat(),
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


def _knowledge_base_embedding_eligible(jurisdiction: str, config: dict | None) -> bool:
    """OPEN-124's rule: which jurisdictions are enrolled is scheduler policy and lives in
    sync_schedule.yaml (`openstates_archive.knowledge_base_embedding`), read by a small
    `_*_eligible()` helper -- never a $STATE test in a script. Same shape as `_scrapebot_eligible`."""
    block = (config or {}).get("knowledge_base_embedding") or {}
    if not block.get("enabled", False):
        return False
    # SYNC-157: WHO is the manifest's enrollment.embedding; the yaml keeps only the global switch.
    return jurisdiction.lower() in (enrolled("embedding", None, _scrape_root(config)) or [])


def _reconcile_max_bills(config: dict | None) -> int:
    """SYNC-95: `knowledge_base_embedding.reconcile.max_bills_per_run`. 0, absent or anything that is
    not a non-negative whole number means the reconcile pass is off: a yaml typo must not spend money."""
    block = ((config or {}).get("knowledge_base_embedding") or {}).get("reconcile") or {}
    value = block.get("max_bills_per_run", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        logger.warning("knowledge_base_reconcile_bad_cap", value=value)
        return 0
    return value


def _ledger_max_bills(config: dict | None) -> int:
    """SYNC-95: `knowledge_base_embedding.ledger.max_bills_per_run` (> 0 switches the hook to the api-v3 ledger
    reconcile, which replaces the watermark; same strictness as `_reconcile_max_bills`)."""
    block = ((config or {}).get("knowledge_base_embedding") or {}).get("ledger") or {}
    value = block.get("max_bills_per_run", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        logger.warning("knowledge_base_ledger_bad_cap", value=value)
        return 0
    return value


def _alert_backlog_over(config: dict | None) -> int:
    """SYNC-95: `knowledge_base_embedding.alert_backlog_over`. A whole number > 0 turns the backlog alert on;
    0, absent or anything else is off, so a yaml typo cannot start paging."""
    value = ((config or {}).get("knowledge_base_embedding") or {}).get("alert_backlog_over", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        logger.warning("knowledge_base_bad_alert_threshold", value=value)
        return 0
    return value


def _backlog_alert_text(jurisdiction: str, totals: dict, threshold: int) -> str | None:
    """SYNC-95: the alert for a knowledge-base run that is not converging, or None. Two cases, both only
    when `threshold` > 0: the ledger was asked for and could not be read (the run fell back to the watermark
    path, whose blind spots are what the ledger exists to close), and a ledger run that left more than
    `threshold` bills still disagreeing (not reached under the cap, or failed). `bills_to_check` is the count
    before the cap and `failed` counts only bills among the `selected`, so the two do not overlap."""
    if not threshold:
        return None
    if totals.get("mode") == "watermark_fallback":
        return (f"knowledge-base embedding for {jurisdiction} could not read api-v3's ledger and ran on the "
                f"watermark path instead (check `knowledge_base_ledger_unavailable` in the ddp-sync log)")
    ledger = totals.get("ledger")
    if ledger:
        left = ledger["bills_to_check"] - ledger["selected"] + ledger["failed"]
        if left > threshold:
            return (f"knowledge-base embedding for {jurisdiction} still has {left} bills out of step with api-v3 "
                    f"after this run ({ledger['failed']} failed, {ledger['bills_to_check'] - ledger['selected']} "
                    f"over the per-run cap); alert threshold {threshold}")
    return None


def _post_slack_alert(text: str) -> None:
    """Best-effort Slack post through `post_alert` (same token, channel and CodeBot identity as `_alert_archive_failure`). Never raises."""
    post_alert(f":warning: {text}", source="openstates_archive.knowledge_base")


def _delete_orphans(config: dict | None) -> bool:
    """SYNC-95: `knowledge_base_embedding.delete_orphans`. Only a literal `true` turns it on: it deletes
    vectors, so a quoted "false", a stray string or a typo must not enable it."""
    return ((config or {}).get("knowledge_base_embedding") or {}).get("delete_orphans") is True


async def _maybe_embed_knowledge_base(
    jurisdiction: str,
    archive_started_at: datetime,
    config: dict | None,
) -> None:
    """SYNC-83 (PLAN-enterprise-search.md 5.6): post-archive hook that embeds the bills this
    archive run touched into the NEW `ddp-knowledge-base` index. Independent of the LegBot hook
    above: its own gate (the yaml block AND `knowledge_base_index_name` being set, both default
    off), its own failure handling, no ordering dependency. Writes only to the new index; the
    legacy votebot-large path is untouched. Log-and-continue like every other post-success hook.
    """
    if not _knowledge_base_embedding_eligible(jurisdiction, config):
        return
    settings = get_settings()
    if not settings.knowledge_base_index_name:
        logger.warning("knowledge_base_embedding_enrolled_but_index_unset", jurisdiction=jurisdiction)
        return

    # Same Mac-vs-EC2 read split as the LegBot hook: only the Mac reaches the local api-v3.
    from ddp_sync.pipelines.knowledge_base_embedding import (
        embed_archived_bills,
        read_target,
    )

    api_base, api_key = read_target(settings, mac_capable=_mac_capable())
    if not api_base:
        logger.warning("knowledge_base_embedding_read_path_not_configured", jurisdiction=jurisdiction)
        return

    totals = await embed_archived_bills(
        jurisdiction,
        archive_started_at,
        settings=settings,
        api_base=api_base,
        api_key=api_key,
        reconcile_max_bills=_reconcile_max_bills(config),
        delete_orphans=_delete_orphans(config),
        ledger_max_bills=_ledger_max_bills(config),
    )
    try:  # an alert is best-effort: building it must never turn a finished embedding run into a hook failure
        text = _backlog_alert_text(jurisdiction, totals, _alert_backlog_over(config))
        if text:
            await asyncio.to_thread(_post_slack_alert, text)
    except Exception as e:  # noqa: BLE001
        logger.error("knowledge_base_alert_error", jurisdiction=jurisdiction, error=str(e))


def _bill_search_refresh_eligible(jurisdiction: str, config: dict | None) -> bool:
    """OPEN-124's rule again: enrollment lives in sync_schedule.yaml
    (`openstates_archive.bill_search_refresh`), not in code. Default off."""
    block = (config or {}).get("bill_search_refresh") or {}
    if not block.get("enabled", False):
        return False
    # SYNC-157: WHO is the manifest's enrollment.search_refresh; the yaml keeps only the global switch.
    return jurisdiction.lower() in (enrolled("search_refresh", None, _scrape_root(config)) or [])


async def _maybe_refresh_bill_search(jurisdiction: str, config: dict | None) -> None:
    """SYNC-87 (PLAN-enterprise-search.md 4.5.5): post-archive hook that refreshes the api-v3
    `ddp_bill_search` projection for this jurisdiction. Independent of the LegBot and embedding
    hooks. ALWAYS targets the RDS-backed api-v3 (`rds_openstates_api_base`), never the Mac's local
    one: the projection table exists only on the RDS side, and archives extract into RDS."""
    if not _bill_search_refresh_eligible(jurisdiction, config):
        return
    settings = get_settings()
    # api-v3 refuses a refresh without a key (403), so an unset key can only ever fail: skip loudly.
    if not settings.rds_openstates_api_base or not settings.rds_openstates_api_key:
        logger.warning("bill_search_refresh_read_path_not_configured", jurisdiction=jurisdiction)
        return

    from ddp_sync.pipelines.bill_search_refresh import refresh_bill_search

    await refresh_bill_search(
        jurisdiction,
        api_base=settings.rds_openstates_api_base,
        api_key=settings.rds_openstates_api_key,
    )


def _archive_jurisdiction_code(scrape_jurisdiction: str) -> str:
    """Maps a scrape-side jurisdiction code to the archive side's own code for the same
    real jurisdiction -- see _SCRAPE_TO_ARCHIVE_JURISDICTION's docstring."""
    return _SCRAPE_TO_ARCHIVE_JURISDICTION.get(scrape_jurisdiction, scrape_jurisdiction)


def _load_archive_config_for_scrape_trigger() -> dict[str, Any]:
    """OPEN-291: independently loads openstates_archive's own config section, the same
    way _maybe_trigger_legbot_for_archive above independently calls get_settings() for
    its own configuration rather than relying on whatever config object the caller
    happens to have on hand. _run_scrape()'s own `config` parameter is scoped to
    openstates_scrape only -- it has no view into openstates_archive at all, and giving
    it one would mean threading an unrelated config section through every scrape job
    wrapper's signature. Reuses scheduler.py's own DEFAULT_CONFIG_PATH (lazy import --
    scheduler.py already imports the pipelines lazily too, so a module-level import here would cycle) rather than
    re-deriving the same path resolution a second time.
    """
    try:
        from ddp_sync.scheduler import DEFAULT_CONFIG_PATH

        import yaml

        with open(DEFAULT_CONFIG_PATH) as f:
            full_config = yaml.safe_load(f) or {}
    except Exception as e:  # noqa: BLE001 -- must never affect the scrape job's own result
        logger.warning("archive_trigger_for_scrape_config_load_failed", error=str(e))
        return {}
    return full_config.get("openstates_archive", {})


async def _archive_with_retry(
    jurisdiction: str, archive_config: dict | None, *, first_attempt: int = 1
) -> tuple[bool | None, int]:
    """`archive_with_retry`'s work, plus the number of attempts made (the manual archive's status record shows it).
    `first_attempt` (SYNC-192, from `resume_inflight_archive_job`): attempts before it already happened in a previous
    process, so the loop continues from there with the same delays and the same final alert."""
    j = _archive_jurisdiction_code(jurisdiction)
    if archive_config is None:
        archive_config = _load_archive_config_for_scrape_trigger()
    if not archive_config.get("enabled", False):
        return None, 0
    # SYNC-153: the manifest's archive.enabled is the ONE list; there is no yaml list and no fallback.
    if j not in (enrolled("archive", None, _scrape_root(archive_config)) or []):
        return None, 0
    attempt = 0
    for attempt, delay in enumerate((0, *ARCHIVE_RETRY_DELAYS_S), start=1):
        if attempt < first_attempt:
            continue
        if delay:
            await asyncio.sleep(delay)
        _archive_attempt.set(attempt)
        try:
            result = await run_single_archive_job(j, archive_config)  # Fargate (or local), then LegBot/search/embedding
        except Exception as e:  # noqa: BLE001 -- an exception is a failed attempt, not the end of the retries
            result = {"success": False, "error": str(e)}
        if result.get("success"):
            logger.info("archive_after_scrape_succeeded", jurisdiction=j, attempt=attempt)
            return True, attempt
        logger.warning("archive_attempt_failed", jurisdiction=j, attempt=attempt, error=result.get("error"))
    try:
        await asyncio.to_thread(
            post_alert,
            f":red_circle: *OpenStates archive failed: {j}* — {attempt} attempts (retried after "
            f"{', '.join(f'{d // 60} min' for d in ARCHIVE_RETRY_DELAYS_S)}); its LegBot, search refresh and embedding "
            f"did not run. Check the ddp-sync log.",
            source="openstates_archive.archive_failure",
        )
    except Exception as e:  # noqa: BLE001 -- the alert must not hide the outcome
        logger.error("archive_failure_alert_failed", jurisdiction=j, error=str(e))
    return False, attempt


async def archive_with_retry(jurisdiction: str, archive_config: dict | None = None) -> bool | None:
    """Archive one state (SYNC-153): None when it is not archived (archive disabled, or the manifest's
    `archive.enabled` is false or unreadable), True on success, False after 1 + len(ARCHIVE_RETRY_DELAYS_S) failed
    attempts and an alert. Never raises; an exception from one attempt counts as a failed attempt.

    Only a failed ARCHIVE is retried: the hooks (LegBot, search refresh, embedding) run inside `_run_archive_with_hook`
    only after a successful archive, each in its own try, and never change its result. A repeated archive skips the
    documents it already saved. The caller holds the state's pipeline lock (`run_state_pipeline`, or
    `run_archive_only`), which is what stops two archives of one state overlapping; the old 30-minute debounce is gone.
    The waits happen inside the caller's job (worst case 2.5 h), which the lock's TTL covers.
    `archive_config` is the `openstates_archive` block; None loads it from sync_schedule.yaml.
    """
    return (await _archive_with_retry(jurisdiction, archive_config))[0]


async def run_archive_only(jurisdiction: str, archive_config: dict, *, lock_token: str | None = None) -> dict[str, Any]:
    """The manual archive trigger's run (SYNC-153): archive one state under its pipeline lock, with the same retry, and
    write its own status record, `openstates_archive_<state>` (completed / failed / skipped, plus the attempt count),
    never the scrape's. The trigger took the lock before answering and passes its token; without one this takes it."""
    from ddp_sync.pipelines.openstates_scrape import pipeline_lock_ttl_s
    from ddp_sync.services.redis_store import state_lock

    j = _archive_jurisdiction_code(jurisdiction)
    started = datetime.now(timezone.utc)
    archived, attempts, status = None, 0, "failed"
    try:
        async with state_lock(j, pipeline_lock_ttl_s(j, None, archive_config), token=lock_token) as held:
            if not held:
                status = "skipped"
                logger.warning("manual_archive_skipped_state_busy", jurisdiction=j)
                return {"success": False, "jurisdiction": j, "skipped": "busy"}
            archived, attempts = await _archive_with_retry(j, archive_config)
            status = "completed" if archived else "failed" if archived is False else "skipped"
            return {"success": archived is not False, "jurisdiction": j, "archived": archived, "attempts": attempts}
    finally:
        await _write_flow_status(f"openstates_archive_{j}", {
            "flow": f"openstates_archive_{j}", "started_at": started.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(), "status": status,
            "archived": archived, "attempts": attempts,
        })


async def _run_archive_with_hook(
    jurisdiction: str,
    openstates_root: str | None = None,
    config: dict | None = None,
) -> dict[str, Any]:
    """Thin wrapper adding the archive-completion LegBot hook (SYNC-65) and the independent
    bill-search refresh (SYNC-87) and knowledge-base embedding (SYNC-83) hooks uniformly over both
    archive branches (local run-archive.sh and Fargate cloud_archiver.py)
    -- run_single_archive_job() needs no copy of its own. (SYNC-153 removed the OPEN-291 debounce that
    used to be claimed here: the state's pipeline lock, held by the caller, is what stops overlap now.)

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
        result = await _run_archive_fargate(jurisdiction, config=config, archive_started_at=archive_started_at)
    else:
        result = await _run_archive(jurisdiction, openstates_root or _get_root(config), config=config)

    if result.get("success"):
        await _run_archive_followups(jurisdiction, archive_started_at, config, run_id=result.get("archive_run_id"))
    return result


async def _run_archive_followups(
    jurisdiction: str,
    archive_started_at: datetime,
    config: dict | None,
    *,
    run_id: str | None = None,
    done: frozenset[str] | set[str] = frozenset(),
) -> None:
    """The follow-ups of a successful archive, in order: LegBot dispatch, bill search refresh, knowledge-base embedding.
    Shared by a normal run (`_run_archive_with_hook`) and a resumed one (`resume_inflight_archive_job`), so they cannot
    drift (SYNC-192). Each runs in its own try and never changes the archive's result.

    With a `run_id` (a Fargate archive, which has an in-flight record) each finished follow-up is noted in the record
    (`record_hook_done`; `done` are those a previous process already finished and are skipped), and the record is
    cleared once all have finished. A cancelled run (ddp-sync stopping) clears nothing, so the next process resumes at
    the follow-up it was in."""
    hooks = (
        ("legbot", "archiver_triggered_legbot_hook_failed",
         lambda: _maybe_trigger_legbot_for_archive(jurisdiction, archive_started_at)),
        # Before the embedding hook: that one can run for hours, and search should not wait for it.
        ("bill_search", "archiver_triggered_bill_search_hook_failed",
         lambda: _maybe_refresh_bill_search(jurisdiction, config)),
        ("knowledge_base", "archiver_triggered_knowledge_base_hook_failed",
         lambda: _maybe_embed_knowledge_base(jurisdiction, archive_started_at, config)),
    )
    for name, failed_event, hook in hooks:
        if name in done:
            continue
        try:
            await hook()
        except Exception as e:  # noqa: BLE001 -- must never affect the archive job's own result
            logger.error(failed_event, jurisdiction=jurisdiction, error=str(e))
        if run_id:
            inflight_fargate_jobs.record_hook_done(run_id, name)
    if run_id:
        inflight_fargate_jobs.clear(run_id)


def _task_known_to_ecs(task_arn: str, fargate_cfg: dict, ecs_client) -> bool:
    """False only when ECS answers that it has no such task (it forgets a stopped task after about an hour). Any error
    counts as known: the wait that follows reports it."""
    try:
        return bool(ecs_client.describe_tasks(cluster=fargate_cfg["cluster"], tasks=[task_arn]).get("tasks"))
    except Exception:  # noqa: BLE001
        return True


async def resume_inflight_archive_job(run_id: str, record: dict[str, Any], *, ecs_client=None) -> dict[str, Any]:
    """SYNC-192: finish an archive a previous ddp-sync process left in flight (called by `reconcile_inflight_fargate_jobs`
    at startup for each `kind: "archive"` record). Never raises (a cancel at shutdown aside, which leaves the record).

    0. A record missing its fields is alerted and cleared. Any other error before the task is known to have stopped (ECS
       throttling, a config read) alerts and KEEPS the record for the next restart: clearing it would hide a task that
       may still be running.
    1. Takes the state's pipeline lock (`startup` cleared every lock, so this puts back the one the archive held; a
       scheduled or manual run of the state is refused while this runs, as during a normal archive). If another run
       already holds it, this goes on without it and logs: the second archive would meet the task's own `_archive_lock`.
    2. Waits for the recorded task (no second archive is launched). A task ECS no longer knows (a stale record) is
       alerted and cleared, nothing relaunched; the state's next archive covers it.
    3. Success: the same follow-ups a normal run runs, minus those the record says finished, then clears the record.
       Failure: clears the record, sends the per-attempt alert as a normal failure does, and continues the retry
       sequence from the next attempt (`_archive_with_retry(first_attempt=)`), ending in the same final alert.
    A resumed wait starts a fresh `max_wait_seconds` (as `reconcile_inflight_fargate_jobs` explains for scrapes)."""
    from ddp_sync.pipelines.cloud_scrape_trigger import _wait_for_task_stop
    from ddp_sync.pipelines.openstates_scrape import pipeline_lock_ttl_s
    from ddp_sync.services.redis_store import state_lock

    start = time.monotonic()
    jurisdiction = record.get("jurisdiction") or run_id
    try:
        task_arn = record["task_arn"]
        fargate_cfg = record["fargate_cfg"]
        started_at = datetime.fromisoformat(record["archive_started_at"])
        attempt = int(record.get("attempt") or 1)
    except (KeyError, TypeError, ValueError) as e:  # a record that can never be resumed: say so, drop it
        inflight_fargate_jobs.clear(run_id)
        logger.error("resumed_archive_record_malformed", jurisdiction=jurisdiction, run_id=run_id, error=str(e))
        await asyncio.to_thread(_alert_archive_failure, jurisdiction, f"unusable in-flight record {run_id}: {e!r}", 0.0)
        return {"success": False, "jurisdiction": jurisdiction, "error": f"malformed_record: {e!r}"}
    try:
        archive_config = _load_archive_config_for_scrape_trigger()
        client = ecs_client or boto3.client("ecs")
        async with state_lock(jurisdiction, pipeline_lock_ttl_s(jurisdiction, None, archive_config)) as held:
            if not held:
                logger.warning("resumed_archive_state_lock_held_by_another_run", jurisdiction=jurisdiction, run_id=run_id)
            if not await asyncio.to_thread(_task_known_to_ecs, task_arn, fargate_cfg, client):
                inflight_fargate_jobs.clear(run_id)
                error = f"ECS no longer knows task {task_arn}; its result and follow-ups are unknown, nothing relaunched"
                logger.error("resumed_archive_task_unknown", jurisdiction=jurisdiction, run_id=run_id, task_arn=task_arn)
                await asyncio.to_thread(_alert_archive_failure, jurisdiction, error, round(time.monotonic() - start, 1))
                return {"success": False, "jurisdiction": jurisdiction, "error": "task_unknown"}
            exit_code, detail = await asyncio.to_thread(
                _wait_for_task_stop, task_arn, fargate_cfg, client, run_lock=(jurisdiction, run_id, "_archive_lock")
            )
            if exit_code == 0:
                logger.info("resumed_archive_task_done", jurisdiction=jurisdiction, run_id=run_id)
                await _run_archive_followups(
                    jurisdiction, started_at, archive_config, run_id=run_id, done=set(record.get("hooks_done") or ())
                )
                return {"success": True, "jurisdiction": jurisdiction, "resumed": True}
            inflight_fargate_jobs.clear(run_id)
            error = "exit_code_none" if exit_code is None else f"exit_code_{exit_code}"
            logger.error("resumed_archive_task_failed", jurisdiction=jurisdiction, run_id=run_id, error=error, detail=detail)
            await asyncio.to_thread(_alert_archive_failure, jurisdiction, f"{error}: {detail}", round(time.monotonic() - start, 1))
            archived, _ = await _archive_with_retry(jurisdiction, archive_config, first_attempt=attempt + 1)
            return {"success": archived is not False, "jurisdiction": jurisdiction, "resumed": True, "archived": archived}
    except Exception as e:  # noqa: BLE001 -- runs unobserved on a background task: nothing may escape
        # NOT cleared: an error here (ECS throttling, a config read) does not show the task has stopped, so the record
        # stays and the next restart tries again (a stale one is then cleared above). The lock was released on the way out.
        logger.error("resumed_archive_failed_unexpectedly", jurisdiction=jurisdiction, run_id=run_id, error=str(e))
        try:
            await asyncio.to_thread(
                _alert_archive_failure, jurisdiction,
                f"resuming the archive failed ({e}); record {run_id} kept for the next restart, task {task_arn}",
                round(time.monotonic() - start, 1),
            )
        except Exception as alert_error:  # noqa: BLE001 -- the alert must not hide the outcome
            logger.error("resumed_archive_alert_failed", jurisdiction=jurisdiction, error=str(alert_error))
        return {"success": False, "jurisdiction": jurisdiction, "error": str(e)}


async def run_single_archive_job(
    jurisdiction: str,
    config: dict | None = None,
) -> dict[str, Any]:
    """One archive attempt for one jurisdiction (`use_fargate` dispatch, then the archive-completion hooks; see
    _run_archive_with_hook). `archive_with_retry` calls it once per attempt."""
    return await _run_archive_with_hook(jurisdiction, config=config)
