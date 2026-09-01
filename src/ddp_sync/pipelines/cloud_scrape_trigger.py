"""
Cloud scrape trigger -- OPEN-193 (Phase 4).

Fills in `_run_scrape()`'s cloud-path branch (OPEN-208's `_cloud_path_owns()` gate), which
until now returned "skipped" and did nothing -- no jurisdiction has ever actually been
triggered by ddp-sync itself. Every cloud-owned run so far (the OPEN-191 rehearsal, 2026-08-
29/30) was a human running `aws ecs run-task` and `cloud_loader.py` by hand -- see
`ddp-open-states/infra/rds/README.md`. This turns that from a one-off rehearsal into an
ongoing, scheduled feed.

Two calls, run synchronously and sequentially. `run_cloud_scrape()` is meant to be awaited via
`asyncio.to_thread()`, exactly like the mac-side subprocess path in openstates_scrape.py --
every call in this module blocks.

  1. Launch an ECS Fargate task running `cloud_collector.py <jurisdiction> [session=...]`
     (OPEN-201, already built and rehearsed -- this file does not reimplement it), wait for it
     to stop, and read its exit code.
  2. On success, run `cloud_loader.py <jurisdiction> <run_id> [session=...]` (OPEN-190,
     likewise already built) as a local subprocess against RDS specifically -- never the local
     Postgres `_run_scrape()`'s own mac-side branch writes to. See `_run_load()`'s docstring
     for why `RDS_DATABASE_URL` is a dedicated variable rather than reusing `DATABASE_URL`.

Both scripts live in the `ddp-open-states` repo (`openstates_root`), not this one -- two
separate repos with two separate deploys, the same reasoning `_run_scrape()` already gives for
checking whether `run-scrape-retrying.sh` exists before trusting it is deployed.

What this deliberately does NOT do, scoped out rather than silently missing (OPEN-193's own
acceptance criteria list "load runs next to the database; ddp-sync runs on EC2" and re-
measured freshness -- not full local-path parity):

  * No stall detection. The local path watches its own scrape output directory's file count
    for a stuck run (SYNC-3); a Fargate task has no such directory to watch from here. ECS's
    own task boundary (stopped, with an exit code) is the only signal available. A wedged
    collector inside the container is bounded by `cloud_collector.py`'s own internal
    timeouts, not by anything on this side of the boundary -- a real gap if that turns out to
    be insufficient, but not one to guess a replacement mechanism for here.
  * No bounded-retry wrapper integration (`_retry_eligible()` / `run-scrape-retrying.sh`).
    That wrapper is mac/bash-specific; a cloud-side retry policy is a real design question
    (retry the whole run under a fresh run_id, or resume?) left open rather than answered by
    assumption.
  * No S3-backed-memory floor check (`_memory_backend_enabled()`). `cloud_collector.py`
    always reads/writes S3 memory itself (OPEN-181, `SCRAPER_MEMORY_PREFIX="prod"` is the
    Fargate task definition's own environment, not something set from here) -- there is no
    mac-side fallback path for a run that already executes in the cloud, so the floor that
    function exists to enforce does not apply on this side.
  * No alerting for a `run_cloud_scrape()` caller who already alerts on the returned dict.
    `_alert_scrape_failure()` IS called from inside the failure branches here, though --
    unlike the mac-side branch (which skips it for a positive returncode because
    run-scrape.sh's own `on_failure()` already paged from inside the process),
    `cloud_collector.py`/`cloud_loader.py` have no Slack/CAMS client of their own, so nothing
    else will page on a cloud-side failure if this doesn't.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from typing import Any

import boto3
import structlog

logger = structlog.get_logger(__name__)

# ECS's own DescribeTasks lastStatus for a task that has finished, one way or another.
_STOPPED = "STOPPED"

# How often to poll DescribeTasks while waiting for the container to finish. boto3's built-in
# `tasks_stopped` waiter caps out at a fixed 100 attempts -- nowhere near enough for a ~9.5h
# cold-start MA run (infra/rds/README.md, OPEN-191 rehearsal) at any delay worth polling at,
# so this polls by hand instead: the ceiling is this file's own `max_wait_seconds` (config),
# not the waiter's fixed one.
_POLL_INTERVAL_S = 30

# Default backstop if cloud_path.fargate.max_wait_seconds isn't set -- generous enough to
# cover the slowest cold-start run seen in the OPEN-191 rehearsal (MA, ~9.5h) with real margin,
# same reasoning SCRAPE_TIMEOUT_S in openstates_scrape.py already uses for the mac-side ceiling.
_DEFAULT_MAX_WAIT_S = 12 * 3600


def _generate_run_id(jurisdiction: str) -> str:
    """The handoff contract (PLAN-scraper-execution-migration.md SS1) leaves run_id "supplied
    by the caller, or generated here". Generating it here -- rather than letting
    cloud_collector.py pick one inside the container -- means this function already knows the
    run_id cloud_loader.py needs to load, with no CloudWatch/S3 parsing required to discover it
    after the fact."""
    return f"{jurisdiction}-{uuid.uuid4().hex[:12]}"


def _fargate_config(config: dict | None) -> dict:
    cfg = ((config or {}).get("cloud_path") or {}).get("fargate") or {}
    required = ("cluster", "task_definition", "subnets", "security_groups")
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise ValueError(f"cloud_path.fargate is missing required config: {', '.join(missing)}")
    return cfg


def _run_fargate_collection(
    jurisdiction: str,
    session_arg: str | None,
    run_id: str,
    fargate_cfg: dict,
    ecs_client,
) -> tuple[bool, int | None, str]:
    """Launch the collection task and block until it stops.

    Returns (started, exit_code, detail). `started=False` means run_task itself failed
    (capacity, config, IAM -- the task never actually ran); `exit_code=None` with
    `started=True` covers a task that stopped without ever reporting one (killed by ECS
    itself -- OOM, spot interruption -- or this function's own wait gave up first).
    """
    command = ["python3", "cloud_collector.py", jurisdiction]
    if session_arg:
        command.append(session_arg)

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
                    "assignPublicIp": "DISABLED",
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": container_name,
                        "command": command,
                        "environment": [{"name": "RUN_ID", "value": run_id}],
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

    task_arn = tasks[0]["taskArn"]
    max_wait = fargate_cfg.get("max_wait_seconds", _DEFAULT_MAX_WAIT_S)
    deadline = time.monotonic() + max_wait

    while True:
        desc = ecs_client.describe_tasks(cluster=fargate_cfg["cluster"], tasks=[task_arn])
        described = desc.get("tasks", [])
        if described and described[0].get("lastStatus") == _STOPPED:
            containers = described[0].get("containers", [])
            match = next((c for c in containers if c.get("name") == container_name), None)
            exit_code = match.get("exitCode") if match else None
            reason = (match or described[0]).get("reason", "")
            return True, exit_code, reason
        if time.monotonic() >= deadline:
            return True, None, f"gave up waiting after {max_wait}s (task_arn={task_arn})"
        time.sleep(_POLL_INTERVAL_S)


def _run_load(
    jurisdiction: str,
    session_arg: str | None,
    run_id: str,
    openstates_root: str,
    fargate_cfg: dict,
    subprocess_runner,
) -> tuple[bool, str]:
    """Run cloud_loader.py locally against RDS. Returns (success, detail).

    Reads the target database from `RDS_DATABASE_URL`, deliberately not `DATABASE_URL`:
    `_run_scrape()`'s own mac-side branch (this same ddp-sync process) uses that name for the
    LOCAL Postgres run-scrape.sh writes to. Reusing it here would make one config mistake
    silently point the mac's own local scrapes at RDS, or this load at the mac's local
    database -- two very different failure modes from one typo. A dedicated name makes that
    class of mistake impossible rather than merely documented against.
    """
    rds_url = os.environ.get("RDS_DATABASE_URL")
    if not rds_url:
        return False, "RDS_DATABASE_URL not set -- refusing to load without a target database"

    script = os.path.join(openstates_root, "cloud_loader.py")
    cmd = ["python3", script, jurisdiction, run_id]
    if session_arg:
        cmd.append(session_arg)

    env = {
        **os.environ,
        "DATABASE_URL": rds_url,
        "MEMORY_BUCKET": fargate_cfg.get("memory_bucket", os.environ.get("MEMORY_BUCKET", "")),
        "MEMORY_PREFIX": fargate_cfg.get("memory_prefix", "prod"),
    }

    try:
        result = subprocess_runner(cmd, env)
    except Exception as e:  # noqa: BLE001
        return False, str(e)

    if result.returncode != 0:
        stderr = result.stderr
        tail = stderr.decode(errors="replace") if isinstance(stderr, bytes) else str(stderr or "")
        return False, f"cloud_loader.py exited {result.returncode}: {tail[-500:]}"
    return True, ""


def run_cloud_scrape(
    jurisdiction: str,
    session_arg: str | None,
    openstates_root: str,
    config: dict | None,
    *,
    ecs_client=None,
    subprocess_runner=None,
) -> dict[str, Any]:
    """Trigger a Fargate collection and, on success, load it into RDS.

    Meant to be called from `_run_scrape()`'s cloud-path branch via `asyncio.to_thread` --
    this function itself makes only blocking calls (ECS polling, a blocking subprocess),
    exactly like the mac-side branch it replaces.

    Returns the same dict shape `_run_scrape()`'s own local branch does on both success
    (`success`, `jurisdiction`, `duration_seconds`) and failure (adds `error`,
    `failure_reason`) so callers -- retry counters, OPEN-22's WAF-escalation history,
    everything downstream of `_run_scrape()` -- need no changes to consume a cloud-owned
    run's result. Adds one extra field, `cloud_run_id`, once a run_id has actually been
    generated (i.e. past the config-validation failure case).
    """
    # Deferred import: openstates_scrape.py imports THIS module at load time, so importing it
    # back at load time here would be circular. By the time this function is actually called,
    # openstates_scrape's module object is already fully initialized -- safe.
    from ddp_sync.pipelines.openstates_scrape import (
        _alert_scrape_failure,
        classify_failure_reason,
    )

    ecs_client = ecs_client or boto3.client("ecs")
    subprocess_runner = subprocess_runner or (
        lambda cmd, env: subprocess.run(cmd, env=env, capture_output=True, check=False)
    )

    label = f"{jurisdiction} {session_arg}" if session_arg else jurisdiction
    start = time.monotonic()

    try:
        fargate_cfg = _fargate_config(config)
    except ValueError as e:
        duration = round(time.monotonic() - start, 1)
        logger.error("cloud_scrape: bad config", jurisdiction=jurisdiction, error=str(e))
        _alert_scrape_failure(label, str(e), duration)
        return {
            "success": False,
            "error": str(e),
            "failure_reason": "config_error",
            "jurisdiction": label,
            "duration_seconds": duration,
        }

    run_id = _generate_run_id(jurisdiction)
    logger.info(
        "cloud_scrape: triggering Fargate collection", jurisdiction=jurisdiction, run_id=run_id
    )

    started, exit_code, detail = _run_fargate_collection(
        jurisdiction, session_arg, run_id, fargate_cfg, ecs_client
    )
    if not started:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "cloud_scrape: run_task failed to start", jurisdiction=jurisdiction, detail=detail
        )
        error = f"run_task_failed: {detail}"
        _alert_scrape_failure(label, error, duration)
        return {
            "success": False,
            "error": error,
            "failure_reason": classify_failure_reason("exit_code_launch", detail),
            "jurisdiction": label,
            "duration_seconds": duration,
            "cloud_run_id": run_id,
        }

    if exit_code != 0:
        duration = round(time.monotonic() - start, 1)
        error = "exit_code_none" if exit_code is None else f"exit_code_{exit_code}"
        logger.error(
            "cloud_scrape: collection failed",
            jurisdiction=jurisdiction,
            run_id=run_id,
            exit_code=exit_code,
            detail=detail,
        )
        _alert_scrape_failure(label, f"collection {error}: {detail}", duration)
        return {
            "success": False,
            "error": error,
            "failure_reason": classify_failure_reason(error, detail),
            "jurisdiction": label,
            "duration_seconds": duration,
            "cloud_run_id": run_id,
        }

    logger.info(
        "cloud_scrape: collection done, loading into RDS",
        jurisdiction=jurisdiction,
        run_id=run_id,
    )
    load_ok, load_detail = _run_load(
        jurisdiction, session_arg, run_id, openstates_root, fargate_cfg, subprocess_runner
    )
    duration = round(time.monotonic() - start, 1)

    if not load_ok:
        logger.error(
            "cloud_scrape: load failed",
            jurisdiction=jurisdiction,
            run_id=run_id,
            detail=load_detail,
        )
        error = f"load_failed: {load_detail}"
        _alert_scrape_failure(label, error, duration)
        return {
            "success": False,
            "error": error,
            "failure_reason": classify_failure_reason("exit_code_load", load_detail),
            "jurisdiction": label,
            "duration_seconds": duration,
            "cloud_run_id": run_id,
        }

    logger.info(
        "cloud_scrape: done", jurisdiction=jurisdiction, run_id=run_id, duration_seconds=duration
    )
    return {
        "success": True,
        "jurisdiction": label,
        "duration_seconds": duration,
        "cloud_run_id": run_id,
    }
