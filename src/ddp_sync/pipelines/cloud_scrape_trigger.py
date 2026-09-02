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

`run_cloud_scrape()`'s whole body runs inside one outer try/except (pm-review, round 1): any
unexpected exception anywhere in this module -- `boto3.client("ecs")` construction,
`describe_tasks` throttling or a transient network blip mid-poll, anything -- must still come
back as this function's normal failure-dict shape and still alert, exactly like `_run_scrape()`
already guarantees for its own mac-side branch (its own top-level `except Exception`). Without
that guarantee, an AWS hiccup on a background thread would raise past `asyncio.to_thread()`
uncaught instead of being logged and reported as one failed run.

Two same-day fixes worth calling out, both landed after the first round of pm-review found
them: a run that gives up waiting (`max_wait_seconds` exceeded) now best-effort `stop_task`s
the orphaned ECS task rather than leaving it running unbounded, and the load subprocess now
carries its own wall-clock timeout (`cloud_path.fargate.load_timeout_seconds`) rather than
being able to hang forever on a stuck database connection.

What this deliberately does NOT do, scoped out rather than silently missing (OPEN-193's own
acceptance criteria list "load runs next to the database; ddp-sync runs on EC2" and re-
measured freshness -- not full local-path parity):

  * No stall detection. The local path watches its own scrape output directory's file count
    for a stuck run (SYNC-3); a Fargate task has no such directory to watch from here. ECS's
    own task boundary (stopped, with an exit code) is the only signal available. A wedged
    collector inside the container is bounded by `cloud_collector.py`'s own internal
    timeouts, not by anything on this side of the boundary -- a real gap if that turns out to
    be insufficient, but not one to guess a replacement mechanism for here.
  * No same-jurisdiction concurrency lock of this module's own. `cloud_collector.py` already
    acquires OPEN-187's cross-machine `SourceLock`, keyed on the jurisdiction alone, the moment
    it starts -- the OPEN-191 rehearsal confirmed live that a second concurrent launch for the
    same source correctly refuses rather than racing (`infra/rds/README.md`). A second lock
    here would duplicate that mechanism rather than add a real guarantee; what this module adds
    on top is only the `stop_task`-on-timeout behavior above, so a run this side gave up on
    doesn't keep occupying that lock indefinitely once ECS itself has genuinely finished with it.
  * No bounded-retry wrapper integration (`_retry_eligible()` / `run-scrape-retrying.sh`).
    That wrapper is mac/bash-specific; a cloud-side retry policy is a real design question
    (retry the whole run under a fresh run_id, or resume?) left open rather than answered by
    assumption.
  * No S3-backed-memory floor check (`_memory_backend_enabled()`). `cloud_collector.py`
    always reads/writes S3 memory itself (OPEN-181, `SCRAPER_MEMORY_PREFIX="prod"` is the
    Fargate task definition's own environment, not something set from here) -- there is no
    mac-side fallback path for a run that already executes in the cloud, so the floor that
    function exists to enforce does not apply on this side.
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

# Default backstop for cloud_loader.py itself (pm-review, round 1: this had no timeout at all).
# The OPEN-191 rehearsal's loads were minutes, not hours, even against multi-thousand-row
# jurisdictions -- collection is what takes hours, not the import -- so this is generous
# headroom for a real load, not a number sized to the collection side's own ceiling.
_DEFAULT_LOAD_TIMEOUT_S = 2 * 3600


def _generate_run_id(jurisdiction: str) -> str:
    """The handoff contract (PLAN-scraper-execution-migration.md SS1) leaves run_id "supplied
    by the caller, or generated here". Generating it here -- rather than letting
    cloud_collector.py pick one inside the container -- means this function already knows the
    run_id cloud_loader.py needs to load, with no CloudWatch/S3 parsing required to discover it
    after the fact."""
    return f"{jurisdiction}-{uuid.uuid4().hex[:12]}"


def _fargate_config(config: dict | None) -> dict:
    """Raises ValueError -- never anything else -- for every malformed shape, including a
    hand-authored YAML mistake that makes `cloud_path` or `cloud_path.fargate` the wrong type
    (found in independent review: the original version called `.get()` on whatever was there
    unconditionally, so a non-dict value raised `AttributeError` instead, which escaped past
    `run_cloud_scrape()`'s own `except ValueError` uncaught -- and past `_run_scrape()`, which
    has no handler of its own, into `openstates_secondary_scrapes()`'s bare `asyncio.gather()`,
    cancelling every other jurisdiction's in-flight scrape in the same batch over one bad
    config value for a single jurisdiction)."""
    # ValueError, not TypeError, deliberately (ruff TRY004 disagrees) -- every config problem
    # this function finds, wrong-shaped or missing, needs to classify identically as
    # "config_error" at the one call site that catches it; splitting the exception type here
    # would just move that classification logic there instead of gaining anything.
    cloud_path_cfg = (config or {}).get("cloud_path") or {}
    if not isinstance(cloud_path_cfg, dict):
        raise ValueError(  # noqa: TRY004
            f"cloud_path must be a mapping, got {type(cloud_path_cfg).__name__}"
        )

    cfg = cloud_path_cfg.get("fargate") or {}
    if not isinstance(cfg, dict):
        raise ValueError(  # noqa: TRY004
            f"cloud_path.fargate must be a mapping, got {type(cfg).__name__}"
        )

    required = ("cluster", "task_definition", "subnets", "security_groups")
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise ValueError(f"cloud_path.fargate is missing required config: {', '.join(missing)}")
    return cfg


def _stop_orphaned_task(ecs_client, cluster: str, task_arn: str) -> str:
    """Best-effort cleanup for a task this function gave up waiting on (pm-review, round 1:
    the ECS task kept running past max_wait_seconds with nothing on this side ever stopping
    it, which could let a later retry or scheduled run start a second collection for the same
    jurisdiction while the first was still going). Never raises -- a failed stop_task here is
    still better reported as "gave up waiting" than as an exception replacing that message."""
    try:
        ecs_client.stop_task(
            cluster=cluster, task=task_arn, reason="ddp-sync: max_wait_seconds exceeded"
        )
        return "task stop requested"
    except Exception as e:  # noqa: BLE001 -- cleanup best-effort, never let this mask the timeout
        return f"stop_task also failed: {e}"


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
    itself -- OOM, spot interruption -- or this function's own wait gave up first, in which
    case it also asked ECS to stop the task -- see `_stop_orphaned_task()`).
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
                    # OPEN-241: every subnet this project has stood up so far (OPEN-200's
                    # spike, and OPEN-193's own canary subnets) is public-by-design with no
                    # NAT gateway -- that's the whole point of assigning a public IP per task
                    # instead. Hardcoding DISABLED here left every task's ENI with no route to
                    # the internet at all, so it could never even reach ECR to pull its own
                    # image. Configurable per fargate_cfg for a future task definition that
                    # does run in a NAT-backed private subnet, but ENABLED is the correct
                    # default given what's actually deployed today.
                    "assignPublicIp": fargate_cfg.get("assign_public_ip", "ENABLED"),
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
            task = described[0]
            containers = task.get("containers", [])
            match = next((c for c in containers if c.get("name") == container_name), None)
            exit_code = match.get("exitCode") if match else None
            # A container-level `reason` (e.g. "OutOfMemoryError") is the more specific
            # diagnosis when present; `stoppedReason` is the task-level fallback ECS sets for
            # failures the container itself never got a chance to report (image pull failure,
            # essential-container-exited-without-one, resource init failure). pm-review, round
            # 1: the original version only ever read the container-level field, which could be
            # empty for exactly the failures worth diagnosing most.
            reason = (match or {}).get("reason") or task.get("stoppedReason", "")
            return True, exit_code, reason
        if time.monotonic() >= deadline:
            stop_detail = _stop_orphaned_task(ecs_client, fargate_cfg["cluster"], task_arn)
            return True, None, f"gave up waiting after {max_wait}s (task_arn={task_arn}); {stop_detail}"
        time.sleep(_POLL_INTERVAL_S)


def _default_subprocess_runner(load_timeout_s: int):
    def _run(cmd, env):
        return subprocess.run(
            cmd, env=env, capture_output=True, check=False, timeout=load_timeout_s
        )

    return _run


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

    `run_cloud_scrape()` already refuses before spending hours on a Fargate collection if
    `RDS_DATABASE_URL` is unset (pm-review, round 1: the original version only discovered this
    here, after the collection had already run). This function keeps its own check too, so a
    caller that invokes it directly -- tests included -- gets the same guarantee.
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

    runner = subprocess_runner or _default_subprocess_runner(
        fargate_cfg.get("load_timeout_seconds", _DEFAULT_LOAD_TIMEOUT_S)
    )

    try:
        result = runner(cmd, env)
    except Exception as e:  # noqa: BLE001 -- includes subprocess.TimeoutExpired
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

    label = f"{jurisdiction} {session_arg}" if session_arg else jurisdiction
    start = time.monotonic()

    try:
        # Nested rather than a separate try/except ValueError at this call site alone
        # (independent review found exactly that gap): _fargate_config() raising something
        # OTHER than ValueError -- a hand-authored YAML mistake making cloud_path or
        # cloud_path.fargate the wrong type raised AttributeError before that function
        # validated its own input -- escaped a narrower catch here uncaught, then propagated
        # past _run_scrape() (no handler of its own) into openstates_secondary_scrapes()'s
        # bare asyncio.gather(), cancelling every other jurisdiction's in-flight scrape in the
        # same batch. _fargate_config() itself now only ever raises ValueError (see its own
        # docstring), but nesting this inside the outer catch-all below means that guarantee
        # no longer has to hold perfectly forever for this function's own promise to hold.
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

        # pm-review, round 1: check this BEFORE triggering an hours-long Fargate collection,
        # not only inside _run_load() after the fact -- a missing RDS target is knowable up
        # front and shouldn't cost real AWS spend to discover.
        if not os.environ.get("RDS_DATABASE_URL"):
            duration = round(time.monotonic() - start, 1)
            error = "RDS_DATABASE_URL not set -- refusing to load without a target database"
            logger.error("cloud_scrape: missing load prerequisite", jurisdiction=jurisdiction)
            _alert_scrape_failure(label, error, duration)
            return {
                "success": False,
                "error": error,
                "failure_reason": "config_error",
                "jurisdiction": label,
                "duration_seconds": duration,
                "cloud_run_id": run_id,
            }

        client = ecs_client or boto3.client("ecs")

        logger.info(
            "cloud_scrape: triggering Fargate collection",
            jurisdiction=jurisdiction,
            run_id=run_id,
        )
        started, exit_code, detail = _run_fargate_collection(
            jurisdiction, session_arg, run_id, fargate_cfg, client
        )
        if not started:
            duration = round(time.monotonic() - start, 1)
            logger.error(
                "cloud_scrape: run_task failed to start",
                jurisdiction=jurisdiction,
                detail=detail,
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
            "cloud_scrape: done",
            jurisdiction=jurisdiction,
            run_id=run_id,
            duration_seconds=duration,
        )
        return {
            "success": True,
            "jurisdiction": label,
            "duration_seconds": duration,
            "cloud_run_id": run_id,
        }
    except Exception as e:  # noqa: BLE001 -- the guarantee this whole function makes to its
        # caller (pm-review, round 1): NOTHING escapes uncaught. boto3 client construction,
        # a describe_tasks throttling error mid-poll, anything -- all come back as this same
        # failure-dict shape and still alert, exactly like _run_scrape()'s own mac-side branch
        # already guarantees via its own top-level `except Exception`.
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "cloud_scrape: unexpected error", jurisdiction=jurisdiction, error=str(e)
        )
        _alert_scrape_failure(label, str(e), duration)
        return {
            "success": False,
            "error": str(e),
            "failure_reason": classify_failure_reason("exit_code_unexpected", str(e)),
            "jurisdiction": label,
            "duration_seconds": duration,
        }
