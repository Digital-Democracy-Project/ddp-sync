"""
Cloud scrape trigger -- OPEN-193 (Phase 4).

Fills in `_run_scrape()`'s cloud-path branch (OPEN-208's `_cloud_path_owns()` gate), which
until now returned "skipped" and did nothing -- no jurisdiction has ever actually been
triggered by ddp-sync itself. Every cloud-owned run so far (the OPEN-191 rehearsal, 2026-08-
29/30) was a human running `aws ecs run-task` and `cloud_loader.py` by hand -- see
`ddp-open-states/infra/rds/README.md`. This turns that from a one-off rehearsal into an
ongoing, scheduled feed.

Two calls, run synchronously and sequentially. `run_cloud_scrape()` is meant to be awaited via
`run_in_cloud_scrape_slot()` (its own thread pool, SYNC-187), like the mac-side subprocess path in openstates_scrape.py --
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

  * (Was: no stall detection. OPEN-477 added it: the scrape wait now ends a run whose CloudWatch
    log stream has been quiet for `stall_seconds`, and no longer stops a run for taking long --
    see `_wait_for_task_stop()`.)
  * No same-jurisdiction concurrency lock of this module's own. `cloud_collector.py` already
    acquires OPEN-187's cross-machine `SourceLock`, keyed on the jurisdiction alone, the moment
    it starts -- the OPEN-191 rehearsal confirmed live that a second concurrent launch for the
    same source correctly refuses rather than racing (`infra/rds/README.md`). A second lock
    here would duplicate that mechanism rather than add a real guarantee; what this module adds
    on top is only the `stop_task`-on-timeout behavior above, so a run this side gave up on
    doesn't keep occupying that lock indefinitely once ECS itself has genuinely finished with it.
    SYNC-167: stopping the task does not free that lock (the container dies before its `finally`), so the stop
    now also releases it (`_release_stopped_run_lock`, only when this run holds it).
  * No bounded-retry wrapper integration (`_retry_eligible()` / `run-scrape-retrying.sh`).
    That wrapper is mac/bash-specific; a cloud-side retry policy is a real design question
    (retry the whole run under a fresh run_id, or resume?) left open rather than answered by
    assumption.
  * No S3-backed-memory floor check (`_memory_backend_enabled()`). `cloud_collector.py`
    always reads/writes S3 memory itself (OPEN-181, `SCRAPER_MEMORY_PREFIX="prod"` is the
    Fargate task definition's own environment, not something set from here) -- there is no
    mac-side fallback path for a run that already executes in the cloud, so the floor that
    function exists to enforce does not apply on this side.

OPEN-251: the wait between "task launched" and "task stopped" (`_wait_for_task_stop()`, up to
`max_wait_seconds`, hours in practice) happens entirely in this process's own memory. A ddp-sync
restart mid-wait does not touch the ECS task itself -- Fargate keeps it running regardless of
who launched it -- but it does lose the only thing watching for it to finish, which is exactly
what let a completed collection's output sit unloaded until the OPEN-193 canary's 123-object
manual recovery. `inflight_fargate_jobs.py` persists each launched task_arn to Redis the moment
it's known and clears it once the job reaches a terminal outcome; `reconcile_inflight_fargate_
jobs()` (called once from app.py's startup) resumes anything still on record, meaning the record
survived an unclean restart. `_finish_after_launch()` is the shared tail both a fresh launch and
a resumed one run through, so the two paths can't drift apart from here on.
"""

from __future__ import annotations

import asyncio
import contextvars
import itertools
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import boto3
import structlog

from ddp_sync.pipelines import inflight_fargate_jobs
from ddp_sync.services.rds_credentials import resolve_rds_database_url
from ddp_sync.slack_alerts import post_alert

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

# OPEN-477: a scrape whose task log has had no new line for this long is stuck, and is stopped. The local
# path's 45 minutes (OPEN-155, SCRAPE_STALL_SECONDS) is the starting point; the longest quiet gap in two
# healthy full Massachusetts walks (10-25 and 11-01, read from CloudWatch for OPEN-475) was 431 s (7.2 min),
# so 45 minutes is over six times the longest healthy gap seen.
_DEFAULT_STALL_S = 45 * 60

# Default backstop for cloud_loader.py itself (pm-review, round 1: this had no timeout at all).
# The OPEN-191 rehearsal's loads were minutes, not hours, even against multi-thousand-row
# jurisdictions -- collection is what takes hours, not the import -- so this is generous
# headroom for a real load, not a number sized to the collection side's own ceiling.
_DEFAULT_LOAD_TIMEOUT_S = 2 * 3600

_STOP_REQUESTED = "task stop requested"

# SYNC-167: how long to wait, after a stop request, for ECS to report the task STOPPED before releasing its lock.
# Fargate gives a container at most 120 s between SIGTERM and SIGKILL (stopTimeout), so this covers the longest case.
_STOP_CONFIRM_S = 150
_STOP_CONFIRM_POLL_S = 5

# SYNC-187: how many cloud scrapes this process watches at once. Each one holds a thread for its whole Fargate wait
# and load (hours; Florida's full re-import about 25 h), so they get a pool of their own: before this they used
# Python's default pool, 8 threads on the 4-CPU EC2 container, shared with alerts and every other `to_thread` call.
# On 2026-11-23 the ninth of a burst waited there with no log line (notes/ops-handoff c35a38c). 32 is Python's own
# ceiling for that default pool and covers every cloud-owned state of the onboarding wave running at once.
CLOUD_SCRAPE_SLOTS = 32
_cloud_scrape_pool = ThreadPoolExecutor(max_workers=CLOUD_SCRAPE_SLOTS, thread_name_prefix="cloud-scrape")
# Runs handed to the pool and not finished, oldest first, by a key of their own (a state can have two at once). Changed
# only on the event loop, and a run leaves it when its thread is done, not when its caller stops waiting (cancelled).
_cloud_scrape_runs: dict[int, str] = {}
_cloud_scrape_keys = itertools.count()


async def run_in_cloud_scrape_slot(jurisdiction: str, func, *args) -> Any:
    """Run one cloud scrape's blocking `func(*args)` on the cloud-scrape pool, like `asyncio.to_thread` (SYNC-187).

    When every slot is taken the run waits in the pool's own first-in-first-out queue, and says so once
    (`cloud_scrape_waiting_for_slot`: the state, its place in the queue, the states holding slots), then
    `cloud_scrape_slot_acquired` with how long it waited. Context variables are carried as `to_thread` does
    (FULL_COLLECTION).
    """
    loop = asyncio.get_running_loop()
    key = next(_cloud_scrape_keys)
    ahead = len(_cloud_scrape_runs)
    _cloud_scrape_runs[key] = jurisdiction
    queued_at = time.monotonic()
    if ahead >= CLOUD_SCRAPE_SLOTS:
        logger.warning(
            "cloud_scrape_waiting_for_slot",
            jurisdiction=jurisdiction,
            queue_position=ahead - CLOUD_SCRAPE_SLOTS + 1,
            slots=CLOUD_SCRAPE_SLOTS,
            running=list(_cloud_scrape_runs.values())[:CLOUD_SCRAPE_SLOTS],
        )

    def _in_slot():
        if ahead >= CLOUD_SCRAPE_SLOTS:
            logger.info("cloud_scrape_slot_acquired", jurisdiction=jurisdiction,
                        waited_seconds=round(time.monotonic() - queued_at, 1))
        return func(*args)

    def _done(_future):
        try:
            loop.call_soon_threadsafe(_cloud_scrape_runs.pop, key, None)
        except RuntimeError:  # the loop is closed (process shutting down): nothing left to count for
            pass

    future = _cloud_scrape_pool.submit(contextvars.copy_context().run, _in_slot)
    future.add_done_callback(_done)  # before wrap_future's own, so the run is gone when the caller resumes
    return await asyncio.wrap_future(future)


def _generate_run_id(jurisdiction: str) -> str:
    """The handoff contract (PLAN-scraper-execution-migration.md SS1) leaves run_id "supplied
    by the caller, or generated here". Generating it here -- rather than letting
    cloud_collector.py pick one inside the container -- means this function already knows the
    run_id cloud_loader.py needs to load, with no CloudWatch/S3 parsing required to discover it
    after the fact."""
    return f"{jurisdiction}-{uuid.uuid4().hex[:12]}"


def _fargate_config(config: dict | None, jurisdiction: str | None = None) -> dict:
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

    # OPEN-476: `max_wait_seconds` is one value for every state, but states need different ceilings (MA's full walk
    # ran 10.35 h on 10-25 and was heading for ~13.8 h on 11-01 when the shared 12 h cut it off). SYNC-157: a state's own
    # ceiling is the manifest's `scrape.fargate_max_wait_s` (null = the shared one), not a yaml map keyed by state.
    if jurisdiction is not None:
        from ddp_sync.pipelines.openstates_scrape import _get_root

        root = _get_root(config)
        # A copy: the caller's config dict is shared across jurisdictions and must not change.
        cfg = {**cfg, "load_on_fargate": state_loads_on_fargate(jurisdiction, root)}
        own = state_max_wait_s(jurisdiction, root)
        if own is not None:
            cfg["max_wait_seconds"] = own
    return cfg


def state_loads_on_fargate(jurisdiction: str, root: str) -> bool:
    """OPEN-633: the manifest's `scrape.load_on_fargate` for one state. Only a real `true` turns it on; a missing
    field (a manifest older than the field) or any other value keeps today's local load."""
    from ddp_sync.services.jurisdiction_enrollment import manifest_value

    return manifest_value(jurisdiction, ("scrape", "load_on_fargate"), root) is True


def state_max_wait_s(jurisdiction: str, root: str) -> int | None:
    """The manifest's `scrape.fargate_max_wait_s` for one state (SYNC-157), or None (use the shared ceiling). A value
    that is not a positive whole number is ignored with a warning (the ddp-open-states validator refuses it anyway)."""
    from ddp_sync.services.jurisdiction_enrollment import manifest_value

    seconds = manifest_value(jurisdiction, ("scrape", "fargate_max_wait_s"), root)
    if seconds is None:
        return None
    if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds <= 0:
        logger.warning("fargate_max_wait_s_invalid_ignored", jurisdiction=jurisdiction, value=seconds)
        return None
    return seconds


def _stop_orphaned_task(
    ecs_client, cluster: str, task_arn: str, reason: str = "ddp-sync: max_wait_seconds exceeded"
) -> str:
    """Best-effort cleanup for a task this function gave up waiting on (pm-review, round 1:
    the ECS task kept running past max_wait_seconds with nothing on this side ever stopping
    it, which could let a later retry or scheduled run start a second collection for the same
    jurisdiction while the first was still going). Never raises -- a failed stop_task here is
    still better reported as "gave up waiting" than as an exception replacing that message."""
    try:
        ecs_client.stop_task(cluster=cluster, task=task_arn, reason=reason)
        return _STOP_REQUESTED
    except Exception as e:  # noqa: BLE001 -- cleanup best-effort, never let this mask the timeout
        return f"stop_task also failed: {e}"


def _s3_client():
    return boto3.client("s3")


def _release_stopped_run_lock(
    ecs_client, task_arn: str, source: str, run_id: str, suffix: str, fargate_cfg: dict
) -> str:
    """SYNC-167: release the S3 run lock of a task ddp-sync has just stopped. `ecs stop-task` kills the container
    without running its `finally` (cloud_collector.py has no SIGTERM handler), so the lock it took (OPEN-187's
    `SourceLock`, 24 h, the same as a nightly state's cadence) would otherwise refuse the state's next run.

    Only once ECS reports the task STOPPED (pm-review: a stop request returns while the container may still be
    running), so the old run and a new one never hold the state at once; then `_release_run_lock`. Called only after a
    stop request that succeeded. Never raises: a release that does not happen leaves the lock to its TTL, as before."""
    if not fargate_cfg.get("memory_bucket", os.environ.get("MEMORY_BUCKET", "")):
        return "lock not released: no memory_bucket configured"
    try:
        deadline = time.monotonic() + _STOP_CONFIRM_S
        while True:
            desc = ecs_client.describe_tasks(cluster=fargate_cfg["cluster"], tasks=[task_arn])
            if any(t.get("lastStatus") == _STOPPED for t in desc.get("tasks", [])):
                break
            if time.monotonic() >= deadline:
                return f"lock not released: task not STOPPED after {_STOP_CONFIRM_S}s"
            time.sleep(_STOP_CONFIRM_POLL_S)
    except Exception as e:  # noqa: BLE001 -- best-effort, like _stop_orphaned_task
        logger.warning("cloud_scrape: could not confirm the stopped task", task_arn=task_arn, run_id=run_id,
                       error=str(e))
        return f"lock not released ({e})"
    return _release_run_lock(source, run_id, suffix, fargate_cfg)


def _release_run_lock(source: str, run_id: str, suffix: str, fargate_cfg: dict) -> str:
    """Release `run_id`'s S3 run lock once the caller knows the process that took it is gone (SYNC-167: a task ECS
    reports STOPPED; SYNC-189: a local load of the ddp-sync process a restart replaced).

    Same object and format as `SourceLock` in ddp-open-states' cloud_collector.py (`<prefix>/<source>/<suffix>`, a JSON
    body with `holder`, `acquired_at`, `expires_at`), released the way `SourceLock.release` does it: written already
    expired, conditional on the ETag just read. Only when that lock's holder is this run_id, so another run's lock is
    never touched. Never raises: a release that does not happen leaves the lock to its TTL, as before."""
    bucket = fargate_cfg.get("memory_bucket", os.environ.get("MEMORY_BUCKET", ""))
    if not bucket:
        return "lock not released: no memory_bucket configured"
    key = f"{fargate_cfg.get('memory_prefix', 'prod')}/{source}/{suffix}"
    try:
        client = _s3_client()
        existing = client.get_object(Bucket=bucket, Key=key)
        holder = json.loads(existing["Body"].read()).get("holder")
        if holder != run_id:
            return f"lock left alone: held by {holder}, not {run_id}"
        now = time.time()
        body = json.dumps({"holder": run_id, "acquired_at": now, "expires_at": now - 1}).encode()
        client.put_object(Bucket=bucket, Key=key, Body=body, IfMatch=existing["ETag"])
    except Exception as e:  # noqa: BLE001 -- best-effort, like _stop_orphaned_task
        logger.warning("cloud_scrape: could not release the run's lock", key=key, run_id=run_id, error=str(e))
        return f"lock not released ({e})"
    logger.info("cloud_scrape: released the run's lock", key=key, run_id=run_id)
    return f"lock {key} released"


# SYNC-181: a whole-session re-import (`/trigger/openstates-scrape/<state>?full=true`). Set by the pipeline for the run;
# `asyncio.to_thread` carries it into this module's blocking calls. The collector reads `full=true` as "no `start=`
# cutoff" (ddp-open-states cloud_collector.py); it is NOT part of `session_arg`, so the load's key and lock stay the
# session's own.
FULL_COLLECTION: contextvars.ContextVar[bool] = contextvars.ContextVar("full_collection", default=False)


def _launch_fargate_task(
    jurisdiction: str,
    session_arg: str | None,
    run_id: str,
    fargate_cfg: dict,
    ecs_client,
) -> tuple[bool, str | None, str]:
    """Just the run_task call and its immediate failure handling.

    Returns (started, task_arn, detail). `started=False` means run_task itself failed
    (capacity, config, IAM -- the task never actually ran), and `task_arn` is None.
    """
    # OPEN-242: docker-entrypoint.sh already runs `exec python3 /app/cloud_collector.py "$@"`,
    # and an ECS `command` override replaces CMD/ARGS, not ENTRYPOINT -- so this list becomes
    # that entrypoint's own "$@". Prepending "python3"/"cloud_collector.py" here double-ran the
    # interpreter and script name as if they were the jurisdiction/session args themselves
    # (`python3 cloud_collector.py python3 cloud_collector.py fl session=2026`), which
    # cloud_collector.py then choked on trying to parse as key=value arguments.
    # SYNC-54: session_arg can itself carry more than one key=value pair space-separated
    # (USA's own config is literally "119 chamber=lower" -- one combined string, not separate
    # session/chamber values). The Mac-side path tolerates this by accident: run-scrape.sh
    # interpolates this same value UNQUOTED when building its own os-update invocation, so
    # bash's own word-splitting silently re-splits it into two real argv tokens before
    # os-update ever sees it. There is no shell anywhere in this path -- ECS's
    # containerOverrides.command is a literal JSON array -- so nothing re-splits it here.
    # Found live: USA's cloud-path canary got "session=119 chamber=lower" as ONE argv token,
    # cloud_collector.py's parse_kv_args() (split on the first "=" only) parsed it as a single
    # garbage key=value pair, and USBillScraper's sitemap filter then matched nothing at all
    # -- a real crawl silently returning zero bills, not the ambiguous no-op case OPEN-244
    # exists for. .split() reproduces the same effective argv bash's word-splitting has always
    # produced for the Mac path; for the common case (no spaces) this is a one-element list,
    # identical to appending the string directly.
    command = [jurisdiction]
    if session_arg:
        command.extend(session_arg.split())
    if FULL_COLLECTION.get():
        command.append("full=true")  # SYNC-181: the collector's own word, never the scraper's

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

    return True, tasks[0]["taskArn"], ""


def _newest_log_line_s(task_arn: str, fargate_cfg: dict, logs_client) -> float | None:
    """Wall-clock time (seconds) of the newest line in the task's CloudWatch log stream, or None when the stream
    exists but holds no line yet. Raises when the stream cannot be read -- including when it does not exist (yet, or
    because the log group is misconfigured): the caller treats that as "cannot tell", never as "quiet"."""
    from ddp_sync.pipelines.openstates_backfill import task_log_stream

    log_group, stream_name = task_log_stream(task_arn, fargate_cfg)
    # startFromHead=False with limit=1 is the stream's last event.
    resp = logs_client.get_log_events(
        logGroupName=log_group, logStreamName=stream_name, startFromHead=False, limit=1
    )
    events = resp.get("events") or []
    if not events:
        return None
    return max(e.get("timestamp", 0) for e in events) / 1000.0


class _UnreadableLogs:
    """Stands in for a CloudWatch Logs client that could not be built: every read raises, so the stall check reports
    it cannot read the log (and never stops the run) instead of the run failing over a client constructor."""

    def __init__(self, error: Exception):
        self._error = error

    def get_log_events(self, **_kwargs):
        raise self._error


def _logs_client():
    try:
        return boto3.client("logs")
    except Exception as e:  # noqa: BLE001 -- see _UnreadableLogs
        return _UnreadableLogs(e)


def _wait_for_task_stop(
    task_arn: str,
    fargate_cfg: dict,
    ecs_client,
    *,
    logs_client=None,
    label: str | None = None,
    run_lock: tuple[str, str, str] | None = None,
) -> tuple[int | None, str]:
    """Poll DescribeTasks until `task_arn` stops.

    Extracted from the old `_run_fargate_collection` (OPEN-251) so it can run against a
    task_arn this process just launched OR one a previous process launched and persisted --
    DescribeTasks doesn't care which. Returns (exit_code, detail); `exit_code=None` covers a
    task that stopped without ever reporting one (killed by ECS itself -- OOM, spot
    interruption -- or this function ended the wait, in which case it also asked ECS to stop
    the task -- see `_stop_orphaned_task()`).

    Two modes:

    * Without `logs_client` (the archive and backfill jobs, unchanged): stop the task once `max_wait_seconds` runs out.
    * With `logs_client` (the scrape, OPEN-477): a stall check replaces the hard limit. Each poll reads the time of the
      newest line in the task's log stream; once no new line has appeared for `stall_seconds` (45 min by default, the
      clock starting when this wait began), the task is stopped and the wait returns a failure (the caller sends the
      one failure alert). A run that keeps logging is never stopped for taking long: at `max_wait_seconds` it gets one
      "still running" alert (sent only after a good read shows it is not stalled) and the wait goes on. A failed read
      never counts as quiet: it is logged, and after `stall_seconds` with no successful read one alert says the stall
      check cannot see this run.

    `run_lock` (SYNC-167): `(source, run_id, suffix)` of the S3 lock the task holds; whenever this wait stops the task,
    that lock is released too once ECS reports the task STOPPED (`_release_stopped_run_lock`), so the next run is not
    refused for up to 24 h. A failed stop request releases nothing.
    """
    container_name = fargate_cfg.get("container_name", "scraper")
    max_wait = fargate_cfg.get("max_wait_seconds", _DEFAULT_MAX_WAIT_S)
    stall_s = fargate_cfg.get("stall_seconds", _DEFAULT_STALL_S)
    started = time.monotonic()
    deadline = started + max_wait
    last_line_at = time.time()  # wall clock: CloudWatch timestamps are wall-clock
    last_read_ok = started
    long_run_alerted = unreadable_alerted = False
    name = label or task_arn

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
            return exit_code, reason

        now = time.monotonic()
        if logs_client is None:
            if now >= deadline:
                stop_detail = _stop_orphaned_task(ecs_client, fargate_cfg["cluster"], task_arn)
                if run_lock and stop_detail == _STOP_REQUESTED:
                    stop_detail += "; " + _release_stopped_run_lock(ecs_client, task_arn, *run_lock, fargate_cfg)
                return None, f"gave up waiting after {max_wait}s (task_arn={task_arn}); {stop_detail}"
        else:
            try:
                newest = _newest_log_line_s(task_arn, fargate_cfg, logs_client)
            except Exception as e:  # noqa: BLE001 -- any read failure means "cannot tell", never "quiet"
                logger.warning("cloud_scrape: stall check could not read the task log", label=name, error=str(e))
                if not unreadable_alerted and now - last_read_ok >= stall_s:
                    unreadable_alerted = True
                    post_alert(
                        f":warning: *OpenStates scrape stall check cannot read the log of {name}* -- no successful "
                        f"read for {stall_s // 60} min ({e}). The run is NOT stopped; nothing will end it if it "
                        f"hangs. ECS task {task_arn}.",
                        source="openstates_scrape.stall_check",
                    )
            else:
                last_read_ok = now
                if newest is not None:
                    last_line_at = max(last_line_at, newest)
                quiet = time.time() - last_line_at
                if quiet >= stall_s:
                    stop_detail = _stop_orphaned_task(
                        ecs_client,
                        fargate_cfg["cluster"],
                        task_arn,
                        reason=f"ddp-sync: no new log line for {int(quiet)}s (stall)",
                    )
                    if run_lock and stop_detail == _STOP_REQUESTED:
                        stop_detail += "; " + _release_stopped_run_lock(ecs_client, task_arn, *run_lock, fargate_cfg)
                    return None, (
                        f"stalled: no new log line for {int(quiet)}s (limit {stall_s}s) "
                        f"(task_arn={task_arn}); {stop_detail}"
                    )
                # pm-review: only after a good read that shows the run is not stalled, so this alert never calls an
                # unreadable run "still logging" nor comes just before a stall stop.
                if not long_run_alerted and now >= deadline:
                    long_run_alerted = True
                    hours = (now - started) / 3600
                    logger.warning("cloud_scrape: still running past its alert time", label=name, hours=round(hours, 1))
                    post_alert(
                        f":hourglass: *OpenStates scrape still running: {name}* -- {hours:.1f} h and still logging, so "
                        f"it was not stopped (OPEN-477: only a run with no new log line for {stall_s // 60} min is "
                        f"stopped). If it will never finish, stop ECS task {task_arn} by hand.",
                        source="openstates_scrape.long_run",
                    )
        time.sleep(_POLL_INTERVAL_S)


def _default_subprocess_runner(load_timeout_s: int):
    def _run(cmd, env):
        return subprocess.run(
            cmd, env=env, capture_output=True, check=False, timeout=load_timeout_s
        )

    return _run


def loader_lock_key(jurisdiction: str, session_arg: str | None) -> str:
    """The key cloud_loader.py takes its `_import_lock` under: ddp-open-states' `derive_scrape_key(source,
    parse_kv_args(argv))`, mirrored here because ddp-sync releases that lock when it stops a load task
    (`_release_stopped_run_lock`). `fl` + `session=2026` -> `fl_2026`; `usa` + `session=119 chamber=lower` ->
    `usa_119_lower`. A wrong key only means the lock is not found and lives out its TTL, as before SYNC-167."""
    params = dict(p.split("=", 1) for p in (session_arg or "").split() if "=" in p)
    session = params.pop("session", None)
    return "_".join([jurisdiction, *([session] if session else []), *(v for _, v in sorted(params.items()))])


def _run_load_on_fargate(
    jurisdiction: str,
    session_arg: str | None,
    run_id: str,
    fargate_cfg: dict,
    ecs_client,
    load_task_arn: str | None = None,
) -> tuple[bool, str]:
    """OPEN-633: run cloud_loader.py as a Fargate task of the scrapers image (`RUNNER_SCRIPT=cloud_loader.py`) instead
    of in this container. Launched through the archive's launcher; the load task's ARN goes into the in-flight record
    so a restarted ddp-sync waits for it instead of starting a second load (`load_task_arn` is that resumed ARN).

    Waited on with a hard ceiling, `load_timeout_seconds` (2 h), the same limit today's local subprocess has: at the
    ceiling the task is stopped and its `_import_lock` released once ECS reports it STOPPED. The import is one
    database transaction, so a stopped load leaves nothing behind. No stall check: in stall mode the ceiling stops
    nothing, and a load is minutes."""
    from ddp_sync.pipelines.openstates_archive import (
        _launch_archive_fargate_task,
        redact_dsn,
    )

    if load_task_arn is None:
        rds_url = None
        if not fargate_cfg.get("rds_from_task_secret"):
            rds_url, error = resolve_rds_database_url()
            if error:
                return False, f"cannot resolve an RDS target: {error}"
        command = [jurisdiction, run_id, *(session_arg.split() if session_arg else [])]
        started, load_task_arn, detail = _launch_archive_fargate_task(
            jurisdiction, run_id, rds_url, fargate_cfg, ecs_client, runner="cloud_loader.py", command=command
        )
        if not started:
            return False, f"load task did not start: {detail}"
        inflight_fargate_jobs.record_load_started(run_id, load_task_arn)
        logger.info("cloud_scrape: load task started", jurisdiction=jurisdiction, run_id=run_id,
                    load_task_arn=load_task_arn)

    load_cfg = {**fargate_cfg, "max_wait_seconds": fargate_cfg.get("load_timeout_seconds", _DEFAULT_LOAD_TIMEOUT_S)}
    exit_code, detail = _wait_for_task_stop(
        load_task_arn,
        load_cfg,
        ecs_client,
        label=f"{jurisdiction} load",
        run_lock=(loader_lock_key(jurisdiction, session_arg), run_id, "_import_lock"),
    )
    if exit_code != 0:
        return False, f"load task exited {exit_code} (task_arn={load_task_arn}): {redact_dsn(detail)}"
    return True, ""


def _run_load(
    jurisdiction: str,
    session_arg: str | None,
    run_id: str,
    openstates_root: str,
    fargate_cfg: dict,
    subprocess_runner,
    *,
    ecs_client=None,
    load_task_arn: str | None = None,
    resumed: bool = False,
) -> tuple[bool, str]:
    """Run cloud_loader.py locally against RDS. Returns (success, detail).

    SYNC-189: `resumed` (a run a previous ddp-sync process left in flight) first releases this run's own
    `_import_lock`. A local load is a child of the ddp-sync process, so a restart killed it before its `finally`, and
    the resumed load would otherwise be refused by its own run's lock for 24 h (Oklahoma 2026, 2026-11-23). Only a lock
    whose holder is this run_id, and only on the local path: a Fargate load task survives the restart and is waited on.

    OPEN-633: when the state's manifest says `scrape.load_on_fargate: true` (folded into `fargate_cfg` at launch, so a
    resumed job replays the same choice), the load runs as its own Fargate task instead (`_run_load_on_fargate`).

    Resolves the target database live from Secrets Manager (OPEN-260), deliberately not from
    a cached `DATABASE_URL`-style env var: `_run_scrape()`'s own mac-side branch (this same
    ddp-sync process) uses that name for the LOCAL Postgres run-scrape.sh writes to, and a
    static env var goes stale the moment RDS's own 7-day automatic credential rotation fires,
    regardless of how recently this container restarted. Resolving here, right before use,
    rather than reusing whatever `run_cloud_scrape()`'s own preflight check resolved earlier,
    is deliberate: a rotation that happens during the collection step (which can run for
    hours) is picked up here instead of carrying forward a value that was current when the
    preflight ran but may not be by the time the load actually happens.

    `run_cloud_scrape()` already refuses before spending hours on a Fargate collection if this
    can't be resolved (pm-review, round 1: the original version only discovered this here,
    after the collection had already run). This function keeps its own check too, so a
    caller that invokes it directly -- tests included -- gets the same guarantee.
    """
    if fargate_cfg.get("load_on_fargate"):
        return _run_load_on_fargate(
            jurisdiction, session_arg, run_id, fargate_cfg, ecs_client or boto3.client("ecs"), load_task_arn
        )

    if resumed:  # before anything that can fail: the killed loader is gone either way
        _release_run_lock(loader_lock_key(jurisdiction, session_arg), run_id, "_import_lock", fargate_cfg)

    rds_url, error = resolve_rds_database_url()
    if error:
        return False, f"cannot resolve an RDS target: {error}"

    script = os.path.join(openstates_root, "cloud_loader.py")
    cmd = ["python3", script, jurisdiction, run_id]
    if session_arg:
        # SYNC-54: same fix as _run_fargate_collection() -- session_arg can carry more than
        # one key=value pair space-separated (USA's own config), and this subprocess call has
        # no shell to word-split it the way the Mac path's bash invocation of run-scrape.sh
        # does by accident. .split() is a no-op for the common single-pair case.
        cmd.extend(session_arg.split())

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


def _finish_after_launch(
    jurisdiction: str,
    session_arg: str | None,
    run_id: str,
    openstates_root: str,
    fargate_cfg: dict,
    task_arn: str,
    ecs_client,
    subprocess_runner,
    label: str,
    logs_client=None,
    load_task_arn: str | None = None,
    resumed: bool = False,
) -> dict[str, Any]:
    """Wait for an already-launched ECS task to stop, then load into RDS on success.

    `resumed` (from `resume_inflight_fargate_job`): see `_run_load` (SYNC-189).

    OPEN-633: `load_task_arn` (from a resumed in-flight record) means the collection already finished and its load
    task was launched; the wait goes straight to that load task instead of starting a second load.

    Shared by `run_cloud_scrape()` (task_arn just came back from `_launch_fargate_task()`) and
    `resume_inflight_fargate_job()` (task_arn was persisted by a previous process -- OPEN-251):
    from here on the two paths are identical, since DescribeTasks doesn't care which process is
    asking and cloud_loader.py doesn't care whether this is the first attempt to run it. Keeping
    one shared tail means those two callers can't quietly drift apart over time.

    `duration_seconds` in the returned dict is measured from this call, not from the task's
    original launch -- for a resumed job that understates true end-to-end time by however long
    the previous process had already been waiting. Deliberately not fixed: reconstructing exact
    original duration would mean persisting and reconciling two different process's clocks for a
    number that is informational only, not used by any retry/escalation logic downstream.
    """
    from ddp_sync.pipelines.openstates_scrape import (
        _alert_scrape_failure,
        classify_failure_reason,
    )

    start = time.monotonic()
    if load_task_arn:
        exit_code, detail = 0, ""
    else:
        # OPEN-477: the scrape wait always runs in stall-check mode (a logs client is what switches it on).
        exit_code, detail = _wait_for_task_stop(
            task_arn,
            fargate_cfg,
            ecs_client,
            logs_client=logs_client or _logs_client(),
            label=label,
            run_lock=(jurisdiction, run_id, "_lock"),
        )

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
        jurisdiction,
        session_arg,
        run_id,
        openstates_root,
        fargate_cfg,
        subprocess_runner,
        ecs_client=ecs_client,
        load_task_arn=load_task_arn,
        resumed=resumed,
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


def resume_inflight_fargate_job(
    run_id: str, record: dict[str, Any], *, ecs_client=None, subprocess_runner=None, logs_client=None
) -> dict[str, Any]:
    """Finish a Fargate job a previous ddp-sync process left in-flight (OPEN-251).

    `run_id` is the Redis Hash field itself (from `inflight_fargate_jobs.list_inflight()`'s own
    keys), passed separately from `record` rather than trusted to also be correct *inside* the
    JSON payload (pm-review, round 1) -- a record can still be identified and cleared by its
    real key even if its payload is partially malformed. `record` is otherwise exactly what
    `run_cloud_scrape()` persisted via `record_started()` at launch time: jurisdiction,
    session_arg, task_arn, fargate_cfg, openstates_root. Resuming replays the same decisions
    the original launch already made, not whatever sync_schedule.yaml says right now -- there
    is no re-read of live config here.

    Runs unobserved on its own background thread (`asyncio.to_thread`, see
    `reconcile_inflight_fargate_jobs()`), so -- exactly like `run_cloud_scrape()`'s own outer
    guarantee for a fresh run (pm-review, round 1) -- nothing may escape this function
    uncaught: a malformed record, an ECS client construction failure, anything. The whole
    point of this function existing is to stop a job from disappearing without a trace: an
    uncaught exception here, on a background task nothing is awaiting, would do exactly that
    silently, which is worse than the restart this is meant to recover from.

    Always clears the record when done -- success, a normal reported failure, or this
    function's own unexpected-exception path alike -- exactly like `run_cloud_scrape()` does
    for a fresh run. Safe to do unconditionally because every one of those paths also alerts
    first: a cleared-but-still-broken run is never silent, only ever loudly reported.
    """
    from ddp_sync.pipelines.openstates_scrape import (
        _alert_scrape_failure,
        classify_failure_reason,
    )

    start = time.monotonic()
    try:
        jurisdiction = record["jurisdiction"]
        session_arg = record.get("session_arg")
        task_arn = record["task_arn"]
        fargate_cfg = record["fargate_cfg"]
        openstates_root = record["openstates_root"]
        label = f"{jurisdiction} {session_arg}" if session_arg else jurisdiction

        client = ecs_client or boto3.client("ecs")
        return _finish_after_launch(
            jurisdiction,
            session_arg,
            run_id,
            openstates_root,
            fargate_cfg,
            task_arn,
            client,
            subprocess_runner,
            label,
            logs_client,
            load_task_arn=record.get("load_task_arn"),
            resumed=True,
        )
    except Exception as e:  # noqa: BLE001 -- see docstring: nothing may escape uncaught here
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "cloud_scrape: resuming an in-flight job failed unexpectedly",
            run_id=run_id,
            error=str(e),
        )
        _alert_scrape_failure(f"resume:{run_id}", str(e), duration)
        return {
            "success": False,
            "error": str(e),
            "failure_reason": classify_failure_reason("exit_code_unexpected", str(e)),
            "cloud_run_id": run_id,
            "duration_seconds": duration,
        }
    finally:
        inflight_fargate_jobs.clear(run_id)


# Reconciliation tasks are otherwise unreferenced once reconcile_inflight_fargate_jobs()
# returns (pm-review, round 1): asyncio only holds a WEAK reference to a task created via
# create_task, so with nothing else keeping it alive it can be garbage-collected mid-run,
# silently abandoning the very job this whole mechanism exists to not lose track of. Kept
# here for the process lifetime, and each one's own outcome logged via a done-callback --
# resume_inflight_fargate_job() should already guarantee it never raises (see its own
# docstring), so this callback firing with an exception is itself a bug worth knowing about,
# not the primary safety net.
_reconciliation_tasks: set[asyncio.Task] = set()


def _log_reconciliation_outcome(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "cloud_scrape: a resumed job's background task raised despite its own "
            "guarantee not to -- this is a bug in resume_inflight_fargate_job() itself",
            error=str(exc),
        )


def reconcile_inflight_fargate_jobs() -> list[asyncio.Task]:
    """Resume any Fargate jobs a previous ddp-sync process left in-flight (OPEN-251).

    Meant to be called once from `app.py`'s startup. Every fresh `run_cloud_scrape()` call
    records its own task_arn the moment the ECS task launches and clears that record itself once
    the job reaches a terminal outcome (collection failure, load failure, or success) -- so
    anything still present here when a NEW process starts was orphaned by the previous process
    dying (crash, deploy restart) mid-wait or mid-load, not a job someone else is still watching.

    Each resumed job runs on the cloud-scrape pool (`run_in_cloud_scrape_slot`, SYNC-187), exactly like a
    fresh `run_cloud_scrape()` call is already run from `openstates_scrape.py` -- this function
    itself never blocks, so it never delays app startup.

    A resumed wait starts its own fresh `max_wait_seconds` budget (`_wait_for_task_stop()` has
    no memory of how long a previous process already waited) -- deliberately not fixed, same
    reasoning as `_finish_after_launch()`'s own `duration_seconds` note: only a restart-heavy
    jurisdiction would ever stack enough resumed waits to matter, and persisting/reconciling
    the original wall-clock launch time across processes just to tighten a safety-net timeout
    is not proportionate to how rarely ddp-sync actually restarts mid-run.
    """
    records = inflight_fargate_jobs.list_inflight()
    tasks = []
    for run_id, record in records.items():
        logger.warning(
            "cloud_scrape: resuming a Fargate job orphaned by a previous restart",
            run_id=run_id,
            jurisdiction=record.get("jurisdiction"),
            task_arn=record.get("task_arn"),
        )
        task = asyncio.create_task(  # SYNC-187: a resumed scrape takes a cloud-scrape slot like a fresh one
            run_in_cloud_scrape_slot(record.get("jurisdiction") or run_id, resume_inflight_fargate_job, run_id, record)
        )
        _reconciliation_tasks.add(task)
        task.add_done_callback(_reconciliation_tasks.discard)
        task.add_done_callback(_log_reconciliation_outcome)
        tasks.append(task)
    return tasks


def run_cloud_scrape(
    jurisdiction: str,
    session_arg: str | None,
    openstates_root: str,
    config: dict | None,
    *,
    ecs_client=None,
    subprocess_runner=None,
    logs_client=None,
) -> dict[str, Any]:
    """Trigger a Fargate collection and, on success, load it into RDS.

    Meant to be called from `_run_scrape()`'s cloud-path branch via `run_in_cloud_scrape_slot` (SYNC-187) --
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
            fargate_cfg = _fargate_config(config, jurisdiction)
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
        #
        # OPEN-260: only checking resolvability here, not keeping the resolved URL for later --
        # _run_load() below resolves it again live, deliberately, so a Secrets Manager rotation
        # that happens during this collection's hours-long run is picked up at load time rather
        # than carrying forward whatever was current when this preflight check ran.
        # OPEN-633: a load task that reads the RDS secret itself needs nothing resolved here.
        preflight_error = ""
        if not (fargate_cfg.get("load_on_fargate") and fargate_cfg.get("rds_from_task_secret")):
            _, preflight_error = resolve_rds_database_url()
        if preflight_error:
            duration = round(time.monotonic() - start, 1)
            error = f"cannot resolve an RDS target: {preflight_error}"
            logger.error("cloud_scrape: missing load prerequisite", jurisdiction=jurisdiction, error=preflight_error)
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
        started, task_arn, detail = _launch_fargate_task(
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

        # OPEN-251: persist this task_arn now, before the -- possibly hours-long -- wait, so a
        # ddp-sync restart mid-wait can find and resume this job instead of losing track of it.
        # See inflight_fargate_jobs.py and reconcile_inflight_fargate_jobs().
        inflight_fargate_jobs.record_started(
            run_id, jurisdiction, session_arg, task_arn, fargate_cfg, openstates_root
        )
        try:
            return _finish_after_launch(
                jurisdiction,
                session_arg,
                run_id,
                openstates_root,
                fargate_cfg,
                task_arn,
                client,
                subprocess_runner,
                label,
                logs_client,
            )
        finally:
            inflight_fargate_jobs.clear(run_id)
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
