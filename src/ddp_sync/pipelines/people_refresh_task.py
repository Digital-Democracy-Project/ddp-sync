"""OPEN-882 (OPEN-633 section 8, D3): the weekly people refresh as a launched Fargate task.

`openstates_scrape.people_refresh.on_fargate` (default false: `run-people-refresh.sh` in this container, kept one release as the
rollback) true makes `run_people_refresh_job` launch ddp-open-states' `cloud_people_refresh.py <run_id> <states>` on
`task_definition_db` (`ddp-scrapers-db`, the one definition that may hold the database login) through the archive's launcher,
wait for it, and read the PER-STATE JSON it wrote to S3 (`working-tier/people-refresh/<run_id>/people-refresh.json`). The
broker scorecard signal then fires only for the states that worked (the script said only "something failed"). The task
downloads the public `people` repo itself when it starts, so a people change needs no image rebuild.

The task is in the in-flight record (`kind: "people_refresh"`, kept until the result has been acted on), so a restarted
ddp-sync waits for the same task, reads the result and still alerts and signals. The schedule, the status record
(`openstates_people_refresh`), the Slack alert and the signal stay here. Nothing in this module raises into the scheduler.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from ddp_sync.pipelines import inflight_fargate_jobs
from ddp_sync.services import jurisdiction_enrollment as enrollment
from ddp_sync.services.rds_credentials import resolve_rds_database_url

logger = structlog.get_logger(__name__)

RUNNER = "cloud_people_refresh.py"
# The local script's own limit (run_people_refresh_job), kept as the task's hard stop.
FARGATE_TIMEOUT_S = 3600
# Where the task writes (ddp-open-states cloud_people_refresh.py: `<WORKING_TIER_PREFIX>/people-refresh/<run_id>/<name>`).
WORKING_TIER_PREFIX = "working-tier"
RESULT_NAME = "people-refresh.json"


def new_run_id() -> str:
    return f"people-refresh-{uuid.uuid4().hex[:12]}"


def launch_task(states: list[str], run_id: str, config: dict | None, *, ecs_client=None) -> tuple[str, dict]:
    """Launch `cloud_people_refresh.py <run_id> <states>` and record the task. Blocking (run it in a thread). Returns
    (task_arn, fargate_cfg). Raises RuntimeError, never with a connection string, when it cannot start."""
    import boto3

    from ddp_sync.pipelines.cloud_scrape_trigger import _fargate_config
    from ddp_sync.pipelines.openstates_archive import _launch_archive_fargate_task

    try:
        fargate_cfg = _fargate_config(config)
    except ValueError as e:
        raise RuntimeError(f"fargate config error: {e}") from e
    rds_url = None
    if not fargate_cfg.get("rds_from_task_secret"):
        rds_url, error = resolve_rds_database_url()
        if error or not rds_url:
            raise RuntimeError(f"cannot resolve an RDS login: {error}")
    client = ecs_client or boto3.client("ecs")
    started, task_arn, detail = _launch_archive_fargate_task(
        "people-refresh", run_id, rds_url, fargate_cfg, client, runner=RUNNER, command=[run_id, ",".join(states)]
    )
    if not started:
        raise RuntimeError(f"task did not start: {detail}")
    inflight_fargate_jobs.record_people_refresh_started(run_id, task_arn, fargate_cfg, states)
    return task_arn, fargate_cfg


def await_result(run_id: str, task_arn: str, fargate_cfg: dict, ecs_client, states: list[str], s3_client=None) -> dict[str, Any]:
    """Wait for the task to stop (a hard stop at `FARGATE_TIMEOUT_S`), then return {state: entry} for every requested state.
    Blocking. Exit 0 (all done) and 1 (some state failed) both left a result; any other exit left none and raises."""
    from ddp_sync.pipelines.cloud_scrape_trigger import _wait_for_task_stop
    from ddp_sync.pipelines.openstates_archive import redact_dsn

    wait_cfg = {**fargate_cfg, "max_wait_seconds": FARGATE_TIMEOUT_S}
    exit_code, detail = _wait_for_task_stop(task_arn, wait_cfg, ecs_client, label="people refresh")
    if exit_code not in (0, 1):
        raise RuntimeError(f"task exited {exit_code} (task_arn={task_arn}): {redact_dsn(detail)}")
    return read_result(run_id, fargate_cfg, states, s3_client)


def read_result(run_id: str, fargate_cfg: dict, states: list[str], s3_client=None) -> dict[str, Any]:
    """{state: entry} for each requested state, from the JSON the task wrote. A requested state the result does not list
    is a failed one. Raises RuntimeError when the object is absent, unreadable or not this run's."""
    from ddp_sync.pipelines.cloud_scrape_trigger import _s3_client

    bucket = fargate_cfg.get("memory_bucket", os.environ.get("MEMORY_BUCKET", ""))
    if not bucket:
        raise RuntimeError("no memory_bucket configured")
    key = f"{WORKING_TIER_PREFIX}/people-refresh/{run_id}/{RESULT_NAME}"
    try:
        document = json.loads((s3_client or _s3_client()).get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception as e:
        raise RuntimeError(f"cannot read the task's result {key}: {type(e).__name__}") from e
    if not isinstance(document, dict) or document.get("run_id") != run_id or not isinstance(document.get("states"), dict):
        raise RuntimeError(f"the task's result {key} is not this run's people-refresh JSON")
    listed = document["states"]
    return {s: listed[s] if isinstance(listed.get(s), dict) else {"status": "failed", "error": "not in the result"} for s in states}


async def finish(results: dict[str, Any] | None, states: list[str], config: dict | None, started_at: datetime,
                 duration: float, error: str | None = None) -> dict[str, Any]:
    """Act on a run's outcome: the status record, one alert if anything failed, then the broker scorecard signal for the
    states that worked only. `results` None (the task or its result is unusable) means every state failed, `error` says why."""
    from ddp_sync.pipelines.openstates_scrape import (
        _alert_scrape_failure,
        _maybe_signal_broker_scorecard_refresh_after_people,
        _write_flow_status,
    )

    results = results or {}
    worked = [s for s in states if (results.get(s) or {}).get("status") == "done"]
    failed = [s for s in states if s not in worked]
    any_failure = bool(failed or error)  # `error` with no state listed (no manifest, a bad record) is still a failure
    status = {
        "flow": "openstates_people_refresh",
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "failed" if any_failure else "completed",
        "duration_seconds": duration,
        "states_ok": worked,
        "states_failed": failed,
    }
    if any_failure:
        status["error"] = error or f"states_failed: {','.join(failed)}"
    await _write_flow_status("openstates_people_refresh", status)
    if any_failure:
        logger.error("openstates_people_refresh: failed", states_failed=failed, states_ok=len(worked), error=error)
        await asyncio.to_thread(
            _alert_scrape_failure, "people refresh",
            error or f"failed for {len(failed)} of {len(states)} states: {', '.join(failed)}", duration,
        )
    else:
        logger.info("openstates_people_refresh: done", states=len(worked), duration_seconds=duration)
    if worked:
        await _maybe_signal_broker_scorecard_refresh_after_people(config, states=worked)
    return {"success": not any_failure, "states_ok": worked, "states_failed": failed, "duration_seconds": duration,
            **({"error": status["error"]} if any_failure else {})}


async def run_on_fargate(config: dict | None, *, ecs_client=None, s3_client=None) -> dict[str, Any]:
    """The Fargate version of `run_people_refresh_job`'s body (same status record, same alert, same result shape)."""
    import boto3

    from ddp_sync.pipelines.openstates_archive import redact_dsn
    from ddp_sync.pipelines.openstates_scrape import _get_root

    started_at = datetime.now(timezone.utc)
    t = time.monotonic()
    logger.info("openstates_people_refresh: starting", on_fargate=True)
    run_id = None
    states: list[str] = []
    try:
        states = enrollment.manifest_codes("people_refresh", _get_root(config)) or []
        if not states:  # as the script: no manifest or an empty list is an error, it must not quietly refresh nobody
            raise RuntimeError("cannot read enrollment.people_refresh from jurisdictions.yaml")
        run_id = new_run_id()
        client = ecs_client or boto3.client("ecs")
        task_arn, fargate_cfg = await asyncio.to_thread(launch_task, states, run_id, config, ecs_client=client)
        results = await asyncio.to_thread(await_result, run_id, task_arn, fargate_cfg, client, states, s3_client)
    except Exception as e:  # noqa: BLE001 -- the status record and the alert say it; never raise into the scheduler
        outcome = await finish(None, states, config, started_at, round(time.monotonic() - t, 1), error=redact_dsn(str(e)))
    else:
        outcome = await finish(results, states, config, started_at, round(time.monotonic() - t, 1))
    # Reached on success and on any error, not on a cancel (ddp-sync stopping): a cancelled wait keeps the record, which is
    # what lets the next process finish the job.
    if run_id:
        inflight_fargate_jobs.clear(run_id)
    return outcome


def _load_scrape_config() -> dict[str, Any]:
    """The `openstates_scrape` section, for a resumed job (the record holds no config; the signal and the status record
    need the switch and the root). Same loader shape as openstates_archive's `_load_archive_config_for_scrape_trigger`."""
    try:
        import yaml

        from ddp_sync.scheduler import DEFAULT_CONFIG_PATH

        with open(DEFAULT_CONFIG_PATH) as f:
            return (yaml.safe_load(f) or {}).get("openstates_scrape", {})
    except Exception as e:  # noqa: BLE001
        logger.warning("people_refresh_resume_config_load_failed", error=str(e))
        return {}


async def resume_inflight_people_refresh_job(run_id: str, record: dict[str, Any], *, ecs_client=None, s3_client=None) -> None:
    """Finish a refresh a previous ddp-sync process left in flight (called by `reconcile_inflight_fargate_jobs` at startup
    for each `kind: "people_refresh"` record). Waits for the recorded task (nothing is launched), reads the per-state
    result, then does what a normal run does (status record, alert, signal for the states that worked) and clears the
    record. A record missing its fields is alerted and dropped. A task ECS forgot (about an hour after it stopped) is read
    straight from S3; with no result there it is a failed refresh, alerted: the next Sunday's run covers it. Never raises
    (a cancel aside, which keeps the record)."""
    import boto3

    from ddp_sync.pipelines.openstates_archive import _task_known_to_ecs, redact_dsn

    started_at = datetime.now(timezone.utc)
    t = time.monotonic()
    states: list[str] = []
    try:
        task_arn, fargate_cfg = record["task_arn"], record["fargate_cfg"]
        states = list(record["states"])
    except (KeyError, TypeError) as e:
        logger.warning("people_refresh_resume_record_unusable", run_id=run_id, error=repr(e))
        await finish(None, [], None, started_at, 0.0, error=f"unusable in-flight record {run_id}: {e!r}")
        inflight_fargate_jobs.clear(run_id)
        return
    config = await asyncio.to_thread(_load_scrape_config)
    try:
        client = ecs_client or boto3.client("ecs")
        if await asyncio.to_thread(_task_known_to_ecs, task_arn, fargate_cfg, client):
            results = await asyncio.to_thread(await_result, run_id, task_arn, fargate_cfg, client, states, s3_client)
        else:  # ECS forgets a stopped task after about an hour: its result may still be in S3
            results = await asyncio.to_thread(read_result, run_id, fargate_cfg, states, s3_client)
    except Exception as e:  # noqa: BLE001 -- runs unobserved on a background task
        await finish(None, states, config, started_at, round(time.monotonic() - t, 1), error=redact_dsn(str(e)))
    else:
        await finish(results, states, config, started_at, round(time.monotonic() - t, 1))
    inflight_fargate_jobs.clear(run_id)
