"""Pipeline for running os-text-extract's ad-hoc data-quality commands as a Fargate task.

OPEN-268: the RDS backfill work this ticket comes out of (reextract/refresh-extraction/
recompute-diff-order, run against MI/UT/FL/WA/VA/US) had been running directly in the EC2
host's own bare venv -- the same host that also runs ddp-broker/api-v3/ddp-sync in production,
sharing its limited CPU/memory, and drifting out of sync with whatever toolchain (Python,
poppler, ...) is actually deployed to Fargate. Neither problem exists if these commands run as
isolated Fargate tasks instead, using the exact image that's actually deployed.

Deliberately modeled on openstates_archive.py's _run_archive_fargate/_launch_archive_fargate_task
(OPEN-192/OPEN-260) rather than reusing them directly -- same "resolve RDS live, pass it as a
containerOverrides.environment value at run_task time" shape, but a different RUNNER_SCRIPT
(cloud_text_extract.py, OPEN-268) and a different, caller-supplied command (subcommand +
jurisdiction + flags) rather than a fixed one-argument invocation.

Unlike the archiver, this is not on a schedule -- there is no unattended cron here for a failure
to go unnoticed by, so this file deliberately does not add Slack/CAMS alerting (openstates_
archive.py's own reasoning for having that: nobody is otherwise watching a scheduled run).
Whoever calls run_backfill_job (a human, or the prod agent) is already watching for the result.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import boto3
import structlog

from ddp_sync.services.rds_credentials import resolve_rds_database_url

logger = structlog.get_logger()

# Matches openstates-core's os-text-extract CLI (openstates/cli/text_extract.py) exactly --
# these three all take a jurisdiction (positional `state`), an optional `--session`, and a
# `--dry-run`/`--commit` flag pair. Anything else on that CLI (e.g. `archive`) already has its
# own dedicated path (cloud_archiver.py) and does not belong here.
ALLOWED_SUBCOMMANDS = frozenset({"reextract", "refresh-extraction", "recompute-diff-order"})
ALLOWED_MODES = frozenset({"dry-run", "commit"})

# Same log group every task in this cluster writes to (logging.tf / ecs.tf's own
# awslogs-group), overridable via cloud_path.fargate.log_group for a differently-configured
# cluster rather than assumed unconditionally.
_DEFAULT_LOG_GROUP = "/aws/ecs/ddp-scrapers"
_LOG_FETCH_ATTEMPTS = 5
_LOG_FETCH_RETRY_S = 2.0


def _build_command(
    subcommand: str, jurisdiction: str, mode: str, session: str | None
) -> list[str]:
    command = [subcommand, jurisdiction]
    if session:
        command.extend(["--session", session])
    command.append("--commit" if mode == "commit" else "--dry-run")
    return command


def _launch_backfill_fargate_task(
    jurisdiction: str,
    subcommand: str,
    mode: str,
    session: str | None,
    run_id: str,
    rds_url: str,
    fargate_cfg: dict,
    ecs_client,
) -> tuple[bool, str | None, str]:
    """Launch cloud_text_extract.py (which just execs os-text-extract, see its own docstring)
    as a Fargate task. Same DATABASE_URL-override shape as
    openstates_archive._launch_archive_fargate_task -- see that function's docstring for why
    this is a small dedicated function rather than a further-generalized shared one.

    No MEMORY_BUCKET/MEMORY_PREFIX here: os-text-extract's data-quality subcommands (unlike
    cloud_collector.py/cloud_archiver.py) never touch the S3 memory/working-tier store --
    recompute-diff-order works entirely from already-stored raw_text in Postgres, and
    reextract/refresh-extraction read already-archived document bytes straight from
    S3_BILL_ARCHIVE_BUCKET via the task role's own S3 access, not the memory store.
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
                        "command": _build_command(subcommand, jurisdiction, mode, session),
                        "environment": [
                            {"name": "RUN_ID", "value": run_id},
                            {"name": "RUNNER_SCRIPT", "value": "cloud_text_extract.py"},
                            {"name": "DATABASE_URL", "value": rds_url},
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


def _fetch_task_output(task_arn: str, fargate_cfg: dict, logs_client) -> str:
    """Best-effort fetch of this task's own stdout/stderr from CloudWatch Logs.

    A dry-run's entire value is the summary line it prints (e.g. "fl: [DRY RUN] 7685 bills
    checked | unchanged=17325 corrected=2712 nulled=1") -- unlike cloud_archiver.py, which
    writes its result straight to the database and can be verified there, a dry-run writes
    nothing anywhere else. Retries a few times: CloudWatch's own ingestion has a short,
    variable delay after a task stops, and the log stream may not be queryable in the same
    instant DescribeTasks first reports STOPPED.

    Never raises -- a log-fetch failure is real information ("logs unavailable"), not a reason
    to fail a task that otherwise exited 0.
    """
    log_group = fargate_cfg.get("log_group", _DEFAULT_LOG_GROUP)
    stream_prefix = fargate_cfg.get("log_stream_prefix", "scraper")
    container_name = fargate_cfg.get("container_name", "scraper")
    task_id = task_arn.rsplit("/", 1)[-1]
    stream_name = f"{stream_prefix}/{container_name}/{task_id}"

    for attempt in range(_LOG_FETCH_ATTEMPTS):
        try:
            resp = logs_client.get_log_events(
                logGroupName=log_group, logStreamName=stream_name, startFromHead=True
            )
            events = resp.get("events", [])
            if events:
                return "\n".join(e.get("message", "") for e in events)
        except Exception as e:  # noqa: BLE001 -- ResourceNotFoundException while logs are still
            # propagating looks the same as "never will exist" from here; only the retry loop
            # below tells them apart.
            logger.debug(
                "openstates_backfill: log fetch attempt failed",
                stream_name=stream_name,
                attempt=attempt,
                error=str(e),
            )
        if attempt < _LOG_FETCH_ATTEMPTS - 1:
            time.sleep(_LOG_FETCH_RETRY_S)

    return ""


async def run_backfill_job(
    jurisdiction: str,
    subcommand: str,
    mode: str = "dry-run",
    session: str | None = None,
    config: dict | None = None,
    ecs_client=None,
    logs_client=None,
) -> dict[str, Any]:
    """Run one os-text-extract data-quality subcommand for one jurisdiction as a Fargate task.

    This is the whole surface OPEN-268 asks for: an ad-hoc reextract/refresh-extraction/
    recompute-diff-order invocation that runs inside the currently-deployed image instead of
    this EC2 host's own bare venv. Returns a result dict with `output` holding whatever the
    task printed (the dry-run summary line, most importantly) whenever the log fetch succeeds.
    """
    from ddp_sync.pipelines.cloud_scrape_trigger import (
        _fargate_config,
        _wait_for_task_stop,
    )

    if subcommand not in ALLOWED_SUBCOMMANDS:
        return {
            "success": False,
            "error": f"unknown_subcommand: {subcommand} (allowed: {sorted(ALLOWED_SUBCOMMANDS)})",
            "jurisdiction": jurisdiction,
            "duration_seconds": 0.0,
        }
    if mode not in ALLOWED_MODES:
        return {
            "success": False,
            "error": f"unknown_mode: {mode} (allowed: {sorted(ALLOWED_MODES)})",
            "jurisdiction": jurisdiction,
            "duration_seconds": 0.0,
        }

    # OPEN-260: resolved live from Secrets Manager on every launch -- see
    # openstates_archive._run_archive_fargate's identical comment for why this can never be a
    # cached value.
    rds_url, rds_error = resolve_rds_database_url()
    if rds_error:
        error = f"cannot resolve an RDS target: {rds_error}"
        logger.error(
            "openstates_backfill: fargate launch refused",
            jurisdiction=jurisdiction,
            subcommand=subcommand,
            error=error,
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
            "openstates_backfill: fargate config error",
            jurisdiction=jurisdiction,
            subcommand=subcommand,
            error=str(e),
        )
        return {
            "success": False,
            "error": f"config_error: {e}",
            "jurisdiction": jurisdiction,
            "duration_seconds": 0.0,
        }

    client = ecs_client or boto3.client("ecs")
    run_id = f"{jurisdiction}-{subcommand}-{mode}-{uuid.uuid4().hex[:12]}"

    start = time.monotonic()
    started, task_arn, detail = _launch_backfill_fargate_task(
        jurisdiction, subcommand, mode, session, run_id, rds_url, fargate_cfg, client
    )
    if not started:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "openstates_backfill: fargate launch failed",
            jurisdiction=jurisdiction,
            subcommand=subcommand,
            detail=detail,
        )
        return {
            "success": False,
            "error": f"run_task_failed: {detail}",
            "jurisdiction": jurisdiction,
            "duration_seconds": duration,
        }

    exit_code, wait_detail = await asyncio.to_thread(
        _wait_for_task_stop, task_arn, fargate_cfg, client
    )
    logs_client_ = logs_client or boto3.client("logs")
    output = await asyncio.to_thread(_fetch_task_output, task_arn, fargate_cfg, logs_client_)
    duration = round(time.monotonic() - start, 1)

    if exit_code != 0:
        error = "exit_code_none" if exit_code is None else f"exit_code_{exit_code}"
        logger.error(
            "openstates_backfill: fargate task failed",
            jurisdiction=jurisdiction,
            subcommand=subcommand,
            exit_code=exit_code,
            detail=wait_detail,
            duration_seconds=duration,
        )
        return {
            "success": False,
            "error": error,
            "jurisdiction": jurisdiction,
            "subcommand": subcommand,
            "mode": mode,
            "duration_seconds": duration,
            "output": output,
        }

    logger.info(
        "openstates_backfill: fargate task done",
        jurisdiction=jurisdiction,
        subcommand=subcommand,
        mode=mode,
        duration_seconds=duration,
    )
    return {
        "success": True,
        "jurisdiction": jurisdiction,
        "subcommand": subcommand,
        "mode": mode,
        "duration_seconds": duration,
        "output": output,
    }
