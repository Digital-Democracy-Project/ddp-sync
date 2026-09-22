"""Pipeline for running backfill-vote-person-resolution.py as a Fargate task.

SYNC-74 (VOTEBOT-7/OPEN-2 recurrence): resolve_person() and the underlying Person/
PersonIdentifier/Membership data are confirmed correct today, but a vote's voter_id is only
ever resolved once, at import time, and never retried on a later scrape of unchanged content.
The fix is re-running backfill-vote-person-resolution.py (ddp-open-states repo root) against
the already-imported rows that are still null.

Deliberately modeled on openstates_backfill.py's run_backfill_job (OPEN-268), reusing its
_fetch_task_output directly rather than a third copy of the same CloudWatch-polling logic --
same "resolve RDS live, pass it as a containerOverrides.environment DATABASE_URL value at
run_task time" shape. A separate module rather than folding into openstates_backfill.py
because the two scripts have genuinely different CLI shapes: os-text-extract's subcommands all
take a jurisdiction (positional `state`) and an optional `--session`; the vote-person backfill
takes neither -- it's scoped internally to US Congress and just accepts --dry-run/--commit. See
openstates_backfill.py's own module docstring for why the archiver's functions aren't reused
directly either.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import boto3
import structlog

from ddp_sync.pipelines.openstates_backfill import _fetch_task_output
from ddp_sync.services.rds_credentials import resolve_rds_database_url

logger = structlog.get_logger()

ALLOWED_MODES = frozenset({"dry-run", "commit"})

# backfill-vote-person-resolution.py is already a complete, directly-executable script
# (argparse --dry-run) copied into /app/ alongside the other RUNNER_SCRIPT-selectable entry
# points -- no separate thin wrapper needed the way cloud_text_extract.py wraps os-text-extract.
_RUNNER_SCRIPT = "backfill-vote-person-resolution.py"


def _build_command(mode: str) -> list[str]:
    return ["--dry-run"] if mode == "dry-run" else []


def _launch_vote_person_backfill_fargate_task(
    mode: str,
    run_id: str,
    rds_url: str,
    fargate_cfg: dict,
    ecs_client,
) -> tuple[bool, str | None, str]:
    """Launch backfill-vote-person-resolution.py as a Fargate task. Same DATABASE_URL-override
    shape as openstates_backfill._launch_backfill_fargate_task -- see that function's docstring
    for why this is a small dedicated function rather than a further-generalized shared one.
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
                        "command": _build_command(mode),
                        "environment": [
                            {"name": "RUN_ID", "value": run_id},
                            {"name": "RUNNER_SCRIPT", "value": _RUNNER_SCRIPT},
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


async def run_vote_person_backfill_job(
    mode: str = "dry-run",
    config: dict | None = None,
    ecs_client=None,
    logs_client=None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run backfill-vote-person-resolution.py as a Fargate task. See openstates_backfill.py's
    run_backfill_job for the identical shape and rationale (run_id threading, etc.) this
    mirrors.
    """
    import asyncio

    from ddp_sync.pipelines.cloud_scrape_trigger import (
        _fargate_config,
        _wait_for_task_stop,
    )

    run_id = run_id or f"vote-person-backfill-{mode}-{uuid.uuid4().hex[:12]}"

    if mode not in ALLOWED_MODES:
        return {
            "success": False,
            "error": f"unknown_mode: {mode} (allowed: {sorted(ALLOWED_MODES)})",
            "run_id": run_id,
            "duration_seconds": 0.0,
        }

    # OPEN-260: resolved live from Secrets Manager on every launch -- never a cached value, see
    # openstates_archive._run_archive_fargate's identical comment.
    rds_url, rds_error = resolve_rds_database_url()
    if rds_error:
        error = f"cannot resolve an RDS target: {rds_error}"
        logger.error("vote_person_backfill: fargate launch refused", run_id=run_id, error=error)
        return {"success": False, "error": error, "run_id": run_id, "duration_seconds": 0.0}

    try:
        fargate_cfg = _fargate_config(config)
    except ValueError as e:
        logger.error("vote_person_backfill: fargate config error", run_id=run_id, error=str(e))
        return {
            "success": False,
            "error": f"config_error: {e}",
            "run_id": run_id,
            "duration_seconds": 0.0,
        }

    client = ecs_client or boto3.client("ecs")

    start = time.monotonic()
    started, task_arn, detail = _launch_vote_person_backfill_fargate_task(
        mode, run_id, rds_url, fargate_cfg, client
    )
    if not started:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "vote_person_backfill: fargate launch failed",
            run_id=run_id,
            detail=detail,
        )
        return {
            "success": False,
            "error": f"run_task_failed: {detail}",
            "run_id": run_id,
            "duration_seconds": duration,
        }

    logger.info("vote_person_backfill: fargate task launched", run_id=run_id, task_arn=task_arn)

    exit_code, wait_detail = await asyncio.to_thread(
        _wait_for_task_stop, task_arn, fargate_cfg, client
    )
    logs_client_ = logs_client or boto3.client("logs")
    output = await asyncio.to_thread(_fetch_task_output, task_arn, fargate_cfg, logs_client_)
    duration = round(time.monotonic() - start, 1)

    if exit_code != 0:
        error = "exit_code_none" if exit_code is None else f"exit_code_{exit_code}"
        logger.error(
            "vote_person_backfill: fargate task failed",
            run_id=run_id,
            exit_code=exit_code,
            detail=wait_detail,
            duration_seconds=duration,
            output=output,
        )
        return {
            "success": False,
            "error": error,
            "run_id": run_id,
            "mode": mode,
            "duration_seconds": duration,
            "output": output,
        }

    logger.info(
        "vote_person_backfill: fargate task done",
        run_id=run_id,
        mode=mode,
        duration_seconds=duration,
        output=output,
    )
    return {
        "success": True,
        "run_id": run_id,
        "mode": mode,
        "duration_seconds": duration,
        "output": output,
    }
