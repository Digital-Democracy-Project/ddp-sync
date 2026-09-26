"""Pipeline for running a registered one-off data-patch script as a Fargate task.

SYNC-74 (VOTEBOT-7/OPEN-2 recurrence): resolve_person() and the underlying Person/
PersonIdentifier/Membership data are confirmed correct today, but a vote's voter_id is only
ever resolved once, at import time, and never retried on a later scrape of unchanged content.
The fix is re-running backfill-vote-person-resolution.py (ddp-open-states repo root) against
the already-imported rows that are still null.

Generalized 2026-09-25 (OPEN-304 recurrence) to look up a `job` key in JOBS below rather than
hardcoding a single runner script. OPEN-304's own one-off script (open304-add-lis-identifiers.py)
needed this exact same shape -- launch RUNNER_SCRIPT on Fargate with a live-resolved
DATABASE_URL, --dry-run/no-flag for dry-run/commit -- and building a third near-identical module
would just repeat the same copy-paste this generalization avoids. A script belongs in JOBS only
if it shares that identical CLI shape (argparse --dry-run, no other required arguments); a script
with a genuinely different shape (a jurisdiction/subcommand pair, e.g.) still belongs in its own
module, the same reasoning openstates_backfill.py's own docstring gives for not folding *this*
one in there.

Deliberately modeled on openstates_backfill.py's run_backfill_job (OPEN-268), reusing its
_fetch_task_output directly rather than a third copy of the same CloudWatch-polling logic.
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

# job key -> runner script filename. Every entry here must already be copied into /app/ in the
# ddp-scrapers image (Dockerfile's COPY line) and be selectable via docker-entrypoint.sh's
# RUNNER_SCRIPT dispatch, and must take exactly the same CLI shape _build_command below builds
# for (argparse --dry-run, nothing else required) -- see the module docstring above.
JOBS = {
    "vote-person-backfill": "backfill-vote-person-resolution.py",
    "open304-lis-identifiers": "open304-add-lis-identifiers.py",
}


def _build_command(mode: str) -> list[str]:
    return ["--dry-run"] if mode == "dry-run" else []


def _launch_fargate_script_task(
    runner_script: str,
    mode: str,
    run_id: str,
    rds_url: str,
    fargate_cfg: dict,
    ecs_client,
) -> tuple[bool, str | None, str]:
    """Launch a registered one-off script as a Fargate task. Same DATABASE_URL-override shape
    as openstates_backfill._launch_backfill_fargate_task -- see that function's docstring for
    why this is a small dedicated function rather than a further-generalized shared one.
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
                            {"name": "RUNNER_SCRIPT", "value": runner_script},
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


async def run_fargate_script_job(
    job: str,
    mode: str = "dry-run",
    config: dict | None = None,
    ecs_client=None,
    logs_client=None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run one of JOBS' registered one-off scripts as a Fargate task. See
    openstates_backfill.py's run_backfill_job for the identical shape and rationale (run_id
    threading, etc.) this mirrors.
    """
    import asyncio

    from ddp_sync.pipelines.cloud_scrape_trigger import (
        _fargate_config,
        _wait_for_task_stop,
    )

    run_id = run_id or f"{job}-{mode}-{uuid.uuid4().hex[:12]}"

    if job not in JOBS:
        return {
            "success": False,
            "error": f"unknown_job: {job} (allowed: {sorted(JOBS)})",
            "run_id": run_id,
            "job": job,
            "duration_seconds": 0.0,
        }
    runner_script = JOBS[job]

    if mode not in ALLOWED_MODES:
        return {
            "success": False,
            "error": f"unknown_mode: {mode} (allowed: {sorted(ALLOWED_MODES)})",
            "run_id": run_id,
            "job": job,
            "duration_seconds": 0.0,
        }

    # OPEN-260: resolved live from Secrets Manager on every launch -- never a cached value, see
    # openstates_archive._run_archive_fargate's identical comment.
    rds_url, rds_error = resolve_rds_database_url()
    if rds_error:
        error = f"cannot resolve an RDS target: {rds_error}"
        logger.error("fargate_script: fargate launch refused", run_id=run_id, job=job, error=error)
        return {"success": False, "error": error, "run_id": run_id, "job": job, "duration_seconds": 0.0}

    try:
        fargate_cfg = _fargate_config(config)
    except ValueError as e:
        logger.error("fargate_script: fargate config error", run_id=run_id, job=job, error=str(e))
        return {
            "success": False,
            "error": f"config_error: {e}",
            "run_id": run_id,
            "job": job,
            "duration_seconds": 0.0,
        }

    client = ecs_client or boto3.client("ecs")

    start = time.monotonic()
    started, task_arn, detail = _launch_fargate_script_task(
        runner_script, mode, run_id, rds_url, fargate_cfg, client
    )
    if not started:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "fargate_script: fargate launch failed",
            run_id=run_id,
            job=job,
            detail=detail,
        )
        return {
            "success": False,
            "error": f"run_task_failed: {detail}",
            "run_id": run_id,
            "job": job,
            "duration_seconds": duration,
        }

    logger.info("fargate_script: fargate task launched", run_id=run_id, job=job, task_arn=task_arn)

    exit_code, wait_detail = await asyncio.to_thread(
        _wait_for_task_stop, task_arn, fargate_cfg, client
    )
    logs_client_ = logs_client or boto3.client("logs")
    output = await asyncio.to_thread(_fetch_task_output, task_arn, fargate_cfg, logs_client_)
    duration = round(time.monotonic() - start, 1)

    if exit_code != 0:
        error = "exit_code_none" if exit_code is None else f"exit_code_{exit_code}"
        logger.error(
            "fargate_script: fargate task failed",
            run_id=run_id,
            job=job,
            exit_code=exit_code,
            detail=wait_detail,
            duration_seconds=duration,
            output=output,
        )
        return {
            "success": False,
            "error": error,
            "run_id": run_id,
            "job": job,
            "mode": mode,
            "duration_seconds": duration,
            "output": output,
        }

    logger.info(
        "fargate_script: fargate task done",
        run_id=run_id,
        job=job,
        mode=mode,
        duration_seconds=duration,
        output=output,
    )
    return {
        "success": True,
        "run_id": run_id,
        "job": job,
        "mode": mode,
        "duration_seconds": duration,
        "output": output,
    }
