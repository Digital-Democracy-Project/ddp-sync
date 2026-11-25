"""OPEN-855: after each state's load, alert when a roll-call voter name matches no one.

Unmatched voter names (a printed name core's `resolve_person()` tied to no legislator, so the vote is stored with no
voter) used to be found by hand in onboarding's Stage 3 (Maine's 17, Kentucky's "Lewis C", New Mexico's "POPE, JR.").
Once a state was live nothing noticed a new one. `run_state_pipeline` now calls `maybe_alert_unmatched_voters` once per
run, after the broker signals and before the archive, when at least one session succeeded (the load into RDS is part of
the scrape step).

* **The count is not made here.** ddp-open-states' `unmatched-voters.py <state>` (with `qa_primitives.
  unresolved_voter_names`, Stage 3's Gate 1 rule: a person-vote with `voter_id` NULL) prints every session's vote and
  unmatched totals and every unmatched name with its count, read-only, as JSON. It runs with the openstates venv's
  Python (ddp-sync's own has no Postgres driver for it) and the live RDS login (`resolve_rds_database_url`), like
  `cloud_loader.py`.
* **Alert** (`post_alert`, #automation-errors) when the state has a name it did not have at its last run, in a session,
  or a session's unmatched share rose above its last run; it lists up to 10 names with counts, new ones first. Names the manifest
  accepts (`quality.accepted_unmatched_voters`, marked `accepted` by the script) are left out of both.
* **Memory:** the last run's names and shares in Redis (`ddp:unmatched_voters:<code>`, no expiry), written only after a
  run that needed no alert or whose alert was delivered, so an undelivered alert is tried again next run. With no
  memory (the first run after the release, or Redis down) every unmatched name counts as new.

* **Where the count runs (OPEN-881).** `unmatched_voter_alert.on_fargate` (default false: the local path above, kept for one
  release as the rollback) true launches ddp-open-states' `cloud_unmatched_voters.py` as a Fargate task of the scrapers
  image on `task_definition_db` (`ddp-scrapers-db`, the one definition that may hold the database login), through the
  archive's launcher, and waits for it. The task writes the SAME JSON to S3 under its run id and ddp-sync reads it; the
  comparison, the Redis memory and the alert are the same code either way. The task is in the in-flight record
  (`kind: "unmatched_voters"`, kept until the comparison has run), so a restarted ddp-sync reads its result and still alerts.

Switch: `openstates_scrape.unmatched_voter_alert.enabled`. No per-state list: every scraped state is checked. Never
raises (the caller's `_logged` would catch it anyway) and never changes the run's outcome.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import uuid
from typing import Any

import structlog

from ddp_sync.pipelines import inflight_fargate_jobs
from ddp_sync.services.rds_credentials import resolve_rds_database_url
from ddp_sync.slack_alerts import post_alert

logger = structlog.get_logger()

# The openstates toolchain's Python in ddp-sync's image (infrastructure/Dockerfile, OPEN-248): psycopg2 and PyYAML.
OPENSTATES_PYTHON = "/opt/venv-openstates/bin/python"
SCRIPT = "unmatched-voters.py"
TIMEOUT_S = 300
TOP_NAMES = 10
_KEY = "ddp:unmatched_voters:{code}"
# OPEN-881: the task's own ceiling (a cold Fargate start plus one read-only query; the local run's limit is 5 minutes),
# and where it writes (ddp-open-states cloud_unmatched_voters.py: `<WORKING_TIER_PREFIX>/<state>/<run_id>/<name>`).
FARGATE_TIMEOUT_S = 900
WORKING_TIER_PREFIX = "working-tier"
RESULT_NAME = "unmatched-voters.json"


def read_unmatched(code: str, openstates_root: str, runner=subprocess.run) -> dict[str, Any]:
    """Run `unmatched-voters.py <code>` against RDS and return its JSON. Raises RuntimeError (never with the connection
    string) when the login cannot be resolved or the script fails."""
    rds_url, error = resolve_rds_database_url()
    if error or not rds_url:
        raise RuntimeError(f"cannot resolve an RDS login: {error}")
    result = runner(
        [OPENSTATES_PYTHON, os.path.join(openstates_root, SCRIPT), code],
        cwd=openstates_root,
        env={**os.environ, "DATABASE_URL": rds_url},
        capture_output=True,
        text=True,
        timeout=TIMEOUT_S,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"{SCRIPT} exited {result.returncode}: {(result.stderr or '')[-300:].strip()}")
    return json.loads(result.stdout)


def new_run_id(code: str) -> str:
    return f"{code}-unmatched-{uuid.uuid4().hex[:12]}"


def read_unmatched_on_fargate(
    code: str, config: dict | None, run_id: str, *, ecs_client=None, s3_client=None
) -> dict[str, Any]:
    """OPEN-881: launch `cloud_unmatched_voters.py <code> <run_id>` (`run_id` from `new_run_id`) on `ddp-scrapers-db`,
    wait for it and return the JSON it wrote to S3. Blocking (run it in a thread). Raises RuntimeError, never with a connection string, when the task
    cannot start, fails, or leaves no readable result. The task's ARN is in the in-flight record from launch until
    `_compare_and_alert` has run (the caller clears it), so a restart during the wait still finds the result."""
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
        code, run_id, rds_url, fargate_cfg, client, runner="cloud_unmatched_voters.py", command=[code, run_id]
    )
    if not started:
        raise RuntimeError(f"task did not start: {detail}")
    inflight_fargate_jobs.record_unmatched_voters_started(run_id, code, task_arn, fargate_cfg)
    return _await_result(code, run_id, task_arn, fargate_cfg, client, s3_client)


def _await_result(code: str, run_id: str, task_arn: str, fargate_cfg: dict, ecs_client, s3_client=None) -> dict[str, Any]:
    """Wait for the recorded task to stop (a hard stop at `FARGATE_TIMEOUT_S`), then read and check its result."""
    from ddp_sync.pipelines.cloud_scrape_trigger import _wait_for_task_stop
    from ddp_sync.pipelines.openstates_archive import redact_dsn

    wait_cfg = {**fargate_cfg, "max_wait_seconds": FARGATE_TIMEOUT_S}
    exit_code, detail = _wait_for_task_stop(task_arn, wait_cfg, ecs_client, label=f"{code} unmatched voters")
    if exit_code != 0:
        raise RuntimeError(f"task exited {exit_code} (task_arn={task_arn}): {redact_dsn(detail)}")
    return _read_result(code, run_id, fargate_cfg, s3_client)


def _read_result(code: str, run_id: str, fargate_cfg: dict, s3_client=None) -> dict[str, Any]:
    """The JSON the task wrote for `run_id`. Raises RuntimeError when it is absent, unreadable, or not this state's."""
    from ddp_sync.pipelines.cloud_scrape_trigger import _s3_client

    bucket = fargate_cfg.get("memory_bucket", os.environ.get("MEMORY_BUCKET", ""))
    if not bucket:
        raise RuntimeError("no memory_bucket configured")
    key = f"{WORKING_TIER_PREFIX}/{code}/{run_id}/{RESULT_NAME}"
    try:
        body = (s3_client or _s3_client()).get_object(Bucket=bucket, Key=key)["Body"].read()
        result = json.loads(body)
    except Exception as e:
        raise RuntimeError(f"cannot read the task's result {key}: {type(e).__name__}") from e
    if not isinstance(result, dict) or result.get("state") != code or "sessions" not in result or "names" not in result:
        raise RuntimeError(f"the task's result {key} is not {code}'s unmatched-voter JSON")
    return result


def snapshot(current: dict[str, Any]) -> dict[str, Any]:
    """What is remembered for the next run: the unaccepted (session, name) pairs, and each session's unaccepted
    unmatched count and total."""
    accepted_counts: dict[str, int] = {}
    names = []
    for entry in current.get("names") or []:
        if entry.get("accepted"):
            accepted_counts[entry["session"]] = accepted_counts.get(entry["session"], 0) + entry["count"]
        else:
            names.append([entry["session"], entry["name"]])
    shares = {
        session: [counts["unresolved"] - accepted_counts.get(session, 0), counts["total"]]
        for session, counts in (current.get("sessions") or {}).items()
    }
    return {"names": sorted(names), "shares": shares}


def _share(pair: list[int]) -> float:
    unmatched, total = pair
    return unmatched / total if total else 0.0


def compare(current: dict[str, Any], previous: dict[str, Any] | None) -> tuple[list[dict], list[tuple[str, float, float]]]:
    """(new names, sessions whose share rose). A new name is an unaccepted (session, name) not seen at the last run (all
    of them when there is no last run); a rise is a session whose unaccepted unmatched share is above its last run's."""
    now = snapshot(current)
    seen = {tuple(pair) for pair in (previous or {}).get("names") or []}
    new = [
        entry for entry in current.get("names") or []
        if not entry.get("accepted") and (entry["session"], entry["name"]) not in seen
    ]
    risen = []
    for session, pair in sorted(now["shares"].items()):
        before = ((previous or {}).get("shares") or {}).get(session)
        if before is not None and _share(pair) > _share(before):
            risen.append((session, _share(before), _share(pair)))
    return new, risen


def alert_text(code: str, current: dict[str, Any], new: list[dict], risen: list[tuple[str, float, float]],
               first_run: bool) -> str:
    """The alert: what changed, then up to TOP_NAMES unaccepted names, each with its votes summed over sessions: the new
    names first (the ones that raised it, so a small new one is never hidden behind big old ones), then the rest."""
    new_names = {entry["name"] for entry in new}
    totals: dict[str, list] = {}
    for entry in current.get("names") or []:
        if not entry.get("accepted"):
            total = totals.setdefault(entry["name"], [0, []])
            total[0] += entry["count"]
            total[1].append(entry["session"])
    lines = [f":warning: {code.upper()} roll-call voter names that match no legislator after this load"
             + (" (first check: nothing earlier to compare with)" if first_run else "")]
    if new:
        lines.append(f"{len(new_names)} new name(s).")
    for session, before, after in risen:
        lines.append(f"Session {session}: unmatched share rose from {before:.2%} to {after:.2%}.")
    lines.append(f"Names (of {len(totals)}, new first):")
    ranked = sorted(totals.items(), key=lambda item: (item[0] not in new_names, -item[1][0], item[0]))
    for name, (count, sessions) in ranked[:TOP_NAMES]:
        mark = " (new)" if name in new_names else ""
        lines.append(f"• \"{name}\": {count:,} votes in {', '.join(sorted(sessions))}{mark}")
    if len(new_names) > TOP_NAMES:
        lines.append(f"... and {len(new_names) - TOP_NAMES} more new name(s).")
    lines.append("Fix: add the printed name to the member's people file (other_names), or list it under the state's "
                 "quality.accepted_unmatched_voters in ddp-open-states' jurisdictions.yaml.")
    return "\n".join(lines)


async def _remembered(code: str) -> dict[str, Any] | None:
    from ddp_sync.services.redis_store import get_redis_store

    client = get_redis_store()._client
    if client is None:
        return None
    raw = await client.get(_KEY.format(code=code))
    return json.loads(raw) if raw else None


async def _remember(code: str, value: dict[str, Any]) -> None:
    from ddp_sync.services.redis_store import get_redis_store

    client = get_redis_store()._client
    if client is not None:
        await client.set(_KEY.format(code=code), json.dumps(value))


async def _compare_and_alert(code: str, current: dict[str, Any]) -> None:
    """Compare one state's unmatched names with the last run's, alert on a change, and remember them (module docstring).
    The same for a result read locally, from a task just waited for, or from a task a restart picked up."""
    try:
        previous = await _remembered(code)
    except Exception as e:  # noqa: BLE001 -- no memory means every name counts as new: louder, never quieter
        logger.warning("unmatched_voter_memory_unreadable", jurisdiction=code, error=str(e))
        previous = None
    new, risen = compare(current, previous)
    delivered = True
    if new or risen:
        text = alert_text(code, current, new, risen, first_run=previous is None)
        delivered = await asyncio.to_thread(post_alert, text, source="openstates_scrape.unmatched_voters")
    logger.info("unmatched_voter_check", jurisdiction=code, new_names=len(new), risen_sessions=len(risen),
                alerted=bool(new or risen), delivered=delivered)
    if delivered:
        await _remember(code, snapshot(current))


def _redacted(error: BaseException) -> str:
    from ddp_sync.pipelines.openstates_archive import redact_dsn

    return redact_dsn(str(error))


async def maybe_alert_unmatched_voters(jurisdiction: str, config: dict | None) -> None:
    """Check one state after its load and alert on a new unmatched voter name or a rising share (module docstring)."""
    run_id = None
    try:
        block = (config or {}).get("unmatched_voter_alert") or {}
        if not block.get("enabled", False):
            return
        from ddp_sync.pipelines.openstates_archive import _archive_jurisdiction_code
        from ddp_sync.pipelines.openstates_scrape import _get_root

        code = _archive_jurisdiction_code(jurisdiction).lower()
        if block.get("on_fargate", False):  # OPEN-881; false = the local path, the one-release rollback
            run_id = new_run_id(code)
            current = await asyncio.to_thread(read_unmatched_on_fargate, code, config, run_id)
        else:
            current = await asyncio.to_thread(read_unmatched, code, _get_root(config))
        await _compare_and_alert(code, current)
    except Exception as e:  # noqa: BLE001 -- must never affect the scrape job's own result
        logger.warning("unmatched_voter_check_failed", jurisdiction=jurisdiction, error=_redacted(e))
    # Reached on success and on any error, not on a cancel (ddp-sync stopping): a cancelled wait keeps the record, which is
    # what lets the next process finish the check. An errored one is dropped: the next load's check does not need it.
    if run_id:
        inflight_fargate_jobs.clear(run_id)


async def resume_inflight_unmatched_voters_job(run_id: str, record: dict[str, Any], *, ecs_client=None, s3_client=None) -> None:
    """OPEN-881: finish a check a previous ddp-sync process left in flight (called by `reconcile_inflight_fargate_jobs` at
    startup for each `kind: "unmatched_voters"` record). Waits for the recorded task (nothing is launched), reads its
    result, then compares, alerts and remembers exactly as a normal check, and clears the record. A record missing its
    fields, a task that failed, ECS no longer knowing the task with no result in S3, or any other error is logged and the
    record dropped: the check never changes anything, and the next load runs it again. Never raises (a cancel aside)."""
    code = record.get("jurisdiction") or run_id
    try:
        task_arn, fargate_cfg = record["task_arn"], record["fargate_cfg"]
    except KeyError as e:
        logger.warning("unmatched_voter_resume_record_unusable", run_id=run_id, error=repr(e))
        inflight_fargate_jobs.clear(run_id)
        return
    try:
        import boto3

        from ddp_sync.pipelines.openstates_archive import _task_known_to_ecs

        client = ecs_client or boto3.client("ecs")
        if await asyncio.to_thread(_task_known_to_ecs, task_arn, fargate_cfg, client):
            current = await asyncio.to_thread(_await_result, code, run_id, task_arn, fargate_cfg, client, s3_client)
        else:  # ECS forgets a stopped task after about an hour: its result may still be in S3
            current = await asyncio.to_thread(_read_result, code, run_id, fargate_cfg, s3_client)
        await _compare_and_alert(code, current)
    except Exception as e:  # noqa: BLE001 -- runs unobserved on a background task
        logger.warning("unmatched_voter_check_failed", jurisdiction=code, run_id=run_id, resumed=True, error=_redacted(e))
    inflight_fargate_jobs.clear(run_id)
