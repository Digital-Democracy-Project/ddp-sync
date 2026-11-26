"""Persisted tracking for in-flight Fargate scrape+load jobs (OPEN-251).

cloud_scrape_trigger.py's run_cloud_scrape() launches an ECS task and then blocks for up to
hours waiting for it to stop (its own poll loop) before running the load step -- see that
module's docstring for why. All of that waiting happens in one Python process's memory: if
ddp-sync restarts while a task is still in flight, the ECS task itself keeps running in AWS
untouched, but nothing is left watching for it to finish or to trigger its load into RDS. The
scraped output then just sits there until someone notices -- exactly what produced the
123-object orphaned-data recovery during OPEN-193's canary.

This module gives that in-flight state a durable home in Redis (a plain Hash, one field per
run_id) so a freshly-started process can find and resume a job a previous process was still
watching, rather than losing track of it. Deliberately a thin, single-purpose store -- not a
general job queue: whole JSON blobs in one Hash, no schema migrations, and every operation is
best-effort exactly like services/redis_store.py's own methods (never raises; a Redis outage
degrades to "an interrupted job requires a human to notice and re-trigger it", the same manual
recovery this project already had to do once, not a new failure mode).

No TTL on this Hash, unlike most keys in redis_store.py -- those expire because a stale cache
value should fall back to a computed default; a record here should survive until the job it
describes actually finishes, however many restarts that takes, or it stops being a durable
record at all.

Uses redis-py's SYNC client, not services/redis_store.py's async one: this module's only caller
(cloud_scrape_trigger.py) is plain blocking functions run via asyncio.to_thread, not coroutines
-- bridging back to an async client from inside a worker thread would need its own event-loop
plumbing for no real benefit over the sync client the same `redis` package already ships.
"""

from __future__ import annotations

import json
import re
from typing import Any

import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger(__name__)

INFLIGHT_KEY = "ddp:inflight_fargate_jobs"

_client = None


def _get_client():
    """Lazily construct the sync Redis client. Returns None -- never raises -- if Redis is
    unreachable, and tries again on the next call rather than caching the failure forever, so
    a Redis instance that comes up after ddp-sync does still gets used once it's there."""
    global _client
    if _client is not None:
        return _client
    try:
        import redis

        settings = get_settings()
        client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
        _client = client
        return _client
    except Exception as e:  # noqa: BLE001 -- any import/connection failure degrades to no-op
        logger.warning(
            "inflight_fargate_jobs: Redis unavailable, in-flight tracking disabled", error=str(e)
        )
        return None


def record_started(
    run_id: str,
    jurisdiction: str,
    session_arg: str | None,
    task_arn: str,
    fargate_cfg: dict,
    openstates_root: str,
) -> None:
    """Persist a job the moment its ECS task has actually launched (task_arn known), before the
    -- possibly hours-long -- wait for it to stop. Best-effort: a failed write here just means a
    restart during this job's wait can't be resumed, the same gap OPEN-251 exists to close, not
    a new one -- never worth failing the scrape itself over.
    """
    client = _get_client()
    if client is None:
        return
    record = {
        "run_id": run_id,
        "jurisdiction": jurisdiction,
        "session_arg": session_arg,
        "task_arn": task_arn,
        "fargate_cfg": fargate_cfg,
        "openstates_root": openstates_root,
    }
    try:
        client.hset(INFLIGHT_KEY, run_id, json.dumps(record))
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "inflight_fargate_jobs: failed to record started job", run_id=run_id, error=str(e)
        )


def record_load_started(run_id: str, load_task_arn: str) -> None:
    """OPEN-633: add the load task's ARN to a job's record once its Fargate load task has launched, so a restart
    during the load waits for that task (`resume_inflight_fargate_job`) instead of starting a second load. Best-effort
    like `record_started`: if it is not written, a restart starts the load again, and that second load meets the
    first one's import lock and refuses (nothing imported)."""
    client = _get_client()
    if client is None:
        return
    try:
        raw = client.hget(INFLIGHT_KEY, run_id)
        if raw is None:
            return
        record = json.loads(raw)
        record["load_task_arn"] = load_task_arn
        client.hset(INFLIGHT_KEY, run_id, json.dumps(record))
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "inflight_fargate_jobs: failed to record the load task", run_id=run_id, error=str(e)
        )


def record_archive_started(
    run_id: str, jurisdiction: str, task_arn: str, fargate_cfg: dict, archive_started_at: str, attempt: int
) -> None:
    """SYNC-192: persist an archive's Fargate task the moment it has launched, in the same Hash and with the same
    rules as `record_started`, so a restarted ddp-sync waits for it (`openstates_archive.resume_inflight_archive_job`)
    instead of losing its follow-ups. `kind: "archive"` is what tells `reconcile_inflight_fargate_jobs` apart from a
    scrape record (which has no `kind`). `archive_started_at` is the floor the LegBot hook resolves sessions from;
    `attempt` is which of the archive's retry attempts this task is; `hooks_done` is filled by `record_hook_done`.
    Best-effort: if it is not written, a restart in that gap leaves the task running and its follow-ups unrun, as before."""
    client = _get_client()
    if client is None:
        return
    record = {
        "kind": "archive",
        "run_id": run_id,
        "jurisdiction": jurisdiction,
        "task_arn": task_arn,
        "fargate_cfg": fargate_cfg,
        "archive_started_at": archive_started_at,
        "attempt": attempt,
        "hooks_done": [],
    }
    try:
        client.hset(INFLIGHT_KEY, run_id, json.dumps(record))
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "inflight_fargate_jobs: failed to record the archive task", run_id=run_id, error=str(e)
        )


def record_unmatched_voters_started(run_id: str, jurisdiction: str, task_arn: str, fargate_cfg: dict) -> None:
    """OPEN-881: persist the after-load unmatched-voter check's Fargate task the moment it has launched, in the same Hash
    and with the same rules as `record_archive_started`, so a restarted ddp-sync reads the task's result and still
    compares and alerts (`unmatched_voter_alert.resume_inflight_unmatched_voters_job`). `kind: "unmatched_voters"` is what
    `reconcile_inflight_fargate_jobs` tells it apart by. Best-effort: if it is not written, a restart in that gap loses
    this one check (the next load's check compares with the same last-run memory)."""
    client = _get_client()
    if client is None:
        return
    record = {
        "kind": "unmatched_voters",
        "run_id": run_id,
        "jurisdiction": jurisdiction,
        "task_arn": task_arn,
        "fargate_cfg": fargate_cfg,
    }
    try:
        client.hset(INFLIGHT_KEY, run_id, json.dumps(record))
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "inflight_fargate_jobs: failed to record the unmatched-voter task", run_id=run_id, error=str(e)
        )


def record_people_refresh_started(run_id: str, task_arn: str, fargate_cfg: dict, states: list[str]) -> None:
    """OPEN-882: persist the weekly people refresh's Fargate task the moment it has launched, in the same Hash and with the
    same rules as `record_unmatched_voters_started`, so a restarted ddp-sync reads the per-state result and still alerts and
    signals (`people_refresh_task.resume_inflight_people_refresh_job`). `kind: "people_refresh"`; `states` is what was asked
    for (the result is read for exactly those). Best-effort: if it is not written, a restart in that gap loses this week's
    result handling (the task still ran; the next Sunday's run covers the data)."""
    client = _get_client()
    if client is None:
        return
    record = {
        "kind": "people_refresh",
        "run_id": run_id,
        "task_arn": task_arn,
        "fargate_cfg": fargate_cfg,
        "states": states,
    }
    try:
        client.hset(INFLIGHT_KEY, run_id, json.dumps(record))
    except Exception as e:  # noqa: BLE001
        logger.warning("inflight_fargate_jobs: failed to record the people refresh task", run_id=run_id, error=str(e))


def record_hook_done(run_id: str, hook: str) -> None:
    """SYNC-192: note that one of an archive's follow-ups has finished (run, or failed and logged), so a restart during a
    later one does not run it a second time. Best-effort; a missing record is left missing."""
    client = _get_client()
    if client is None:
        return
    try:
        raw = client.hget(INFLIGHT_KEY, run_id)
        if raw is None:
            return
        record = json.loads(raw)
        if hook not in record.setdefault("hooks_done", []):
            record["hooks_done"].append(hook)
        client.hset(INFLIGHT_KEY, run_id, json.dumps(record))
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "inflight_fargate_jobs: failed to record a finished follow-up", run_id=run_id, hook=hook, error=str(e)
        )


def clear(run_id: str) -> None:
    """Remove a job's record once it has reached a terminal outcome -- collection failure, load
    failure, or success. From here on its fate is already captured by the caller's own
    alerting/return value, so there is nothing left to reconcile after a restart.
    """
    client = _get_client()
    if client is None:
        return
    try:
        client.hdel(INFLIGHT_KEY, run_id)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "inflight_fargate_jobs: failed to clear job record", run_id=run_id, error=str(e)
        )


def list_inflight() -> dict[str, dict[str, Any]]:
    """All persisted in-flight records, keyed by run_id. Empty on any failure -- a Redis outage
    at startup should mean "nothing to reconcile", not a startup crash.
    """
    client = _get_client()
    if client is None:
        return {}
    try:
        raw = client.hgetall(INFLIGHT_KEY)
    except Exception as e:  # noqa: BLE001
        logger.warning("inflight_fargate_jobs: failed to list in-flight jobs", error=str(e))
        return {}
    records: dict[str, dict[str, Any]] = {}
    for run_id, value in raw.items():
        try:
            records[run_id] = json.loads(value)
        except (TypeError, ValueError) as e:
            logger.warning(
                "inflight_fargate_jobs: skipping unparseable record", run_id=run_id, error=str(e)
            )
    return records


# ---- SYNC-197: records the test suite leaked ------------------------------------------------------------------------------
#
# Before SYNC-192's conftest guard, test runs on the Mac wrote archive records into the live Hash (localhost Redis is up there),
# and every ddp-sync restart then tried to resume archives that never existed. A real Fargate task ARN is
# `arn:aws:ecs:<region>:<12-digit account>:task/<cluster>/<task id>`, so the task ids the tests use (below) cannot be one.
# This matches only those exact values, not "any odd record" and not "any record in some state": a real in-flight archive is
# never touched, and anything else in the Hash is listed and left for a person.
_LEAKED_TEST_TASK_ARN = re.compile(r"^(arn:task/archive|arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc(123)?)$")
_ARCHIVE_RUN_ID = re.compile(r"^[a-z]{2,3}-archive-[0-9a-f]{12}$")  # `_run_archive_fargate`'s `<state>-archive-<12 hex>`


def is_leaked_test_archive_record(run_id: str, record: dict[str, Any]) -> bool:
    """True only for an archive record whose run id has the archive shape and whose task id is one the tests use."""
    return (
        record.get("kind") == "archive"
        and bool(_ARCHIVE_RUN_ID.match(run_id))
        and bool(_LEAKED_TEST_TASK_ARN.match(str(record.get("task_arn", ""))))
    )


def purge_leaked_test_archive_records(*, apply: bool = False) -> dict[str, Any]:
    """Look at every record in the in-flight Hash and, with `apply`, delete the leaked test archive records and nothing else.

    A dry run (the default) deletes nothing. Each record is judged on its own content; with `apply`, a leaked record is
    deleted only if it is still exactly what was judged (re-read right before the delete). Returns
    `{"error", "records", "leaked": [...], "kept": [...], "deleted": [...], "changed": [...]}`, each list item
    `{"run_id", "kind", "jurisdiction", "task_arn", "archive_started_at"}`. Redis unreachable: `error` set, nothing touched."""
    report: dict[str, Any] = {"error": None, "records": 0, "leaked": [], "kept": [], "deleted": [], "changed": []}
    client = _get_client()
    if client is None:
        report["error"] = "redis_unavailable"
        return report
    try:
        raw = client.hgetall(INFLIGHT_KEY)
    except Exception as e:  # noqa: BLE001
        report["error"] = f"read_failed: {e}"
        return report
    report["records"] = len(raw)

    def summary(run_id: str, record: dict[str, Any]) -> dict[str, Any]:
        return {key: record.get(key) for key in ("kind", "jurisdiction", "task_arn", "archive_started_at")} | {"run_id": run_id}

    judged: dict[str, str] = {}
    for run_id, value in sorted(raw.items()):
        try:
            record = json.loads(value)
        except (TypeError, ValueError):
            record = None
        if isinstance(record, dict) and is_leaked_test_archive_record(run_id, record):
            report["leaked"].append(summary(run_id, record))
            judged[run_id] = value
        else:
            report["kept"].append(summary(run_id, record) if isinstance(record, dict) else {"run_id": run_id, "unparseable": True})
    if not apply:
        return report
    for item in report["leaked"]:
        run_id = item["run_id"]
        try:
            if client.hget(INFLIGHT_KEY, run_id) != judged[run_id]:  # changed (or gone) since it was judged: leave it
                report["changed"].append(item)
                continue
            if not client.hdel(INFLIGHT_KEY, run_id):  # nothing was removed (already gone): do not report a delete
                report["changed"].append(item)
                continue
            report["deleted"].append(item)
        except Exception as e:  # noqa: BLE001
            report["error"] = f"delete_failed: {e}"
            break
    return report
