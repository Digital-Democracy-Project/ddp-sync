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
