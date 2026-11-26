"""SYNC-178: what this ddp-sync process is running right now, for `ddp-sync status` (GET /ddp-sync/v1/status).

Before it, a session pipeline walking a whole session was visible only in the log: CAMS saw nothing while most bills were
skipped, and no status screen could say whether a restart would interrupt it. Two sources feed this register:

* the scheduler: every scheduled job, from APScheduler's own submitted/executed events (`scheduler_listener`);
* the session pipeline (`session_pipeline_runner.run_legbot_pipeline`): its progress per bill (`start`, `bill_started`,
  `bill_finished`, `finish`).

The register itself is in this process's memory: a run is alive exactly while it is in it. Each entry is mirrored to ONE
Redis hash (`RUNNING_KEY`) only so that a run killed by a restart is not lost from sight: at start-up, `sweep_restart_leftovers`
moves what the previous process left there into the recent list as "stopped by a restart". Finished runs go to a short
recent list (`RECENT_KEY`, newest first, `RECENT_LIMIT` entries). Every Redis call fails open: this is a view, and it must
never break or slow a run. Do not add a second register: anything new that runs for a long time reports here.

One process per host is assumed, as SYNC-153's start-up lock clearing already does: uvicorn runs without `--workers`, and a
restart (launchd `kickstart -k`, systemd `restart`) stops the old process before the new one starts, so the sweep never
sees a live process's entries. Running two workers would break this register and the lock clearing alike.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

RUNNING_KEY = "ddp:activity:running"
RECENT_KEY = "ddp:activity:recent"
RECENT_LIMIT = 20
RECENT_TTL_SECONDS = 86400 * 7

_RUNS: dict[str, dict[str, Any]] = {}
_background: set[asyncio.Task] = set()
# The mirror writes run one at a time, in the order they were asked for (pm-review): a run's finish can never reach Redis
# before its start, which would leave a stale entry that the next start-up reports as "stopped by a restart".
_previous_write: asyncio.Task | None = None


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _client():
    try:
        from ddp_sync.services.redis_store import get_redis_store

        return get_redis_store()._client
    except Exception:  # noqa: BLE001 -- a view; never break the caller
        return None


def _in_background(coro) -> None:
    """Run a Redis mirror write without making the caller wait (and without losing the task to garbage collection).
    With no Redis connection there is nothing to write, so no task is created at all."""
    if _client() is None:
        coro.close()
        return
    global _previous_write
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # no running loop (a synchronous caller in a test): nothing to mirror to
        coro.close()
        return
    previous = _previous_write if _previous_write is not None and _previous_write.get_loop() is loop else None

    async def in_order():
        if previous is not None:
            await asyncio.gather(previous, return_exceptions=True)
        await coro

    task = loop.create_task(in_order())
    _previous_write = task
    _background.add(task)
    task.add_done_callback(_background.discard)


async def _mirror_running(entry: dict[str, Any]) -> None:
    client = _client()
    if client is None:
        return
    try:
        await client.hset(RUNNING_KEY, entry["run_id"], json.dumps(entry))
    except Exception as exc:  # noqa: BLE001
        logger.warning("activity_mirror_failed", run_id=entry.get("run_id"), error=str(exc))


async def _mirror_finished(entry: dict[str, Any]) -> None:
    client = _client()
    if client is None:
        return
    try:
        await client.hdel(RUNNING_KEY, entry["run_id"])
        await client.lpush(RECENT_KEY, json.dumps(entry))
        await client.ltrim(RECENT_KEY, 0, RECENT_LIMIT - 1)
        await client.expire(RECENT_KEY, RECENT_TTL_SECONDS)
    except Exception as exc:  # noqa: BLE001
        logger.warning("activity_mirror_failed", run_id=entry.get("run_id"), error=str(exc))


def start(run_id: str, kind: str, *, started_by: str, jurisdiction: str | None = None, session: str | None = None,
          total: int | None = None, detail: str | None = None) -> None:
    """A run has begun. `started_by` is "schedule" or "trigger"; `total` is how many items it will process, when known."""
    entry = {
        "run_id": run_id, "kind": kind, "started_by": started_by, "jurisdiction": jurisdiction, "session": session,
        "detail": detail, "started_at": _now(), "total": total, "done": 0, "generated": 0, "skipped": 0, "failed": 0,
        "current": [],
    }
    _RUNS[run_id] = entry
    _in_background(_mirror_running(dict(entry)))


def set_total(run_id: str, total: int) -> None:
    entry = _RUNS.get(run_id)
    if entry is not None:
        entry["total"] = total


def bill_started(run_id: str, bill: str) -> None:
    entry = _RUNS.get(run_id)
    if entry is not None and bill not in entry["current"]:
        entry["current"].append(bill)


def bill_finished(run_id: str, bill: str, outcome: str) -> None:
    """`outcome` is "generated" (LegBot wrote something), "skipped" (nothing needed) or "failed"."""
    entry = _RUNS.get(run_id)
    if entry is None:
        return
    if bill in entry["current"]:
        entry["current"].remove(bill)
    entry["done"] += 1
    if outcome in ("generated", "skipped", "failed"):
        entry[outcome] += 1


def finish(run_id: str, outcome: str, error: str | None = None) -> None:
    """A run has ended: `outcome` "done", "partial" (SYNC-196: stopped early or had failed bills) or "failed". It leaves the
    register and goes to the recent list."""
    entry = _RUNS.pop(run_id, None)
    if entry is None:
        return
    entry.update({"outcome": outcome, "error": error, "finished_at": _now(), "current": []})
    _in_background(_mirror_finished(entry))


def running() -> list[dict[str, Any]]:
    """Every run this process has in flight, oldest first (copies)."""
    return sorted((dict(e, current=list(e["current"])) for e in _RUNS.values()), key=lambda e: e["started_at"])


async def recent() -> list[dict[str, Any]]:
    """The last finished runs, newest first, including runs a restart stopped. [] when Redis cannot answer."""
    client = _client()
    if client is None:
        return []
    try:
        rows = await client.lrange(RECENT_KEY, 0, RECENT_LIMIT - 1)
    except Exception:  # noqa: BLE001
        return []
    out = []
    for raw in rows:
        try:
            out.append(json.loads(raw))
        except (TypeError, ValueError):
            continue
    return out


async def sweep_restart_leftovers() -> int:
    """At start-up, before anything runs: what the previous process left in `RUNNING_KEY` was stopped by the restart.
    Each one goes to the recent list with outcome "stopped by a restart". Returns how many."""
    client = _client()
    if client is None:
        return 0
    try:
        leftovers = await client.hgetall(RUNNING_KEY)
        moved = 0
        for raw in leftovers.values():
            try:
                entry = json.loads(raw)
            except (TypeError, ValueError):
                continue
            entry.update({"outcome": "stopped by a restart", "finished_at": None, "current": []})
            await client.lpush(RECENT_KEY, json.dumps(entry))
            moved += 1
        await client.delete(RUNNING_KEY)
        if moved:
            await client.ltrim(RECENT_KEY, 0, RECENT_LIMIT - 1)
            await client.expire(RECENT_KEY, RECENT_TTL_SECONDS)
            logger.warning("activity_runs_stopped_by_restart", count=moved)
        return moved
    except Exception as exc:  # noqa: BLE001
        logger.warning("activity_restart_sweep_failed", error=str(exc))
        return 0


def scheduler_listener(event) -> None:
    """APScheduler listener (EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR): every scheduled job is a run
    here while it runs. Keyed by the job id: every ddp-sync job runs one instance at a time (APScheduler's default `max_instances=1`, which no job raises), and the
    submission event carries a list of run times while each finish carries one, so the time cannot pair them."""
    from apscheduler.events import (
        EVENT_JOB_ERROR,
        EVENT_JOB_EXECUTED,
        EVENT_JOB_SUBMITTED,
    )

    run_id = f"scheduled:{event.job_id}"
    if event.code == EVENT_JOB_SUBMITTED:
        start(run_id, "scheduled job", started_by="schedule", detail=event.job_id)
    elif event.code == EVENT_JOB_EXECUTED:
        finish(run_id, "done")
    elif event.code == EVENT_JOB_ERROR:
        finish(run_id, "failed", error=type(getattr(event, "exception", None)).__name__)


def _reset_for_tests() -> None:
    global _previous_write
    _RUNS.clear()
    _previous_write = None
