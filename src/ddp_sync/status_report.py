"""SYNC-178: `ddp-sync status`, everything this ddp-sync is running, holding and has scheduled, in one answer.

`collect()` gathers it (GET /ddp-sync/v1/status returns it as JSON); `format_text()` turns it into the plain text the
`ddp-sync status` command prints, next to what `cams status` shows for CAMS. Read only. Every source is one ddp-sync already
keeps; the only new one is `services.activity` (what this process is running, with a session pipeline's progress).

Records are copied field by field, never passed through whole: a LegBot task record can hold a broker token
(`bill_artifact_generation`, SYNC-61), and a Fargate record holds its whole config. Nothing from `.env` is read here.
"""

from __future__ import annotations

import fnmatch
import json
from datetime import UTC, datetime
from typing import Any

from ddp_sync.services import activity

# Every lock or lease ddp-sync takes, with what it guards. A held one is why a trigger answers 409 (or skips) right now.
# A new lock belongs in this list, or `ddp-sync status` cannot show it.
LOCK_PATTERNS: list[tuple[str, str]] = [
    ("ddp:pipeline_lock:*", "a state's scrape/archive pipeline (SYNC-153)"),
    ("ddp_sync:scraper_triggered_legbot:lock:*", "a LegBot session run for one jurisdiction and session"),
    ("ddp_sync:organization_logo_catch_up:lock", "the organization logo catch-up (SYNC-161)"),
    ("ddp_sync:kb_backfill:lock:*", "a knowledge-base backfill"),
    ("votebot:eval:running", "the weekly VoteBot eval"),
]


def _record_patterns() -> tuple[str, str, str]:
    from ddp_sync.services.redis_store import RedisStore

    return (f"{RedisStore.LEGBOT_SESSION_RUN_PREFIX}*", f"{RedisStore.SYNC_TASK_PREFIX}*",
            f"{RedisStore.LEGBOT_TASK_PREFIX}*")


_SESSION_RUNS, _SYNC_TASKS, _LEGBOT_TASKS = _record_patterns()


def _client():
    try:
        from ddp_sync.services.redis_store import get_redis_store

        return get_redis_store()._client
    except Exception:  # noqa: BLE001 -- a view; report what can be read
        return None


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


async def _keys_by_pattern(client, patterns: list[str]) -> dict[str, list[str]]:
    """ONE pass over the keyspace (the Mac's Redis held 58,211 keys on 2026-11-18; a scan per pattern took 2.5 s a call),
    each key sorted into every pattern it matches."""
    found: dict[str, list[str]] = {pattern: [] for pattern in patterns}
    async for key in client.scan_iter(count=1000):
        key = _text(key)
        for pattern in patterns:
            if fnmatch.fnmatchcase(key, pattern):
                found[pattern].append(key)
    return found


async def _locks(client, keys: dict[str, list[str]]) -> list[dict[str, Any]]:
    out = []
    now = datetime.now(UTC)
    for pattern, guards in LOCK_PATTERNS:
        for key in keys[pattern]:
            ttl = await client.ttl(key)
            owner = await client.get(key)
            if owner is None:  # gone since the scan
                continue
            out.append({
                "key": key, "guards": guards, "owner": _text(owner),
                "expires_at": (datetime.fromtimestamp(now.timestamp() + ttl, UTC).isoformat() if ttl and ttl > 0 else None),
                "expires_in_s": ttl if ttl and ttl > 0 else None,
            })
    return out


async def _json_values(client, keys: list[str]) -> list[tuple[str, dict[str, Any]]]:
    out = []
    for key in keys:
        raw = await client.get(key)
        try:
            out.append((key, json.loads(raw)))
        except (TypeError, ValueError):
            continue
    return out


async def _background_session_runs(client, keys: dict[str, list[str]]) -> list[dict[str, Any]]:
    from ddp_sync.pipelines.scraper_triggered_legbot import session_run_as_read

    rows = []
    for _key, record in await _json_values(client, keys[_SESSION_RUNS]):
        r = session_run_as_read(record)
        rows.append({k: r.get(k) for k in ("run_id", "status", "jurisdiction_iso2", "session_code", "accepted_at",
                                           "updated_at", "finished_at", "error")})
    return rows


async def _sync_tasks(client, keys: dict[str, list[str]]) -> list[dict[str, Any]]:
    from ddp_sync.services.redis_store import RedisStore

    rows = []
    for key, t in await _json_values(client, keys[_SYNC_TASKS]):
        if t.get("status") not in ("accepted", "running"):
            continue
        result = t.get("result") or {}
        rows.append({"task_id": key.removeprefix(RedisStore.SYNC_TASK_PREFIX), "status": t.get("status"),
                     "content_type": t.get("content_type"), "last_heartbeat": t.get("last_heartbeat"),
                     "items_processed": result.get("items_processed"), "items_failed": result.get("items_failed")})
    return rows


async def _legbot_tasks(client, keys: dict[str, list[str]]) -> list[dict[str, Any]]:
    from ddp_sync.services.redis_store import RedisStore

    return [{"cams_task_id": key.removeprefix(RedisStore.LEGBOT_TASK_PREFIX), "artifact_type": t.get("artifact_type"),
             "jurisdiction": t.get("jurisdiction"), "session_code": t.get("session_code"),
             "bill_openstates_id": t.get("bill_openstates_id")}
            for key, t in await _json_values(client, keys[_LEGBOT_TASKS])]


def _fargate_jobs() -> list[dict[str, Any]]:
    from ddp_sync.pipelines.inflight_fargate_jobs import list_inflight

    rows = []
    for run_id, r in list_inflight().items():
        rows.append({"run_id": run_id, "jurisdiction": r.get("jurisdiction"), "session": r.get("session_arg"),
                     "ecs_task": str(r.get("task_arn") or "").rsplit("/", 1)[-1] or None})
    return rows


def scheduled_jobs() -> dict[str, Any]:
    """The scheduler's jobs with their next run times (what GET /schedule returns), next run first."""
    from ddp_sync.scheduler import get_scheduler

    scheduler = get_scheduler()
    if not scheduler or not scheduler.scheduler.running:
        return {"status": "scheduler_not_running", "jobs": []}
    jobs = [{"id": job.id, "name": job.name, "next_run": str(job.next_run_time) if job.next_run_time else None,
             "trigger": str(job.trigger)} for job in scheduler.scheduler.get_jobs()]
    jobs.sort(key=lambda j: (j["next_run"] is None, j["next_run"] or ""))
    return {"status": "running", "jobs": jobs, "openstates_cadence": getattr(scheduler, "_openstates_cadence", {})}


async def collect() -> dict[str, Any]:
    """Everything `ddp-sync status` shows. A source that cannot be read is named in `unreadable`, never guessed."""
    unreadable: list[str] = []
    data: dict[str, Any] = {"generated_at": datetime.now(UTC).isoformat(), "running": activity.running(),
                            "background_session_runs": [], "sync_tasks": [], "legbot_tasks": [], "fargate_jobs": [],
                            "locks": [], "recent": [], "unreadable": unreadable}
    client = _client()
    if client is None:
        unreadable.append("redis (locks, LegBot tasks, background runs, recent runs)")
    else:
        try:
            keys = await _keys_by_pattern(client, [p for p, _ in LOCK_PATTERNS] + [_SESSION_RUNS, _SYNC_TASKS,
                                                                                  _LEGBOT_TASKS])
        except Exception:  # noqa: BLE001
            keys = None
            unreadable.append("redis (locks, LegBot tasks, background runs, sync tasks)")
        if keys is not None:
            for name, reader in (("locks", _locks), ("background_session_runs", _background_session_runs),
                                 ("sync_tasks", _sync_tasks), ("legbot_tasks", _legbot_tasks)):
                try:
                    data[name] = await reader(client, keys)
                except Exception:  # noqa: BLE001
                    unreadable.append(name)
        data["recent"] = await activity.recent()
    try:
        data["fargate_jobs"] = _fargate_jobs()
    except Exception:  # noqa: BLE001
        unreadable.append("fargate_jobs")
    try:
        data["scheduled"] = scheduled_jobs()
    except Exception as exc:  # noqa: BLE001
        data["scheduled"] = {"status": f"error: {type(exc).__name__}", "jobs": []}
    # The register is read again after the keyspace pass, so a run this process started while it was being read is not
    # missed (pm-review). It stays a snapshot, like `cams status`: something can always start a moment later.
    seen = {r["run_id"] for r in data["running"]}
    data["running"] += [r for r in activity.running() if r["run_id"] not in seen]
    data["restart"] = restart_safety(data)
    return data


def restart_safety(data: dict[str, Any]) -> dict[str, Any]:
    """Whether restarting ddp-sync now would interrupt something, and what. Fargate jobs and LegBot tasks are not
    interrupted (the process resumes the one and the recovery sweep collects the other); they are listed apart."""
    interrupted = [f"{r['kind']} ({_where(r)})" for r in data.get("running", [])]
    interrupted += [f"background LegBot session run {r['run_id']} ({r.get('jurisdiction_iso2')} {r.get('session_code')})"
                    for r in data.get("background_session_runs", []) if r.get("status") in ("queued", "running")]
    interrupted += [f"sync task {r['task_id']} ({r.get('content_type')})" for r in data.get("sync_tasks", [])]
    resumed = [f"Fargate job {r['run_id']} ({r.get('jurisdiction')})" for r in data.get("fargate_jobs", [])]
    resumed += [f"{len(data.get('legbot_tasks', []))} LegBot task(s) in CAMS"] if data.get("legbot_tasks") else []
    # "yes" only when every source was read and nothing is running. A source that could not be read, or a lock held
    # while nothing listed is running, leaves the answer unknown (pm-review: never a reassuring "yes" on missing data).
    unknown = []
    if data.get("unreadable"):
        unknown.append(f"could not read {', '.join(data['unreadable'])}")
    if not interrupted and data.get("locks"):
        unknown.append(f"held but nothing listed is running: {', '.join(lock['key'] for lock in data['locks'])}")
    safe = False if interrupted else (None if unknown else True)
    return {"safe": safe, "would_interrupt": interrupted, "would_resume": resumed, "unknown_because": unknown}


def _where(run: dict[str, Any]) -> str:
    parts = [p for p in (run.get("jurisdiction"), run.get("session"), run.get("detail")) if p]
    return " ".join(str(p) for p in parts) or run.get("run_id", "?")


def _age(iso: str | None, now: datetime) -> str:
    if not iso:
        return "?"
    try:
        secs = max(int((now - datetime.fromisoformat(iso)).total_seconds()), 0)
    except (TypeError, ValueError):
        return "?"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    return f"{secs // 86400}d {(secs % 86400) // 3600}h"


def _clock(iso: str | None) -> str:
    try:
        return datetime.fromisoformat(iso).astimezone(UTC).strftime("%m-%d %H:%M UTC")
    except (TypeError, ValueError):
        return "?"


def format_text(data: dict[str, Any], now: datetime | None = None) -> str:
    """The plain text `ddp-sync status` prints."""
    now = now or datetime.now(UTC)
    lines: list[str] = []
    restart = data.get("restart") or restart_safety(data)
    if restart["safe"] is True:
        lines.append("Safe to restart: yes (nothing running would be interrupted, as of this moment)")
    elif restart["safe"] is None:
        lines.append(f"Safe to restart: UNKNOWN ({'; '.join(restart.get('unknown_because', []))})")
    else:
        lines.append(f"Safe to restart: NO, it would interrupt: {'; '.join(restart['would_interrupt'])}")
    if restart.get("would_resume"):
        lines.append(f"  (a restart does not lose: {'; '.join(restart['would_resume'])})")

    lines += ["", "Running now:"]
    running = data.get("running", [])
    background = [r for r in data.get("background_session_runs", []) if r.get("status") in ("queued", "running")]
    if not running and not background and not data.get("sync_tasks") and not data.get("fargate_jobs"):
        lines.append("  nothing")
    for r in running:
        lines.append(f"  ▶ {r['kind']}: {_where(r)}   run {r['run_id']}   started by {r['started_by']}")
        lines.append(f"      since {_clock(r['started_at'])} ({_age(r['started_at'], now)})")
        if r.get("total") is not None or r.get("done"):
            total = r.get("total") if r.get("total") is not None else "?"
            lines.append(f"      bills {r['done']}/{total}: {r['generated']} generated, {r['skipped']} skipped, "
                         f"{r['failed']} failed")
        if r.get("current"):
            lines.append(f"      on now: {', '.join(r['current'])}")
    for r in background:
        lines.append(f"  · background LegBot session run {r['run_id']} ({r.get('jurisdiction_iso2')} "
                     f"{r.get('session_code')}): {r.get('status')}, accepted {_clock(r.get('accepted_at'))}")
    for t in data.get("sync_tasks", []):
        lines.append(f"  ▶ sync task {t['task_id']} ({t.get('content_type')}): {t.get('status')}, "
                     f"{t.get('items_processed') or 0} items, {t.get('items_failed') or 0} failed, "
                     f"last heartbeat {_age(t.get('last_heartbeat'), now)} ago")
    for f in data.get("fargate_jobs", []):
        lines.append(f"  ⏳ waiting on Fargate: {f.get('jurisdiction')} {f.get('session') or ''}   run {f['run_id']}   "
                     f"ECS task {f.get('ecs_task')}")
    legbot = data.get("legbot_tasks", [])
    if legbot:
        lines.append(f"  LegBot tasks in CAMS ({len(legbot)}):")
        for t in legbot[:10]:
            lines.append(f"    {t['cams_task_id'][:8]}  {t.get('artifact_type')}  {t.get('jurisdiction')} "
                         f"{t.get('session_code')}")
        if len(legbot) > 10:
            lines.append(f"    ... and {len(legbot) - 10} more")

    lines += ["", "Locks held:"]
    if not data.get("locks"):
        lines.append("  none")
    for lock in data.get("locks", []):
        expiry = (f"expires {_clock(lock['expires_at'])} (in {lock['expires_in_s'] // 60}m)" if lock.get("expires_at")
                  else "no expiry")
        lines.append(f"  {lock['key']}  ({lock['guards']})  owner {lock.get('owner')}  {expiry}")

    lines += ["", "Scheduled next:"]
    scheduled = data.get("scheduled") or {}
    if scheduled.get("status") != "running":
        lines.append(f"  scheduler: {scheduled.get('status', 'unknown')}")
    for job in (scheduled.get("jobs") or [])[:12]:
        lines.append(f"  {_clock(job['next_run']) if job.get('next_run') else 'paused':<16} {job['id']}")
    if len(scheduled.get("jobs") or []) > 12:
        lines.append(f"  ... and {len(scheduled['jobs']) - 12} more (GET /ddp-sync/v1/schedule)")

    lines += ["", "Recently finished:"]
    recent = data.get("recent", [])
    finished_background = [r for r in data.get("background_session_runs", []) if r.get("status") not in ("queued", "running")]
    if not recent and not finished_background:
        lines.append("  nothing recorded yet")
    for r in recent[:10]:
        when = _clock(r.get("finished_at")) if r.get("finished_at") else f"started {_clock(r.get('started_at'))}"
        progress = (f", bills {r.get('done')}/{r.get('total')}" if r.get("total") is not None else "")
        error = f" ({r['error']})" if r.get("error") else ""
        lines.append(f"  {r.get('outcome', '?'):<20} {r.get('kind')}: {_where(r)}  {when}{progress}{error}")
    for r in sorted(finished_background, key=lambda r: r.get("updated_at") or "", reverse=True)[:5]:
        error = f" ({r['error']})" if r.get("error") else ""
        lines.append(f"  {r.get('status', '?'):<20} background LegBot session run {r['run_id']} "
                     f"({r.get('jurisdiction_iso2')} {r.get('session_code')})  "
                     f"{_clock(r.get('finished_at') or r.get('updated_at'))}{error}")

    if data.get("unreadable"):
        lines += ["", f"Could not read: {', '.join(data['unreadable'])}"]
    return "\n".join(lines)
