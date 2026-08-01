"""Pipeline that runs OpenStates jurisdiction scrapes on a schedule.

Managed by ddp-sync's APScheduler — replaces the ad-hoc run-all-scrapes.sh
launchd job that ran everything sequentially and caused Sunday jobs to be
skipped when Saturday's FL scrape overran into Sunday morning.

Each primary jurisdiction (FL, WA, USA) is an independent APScheduler job so
a long-running FL scrape does not delay WA or USA. Secondary states (VA, MI,
MA, UT, AZ) are fanned out concurrently inside a single Sunday job since they
use independent _data/{state}/ directories and don't conflict.

run-scrape.sh is invoked with SKIP_PATCHES=1; a dedicated patch_refresh job
at 01:00 UTC handles apply-local-patches.sh before the 02:00 UTC scrapes.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from typing import Any

import requests
import structlog

logger = structlog.get_logger()

DEFAULT_OPENSTATES_ROOT = "/Users/agentsmith/Developer/repos/ddp-open-states"

# Per-jurisdiction scrape timeouts. FL 2026 regularly takes 12+ hours.
SCRAPE_TIMEOUT_S: dict[str, int] = {
    "fl": 16 * 3600,
    "wa": 8 * 3600,
    "usa": 4 * 3600,
    "default": 6 * 3600,
}


def _get_root(config: dict | None) -> str:
    return (config or {}).get("openstates_root", DEFAULT_OPENSTATES_ROOT)


def _alert_scrape_failure(label: str, error: str, duration_seconds: float) -> None:
    """Best-effort Slack + CAMS alert for a scrape that ddp-sync itself gave up on.

    run-scrape.sh has its own Slack/CAMS alerting (run-scrape.sh's on_failure()), but that
    only fires from *inside* the script's own process — a ddp-sync subprocess.run(timeout=...)
    kill (subprocess.TimeoutExpired) or any other exception here happens outside that process
    entirely, so run-scrape.sh never gets a chance to alert on it. Before this, a timeout-kill
    was 100% silent: logged at ERROR and written to a Redis flow-status key that health.py
    doesn't even surface. Found live 2026-08-01 while scoping a scrape auto-retry feature —
    MA's own ~5h failure was close enough to its 6h default timeout that a retry wrapper could
    plausibly hit this exact silent path. Never raises — same convention as every other
    alerting call site in this codebase (push_health_alert, run-scrape.sh's on_failure).
    """
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if token:
        channel = os.getenv("HEALTH_ALERT_SLACK_CHANNEL", "#automation-errors")
        text = (
            f":red_circle: *OpenStates scrape failed: {label}* — {error} "
            f"(after {duration_seconds:.0f}s) — check ddp-sync logs / scraper.log"
        )
        try:
            resp = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {token}"},
                json={"channel": channel, "text": text},
                timeout=15,
            )
            if not (resp.ok and resp.json().get("ok")):
                logger.error("openstates_scrape: Slack alert failed", response=resp.text[:200])
        except Exception as e:  # noqa: BLE001
            logger.error("openstates_scrape: Slack alert error", error=str(e))
    else:
        logger.warning("openstates_scrape: SLACK_BOT_TOKEN not set — cannot alert on scrape failure")

    cams_token = os.getenv("CAMS_API_TOKEN", "")
    if cams_token:
        cams_url = os.getenv("CAMS_BASE_URL", "http://localhost:8000")
        payload = {
            "v": 1,
            "service": "ddp-sync",
            "error_type": "ScrapeTimeoutOrSubprocessError",
            "message": f"scrape failed for {label}: {error} (after {duration_seconds:.0f}s)",
            "metadata": {"jurisdiction": label},
        }
        try:
            resp = requests.post(
                f"{cams_url}/api/v1/failures",
                headers={"Authorization": f"Bearer {cams_token}", "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10,
            )
            if not resp.ok:
                logger.error("openstates_scrape: CAMS report failed", status=resp.status_code)
        except Exception as e:  # noqa: BLE001
            logger.error("openstates_scrape: CAMS report error", error=str(e))


def _run_with_group_kill(
    cmd: list[str], env: dict, timeout: int
) -> tuple[int, bytes, bytes, bool]:
    """Run cmd to completion or timeout, killing its whole process group on timeout.

    subprocess.run(timeout=...) only kills the direct child on TimeoutExpired — verified
    empirically 2026-08-01 that a grandchild process survives a plain subprocess.run
    timeout-kill even with start_new_session=True (that flag only makes the child its own
    process-group leader; nothing then targets that group). For run-scrape.sh specifically,
    the surviving grandchildren are exactly the processes actually doing work — os-update's
    scrape/import, the backgrounded sweep-import loop — which would otherwise keep running
    (and keep holding the import lock, keep writing into $STATE_DATADIR) after ddp-sync has
    already decided the run failed and moved on. Managing the Popen object directly here so a
    timeout can os.killpg() the whole group instead of just the one process we started.
    """
    process = subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        return process.returncode, stdout, stderr, False
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass  # process (and its group) already gone
        stdout, stderr = process.communicate()  # reap; collect whatever was already buffered
        return process.returncode, stdout, stderr, True


async def _run_scrape(
    jurisdiction: str,
    session_arg: str | None,
    openstates_root: str,
    timeout_s: int | None = None,
) -> dict[str, Any]:
    """Run run-scrape.sh for one jurisdiction off the event loop.

    Uses asyncio.to_thread so concurrent job coroutines (e.g. WA and USA
    both at 02:00 UTC) don't block each other in the event loop.
    SKIP_PATCHES=1 is set so apply-local-patches.sh is not re-run for every
    jurisdiction — the patch_refresh job owns that step.
    """
    script = os.path.join(openstates_root, "run-scrape.sh")
    cmd = ["/bin/bash", script, jurisdiction]
    if session_arg:
        cmd.append(session_arg)

    env = {**os.environ, "SKIP_PATCHES": "1"}
    timeout = timeout_s or SCRAPE_TIMEOUT_S.get(jurisdiction, SCRAPE_TIMEOUT_S["default"])
    label = f"{jurisdiction} {session_arg}" if session_arg else jurisdiction

    logger.info(
        "openstates_scrape: starting",
        jurisdiction=jurisdiction,
        session=session_arg,
        timeout_s=timeout,
    )

    start = time.monotonic()
    try:
        returncode, _stdout, stderr, timed_out = await asyncio.to_thread(
            _run_with_group_kill, cmd, env, timeout
        )
        duration = round(time.monotonic() - start, 1)

        if timed_out:
            logger.error(
                "openstates_scrape: timeout",
                jurisdiction=jurisdiction,
                session=session_arg,
                timeout_s=timeout,
                duration_seconds=duration,
            )
            # Whole process group killed above, before run-scrape.sh's own ERR trap ever got a
            # chance to run — it never alerted on this one. We're the only ones who know it
            # happened, so we're the only ones who can alert.
            _alert_scrape_failure(label, f"timed out after {timeout}s", duration)
            return {
                "success": False,
                "error": "timeout",
                "jurisdiction": label,
                "duration_seconds": duration,
            }

        if returncode != 0:
            stderr_tail = (stderr or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_scrape: failed",
                jurisdiction=jurisdiction,
                session=session_arg,
                returncode=returncode,
                stderr_tail=stderr_tail,
                duration_seconds=duration,
            )
            if returncode < 0:
                # Negative returncode = killed by a signal that didn't originate from our own
                # timeout handling above (OOM killer, `kill` from an operator, another
                # supervisor). run-scrape.sh's `trap ... ERR` only fires on an ordinary command
                # failure inside the script, not on the script's own process receiving a
                # terminating signal — so unlike a plain nonzero exit, this one was never
                # self-alerted, and we're the only ones who saw it.
                _alert_scrape_failure(label, f"killed by signal {-returncode}", duration)
            # else (positive returncode): run-scrape.sh's own on_failure() already fired its
            # Slack/CAMS alert from inside the process before exiting nonzero — alerting again
            # here would double-page for the exact same failure.
            return {
                "success": False,
                "error": f"exit_code_{returncode}",
                "jurisdiction": label,
                "duration_seconds": duration,
            }

        logger.info(
            "openstates_scrape: done",
            jurisdiction=jurisdiction,
            session=session_arg,
            duration_seconds=duration,
        )
        return {"success": True, "jurisdiction": label, "duration_seconds": duration}

    except Exception as e:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "openstates_scrape: subprocess error",
            jurisdiction=jurisdiction,
            session=session_arg,
            error=str(e),
            duration_seconds=duration,
        )
        # Something failed before/while invoking the subprocess itself (e.g. the script or
        # openstates_root path doesn't exist) — run-scrape.sh never started, so it never had a
        # chance to alert either.
        _alert_scrape_failure(label, str(e), duration)
        return {
            "success": False,
            "error": str(e),
            "jurisdiction": label,
            "duration_seconds": duration,
        }


async def _write_flow_status(flow_key: str, status: dict) -> None:
    """Best-effort Redis flow_status write. Never raises."""
    try:
        from ddp_sync.services.redis_store import get_redis_store
        redis_store = get_redis_store()
        await redis_store.set_flow_status(flow_key, status)
    except Exception as e:
        logger.warning("openstates_scrape: redis write failed", flow=flow_key, error=str(e))


# ---------------------------------------------------------------------------
# Public job functions — called by the scheduler (via closures) and by
# the trigger endpoint directly. Each accepts the openstates_scrape config
# block so settings are driven from sync_schedule.yaml.
# ---------------------------------------------------------------------------

async def run_patch_refresh_job(config: dict | None = None) -> dict[str, Any]:
    """Apply local patches to openstates-core and openstates-scrapers.

    Runs apply-local-patches.sh once daily at 01:00 UTC before any scrapes
    start. The scrape jobs set SKIP_PATCHES=1 so they don't repeat this step.
    """
    openstates_root = _get_root(config)
    script = os.path.join(openstates_root, "apply-local-patches.sh")
    start_time = datetime.now(timezone.utc)

    logger.info("openstates_patch_refresh: starting", openstates_root=openstates_root)
    t = time.monotonic()

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            ["/bin/bash", script],
            cwd=openstates_root,
            capture_output=True,
            timeout=300,
        )
        duration = round(time.monotonic() - t, 1)

        if result.returncode != 0:
            stderr = (result.stderr or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_patch_refresh: failed",
                returncode=result.returncode,
                stderr_tail=stderr,
                duration_seconds=duration,
            )
            await _write_flow_status("openstates_patch_refresh", {
                "flow": "openstates_patch_refresh",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "status": "failed",
                "error": f"exit_code_{result.returncode}",
                "duration_seconds": duration,
            })
            return {"success": False, "error": f"exit_code_{result.returncode}", "duration_seconds": duration}

        logger.info("openstates_patch_refresh: done", duration_seconds=duration)
        await _write_flow_status("openstates_patch_refresh", {
            "flow": "openstates_patch_refresh",
            "started_at": start_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "duration_seconds": duration,
        })
        return {"success": True, "duration_seconds": duration}

    except subprocess.TimeoutExpired:
        duration = round(time.monotonic() - t, 1)
        logger.error("openstates_patch_refresh: timeout", duration_seconds=duration)
        return {"success": False, "error": "timeout", "duration_seconds": duration}
    except Exception as e:
        duration = round(time.monotonic() - t, 1)
        logger.error("openstates_patch_refresh: error", error=str(e), duration_seconds=duration)
        return {"success": False, "error": str(e), "duration_seconds": duration}


async def run_fl_scrapes_job(config: dict | None = None) -> dict[str, Any]:
    """Run all FL sessions sequentially (they share _data/fl/).

    Sessions run in order: 2026 → 2026D → 2026E → 2026F. A failed session
    is logged but does not abort the remaining sessions.
    """
    openstates_root = _get_root(config)
    sessions = (
        (config or {}).get("primary", {}).get("fl", {})
        .get("sessions", ["2026", "2026D", "2026E", "2026F"])
    )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_fl_scrapes: starting", sessions=sessions)

    results = []
    for session in sessions:
        result = await _run_scrape(
            "fl", f"session={session}", openstates_root, SCRAPE_TIMEOUT_S["fl"]
        )
        results.append(result)

    duration = round(time.monotonic() - t, 1)
    failed = [r for r in results if not r["success"]]

    log_fn = logger.error if failed else logger.info
    log_fn(
        "openstates_fl_scrapes: completed",
        sessions=sessions,
        total=len(results),
        failed=len(failed),
        duration_seconds=duration,
    )

    await _write_flow_status("openstates_fl_scrapes", {
        "flow": "openstates_fl_scrapes",
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if not failed else "completed_with_errors",
        "sessions": sessions,
        "total": len(results),
        "failed": len(failed),
        "results": results,
        "duration_seconds": duration,
    })
    return {
        "success": not failed,
        "sessions": sessions,
        "results": results,
        "failed": len(failed),
        "duration_seconds": duration,
    }


async def run_wa_scrape_job(config: dict | None = None) -> dict[str, Any]:
    """Run the WA scrape. Runs in parallel with FL and USA on the event loop."""
    openstates_root = _get_root(config)
    start_time = datetime.now(timezone.utc)

    result = await _run_scrape("wa", None, openstates_root, SCRAPE_TIMEOUT_S["wa"])

    await _write_flow_status("openstates_wa_scrape", {
        "flow": "openstates_wa_scrape",
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if result["success"] else "failed",
        **result,
    })
    return result


async def run_usa_scrapes_job(config: dict | None = None) -> dict[str, Any]:
    """Run USA lower then upper sequentially (they share _data/usa/)."""
    openstates_root = _get_root(config)
    sessions = (
        (config or {}).get("primary", {}).get("usa", {})
        .get("sessions", ["119 chamber=lower", "119 chamber=upper"])
    )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_usa_scrapes: starting", sessions=sessions)

    results = []
    for session in sessions:
        result = await _run_scrape(
            "usa", f"session={session}", openstates_root, SCRAPE_TIMEOUT_S["usa"]
        )
        results.append(result)

    duration = round(time.monotonic() - t, 1)
    failed = [r for r in results if not r["success"]]

    log_fn = logger.error if failed else logger.info
    log_fn(
        "openstates_usa_scrapes: completed",
        total=len(results),
        failed=len(failed),
        duration_seconds=duration,
    )

    await _write_flow_status("openstates_usa_scrapes", {
        "flow": "openstates_usa_scrapes",
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if not failed else "completed_with_errors",
        "sessions": sessions,
        "total": len(results),
        "failed": len(failed),
        "results": results,
        "duration_seconds": duration,
    })
    return {
        "success": not failed,
        "sessions": sessions,
        "results": results,
        "failed": len(failed),
        "duration_seconds": duration,
    }


async def run_secondary_scrapes_job(config: dict | None = None) -> dict[str, Any]:
    """Run secondary states (VA, MI, MA, UT, AZ) concurrently.

    Each jurisdiction uses a distinct _data/{state}/ directory so they don't
    conflict. asyncio.gather fans them out into separate threads simultaneously,
    cutting total wall-clock from ~sum(durations) to ~max(durations).
    """
    openstates_root = _get_root(config)
    jurisdictions: list[str] = (
        (config or {}).get("secondary", {})
        .get("jurisdictions", ["va", "mi", "ma", "ut", "az"])
    )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_secondary_scrapes: starting", jurisdictions=jurisdictions)

    results: list[dict[str, Any]] = await asyncio.gather(
        *[_run_scrape(j, None, openstates_root) for j in jurisdictions]
    )

    duration = round(time.monotonic() - t, 1)
    failed = [r for r in results if not r["success"]]

    log_fn = logger.error if failed else logger.info
    log_fn(
        "openstates_secondary_scrapes: completed",
        jurisdictions=jurisdictions,
        total=len(results),
        failed=len(failed),
        duration_seconds=duration,
    )

    await _write_flow_status("openstates_secondary_scrapes", {
        "flow": "openstates_secondary_scrapes",
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if not failed else "completed_with_errors",
        "jurisdictions": jurisdictions,
        "total": len(results),
        "failed": len(failed),
        "results": list(results),
        "duration_seconds": duration,
    })
    return {
        "success": not failed,
        "jurisdictions": jurisdictions,
        "results": list(results),
        "failed": len(failed),
        "duration_seconds": duration,
    }


async def run_single_scrape_job(
    jurisdiction: str,
    config: dict | None = None,
) -> dict[str, Any]:
    """Run a single arbitrary jurisdiction. Used by the manual trigger endpoint."""
    openstates_root = _get_root(config)
    return await _run_scrape(jurisdiction, None, openstates_root)


async def run_people_refresh_job(config: dict | None = None) -> dict[str, Any]:
    """Pull the people repo and run os-people to-database for all states."""
    openstates_root = _get_root(config)
    script = os.path.join(openstates_root, "run-people-refresh.sh")
    start_time = datetime.now(timezone.utc)

    logger.info("openstates_people_refresh: starting")
    t = time.monotonic()

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            ["/bin/bash", script],
            capture_output=True,
            timeout=3600,
            start_new_session=True,
        )
        duration = round(time.monotonic() - t, 1)

        if result.returncode != 0:
            stderr_tail = (result.stderr or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_people_refresh: failed",
                returncode=result.returncode,
                stderr_tail=stderr_tail,
                duration_seconds=duration,
            )
            await _write_flow_status("openstates_people_refresh", {
                "flow": "openstates_people_refresh",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "status": "failed",
                "error": f"exit_code_{result.returncode}",
                "duration_seconds": duration,
            })
            return {"success": False, "error": f"exit_code_{result.returncode}", "duration_seconds": duration}

        logger.info("openstates_people_refresh: done", duration_seconds=duration)
        await _write_flow_status("openstates_people_refresh", {
            "flow": "openstates_people_refresh",
            "started_at": start_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "duration_seconds": duration,
        })
        return {"success": True, "duration_seconds": duration}

    except subprocess.TimeoutExpired:
        duration = round(time.monotonic() - t, 1)
        logger.error("openstates_people_refresh: timeout", duration_seconds=duration)
        return {"success": False, "error": "timeout", "duration_seconds": duration}
    except Exception as e:
        duration = round(time.monotonic() - t, 1)
        logger.error("openstates_people_refresh: error", error=str(e), duration_seconds=duration)
        return {"success": False, "error": str(e), "duration_seconds": duration}
