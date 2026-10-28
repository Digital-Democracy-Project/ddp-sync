"""Nightly Docker cleanup on the Mac (SYNC-119, the ddp-sync half of AGENTS-138).

Runs ddp-agents' `deployment/scripts/docker-prune.sh` and nothing else. The script owns every rule (what
it removes, what it never touches, one failed step not stopping the next), its log
(`ddp-agents/logs/docker-prune.log`) and its own Slack alert on failure; none of that is repeated here.
This job adds what the script cannot do for itself:
  - a timeout that kills the whole process group, so a wedged `docker` call never hangs the scheduler;
  - an alert through slack_alerts.post_alert when the script exits non-zero, times out or cannot start,
    in case the script itself could not post (no token, Slack down, killed by the timeout).

Only the Mac runs Docker (Colima), so the per-host DOCKER_CLEANUP_ENABLED flag is off unless that host's
environment says true. The script finds Colima's socket itself (~/.colima/default/docker.sock, with an
absolute fallback), and the ddp-sync LaunchDaemon gives it HOME and /opt/homebrew/bin on PATH.
"""

from __future__ import annotations

import asyncio
import os
import time

import structlog

from ddp_sync.pipelines.openstates_scrape import _run_with_group_kill
from ddp_sync.slack_alerts import post_alert

logger = structlog.get_logger()

DEFAULT_TIMEOUT_SECONDS = 1800

# One run at a time per process: the nightly job and a manual trigger must never prune concurrently.
_run_lock = asyncio.Lock()


async def run_docker_cleanup_job(script_path: str, *, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> dict:
    """Run the cleanup script once. Returns {"success", "error", "duration_seconds"}, or {"skipped": ...}
    when a run is already going. Never raises (post_alert never raises either)."""
    if _run_lock.locked():
        logger.info("docker_cleanup: a run is already in progress -- skipping", script=script_path)
        return {"success": False, "skipped": "already_running", "error": None, "duration_seconds": 0.0}
    async with _run_lock:
        return await _run_once(script_path, timeout_seconds)


async def _run_once(script_path: str, timeout_seconds: int) -> dict:
    t = time.monotonic()
    logger.info("docker_cleanup: starting", script=script_path)
    error = None
    try:
        returncode, _stdout, stderr, timed_out, _stalled = await asyncio.to_thread(
            _run_with_group_kill, [script_path], dict(os.environ), timeout_seconds
        )
        if timed_out:
            error = f"timed out after {timeout_seconds}s and was killed"
        elif returncode != 0:
            tail = (stderr or b"").decode(errors="replace")[-300:].strip()
            error = f"exit code {returncode}" + (f": {tail}" if tail else "")
    except OSError as exc:  # script missing or not executable
        error = f"{type(exc).__name__}: {exc}"
    duration = round(time.monotonic() - t, 1)
    if error:
        logger.error("docker_cleanup: failed", error=error, duration_seconds=duration)
        await asyncio.to_thread(post_alert,
            f":red_circle: Nightly Docker cleanup failed ({error[:200]}). "
            "See ddp-agents logs/docker-prune.log.",
            source="docker_cleanup",
        )
    else:
        logger.info("docker_cleanup: done", duration_seconds=duration)
    return {"success": error is None, "error": error, "duration_seconds": duration}
