"""Pipeline that runs OpenStates bill-document archiving on its own schedule.

Split out of openstates_scrape.py (2026-07-31). Archiving to DDP-HOT used to
run as the last step inside run-scrape.sh, which meant the incremental cutoff
marker (logs/last-run/<key>.ts) didn't advance until archiving finished too.
That created a compounding failure mode: a run whose archive step ran long
or died left the cutoff stuck at its old value, so the next run had to treat
more bills as "changed since cutoff," making that run slower too and more
likely to also miss its own archive window — observed live 2026-07-30/31: a
WA run was still archiving 1h45m+ after scrape+import had already finished
cleanly.

run-scrape.sh no longer touches archiving at all (ddp-open-states
PLAN-open-states.md, incremental-scraping section). This module runs
run-archive.sh <state> for each ARCHIVE_ENABLED_STATE independently, on its
own cadence, with no relationship to when/whether a scrape ran. Safe to run
concurrently with a scrape for the same jurisdiction — os-text-extract's
natural-key skip check makes an already-archived version a cheap DB check,
not a re-fetch — or with any other scrape.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger()

DEFAULT_OPENSTATES_ROOT = "/Users/agentsmith/Developer/repos/ddp-open-states"

# Per-jurisdiction archive timeouts. Downloads + extracts every not-yet-captured
# document, so these scale with total historical bill count, not just what
# changed recently — sized generously, matching the scrape timeouts' shape.
ARCHIVE_TIMEOUT_S: dict[str, int] = {
    "fl": 16 * 3600,
    "wa": 8 * 3600,
    "default": 4 * 3600,
}


def _get_root(config: dict | None) -> str:
    return (config or {}).get("openstates_root", DEFAULT_OPENSTATES_ROOT)


async def _run_archive(
    jurisdiction: str,
    openstates_root: str,
    timeout_s: int | None = None,
) -> dict[str, Any]:
    """Run run-archive.sh for one jurisdiction off the event loop."""
    script = os.path.join(openstates_root, "run-archive.sh")
    cmd = ["/bin/bash", script, jurisdiction]

    timeout = timeout_s or ARCHIVE_TIMEOUT_S.get(jurisdiction, ARCHIVE_TIMEOUT_S["default"])

    logger.info("openstates_archive: starting", jurisdiction=jurisdiction, timeout_s=timeout)

    start = time.monotonic()
    try:
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            timeout=timeout,
            start_new_session=True,  # kill grandchildren on timeout
        )
        duration = round(time.monotonic() - start, 1)

        if result.returncode != 0:
            stderr_tail = (result.stderr or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_archive: failed",
                jurisdiction=jurisdiction,
                returncode=result.returncode,
                stderr_tail=stderr_tail,
                duration_seconds=duration,
            )
            return {
                "success": False,
                "error": f"exit_code_{result.returncode}",
                "jurisdiction": jurisdiction,
                "duration_seconds": duration,
            }

        logger.info(
            "openstates_archive: done",
            jurisdiction=jurisdiction,
            duration_seconds=duration,
        )
        return {"success": True, "jurisdiction": jurisdiction, "duration_seconds": duration}

    except subprocess.TimeoutExpired:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "openstates_archive: timeout",
            jurisdiction=jurisdiction,
            timeout_s=timeout,
            duration_seconds=duration,
        )
        return {
            "success": False,
            "error": "timeout",
            "jurisdiction": jurisdiction,
            "duration_seconds": duration,
        }
    except Exception as e:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "openstates_archive: subprocess error",
            jurisdiction=jurisdiction,
            error=str(e),
            duration_seconds=duration,
        )
        return {
            "success": False,
            "error": str(e),
            "jurisdiction": jurisdiction,
            "duration_seconds": duration,
        }


async def _write_flow_status(flow_key: str, status: dict) -> None:
    """Best-effort Redis flow_status write. Never raises."""
    try:
        from ddp_sync.services.redis_store import get_redis_store
        redis_store = get_redis_store()
        await redis_store.set_flow_status(flow_key, status)
    except Exception as e:
        logger.warning("openstates_archive: redis write failed", flow=flow_key, error=str(e))


async def run_archive_jobs(config: dict | None = None) -> dict[str, Any]:
    """Archive every jurisdiction in ARCHIVE_ENABLED_STATES concurrently.

    Independent of the scrape schedule entirely — each jurisdiction's own
    natural-key skip check makes this safe to run at any cadence, on any
    subset of jurisdictions, without coordinating with when that
    jurisdiction's own scrape last ran.
    """
    openstates_root = _get_root(config)
    jurisdictions: list[str] = (config or {}).get(
        "jurisdictions", ["fl", "ut", "az", "wa", "va", "mi"]
    )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_archive: starting batch", jurisdictions=jurisdictions)

    results: list[dict[str, Any]] = await asyncio.gather(
        *[_run_archive(j, openstates_root) for j in jurisdictions]
    )

    duration = round(time.monotonic() - t, 1)
    failed = [r for r in results if not r["success"]]

    log_fn = logger.error if failed else logger.info
    log_fn(
        "openstates_archive: batch completed",
        jurisdictions=jurisdictions,
        total=len(results),
        failed=len(failed),
        duration_seconds=duration,
    )

    await _write_flow_status("openstates_archive", {
        "flow": "openstates_archive",
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


async def run_single_archive_job(
    jurisdiction: str,
    config: dict | None = None,
) -> dict[str, Any]:
    """Archive a single arbitrary jurisdiction. Used by the manual trigger endpoint."""
    openstates_root = _get_root(config)
    return await _run_archive(jurisdiction, openstates_root)
