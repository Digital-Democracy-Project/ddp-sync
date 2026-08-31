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
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
import structlog

from ddp_sync.pipelines.openstates_scrape import _run_with_group_kill
from ddp_sync.services import scrapebot_client

logger = structlog.get_logger()

DEFAULT_OPENSTATES_ROOT = "/Users/agentsmith/Developer/repos/ddp-open-states"

# Same jurisdiction list as ddp-open-states's ARCHIVE_ENABLED_STATES (activate.sh) and
# openstates_archive.jurisdictions in sync_schedule.yaml. Exported so callers (the manual
# trigger endpoint, run_archive_jobs' own default) share one definition instead of each
# hardcoding their own copy -- triggers.py's copy silently went stale when ma/al/us were
# added here 2026-08-10, exactly the class of bug a single shared constant prevents.
DEFAULT_ARCHIVE_JURISDICTIONS = ["fl", "ut", "az", "wa", "va", "mi", "ma", "al", "us"]

# Per-jurisdiction archive timeouts. Downloads + extracts every not-yet-captured
# document, so these scale with total historical bill count, not just what
# changed recently — sized generously, matching the scrape timeouts' shape.
# `us` has ~83k never-archived documents as of 2026-08-10 (vs. low
# thousands/tens-of-thousands for the state jurisdictions) — its first several
# weekly runs are a cold backfill, not steady-state, so it gets the longest
# runway here.
#
# SYNC-2: `mi` needs its own entry rather than falling through to `default`.
# MI's document backlog is ~8k docs at ~10s/doc via os-text-extract -- 20h+ for
# a healthy run -- so the 4h default kills a run that is making genuine
# progress every single day, not just on a bad day. Given `us`'s 24h for a
# comparable cold-backfill shape, mi gets the same runway rather than a
# number sized to one measurement that the next run then exceeds (the same
# trap openstates_scrape.py's SCRAPE_TIMEOUT_S comment describes for MA).
ARCHIVE_TIMEOUT_S: dict[str, int] = {
    "fl": 16 * 3600,
    "wa": 8 * 3600,
    "mi": 24 * 3600,
    "us": 24 * 3600,
    "default": 4 * 3600,
}


def _get_root(config: dict | None) -> str:
    return (config or {}).get("openstates_root", DEFAULT_OPENSTATES_ROOT)


def _alert_archive_failure(jurisdiction: str, error: str, duration_seconds: float) -> None:
    """Best-effort Slack + CAMS alert for an archive run that ddp-sync itself gave up on.

    Mirrors openstates_scrape.py's _alert_scrape_failure exactly (same channel/token
    convention, same CAMS payload shape) -- SYNC-2: the archive pipeline never got any of
    the scrape pipeline's operational hardening. run-archive.sh has its own ERR trap that
    posts to Slack #automation-errors and CAMS on an ordinary in-process failure, but that
    only fires from *inside* the script's own process. A ddp-sync group-kill on timeout, a
    signal delivered straight to run-archive.sh's own process, or an exception raised here
    before/while invoking the subprocess all happen outside that process entirely, so
    run-archive.sh never gets a chance to alert on any of them. Before this, all three were
    100% silent: logged at ERROR and written to a Redis flow-status key nothing surfaces.
    Real incident 2026-08-07: MI's 4h archive timeout killed the wrapper, its own alert
    never fired, and the orphaned os-text-extract archiver ran ~24h more with no one told.
    Never raises -- same convention as every other alerting call site in this codebase.
    """
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if token:
        channel = os.getenv("HEALTH_ALERT_SLACK_CHANNEL", "#automation-errors")
        text = (
            f":red_circle: *OpenStates archive failed: {jurisdiction}* — {error} "
            f"(after {duration_seconds:.0f}s) — check ddp-sync logs / os-text-extract logs"
        )
        try:
            resp = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {token}"},
                json={"channel": channel, "text": text},
                timeout=15,
            )
            if not (resp.ok and resp.json().get("ok")):
                logger.error("openstates_archive: Slack alert failed", response=resp.text[:200])
        except Exception as e:  # noqa: BLE001
            logger.error("openstates_archive: Slack alert error", error=str(e))
    else:
        logger.warning(
            "openstates_archive: SLACK_BOT_TOKEN not set — cannot alert on archive failure"
        )

    cams_token = os.getenv("CAMS_API_TOKEN", "")
    if cams_token:
        cams_url = os.getenv("CAMS_BASE_URL", "http://localhost:8000")
        payload = {
            "v": 1,
            "service": "ddp-sync",
            "error_type": "ArchiveTimeoutOrSubprocessError",
            "message": f"archive failed for {jurisdiction}: {error} (after {duration_seconds:.0f}s)",
            "metadata": {"jurisdiction": jurisdiction},
        }
        try:
            resp = requests.post(
                f"{cams_url}/api/v1/failures",
                headers={"Authorization": f"Bearer {cams_token}", "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10,
            )
            if not resp.ok:
                logger.error("openstates_archive: CAMS report failed", status=resp.status_code)
        except Exception as e:  # noqa: BLE001
            logger.error("openstates_archive: CAMS report error", error=str(e))


def _scrapebot_eligible(jurisdiction: str, config: dict | None) -> bool:
    """Is this jurisdiction opted into ScrapeBot cookie pre-seeding for archive runs?

    Config-gated per jurisdiction under openstates_archive.scrapebot_fallback in
    sync_schedule.yaml -- absent/disabled by default, so adding this never changes
    behavior for a jurisdiction that hasn't explicitly opted in. Same shape as the
    scrape pipeline's secondary.scrapebot_fallback block (openstates_scrape.py), but
    keyed inside this pipeline's own config subtree: run_archive_jobs() receives the
    openstates_archive: section directly, so there is no "secondary" level here.
    """
    fallback_cfg = (config or {}).get("scrapebot_fallback", {})
    if not fallback_cfg.get("enabled", False):
        return False
    return jurisdiction in fallback_cfg.get("jurisdictions", [])


async def _maybe_preseed_scrapebot_cookies(
    jurisdiction: str,
    config: dict | None,
    openstates_root: str,
) -> None:
    """Proactively mint fresh WAF-passing cookies via ScrapeBot before archiving a
    jurisdiction opted into scrapebot_fallback -- the same proactive pre-seed the
    scrape pipeline has done since 2026-08-04 (openstates_scrape.py,
    PLAN-scrapebot.md §3.7), which this pipeline never got when archiving was split
    out of run-scrape.sh on 2026-07-31.

    Without it, an archive run depends on whatever cookies the last scrape's
    pre-seed left in the cache file; once a mid-run WAF block invalidates that
    cache, CookieProvider falls back to its own local Playwright self-warm --
    observed 2026-08-07/08: a MI archive run ground through ~24h at a 30s warm-up
    timeout per document, with the WAF refusing to issue the required cookies to
    the local headless browser at all.

    Best-effort: a mint failure here must never block or fail the archive run that
    follows -- it just proceeds with whatever's already cached, or CookieProvider's
    own self-warm.
    """
    if not _scrapebot_eligible(jurisdiction, config):
        return
    try:
        mint_result = await scrapebot_client.dispatch_mint_cookies(jurisdiction)
        cache_path = scrapebot_client.cache_path_for(jurisdiction, openstates_root)
        scrapebot_client.write_cookie_cache(
            cache_path,
            cookies=mint_result["cookies"],
            user_agent=mint_result["user_agent"],
        )
        logger.info(
            "openstates_archive: ScrapeBot pre-seeded fresh cookies before archive",
            jurisdiction=jurisdiction,
            cache_path=cache_path,
        )
    except scrapebot_client.ScrapeBotDispatchError as e:
        logger.warning(
            "openstates_archive: ScrapeBot pre-seed mint failed, proceeding with "
            "existing/self-warmed cookies",
            jurisdiction=jurisdiction,
            error=str(e),
        )


async def _run_archive(
    jurisdiction: str,
    openstates_root: str,
    timeout_s: int | None = None,
    config: dict | None = None,
) -> dict[str, Any]:
    """Run run-archive.sh for one jurisdiction off the event loop.

    Pre-seeds ScrapeBot cookies first for any jurisdiction opted into
    scrapebot_fallback -- see _maybe_preseed_scrapebot_cookies()'s docstring. A
    no-op for every jurisdiction not opted in (config defaults to None, in which
    case it's always a no-op).

    SYNC-2: uses openstates_scrape's _run_with_group_kill rather than a bare
    subprocess.run(timeout=...). subprocess.run(timeout=...) only kills the direct
    child (run-archive.sh itself) on TimeoutExpired -- start_new_session=True makes
    the wrapper the leader of its own process group, but nothing then targets that
    group, so its grandchildren (os-text-extract archive <state> and its tee)
    survive, orphaned and unsupervised, in the detached session. Observed live
    2026-08-07: MI's archiver kept running headless for ~24h after its 4h wrapper
    timeout killed only the wrapper. _run_with_group_kill's timeout path instead
    killpg()s the whole group, reaching the grandchildren too. Only that half of
    the helper is used here -- progress_dir/stall_seconds are left unset, so no
    stall watchdog runs; OPEN-155's stall detection is out of this ticket's scope.
    """
    await _maybe_preseed_scrapebot_cookies(jurisdiction, config, openstates_root)

    script = os.path.join(openstates_root, "run-archive.sh")
    cmd = ["/bin/bash", script, jurisdiction]

    timeout = timeout_s or ARCHIVE_TIMEOUT_S.get(jurisdiction, ARCHIVE_TIMEOUT_S["default"])

    logger.info("openstates_archive: starting", jurisdiction=jurisdiction, timeout_s=timeout)

    start = time.monotonic()
    try:
        returncode, _stdout, stderr, timed_out, _stalled = await asyncio.to_thread(
            _run_with_group_kill, cmd, dict(os.environ), timeout
        )
        duration = round(time.monotonic() - start, 1)

        if timed_out:
            logger.error(
                "openstates_archive: timeout",
                jurisdiction=jurisdiction,
                timeout_s=timeout,
                duration_seconds=duration,
            )
            # Whole process group killed above, before run-archive.sh's own ERR trap ever got
            # a chance to run -- it never alerted on this one. We're the only ones who know it
            # happened, so we're the only ones who can alert.
            _alert_archive_failure(jurisdiction, f"timed out after {timeout}s", duration)
            return {
                "success": False,
                "error": "timeout",
                "jurisdiction": jurisdiction,
                "duration_seconds": duration,
            }

        if returncode != 0:
            stderr_tail = (stderr or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_archive: failed",
                jurisdiction=jurisdiction,
                returncode=returncode,
                stderr_tail=stderr_tail,
                duration_seconds=duration,
            )
            if returncode < 0:
                # Negative returncode = killed by a signal that didn't originate from our own
                # timeout handling above (OOM killer, an operator's `kill`, another supervisor).
                # run-archive.sh's own ERR trap only fires on an ordinary command failure inside
                # the script, not on the script's own process receiving a terminating signal --
                # so unlike a plain nonzero exit, this one was never self-alerted.
                _alert_archive_failure(jurisdiction, f"killed by signal {-returncode}", duration)
            # else (positive returncode): run-archive.sh's own ERR trap already fired its
            # Slack/CAMS alert from inside the process before exiting nonzero -- alerting again
            # here would double-page for the exact same failure.
            return {
                "success": False,
                "error": f"exit_code_{returncode}",
                "jurisdiction": jurisdiction,
                "duration_seconds": duration,
            }

        logger.info(
            "openstates_archive: done",
            jurisdiction=jurisdiction,
            duration_seconds=duration,
        )
        return {"success": True, "jurisdiction": jurisdiction, "duration_seconds": duration}

    except Exception as e:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "openstates_archive: subprocess error",
            jurisdiction=jurisdiction,
            error=str(e),
            duration_seconds=duration,
        )
        # Something failed before/while invoking the subprocess itself (e.g. the script or
        # openstates_root path doesn't exist) -- run-archive.sh never started, so it never had
        # a chance to alert either.
        _alert_archive_failure(jurisdiction, str(e), duration)
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
        "jurisdictions", DEFAULT_ARCHIVE_JURISDICTIONS
    )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_archive: starting batch", jurisdictions=jurisdictions)

    results: list[dict[str, Any]] = await asyncio.gather(
        *[_run_archive(j, openstates_root, config=config) for j in jurisdictions]
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
    return await _run_archive(jurisdiction, openstates_root, config=config)
