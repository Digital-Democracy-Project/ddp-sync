"""SYNC-48: overlap-safe, independently-gated scraper-triggered LegBot dispatch.

Where this came from: ddp-infra's PLAN-legbot.md §32 (production-operations
readiness gate) found that every LegBot dispatch to date has been a manual
batch call via POST /trigger/bill-artifact-generation -- nothing defines or
protects the actual "run this automatically whenever a scraper finishes"
hook ddp-sync would need for a real pilot. This module is that primitive:
a thin, overlap-safe wrapper around run_legbot_pipeline
(session_pipeline_runner.py), meant to be called by whatever eventually
becomes the real scraper-completion hook.

Three design points, already decided in SYNC-48's own ticket (not open
questions -- see the ticket for the full rationale):

1. Full-session re-evaluation, not changed-bills-only. This function does
   not track "what the scraper noticed as new" -- it always asks
   run_legbot_pipeline to re-check the whole session's coverage, exactly
   like a manual dispatch does. run_legbot_pipeline already "fills in
   whatever's missing" (its own docstring), so this is a property this
   module *relies on*, not one it re-implements.

2. Overlap handling: reject outright, never queue or coalesce. A plain
   Redis SET-NX-EX lock keyed by (jurisdiction_iso2, session_code) -- same
   primitive/pattern votebot_eval.py's own concurrency lock already uses in
   this codebase (LOCK_KEY, ex=lock_ttl, fenced release). Full
   lock-correctness machinery (atomic multi-key acquisition, stale-lock
   recovery, fencing tokens beyond the simple owner-check below) is
   deliberately NOT built here, per an explicit operator anti-overengineering
   instruction on this ticket -- and it isn't needed: because of point 1,
   a rejected trigger's work is never lost, only deferred to the next
   trigger for that same jurisdiction/session. A lock that occasionally
   over-rejects (e.g. a slow release) costs a few minutes of staleness, not
   a missed bill.

   OPEN-292: this lock is now a renewed LEASE, not a flat one-shot TTL. A
   real full-session sweep (limit=5000, no bill_candidates cap) has no
   bound on how long it takes -- MI's own 2025-2026 sweep ran 24-30+ hours,
   far past the original flat 4-hour TTL, leaving the run with zero overlap
   protection for most of its own runtime. `_lock_heartbeat_loop` below
   renews the lease every `lock_renewal_seconds` (default 120s, comfortably
   under the now-shorter `lock_ttl_seconds` default of 600s) for as long as
   the pipeline is still genuinely running, re-checking ownership before
   each renewal (same GET-and-compare pattern the existing fenced release
   already uses) so it can never resurrect a lock a later trigger has
   already legitimately acquired. This is still the anti-overengineering
   instruction's spirit, not a reversal of it: a short TTL plus renewal is
   less machinery than a fixed-but-large TTL that still eventually needs
   raising again for whatever the next longer session turns out to be --
   and it has a real, concrete side benefit the old flat TTL did not: a
   process that crashes hard (no clean shutdown, no `finally` reached) now
   loses its lock within roughly one TTL window instead of up to 4 hours.

3. A trigger-specific enable flag, independent of CAMS's own LEGBOT_ENABLED.
   LEGBOT_ENABLED (on the ddp-agents/CAMS side) is the last-resort "stop
   everything, including Agent Smith's manual dispatches" switch. This
   module's own LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED flag (SyncSettings)
   is a routine, ddp-sync-local control: flip it off to pause only the
   automated path while manual dispatches (this endpoint's own sibling,
   /trigger/bill-artifact-generation) keep working.

What this module deliberately does NOT do: wire itself into a real scraper
job. openstates_scrape.py's own job functions (run_fl_scrapes_job,
run_secondary_scrapes_job, etc.) don't currently carry a resolved
session_code per jurisdiction -- run_secondary_scrapes_job's own docstring
explains why session is passed as None (VA and UT have had two sessions
simultaneously active at once, so there is no single "the session that just
finished scraping" per jurisdiction to hand this function without first
solving that separately). Wiring a real completion hook needs that problem
solved first; forcing a wrong session_code through this trigger to make the
wiring exist would defeat point 1's whole safety argument (full-session
re-evaluation only makes overlap-rejection safe for the session it actually
re-evaluates). Tracked as a named follow-up, not built here.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

_LOCK_KEY_PREFIX = "ddp_sync:scraper_triggered_legbot:lock:"


async def _renew_lock_if_owned(redis_store, lock_key: str, run_id: str, ttl: int) -> bool:
    """OPEN-292: one lease-renewal attempt. Re-checks ownership (same GET-and-
    compare pattern the fenced release below already uses) before extending the
    expiry -- an EXPIRE issued blindly could otherwise resurrect a lock that has
    already legitimately passed to a newer trigger (this run's own TTL lapsed
    and nothing renewed it in time), silently defeating the exact overlap
    protection this lock exists for.

    Never raises: a renewal failure (a Redis blip, a dropped connection) must
    never abort the LegBot pipeline this lease is only there to protect --
    same "an auxiliary concern can't affect the primary job's own result"
    contract every other hook in this codebase already follows. Returns
    whether the renewal actually happened, for the heartbeat loop's own
    logging; a caller that only cares about "did this raise" doesn't need to
    look at the return value.
    """
    try:
        current = await redis_store._client.get(lock_key)
        current_id = current.decode() if isinstance(current, bytes) else current
        if current_id != run_id:
            return False
        await redis_store._client.expire(lock_key, ttl)
        return True
    except Exception as e:  # noqa: BLE001 -- must never affect the pipeline's own result
        logger.warning(
            "scraper_triggered_legbot_lock_renewal_failed",
            run_id=run_id,
            lock_key=lock_key,
            error=str(e),
        )
        return False


async def _lock_heartbeat_loop(redis_store, lock_key: str, run_id: str, ttl: int, interval: int) -> None:
    """OPEN-292: renews the lease on a fixed cadence for as long as this task is
    alive. The caller starts this as a background asyncio task right after
    acquiring the lock, and cancels it (see trigger_scraper_session_pipeline's
    own finally block) the moment the pipeline itself finishes -- cancellation
    is the only way this loop ever stops short of the process dying outright,
    which is exactly the behavior wanted: a genuinely-crashed process stops
    renewing and its lock expires within roughly one `ttl` window, same as
    before this existed, just a much shorter window now.
    """
    while True:
        await asyncio.sleep(interval)
        await _renew_lock_if_owned(redis_store, lock_key, run_id, ttl)


def _lock_key(jurisdiction_iso2: str, session_code: str) -> str:
    """Uppercased (pm-review, OPEN-290): the two real callers that now share
    this lock don't agree on casing -- the archive-completion hook always
    passes jurisdiction.upper() (openstates_archive.py), while a manual
    bill-artifact-generation caller can send whatever case they typed (e.g.
    the lowercase 'fl' this project's own tests use). Before OPEN-290 this
    never mattered (only one caller, always uppercase, ever touched the
    lock); consolidating a second caller with untrusted casing onto the same
    lock means an exact-string mismatch would silently defeat the very
    overlap protection this ticket exists to add. session_pipeline_runner.py
    already treats jurisdiction_iso2 as case-insensitive the same way
    (`.upper()` before its own allowlist check) -- same normalization here,
    for the same reason.
    """
    return f"{_LOCK_KEY_PREFIX}{jurisdiction_iso2.upper()}:{session_code.upper()}"


async def trigger_scraper_session_pipeline(
    jurisdiction_iso2: str,
    session_code: str,
    artifact_types: list[str],
    include_org_research: bool,
    limit: int,
    *,
    include_concept_statements: bool,
    retry_failed: bool = False,
    dry_run: bool = False,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
    bill_candidates: list[dict] | None = None,
    require_trigger_enabled: bool = True,
) -> dict[str, Any]:
    """Overlap-safe entry point for both an automated scraper/archive-
    completion caller AND (OPEN-290) a manual dispatch via
    /trigger/bill-artifact-generation. Both now share this same Redis
    overlap lock, closing the gap where a manual call and an automated call
    for the same jurisdiction+session could run concurrently -- confirmed as
    a real, live incident on 2026-09-13 (two full FL/2026E runs dispatched
    20 seconds apart through the two previously-separate endpoints, neither
    visible to the other's lock).

    require_trigger_enabled: the automated caller's own
    LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED gate exists to let an operator
    pause *only* the automated path while manual dispatches keep working
    (see module docstring, point 3) -- a manual caller must not become
    newly subject to that same flag just by sharing this function, or
    flipping it off would silently break the manual endpoint too. Pass
    False for a manual/human-reviewed call; the lock itself (below) still
    always applies regardless of this flag's value.

    Never raises: mirrors run_votebot_eval's own "always return a dict,
    caller decides what to do with success=False" convention, since this is
    meant to be called from a background job path with no request/response
    cycle to raise into.

    Returns one of:
        {"success": False, "error": "trigger_disabled"}
        {"success": False, "error": "redis_unavailable"}
        {"success": False, "error": "already_running", "current_run_id": str}
        {"success": False, "error": "invalid_request", "detail": str}
        {"success": False, "error": "pipeline_error", "detail": str}
        {"success": True, "run_id": str, **run_legbot_pipeline's own result}
    """
    settings = get_settings()
    if require_trigger_enabled and not settings.legbot_scrape_completion_trigger_enabled:
        logger.info(
            "scraper_triggered_legbot_disabled",
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
        )
        return {"success": False, "error": "trigger_disabled"}

    from ddp_sync.services.redis_store import get_redis_store

    redis_store = get_redis_store()
    if not redis_store._client:
        logger.error(
            "scraper_triggered_legbot_redis_unavailable",
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
        )
        return {"success": False, "error": "redis_unavailable"}

    run_id = uuid.uuid4().hex
    lock_key = _lock_key(jurisdiction_iso2, session_code)
    lock_ttl = settings.legbot_scrape_completion_trigger_lock_ttl_seconds

    # PM review (SYNC-48): acquisition itself must not raise either -- a
    # Redis timeout/connection drop here previously would have propagated
    # out of this function despite the "never raises" contract documented
    # above. Same fenceable failure mode as the pre-pipeline branch further
    # down; both convert to redis_unavailable rather than escaping.
    try:
        acquired = await redis_store._client.set(lock_key, run_id, nx=True, ex=lock_ttl)
        if not acquired:
            existing = await redis_store._client.get(lock_key)
            existing_id = existing.decode() if isinstance(existing, bytes) else existing
    except Exception as e:  # noqa: BLE001
        logger.error(
            "scraper_triggered_legbot_redis_unavailable",
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
            error=str(e),
        )
        return {"success": False, "error": "redis_unavailable"}

    if not acquired:
        logger.warning(
            "scraper_triggered_legbot_overlap_rejected",
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
            current_run_id=existing_id,
        )
        return {
            "success": False,
            "error": "already_running",
            "current_run_id": existing_id,
        }

    start_time = datetime.now(timezone.utc)
    logger.info(
        "scraper_triggered_legbot_start",
        run_id=run_id,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
    )
    # OPEN-292: keeps the lease above alive for as long as the pipeline call below
    # actually runs -- started only now (after acquisition succeeded), cancelled in
    # the finally block below before the existing fenced release runs, so the two
    # never race each other over the same key.
    renewal_seconds = settings.legbot_scrape_completion_trigger_lock_renewal_seconds
    heartbeat_task = asyncio.create_task(
        _lock_heartbeat_loop(redis_store, lock_key, run_id, lock_ttl, renewal_seconds)
    )
    try:
        from ddp_sync.pipelines.session_pipeline_runner import run_legbot_pipeline

        result = await run_legbot_pipeline(
            jurisdiction_iso2,
            session_code,
            artifact_types,
            include_org_research,
            limit,
            include_concept_statements=include_concept_statements,
            retry_failed=retry_failed,
            dry_run=dry_run,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
            bill_candidates=bill_candidates,
        )
        logger.info(
            "scraper_triggered_legbot_complete",
            run_id=run_id,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
            duration_seconds=(datetime.now(timezone.utc) - start_time).total_seconds(),
        )
        return {"success": True, "run_id": run_id, **result}
    except ValueError as e:
        # OPEN-290: a manual caller (bill-artifact-generation) needs this
        # distinguished from pipeline_error so its route can map it to 400,
        # same as before this function mediated the call -- run_legbot_
        # pipeline raises ValueError for caller-fixable bad input (unknown
        # artifact_types, missing jurisdiction, etc.), not a server fault.
        logger.warning(
            "scraper_triggered_legbot_invalid_request",
            run_id=run_id,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
            error=str(e),
        )
        return {"success": False, "error": "invalid_request", "detail": str(e)}
    except Exception as e:  # noqa: BLE001 -- never raise into a background caller
        logger.exception(
            "scraper_triggered_legbot_pipeline_error",
            run_id=run_id,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
        )
        return {"success": False, "error": "pipeline_error", "detail": str(e)}
    finally:
        # OPEN-292: stop renewing before releasing, on every exit path -- a
        # cancelled-but-still-pending renewal racing the delete below could
        # otherwise re-set the TTL on a key this same call is simultaneously
        # deleting. asyncio.CancelledError is expected here, not a real failure;
        # suppressing it is what "cancel and wait for it to actually stop"
        # means for a task, not just "ask it to stop and move on."
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task

        # Fenced release, same pattern as votebot_eval.py's own lock: only
        # delete the lock if we still own it, so a run that outlived its own
        # TTL (and was already picked up by a newer trigger) doesn't delete
        # that newer trigger's lock out from under it.
        try:
            current = await redis_store._client.get(lock_key)
            current_id = current.decode() if isinstance(current, bytes) else current
            if current_id == run_id:
                await redis_store._client.delete(lock_key)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "scraper_triggered_legbot_lock_release_failed",
                run_id=run_id,
                error=str(e),
            )
