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

3. A trigger-specific enable flag, independent of CAMS's own LEGBOT_ENABLED.
   LEGBOT_ENABLED (on the ddp-agents/CAMS side) is the last-resort "stop
   everything, including Agent Smith's manual dispatches" switch. This
   module's own SESSION_PIPELINE_SCRAPER_TRIGGER_ENABLED flag (SyncSettings)
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

import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

_LOCK_KEY_PREFIX = "ddp_sync:scraper_triggered_legbot:lock:"


def _lock_key(jurisdiction_iso2: str, session_code: str) -> str:
    return f"{_LOCK_KEY_PREFIX}{jurisdiction_iso2}:{session_code}"


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
) -> dict[str, Any]:
    """Overlap-safe, independently-gated entry point for an automated
    scraper-completion caller. Manual dispatches (the existing
    /trigger/bill-artifact-generation endpoint, Agent Smith's own dispatches)
    should keep calling run_legbot_pipeline directly -- this wrapper's lock
    and enable-flag gate exist specifically for an automated caller with no
    human reviewing each call before it fires.

    Never raises: mirrors run_votebot_eval's own "always return a dict,
    caller decides what to do with success=False" convention, since this is
    meant to be called from a background job path with no request/response
    cycle to raise into.

    Returns one of:
        {"success": False, "error": "trigger_disabled"}
        {"success": False, "error": "redis_unavailable"}
        {"success": False, "error": "already_running", "current_run_id": str}
        {"success": False, "error": "pipeline_error", "detail": str}
        {"success": True, "run_id": str, **run_legbot_pipeline's own result}
    """
    settings = get_settings()
    if not settings.session_pipeline_scraper_trigger_enabled:
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
    lock_ttl = settings.session_pipeline_scraper_trigger_lock_ttl_seconds

    acquired = await redis_store._client.set(lock_key, run_id, nx=True, ex=lock_ttl)
    if not acquired:
        existing = await redis_store._client.get(lock_key)
        existing_id = existing.decode() if isinstance(existing, bytes) else existing
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
        )
        logger.info(
            "scraper_triggered_legbot_complete",
            run_id=run_id,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
            duration_seconds=(datetime.now(timezone.utc) - start_time).total_seconds(),
        )
        return {"success": True, "run_id": run_id, **result}
    except Exception as e:  # noqa: BLE001 -- never raise into a background caller
        logger.exception(
            "scraper_triggered_legbot_pipeline_error",
            run_id=run_id,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
        )
        return {"success": False, "error": "pipeline_error", "detail": str(e)}
    finally:
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
