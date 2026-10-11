"""SYNC-36: monthly trigger for GrantBot's Notion funder-scrape + Kindora
enrichment + shortlist pipeline (and, since AGENTS-128, the twice-yearly
grantee-similarity refresh, which reuses the same CAMS call).

GrantBot's job used to self-schedule weekly entirely inside CAMS's own
internal CronScheduler -- the only recurring pipeline in the stack that
worked that way. AGENTS-54 removes that internal registration; this module
is the other half, moving the job onto the same convention every other
recurring pipeline here already follows: ddp-sync schedules it, and the
target service (CAMS) executes it via an on-demand API call. Confirmed with
Ramon (2026-08-23) that the cadence itself should also change, from weekly
to monthly.

Mac-Studio-only by construction, same as legbot_client.py and the
/trigger/legbot-analyze-bill* endpoints: CAMS only runs co-located with
ddp-sync's Mac Studio instance, so this job (and its matching on-demand
trigger, /trigger/grantbot-scrape-funders) only ever does anything useful
there -- gated on cams_api_token being configured, the same signal those
endpoints use.

Fire-and-forget by design (SYNC-36's own ask): CAMS's
POST /api/v1/admin/scrape-funders already launches the full scrape +
enrichment + shortlist-post as its own background task and holds its own
Redis lock (grantbot:notion_scrape:running) against a concurrent run, so
there is nothing for this caller to poll or await -- unlike
legbot_client.py's dispatch-and-poll shape for LegBot's synchronous
analyze_bill tasks.
"""
from __future__ import annotations

import asyncio

import httpx
import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

# pm-review: a single failed attempt used to be a wait-until-next-Monday cost;
# now that the trigger is monthly, the same single failure costs roughly a
# month instead of a week. A small bounded retry -- not a generic retry
# framework, just enough to ride out CAMS being mid-`cams reload` -- is
# proportionate here in a way it wasn't at weekly cadence.
_MAX_ATTEMPTS = 3
_RETRY_DELAY_S = 30.0


async def _post_to_cams(path: str, label: str, *, trigger: str, conflict_is_success: bool = False) -> dict:
    """POST to one of CAMS's admin endpoints (``/api/v1/admin/<path>``).

    Never raises -- a bad tick must not crash the scheduler, matching every
    other job in this file's family (see mi_cookie_publish.py's own
    established shape). ``label`` prefixes the log lines so each job reads as its own.
    ``conflict_is_success``: a 409 means "a run is already in progress" (the similarity refresh holds a lock),
    which is a fine outcome and is not retried. Off by default, so the funder scrape treats a 409 as the
    error it always did.

    Retries a connection failure or a 5xx up to _MAX_ATTEMPTS times,
    _RETRY_DELAY_S apart -- enough to survive CAMS being mid-restart, not a
    general resilience mechanism. A 4xx (e.g. an auth misconfiguration) is
    not retried; retrying it would just fail the same way three times.
    """
    settings = get_settings()
    if not settings.cams_api_token:
        logger.error(
            f"{label}: CAMS_API_TOKEN not configured -- skipping "
            "(this job only runs on the Mac Studio instance, co-located "
            "with CAMS)",
            trigger=trigger,
        )
        return {"success": False, "error": "cams_not_configured"}

    url = f"{settings.cams_base_url.rstrip('/')}/api/v1/admin/{path}"
    headers = {"Authorization": f"Bearer {settings.cams_api_token}"}

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        last_attempt = attempt == _MAX_ATTEMPTS
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, headers=headers)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if conflict_is_success and e.response.status_code == 409:
                logger.info(f"{label}: CAMS says a run is already in progress", trigger=trigger)
                return {"success": True, "status": "already_running"}
            if e.response.status_code < 500 or last_attempt:
                logger.error(
                    f"{label}: CAMS returned an error",
                    trigger=trigger,
                    attempt=attempt,
                    status_code=e.response.status_code,
                    body=e.response.text[:500],
                )
                return {
                    "success": False,
                    "error": "cams_error",
                    "status_code": e.response.status_code,
                }
            logger.warning(
                f"{label}: CAMS returned a 5xx -- retrying",
                trigger=trigger, attempt=attempt, status_code=e.response.status_code,
            )
            await asyncio.sleep(_RETRY_DELAY_S)
            continue
        except httpx.RequestError as e:
            if last_attempt:
                logger.error(
                    f"{label}: could not reach CAMS",
                    trigger=trigger, attempt=attempt, error=str(e),
                )
                return {"success": False, "error": "cams_unreachable", "detail": str(e)}
            logger.warning(
                f"{label}: could not reach CAMS -- retrying",
                trigger=trigger, attempt=attempt, error=str(e),
            )
            await asyncio.sleep(_RETRY_DELAY_S)
            continue

        result = response.json()
        logger.info(
            f"{label}: dispatched to CAMS",
            trigger=trigger,
            attempt=attempt,
            status=result.get("status"),
        )
        return {"success": True, **result}

    # Unreachable: the loop above always returns on its last_attempt branch.
    raise AssertionError("_post_to_cams: retry loop exited without returning")


async def run_grantbot_scrape_job(
    config: dict | None = None, *, trigger: str = "scheduled",
) -> dict:
    """POST to CAMS's admin/scrape-funders endpoint (monthly funder scrape + Kindora enrichment).

    `config` is accepted for parity with every other scheduled job's wrapper
    signature but currently unused -- there is no per-run parameter to pass;
    CAMS runs the same closure every time. See ``_post_to_cams`` for the
    retry and never-raises behavior.
    """
    return await _post_to_cams("scrape-funders", "grantbot_scrape", trigger=trigger)


async def run_grantbot_similarity_refresh_job(
    config: dict | None = None, *, trigger: str = "scheduled",
) -> dict:
    """POST to CAMS's admin/refresh-grantee-similarity endpoint (AGENTS-128).

    The twice-yearly full refresh of grantee similarity (recollect stale grant history, then
    recompute similarity for every funder). CAMS runs it in the background for a day or more, and
    answers 409 if one is already running, which counts as success here.
    """
    return await _post_to_cams(
        "refresh-grantee-similarity", "grantbot_similarity_refresh", trigger=trigger, conflict_is_success=True,
    )
