"""SYNC-36: monthly trigger for GrantBot's Notion funder-scrape + Kindora
enrichment + shortlist pipeline.

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

import httpx
import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()


async def run_grantbot_scrape_job(
    config: dict | None = None, *, trigger: str = "scheduled",
) -> dict:
    """POST to CAMS's admin/scrape-funders endpoint.

    Never raises -- a bad tick must not crash the scheduler, matching every
    other job in this file's family (see mi_cookie_publish.py's own
    established shape). `config` is accepted for parity with every other
    scheduled job's wrapper signature but currently unused -- there is no
    per-run parameter to pass; CAMS runs the same closure every time.
    """
    settings = get_settings()
    if not settings.cams_api_token:
        logger.error(
            "grantbot_scrape: CAMS_API_TOKEN not configured -- skipping "
            "(this job only runs on the Mac Studio instance, co-located "
            "with CAMS)",
            trigger=trigger,
        )
        return {"success": False, "error": "cams_not_configured"}

    url = f"{settings.cams_base_url.rstrip('/')}/api/v1/admin/scrape-funders"
    headers = {"Authorization": f"Bearer {settings.cams_api_token}"}

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=headers)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(
            "grantbot_scrape: CAMS returned an error",
            trigger=trigger,
            status_code=e.response.status_code,
            body=e.response.text[:500],
        )
        return {
            "success": False,
            "error": "cams_error",
            "status_code": e.response.status_code,
        }
    except httpx.RequestError as e:
        logger.error(
            "grantbot_scrape: could not reach CAMS", trigger=trigger, error=str(e),
        )
        return {"success": False, "error": "cams_unreachable", "detail": str(e)}

    result = response.json()
    logger.info(
        "grantbot_scrape: dispatched to CAMS",
        trigger=trigger,
        status=result.get("status"),
    )
    return {"success": True, **result}
