"""SYNC-87: keep the api-v3 `ddp_bill_search` projection current after a jurisdiction's archive.

PLAN-enterprise-search.md 4.5.5. `ddp_bill_search` is a derived table inside the ddp-openstates
database (api-v3 fork, OPEN-308/309) that powers `/ddp/search`. It only changes when bills or their
archived documents change, i.e. right after an archive run, so the refresh is one more independent
post-archive hook (`openstates_archive._maybe_refresh_bill_search`), same shape as the LegBot and
knowledge-base hooks. This module only holds the call.

Which api-v3: ALWAYS `settings.rds_openstates_api_base` / `rds_openstates_api_key`. The projection
table exists only on the RDS-backed instance (the Mac's api-v3 has none), and archives run on
Fargate and extract into RDS, so there is no Mac-vs-EC2 read split here (unlike the LegBot and
embedding hooks). From the Mac that base is reached over WireGuard.

`POST /ddp/search/refresh?jurisdiction=<iso2>&limit=<n>` is bounded per call (a ~20 s time budget
server-side) and returns `{refreshed, with_text, orphans_removed, more, busy}`; a caller loops while
`more`. `busy` means another refresh of the same jurisdiction holds its advisory lock; nothing was
done and the caller retries. A refresh only rebuilds rows whose bill changed, so a run that stops
early is repaired by the next archive run and never leaves bad data, only stale search results,
which is why a failure here is logged, never raised and never retried beyond `busy`.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import structlog

logger = structlog.get_logger()

REFRESH_PATH = "/ddp/search/refresh"
REFRESH_LIMIT = 200  # bills per call (api-v3's own default; its maximum is 1000)
BUSY_MAX_ATTEMPTS = 3  # SYNC-87: retries of a `busy` response, per call
BUSY_BACKOFF_SECONDS = 20.0
# A call is bounded to ~20 s server-side but the statement itself can run longer on RDS.
_REQUEST_TIMEOUT_SECONDS = 120.0
# Ceiling on calls per run so a misbehaving endpoint that always says `more` cannot loop forever.
# The measured first build of the largest jurisdiction (US, ~38k bills) took about 50 calls.
MAX_CALLS_PER_RUN = 200


async def refresh_bill_search(
    jurisdiction: str,
    *,
    api_base: str,
    api_key: str = "",
) -> dict[str, Any]:
    """Refresh one jurisdiction's `ddp_bill_search` rows until api-v3 says nothing is left.

    Never raises. Returns run totals with `drained`: True only when the last call reported neither
    `more` nor `busy`. When it is False, logs WARNING `bill_search_refresh_incomplete` with the
    reason; the next archive run picks the remaining stale rows up."""
    totals: dict[str, Any] = {
        "jurisdiction": jurisdiction, "calls": 0, "refreshed": 0, "with_text": 0,
        "orphans_removed": 0, "drained": False,
    }
    reason = await _loop(jurisdiction, api_base, api_key, totals)
    if reason is None:
        totals["drained"] = True
        logger.info("bill_search_refresh_run", **totals)
    else:
        logger.warning("bill_search_refresh_incomplete", reason=reason, **totals)
    return totals


async def _loop(jurisdiction: str, api_base: str, api_key: str, totals: dict[str, Any]) -> str | None:
    """Run the call loop, adding to `totals`. Returns None when drained, else why it stopped."""
    headers = {"x-api-key": api_key} if api_key else {}
    params = {"jurisdiction": jurisdiction.lower(), "limit": str(REFRESH_LIMIT)}
    url = f"{api_base}{REFRESH_PATH}"

    while totals["calls"] < MAX_CALLS_PER_RUN:
        result = None
        for attempt in range(1, BUSY_MAX_ATTEMPTS + 1):
            try:
                async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
                    resp = await client.post(url, params=params, headers=headers)
            except httpx.RequestError as exc:
                return f"api-v3 unreachable: {exc}"
            if resp.status_code >= 400:
                return f"api-v3 rejected the refresh ({resp.status_code})"
            try:
                result = resp.json()
            except ValueError:
                return "api-v3 returned a non-JSON body"
            if not isinstance(result, dict):
                return "api-v3 returned an unexpected body"
            if not result.get("busy"):
                break
            result = None
            if attempt < BUSY_MAX_ATTEMPTS:
                await asyncio.sleep(BUSY_BACKOFF_SECONDS)
        if result is None:
            return f"jurisdiction still busy after {BUSY_MAX_ATTEMPTS} attempts"

        totals["calls"] += 1
        progress = 0
        for key in ("refreshed", "with_text", "orphans_removed"):
            value = int(result.get(key) or 0)
            totals[key] += value
            if key != "with_text":
                progress += value
        if not result.get("more"):
            return None
        if progress == 0:
            return "api-v3 reports more work but made no progress"
    return f"stopped after {MAX_CALLS_PER_RUN} calls"

