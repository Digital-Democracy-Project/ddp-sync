"""ScrapeBot dispatch client — ddp-agents' PLAN-scrapebot.md §3.3/§3.7.

Calls CAMS's generic task API (bot="scrapebot", task_type="mint_scrape_cookies")
to get real, WAF-passing cookies + the User-Agent that minted them for a
configured jurisdiction — the same dispatch shape legbot_client.py already
uses for LegBot's analyze_bill. No CAMS-side code exists specific to this
caller beyond ScrapeBot's own agent; this mirrors an already-established
pattern rather than inventing a new one.

Scope note: this module dispatches, reads the result, and writes it into
openstates-core's existing CookieProvider on-disk cache file. It does NOT
change CookieProvider, MI_COOKIE_PROVIDER, mi_waf_get(), or any scraper
code — the cache file is the entire integration surface (PLAN §3.3).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import httpx
import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

_POLL_INTERVAL_SECONDS = 5
# 90s (PLAN §3.3, PM-review round 3 fold) was sized against ScrapeBot's
# original MoE model. ddp-agents' PLAN-scrapebot.md §9.3.2 (2026-08-05
# benchmark data): the dense model that replaced it as mint_cookies'
# default took 103.4s for a trivial 2-iter, no-CAPTCHA mint alone -- already
# over 90s. A real read/type/submit/re-check CAPTCHA loop plausibly adds
# 2-4 more iterations at a similar per-iteration cost, so total time can
# reasonably reach 200-300s+. Bumped to 240s as a conservative interim
# estimate, not a final number -- revisit once a real CAPTCHA-solve dispatch
# has actually been measured end to end.
_DEFAULT_TIMEOUT_SECONDS = 240.0
_TERMINAL_STATUSES = ("completed", "failed", "cancelled")

# CookieProvider's own session-cookie TTL fallback (openstates-core's
# cookie_provider.py, _DEFAULT_SESSION_COOKIE_TTL_SECONDS) — matched here
# exactly rather than guessed, so a session-scoped cookie (expires <= 0)
# gets the identical durability CookieProvider's own warm-up would have
# given it.
_SESSION_COOKIE_TTL_SECONDS = 3600


class ScrapeBotDispatchError(Exception):
    """Raised when a ScrapeBot dispatch fails to produce a usable
    cookie/user-agent pair.

    Callers decide how to handle this (log and move on, alert) — this
    function never swallows a failure into a fake/empty result. Per
    PLAN §3.7, a failed mint should not itself fail the caller's scrape
    job — it's an opportunistic cache-seed for the *next* scheduled run,
    not something the current run depends on.
    """


async def dispatch_mint_cookies(
    jurisdiction: str,
    *,
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
) -> dict:
    """Dispatch a mint_scrape_cookies task to ScrapeBot and return its
    real cookies + user_agent.

    Args:
        jurisdiction: a key in ScrapeBot's own
            config/scrapebot_jurisdictions.yaml (ddp-agents side) — e.g.
            "mi". Everything else (target URL, cookie names) is resolved
            CAMS-side; this client supplies only the jurisdiction key.
        timeout_seconds: how long to poll before giving up.

    Returns:
        {"cookies": [{"name": ..., "value": ..., "expires": ...}, ...],
         "user_agent": "..."} — read from task_result.json's
        WM_SNAPSHOT_KEYS["scrapebot"]-populated fields, NOT from the
        handler's own AgentResult.output (which only carries
        {"cookie_count": N} — see ddp-agents' scrapebot/handlers.py).

    Raises:
        ScrapeBotDispatchError: task failed, timed out, or its result
            couldn't be read from disk / was missing cookies or user_agent.
    """
    settings = get_settings()
    if not settings.cams_artifacts_dir:
        raise ScrapeBotDispatchError(
            "CAMS_ARTIFACTS_DIR is not configured — cannot read ScrapeBot's result."
        )

    headers = {"Authorization": f"Bearer {settings.cams_api_token}"}
    create_payload = {
        "bot": "scrapebot",
        "task_type": "mint_scrape_cookies",
        "payload": {"jurisdiction": jurisdiction},
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{settings.cams_base_url}/api/v1/tasks", headers=headers, json=create_payload
        )
        resp.raise_for_status()
        task_id = resp.json()["task_id"]
        logger.info("ScrapeBot task dispatched", task_id=task_id, jurisdiction=jurisdiction)

        status = "queued"
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            status_resp = await client.get(
                f"{settings.cams_base_url}/api/v1/tasks/{task_id}", headers=headers
            )
            status_resp.raise_for_status()
            status = status_resp.json()["status"]
            if status in _TERMINAL_STATUSES:
                break
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        else:
            raise ScrapeBotDispatchError(
                f"ScrapeBot task {task_id} did not finish within {timeout_seconds}s "
                f"(last status: {status})"
            )

    if status != "completed":
        raise ScrapeBotDispatchError(f"ScrapeBot task {task_id} ended with status={status}")

    result_path = Path(settings.cams_artifacts_dir) / task_id / "task_result.json"
    try:
        snapshot = json.loads(result_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ScrapeBotDispatchError(
            f"Could not read task_result.json for {task_id}: {exc}"
        ) from exc

    cookies, user_agent = snapshot.get("cookies"), snapshot.get("user_agent")
    # A missing key here means something upstream (the WM snapshot, a
    # handler bug) didn't populate what ScrapeBot's own contract promises
    # — treat that as a dispatch failure explicitly rather than handing
    # None through to write_cookie_cache, where it would fail unclearly
    # deep inside a dict/list comprehension instead of at this boundary.
    if not cookies or not user_agent:
        raise ScrapeBotDispatchError(
            f"task_result.json for {task_id} is missing cookies/user_agent "
            f"(got cookies={cookies!r}, user_agent={user_agent!r})"
        )

    logger.info(
        "ScrapeBot task completed", task_id=task_id, jurisdiction=jurisdiction,
        cookie_count=len(cookies),
    )
    return {"cookies": cookies, "user_agent": user_agent}


def cache_path_for(jurisdiction: str, openstates_root: str) -> str:
    """The exact on-disk path openstates-core's CookieProvider instance for
    this jurisdiction reads/writes — same value as
    os.path.join(settings.CACHE_DIR, "<jurisdiction>_waf_cookies.json")
    inside openstates-core (activate.sh's own CACHE_DIR export:
    <openstates_root>/openstates-scrapers/_cache). openstates_root is the
    SAME value _run_scrape() already reads for the same subprocess call —
    not an independently-tracked path that could drift out of sync.
    """
    cache_dir = os.path.join(openstates_root, "openstates-scrapers", "_cache")
    return os.path.join(cache_dir, f"{jurisdiction}_waf_cookies.json")


def write_cookie_cache(cache_path: str, cookies: list[dict], user_agent: str) -> None:
    """Seed a CookieProvider-compatible cache file from a ScrapeBot result.

    Byte-format matches openstates-core's CookieProvider._warm_up_and_cache()
    exactly: {cookie_name: {value, expires}, "_meta": {"user_agent": ...}}.
    CookieProvider._read_cache() reads this file cold, from a fresh process,
    on the next run-scrape.sh invocation — nothing about CookieProvider
    cares who wrote it.
    """
    now = time.time()
    data: dict = {}
    for c in cookies:
        expires = c.get("expires") or 0
        if expires <= 0:
            # Matches CookieProvider's own fallback for a session-scoped
            # cookie exactly (cookie_provider.py's
            # _DEFAULT_SESSION_COOKIE_TTL_SECONDS) — not a new guess.
            expires = now + _SESSION_COOKIE_TTL_SECONDS
        # Plain last-one-wins dict assignment on a name collision, matching
        # CookieProvider._warm_up_and_cache()'s own identical behavior —
        # adding new dedup/domain-path disambiguation here would be scope
        # this PLAN was explicitly asked to guard against.
        data[c["name"]] = {"value": c["value"], "expires": expires}
    data["_meta"] = {"user_agent": user_agent}

    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(data, f)
