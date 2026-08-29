"""OPEN-188: mint Michigan's WAF cookies on a schedule and publish them to the shared S3
memory store, so the cloud collector reads a recent cookie instead of calling in for one at
scrape time (PLAN-scraper-execution-migration.md §6, "publish, do not call in").

The original sketch had the cloud collector mint its own cookie before scraping. That creates
two things this design avoids: the collection depends on the publisher being up at that exact
moment, and something must reach *inbound* to mint. Publishing on an independent schedule and
letting the collector pull whenever it runs needs neither -- no inbound path, no availability
coupling at scrape time, and a cookie has a lifetime of its own so a recent one is as good as a
fresh one.

Uses ScrapeBot's existing shipped mint path (scrapebot_client.dispatch_mint_cookies /
write_cookie_cache) -- the same one _maybe_preseed_scrapebot_cookies already uses to warm the
mac's own local cache before a scheduled Michigan scrape. This job does the same mint, on its
own independent schedule, and additionally publishes the result to S3 so a cloud run (which has
no local cache to warm) can read it too.

Publishing goes through the same sudo-gated wrapper scraper-memory.sh already uses for every
other mac-side write to the memory store (SCRAPER_MEMORY_S3_CMD), at the identical cache key
scraper_memory_cache_key("mi", "mi_waf_cookies.json") computes
(`${SCRAPER_MEMORY_PREFIX}/mi/_cache/mi_waf_cookies.json`) -- the same key
cloud_collector.py's own S3Memory.cache_key() and _MI_WAF_COOKIE_GLOB already read from,
so nothing on the reading side needs to change to find what this job writes.
"""

from __future__ import annotations

import os
import subprocess
import tempfile

import structlog

from ddp_sync.services import scrapebot_client

logger = structlog.get_logger()

JURISDICTION = "mi"
CACHE_FILENAME = "mi_waf_cookies.json"


def _publish_key(prefix: str) -> str:
    """Matches scraper_memory_cache_key's own formula (scraper-memory.sh) and
    S3Memory.cache_key's (cloud_collector.py) exactly -- all three have to agree on this
    string or the publisher and the two readers are talking about different objects."""
    return f"{prefix}/{JURISDICTION}/_cache/{CACHE_FILENAME}"


async def run_mi_cookie_publish_job(config: dict | None = None) -> dict:
    """Mint fresh Michigan WAF cookies and publish them to the shared S3 memory store.

    Best-effort, matching _maybe_preseed_scrapebot_cookies' own established philosophy: a
    failed mint or publish on one scheduled tick must not raise or crash the scheduler --
    whatever was published last stays in place, and a genuinely stale store is caught by
    cloud_collector.py's own staleness check at read time (OPEN-188's other, gated half),
    not by this job succeeding on every single tick.
    """
    bucket_cmd = os.environ.get("SCRAPER_MEMORY_S3_CMD", "ddp-prod-s3-openstates-backups")
    prefix = os.environ.get("SCRAPER_MEMORY_PREFIX")
    if not prefix:
        # Matches SourceLock/S3Memory's own refusal (cloud_collector.py) and
        # scraper-memory.sh's SCRAPER_MEMORY_PREFIX requirement -- an unnamespaced key
        # would let this overwrite a different environment's published cookie (OPEN-159/172).
        logger.error(
            "mi_cookie_publish: SCRAPER_MEMORY_PREFIX is not set -- refusing to publish "
            "under an unnamespaced key"
        )
        return {"success": False, "reason": "missing_scraper_memory_prefix"}

    try:
        mint_result = await scrapebot_client.dispatch_mint_cookies(JURISDICTION)
    except scrapebot_client.ScrapeBotDispatchError as e:
        logger.warning(
            "mi_cookie_publish: mint failed, leaving the previously published cookie "
            "(if any) in place",
            error=str(e),
        )
        return {"success": False, "reason": "mint_failed", "error": str(e)}

    # pm-review: the docstring's "never raises" claim wasn't actually true past this point --
    # a malformed mint_result, a write_cookie_cache failure, a missing/non-executable
    # bucket_cmd (FileNotFoundError), or subprocess.run hanging indefinitely (no timeout) could
    # all have raised out of this best-effort job and crashed the scheduler. Caught broadly and
    # explicitly, with a timeout, rather than letting any of them propagate.
    key = _publish_key(prefix)
    try:
        with tempfile.TemporaryDirectory(prefix="mi-cookie-publish-") as tmp:
            local_path = os.path.join(tmp, CACHE_FILENAME)
            scrapebot_client.write_cookie_cache(
                local_path, cookies=mint_result["cookies"], user_agent=mint_result["user_agent"]
            )
            proc = subprocess.run(
                [bucket_cmd, "put", local_path, key],
                capture_output=True, text=True, timeout=60, check=False,
            )
    except subprocess.TimeoutExpired:
        logger.error(
            "mi_cookie_publish: publish to S3 timed out -- the previously published cookie "
            "(if any) is unchanged"
        )
        return {"success": False, "reason": "publish_timed_out"}
    except Exception as e:  # noqa: BLE001 -- a bad tick must never crash the scheduler
        logger.error(
            "mi_cookie_publish: unexpected error while writing/publishing the cookie -- the "
            "previously published cookie (if any) is unchanged",
            error=str(e),
        )
        return {"success": False, "reason": "unexpected_error", "error": str(e)}

    if proc.returncode != 0:
        logger.error(
            "mi_cookie_publish: publish to S3 failed -- the previously published "
            "cookie (if any) is unchanged",
            stderr=proc.stderr,
        )
        return {"success": False, "reason": "publish_failed", "error": proc.stderr}

    logger.info("mi_cookie_publish: published fresh Michigan WAF cookies", key=key)
    return {"success": True, "key": key}
