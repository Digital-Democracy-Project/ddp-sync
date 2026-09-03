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

SYNC-53: publishing used to go through the same sudo-gated wrapper scraper-memory.sh uses for
mac-side writes (SCRAPER_MEMORY_S3_CMD, default a wrapper around
`/usr/local/ddp-db-proxy/s3-openstates-backups.sh`) -- but that wrapper still targets the OLD
`ddp-openstates-backups` bucket, never repointed at `ddp-openstates-scraper-memory` (created
2026-08-29 specifically for scraper memory/working-tier data, precisely so it would NOT be
subject to the backups bucket's own 30-day deletion lifecycle rule). Confirmed live: every
scheduled publish "succeeded" (the wrapper exited 0) while silently writing to a bucket
cloud_collector.py's S3Memory never reads, leaving the real target's copy frozen since the
migration -- ~4 days stale before this was noticed.

Now writes directly via boto3 straight to SCRAPER_MEMORY_S3_BUCKET, the same shape
cloud_collector.py's own S3Memory.store() already uses for this exact bucket (a plain
`upload_file`, no sudo, no intermediate wrapper script to fall out of sync with a bucket
migration again) -- rather than pointing SCRAPER_MEMORY_S3_CMD at a second wrapper script that
would need its own deploy to every host and would reproduce the same "shared indirection can
silently drift" shape this bug already demonstrated once. At the identical cache key
scraper_memory_cache_key("mi", "mi_waf_cookies.json") computes
(`${SCRAPER_MEMORY_PREFIX}/mi/_cache/mi_waf_cookies.json`) -- the same key
cloud_collector.py's own S3Memory.cache_key() and _MI_WAF_COOKIE_GLOB already read from,
so nothing on the reading side needs to change to find what this job writes.
"""

from __future__ import annotations

import os
import tempfile

import boto3
import structlog
from boto3.exceptions import S3UploadFailedError

from ddp_sync.services import scrapebot_client

logger = structlog.get_logger()

JURISDICTION = "mi"
CACHE_FILENAME = "mi_waf_cookies.json"

# SYNC-53: the bucket cloud_collector.py's own S3Memory already reads MI's published cookie
# from -- see this module's own docstring for why the old sudo-gated wrapper's default no
# longer belongs here.
DEFAULT_SCRAPER_MEMORY_S3_BUCKET = "ddp-openstates-scraper-memory"


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
    bucket = os.environ.get("SCRAPER_MEMORY_S3_BUCKET", DEFAULT_SCRAPER_MEMORY_S3_BUCKET)
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

    # An earlier pm-review already established the "never raises past this point" contract
    # this try/except exists to keep (a malformed mint_result or a write_cookie_cache failure
    # must not crash the scheduler); SYNC-53 extends the same contract to the S3 upload itself
    # (bad/missing credentials, AccessDenied, a network blip). A second pm-review pass caught
    # that upload_file() itself never lets a raw botocore.exceptions.ClientError escape --
    # boto3's own S3Transfer.upload_file wraps any ClientError it hits in
    # boto3.exceptions.S3UploadFailedError (confirmed by reading that method's source), a plain
    # Exception subclass, not a ClientError subclass -- so catching ClientError here would never
    # actually fire, and every real upload failure would fall through to the generic
    # "unexpected_error" branch below instead of "publish_failed". boto3's own default
    # connect/read timeouts (60s each) still bound a hang the same way the old subprocess
    # `timeout=60` did -- no extra timeout plumbing needed to keep that same ceiling.
    key = _publish_key(prefix)
    try:
        with tempfile.TemporaryDirectory(prefix="mi-cookie-publish-") as tmp:
            local_path = os.path.join(tmp, CACHE_FILENAME)
            scrapebot_client.write_cookie_cache(
                local_path, cookies=mint_result["cookies"], user_agent=mint_result["user_agent"]
            )
            boto3.client("s3").upload_file(local_path, bucket, key)
    except S3UploadFailedError as e:
        logger.error(
            "mi_cookie_publish: publish to S3 failed -- the previously published "
            "cookie (if any) is unchanged",
            bucket=bucket,
            error=str(e),
        )
        return {"success": False, "reason": "publish_failed", "error": str(e)}
    except Exception as e:  # noqa: BLE001 -- a bad tick must never crash the scheduler
        logger.error(
            "mi_cookie_publish: unexpected error while writing/publishing the cookie -- the "
            "previously published cookie (if any) is unchanged",
            error=str(e),
        )
        return {"success": False, "reason": "unexpected_error", "error": str(e)}

    logger.info("mi_cookie_publish: published fresh Michigan WAF cookies", bucket=bucket, key=key)
    return {"success": True, "key": key}
