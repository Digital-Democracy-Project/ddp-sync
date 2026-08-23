"""Redis client wrapper for shared state.

Provides:
- Sync task state and heartbeat tracking
- Sync checkpoint tracking for crash resume
- Active jurisdiction tracking
- Bill version cache (90-day TTL)
- Graceful fallback: all methods no-op when Redis is unavailable
"""

import json
from typing import Optional

import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

# Redis key constants
ACTIVE_JURISDICTIONS_KEY = "ddp:active_jurisdictions"
SYNC_CHECKPOINT_PREFIX = "ddp:sync:checkpoint:"
SYNC_CHECKPOINT_TTL = 86400  # 24 hours
BILL_VERSION_PREFIX = "ddp:bill_version:"
BILL_VERSION_TTL = 86400 * 90  # 90 days
BILL_STATUS_PREFIX = "ddp:bill_status:"
BILL_STATUS_TTL = 86400 * 90  # 90 days
FLOW_STATUS_PREFIX = "ddp:flow:"
FLOW_STATUS_TTL = 86400 * 7  # 7 days


class RedisStore:
    """Thin wrapper around redis.asyncio for shared state."""

    def __init__(self):
        self._client = None

    @property
    def is_available(self) -> bool:
        return self._client is not None

    async def connect(self):
        """Connect to Redis. Called from app.py lifespan startup."""
        try:
            import redis.asyncio as aioredis

            settings = get_settings()
            self._client = aioredis.from_url(
                settings.redis_url,
                decode_responses=True,
            )
            # Verify connectivity
            await self._client.ping()
            logger.info("Redis connected", url=settings.redis_url)
        except Exception as e:
            logger.warning(
                "Redis unavailable — falling back to in-memory state",
                error=str(e),
            )
            self._client = None

    async def disconnect(self):
        """Disconnect from Redis. Called from app.py lifespan shutdown."""
        if self._client:
            try:
                await self._client.close()
            except Exception:
                pass
            self._client = None
            logger.info("Redis disconnected")

    # -- Pub/sub publish --

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message on a Redis pub/sub channel.

        Returns the number of subscribers that received the message (0 if Redis
        is down or no subscribers). Failures are logged but never raised — pub/sub
        delivery is fire-and-forget by design (subscribers handle missed events
        via reconciliation on startup).
        """
        if not self._client:
            return 0
        try:
            return int(await self._client.publish(channel, message))
        except Exception as e:
            logger.warning("Redis: publish failed", channel=channel, error=str(e))
            return 0

    # -- Sync task storage --

    SYNC_TASK_PREFIX = "ddp:sync:task:"
    SYNC_TASK_TTL = 86400  # 24 hours

    async def set_sync_task(self, task_id: str, task_data: dict):
        """Store sync task state in Redis with TTL."""
        if not self._client:
            return
        try:
            await self._client.set(
                f"{self.SYNC_TASK_PREFIX}{task_id}",
                json.dumps(task_data),
                ex=self.SYNC_TASK_TTL,
            )
        except Exception as e:
            logger.error("Redis: failed to set sync task", task_id=task_id, error=str(e))

    async def get_sync_task(self, task_id: str) -> dict | None:
        """Retrieve sync task state from Redis."""
        if not self._client:
            return None
        try:
            data = await self._client.get(f"{self.SYNC_TASK_PREFIX}{task_id}")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error("Redis: failed to get sync task", task_id=task_id, error=str(e))
        return None

    # -- Sync checkpoint tracking --

    async def add_sync_checkpoint(self, task_id: str, item_id: str):
        """Record a completed item ID for a sync task (SADD + TTL refresh)."""
        if not self._client:
            return
        try:
            key = f"{SYNC_CHECKPOINT_PREFIX}{task_id}"
            await self._client.sadd(key, item_id)
            await self._client.expire(key, SYNC_CHECKPOINT_TTL)
        except Exception as e:
            logger.error("Redis: failed to add sync checkpoint", task_id=task_id, error=str(e))

    async def get_sync_checkpoints(self, task_id: str) -> set[str]:
        """Get all completed item IDs for a sync task."""
        if not self._client:
            return set()
        try:
            return await self._client.smembers(f"{SYNC_CHECKPOINT_PREFIX}{task_id}")
        except Exception as e:
            logger.error("Redis: failed to get sync checkpoints", task_id=task_id, error=str(e))
            return set()

    async def copy_sync_checkpoints(self, from_task_id: str, to_task_id: str) -> int:
        """Copy checkpoints from a previous task to a new one for resume.

        Returns the number of checkpoints copied.
        """
        if not self._client:
            return 0
        try:
            src_key = f"{SYNC_CHECKPOINT_PREFIX}{from_task_id}"
            dst_key = f"{SYNC_CHECKPOINT_PREFIX}{to_task_id}"
            # COPY command (Redis 6.2+). Fall back to SUNIONSTORE if unavailable.
            try:
                await self._client.copy(src_key, dst_key, replace=True)
            except Exception:
                await self._client.sunionstore(dst_key, src_key)
            await self._client.expire(dst_key, SYNC_CHECKPOINT_TTL)
            count = await self._client.scard(dst_key)
            return count
        except Exception as e:
            logger.error(
                "Redis: failed to copy sync checkpoints",
                from_task_id=from_task_id,
                to_task_id=to_task_id,
                error=str(e),
            )
            return 0

    # -- Active jurisdictions tracking --

    async def add_active_jurisdiction(self, code: str):
        """Register a jurisdiction as actively tracked."""
        if not self._client:
            return
        try:
            await self._client.sadd(ACTIVE_JURISDICTIONS_KEY, code.upper())
        except Exception as e:
            logger.warning("Redis: failed to add active jurisdiction", code=code, error=str(e))

    async def get_active_jurisdictions(self) -> set[str]:
        """Get all actively tracked jurisdictions."""
        if not self._client:
            return set()
        try:
            return await self._client.smembers(ACTIVE_JURISDICTIONS_KEY)
        except Exception as e:
            logger.warning("Redis: failed to get active jurisdictions", error=str(e))
            return set()

    # -- Bill version tracking --

    async def set_bill_version(self, webflow_id: str, version_data: dict):
        """Store last-ingested version info for a bill.

        Args:
            webflow_id: Webflow item ID for the bill
            version_data: Dict with version_date, version_note, text_url, media_type, last_checked
        """
        if not self._client:
            return
        try:
            await self._client.set(
                f"{BILL_VERSION_PREFIX}{webflow_id}",
                json.dumps(version_data),
                ex=BILL_VERSION_TTL,
            )
        except Exception as e:
            logger.error("Redis: failed to set bill version", webflow_id=webflow_id, error=str(e))

    async def get_bill_version(self, webflow_id: str) -> dict | None:
        """Retrieve last-ingested version info for a bill.

        Args:
            webflow_id: Webflow item ID for the bill

        Returns:
            Dict with version info or None if not cached/unavailable
        """
        if not self._client:
            return None
        try:
            data = await self._client.get(f"{BILL_VERSION_PREFIX}{webflow_id}")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error("Redis: failed to get bill version", webflow_id=webflow_id, error=str(e))
        return None

    # -- Bill status cache (Flow 1: OpenStates → Webflow CMS) --

    async def set_bill_status(self, webflow_id: str, status_data: dict):
        """Cache the last-synced Webflow status fields for a bill.

        Args:
            webflow_id: Webflow item ID
            status_data: Dict with status, status_date, status_chamber, gov_url, last_synced
        """
        if not self._client:
            return
        try:
            await self._client.set(
                f"{BILL_STATUS_PREFIX}{webflow_id}",
                json.dumps(status_data),
                ex=BILL_STATUS_TTL,
            )
        except Exception as e:
            logger.error("Redis: failed to set bill status", webflow_id=webflow_id, error=str(e))

    async def get_bill_status(self, webflow_id: str) -> dict | None:
        """Get cached Webflow status fields for a bill.

        Args:
            webflow_id: Webflow item ID

        Returns:
            Dict with cached status fields or None
        """
        if not self._client:
            return None
        try:
            data = await self._client.get(f"{BILL_STATUS_PREFIX}{webflow_id}")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error("Redis: failed to get bill status", webflow_id=webflow_id, error=str(e))
        return None

    # -- Flow run status tracking --

    async def set_flow_status(self, flow_name: str, status_data: dict):
        """Record a flow run's outcome.

        Args:
            flow_name: Flow identifier (e.g. 'daily_bill_sync', 'webflow_status')
            status_data: Dict with started_at, completed_at, status, results, etc.
        """
        if not self._client:
            return
        try:
            await self._client.set(
                f"{FLOW_STATUS_PREFIX}{flow_name}",
                json.dumps(status_data),
                ex=FLOW_STATUS_TTL,
            )
        except Exception as e:
            logger.error("Redis: failed to set flow status", flow_name=flow_name, error=str(e))

    async def get_flow_status(self, flow_name: str) -> dict | None:
        """Get last run status for a flow.

        Args:
            flow_name: Flow identifier

        Returns:
            Dict with flow run status or None
        """
        if not self._client:
            return None
        try:
            data = await self._client.get(f"{FLOW_STATUS_PREFIX}{flow_name}")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error("Redis: failed to get flow status", flow_name=flow_name, error=str(e))
        return None

    # -- Flow run history tracking (rolling window, OPEN-22 AC0) --
    #
    # set_flow_status/get_flow_status above is a plain SET/GET that every weekly run
    # overwrites -- there is no history to check a sustained pattern against. Deliberately
    # additive rather than changing that primitive in place: it has other callers
    # (scheduler.py's daily_bill_sync/webflow_status, openstates_archive.py, votebot_eval.py,
    # health.py's status-page read) this module has no way to verify the full blast radius
    # of, so a capped rolling-history list lives under its own key instead.

    RUN_HISTORY_PREFIX = "ddp:flow_history:"
    RUN_HISTORY_TTL = 86400 * 30  # 30 days -- comfortable margin over the weekly cadence this
    # exists for (a window of the last 3-4 runs), so a slow/delayed run doesn't age out early.

    # OPEN-140: the runtime scrape cadence per jurisdiction. Redis is the source of truth and the
    # YAML value is a floor, so an absent or unreadable key is not a failure -- it means "use the
    # configured cadence", which is what resolve_cadence() does with None. Deliberately NO TTL:
    # an expiry would silently demote a jurisdiction back to its floor, which is precisely the
    # unattended demotion OPEN-140 refuses to do on purpose.
    CADENCE_PREFIX = "ddp:scrape_cadence:"

    async def get_scrape_cadence(self, jurisdiction: str) -> str | None:
        """Runtime cadence override for a jurisdiction, or None if there isn't one.

        None on any failure, deliberately: the caller floors it at the configured cadence, so a
        Redis outage degrades to sync_schedule.yaml rather than to an unscheduled jurisdiction.
        """
        if not self._client:
            return None
        try:
            v = await self._client.get(f"{self.CADENCE_PREFIX}{jurisdiction}")
            return v.decode() if isinstance(v, bytes) else v
        except Exception as e:
            logger.error(
                "Redis: failed to read scrape cadence", jurisdiction=jurisdiction, error=str(e)
            )
            return None

    async def set_scrape_cadence(self, jurisdiction: str, cadence: str) -> bool:
        """Record a runtime cadence override. Returns whether it was actually stored.

        The boolean matters: a caller that has just decided to escalate needs to know the
        decision did not persist, or it will re-decide and re-log it every review.

        Rejects a value the reader would not recognise. resolve_cadence() would floor junk at the
        configured cadence anyway, so this is not a safety fix -- it is so that a successful write
        means something, and so a typo surfaces at the write rather than as a jurisdiction quietly
        sitting at its floor with an override that looks present.
        """
        if cadence not in ("nightly", "weekly"):
            logger.error(
                "Redis: refusing to store an unrecognised scrape cadence",
                jurisdiction=jurisdiction,
                cadence=cadence,
            )
            return False
        if not self._client:
            return False
        try:
            await self._client.set(f"{self.CADENCE_PREFIX}{jurisdiction}", cadence)
            return True
        except Exception as e:
            logger.error(
                "Redis: failed to write scrape cadence",
                jurisdiction=jurisdiction,
                cadence=cadence,
                error=str(e),
            )
            return False

    async def append_run_history(
        self, flow_name: str, jurisdiction: str, record: dict, max_len: int = 20
    ) -> None:
        """Append one run's outcome to a capped rolling history (Redis List: RPUSH + LTRIM).

        Args:
            flow_name: Flow identifier (e.g. 'openstates_secondary_scrapes')
            jurisdiction: Jurisdiction code the record is for (e.g. 'mi')
            record: JSON-serializable outcome, e.g. {"timestamp": ..., "success": bool,
                "failure_reason": str | None}
            max_len: Cap on retained history length (oldest entries drop off first)
        """
        if not self._client:
            return
        try:
            key = f"{self.RUN_HISTORY_PREFIX}{flow_name}:{jurisdiction}"
            await self._client.rpush(key, json.dumps(record))
            await self._client.ltrim(key, -max_len, -1)
            await self._client.expire(key, self.RUN_HISTORY_TTL)
        except Exception as e:
            logger.error(
                "Redis: failed to append run history",
                flow_name=flow_name,
                jurisdiction=jurisdiction,
                error=str(e),
            )

    async def get_run_history(self, flow_name: str, jurisdiction: str) -> list[dict]:
        """Get the rolling history of past run outcomes for a jurisdiction, oldest first.

        Returns:
            List of previously-appended records (empty list if none/unavailable)
        """
        if not self._client:
            return []
        try:
            key = f"{self.RUN_HISTORY_PREFIX}{flow_name}:{jurisdiction}"
            raw = await self._client.lrange(key, 0, -1)
            return [json.loads(r) for r in raw]
        except Exception as e:
            logger.error(
                "Redis: failed to get run history",
                flow_name=flow_name,
                jurisdiction=jurisdiction,
                error=str(e),
            )
            return []


# Singleton
_redis_store: Optional[RedisStore] = None


def get_redis_store() -> RedisStore:
    """Get the singleton RedisStore instance."""
    global _redis_store
    if _redis_store is None:
        _redis_store = RedisStore()
    return _redis_store
