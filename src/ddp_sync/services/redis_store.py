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


# Singleton
_redis_store: Optional[RedisStore] = None


def get_redis_store() -> RedisStore:
    """Get the singleton RedisStore instance."""
    global _redis_store
    if _redis_store is None:
        _redis_store = RedisStore()
    return _redis_store
