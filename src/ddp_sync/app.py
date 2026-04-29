"""DDP-Sync FastAPI application."""

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from ddp_sync.config import get_settings

logger = logging.getLogger(__name__)

API_PREFIX = "/ddp-sync/v1"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: connect Redis, start scheduler. Shutdown: stop scheduler, disconnect."""
    settings = get_settings()

    # Set up logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Connect Redis
    from ddp_sync.services.redis_store import get_redis_store
    redis_store = get_redis_store()
    await redis_store.connect()

    # Start scheduler (unconditionally — single worker, no leader election)
    from ddp_sync.scheduler import UpdateScheduler, UpdateSchedulerFactory
    scheduler = UpdateSchedulerFactory.get_instance(settings)
    scheduler.start()
    logger.info(
        "Scheduler started with %d jobs",
        len(scheduler.scheduler.get_jobs()),
    )

    # Pre-warm the congress-legislators YAML cache so the first
    # /trigger/legislator-bio-sync request doesn't pay a cold-start parse
    # penalty (~55s for the 8.6 MB historical file). Fire-and-forget; the
    # bio-sync orchestrator awaits warm_cache() defensively, which is
    # idempotent and will be done by the time the first trigger fires.
    # See PLAN-legislator-bio-sync.md round-6 fixes.
    from ddp_sync.services.congress_legislators import (
        CongressLegislatorsSource,
    )
    bio_sync_source = CongressLegislatorsSource()
    app.state.congress_legislators = bio_sync_source
    bio_warm_task = asyncio.create_task(
        _prewarm_congress_legislators(bio_sync_source)
    )

    # Start zombie watchdog (simplified — no leader gating)
    watchdog_task = asyncio.create_task(_zombie_sync_watchdog(redis_store))

    yield

    # Shutdown
    watchdog_task.cancel()
    bio_warm_task.cancel()
    for t in (watchdog_task, bio_warm_task):
        try:
            await t
        except asyncio.CancelledError:
            pass
    scheduler.stop()
    await redis_store.disconnect()
    logger.info("DDP-Sync shutdown complete")


async def _prewarm_congress_legislators(source) -> None:
    """Background task to warm the congress-legislators YAML cache at startup.

    Errors are logged but never raised — startup must succeed even if the
    unitedstates.github.io endpoint is transiently down. The bio-sync
    orchestrator will retry the warm on its next call.
    """
    try:
        await source.warm_cache()
        logger.info("Congress-legislators cache pre-warm complete")
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Congress-legislators pre-warm failed; bio-sync will retry on demand",
            extra={"error": str(e)},
        )


async def _zombie_sync_watchdog(redis_store):
    """Poll for stale sync tasks every 30 minutes."""
    while True:
        try:
            await asyncio.sleep(1800)
            await _check_and_resume_stale_syncs(redis_store)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Watchdog error: {e}")


async def _check_and_resume_stale_syncs(
    redis_store, stale_threshold=300, max_retries=3
):
    """Detect zombie tasks and auto-resume.

    Checks all running tasks. If a task's last_heartbeat is older than
    stale_threshold seconds, it's considered stale (likely killed by OOM
    or crash). Auto-resumes with checkpoint copy up to max_retries times.
    """
    from ddp_sync.api.routes.sync_unified import (
        _background_tasks,
        _run_batch_sync_background,
    )
    from ddp_sync.sync import ContentType, SyncOptions

    try:
        # Scan for running tasks
        if not redis_store._client:
            return

        task_keys = await redis_store._client.keys("ddp:sync:task:*")

        for key in task_keys:
            task_id_str = key.decode().split(":")[-1]
            task_data = await redis_store.get_sync_task(task_id_str)
            if not task_data or task_data.get("status") != "running":
                continue

            # Check heartbeat freshness
            last_heartbeat = task_data.get("last_heartbeat")
            if not last_heartbeat:
                continue

            heartbeat_time = datetime.fromisoformat(last_heartbeat)
            age = (datetime.now(timezone.utc) - heartbeat_time).total_seconds()

            if age < stale_threshold:
                continue  # Still alive

            task_id = task_data["task_id"]
            retry_count = task_data.get("retry_count", 0)

            if retry_count >= max_retries:
                logger.warning(
                    f"Task {task_id} exceeded max retries ({max_retries}), "
                    "marking permanently_failed"
                )
                await redis_store.set_sync_task(
                    task_id,
                    {
                        **task_data,
                        "status": "permanently_failed",
                        "error": f"Exceeded max retries after {retry_count} attempts",
                    },
                )
                continue

            # Auto-resume
            logger.warning(
                f"Stale task detected: {task_id} "
                f"(heartbeat {age:.0f}s ago, retry {retry_count + 1})"
            )

            # Mark old task as failed
            await redis_store.set_sync_task(
                task_id,
                {
                    **task_data,
                    "status": "failed",
                    "error": f"Stale heartbeat ({age:.0f}s), auto-resuming",
                },
            )

            # Create new task with checkpoint copy
            new_task_id = str(uuid.uuid4())
            saved_options = task_data.get("options", {})

            # Copy checkpoints from old task
            await redis_store.copy_sync_checkpoints(task_id, new_task_id)

            # Reconstruct content type
            content_type = ContentType(
                saved_options.get("content_type", "bill")
            )

            options = SyncOptions(
                task_id=new_task_id,
                resume_task_id=task_id,
            )

            # Register and start new background task
            new_task_data = {
                "task_id": new_task_id,
                "status": "running",
                "content_type": content_type.value,
                "retry_count": retry_count + 1,
                "resumed_from": task_id,
                "options": saved_options,
            }
            await redis_store.set_sync_task(new_task_id, new_task_data)

            task = asyncio.create_task(
                _run_batch_sync_background(new_task_id, content_type, options)
            )
            _background_tasks[new_task_id] = task

            logger.info(f"Auto-resumed stale task {task_id} -> {new_task_id}")

    except Exception as e:
        logger.error(f"Error checking stale syncs: {e}")


def create_app() -> FastAPI:
    app = FastAPI(
        title="DDP-Sync",
        description="Unified data pipeline service",
        version="0.1.0",
        lifespan=lifespan,
    )

    from ddp_sync.api.routes.health import router as health_router
    from ddp_sync.api.routes.sync_unified import router as sync_router
    from ddp_sync.api.routes.triggers import router as trigger_router

    app.include_router(health_router, prefix=API_PREFIX)
    app.include_router(sync_router, prefix=API_PREFIX)
    app.include_router(trigger_router, prefix=API_PREFIX)

    return app


app = create_app()
