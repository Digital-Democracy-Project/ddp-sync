"""Health check and schedule endpoints."""

import logging
from fastapi import APIRouter

from ddp_sync.config import get_settings, get_config_source

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/health")
async def health():
    """Health check with dependency status."""
    settings = get_settings()
    result = {
        "status": "healthy",
        "service": "ddp-sync",
        "version": "0.1.0",
        "config_source": get_config_source(),
        "scheduler": {"running": False, "jobs": 0},
        "redis": "unknown",
        "pinecone": "unknown",
    }

    # Scheduler status
    try:
        from ddp_sync.scheduler import get_scheduler
        scheduler = get_scheduler()
        if scheduler and scheduler.scheduler.running:
            jobs = scheduler.scheduler.get_jobs()
            next_run = min(
                (j.next_run_time for j in jobs if j.next_run_time),
                default=None,
            )
            result["scheduler"] = {
                "running": True,
                "jobs": len(jobs),
                "next_run": str(next_run) if next_run else None,
            }
    except Exception:
        pass

    # Redis
    try:
        from ddp_sync.services.redis_store import get_redis_store
        store = get_redis_store()
        if store._redis:
            await store._redis.ping()
            result["redis"] = "connected"
        else:
            result["redis"] = "not_connected"
    except Exception as e:
        result["redis"] = f"error: {e}"

    # Pinecone
    try:
        from ddp_sync.services.vector_store import VectorStoreService
        vs = VectorStoreService(settings)
        stats = vs.index.describe_index_stats()
        result["pinecone"] = f"connected ({stats.total_vector_count} vectors)"
    except Exception as e:
        result["pinecone"] = f"error: {e}"

    return result


@router.get("/schedule")
async def schedule():
    """Show all scheduled jobs and their next run times."""
    try:
        from ddp_sync.scheduler import get_scheduler
        scheduler = get_scheduler()
        if not scheduler or not scheduler.scheduler.running:
            return {"status": "scheduler_not_running", "jobs": []}

        jobs = []
        for job in scheduler.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else None,
                "trigger": str(job.trigger),
            })
        return {"status": "running", "jobs": jobs}
    except Exception as e:
        return {"status": f"error: {e}", "jobs": []}
