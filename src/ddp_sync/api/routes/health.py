"""Health check and schedule endpoints."""

import logging

from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse

from ddp_sync.api.auth import api_key_auth
from ddp_sync.config import get_config_source, get_settings

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
        if store._client:
            await store._client.ping()
            result["redis"] = "connected"
        else:
            result["redis"] = "not_connected"
    except Exception as e:
        result["redis"] = f"error: {e}"

    # Flow status (last run info for each data flow)
    try:
        from ddp_sync.services.redis_store import get_redis_store as _get_redis
        _store = _get_redis()
        flow_statuses = {}
        for flow_name in ("daily_bill_sync", "webflow_status", "pinecone_ingestion", "openstates_people_pull", "fork_upstream_check"):
            flow_data = await _store.get_flow_status(flow_name)
            if flow_data:
                flow_statuses[flow_name] = {
                    "status": flow_data.get("status"),
                    "completed_at": flow_data.get("completed_at"),
                    "duration_seconds": flow_data.get("duration_seconds"),
                }
        if flow_statuses:
            result["flows"] = flow_statuses
    except Exception:
        pass

    # Pinecone
    try:
        if settings.pinecone_api_key:
            from ddp_sync.services.vector_store import VectorStoreService
            vs = VectorStoreService(settings)
            stats = vs.index.describe_index_stats()
            result["pinecone"] = f"connected ({stats.total_vector_count} vectors)"
        else:
            result["pinecone"] = "not_configured"
    except Exception as e:
        result["pinecone"] = f"error: {e}"

    return result


@router.get("/schedule")
async def schedule():
    """Show all scheduled jobs and their next run times, next run first. OPEN-140: with the effective scrape cadence per
    jurisdiction (Redis is the runtime source of truth and the YAML value only a floor, so without this it would take a
    redis-cli to answer "is Florida nightly right now?"). The same listing `ddp-sync status` shows (SYNC-178)."""
    try:
        from ddp_sync.status_report import scheduled_jobs

        return scheduled_jobs()
    except Exception as e:
        return {"status": f"error: {e}", "jobs": []}


@router.get("/status")
async def status(format: str = "json", token: str = Depends(api_key_auth)):
    """SYNC-178: everything this ddp-sync is running, holding and has scheduled (`ddp-sync status` prints it):
    runs in flight with their progress, locks with their expiry, scheduled jobs (next first), recently finished runs
    (including any a restart stopped), and whether a restart now would interrupt something. Read only; behind the same
    API key as the trigger routes. `?format=text` answers in the plain text the command prints."""
    from ddp_sync.status_report import collect, format_text

    data = await collect()
    if format == "text":
        return PlainTextResponse(format_text(data) + "\n")
    return data
