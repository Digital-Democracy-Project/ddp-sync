"""SYNC-90: staged, resumable backfill of the NEW `ddp-knowledge-base` Pinecone index.

PLAN-enterprise-search.md 5.6. The post-archive hook (`knowledge_base_embedding`, SYNC-83) only
embeds bills an archive run touched; this walks the whole archived corpus of one jurisdiction. It
does not embed anything itself: every write goes through `KnowledgeBaseEmbedder.embed_bill`, the
same code the live hook runs, narrowed by an `EmbedScope`, so live and backfill writes cannot
diverge and an already-embedded document is skipped by the same content-hash check.

Stages, in order (`STAGES`). "Current" is api-v3's own newest classifiable version of a bill and
"current session" is the jurisdiction's current session (`OpenStatesSource`), nothing re-derived:

1. `current`        current version text of every current-session bill
2. `votes`          the `bill-votes` document of every bill
3. `diffs`          the `bill-version-diff` documents of every bill
4. `prior-sessions` current version text of the prior-session bills
5. `history`        every older version's text (and stage-unknown versions) of every bill

Resumable and idempotent: one Redis checkpoint per jurisdiction and stage (last bill id, bills that
failed and are retried first, cumulative totals, `done`). A finished stage is a no-op unless
`restart`. The jurisdiction is walked sequentially, one bill at a time (SYNC-89 measured ~20
documents/minute that way with no OpenAI rate-limit responses), and pauses through a UTC blackout
window around the 05:00 archive start. One backfill per jurisdiction at a time, via a renewed Redis
lease (the SYNC-48/OPEN-292 helper). It may run alongside the live hook: `embed_bill` re-reads a
document's cache entry right before writing it and leaves it alone if another writer changed it.

What that guard does and does not promise: it narrows the window in which a backfill write can
land on a document a live write just changed to the gap between that re-read and the upsert (a
moment, not the seconds a whole bill takes). It is not atomic across Pinecone and Redis and does not
try to be. Any residual mismatch heals itself, because every later pass compares the cache hash with
api-v3's current text and re-embeds on a difference, and the blackout window keeps the backfill away
from the archive runs that trigger live writes. Other limits, on purpose: the blackout stops NEW bill
starts, not one already in flight; a bill that first appears below a resumed run's cursor is left
to the live hook (the backfill is the historical corpus, the hook handles new and changed bills);
`history` embeds every version but skips what stages 1 and 4 already wrote (same content hash), and
is correct on its own if they were never run.

Deliberately NOT here: DDP-curated-bills-first ordering (needs a ddp-broker-py read ddp-sync has no
client for), and BROKER-161's Pinecone presence check (a separate script, pointed at the new index).
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from ddp_sync.config import SyncSettings
from ddp_sync.ingestion.sources.openstates import OpenStatesSource
from ddp_sync.pipelines.knowledge_base_embedding import (
    EmbedScope,
    KnowledgeBaseEmbedder,
)
from ddp_sync.pipelines.scraper_triggered_legbot import _lock_heartbeat_loop
from ddp_sync.services import local_openstates_client
from ddp_sync.services.redis_store import get_redis_store

logger = structlog.get_logger()

STAGES = ("current", "votes", "diffs", "prior-sessions", "history")
STAGE_SCOPES: dict[str, EmbedScope] = {
    "current": EmbedScope(text="current", diffs=False, votes=False),
    "votes": EmbedScope(text=None, diffs=False, votes=True),
    "diffs": EmbedScope(text=None, diffs=True, votes=False),
    "prior-sessions": EmbedScope(text="current", diffs=False, votes=False),
    "history": EmbedScope(text="all", diffs=False, votes=False),
}

CHECKPOINT_EVERY = 50  # bills between checkpoint saves and progress log lines
MAX_FAILED_BILLS = 200  # a stage that fails this many bills is broken, not unlucky: stop and say so
LOCK_PREFIX = "ddp_sync:kb_backfill:lock:"
LOCK_TTL_SECONDS = 600
LOCK_RENEWAL_SECONDS = 120
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)  # `document_updated_since` = lists every bill with an archived document
DEFAULT_BLACKOUT_UTC = ("04:45", "07:00")  # the 05:00 UTC archive window


_sleep = asyncio.sleep  # module-level so tests can pause the blackout without touching other sleeps


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def lock_key(jurisdiction: str) -> str:
    return f"{LOCK_PREFIX}{jurisdiction.lower()}"


async def lock_holder(jurisdiction: str) -> str | None:
    """The run id holding this jurisdiction's backfill lock, or None (also None if Redis is down)."""
    client = get_redis_store()._client
    if client is None:
        return None
    try:
        value = await client.get(lock_key(jurisdiction))
    except Exception:  # noqa: BLE001
        return None
    return value.decode() if isinstance(value, bytes) else value


def _blackout_seconds_left(now: datetime, config: dict | None) -> float:
    """Seconds until the blackout window ends if `now` is inside it, else 0. The window is
    `knowledge_base_embedding.backfill.blackout_start_utc` / `blackout_end_utc` ("HH:MM", UTC)."""
    block = ((config or {}).get("knowledge_base_embedding") or {}).get("backfill") or {}
    start_s = block.get("blackout_start_utc", DEFAULT_BLACKOUT_UTC[0])
    end_s = block.get("blackout_end_utc", DEFAULT_BLACKOUT_UTC[1])
    sh, sm = (int(x) for x in str(start_s).split(":"))
    eh, em = (int(x) for x in str(end_s).split(":"))
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start, end = day + timedelta(hours=sh, minutes=sm), day + timedelta(hours=eh, minutes=em)
    if end <= start:  # window crosses midnight
        if now >= start:
            end += timedelta(days=1)
        else:
            start -= timedelta(days=1)
    return (end - now).total_seconds() if start <= now < end else 0.0


async def _select_bills(
    jurisdiction: str, stage: str, api_base: str, api_key: str, run_id: str
) -> list[str] | None:
    """Sorted bare ocd bill ids this stage covers, or None when they cannot be listed completely
    (a truncated listing would silently skip bills, and for the session split would misfile them)."""
    async def listing(session: str | None = None) -> set[str] | None:
        result = await local_openstates_client.list_touched_bill_ids(
            jurisdiction, since=_EPOCH, api_base=api_base, api_key=api_key, session=session
        )
        if result is None or not result[1]:
            return None
        return set(result[0])

    if stage in ("current", "prior-sessions"):
        session = await OpenStatesSource().get_current_session_identifier(jurisdiction)
        if not session:
            logger.error("knowledge_base_backfill_failed", run_id=run_id, jurisdiction=jurisdiction,
                         stage=stage, error="current session could not be determined")
            return None
        current = await listing(session)
        if current is None:
            return None
        if stage == "current":
            return sorted(current)
        everything = await listing()
        return None if everything is None else sorted(everything - current)
    everything = await listing()
    return None if everything is None else sorted(everything)


async def _wait_out_blackout(config: dict | None, run_id: str) -> None:
    seconds = _blackout_seconds_left(_utcnow(), config)
    if seconds > 0:
        logger.info("knowledge_base_backfill_blackout_wait", run_id=run_id, seconds=int(seconds))
        await _sleep(seconds)


async def backfill_stage(
    jurisdiction: str,
    stage: str,
    *,
    embedder: KnowledgeBaseEmbedder | None,
    api_base: str,
    api_key: str,
    config: dict | None,
    run_id: str,
    dry_run: bool = True,
    restart: bool = False,
) -> dict[str, Any]:
    """Run (or, with `dry_run`, only count) one stage. Never raises for a per-bill problem."""
    redis = get_redis_store()
    scope = STAGE_SCOPES[stage]
    if restart and not dry_run:
        await redis.delete_kb_backfill_checkpoint(jurisdiction, stage)
    checkpoint = None if restart else await redis.get_kb_backfill_checkpoint(jurisdiction, stage)
    if checkpoint and checkpoint.get("done"):
        return {"stage": stage, "status": "already_complete", "totals": checkpoint.get("totals", {})}

    ids = await _select_bills(jurisdiction, stage, api_base, api_key, run_id)
    if ids is None:
        logger.error("knowledge_base_backfill_failed", run_id=run_id, jurisdiction=jurisdiction,
                     stage=stage, error="bills could not be listed completely")
        return {"stage": stage, "status": "error", "error": "listing_failed"}

    checkpoint = checkpoint or {}
    last = checkpoint.get("last_bill_id") or ""
    retry_first = list(checkpoint.get("failed_ids") or [])
    retry_set = set(retry_first)
    todo = retry_first + [i for i in ids if i > last and i not in retry_set]
    if dry_run:
        return {"stage": stage, "status": "dry_run", "bills_in_stage": len(ids), "bills_remaining": len(todo)}

    totals: dict[str, Any] = {k: 0 for k in ("bills", "documents", "diffs", "votes", "chunks", "chars",
                                             "raced", "failed_bills")}
    totals.update(checkpoint.get("totals") or {})
    failed_ids: list[str] = []  # bills that failed during THIS run
    retry_left = list(retry_first)  # carried-over failures not yet retried this run

    async def save(done: bool) -> None:
        # Every save keeps the carried-over failures that have not been retried yet: they sit below
        # `last`, so dropping them on an interrupted run would lose them for good.
        await redis.set_kb_backfill_checkpoint(jurisdiction, stage, {
            "last_bill_id": last, "failed_ids": failed_ids + retry_left, "done": done, "totals": totals,
            "updated_at": _utcnow().isoformat(),
        })

    for count, ocd_bill_id in enumerate(todo, start=1):
        await _wait_out_blackout(config, run_id)
        stats = None
        try:
            bill = await local_openstates_client.fetch_bill_for_embedding(
                ocd_bill_id, api_base=api_base, api_key=api_key
            )
            if bill is None:
                raise RuntimeError("bill detail could not be read from api-v3")
            stats = await embedder.embed_bill(ocd_bill_id, jurisdiction, bill, scope)
        except Exception as e:  # noqa: BLE001 -- one bad bill must not stop the stage
            logger.warning("knowledge_base_backfill_bill_undone", run_id=run_id, jurisdiction=jurisdiction,
                           stage=stage, ocd_bill_id=ocd_bill_id, error=str(e))
        totals["bills"] += 1
        if ocd_bill_id in retry_left:
            retry_left.remove(ocd_bill_id)  # retried now; it re-enters failed_ids below if it fails again
        if stats is not None:
            for k in ("documents", "diffs", "votes", "chunks", "chars", "raced"):
                totals[k] += stats[k]
        if stats is None or stats["undone"]:
            failed_ids.append(ocd_bill_id)
            totals["failed_bills"] += 1
        last = max(last, ocd_bill_id)

        if len(failed_ids) > MAX_FAILED_BILLS:
            await save(False)
            logger.error("knowledge_base_backfill_aborted", run_id=run_id, jurisdiction=jurisdiction,
                         stage=stage, reason=f"more than {MAX_FAILED_BILLS} bills failed", **totals)
            return {"stage": stage, "status": "aborted", "totals": totals}
        if count % CHECKPOINT_EVERY == 0:
            await save(False)
            logger.info("knowledge_base_backfill_progress", run_id=run_id, jurisdiction=jurisdiction,
                        stage=stage, remaining=len(todo) - count,
                        approx_tokens=totals["chars"] // 4, **totals)

    done = not failed_ids and not retry_left
    await save(done)
    log = logger.info if done else logger.warning
    log("knowledge_base_backfill_stage_complete" if done else "knowledge_base_backfill_stage_incomplete",
        run_id=run_id, jurisdiction=jurisdiction, stage=stage, failed=len(failed_ids),
        approx_tokens=totals["chars"] // 4, **totals)
    return {"stage": stage, "status": "complete" if done else "incomplete", "totals": totals}


async def run_knowledge_base_backfill(
    jurisdiction: str,
    stages: list[str] | None = None,
    *,
    settings: SyncSettings,
    api_base: str,
    api_key: str = "",
    config: dict | None = None,
    dry_run: bool = True,
    restart: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Backfill `stages` (default: all, in order) for one jurisdiction. A dry run only counts, takes
    no lock and writes nothing. Otherwise holds the per-jurisdiction lease for the whole run."""
    run_id = run_id or f"{jurisdiction}-kb-backfill-{uuid.uuid4().hex[:12]}"
    stages = list(stages or STAGES)
    logger.info("knowledge_base_backfill_start", run_id=run_id, jurisdiction=jurisdiction,
                stages=stages, dry_run=dry_run, restart=restart)

    if dry_run:
        results = [await backfill_stage(jurisdiction, st, embedder=None, api_base=api_base,
                                        api_key=api_key, config=config, run_id=run_id,
                                        dry_run=True, restart=restart) for st in stages]
        logger.info("knowledge_base_backfill_dry_run", run_id=run_id, jurisdiction=jurisdiction,
                    stages=results)
        return {"status": "dry_run", "run_id": run_id, "stages": results}

    try:  # fail fast: a malformed window would otherwise crash the first bill, hours into the run
        _blackout_seconds_left(_utcnow(), config)
    except (ValueError, TypeError) as e:
        logger.error("knowledge_base_backfill_failed", run_id=run_id, jurisdiction=jurisdiction,
                     error=f"malformed blackout window in knowledge_base_embedding.backfill: {e}")
        return {"status": "error", "error": "bad_blackout_config", "run_id": run_id}
    redis = get_redis_store()
    if not redis.is_available:
        logger.error("knowledge_base_backfill_failed", run_id=run_id, jurisdiction=jurisdiction,
                     error="Redis unavailable; a backfill needs its checkpoint and lock")
        return {"status": "error", "error": "redis_unavailable", "run_id": run_id}
    key = lock_key(jurisdiction)
    if not await redis._client.set(key, run_id, nx=True, ex=LOCK_TTL_SECONDS):
        holder = await lock_holder(jurisdiction)
        logger.warning("knowledge_base_backfill_overlap_rejected", run_id=run_id,
                       jurisdiction=jurisdiction, current_run_id=holder)
        return {"status": "already_running", "current_run_id": holder, "run_id": run_id}

    heartbeat = asyncio.create_task(
        _lock_heartbeat_loop(redis, key, run_id, LOCK_TTL_SECONDS, LOCK_RENEWAL_SECONDS)
    )
    results: list[dict[str, Any]] = []
    try:
        embedder = KnowledgeBaseEmbedder(settings)
        for st in stages:
            result = await backfill_stage(jurisdiction, st, embedder=embedder, api_base=api_base,
                                          api_key=api_key, config=config, run_id=run_id,
                                          dry_run=False, restart=restart)
            results.append(result)
            if result["status"] in ("error", "aborted"):
                break  # later stages would hit the same problem; the checkpoint resumes this one
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
        try:  # fenced release: only delete a lock we still own
            current = await redis._client.get(key)
            if (current.decode() if isinstance(current, bytes) else current) == run_id:
                await redis._client.delete(key)
        except Exception as e:  # noqa: BLE001
            logger.warning("knowledge_base_backfill_lock_release_failed", run_id=run_id, error=str(e))

    ok = len(results) == len(stages) and all(r["status"] in ("complete", "already_complete") for r in results)
    (logger.info if ok else logger.warning)(
        "knowledge_base_backfill_run", run_id=run_id, jurisdiction=jurisdiction,
        complete=ok, stages=[{"stage": r["stage"], "status": r["status"]} for r in results],
    )
    return {"status": "complete" if ok else "incomplete", "run_id": run_id, "stages": results}
