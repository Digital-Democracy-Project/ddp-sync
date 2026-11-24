"""SYNC-191: remove the knowledge-base vectors of drafts a bill has since replaced (OPEN-861, OPEN-870).

Ramon (2026-11-24): a draft is covered while it is a draft and left out once a bill replaces it (Montana's pre-filing
"LC nnnn" records). SYNC-190 stops embedding one; this clears what was embedded before. SYNC-95's orphan delete cannot: it
only fires for a document a still-listed bill no longer lists, and an introduced draft is no longer listed at all.

Which bills are introduced drafts is api-v3's answer, not a rule here: they are the bills on the ledger without
`exclude_introduced_drafts` that are missing from the ledger with it (both lists must be complete). What is removed is
exactly what the embedder recorded for them (`ddp:bill_version:<id>`: text and diff vectors by exact id, the embedder's own
`_delete_document_vectors`); then the record is kept but emptied, so the bill reads as "recorded, nothing embedded".
Bill data in api-v3 and the broker (LegBot's `BillArtifact` rows included) is never touched. Dry run by default.

Limits, on purpose (pm-review): the two ledger reads are not one snapshot and the record re-read narrows, not closes, the race
with a live writer (the same as SYNC-95's orphan delete). Both are self-healing: a bill wrongly emptied shows its documents as
`missing` on the next ledger pass and is embedded again, so the worst case is a re-embed, never lost bill data. Read the dry run
before applying; an introduced draft is no longer read by the embedder (SYNC-190), so nothing normally writes to it.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog

from ddp_sync.config import SyncSettings, get_settings
from ddp_sync.pipelines.knowledge_base_embedding import (
    CACHE_SCHEMA,
    MAX_ORPHAN_DOCUMENTS_PER_RUN,
    KnowledgeBaseEmbedder,
    diff_document_key,
    text_document_key,
)
from ddp_sync.services import local_openstates_client
from ddp_sync.services.redis_store import get_redis_store

logger = structlog.get_logger()


async def remove_introduced_draft_vectors(
    jurisdiction: str, *, api_base: str, api_key: str = "", settings: SyncSettings | None = None,
    apply: bool = False, run_id: str | None = None, embedder: KnowledgeBaseEmbedder | None = None, redis=None,
) -> dict:
    """Find the jurisdiction's introduced drafts and, with `apply`, delete their vectors and empty their records.

    Returns (and logs as `knowledge_base_draft_cleanup`) `status`: `done`; `error` (`ledger_unavailable`,
    `ledger_incomplete`, `redis_unavailable`, `records_unreadable`); `refused` (more documents than
    `MAX_ORPHAN_DOCUMENTS_PER_RUN`: something systematic, nothing is deleted); or `partial` (a delete or a record write
    failed; the record stays, so a re-run retries). `drafts` is every introduced draft api-v3 names, `with_vectors` those
    with a record that lists documents, `documents` and `vectors` what those hold, `removed_documents` and
    `removed_vectors` what an `apply` run deleted, and `bills` each draft's id and counts."""
    result: dict[str, Any] = {"jurisdiction": jurisdiction, "status": "done", "dry_run": not apply}
    if run_id:
        result["run_id"] = run_id
    redis = redis or get_redis_store()
    if not redis.is_available:
        result.update(status="error", error="redis_unavailable")
        return _done(result)
    everything = await local_openstates_client.list_embedding_ledger(
        jurisdiction, api_base=api_base, api_key=api_key, exclude_introduced_drafts=False)
    kept = await local_openstates_client.list_embedding_ledger(
        jurisdiction, api_base=api_base, api_key=api_key, exclude_introduced_drafts=True)
    if everything is None or kept is None:
        result.update(status="error", error="ledger_unavailable")
        return _done(result)
    if not everything[1] or not kept[1]:  # a partial list proves nothing about what is missing from it
        result.update(status="error", error="ledger_incomplete")
        return _done(result)
    draft_ids = sorted(set(everything[0]) - set(kept[0]))
    records = await redis.get_bill_versions(draft_ids) if draft_ids else {}
    if records is None:
        result.update(status="error", error="records_unreadable")
        return _done(result)

    bills = []
    for ocd_bill_id in draft_ids:
        record = records.get(ocd_bill_id)
        docs = record.get("documents") or {} if record and record.get("schema") == CACHE_SCHEMA else {}
        bills.append({
            "ocd_bill_id": ocd_bill_id, "documents": len(docs),
            "vectors": sum(d.get("chunks", 0) + d.get("diff_chunks", 0) for d in docs.values()),
        })
    with_vectors = [b for b in bills if b["documents"]]
    result.update(
        drafts=len(draft_ids), with_vectors=len(with_vectors), bills=bills,
        documents=sum(b["documents"] for b in bills), vectors=sum(b["vectors"] for b in bills),
        removed_documents=0, removed_vectors=0,
    )
    if result["documents"] > MAX_ORPHAN_DOCUMENTS_PER_RUN:
        result.update(status="refused", error="more_documents_than_the_orphan_cap")
        return _done(result)
    if not apply or not with_vectors:
        return _done(result)

    embedder = embedder or KnowledgeBaseEmbedder(settings or get_settings(), redis_store=redis)
    for bill in with_vectors:
        ocd_bill_id = bill["ocd_bill_id"]
        recorded = records[ocd_bill_id]
        fresh = await redis.get_bill_version(ocd_bill_id)
        if fresh != recorded:  # another writer changed it since it was read: leave it to the next run
            result["status"] = "partial"
            bill["error"] = "record_changed"
            continue
        try:
            for aid, doc in recorded["documents"].items():
                await embedder._delete_document_vectors(text_document_key(ocd_bill_id, aid), doc.get("chunks", 0))
                await embedder._delete_document_vectors(diff_document_key(ocd_bill_id, aid), doc.get("diff_chunks", 0))
        except Exception as e:  # noqa: BLE001 -- the record stays whole, so a re-run retries every document
            result["status"] = "partial"
            bill["error"] = f"delete_failed: {e}"
            continue
        written = await redis.set_bill_version(ocd_bill_id, {
            "schema": CACHE_SCHEMA, "documents": {}, "last_checked": datetime.now(timezone.utc).isoformat(),
        }, persistent=True)
        if not written:
            result["status"] = "partial"
            bill["error"] = "record_not_written"
            continue
        result["removed_documents"] += bill["documents"]
        result["removed_vectors"] += bill["vectors"]
    return _done(result)


def _done(result: dict) -> dict:
    (logger.info if result["status"] == "done" else logger.warning)("knowledge_base_draft_cleanup", **result)
    return result
