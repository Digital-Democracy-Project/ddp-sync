"""SYNC-83: embed archived bill text and version diffs into the NEW Pinecone index.

PLAN-enterprise-search.md 5.6. One more independent post-archive hook (see
`openstates_archive._maybe_embed_knowledge_base`), same shape as the LegBot hook: after a
jurisdiction's archive succeeds, embed what changed. Everything goes through
`IngestionPipeline.ingest_document` with settings whose index is `knowledge_base_index_name`
(`config.knowledge_base_settings`), so this path can never write `votebot-large`.

What is written, per bill (canonical ids, PLAN-local-openstates-migration.md 2.1):

* `bill-text:{ocd_bill_id}:{document_id}` -- one document per archived version. `document_id` is
  api-v3's `archived_document_id` (`ddp_bill_version_document.id`). Its metadata key
  `document_id` carries that same id (as a string) and overrides the pipeline key, because
  retrieval filters on it; chunk ids (`<document key>-chunk-<n>`) still identify the vectors.
* `bill-version-diff:{ocd_bill_id}:{document_id}` -- api-v3's stored `diff_from_previous_version`,
  verbatim, labelled with the from/to versions. Never generated here.

Votes are deliberately NOT embedded (SYNC-94): they are structured data that changes regularly,
so Votebot reads them from the vote records instead. The legacy `votebot-large` path still writes
its own `bill-votes-{webflow_id}` documents and is untouched.

Version order, stage and ordinal come from api-v3 (`version_stage`, `version_ordinal`, array
order); nothing here classifies or sorts. No LegBot output is embedded.

Redis cache `ddp:bill_version:{ocd_bill_id}` (`schema` 2): the per-document text/diff hashes and
chunk counts. It is written only after every Pinecone write for the bill has succeeded, so a failed
bill is retried by the next run (the per-jurisdiction watermark is not advanced either), and a
WARNING names what was left undone. `schema` stays 2 after SYNC-94 on purpose: bumping it would
discard every existing entry and re-embed documents already paid for. An older entry may still
carry a `votes` field; it is ignored and dropped the next time the entry is written.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timezone
from typing import Any

import structlog

from ddp_sync.config import SyncSettings, knowledge_base_settings
from ddp_sync.ingestion.metadata import DocumentMetadata
from ddp_sync.ingestion.pipeline import IngestionPipeline
from ddp_sync.pipelines.bill_version import delete_surplus_chunks
from ddp_sync.services import local_openstates_client
from ddp_sync.services.redis_store import get_redis_store

logger = structlog.get_logger()

CACHE_SCHEMA = 2
DOCUMENT_TYPE_TEXT = "bill-text"
DOCUMENT_TYPE_DIFF = "bill-version-diff"
STAGE_UNKNOWN = "unknown"  # api-v3's label for STAGE_UNKNOWN
_SOURCE = "OpenStates archive"


def text_document_key(ocd_bill_id: str, archive_id: Any) -> str:
    return f"{DOCUMENT_TYPE_TEXT}:{ocd_bill_id}:{archive_id}"


def diff_document_key(ocd_bill_id: str, archive_id: Any) -> str:
    return f"{DOCUMENT_TYPE_DIFF}:{ocd_bill_id}:{archive_id}"


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def version_text(version: dict) -> str:
    """The archived text api-v3 attached to a version: the link-level raw_text for classifiable
    versions, `archived_raw_text` for stage-unknown ones. '' when nothing is archived yet."""
    for link in version.get("links") or []:
        if link.get("raw_text"):
            return link["raw_text"]
    return version.get("archived_raw_text") or ""


@dataclass(frozen=True)
class EmbedScope:
    """Which parts of a bill `embed_bill` writes. The default is everything, which is what the
    live post-archive hook uses; the SYNC-90 backfill narrows it stage by stage so that live and
    backfill writes go through the SAME code and cannot diverge.

    text: "all" (every version), "current" (only the bill's current version, i.e. the highest
    `version_ordinal` among classifiable versions; stage-unknown versions are never current), or
    None (no text documents). diffs: whether to write the version-diff documents. **Off by default
    (Ramon, 2026-10-05):** version diffs are not embedded, because a vector of a raw unified diff only captures
    the topics of the changed lines; a "what changed" question is answered from the two versions' text or the
    stored diff read live. The code path stays so a stage can still ask for it deliberately; nothing does."""

    text: str | None = "all"
    diffs: bool = False


SCOPE_ALL = EmbedScope()

_UNREADABLE = object()  # a cache entry that existed when embed_bill started can no longer be read


def read_target(settings: SyncSettings, *, mac_capable: bool) -> tuple[str, str]:
    """`(api_base, api_key)` of the api-v3 the embedding reads: the Mac's local instance on the Mac,
    the RDS-backed one elsewhere (same split as the LegBot hook). Shared by the live hook and the
    SYNC-90 backfill so both read the same data."""
    if mac_capable:
        return settings.local_openstates_api_base, settings.local_openstates_api_key
    return settings.rds_openstates_api_base, settings.rds_openstates_api_key


class KnowledgeBaseEmbedder:
    """Embeds one bill at a time into the knowledge-base index. Construct once per run."""

    def __init__(
        self,
        settings: SyncSettings,
        *,
        pipeline: IngestionPipeline | None = None,
        redis_store=None,
    ):
        self.settings = knowledge_base_settings(settings)  # raises if unset / equals legacy index
        self.pipeline = pipeline or IngestionPipeline(self.settings)
        self.redis = redis_store or get_redis_store()

    async def _delete_document_vectors(self, key: str, chunks: int) -> int:
        """Delete the `chunks` vectors this path recorded for one document, by exact id
        (`<key>-chunk-<n>`); never a metadata scan or a prefix, so only vectors we wrote are touched.
        Not `delete_surplus_chunks`, whose guard refuses a delete down to zero chunks. Raises on a
        Pinecone failure; the caller leaves the record in place so the next run tries again."""
        if not chunks:
            return 0
        from ddp_sync.services.vector_store import VectorStoreService

        await VectorStoreService(self.settings).delete(ids=[f"{key}-chunk-{i}" for i in range(chunks)])
        return chunks

    async def _ingest(self, key: str, content: str, metadata: DocumentMetadata, old_chunks: int) -> int:
        """Ingest one document, then drop chunks a shrunken document no longer has. Returns the
        chunk count; raises RuntimeError on any failure so the caller leaves the cache alone."""
        result = await self.pipeline.ingest_document(content, metadata, skip_duplicates=False)
        if result.errors:
            raise RuntimeError("; ".join(result.errors))
        if result.chunks_upserted < result.chunks_created:
            raise RuntimeError(f"upserted {result.chunks_upserted} of {result.chunks_created} chunks")
        await delete_surplus_chunks(self.settings, key, old_chunks, result.chunks_created)
        return result.chunks_created

    @staticmethod
    def _base_extra(bill: dict, ocd_bill_id: str) -> dict[str, Any]:
        sources = bill.get("sources") or []
        return {
            "ocd_bill_id": ocd_bill_id,
            "session_code": bill.get("session"),
            "gov_id": bill.get("identifier"),
            "source_url": sources[0].get("url") if sources and isinstance(sources[0], dict) else None,
        }

    def _metadata(
        self, key: str, doc_type: str, title: str, jurisdiction: str, ocd_bill_id: str, extra: dict
    ) -> DocumentMetadata:
        return DocumentMetadata(
            document_id=key,
            document_type=doc_type,
            source=_SOURCE,
            title=title,
            jurisdiction=jurisdiction.upper(),
            bill_id=ocd_bill_id,
            url=extra.get("source_url"),
            extra=extra,
        )

    async def embed_entity(
        self, key: str, content: str, metadata: DocumentMetadata, *, dry_run: bool = False
    ) -> str:
        """SYNC-91: embed one standalone document (an organization) unless its cached digest
        (content plus metadata) already matches. Returns "written", "unchanged", "would_write" (dry
        run: nothing is written, not even the cache) or "undone:<why>". The cache entry is stored
        under the document id itself (`ddp:bill_version:organization:<id>`), which cannot collide
        with a bare ocd bill id or a legacy webflow id, and is written only after the Pinecone
        write succeeded."""
        cache = await self.redis.get_bill_version(key) or {}
        if cache.get("schema") != CACHE_SCHEMA:
            cache = {}
        # The digest covers the metadata too (it is stored with every vector): a changed slug, url or
        # type must re-embed even when the rendered text is identical. The two timestamps
        # `to_dict()` stamps with "now" are left out, or nothing would ever look unchanged.
        stable = {k: v for k, v in metadata.to_dict().items() if k not in ("created_at", "updated_at")}
        digest = content_hash(content + "\n" + json.dumps(stable, sort_keys=True, default=str))
        if cache.get("hash") == digest:
            if not dry_run:
                # A record written before this path was persistent still carries the 90-day expiry; make it
                # permanent now (a no-op when it already is), or an unchanged re-run would never fix it and the
                # digest-skip would be lost after 90 days. The ledger does not cover these records.
                await self.redis.find_unrecorded_bill_versions([key])
            return "unchanged"
        if dry_run:
            return "would_write"
        try:
            chunks = await self._ingest(key, content, metadata, cache.get("chunks", 0))
        except Exception as e:  # noqa: BLE001 -- the caller records it as undone
            return f"undone:{e}"
        written = await self.redis.set_bill_version(key, {
            "schema": CACHE_SCHEMA, "hash": digest, "chunks": chunks,
            "last_checked": datetime.now(timezone.utc).isoformat(),
        }, persistent=True)  # no expiry, like the bill records: an aged-out record would look never embedded
        return "written" if written else "undone:version cache not written (Redis)"

    async def _cached_field(
        self, ocd_bill_id: str, archive_id: str, field: str, *, expect_cache: bool
    ) -> Any:
        """Fresh read of one cached value: a document's `field`. None when nothing is cached (or
        the entry is not this path's schema).
        `get_bill_version` returns None both for "no entry" and for a Redis error, so when an entry
        existed at the start of the call (`expect_cache`) a None now is `_UNREADABLE`: most likely a
        blip, never evidence that another writer changed the document."""
        fresh = await self.redis.get_bill_version(ocd_bill_id)
        if fresh is None:
            return _UNREADABLE if expect_cache else None
        if fresh.get("schema") != CACHE_SCHEMA:
            return None
        return ((fresh.get("documents") or {}).get(archive_id) or {}).get(field)

    async def embed_bill(
        self, ocd_bill_id: str, jurisdiction: str, bill: dict, scope: EmbedScope = SCOPE_ALL,
        *, max_orphans: int | None = None,
    ) -> dict:
        """Embed whatever of `bill` (an api-v3 detail response with versions and sources) is
        not yet embedded, limited to `scope`. Returns a stats dict; `undone` lists what could not
        be finished, and when it is non-empty the Redis cache was NOT advanced. `raced` counts
        documents skipped because another writer (the live hook or a backfill) changed their cache
        entry after this call read it; they are left to that writer, never overwritten (SYNC-90).
        The re-read narrows that race to the moment between it and the upsert; it is not atomic
        across Pinecone and Redis, and a residual mismatch is healed by the next pass, which compares
        the cached hash with api-v3's current text.

        `max_orphans` (SYNC-95; None = off): also delete the vectors of up to that many documents this
        path recorded for the bill whose archived document id api-v3 no longer returns for ANY of its
        versions (the picker re-chose a different row, a version was removed, or its row no longer has
        usable text). api-v3 returns every version of a bill in one detail response, each with the id of
        the row its picker chose, so an id missing from a non-empty response really is gone; a response
        with no archived ids at all removes nothing, and a version still returned with an id but no text
        is not an orphan. `stats["orphans"]` counts the documents removed and `stats["orphans_over_budget"]`
        those left because `max_orphans` was reached (they stay recorded). Each deletion re-reads the
        record first and skips a document another writer changed since this call read it (`raced`); like
        the write guard, that narrows the race, it does not close it."""
        stats: dict[str, Any] = {
            "ocd_bill_id": ocd_bill_id, "documents": 0, "diffs": 0, "orphans": 0, "orphans_over_budget": 0,
            "chunks": 0, "chars": 0, "no_text_yet": 0, "raced": 0, "undone": [],
        }
        cache = await self.redis.get_bill_version(ocd_bill_id) or {}
        if cache.get("schema") != CACHE_SCHEMA:
            cache = {}
        had_cache = bool(cache)  # an entry exists now; losing sight of it later is a blip, not a race
        docs: dict[str, dict] = {k: dict(v) for k, v in (cache.get("documents") or {}).items()}
        updates: dict[str, dict] = {}  # archive id -> the cache fields THIS call set

        base = self._base_extra(bill, ocd_bill_id)
        gov_id = bill.get("identifier") or ocd_bill_id
        previous: dict | None = None  # the last classifiable version seen, for diff labels
        versions = bill.get("versions") or []
        # The current version is the last classifiable one in api-v3's own order (never re-derived).
        current_ordinal = max(
            (v["version_ordinal"] for v in versions
             if v.get("version_ordinal") is not None
             and (v.get("version_stage") or STAGE_UNKNOWN) != STAGE_UNKNOWN),
            default=None,
        )

        for version in versions:
            note, date = version.get("note") or "", version.get("date") or ""
            stage = version.get("version_stage") or STAGE_UNKNOWN
            text = version_text(version)
            if not text:
                stats["no_text_yet"] += 1  # not archived yet; a later run finds it
                continue
            archive_id = version.get("archived_document_id")
            if archive_id is None:
                stats["undone"].append(f"'{note}': api-v3 sent text but no archived_document_id")
                continue
            aid = str(archive_id)

            label = {
                "document_id": aid, "version_note": note, "version_date": date,
                "version_stage": stage, "version_ordinal": version.get("version_ordinal"),
            }
            entry = docs.get(aid, {})

            wanted = scope.text == "all" or (
                scope.text == "current" and stage != STAGE_UNKNOWN
                and version.get("version_ordinal") == current_ordinal
            )
            h = content_hash(text)
            if wanted and entry.get("text_hash") != h:
                key = text_document_key(ocd_bill_id, archive_id)
                current = await self._cached_field(ocd_bill_id, aid, "text_hash", expect_cache=had_cache)
                if current is _UNREADABLE:
                    stats["undone"].append(f"{key}: version cache unreadable before write")
                elif current != entry.get("text_hash"):
                    stats["raced"] += 1  # someone wrote this document since we read the cache
                else:
                    meta = self._metadata(key, DOCUMENT_TYPE_TEXT, f"{gov_id} - {note}", jurisdiction,
                                          ocd_bill_id, {**base, **label})
                    try:
                        n = await self._ingest(key, text, meta, entry.get("chunks", 0))
                    except Exception as e:  # noqa: BLE001 -- recorded as undone, never aborts the bill
                        stats["undone"].append(f"{key}: {e}")
                    else:
                        fields = {"text_hash": h, "chunks": n}
                        docs.setdefault(aid, {}).update(fields)
                        updates.setdefault(aid, {}).update(fields)
                        stats["documents"] += 1
                        stats["chunks"] += n
                        stats["chars"] += len(text)

            # SYNC-95: remember the source row's updated_at this text was checked against (only for a document
            # that is recorded in sync with api-v3's text), so the ledger pass can tell "unchanged" without
            # reading the bill. A stamp is written even when the text itself needed nothing.
            stamp = version.get("archived_updated_at")
            if stamp and docs.get(aid, {}).get("text_hash") == h and docs[aid].get("source_updated_at") != stamp:
                docs[aid]["source_updated_at"] = stamp
                updates.setdefault(aid, {})["source_updated_at"] = stamp

            diff = version.get("diff_from_previous_version")
            if scope.diffs and diff and stage != STAGE_UNKNOWN:
                dh = content_hash(diff)
                if entry.get("diff_hash") != dh:
                    key = diff_document_key(ocd_bill_id, archive_id)
                    current = await self._cached_field(ocd_bill_id, aid, "diff_hash", expect_cache=had_cache)
                    if current is _UNREADABLE:
                        stats["undone"].append(f"{key}: version cache unreadable before write")
                    elif current != entry.get("diff_hash"):
                        stats["raced"] += 1
                    else:
                        from_label = {
                            "from_document_id": str(previous["archive_id"]) if previous else None,
                            "from_version_note": previous["note"] if previous else None,
                            "from_version_date": previous["date"] if previous else None,
                        }
                        meta = self._metadata(key, DOCUMENT_TYPE_DIFF, f"{gov_id} - changes in {note}",
                                              jurisdiction, ocd_bill_id, {**base, **label, **from_label})
                        try:
                            n = await self._ingest(key, diff, meta, entry.get("diff_chunks", 0))
                        except Exception as e:  # noqa: BLE001
                            stats["undone"].append(f"{key}: {e}")
                        else:
                            fields = {"diff_hash": dh, "diff_chunks": n}
                            docs.setdefault(aid, {}).update(fields)
                            updates.setdefault(aid, {}).update(fields)
                            stats["diffs"] += 1
                            stats["chunks"] += n
                            stats["chars"] += len(diff)
            if stage != STAGE_UNKNOWN:
                previous = {"archive_id": archive_id, "note": note, "date": date}

        removed: dict[str, dict] = {}  # archived id -> the record we deleted the vectors of
        if max_orphans is not None:
            listed_ids = {str(v["archived_document_id"]) for v in versions if v.get("archived_document_id") is not None}
            orphans = [a for a in docs if a not in listed_ids] if listed_ids else []
            stats["orphans_over_budget"] = max(0, len(orphans) - max_orphans)
            for aid in orphans[:max_orphans]:
                recorded = docs[aid]
                fresh = await self.redis.get_bill_version(ocd_bill_id)
                if fresh is None and had_cache:
                    stats["undone"].append(f"{ocd_bill_id}: version cache unreadable before removing {aid}")
                    continue
                fresh_entry = ((fresh or {}).get("documents") or {}).get(aid) if (fresh or {}).get("schema") == CACHE_SCHEMA else None
                if fresh_entry != recorded:  # another writer changed or removed it since this call read it
                    stats["raced"] += 1
                    continue
                try:
                    await self._delete_document_vectors(text_document_key(ocd_bill_id, aid), recorded.get("chunks", 0))
                    await self._delete_document_vectors(diff_document_key(ocd_bill_id, aid), recorded.get("diff_chunks", 0))
                except Exception as e:  # noqa: BLE001 -- recorded as undone; the record stays so the next run retries
                    stats["undone"].append(f"{ocd_bill_id}: orphaned document {aid} not removed: {e}")
                    continue
                removed[aid] = recorded
                stats["orphans"] += 1
        if not stats["undone"] and (updates or removed):
            # Advanced last: every Pinecone write for this bill has succeeded. Merged onto a fresh
            # read so another writer's entries for OTHER documents are kept, not clobbered.
            latest = await self.redis.get_bill_version(ocd_bill_id)
            if latest is None and had_cache:
                # Unreadable (or gone) since we started: writing only our documents would erase the
                # others' entries. Leave the cache alone; the next pass re-embeds (idempotent).
                stats["undone"].append("version cache unreadable at write")
            else:
                latest = latest if latest and latest.get("schema") == CACHE_SCHEMA else {}
                merged_docs = {k: dict(v) for k, v in (latest.get("documents") or {}).items()}
                for aid, fields in updates.items():
                    merged_docs.setdefault(aid, {}).update(fields)
                for aid, recorded in removed.items():
                    if merged_docs.get(aid) == recorded:  # not if a writer changed it since the re-read above
                        merged_docs.pop(aid, None)
                written = await self.redis.set_bill_version(ocd_bill_id, {
                    "schema": CACHE_SCHEMA, "documents": merged_docs,
                    "last_checked": datetime.now(timezone.utc).isoformat(),
                }, persistent=True)
                if not written:
                    stats["undone"].append("version cache not written (Redis)")
        if stats["undone"]:
            logger.warning(
                "knowledge_base_embedding_work_left_undone",
                ocd_bill_id=ocd_bill_id, jurisdiction=jurisdiction, undone=stats["undone"],
            )
        self.pipeline.reset_hash_cache()
        return stats


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)  # `document_updated_since` this old lists every bill with an archived document

# The dry run's cost model, from the SYNC-89 measurement on real Utah documents (about four characters
# per token; 0.279 vectors per 1,000 characters) at OpenAI's list price for text-embedding-3-large.
# The backfill's `approx_tokens` uses the same four characters per token.
_CHARS_PER_TOKEN = 4
_CHUNKS_PER_1000_CHARS = 0.279
_USD_PER_MILLION_TOKENS = 0.13
RECONCILE_PLAN_SAMPLE = 20  # bills read to estimate the size of the rest

# A circuit breaker for orphan removal: if one run finds more orphaned documents than this, something systematic
# changed (say, api-v3 started returning ids in a different form) and deleting on would wipe real vectors.
MAX_ORPHAN_DOCUMENTS_PER_RUN = 200


def _embeddable_size(bill: dict) -> tuple[int, int]:
    """`(documents, characters)` that `embed_bill` would write for `bill` at the default scope: every
    version's text (version diffs are not embedded, Ramon 2026-10-05), minus what it skips (no archived
    text, no archived document id)."""
    documents = characters = 0
    for version in bill.get("versions") or []:
        text = version_text(version)
        if not text or version.get("archived_document_id") is None:
            continue
        documents += 1
        characters += len(text)
    return documents, characters


async def plan_reconcile(
    jurisdiction: str, *, api_base: str, api_key: str = "", sample_size: int = RECONCILE_PLAN_SAMPLE,
    run_id: str | None = None, use_ledger: bool = False, delete_orphans: bool = False,
) -> dict:
    """SYNC-95's dry run: what a reconcile pass would find for `jurisdiction`, before any money is
    spent. Writes nothing (not even the persistence repair) and calls no embedding API; it costs the
    bill listing, one Redis read per bill, and `sample_size` bill reads from api-v3.

    Lists every archived bill, counts the ones with no record, reads a random sample of those, and
    extrapolates documents, chunks, tokens and dollars from the sample's mean size. The estimate is an
    order of magnitude, not a quote: bill sizes vary by orders of magnitude (a federal bill can be a
    hundred times a state bill), so a small sample can be far off.

    It says when it is weak instead of looking fine: `sampled` and `sample_failed` count the bills that
    could and could not be used (a bill that cannot be read, or whose data is malformed, is skipped
    and counted); with unrecorded bills but no usable sample the status is `error` and there is no
    estimate; an incomplete listing makes the status `incomplete` (the counts are lower bounds). A failed
    read never raises.

    `use_ledger` plans the ledger pass instead (`_reconcile_from_ledger`): `unrecorded` then counts the bills
    that disagree with api-v3's ledger, and `missing_documents`, `changed_documents` and `orphaned_documents`
    (when `delete_orphans`) break that down. The estimate samples those bills at their full size, so it
    over-states a bill that only needs a new stamp."""
    plan: dict[str, Any] = {"jurisdiction": jurisdiction, "status": "ok"}
    if run_id:
        plan["run_id"] = run_id  # the one the trigger returned, so the result line can be found
    redis = get_redis_store()
    if not redis.is_available:
        plan.update(status="error", error="redis_unavailable")
    else:
        if use_ledger:
            work = await _ledger_work(jurisdiction, redis, api_base=api_base, api_key=api_key,
                                      delete_orphans=delete_orphans, persist_expiring=False)
            if work is None:
                plan.update(status="error", error="ledger_unavailable")
                listed = None
            else:
                findings, complete, count = work
                listed = (findings, complete)
                plan.update(listed=count, missing_documents=sum(f["missing"] for f in findings.values()),
                            changed_documents=sum(f["changed"] for f in findings.values()),
                            orphaned_documents=sum(f["orphaned"] for f in findings.values()))
        else:
            listed = await local_openstates_client.list_touched_bill_ids(
                jurisdiction, since=_EPOCH, api_base=api_base, api_key=api_key
            )
            if listed is None:
                plan.update(status="error", error="listing_failed")
        if listed is not None:
            ids, complete = listed
            missing = list(ids) if use_ledger else await redis.find_unrecorded_bill_versions(ids, persist_expiring=False)
            if missing is None:
                plan.update(status="error", error="records_unreadable")
            else:
                plan.update(listing_complete=complete, unrecorded=len(missing))
                if not use_ledger:
                    plan["listed"] = len(ids)
                sizes: list[tuple[int, int]] = []
                failed = 0
                for ocd_bill_id in random.sample(missing, min(sample_size, len(missing))):
                    bill = await local_openstates_client.fetch_bill_for_embedding(
                        ocd_bill_id, api_base=api_base, api_key=api_key
                    )
                    try:
                        if bill is None:
                            raise ValueError("bill detail could not be read from api-v3")
                        sizes.append(_embeddable_size(bill))
                    except Exception as e:  # noqa: BLE001 -- one unusable bill must not end the plan
                        failed += 1
                        logger.warning("knowledge_base_reconcile_plan_sample_failed", jurisdiction=jurisdiction,
                                       ocd_bill_id=ocd_bill_id, error=str(e))
                plan["sampled"], plan["sample_failed"] = len(sizes), failed
                if not complete:
                    plan.update(status="incomplete", note="the bill listing was incomplete: the counts and the estimate are lower bounds")
                if missing and not sizes:
                    plan.update(status="error", error="sample_unreadable")
                if sizes:
                    mean_documents = sum(d for d, _ in sizes) / len(sizes)
                    mean_chars = sum(c for _, c in sizes) / len(sizes)
                    chars = mean_chars * len(missing)
                    tokens = chars / _CHARS_PER_TOKEN
                    plan["estimate"] = {
                        "documents": round(mean_documents * len(missing)),
                        "chunks": round(chars * _CHUNKS_PER_1000_CHARS / 1000),
                        "tokens": round(tokens),
                        "usd": round(tokens / 1_000_000 * _USD_PER_MILLION_TOKENS, 2),
                        "largest_sampled_bill_chars": max(c for _, c in sizes),
                    }
    (logger.info if plan["status"] == "ok" else logger.warning)("knowledge_base_reconcile_plan", **plan)
    return plan


async def _fetch_and_embed(
    embedder: KnowledgeBaseEmbedder, jurisdiction: str, ocd_bill_id: str, *, api_base: str, api_key: str,
    max_orphans: int | None = None,
) -> dict:
    """Read one bill from api-v3 and embed whatever of it is not yet embedded. Raises on a read
    failure; the caller records that bill as failed and carries on."""
    bill = await local_openstates_client.fetch_bill_for_embedding(ocd_bill_id, api_base=api_base, api_key=api_key)
    if bill is None:
        raise RuntimeError("bill detail could not be read from api-v3")
    return await embedder.embed_bill(ocd_bill_id, jurisdiction, bill, max_orphans=max_orphans)


async def _reconcile_unembedded(
    jurisdiction: str,
    redis,
    embedder: KnowledgeBaseEmbedder,
    totals: dict,
    *,
    touched: set[str],
    max_bills: int,
    api_base: str,
    api_key: str,
) -> dict[str, int]:
    """SYNC-95: embed bills the version cache has no record of, the way the LegBot pipeline checks
    which bills are already covered and dispatches only what is missing. A bill nobody changed
    since it was archived is never in the touched list, so without this it stays unembedded forever
    (a newly enrolled jurisdiction, a Pinecone or Redis gap, a bill added mid-backfill).

    Lists every bill api-v3 has an archived document for, drops the ones this run already handled,
    asks Redis which have no record, and embeds up to `max_bills` of them (bills *attempted*, not tokens;
    the listing and the Redis check above it are not capped). The slice is random when there are more
    missing than the cap, so a bill that fails every time cannot hold a slot at the front forever; the
    record is the cursor, so embedded bills drop out and later runs reach the rest. A failed bill keeps
    no record and is found again. A listing that came back incomplete is still safe to use: every bill
    on it that has no record really is unrecorded. Never raises; a failed read just skips the pass."""
    result = {"missing": 0, "selected": 0, "embedded": 0, "nothing_to_embed": 0, "failed": 0}
    listed = await local_openstates_client.list_touched_bill_ids(
        jurisdiction, since=_EPOCH, api_base=api_base, api_key=api_key
    )
    if listed is None:
        logger.warning("knowledge_base_reconcile_skipped", jurisdiction=jurisdiction, reason="bills could not be listed")
        return result
    candidates = [i for i in listed[0] if i not in touched]
    missing = await redis.find_unrecorded_bill_versions(candidates)
    if missing is None:  # Redis could not answer: an outage must not look like "nothing is embedded"
        logger.warning("knowledge_base_reconcile_skipped", jurisdiction=jurisdiction, reason="version records unreadable")
        return result
    chosen = missing if len(missing) <= max_bills else random.sample(missing, max_bills)
    result["missing"], result["selected"] = len(missing), len(chosen)
    if len(missing) > len(chosen):
        logger.warning("knowledge_base_reconcile_backlog", jurisdiction=jurisdiction,
                       remaining=len(missing) - len(chosen), cap=max_bills)

    for ocd_bill_id in chosen:
        try:
            stats = await _fetch_and_embed(embedder, jurisdiction, ocd_bill_id, api_base=api_base, api_key=api_key)
        except Exception as e:  # noqa: BLE001 -- one bad bill must not stop the pass
            result["failed"] += 1
            logger.warning("knowledge_base_reconcile_bill_failed", jurisdiction=jurisdiction,
                           ocd_bill_id=ocd_bill_id, error=str(e))
            continue
        for k in ("documents", "diffs", "chunks"):
            totals[k] += stats[k]
        if stats["undone"]:
            result["failed"] += 1
        elif stats["documents"] == 0 and stats["diffs"] == 0:
            # Nothing embeddable yet (no archived text). Record that it was checked so it is not read
            # again every night; a later archive that adds text lists it as touched and embeds it.
            # Set-if-absent, so a record another writer created meanwhile is never replaced.
            await redis.set_bill_version(ocd_bill_id, {
                "schema": CACHE_SCHEMA, "documents": {},
                "last_checked": datetime.now(UTC).isoformat(),
            }, persistent=True, only_if_absent=True)
            result["nothing_to_embed"] += 1
        else:
            result["embedded"] += 1
    return result


def ledger_findings(ledger: dict[str, dict[str, str | None]], records: dict[str, dict | None],
                    *, delete_orphans: bool) -> dict[str, dict[str, int]]:
    """SYNC-95: per bill, what api-v3's ledger and this path's own version records disagree about; only
    bills with a disagreement appear. `missing`: a listed document the record does not have (a bill with
    no record at all has all of them missing). `changed`: a recorded document whose source row's
    `updated_at` is not the one stamped on the record, or has no stamp yet (one cheap re-check after the
    stamp exists, then it is quiet); `updated_at` only means "look again", the bill read compares the text
    hash. `orphaned` (only counted with `delete_orphans`): a recorded document the ledger no longer lists."""
    findings: dict[str, dict[str, int]] = {}
    for ocd_bill_id, listed in ledger.items():
        record = records.get(ocd_bill_id)
        recorded = (record.get("documents") or {}) if record and record.get("schema") == CACHE_SCHEMA else {}
        missing = sum(1 for a in listed if a not in recorded)
        changed = sum(1 for a, updated in listed.items()
                      if a in recorded and updated is not None and recorded[a].get("source_updated_at") != updated)
        orphaned = sum(1 for a in recorded if a not in listed) if delete_orphans else 0
        if missing or changed or orphaned:
            findings[ocd_bill_id] = {"missing": missing, "changed": changed, "orphaned": orphaned}
    return findings


async def _ledger_work(jurisdiction: str, redis, *, api_base: str, api_key: str, delete_orphans: bool,
                       persist_expiring: bool = True) -> tuple[dict[str, dict[str, int]], bool, int] | None:
    """`(findings, ledger_complete, ledger_bills)`, or None when the ledger or the records could not be read
    (api-v3 without the endpoint, an outage): the caller must not treat that as "nothing to do"."""
    listed = await local_openstates_client.list_embedding_ledger(jurisdiction, api_base=api_base, api_key=api_key)
    if listed is None:
        return None
    ledger, complete = listed
    ids = list(ledger)
    if await redis.find_unrecorded_bill_versions(ids, persist_expiring=persist_expiring) is None:  # keeps records from aging out
        return None
    records = await redis.get_bill_versions(ids)
    if records is None:
        return None
    return ledger_findings(ledger, records, delete_orphans=delete_orphans), complete, len(ledger)


async def _reconcile_from_ledger(
    jurisdiction: str, redis, embedder: KnowledgeBaseEmbedder, totals: dict, *, max_bills: int,
    delete_orphans: bool, api_base: str, api_key: str,
) -> dict | None:
    """SYNC-95: one loop over what api-v3 says should be embedded versus what this path recorded, in place
    of the watermark's touched list and the unrecorded-bill pass: it finds a document that is missing, one
    whose source row changed, and (with `delete_orphans`) one api-v3 no longer lists, without a watermark.
    Reads each bill that disagrees and lets `embed_bill` decide what to write (it compares hashes, so a
    changed `updated_at` with the same text writes nothing but the new stamp). At most `max_bills` bills
    are attempted per run, a random slice when more disagree, so a bill that fails every time cannot
    hold a slot forever; a fixed bill drops out and later runs reach the rest. Returns None when the ledger
    could not be read (the caller falls back to the touched pass); a bill that fails is counted, not raised."""
    work = await _ledger_work(jurisdiction, redis, api_base=api_base, api_key=api_key, delete_orphans=delete_orphans)
    if work is None:
        logger.warning("knowledge_base_ledger_unavailable", jurisdiction=jurisdiction)
        return None
    findings, complete, listed = work
    pending = list(findings)
    chosen = pending if len(pending) <= max_bills else random.sample(pending, max_bills)
    result: dict[str, Any] = {
        "listed": listed, "listing_complete": complete, "bills_to_check": len(pending), "selected": len(chosen),
        "missing_documents": sum(f["missing"] for f in findings.values()),
        "changed_documents": sum(f["changed"] for f in findings.values()),
        "orphaned_documents": sum(f["orphaned"] for f in findings.values()), "failed": 0,
    }
    if len(pending) > len(chosen):
        logger.warning("knowledge_base_reconcile_backlog", jurisdiction=jurisdiction,
                       remaining=len(pending) - len(chosen), cap=max_bills)
    for ocd_bill_id in chosen:
        budget = max(0, MAX_ORPHAN_DOCUMENTS_PER_RUN - totals["orphans"]) if delete_orphans else None
        totals["bills"] += 1
        try:
            stats = await _fetch_and_embed(embedder, jurisdiction, ocd_bill_id, api_base=api_base,
                                           api_key=api_key, max_orphans=budget)
        except Exception as e:  # noqa: BLE001 -- one bad bill must not stop the pass
            result["failed"] += 1
            logger.warning("knowledge_base_reconcile_bill_failed", jurisdiction=jurisdiction,
                           ocd_bill_id=ocd_bill_id, error=str(e))
            continue
        for k in ("documents", "diffs", "chunks", "orphans", "orphans_over_budget"):
            totals[k] += stats[k]
        if stats["undone"]:
            result["failed"] += 1
    return result


async def embed_archived_bills(
    jurisdiction: str,
    archive_started_at: datetime,
    *,
    settings: SyncSettings,
    api_base: str,
    api_key: str = "",
    embedder: KnowledgeBaseEmbedder | None = None,
    reconcile_max_bills: int = 0,
    delete_orphans: bool = False,
    ledger_max_bills: int = 0,
) -> dict:
    """Embed every bill whose archived documents changed since the last run that left nothing
    undone (or `archive_started_at` when there is none). Never raises for a per-bill problem.
    Returns run totals; `complete` is True only when nothing was left undone, and only then is
    the jurisdiction's watermark advanced to `archive_started_at`.

    `reconcile_max_bills` > 0 (SYNC-95, `knowledge_base_embedding.reconcile.max_bills_per_run`) also
    embeds up to that many bills the version cache has no record of; 0 (the default) skips that pass.
    Its outcome is in `totals["reconcile"]` and never affects `complete` or the watermark (`complete`
    describes the touched pass only): a bill it fails on keeps no record, so a later reconcile finds
    it again.

    `delete_orphans` (SYNC-95, `knowledge_base_embedding.delete_orphans`) removes the vectors of
    documents api-v3 no longer returns for a bill the run touches. `totals["orphans"]` counts them. The
    run never deletes more than `MAX_ORPHAN_DOCUMENTS_PER_RUN` documents in total (each bill is given
    only what is left of that budget); the rest stay recorded, `totals["orphans_over_budget"]` counts them,
    and `knowledge_base_orphan_removal_paused` is logged once.

    `ledger_max_bills` > 0 (SYNC-95, `knowledge_base_embedding.ledger.max_bills_per_run`) replaces all of the
    above with one reconcile against api-v3's ledger (`_reconcile_from_ledger`): no watermark is read or
    written, and `reconcile_max_bills` is not used. It falls back to the watermark path when the ledger
    cannot be read."""
    redis = get_redis_store()
    totals: dict[str, Any] = {
        "jurisdiction": jurisdiction, "bills": 0, "documents": 0, "diffs": 0, "orphans": 0,
        "orphans_over_budget": 0, "chunks": 0, "failed_bills": 0, "complete": False,
        # "ledger" | "watermark" | "watermark_fallback" (the ledger was asked for and could not be read)
        "mode": "ledger" if ledger_max_bills > 0 else "watermark",
    }
    if not redis.is_available:
        # Without the version cache every touched bill would be re-embedded in full.
        logger.warning("knowledge_base_embedding_work_left_undone", jurisdiction=jurisdiction,
                       undone=["Redis unavailable; nothing embedded this run"])
        return totals

    if ledger_max_bills > 0:
        embedder = embedder or KnowledgeBaseEmbedder(settings)
        outcome = await _reconcile_from_ledger(
            jurisdiction, redis, embedder, totals, max_bills=ledger_max_bills, delete_orphans=delete_orphans,
            api_base=api_base, api_key=api_key,
        )
        if outcome is not None:
            totals["ledger"] = outcome
            totals["failed_bills"] = outcome["failed"]
            # Complete only when the whole ledger was read and nothing disagreeing was left (over the cap or failed).
            totals["complete"] = (outcome["listing_complete"] and outcome["failed"] == 0
                                  and outcome["selected"] == outcome["bills_to_check"])
            if totals["orphans_over_budget"]:
                logger.warning("knowledge_base_orphan_removal_paused", jurisdiction=jurisdiction, removed=totals["orphans"],
                               left_recorded=totals["orphans_over_budget"], cap=MAX_ORPHAN_DOCUMENTS_PER_RUN)
            (logger.info if totals["complete"] else logger.warning)("knowledge_base_embedding_run", **totals)
            return totals
        # api-v3 has no ledger yet (or it failed): carry on with the watermark path below, as before.
        # It reads and advances the watermark, so a fallback run reports that it was one.
        totals["mode"] = "watermark_fallback"

    since = archive_started_at
    previous = await redis.get_kb_embed_watermark(jurisdiction)
    if previous:
        try:
            since = min(since, datetime.fromisoformat(previous))
        except (ValueError, TypeError):
            logger.warning("knowledge_base_embedding_bad_watermark", value=previous)

    listed = await local_openstates_client.list_touched_bill_ids(
        jurisdiction, since=since, api_base=api_base, api_key=api_key
    )
    if listed is None:
        logger.warning("knowledge_base_embedding_work_left_undone", jurisdiction=jurisdiction,
                       undone=["could not list touched bills from api-v3"])
        return totals
    bill_ids, complete = listed

    embedder = embedder or KnowledgeBaseEmbedder(settings)
    for ocd_bill_id in bill_ids:
        totals["bills"] += 1
        budget = max(0, MAX_ORPHAN_DOCUMENTS_PER_RUN - totals["orphans"]) if delete_orphans else None
        try:
            stats = await _fetch_and_embed(embedder, jurisdiction, ocd_bill_id, api_base=api_base,
                                           api_key=api_key, max_orphans=budget)
        except Exception as e:  # noqa: BLE001 -- one bad bill must not stop the run
            totals["failed_bills"] += 1
            logger.warning("knowledge_base_embedding_work_left_undone", jurisdiction=jurisdiction,
                           ocd_bill_id=ocd_bill_id, undone=[str(e)])
            continue
        for k in ("documents", "diffs", "chunks", "orphans", "orphans_over_budget"):
            totals[k] += stats[k]
        if stats["undone"]:
            totals["failed_bills"] += 1

    totals["complete"] = complete and totals["failed_bills"] == 0
    if totals["orphans_over_budget"]:
        logger.warning("knowledge_base_orphan_removal_paused", jurisdiction=jurisdiction, removed=totals["orphans"],
                       left_recorded=totals["orphans_over_budget"], cap=MAX_ORPHAN_DOCUMENTS_PER_RUN)
    if totals["complete"]:
        await redis.set_kb_embed_watermark(jurisdiction, archive_started_at.isoformat())
    if reconcile_max_bills > 0:
        totals["reconcile"] = await _reconcile_unembedded(
            jurisdiction, redis, embedder, totals, touched=set(bill_ids), max_bills=reconcile_max_bills,
            api_base=api_base, api_key=api_key,
        )
    log = logger.info if totals["complete"] else logger.warning
    log("knowledge_base_embedding_run", since=since.isoformat(), **totals)
    return totals
