"""SYNC-83: embed archived bill text, version diffs and votes into the NEW Pinecone index.

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
* `bill-votes:{ocd_bill_id}` -- refreshed on its own change signal (vote count + latest date).

Version order, stage and ordinal come from api-v3 (`version_stage`, `version_ordinal`, array
order); nothing here classifies or sorts. No LegBot output is embedded.

Redis cache `ddp:bill_version:{ocd_bill_id}` (`schema` 2): the per-document text/diff hashes and
chunk counts, and the votes fingerprint and chunk count. It is written only after every Pinecone
write for the bill has succeeded, so a failed bill is retried by the next run (the per-jurisdiction
watermark is not advanced either), and a WARNING names what was left undone.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
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
DOCUMENT_TYPE_VOTES = "bill-votes"
STAGE_UNKNOWN = "unknown"  # api-v3's label for STAGE_UNKNOWN
_SOURCE = "OpenStates archive"


def text_document_key(ocd_bill_id: str, archive_id: Any) -> str:
    return f"{DOCUMENT_TYPE_TEXT}:{ocd_bill_id}:{archive_id}"


def diff_document_key(ocd_bill_id: str, archive_id: Any) -> str:
    return f"{DOCUMENT_TYPE_DIFF}:{ocd_bill_id}:{archive_id}"


def votes_document_key(ocd_bill_id: str) -> str:
    return f"{DOCUMENT_TYPE_VOTES}:{ocd_bill_id}"


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def vote_fingerprint(votes: list[dict]) -> str:
    """Vote count and latest vote date -- the change signal PLAN 5.6 names. '' when no votes."""
    if not votes:
        return ""
    latest = max((str(v.get("start_date") or "") for v in votes), default="")
    return f"{len(votes)}:{latest}"


def version_text(version: dict) -> str:
    """The archived text api-v3 attached to a version: the link-level raw_text for classifiable
    versions, `archived_raw_text` for stage-unknown ones. '' when nothing is archived yet."""
    for link in version.get("links") or []:
        if link.get("raw_text"):
            return link["raw_text"]
    return version.get("archived_raw_text") or ""


class KnowledgeBaseEmbedder:
    """Embeds one bill at a time into the knowledge-base index. Construct once per run."""

    def __init__(
        self,
        settings: SyncSettings,
        *,
        pipeline: IngestionPipeline | None = None,
        votes_formatter=None,
        redis_store=None,
    ):
        self.settings = knowledge_base_settings(settings)  # raises if unset / equals legacy index
        self.pipeline = pipeline or IngestionPipeline(self.settings)
        self._votes_formatter = votes_formatter
        self.redis = redis_store or get_redis_store()

    def _format_votes(self, bill: dict) -> tuple[str, dict] | None:
        if self._votes_formatter is None:
            # Reused as-is so the votes document reads exactly like today's; built with the
            # knowledge-base settings, so even its own pipeline points at the new index.
            from ddp_sync.pipelines.bill_sync import BillSyncService

            self._votes_formatter = BillSyncService(self.settings).format_bill_votes_chunk
        return self._votes_formatter(bill, ddp_url=None)

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
        """SYNC-91: embed one standalone document (a legislator or an organization) unless its
        cached digest (content plus metadata) already matches. Returns "written", "unchanged", "would_write" (dry
        run: nothing is written, not even the cache) or "undone:<why>". The cache entry is stored
        under the document id itself (`ddp:bill_version:legislator-<uuid>`,
        `ddp:bill_version:organization:<id>`), which cannot collide with a bare ocd bill id or a
        legacy webflow id, and is written only after the Pinecone write succeeded."""
        cache = await self.redis.get_bill_version(key) or {}
        if cache.get("schema") != CACHE_SCHEMA:
            cache = {}
        # The digest covers the metadata too (it is stored with every vector): a changed slug, url or
        # type must re-embed even when the rendered text is identical. The two timestamps
        # `to_dict()` stamps with "now" are left out, or nothing would ever look unchanged.
        stable = {k: v for k, v in metadata.to_dict().items() if k not in ("created_at", "updated_at")}
        digest = content_hash(content + "\n" + json.dumps(stable, sort_keys=True, default=str))
        if cache.get("hash") == digest:
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
        })
        return "written" if written else "undone:version cache not written (Redis)"

    async def embed_bill(self, ocd_bill_id: str, jurisdiction: str, bill: dict) -> dict:
        """Embed whatever of `bill` (an api-v3 detail response with versions, votes, sources) is
        not yet embedded. Returns a stats dict; `undone` lists what could not be finished, and
        when it is non-empty the Redis cache was NOT advanced."""
        stats: dict[str, Any] = {
            "ocd_bill_id": ocd_bill_id, "documents": 0, "diffs": 0, "votes": 0,
            "chunks": 0, "no_text_yet": 0, "undone": [],
        }
        cache = await self.redis.get_bill_version(ocd_bill_id) or {}
        if cache.get("schema") != CACHE_SCHEMA:
            cache = {}
        docs: dict[str, dict] = {k: dict(v) for k, v in (cache.get("documents") or {}).items()}
        votes_cache: dict = dict(cache.get("votes") or {})

        base = self._base_extra(bill, ocd_bill_id)
        gov_id = bill.get("identifier") or ocd_bill_id
        bill_title = bill.get("title") or ""
        changed = False
        previous: dict | None = None  # the last classifiable version seen, for diff labels

        for version in bill.get("versions") or []:
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

            label = {
                "document_id": str(archive_id), "version_note": note, "version_date": date,
                "version_stage": stage, "version_ordinal": version.get("version_ordinal"),
            }
            entry = docs.setdefault(str(archive_id), {})

            h = content_hash(text)
            if entry.get("text_hash") != h:
                key = text_document_key(ocd_bill_id, archive_id)
                meta = self._metadata(key, DOCUMENT_TYPE_TEXT, f"{gov_id} - {note}", jurisdiction,
                                      ocd_bill_id, {**base, **label})
                try:
                    n = await self._ingest(key, text, meta, entry.get("chunks", 0))
                except Exception as e:  # noqa: BLE001 -- recorded as undone, never aborts the bill
                    stats["undone"].append(f"{key}: {e}")
                else:
                    entry.update(text_hash=h, chunks=n)
                    stats["documents"] += 1
                    stats["chunks"] += n
                    changed = True

            diff = version.get("diff_from_previous_version")
            if diff and stage != STAGE_UNKNOWN:
                dh = content_hash(diff)
                if entry.get("diff_hash") != dh:
                    key = diff_document_key(ocd_bill_id, archive_id)
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
                        entry.update(diff_hash=dh, diff_chunks=n)
                        stats["diffs"] += 1
                        stats["chunks"] += n
                        changed = True
            if stage != STAGE_UNKNOWN:
                previous = {"archive_id": archive_id, "note": note, "date": date}

        votes = bill.get("votes") or []
        fingerprint = vote_fingerprint(votes)
        if fingerprint and fingerprint != votes_cache.get("fingerprint"):
            key = votes_document_key(ocd_bill_id)
            formatted = self._format_votes(bill)
            if formatted:
                content, ids = formatted
                meta = self._metadata(
                    key, DOCUMENT_TYPE_VOTES, f"{gov_id} - {bill_title} - Voting Record", jurisdiction,
                    ocd_bill_id,
                    {**base, "vote_count": len(votes), "voter_count": ids.get("voter_count", 0),
                     "voter_person_ids": ids.get("voter_person_ids", [])},
                )
                try:
                    n = await self._ingest(key, content, meta, votes_cache.get("chunks", 0))
                except Exception as e:  # noqa: BLE001
                    stats["undone"].append(f"{key}: {e}")
                else:
                    votes_cache = {"fingerprint": fingerprint, "chunks": n}
                    stats["votes"] += 1
                    stats["chunks"] += n
                    changed = True

        if stats["undone"]:
            logger.warning(
                "knowledge_base_embedding_work_left_undone",
                ocd_bill_id=ocd_bill_id, jurisdiction=jurisdiction, undone=stats["undone"],
            )
        elif changed:
            # Advanced last: every Pinecone write for this bill has succeeded.
            written = await self.redis.set_bill_version(ocd_bill_id, {
                "schema": CACHE_SCHEMA, "documents": docs, "votes": votes_cache,
                "last_checked": datetime.now(timezone.utc).isoformat(),
            })
            if not written:
                stats["undone"].append("version cache not written (Redis)")
                logger.warning(
                    "knowledge_base_embedding_work_left_undone",
                    ocd_bill_id=ocd_bill_id, jurisdiction=jurisdiction, undone=stats["undone"],
                )
        self.pipeline.reset_hash_cache()
        return stats


async def embed_archived_bills(
    jurisdiction: str,
    archive_started_at: datetime,
    *,
    settings: SyncSettings,
    api_base: str,
    api_key: str = "",
    embedder: KnowledgeBaseEmbedder | None = None,
) -> dict:
    """Embed every bill whose archived documents changed since the last run that left nothing
    undone (or `archive_started_at` when there is none). Never raises for a per-bill problem.
    Returns run totals; `complete` is True only when nothing was left undone, and only then is
    the jurisdiction's watermark advanced to `archive_started_at`."""
    redis = get_redis_store()
    totals: dict[str, Any] = {
        "jurisdiction": jurisdiction, "bills": 0, "documents": 0, "diffs": 0, "votes": 0,
        "chunks": 0, "failed_bills": 0, "complete": False,
    }
    if not redis.is_available:
        # Without the version cache every touched bill would be re-embedded in full.
        logger.warning("knowledge_base_embedding_work_left_undone", jurisdiction=jurisdiction,
                       undone=["Redis unavailable; nothing embedded this run"])
        return totals

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
        try:
            bill = await local_openstates_client.fetch_bill_for_embedding(
                ocd_bill_id, api_base=api_base, api_key=api_key
            )
            if bill is None:
                raise RuntimeError("bill detail could not be read from api-v3")
            stats = await embedder.embed_bill(ocd_bill_id, jurisdiction, bill)
        except Exception as e:  # noqa: BLE001 -- one bad bill must not stop the run
            totals["failed_bills"] += 1
            logger.warning("knowledge_base_embedding_work_left_undone", jurisdiction=jurisdiction,
                           ocd_bill_id=ocd_bill_id, undone=[str(e)])
            continue
        for k in ("documents", "diffs", "votes", "chunks"):
            totals[k] += stats[k]
        if stats["undone"]:
            totals["failed_bills"] += 1

    totals["complete"] = complete and totals["failed_bills"] == 0
    if totals["complete"]:
        await redis.set_kb_embed_watermark(jurisdiction, archive_started_at.isoformat())
    log = logger.info if totals["complete"] else logger.warning
    log("knowledge_base_embedding_run", since=since.isoformat(), **totals)
    return totals
