# PLAN: Bill Version History + Changelog

**Status:** ✅ Phase 1 + Phase 2 SHIPPED 2026-06-06. 15 ddp-sync tests + 22 VoteBot tests pass. Functionally verified end-to-end. Pending: spot-check first real changelog quality after 04:00 UTC sync.
**Created:** 2026-06-06
**Repos:** ddp-sync (Phase 1), votebot (Phase 2)

---

## Goal

When a bill's text is updated in OpenStates, preserve the previous version in Pinecone as a permanent historical record and generate an LLM-produced changelog document summarising what changed. This solves two independent problems:

1. **The orphan chunk problem** — today, if a new bill version produces fewer chunks than the old one, excess old chunks remain in Pinecone with stale content silently polluting retrieval.
2. **The "what changed" use case** — VoteBot has no way to answer questions like "what was amended in this bill?" or "how has S.2 changed since it was introduced?".

---

## Background

The daily bill sync (`bill_version.py`) detects new versions by comparing the latest OpenStates `versions[]` entry against a Redis cache keyed by `webflow_id`. When a newer version is found, it:

1. Downloads the new bill text (PDF or HTML)
2. Ingests it via `IngestionPipeline.ingest_document()` with `skip_duplicates=False`
3. Updates the Webflow CMS status fields
4. Publishes a `votebot:cache:invalidate` event

The ingest overwrites matching chunk IDs in Pinecone (e.g. `bill-pdf-{webflow_id}-chunk-0`). If the new version produces fewer chunks than the old, the excess old chunks remain — stale content that VoteBot can retrieve without any indication it's out of date.

The Redis version cache (`ddp:bill_version:{webflow_id}`) already stores the old version's `text_url` at the moment a new version is detected. This plan also adds `chunk_count` to the cache so surplus old chunks can be deleted by exact ID without a Pinecone metadata scan.

---

## Solution: Hybrid Approach

Every time a new bill version is ingested, the pipeline:

- **Eliminates orphan chunks** by upsert-then-delete: upsert new chunks first (no availability window), then delete any chunk IDs from the previous version that exceed the new chunk count. The previous chunk count is stored in the Redis version cache.
- **Stores the new text as a permanent historical record** (`document_type="bill-text-history"`) with a versioned document ID so it is never overwritten by future updates.
- **Generates a changelog document** (`document_type="bill-changelog"`) by re-downloading the old text from the cached URL, passing both versions to an LLM, and storing the structured diff summary in Pinecone.

VoteBot's retrieval layer adds a changelog phase so "what changed" queries surface the changelog document directly.

```
New version detected (cached = old version, latest_version = new)
  │
  ├─ 1. Download NEW text from latest_version["text_url"]
  │
  ├─ 2. Upsert NEW chunks as bill-text (current content live immediately)
  │
  ├─ 3. Delete surplus old chunks from bill-text
  │      old_count = cached["chunk_count"]     ← stored in Redis version cache
  │      new_count = len(new_chunks)
  │      if old_count > new_count:
  │          delete IDs: bill-pdf-{webflow_id}-chunk-{new_count..old_count-1}
  │
  ├─ 4. Ingest NEW as bill-text-history-{new_version_date} (permanent)
  │
  └─ 5. Attempt changelog generation
         ├─ Try: download OLD text from cached["text_url"]
         ├─ If success → call LLM → ingest as bill-changelog-{new_version_date}
         └─ If stale URL / download failure → log warning, skip changelog
                                              (ingest still succeeds)
```

### Orphan chunk deletion — why upsert-then-delete

Deleting before upserting would create a brief window where the bill has zero chunks in Pinecone. Instead we upsert first (new content is immediately live), then delete only the excess old chunk IDs by their exact IDs. No Pinecone metadata scan is required — we know the chunk IDs deterministically from the document_id pattern and the stored chunk count.

---

## Retrieval Isolation Guarantee

`bill-text-history` and `bill-changelog` documents are **structurally invisible** to normal VoteBot queries. This is not enforced by a runtime flag — it is guaranteed by how retrieval already works:

VoteBot's multi-phase bill retrieval filters by explicit `document_type` in every phase:
- Phase 1: `document_type="bill-text"` — only the current version
- Phase 2: `document_type="bill"` — only CMS summary docs
- Phase 3: `document_type="bill-votes"` — only vote records

Because `bill-text-history` and `bill-changelog` match none of these filters, they cannot appear in any existing retrieval path. Phase 2 of this plan adds one new path — Phase 5 in VoteBot — which queries `document_type="bill-changelog"` only when changelog intent is detected. `bill-text-history` is never queried by VoteBot in either phase; it is stored for future use only.

This guarantee holds as long as the retrieval phases continue to filter by explicit `document_type`. If a future change adds an unfiltered fallback query, this assumption should be revisited.

---

## Pinecone Document Schema

### bill-text (unchanged)
Existing document. Overwritten on every new version. Serves as the authoritative current-version source for retrieval.

| Field | Value |
|---|---|
| `document_id` | `bill-pdf-{webflow_id}` |
| `document_type` | `bill-text` |
| `chunk_id` | `bill-pdf-{webflow_id}-chunk-{n}` |

### bill-text-history (new)
One document per bill version. Never overwritten. Created for every ingest including the first.

| Field | Value |
|---|---|
| `document_id` | `bill-text-history-{webflow_id}-{version_date}` |
| `document_type` | `bill-text-history` |
| `chunk_id` | `bill-text-history-{webflow_id}-{version_date}-chunk-{n}` |
| `version_date` | e.g. `2026-05-20` |
| `version_note` | e.g. `Placed on Calendar Senate` |
| `webflow_id` | Webflow item ID (for filtering) |
| `bill_slug` | Webflow slug |
| `jurisdiction` | State code |

### bill-changelog (new)
One document per version transition. Created only when old text is successfully retrieved. Never overwritten.

| Field | Value |
|---|---|
| `document_id` | `bill-changelog-{webflow_id}-{new_version_date}` |
| `document_type` | `bill-changelog` |
| `chunk_id` | `bill-changelog-{webflow_id}-{new_version_date}-chunk-{n}` |
| `version_from_date` | Old version date |
| `version_from_note` | Old version note (e.g. `Introduced in Senate`) |
| `version_to_date` | New version date |
| `version_to_note` | New version note (e.g. `Placed on Calendar Senate`) |
| `webflow_id` | Webflow item ID |
| `bill_slug` | Webflow slug |
| `jurisdiction` | State code |

---

## Changelog Format

The LLM prompt asks for a structured summary optimised for RAG retrieval — concise enough to fit cleanly in Pinecone chunks but rich enough for VoteBot to cite directly:

```
## What Changed: {bill_title}
**From:** {version_from_note} ({version_from_date})
**To:** {version_to_note} ({version_to_date})

### Summary
{1–2 sentence overview of the nature of the changes}

### Sections Added
- {bullet list}

### Sections Removed
- {bullet list}

### Sections Modified
- {bullet list}

### Key Policy Implications
- {bullet list}
```

The changelog is generated via the OpenAI API (already a ddp-sync dependency — `openai>=1.10.0`). Model: `gpt-4o-mini` (cost-efficient; this is a structured diff task, not complex reasoning). If old text retrieval fails, no changelog is generated and the pipeline continues normally.

---

## Why Not S3

During design review we considered using AWS S3 as a document store to hold raw bill text for every ingested version, eliminating dependence on government URLs being stable when changelog generation runs.

**We chose not to implement S3 at this time for the following reasons:**

1. **The stale URL problem is narrower than it appears.** Federal bills on govinfo.gov use permanent document IDs and are highly stable. FL, WA, VA, and MA state legislature URLs are generally stable across version transitions. The problematic jurisdictions (primarily AZ) represent a small fraction of the bill catalogue.

2. **The cost of a missing changelog is low.** A stale URL means the changelog for that particular transition is skipped — the ingest still succeeds, the new bill-text is current in Pinecone, and the bill-text-history record is still created. VoteBot continues to answer questions correctly; it just cannot answer "what changed in this transition" for that one version pair.

3. **S3 adds real operational complexity.** IAM policies, bucket lifecycle rules, failure handling (does a failed S3 write block the ingest?), and cost management would all need to be designed and maintained. That cost is not justified without data showing the stale URL failure rate is material.

4. **The right decision point is data-driven.** After shipping this feature, the changelog skip rate (logged as a structured metric) can be monitored. If more than ~15–20% of changelog generations are being skipped due to stale URLs, that is the right time to introduce S3 as a document archive. We will have real data instead of a hypothetical.

**Future path:** If S3 is added later, the integration point is `_generate_and_ingest_changelog()` in `bill_version.py` — the old-text fetch is isolated there and can be swapped from URL-fetch to S3-fetch with no changes to the surrounding logic.

---

## Implementation

### Phase 1 — ddp-sync

**Files changed:**
- `src/ddp_sync/pipelines/bill_version.py` — core changes
- `src/ddp_sync/ingestion/pipeline.py` — no changes expected (existing `ingest_document` handles new document types)
- `config/sync_schedule.yaml` — optional: add `changelog_enabled` flag
- `tests/test_bill_version_history.py` — new test file

#### Step 1 — Update `VersionCheckResult` and `VersionSyncBatchResult`

Add fields to track history/changelog outcomes:

```python
@dataclass
class VersionCheckResult:
    ...
    history_chunks_created: int = 0
    changelog_chunks_created: int = 0
    changelog_skipped: bool = False
    changelog_skip_reason: str = ""

@dataclass
class VersionSyncBatchResult:
    ...
    history_chunks_created: int = 0
    changelog_chunks_created: int = 0
    changelogs_skipped: int = 0
```

#### Step 2 — Add `_ingest_bill_history()`

New method on `BillVersionSyncService`. Takes the new bill text content + metadata and ingests it as `bill-text-history` with a versioned document ID. Called unconditionally whenever a new version is ingested (including first ingest).

```python
async def _ingest_bill_history(
    self,
    webflow_id: str,
    bill_title: str,
    bill_slug: str,
    text_url: str,
    media_type: str,
    version_date: str,
    version_note: str,
    fields: dict,
    content: str,           # already-extracted text, passed in to avoid re-download
) -> int:
    """Ingest bill text as a permanent historical record."""
```

The `content` parameter is passed in from `_ingest_bill_text()` so we don't re-download the PDF. `_ingest_bill_text()` needs to return the extracted text alongside the chunk count.

#### Step 3 — Add `_generate_and_ingest_changelog()`

New method. Attempts to download old text from `cached["text_url"]`, calls the OpenAI API to generate a structured changelog, and ingests the result as `bill-changelog`.

```python
async def _generate_and_ingest_changelog(
    self,
    webflow_id: str,
    bill_title: str,
    bill_slug: str,
    jurisdiction: str,
    old_version: dict,      # full cached Redis entry (has text_url, version_date, version_note)
    new_version_date: str,
    new_version_note: str,
    new_content: str,       # already-extracted new bill text
) -> tuple[int, bool, str]:
    """
    Returns: (chunks_created, skipped, skip_reason)
    skipped=True if old text retrieval fails — caller logs warning and continues.
    """
```

Failure modes handled gracefully (all return `skipped=True`):
- Old URL fetch fails (404, timeout, non-200)
- Old URL returns non-text content
- OpenAI API error
- Extracted old text is too short to diff meaningfully (<500 chars)

#### Step 4 — Update `check_and_reingest_version()`

Modify to call the two new methods after a successful ingest:

```python
# After _ingest_bill_text() succeeds:

# 1. Store permanent history record
history_chunks = await self._ingest_bill_history(
    ...,
    content=extracted_content,   # returned from updated _ingest_bill_text()
)

# 2. Attempt changelog generation (only if previous version exists)
if cached:
    changelog_chunks, skipped, skip_reason = await self._generate_and_ingest_changelog(
        ...,
        old_version=cached,
        new_content=extracted_content,
    )
    if skipped:
        logger.warning("Changelog generation skipped", reason=skip_reason, ...)
```

#### Step 5 — Update `_ingest_bill_text()`

Return the extracted text content alongside the chunk count so the two new methods can reuse it without re-downloading. Current signature returns `int`; update to return `tuple[int, str]` (chunks_created, extracted_content).

#### Step 6 — Add `_delete_surplus_chunks()`

New method. Called after a successful upsert when `cached["chunk_count"]` > new chunk count. Deletes the exact excess chunk IDs from Pinecone by ID (no metadata scan).

Includes a defensive guard: if `old_chunk_count` is absent, zero, or implausibly large (> `new_chunk_count * 4`), the deletion is skipped and a warning is logged. This protects against a missing or corrupted Redis value wiping valid new chunks.

```python
async def _delete_surplus_chunks(
    self,
    document_id: str,           # e.g. "bill-pdf-{webflow_id}"
    old_chunk_count: int | None,
    new_chunk_count: int,
) -> int:
    """Delete chunk IDs from new_chunk_count up to old_chunk_count - 1.
    Returns number of chunks deleted. No-op if old_count <= new_count.
    Skips deletion if old_count is missing or implausibly large (> new_count * 4).
    """
    if not old_chunk_count:
        return 0
    if old_chunk_count > new_chunk_count * 4:
        logger.warning(
            "Skipping surplus chunk deletion — old_chunk_count implausibly large",
            document_id=document_id,
            old_chunk_count=old_chunk_count,
            new_chunk_count=new_chunk_count,
        )
        return 0
    if old_chunk_count <= new_chunk_count:
        return 0
    ids_to_delete = [
        f"{document_id}-chunk-{i}"
        for i in range(new_chunk_count, old_chunk_count)
    ]
    await self.vector_store.delete(ids=ids_to_delete)
    return len(ids_to_delete)
```

#### Step 7 — Store `chunk_count` in Redis version cache

Add `chunk_count` to the version data written to Redis after each ingest:

```python
version_data = {
    "version_date": ...,
    "version_note": ...,
    "text_url": text_url,
    "media_type": media_type,
    "chunk_count": new_chunk_count,   # ← new
    "last_checked": ...,
    "bill_slug": bill_slug,
}
```

On first ingest (no `cached`), no surplus deletion occurs. On subsequent ingests, `cached["chunk_count"]` is used to compute which IDs to delete.

#### Step 8 — Update batch logging

`sync_bill_versions()` summary log and Redis flow-status record should include `history_chunks_created`, `changelog_chunks_created`, `changelogs_skipped`, `surplus_chunks_deleted`.

#### Step 9 — Tests (`tests/test_bill_version_history.py`)

- `test_surplus_chunks_deleted_when_new_version_is_shorter` — cached chunk_count=5, new version produces 3 chunks; verifies chunks 3 and 4 are deleted from Pinecone by ID
- `test_no_deletion_when_new_version_is_longer` — cached chunk_count=3, new version produces 5 chunks; verifies no delete call is made
- `test_no_deletion_on_first_ingest` — no cached version; verifies no delete call is made
- `test_history_created_on_first_ingest` — no cached version; verifies `bill-text-history` document ingested
- `test_history_created_on_version_update` — cached version present; verifies both `bill-text-history` and `bill-changelog` ingested
- `test_changelog_skipped_on_stale_url` — old URL returns 404; verifies ingest succeeds, changelog skipped with warning
- `test_changelog_skipped_on_openai_error` — OpenAI call raises; verifies graceful degradation
- `test_versioned_document_ids_are_unique` — two version transitions produce non-colliding document IDs
- `test_chunk_count_written_to_redis_cache` — verifies `chunk_count` is persisted after ingest
- `test_batch_result_tracks_all_counts` — history_chunks, changelog_chunks, changelogs_skipped, surplus_deleted all reflected in batch result

---

### Phase 2 — VoteBot

**Files changed:**
- `src/votebot/core/retrieval.py` — new changelog retrieval phase
- `src/votebot/core/prompts.py` — changelog context instructions
- `src/votebot/utils/intent.py` — changelog intent keywords
- `tests/` — retrieval tests for changelog phase

#### Step 1 — Changelog intent detection (`intent.py`)

Add `changelog` sub-intent triggered by keywords: `changed`, `amended`, `amendment`, `difference`, `what's new`, `updated since`, `revision`, `compare`, `how has.*changed`, `what was added`, `what was removed`.

#### Step 2 — Changelog retrieval phase (`retrieval.py`)

Add Phase 5 to `_retrieve_bill_with_text_priority()`:

```python
# Phase 5: Changelog (if changelog intent detected)
if is_changelog_query:
    changelog_results = await self._retrieve_by_type(
        query=query,
        document_type="bill-changelog",
        webflow_id=page_context.webflow_id,
        top_k=3,                              # changelogs are concise; few chunks needed
    )
```

Result ordering for changelog queries:
```
changelog → bill-text → bill-summary → votes → org
```

For non-changelog queries, `bill-changelog` and `bill-text-history` are never retrieved — no change to existing retrieval behaviour.

#### Step 3 — Prompt update (`prompts.py`)

Add guidance to the bill context prompt for when changelog chunks are present:
- Cite the version transition explicitly (`From: X → To: Y`)
- If multiple changelogs are retrieved (multiple transitions), present them in chronological order
- Acknowledge if no changelog is available for a specific transition ("I have the bill text for both versions but no summary of changes for that specific update")

#### Step 4 — Tests

- Changelog intent detected for relevant keywords
- Changelog phase fires only on bill pages with changelog intent
- Changelog phase does not fire for non-changelog bill queries
- Result ordering is correct when changelog chunks present

---

## Operational Notes

**Pinecone growth:** At 3–8 version transitions per day across ~1,253 bills, history doc accumulation is slow (single-digit MB per year at current scale). No retention policy is required now. Revisit if total `bill-text-history` vector count exceeds ~5,000.

**Partial delete failure:** If the surplus chunk delete step fails (Pinecone error), log the specific IDs as an error and continue — the ingest is not rolled back. Orphans from a failed delete will be cleaned up on the next version transition for that bill.

**Concurrency:** ddp-sync runs as a single worker on EC2. No ingest mutex is required under the current deployment model. If multi-worker is ever introduced, a per-bill Redis lock should be added before the upsert-delete sequence.

**LLM changelog quality:** Before enabling Phase 2 (VoteBot changelog retrieval), manually spot-check the first 10–20 changelogs generated in production to verify accuracy. This is a named prerequisite for Phase 2 go-live, not a code requirement.

**Delete telemetry:** `surplus_chunks_deleted` is included in the batch result and summary log. If the implausibly-large guard fires, the warning is observable in structured logs. A formal metric or alert can be added later if the operational pattern warrants it.

---

## Out of Scope

- **Backfill**: Re-ingesting existing bills as `bill-text-history`. Only new version transitions (from ship date onward) will generate history records. A future one-off script can backfill using the Redis version cache's `text_url` if needed.
- **S3 document archive**: See "Why Not S3" above. Revisit when skip-rate data is available.
- **`bill-text-history` retrieval in VoteBot**: Phase 2 only adds changelog retrieval. Raw historical version retrieval (e.g. "show me the introduced text of S.2") is a future use case that the stored documents will support.
- **History for `bill-webflow-{webflow_id}` documents**: The CMS summary document (description, support/oppose arguments) is not versioned — only the legislative text is. CMS content changes are already reflected immediately via the existing Webflow sync.
- **Changelog generation for `bill-votes` documents**: Vote records are append-only by nature; versioning does not apply.

---

## Resolved Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Changelog format | Structured LLM summary (sections added/removed/modified + policy implications) | More predictable for RAG retrieval than narrative prose; VoteBot can cite sections directly |
| Changelog LLM | OpenAI `gpt-4o-mini` | Already a ddp-sync dependency; cost-efficient for structured diff task |
| History on first ingest | Yes | Completeness — history collection is accurate from day one, not only from first version transition |
| Backfill existing bills | No (out of scope) | Can be done later with a one-off script; not required for the feature to be useful |
| S3 document store | No (see above) | Unjustified complexity without failure-rate data |
| `bill-text-history` retrieval in VoteBot | Phase 2 out of scope | Changelog is the right first surface; raw history retrieval is a future use case |
