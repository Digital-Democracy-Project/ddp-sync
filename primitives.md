---
name: ddp-sync primitives & building blocks inventory
description: Catalog of every service, pipeline, dataclass, helper, and convention in the codebase. Read at the start of every PLAN session before designing new shapes.
type: reference
---

# READ THIS FIRST — BEFORE DESIGNING NEW PRIMITIVES

Before sketching new dataclasses, services, or helpers in any PLAN session, scan this file and grep the relevant module. The pattern to avoid: drafting a "new primitive" that duplicates something already at a known path.

```bash
grep -rn "class <Name>\|def <name>" src/ddp_sync/
```

---

## Pinecone layer (`services/vector_store.py`)

- **`VectorStoreService`** — Pinecone client. Lazy-initialized. Methods:
  - `upsert_documents(documents: list[Document], batch_size=100) -> int`
  - `query(query, top_k, filter, include_metadata) -> list[SearchResult]`
  - `query_with_filter(query, document_type, bill_id, legislator_id, jurisdiction, top_k) -> list[SearchResult]`
  - `delete(ids=None, filter=None, delete_all=False) -> None` — delete by ID list, metadata filter, or full namespace wipe
  - `health_check() -> bool`
- **`Document`** — `id, content, metadata, embedding`
- **`SearchResult`** — `id, content, score, metadata`
- **`VectorStoreServiceFactory.get_instance()`** — singleton accessor
- **Two indexes, two settings (SYNC-89, `PLAN-enterprise-search.md` §5.6).** `pinecone_index_name` (`votebot-large`) is the legacy path and is never changed by the new work. `knowledge_base_index_name` (env `KNOWLEDGE_BASE_INDEX_NAME`, default unset = new path disabled) names the NEW index `ddp-knowledge-base`. `config.knowledge_base_settings(settings)` returns a copy whose `pinecone_index_name` is the new index — hand *that* to `IngestionPipeline`/`VectorStoreService` for the new path; it raises if the setting is unset or equals the legacy index. `scripts/measure_kb_embedding_throughput.py` embeds N real archived documents into the new index only and deletes exactly what it wrote.

## Ingestion pipeline (`ingestion/pipeline.py`)

- **`IngestionPipeline`** — chunk → embed → upsert. Methods:
  - `ingest_document(content, metadata: DocumentMetadata, skip_duplicates=True) -> IngestionResult`
  - `ingest_batch(sources: list[DocumentSource], batch_size=10) -> IngestionResult`
  - `delete_document(document_id) -> bool` — calls `VectorStoreService.delete(filter={"document_id": ...})`
  - `reset_hash_cache()` — clears in-session duplicate detection
- **`IngestionResult`** — `documents_processed, chunks_created, chunks_upserted, errors, skipped`
- **`DocumentSource`** — `content: str, metadata: DocumentMetadata, content_hash`

## Document metadata (`ingestion/metadata.py`)

- **`DocumentMetadata`** — the universal metadata shape passed to every ingest call:
  - `document_id: str` — unique, deterministic (e.g. `bill-pdf-{webflow_id}`, `bill-text-history-{webflow_id}-{version_date}`)
  - `document_type: str` — controlled vocab, see Pinecone document types below
  - `source: str`, `title`, `jurisdiction`, `bill_id`, `legislator_id`, `url`
  - `extra: dict` — Pinecone metadata overflow (flattened via `to_dict()`)
  - `to_dict() -> dict` — converts to Pinecone-compatible flat dict; lists → comma-joined strings, nested dicts skipped
- **`MetadataExtractor`** — helper with `extract_bill_metadata()`, `extract_legislator_metadata()`, `extract_organization_metadata()`, `extract_web_content_metadata()`

## Pinecone document types (controlled vocabulary)

| `document_type` | `document_id` pattern | Created by |
|---|---|---|
| `bill` | `bill-webflow-{webflow_id}` | `WebflowSource._process_bill_item()` |
| `bill-text` | `bill-pdf-{webflow_id}` | `BillVersionSyncService._ingest_bill_text()` — overwritten each version |
| `bill-text-history` | `bill-text-history-{webflow_id}-{version_date}` | `BillVersionSyncService._ingest_bill_history()` — permanent |
| `bill-changelog` | `bill-changelog-{webflow_id}-{version_date}` | `BillVersionSyncService._generate_and_ingest_changelog()` — permanent |
| `bill-votes` | `bill-votes-{webflow_id}` | `BillSyncService.sync_bill()` |
| `legislator` | `legislator-{openstates_id}` | `WebflowSource._process_legislator_item()` |
| `legislator-votes` | `legislator-votes-{person_uuid}` | `LegislatorVotesBuilder` |
| `organization` | `organization-{webflow_id}` | `WebflowSource._process_organization_item()` |
| `training` | `training-{filename_stem}` | `IngestionPipeline._ingest_training_docs()` |
| `bill-text` (new index only) | `bill-text:{ocd_bill_id}:{archived_document_id}` — one per version | `KnowledgeBaseEmbedder.embed_bill()` (SYNC-83) |
| `bill-version-diff` (new index only; **not written** — Ramon decided 2026-10-05 diffs are out; reachable only via an explicit `EmbedScope(diffs=True)`) | `bill-version-diff:{ocd_bill_id}:{archived_document_id}` | `KnowledgeBaseEmbedder.embed_bill()` — api-v3's `diff_from_previous_version`, verbatim |
| `organization` (new index only) | `organization:{broker_org_id}` | `knowledge_base_entities.embed_organizations()` (SYNC-91) — ddp-broker-py `/api/organizations/`, never Webflow |

**Retrieval isolation**: `bill-text-history` and `bill-changelog` are structurally invisible to VoteBot's normal retrieval phases (which filter by explicit `document_type`). Do not add unfiltered fallback queries.

**LegBot's `BillArtifact` generation does NOT use this pipeline** (`pipelines/bill_artifact_generation.py` — `generate_and_store_bill_artifact`/`generate_and_store_bill_changelog`, ddp-infra's `PLAN-bill-document-provenance.md` Phase 8). Decided 2026-08-10 (Ramon): LegBot's output already lands in queryable `BillArtifact` rows VoteBot can read directly, so re-embedding LegBot's own summary into Pinecone would duplicate the original "embed the full bill document" design intent for a different purpose (deep full-text query, not structured artifact lookup) without serving either well. These two functions used to call `IngestionPipeline`/`DocumentMetadata` directly (document types `bill-artifact-{artifact_type}` / `bill-artifact-bill_changelog`, never added to the table above) — removed, not just made optional. If VoteBot needs full-bill-text Pinecone search over archived bill text, that belongs in a new task or an extension of `ddp-open-states`' bill archiver (which already owns the full archived text), reusing `IngestionPipeline`/`DocumentMetadata` below — not revived inside LegBot's artifact-generation path.

## Knowledge-base embedding hook (`pipelines/knowledge_base_embedding.py`, SYNC-83)

Post-archive hook, independent of the LegBot one: `openstates_archive._maybe_embed_knowledge_base` (gated by `openstates_archive.knowledge_base_embedding` in `sync_schedule.yaml`, default `enabled: false`, **and** `knowledge_base_index_name` being set) → `embed_archived_bills(jurisdiction, archive_started_at, ...)` → `KnowledgeBaseEmbedder.embed_bill(ocd_bill_id, jurisdiction, bill)`. Writes ONLY the new index (its settings come from `knowledge_base_settings`). Reads api-v3 (`local_openstates_client.list_touched_bill_ids` / `fetch_bill_for_embedding`), never a live URL, and never classifies, sorts or diffs — it carries api-v3's `version_stage`, `version_ordinal`, array order and `diff_from_previous_version`. Needs an api-v3 that exposes `archived_document_id` (companion change; without it the hook logs `knowledge_base_embedding_work_left_undone` and embeds nothing for that version).

- Cache `ddp:bill_version:{ocd_bill_id}` (`schema` 2: per-document `text_hash`/`chunks`/`diff_hash`/`diff_chunks`; an older entry may still carry an ignored `votes` field, SYNC-94 did not bump the schema so nothing already embedded is redone) — same prefix as the legacy webflow-keyed entries, disjoint keys. Written only after every Pinecone write for the bill succeeded (`RedisStore.set_bill_version` now returns a bool).
- Per-jurisdiction watermark `ddp:kb_embed:since:{jur}` (`RedisStore.get/set_kb_embed_watermark`): advanced only by a run that left nothing undone, so the next run rescans from it -- the repair path for bills that were *touched*.
- **Reconcile (SYNC-95): the repair path for bills nobody touched.** `embed_archived_bills(..., reconcile_max_bills=N)` (from `openstates_archive.knowledge_base_embedding.reconcile.max_bills_per_run`, default `0` = off) also lists every bill api-v3 has an archived document for (`list_touched_bill_ids(since=epoch)`), drops the ones this run already handled, asks Redis which have **no** `ddp:bill_version:<ocd_bill_id>` entry (`RedisStore.find_unrecorded_bill_versions`, which returns `None`, never "all missing", when Redis cannot answer), and embeds up to N of them (bills *attempted*; a random slice when more are missing, so a bill that always fails cannot hold a slot) through the same `embed_bill`; the listing and Redis check are not capped. It is the LegBot pattern (read what is already covered, dispatch only what is missing): the record is the cursor, so the next run takes the next slice and a new state fills itself. A bill with nothing embeddable yet gets an empty record so it is not re-read every night; a bill that fails keeps none and is found again. Its result is `totals["reconcile"]`; it never affects `complete` or the watermark. **The version cache is therefore the record of what is embedded**: the knowledge-base path writes its bill entries with `set_bill_version(..., persistent=True)` (no expiry; the legacy webflow-keyed entries keep their 90 days) and `find_unrecorded_bill_versions` makes still-expiring entries persistent as it goes. **Orphans (SYNC-95, `knowledge_base_embedding.delete_orphans`, default off, literal `true` only):** `embed_bill(..., max_orphans=N)` (N is a budget; `None` = off) deletes, by exact id (`<document key>-chunk-<n>`, from the chunk counts it recorded, never a prefix or metadata scan), the vectors of up to N recorded documents whose archived id api-v3 no longer returns for any version of the bill (the picker re-chose a row, a version was removed, or its row has no usable text), and drops them from the record. api-v3 returns every version of a bill in one detail response, each with the id its picker chose, so an id missing from a non-empty response is really gone; a response with no archived ids removes nothing; a version returned with an id but no text is not an orphan. Each deletion re-reads the record first and skips a document another writer changed (`raced`), and the final merge only drops an entry that still equals what was deleted; this narrows the race, it does not close it. A Pinecone failure keeps the record so the next run retries (deleting an absent id is a no-op). `embed_archived_bills` gives each bill only what is left of `MAX_ORPHAN_DOCUMENTS_PER_RUN` (200): the run never deletes more, the rest stay recorded, `totals["orphans_over_budget"]` counts them and `knowledge_base_orphan_removal_paused` is logged. It only sees bills the run touches. Not `delete_surplus_chunks`, whose guard refuses a delete down to zero chunks. **Dry run:** `plan_reconcile(jurisdiction, ...)` (route `POST /trigger/knowledge-base-reconcile/{jurisdiction}`) lists, counts the unrecorded bills (`find_unrecorded_bill_versions(..., persist_expiring=False)`, so it only reads), reads a random sample of 20 of them and extrapolates documents, chunks, tokens and dollars (4 characters per token, 0.279 vectors per 1,000 characters, $0.13 per million tokens: the SYNC-89 measurements and list price; an order of magnitude, since bill sizes vary a hundredfold). Limits, on purpose: it works per bill (a bill with a record is never re-checked for missing *documents*), and a Redis flush makes every bill look unembedded (bounded by the cap; a durable record such as an RDS table or Pinecone's own id list is the open alternative).
- **Ledger reconcile (SYNC-95 / OPEN-319): one loop instead of the watermark.** `embed_archived_bills(..., ledger_max_bills=N)`
  (from `knowledge_base_embedding.ledger.max_bills_per_run`; 0 = off) calls `_reconcile_from_ledger`: `local_openstates_client.list_embedding_ledger`
  pages `GET /ddp/embedding/ledger` into `{ocd_bill_id: {archived_id: updated_at}}`, `RedisStore.get_bill_versions` reads the records in
  a pipelined multi-get, `ledger_findings` lists the bills with a `missing`, `changed` (stamp differs or is absent) or `orphaned` document, and a random
  slice of at most N is read and given to `embed_bill`, which now also stamps `source_updated_at` on each in-sync document. No
  watermark key is read or written, `complete` means the whole ledger was read and nothing disagreeing was left, and an unreadable ledger falls
  back to the watermark path. `plan_reconcile(use_ledger=True)` / `?ledger=true` is its dry run.
- Surplus chunks: `bill_version.delete_surplus_chunks(settings, document_id, old, new)` (module-level; `BillVersionSyncService._delete_surplus_chunks` delegates to it).
- Always `skip_duplicates=False`: a content-hash skip would leave a version with no vectors while the cache says it is embedded.
- Not embedded: any LegBot/`BillArtifact` output, `bill-changelog`, and **votes** (SYNC-94: structured data that changes regularly; Votebot reads it from the vote records, and the legacy `bill-votes-{webflow_id}` documents are unchanged). Not yet written: `is_ddp_curated` / `ddp_url` (needs a ddp-broker-py read).


## Knowledge-base backfill (`pipelines/knowledge_base_backfill.py`, SYNC-90)

Walks a jurisdiction's whole archived corpus into the new index through the SAME `KnowledgeBaseEmbedder.embed_bill` the live hook uses, narrowed by `knowledge_base_embedding.EmbedScope(text="all"|"current"|None, diffs)` (default = text for every version, no diffs, no votes). **Do not write a second embedding path**: add a stage as a new `STAGE_SCOPES` entry. Default stages (`DEFAULT_STAGES`) are `current`, `prior-sessions`, `history`; `STAGES` also lists `diffs`, which no default run includes and must not be run. In order (`STAGES`): `current` (current-session bills' current version), `diffs`, `prior-sessions` (prior-session bills' current version), `history` (every older version). Bills are listed with `local_openstates_client.list_touched_bill_ids(since=epoch, session=...)`, the current session comes from `OpenStatesSource.get_current_session_identifier`. Resumable via a Redis checkpoint per jurisdiction and stage (`redis_store.get/set/delete_kb_backfill_checkpoint`: last bill id, failed ids retried first, cumulative totals, `done`); a done stage is a no-op unless `restart`; one run per jurisdiction via a renewed lease (`scraper_triggered_legbot._lock_heartbeat_loop`); sequential; pauses through the UTC blackout window `openstates_archive.knowledge_base_embedding.backfill` (default 04:45-07:00); aborts a stage after 200 failed bills. `embed_bill` re-reads a document's cache entry right before writing it and skips (`raced`) one another writer changed, and merges its cache write onto a fresh read, so a backfill never overwrites a live write. Manual entry: `POST /trigger/knowledge-base-backfill/{jurisdiction}?stage=&dry_run=true&restart=false` (202, results in the log: `knowledge_base_backfill_dry_run` / `_progress` / `_stage_complete` / `_stage_incomplete` / `_aborted`). Needs `KNOWLEDGE_BASE_INDEX_NAME`, an api-v3 exposing the OPEN-311 fields, and the jurisdiction in `knowledge_base_embedding.jurisdictions` (the `enabled` flag only gates the live hook).

## Bill-search refresh hook (`pipelines/bill_search_refresh.py`, SYNC-87)

Post-archive hook, independent of the LegBot and embedding hooks: `openstates_archive._maybe_refresh_bill_search` (gated by `openstates_archive.bill_search_refresh` in `sync_schedule.yaml`, default `enabled: false`) → `refresh_bill_search(jurisdiction, api_base=, api_key=)`, which loops `POST /ddp/search/refresh?jurisdiction=&limit=200` on api-v3 (header `x-api-key`) while the response says `more`, retries `busy` 3 times 20 s apart, and logs `bill_search_refresh_incomplete` (WARNING, never raises) whenever the run was not drained. **Always the RDS-backed api-v3** (`settings.rds_openstates_api_base`/`rds_openstates_api_key`), never `local_openstates_api_base`: the `ddp_bill_search` table exists only there. It runs before the embedding hook so search does not wait for a long embed. A refresh only rebuilds changed rows, so a partial run is repaired by the next archive run.

## Bill version pipeline (`pipelines/bill_version.py`)

The daily bill sync entry point. **Do not reinvent these methods.**

- **`BillVersionSyncService`** — detects new bill versions and runs two independent write paths.
  - `sync_bill_versions(bills, heartbeat_callback) -> VersionSyncBatchResult` — batch entry point (Flow 1 + Flow 2)
  - `sync_bill_statuses(bills, all_sessions, jurisdiction, heartbeat_callback) -> VersionSyncBatchResult` — Flow 1 only (Webflow CMS status, no Pinecone)
  - `check_and_update_bill(webflow_id, bill_title, jurisdiction_code, openstates_url, bill_slug, fields) -> VersionCheckResult` — single-bill entry point
  - `update_bill_status(webflow_id, ...) -> dict` — Flow 1 write: PATCH Webflow CMS status/status-date/status-chamber/gov-url
  - `check_and_reingest_version(webflow_id, ...) -> dict` — Flow 2 write: version cache check → ingest → history → changelog
  - `_ingest_bill_text(...) -> tuple[int, str]` — returns `(chunks_created, extracted_content)`. Note return type — callers reuse `extracted_content` to avoid re-downloading
  - `_delete_surplus_chunks(document_id, old_chunk_count, new_chunk_count) -> int` — upsert-then-delete; guards against `old_chunk_count > new_chunk_count * 4`
  - `_ingest_bill_history(..., content: str) -> int` — ingests already-extracted text as `bill-text-history`
  - `_generate_and_ingest_changelog(...) -> tuple[int, bool, str]` — `(chunks_created, skipped, skip_reason)`; fails gracefully on stale URL / OpenAI error
  - `_is_newer_version(latest_version, cached) -> bool` — compares date, note, URL against Redis cache
  - `_get_latest_version(versions) -> dict | None`
  - `_get_best_text_url(version) -> tuple[str, str] | None` — returns `(url, media_type)`, prefers PDF over HTML
  - `_dates_match(cms_date, openstates_date) -> bool` — normalises to YYYY-MM-DD before comparing

- **`VersionCheckResult`** — single-bill outcome: `webflow_id, bill_title, jurisdiction, status, version_note, version_date, text_url, chunks_created, history_chunks_created, changelog_chunks_created, changelog_skipped, changelog_skip_reason, surplus_chunks_deleted, webflow_updated, status_updated, webflow_patch_skipped, error`
- **`VersionSyncBatchResult`** — batch outcome: `total_bills, checked, updated, unchanged, no_versions, skipped, failed, chunks_created, history_chunks_created, changelog_chunks_created, changelogs_skipped, surplus_chunks_deleted, webflow_updates, status_updates, webflow_skipped, webflow_patch_failures, skipped_no_url, skipped_not_current, skipped_jurisdiction, errors`

## Bill sync service (`pipelines/bill_sync.py`)

- **`BillSyncService`** — OpenStates fetch + vote ingestion. Used by `BillVersionSyncService` internally; also called directly for backloads.
  - `sync_current_session_bills(bills, heartbeat_callback) -> SyncBatchResult`
  - `backload_all_bills(bills) -> SyncBatchResult` — ignores session filter
  - `sync_bill(openstates_url, webflow_bill_id, bill_title, jurisdiction_name, bill_slug) -> BillSyncResult`
  - `fetch_bill_from_openstates(jurisdiction, session, bill_id) -> dict | None` — rate-limited, retried; includes all `?include=` params. Routes to the local OpenStates replica (`settings.local_openstates_api_base`/`local_openstates_api_key`) instead of the public API (`settings.openstates_api_base`/`openstates_api_key`) when `jurisdiction` is in `settings.ddp_openstates_jurisdictions` (env `DDP_OPENSTATES_JURISDICTIONS`) — mirrors ddp-broker-py's `OpenStatesService._get_client_for_jurisdiction()`; see `_get_api_base_and_key(jurisdiction)` (SYNC-6)
  - `parse_openstates_url(url) -> OpenStatesUrl | None`
  - `resolve_jurisdiction_code(jurisdiction_id, openstates_url) -> str` — JURISDICTION_MAP first, OpenStates URL fallback
  - `is_current_session_async(session_year, session_code, jurisdiction) -> bool` — prefers live OpenStates session data
  - `should_sync_jurisdiction(jurisdiction_code) -> bool` — always True for US; checks legislative calendar for states; Monday fallback for out-of-session
  - `format_bill_votes_chunk(bill_data, ddp_url) -> tuple[str, dict] | None`
  - `get_jurisdiction_info(jurisdiction) -> JurisdictionInfo | None` — cached
  - `_apply_rate_limit()` — alias for `self.rate_limiter.apply()`
- **`BillSyncResult`** — `bill_id, jurisdiction, success, chunks_created, error`
- **`SyncBatchResult`** ⚠️ — `total_bills, successful, failed, chunks_created, errors`. **Name collision**: `pipelines/legislator_sync.py` also defines a class called `SyncBatchResult` with different fields. Always import from the correct module. Don't add a third `SyncBatchResult` — rename any new one to something specific (e.g. `OrgSyncBatchResult`).
- **`OpenStatesUrl`** — `jurisdiction, session, bill_id, original_url`

## Legislator sync service (`pipelines/legislator_sync.py`)

- **`LegislatorSyncService`** — legislator sponsored-bills + voting-record sync (`legislator-bills`/`legislator-votes` documents). Key methods: `fetch_sponsored_bills(person_id, jurisdiction, ...)`, `fetch_legislator_votes(person_id, jurisdiction, session, ...)`, `sync_legislator(legislator, ...)`, `sync_all_legislators(legislators, ...)`. `_get_sponsor_name(person_id, jurisdiction=None)` (SYNC-78) routes to the local OpenStates replica the same way as the other two methods when `jurisdiction` is passed and covered — `fetch_sponsored_bills` passes its own `jurisdiction` through at the call site; omitted (`None`), it falls back to the public API, same as before SYNC-78. `_ocd_person_id(person_id)` (SYNC-77, module-level function) normalizes the CMS's bare-UUID `openstates_id` to OpenStates' `ocd-person/{uuid}` form — applied at all three OpenStates-facing call sites (`_get_sponsor_name`, `fetch_sponsored_bills`, `fetch_legislator_votes`); document IDs/`legislator_id` metadata elsewhere stay on the bare UUID by convention, this only normalizes the request/comparison boundary. `_get_api_base_and_key(jurisdiction) -> tuple[str, str, bool]` routes to the local OpenStates replica when `jurisdiction` is in `settings.ddp_openstates_jurisdictions` — mirrors `BillSyncService._get_api_base_and_key()` (SYNC-6/SYNC-8).

## Knowledge-base organizations (`pipelines/knowledge_base_entities.py`, SYNC-91)

One standalone document per organization into the new index, through the same `KnowledgeBaseEmbedder` as the bills via `embed_entity(key, content, metadata, dry_run=)` (content-hash skip, surplus-chunk cleanup, refuses the legacy index; the cache is `ddp:bill_version:<document id>` with `schema: 2`, `hash`, `chunks`, advanced only after the write). `embed_organizations(...)` reads ddp-broker-py `broker_client.list_organizations` / `get_organization` (BROKER-144's public `/api/organizations/`), key `organization:<broker id>`, **never Webflow** (do not add a Webflow path); an organization with no description and no focus areas is skipped. Organizations are not per jurisdiction, so they only run from `POST /trigger/knowledge-base-entities/organizations?dry_run=true` (202, totals in the log: `knowledge_base_entities_run` / `_incomplete`, per-entity `knowledge_base_entity_undone`); any other entity name is a 404. **Legislators are not embedded (SYNC-94, Ramon 2026-10-02)**: their structured facts come from api-v3 directly and DDP has no narrative biography to embed; the legacy `legislator-bills`/`legislator-votes` documents (`LegislatorSyncService`) stay on `votebot-large` and are unrelated.

## Rate limiter (`services/rate_limiter.py`)

- **`RateLimiter`** — `asyncio.Lock`-guarded token-bucket. Method: `apply() -> None` (sleeps if needed). `enforced_sleeps` counter for observability.
- **`RateLimitConfig`** — `requests_per_minute, delay_between_requests, max_retry_attempts, retry_backoff_seconds`. Factory: `RateLimitConfig.from_yaml(config_path)` — reads `rate_limit:` block from `sync_schedule.yaml`. **Never construct inline** — use `from_yaml`.

## Redis store (`services/redis_store.py`)

Singleton: `get_redis_store() -> RedisStore`. All methods no-op gracefully when Redis is down.

- **`set_bill_version(webflow_id, data)` / `get_bill_version(webflow_id)`** — 90-day TTL. Data shape: `{version_date, version_note, text_url, media_type, chunk_count, last_checked, bill_slug}`. `chunk_count` added 2026-06-06 for surplus chunk deletion.
- **`set_bill_status(webflow_id, data)` / `get_bill_status(webflow_id)`** — 90-day TTL. Data shape: `{status, status_date, status_chamber, gov_url, last_synced}`.
- **`set_flow_status(flow_name, data)` / `get_flow_status(flow_name)`** — 7-day TTL. Records run outcomes for `/health` endpoint.
- **`add_active_jurisdiction(code)` / `get_active_jurisdictions()`** — set at `ddp:active_jurisdictions`.
- **`publish(channel, message) -> int`** — fire-and-forget pub/sub. Returns subscriber count.
- **`add_sync_checkpoint(task_id, item_id)` / `get_sync_checkpoints(task_id)` / `copy_sync_checkpoints(from, to)`** — crash-resume support.

Redis key constants (import, don't hardcode):
- `BILL_VERSION_PREFIX = "ddp:bill_version:"`
- `BILL_STATUS_PREFIX = "ddp:bill_status:"`
- `FLOW_STATUS_PREFIX = "ddp:flow:"`
- `ACTIVE_JURISDICTIONS_KEY = "ddp:active_jurisdictions"`

Pub/sub channels (hardcoded strings in callers):
- `"votebot:cache:invalidate"` — published after successful bill text re-ingestion; payload `{slug, reason, version_note}`
- `"votebot:eval:running"` — votebot eval concurrency lock

## Webflow write service (`services/webflow_lookup.py`)

- **`WebflowLookupService`** ⚠️ — **Mixed read/write despite "Lookup" name.** Primary purpose is CMS PATCHes, but also exposes `get_legislator_details()` as a read path. Don't let the name mislead you into thinking reads need to go elsewhere — this class handles both. VoteBot has its own `WebflowLookupService` that is read-only; the two share a name but have entirely different method sets.
  - `update_bill_fields(webflow_id, field_data, api_key=None) -> bool`
  - `update_legislator_fields(webflow_id, field_data, api_key=None) -> bool`
  - `create_legislator_draft(field_data, api_key=None) -> WebflowCreateResult`
  - `get_legislator_details(slug) -> LegislatorDetails` — read path used by VoteBot for slug→ID resolution
  - Has rate limiter + 429/Retry-After handling via `WebflowRateLimitError`
- **`WebflowError`** / **`WebflowRateLimitError`** — exception hierarchy
- **`WebflowPatchResult`** — `success, status_code, response_body`
- **`WebflowCreateResult`** — `success, item_id, error`

## Webflow assets service (`services/webflow_assets.py`)

- **`WebflowAssetService`** — two-step Webflow Assets v2 API. Requires `webflow_assets_read_write_key` (separate from the CMS token — do not use `webflow_api_token`).
  - `upload_image(image_url, filename, webflow_item_id, ...) -> AssetReference`
- **`AssetReference`** — `asset_id, url, file_name`
- **`WebflowAssetError`** — raised on upload failure

**Token split rule**: `webflow_api_token` has `cms:*` scope only. `webflow_assets_read_write_key` has `assets:*` scope only. They are not interchangeable.

## Webflow CMS client (`webflow_cms/client.py`)

- **`WebflowClient`** — low-level paginated fetcher for the CMS. Used by the `webflow_cms/` services subpackage.
- **`webflow_cms/models.py`** — shared result shapes: `UpdateResult, DeleteResult, FillResult, SyncResult, MergeResult, DuplicateGroup`
- **`webflow_cms/exceptions.py`** — `WebflowCMSError, WebflowAPIError, WebflowConflictError, ConfigurationError, ParseError`

## Webflow CMS batch services (`webflow_cms/services/`)

- **`BillOrgSyncService`** — syncs organization references on bill items
- **`OrgMergeService`** — deduplicates Member Organizations by name (weekly cron). `run_merge() -> MergeResult`
- **`DuplicateBillsService`** — detects and reports duplicate bills
- **`SessionCodeService`** — fills `session-code` field from OpenStates URL
- **`MapUrlService`** — fills `map-url` field
- **`GovUrlService`** — fills `gov-url` field
- **`DeleteItemService`** — deletes CMS items by ID

## Ingestion sources (`ingestion/sources/`)

- **`WebflowSource`** — fetches from Webflow CMS. Key methods:
  - `_process_bill_pdf(pdf_url, fields, item_id) -> DocumentSource | None`
  - `_process_bill_html(html_url, fields, item_id) -> DocumentSource | None`
  - `_get_url_content_type(url) -> str | None` — returns `"pdf"`, `"html"`, or `None`
  - `fetch_item_by_id(collection_id, item_id) -> dict | None`
  - `_process_bill_item(item, include_pdfs) -> AsyncIterator[DocumentSource]`
  - `_process_legislator_item(item) -> DocumentSource | None`
  - `_process_organization_item(item) -> DocumentSource | None`
- **`OpenStatesSource`** — general ingestion pipeline (jurisdiction/session fetch, bill fetch, legislator fetch/batch-fetch). Key methods:
  - `fetch_jurisdiction(jurisdiction) -> JurisdictionInfo | None`
  - `fetch(jurisdiction, session, limit) -> AsyncIterator[DocumentSource]` — bill search + detail
  - `fetch_legislators(jurisdiction, limit) -> AsyncIterator[DocumentSource]`
  - `fetch_bill(bill_id) -> DocumentSource | None` / `fetch_legislator_by_id(person_id) -> DocumentSource | None` — single opaque-ID lookups, always public API (no jurisdiction to route on)
  - `fetch_legislators_batch(person_ids) -> AsyncIterator[DocumentSource]`
  - `_get_api_base_and_key(jurisdiction) -> tuple[str, str, bool]` — routes `fetch_jurisdiction`/`fetch`/`fetch_legislators` to the local OpenStates replica (`settings.local_openstates_api_base`/`local_openstates_api_key`) instead of the public API (`settings.openstates_api_base`/`openstates_api_key`) when `jurisdiction` is in `settings.ddp_openstates_jurisdictions` — mirrors `BillSyncService._get_api_base_and_key()` (SYNC-6/SYNC-8)
- **`JurisdictionInfo`** — `jurisdiction_id, name, sessions: list[LegislativeSession], latest_bill_update`. Method: `get_current_session() -> LegislativeSession | None`
- **`LegislativeSession`** — `identifier, name, start_date, end_date, active`
- **`PDFSource`** — `process_url(url, max_pages) -> DocumentSource | None`
- **`ChunkingService`** — `chunk_text(content, metadata_dict) -> list[Chunk]`. `Chunk`: `content, index, metadata`

## Embeddings service (`services/embeddings.py`)

- **`EmbeddingService`** — OpenAI `text-embedding-3-large` (3072-dim). Methods:
  - `embed_documents(texts) -> list[list[float]]`
  - `embed_query(text) -> list[float]`
  - `EmbeddingService.get_dimension() -> int` — returns 3072; use this, don't hardcode
- **`EmbeddingResult`** — `embedding, tokens_used, model`

## Legislative calendar (`services/legislative_calendar.py`)

- **`StateLegislativeCalendar`** — `is_in_session(state_code) -> bool` (raises `ValueError` for unknown states); `warm_cache(jurisdiction_data: dict[str, JurisdictionInfo])` — pre-loads live OpenStates session data into the calendar before batch processing.

## OpenStates People client (`services/openstates_people.py`)

- **`OpenStatesPeopleClient`** — person lookups for bio sync. Methods: `fetch_by_id(openstates_id) -> OpenStatesPerson | None` (single opaque-ID lookup, always public API — no jurisdiction to route on), `iter_jurisdiction(jurisdiction) -> AsyncIterator[OpenStatesPerson]`. Deliberately thin — no `Settings` dependency; takes `openstates_api_base`/`local_openstates_api_base`/`local_openstates_api_key`/`ddp_openstates_jurisdictions` as constructor kwargs instead. `_get_api_base_and_key(jurisdiction) -> tuple[str, str, bool]` routes `iter_jurisdiction` to the local OpenStates replica when `jurisdiction` is in `ddp_openstates_jurisdictions` — mirrors `BillSyncService._get_api_base_and_key()` (SYNC-6/SYNC-8). Callers: `pipelines/legislator_bio.py`'s `BioSyncOrchestrator`, `scripts/backfill_legislator_party.py`.
- **`OpenStatesPerson`** — bio data shape
- **`OpenStatesError`** / **`OpenStatesRateLimitError`** — exception hierarchy

## Congress legislators source (`services/congress_legislators.py`)

- **`CongressLegislatorsSource`** — pre-warmed at app startup (reads 8.6 MB unitedstates YAML). Accessed from `app.state.congress_legislators`. **Do not re-read the YAML** — always pass the pre-warmed instance.
- **`CongressLegislator`** — bio data shape

## Legislator bio pipeline (`pipelines/legislator_bio.py`)

- **`LegislatorBioPipeline`** — orchestrates bio sync. Entry: `run(options: BioSyncOptions) -> BioSyncReport`
  - `audit_federal_join_keys() -> AuditReport` — Audit A
  - `audit_bulk_import_readiness() -> AuditReport` — Audit B
  - `audit_state_join_keys(jurisdiction) -> AuditReport` — Audit C
- **`BioSyncOptions`** — `target, jurisdiction, auto_create, dry_run, limit, historical_since, strict_schema, upload_photos, upload_photos_dry_run`
- **`BioSyncReport`** — run summary with Zapier-formatted fields
- **`CMSLegislator`** — current CMS record shape (read side)
- **`AuditEntry`** / **`AuditReport`** — audit output shapes

## Votebot eval pipeline (`pipelines/votebot_eval.py`)

- **`run_votebot_eval(days, yaml_config, trigger) -> dict`** — main entry point (shared by cron + manual trigger)
- **`detect_regressions(metrics, thresholds, last_run) -> list[dict]`**
- **`push_eval_alert(headline, regressions, ...) -> bool`**
- Metric string constants (pinned — external log monitoring depends on these):
  - `METRIC_RUN_COMPLETED = "votebot_eval.scheduled_run_completed"`
  - `METRIC_REGRESSION = "votebot_eval.regression_detected"`
  - `METRIC_RUN_FAILED = "votebot_eval.run_failed"`
  - `METRIC_ALERT_SENT = "votebot_eval.alert_sent"`
  - `METRIC_ALERT_SKIPPED = "votebot_eval.alert_skipped"`
  - `METRIC_ALERT_FAILED = "votebot_eval.alert_failed"`
  - `METRIC_UNKNOWN_YAML_KEY = "votebot_eval.unknown_yaml_key"`
- Redis lock keys: `LOCK_KEY = "votebot:eval:running"`, `LAST_RUN_KEY = "votebot:eval:last_run"`

## Slack alerts (`slack_alerts.py`, `slack_identity.py`, SYNC-99)

**Every alert posted to Slack with `chat.postMessage` goes through one function. Don't write another `requests.post` to it.**

- **`post_alert(text, *, source, channel=None) -> bool`** (`slack_alerts.py`) — posts to the alerts channel
  (`HEALTH_ALERT_SLACK_CHANNEL`, default `#automation-errors`) with `SLACK_BOT_TOKEN`, 15s timeout, **as CodeBot**.
  Never raises; returns whether Slack accepted it. `source` (e.g. `"openstates_scrape.scrape_failure"`) names the
  caller in the log line. A missing token or a refused post logs `slack_alert_not_sent_no_token` /
  `slack_alert_failed` / `slack_alert_error` (with `source`) instead of a per-site event.
- **`codebot_identity() -> dict`** (`slack_identity.py`) — the `username` / `icon_emoji` fields. Mirrors
  ddp-agents' `cams.slack_identity.identity_kwargs("codebot")` (this service can't import `cams`): same
  `CODEBOT_SLACK_USERNAME` / `CODEBOT_SLACK_ICON_EMOJI` variables, same defaults (`CodeBot`, `:robot_face:`), and an
  empty value means the default. Only `post_alert` should call it.
- **Why one function:** a payload with only `channel` and `text` makes Slack use the Slack app's own name, so the alert
  posts as **Agent Smith** instead of CodeBot. Setting the identity at each of six call sites fixed that once, but a
  seventh alert could copy the old payload; one function can't. `tests/test_slack_identity_alerts.py` **fails if any
  other module under `src/` contains the Slack post URL**.
- **Callers:** `_alert_scrape_failure`, `_alert_sustained_block`, `_alert_quiet_jurisdiction`
  (`pipelines/openstates_scrape.py`), `_alert_archive_failure` and the knowledge-base `_post_slack_alert`
  (`pipelines/openstates_archive.py`), `push_health_alert` (`pipelines/api_health_check.py`).
- **Not covered:** alerts relayed through **Zapier webhooks** (`push_eval_alert`, the bio-sync and voatz-brevo
  summaries, `bill_org_sync`) never call `chat.postMessage`; their sender identity is set in the Zap, and the guard
  test does not see them.
- Needs the `chat:write.customize` scope on the Slack app; without it Slack ignores the two fields and the post still
  succeeds under the default name. On the Mac, `scripts/start-ddp-sync.sh` copies the two `CODEBOT_*` variables from
  `ddp-agents/.env` (the icon is configured only there; PR #195); EC2 does not run that script and gets them only from its compose environment (per PR #195; not checked on the host).
- Mirrors in other repos: `ddp-open-states/lib/slack-alert.sh::post_slack_alert` (shell scripts) and the persona
  registry in ddp-agents (`cams/slack_identity.py`). Change a default name/icon/variable in one, change all three.
- **Merged 2026-10-06:** PRs #194 (identity), #195 (start script) and #196 (this single function). Not yet verified against live Slack.

## Scheduler (`scheduler.py`)

- **`UpdateScheduler`** — APScheduler orchestrator. Singleton: `get_scheduler() -> UpdateScheduler | None`.
  - `start()` / `stop()`
  - `trigger_openstates_sync(force_all, webflow_only) -> dict` — manual bill sync
  - `trigger_bill_status_sync(all_sessions, jurisdiction) -> dict` — Flow 1 only
  - `_fetch_webflow_bills() -> list[dict]` — paginated Webflow CMS fetch (all bills)
  - `_run_daily_bill_sync() -> dict` — runs Flow 1 + Flow 2 based on `bill_sync` config block
- **`UpdateSchedulerFactory.get_instance()`** — singleton accessor

## Sync types (`sync/types.py`)

- **`ContentType`** enum — `BILL, LEGISLATOR, ORGANIZATION, WEBPAGE, TRAINING`
- **`SyncMode`** enum — `SINGLE, BATCH`
- **`SyncTarget`** enum — `ALL, WEBFLOW, PINECONE`
- **`SyncOptions`** — `content_type, mode, target, jurisdiction, limit, include_pdfs, include_openstates, all_sessions, slug, webflow_id, resume_task_id`
- **`SyncIdentifier`** — `slug, webflow_id, url`
- **`SyncResult`** — `success, items_processed, items_successful, items_failed, chunks_created, errors`

## Federal legislator cache (`sync/federal_legislator_cache.py`)

- **`FederalLegislatorCache`** — in-memory cache of 538 Congress members. `lookup_with_info(name) -> dict | None` — returns `{person_id, name, party, state}`. Used to enrich federal vote records with stable OpenStates person IDs. `refresh()` always fetches jurisdiction "us"; `_get_api_base_and_key("us") -> tuple[str, str, bool]` routes it to the local OpenStates replica instead of the public API when `"US"` is in `settings.ddp_openstates_jurisdictions` — mirrors `BillSyncService._get_api_base_and_key()` (SYNC-6/SYNC-8).

## Webflow API tokens (two, not interchangeable)

| Setting key | Scope | Used by |
|---|---|---|
| `webflow_api_token` | `cms:read cms:write` | All CMS PATCHes (bills, legislators, orgs) |
| `webflow_assets_read_write_key` | `assets:read assets:write` | Photo upload pipeline only |

Use `settings.webflow_scheduler_api_key` for scheduled write operations (has broader CMS write permissions than `webflow_votebot_api_key` which is read-mostly).

## Trigger endpoints (`api/routes/triggers.py`)

| Endpoint | Method | Description |
|---|---|---|
| `POST /trigger/bill-version-check` | Calls `scheduler.trigger_openstates_sync(force_all=False)` | Daily bill sync (Flow 1 + Flow 2) |
| `POST /trigger/bill-status-sync` | Calls `scheduler.trigger_bill_status_sync(...)` | Flow 1 only |
| `POST /trigger/legislator-bio-sync` | Calls `LegislatorBioPipeline.run(options)` | Bio + photo sync |
| `POST /trigger/knowledge-base-reconcile/{jurisdiction}` | Calls `knowledge_base_embedding.plan_reconcile(...)` in the background | SYNC-95 dry run of the reconcile pass; always read-only; result in the log |
| `POST /trigger/votebot-eval` | Calls `run_votebot_eval(...)` | Votebot evaluation run |
| `POST /trigger/webflow/{job}` | Runs webflow batch job by name | CMS batch jobs |
| `POST /trigger/verify-org-citations` | Calls `verify_org_citations(...)` in the background (SYNC-100) | Verifies + stores organization-position citations from an external source (e.g. Slack #legislation) via `verify_and_store_position`, the same tail `find_bill_positions` research uses; skips rows the broker already settled (`broker_client.get_bill_organization_positions_existing`); serial; defaults to `dry_run=true`; result in the log line `org_citation_verify_summary` |

## sync_schedule.yaml config blocks

Key config paths referenced in code (don't hardcode — always read from `self._sync_config`):

| Path | Default | Effect |
|---|---|---|
| `bill_sync.webflow_status.enabled` | `true` | Flow 1 on/off |
| `bill_sync.version_check.enabled` | `true` | Flow 2 (Pinecone) on/off |
| `openstates_archive.knowledge_base_embedding.enabled` / `.jurisdictions` | `false` in code, **`true` in the checked-in file since 2026-10-05** / `[fl, us, va, mi, wa, az, ut]` | SYNC-83 embedding hook on/off and enrolled jurisdictions; a host only acts if it also has `KNOWLEDGE_BASE_INDEX_NAME` |
| `openstates_archive.knowledge_base_embedding.backfill.blackout_start_utc` / `.blackout_end_utc` | `"04:45"` / `"07:00"` | SYNC-90 backfill pauses inside this UTC window (the 05:00 archive start) |
| `openstates_archive.bill_search_refresh.enabled` / `.jurisdictions` | `false` in code, **`true` in the checked-in file since 2026-10-05** / `[us, fl, mi, az, va, wa, ut, nc]` | SYNC-87 bill-search refresh hook on/off and enrolled jurisdictions; a host only acts if it also has `RDS_OPENSTATES_API_BASE` and its key |
| `bill_version_check.max_updates_per_run` | `0` (unlimited) | Cap re-ingestions per run |
| `bill_version_check.skip_webflow_update` | `false` | Suppress Flow 1 writes |
| `rate_limit.requests_per_minute` | varies | Rate limiter config |
| `votebot_eval.thresholds.*` | see yaml | Regression detection thresholds |

---

## Discipline checklist for every new PLAN

Before sketching a new dataclass / service / helper:

1. **Grep first.** `grep -rn "class <Name>\|def <name>" src/ddp_sync/` and check this catalog.
2. **Check result types.** `VersionCheckResult`, `VersionSyncBatchResult`, `BillSyncResult`, `SyncBatchResult`, `IngestionResult` cover most pipeline return shapes. Don't add per-method result types.
3. **Check write paths.** `WebflowLookupService.update_bill_fields()` is the one Webflow PATCH primitive. Don't inline httpx calls to the Webflow API.
4. **Check Redis patterns.** `get_redis_store()` is the singleton. All methods no-op gracefully. Don't create new Redis clients.
5. **Check the rate limiter.** `RateLimitConfig.from_yaml()` + `RateLimiter` is the shared primitive. Don't add per-pipeline sleep loops.
6. **Check ingestion.** `IngestionPipeline.ingest_document()` is the one path to Pinecone. Don't call `VectorStoreService.upsert_documents()` directly from pipelines.
7. **Check document types.** New document types must be added to the table above AND to VoteBot's `VALID_RETRIEVAL_SOURCES` to avoid analytics warnings.
