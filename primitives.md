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

**Retrieval isolation**: `bill-text-history` and `bill-changelog` are structurally invisible to VoteBot's normal retrieval phases (which filter by explicit `document_type`). Do not add unfiltered fallback queries.

**LegBot's `BillArtifact` generation does NOT use this pipeline** (`pipelines/bill_artifact_generation.py` — `generate_and_store_bill_artifact`/`generate_and_store_bill_changelog`, ddp-infra's `PLAN-bill-document-provenance.md` Phase 8). Decided 2026-08-10 (Ramon): LegBot's output already lands in queryable `BillArtifact` rows VoteBot can read directly, so re-embedding LegBot's own summary into Pinecone would duplicate the original "embed the full bill document" design intent for a different purpose (deep full-text query, not structured artifact lookup) without serving either well. These two functions used to call `IngestionPipeline`/`DocumentMetadata` directly (document types `bill-artifact-{artifact_type}` / `bill-artifact-bill_changelog`, never added to the table above) — removed, not just made optional. If VoteBot needs full-bill-text Pinecone search over archived bill text, that belongs in a new task or an extension of `ddp-open-states`' bill archiver (which already owns the full archived text), reusing `IngestionPipeline`/`DocumentMetadata` below — not revived inside LegBot's artifact-generation path.

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
- **`OpenStatesSource`** — `fetch_jurisdiction(jurisdiction) -> JurisdictionInfo | None`
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

- **`OpenStatesPeopleClient`** — person lookups for bio sync. Methods: `get_person(person_id)`, `search_people(name, jurisdiction, ...)`
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

- **`FederalLegislatorCache`** — in-memory cache of 538 Congress members. `lookup_with_info(name) -> dict | None` — returns `{person_id, name, party, state}`. Used to enrich federal vote records with stable OpenStates person IDs.

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
| `POST /trigger/votebot-eval` | Calls `run_votebot_eval(...)` | Votebot evaluation run |
| `POST /trigger/webflow/{job}` | Runs webflow batch job by name | CMS batch jobs |

## sync_schedule.yaml config blocks

Key config paths referenced in code (don't hardcode — always read from `self._sync_config`):

| Path | Default | Effect |
|---|---|---|
| `bill_sync.webflow_status.enabled` | `true` | Flow 1 on/off |
| `bill_sync.version_check.enabled` | `true` | Flow 2 (Pinecone) on/off |
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
