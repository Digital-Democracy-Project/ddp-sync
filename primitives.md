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

## Bill-topics taxonomy (`data/bill_topics_taxonomy.json`, SYNC-124)

`bill_topics` answers are validated against `src/ddp_sync/data/bill_topics_taxonomy.json` (`version`, `max_topics`, `topics`), a verbatim copy of the file ddp-broker-py generates from its `Topic` table (BROKER-143, `manage.py generate_bill_topics_taxonomy`; format in that repo's `docs/bill-topics-taxonomy.md`). `bill_artifact_generation.py` reads it once at import into `_BILL_TOPICS_TAXONOMY`, `_BILL_TOPICS_MAX` and `_BILL_TOPICS_VERSION`; `_filter_bill_topics` is unchanged. **Never edit the copy by hand and never fetch the list at run time:** when the broker's list changes, copy the regenerated file here in the same window as ddp-agents' LegBot prompt (AGENTS-140). `tests/test_bill_topics_taxonomy.py` fails if the copy was edited (its `version` must be the broker's hash of its content). Shipped as package data (`pyproject.toml`), so the non-editable EC2 image includes it.

## Organization position research (`pipelines/bill_organization_position_research.py`)

`generate_and_store_bill_organization_positions(...)` researches ONE bill: `find_bill_positions` (live web search) names organizations, then each finding goes through `verify_and_store_position(...)` (verify_bill_position, then one broker write; failures isolated per organization). **`verify_and_store_position` is the shared tail**: `verify_org_citations` (SYNC-100) calls it too, so anything added there reaches both. It returns, besides the verdicts, the broker's `organization_id` and `organization_needs_content` (BROKER-204: the organization the finding resolved to, and whether its description is still blank).

**After the write: `resolve_and_describe_organizations(results, citations=...)` (SYNC-127).** The resolve step and the About step below live in this one function, called by both `generate_and_store_bill_organization_positions` (once per bill, after the loop) and `verify_org_citations` (after each item's write), so a stub created from a cited position (Slack #legislation, a form) is matched and described exactly like one research created, under the same two settings. `citations` maps a written position id to the (url, excerpt) the matcher is shown; the citation path has no excerpt.

**Organization "About" content (SYNC-107, with AGENTS-104 and BROKER-204).** After the per-finding loop, `generate_and_store_bill_organization_positions` asks LegBot for each organization that has a *confirmed* position here and still `organization_needs_content`, once per organization, through `generate_and_store_organization_content(organization_id, org_name)`: `legbot_client.dispatch_describe_organization` (question type `describe_organization`, AGENTS-104) then `broker_client.write_organization_content` -> `POST /api/organizations/<pk>/content/`. The broker fills only empty fields and never replaces existing text. LegBot answers in the broker's own field names (`description`, `org_type`, `policy_focus_areas`, `funding`, `affiliates`, `website`, `email`, `contact_page_url`), listed in `_ABOUT_FIELDS`; its `sources` is ignored and list answers are joined. Gated by `LEGBOT_ORGANIZATION_CONTENT_ENABLED` (default **off**: needs the broker write deployed and CAMS reloaded onto AGENTS-104 first). It runs from `resolve_and_describe_organizations` (above), not inside `verify_and_store_position`. Eligibility is any organization with a confirmed position here whose description is still blank (stub or not), as reported by the broker's position write; against an older broker that omits `organization_id` nothing is asked. One `organization_about_summary` log line per bill gives the outcome counts. The outcome is on the first confirmed result for the organization as `about_content` (`written` / `nothing_written` / `dispatch_failed` / `broker_write_failed`); a failure never fails the position write or the bill. **Do not write a second content path or a second Organization write**; the Webflow descriptions were imported once by the broker (BROKER-144).

**Organization resolve step (SYNC-97, `PLAN-notifications-alert-engine.md` 6.7.4; broker side BROKER-191, LegBot side AGENTS-112, neither built yet).** After the write and before the About step, `resolve_and_describe_organizations` calls `resolve_organization_for_position(...)` for each *newly created, confirmed* position (`outcome == "written"`, `result == "created"`, verdict `confirmed`). It asks the broker for a shortlist (`broker_client.get_organization_resolution_shortlist`, which opens the attempt and decides eligibility: a pipeline-created stub, not resolved before), asks LegBot `resolve_organization` to choose among the candidates or answer null (`legbot_client.dispatch_resolve_organization`), reports the answer to the attempt once (`report_organization_resolution_answer`), and only in `act` mode with a non-null answer asks the broker to apply it by attempt id (`request_organization_resolution_act`; the broker may refuse). A choice outside the shortlist is reported as null. Every failure leaves the stub and the position exactly as it was and never raises; the per-position result gets `resolution` (`outcome`: `not_eligible` / `no_candidates` / `null_answer` / `suggested` / `acted` / `act_refused` / `shortlist_failed` / `dispatch_failed` / `report_failed` / `act_failed` / `failed`), logged as `organization_resolution_attempt`, plus one `organization_resolution_summary` per call. Setting: `LEGBOT_ORGANIZATION_RESOLUTION_MODE` = `off` (default for now) / `suggest` / `act`; it only says what ddp-sync *asks* for, the broker's own act setting permits a change. The broker routes it expects are under `/api/organization-resolution/` (`shortlist/`, `attempts/<id>/answer/`, `attempts/<id>/act/`). **Do not add a second organization-matching path here**; exact and alias matching stay in the broker's `Organization.resolve_name`.

## Organization resolution release gates (`pipelines/org_resolution_gates.py`, SYNC-98)

Pure scoring for the two gates that must pass before the broker's act mode is switched on (`PLAN-notifications-alert-engine.md` 6.7.6): `evaluate_offline_gate(results: list[CaseResult], min_recall=0.80, meta=None)` (zero wrong matches over every case AND at least 80% of the positive spellings resolved correctly; a null counts only against recall; `exact` cases are reported, not counted in recall). **Scoring is by GROUP (SYNC-109, `SCORING_VERSION` 2):** a case is correct when the answered record is in the expected group, i.e. ANY record of the same real organization (the duplicate-record problem is the canonical-list cleanup's, not the model's); a chapter and its parent are different groups, so choosing the parent is still wrong. `Groups.from_loaded(loaded)` reads the group labels from the version 2 `loaded.json` (`org_groups`, each case's `expected_group_id`, `verdicts_source`) and REFUSES a version 1 file; `Groups.result(case, answered_org_id=, tier=)` is the one way a case becomes a `CaseResult`. The report also counts, never as errors, correct answers that chose another record than the key's and how many of those were stubs. A fixture whose `verdicts_source` is not `reviewed` is PROVISIONAL: reported, exit 1, never a gate result. Both CLIs take `--report-out` (JSON with seed sha256, seed_version, verdicts_source, scoring_version) and `evaluate_shadow_gate(attempts, reviews)` (200 eligible invocations, 30 non-null suggestions, every non-null one reviewed, none wrong). Command: `.venv/bin/python scripts/org_resolution_gates.py offline RESULTS.jsonl --loaded LOADED.json` or `... shadow ATTEMPTS.jsonl REVIEWS.json` (formats in the script docstring); exit 0 = pass. Re-run both after any change to the prompt, model, shortlist logic, cleaned-name key, candidate filters or broker checks. **This module only scores.** The labeled set is BROKER-192's evaluation seed, loaded into a throwaway broker by `manage.py load_resolution_eval_fixture`; `pipelines/org_resolution_harness.py` + `scripts/org_resolution_harness.py loaded.json --broker-base URL --broker-is-throwaway` runs each case through the pipeline's own resolve step (`resolve_organization_for_position`, SYNC-97; the answers it records on the broker's attempt rows carry the model and prompt version), reads the exact/alias-tier answers the loader already recorded, and scores with the gate above. A case the resolver could not finish is an error and the run cannot pass. **Do not add a second scoring path or a second way to run the resolver.**

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
- **`get_processed_slack_citations(ts_list) -> set | None` / `mark_slack_citations_processed(ts_list)`** — SYNC-127: the Slack #legislation messages whose citation is settled, one set at `ddp:slack_citations:processed`, no expiry. The read returns `None` (never an empty set) when Redis cannot answer, and the nightly pull then stops instead of resending everything.

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
- **`webflow_cms/models.py`** — shared result shapes: `UpdateResult, DeleteResult, FillResult, SyncResult, DuplicateGroup`
- **`webflow_cms/exceptions.py`** — `WebflowCMSError, WebflowAPIError, WebflowConflictError, ConfigurationError, ParseError`

## Webflow CMS batch services (`webflow_cms/services/`)

- **`BillOrgSyncService`** — syncs organization references on bill items
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

## Broker bill-event signal (`pipelines/openstates_scrape.py`, SYNC-128)

- **`_maybe_signal_broker_bill_events(jurisdiction, config)`** — called from `_run_scrape` on success, before the OPEN-291 archive hook. Gated by `openstates_scrape.broker_bill_event_signal` (`enabled`, `jurisdictions`; default off), maps `usa` to `us` with `openstates_archive._archive_jurisdiction_code`, debounces 15 minutes per jurisdiction (`ddp:broker_bill_event_signal:<code>`, SET NX EX, fails open). Never raises, never retries.
- **`broker_client.request_bill_event_check(jurisdiction)`** — `POST {DDP_BROKER_API_BASE}/api/openstates-bill-events/check/` `{"jurisdiction": ...}` with the service token (BROKER-300; 202 queued). Raises `BrokerClientError` on an unset base/token, unreachable broker, or any 4xx/5xx (400 unknown jurisdiction, 401 token, 503 queue down).

## Voatz → Brevo opt-outs (`pipelines/voatz_brevo.py`, SYNC-122)

- **`fetch_ddp_suppressed_emails()`** — DDP's suppression list from ddp-broker-py (`GET /api/email/suppressions/`, BROKER-305), lower-cased; `None` when unreadable (callers then add nobody).
- **`brevo_contact_opted_out(api_key, email, list_id)`** — `GET /v3/contacts/{email}`: True if `emailBlacklisted` or the list is in `listUnsubscribed`, False for an unknown address, `None` when Brevo cannot answer (skip for this run).
- `sync_org(..., suppressed_emails)` checks both before adding; `full_sync_org(..., suppressed_emails)` re-imports only current, non-blocklisted list members not on the suppression list.

## People checkout pull (`pipelines/openstates_scrape.py`)

- **`run_people_pull_job(config)`** — SYNC-102. Daily `git -C <OPENSTATES_ROOT>/people pull --ff-only` (03:00 UTC, `openstates_scrape.people_pull` in `sync_schedule.yaml`); no DB import (that is `run_people_refresh_job`, weekly). Registered only when `OPENSTATES_PEOPLE_PULL_ENABLED=true` on the host (default off). It has its own registration step (`_register_people_pull_job`), independent of `OPENSTATES_SCRAPE_ENABLED` (SYNC-108, PR 217): before that it never registered on a host with scrapes off, which is the Mac's case. Writes flow status `openstates_people_pull` (shown by `/health`) with before/after head sha + commit date, branch, git's summary line; every failure path writes `failed` and alerts via `_alert_scrape_failure`. Manual: `POST /trigger/openstates-scrape/people-pull`.

## Jurisdiction source names (`services/jurisdiction_names.py`, OPEN-420)

`source_name(code)` is the one place that turns a jurisdiction code into the label put on ingested documents ("Florida
Legislature", "US Congress"), used by `bill_sync`, `legislator_sync` and the OpenStates source (each keeps a thin
`_get_source_name` that delegates). It was three identical copies, none with `nc`. The manifest cannot supply the strings
(its `name` is "North Carolina"), so this stays its own table; `tests/test_jurisdiction_names.py` pins the existing names
and, when `OPENSTATES_ROOT` points at a ddp-open-states checkout (set at import, other tests clear it), fails if a manifest
jurisdiction has no entry. **Adding a state: add its line here.** An unknown code still falls back to "<CODE> Legislature".

## Jurisdiction manifest dual-read (`services/jurisdiction_enrollment.py`, OPEN-318 / OPEN-328)

ddp-open-states' `jurisdictions.yaml` says which jurisdictions are enrolled in which feature; ddp-sync's own
lists in `sync_schedule.yaml` are still the authority. `enrolled(feature, yaml_codes, root)` is the one call a
consumer makes (`root` = `openstates_scrape._get_root(config)`, which honours `OPENSTATES_ROOT`): in **shadow
mode (default)** it returns the yaml's list and logs a `jurisdiction_manifest_disagrees` WARNING (once per
distinct difference, with the manifest file's mtime so a stale checkout is not mistaken for drift); with
`JURISDICTION_MANIFEST_MODE=authoritative` in a host's env it returns the manifest's. A yaml with no list falls
back to the manifest, then to the caller's last-resort constant (`None` means "neither could answer"). Never
raises: a missing, invalid or incomplete manifest degrades to the yaml's answer with one log line.
`log_config_disagreements(config, root)` compares every list at once; the scheduler calls it at start-up, and
the OPEN-330 daily check reuses it. Features: `archive` (the manifest's `archive.scheduled`, the recurring list; `archive.enabled` is only the shell gate and the Stage 4 manual run, OPEN-418; a manifest from before that split reads as "cannot say"), `embedding`, `search_refresh` (spelled `us`) and
`cloud_path`, `scrape_primary`, `scrape_secondary` (spelled `usa`), mirroring `check-jurisdiction-drift.py`'s
`project()` in ddp-open-states. Wired so far: the embedding and search-refresh hook gates, the manual archive
trigger and the knowledge-base 404 gates, the manual single-state scrape targets, and the two in-code
fallbacks. **Not wired (still yaml only, deliberately):** scheduler registration of the scrape/archive jobs,
`_cloud_path_owns`, `_memory_backend_enabled`, `maybe_trigger_archive_after_scrape`, and the per-feature
opt-in lists (`sweep_import`, `scrape_retry`, `full_walk`, `scrapebot_fallback`), because a wrong answer there
changes what production scrapes. The manifest is read from the ddp-open-states checkout on each host (a bind
mount on the EC2 broker host that nothing here pulls).

**Cutover checklist (per host, before setting `JURISDICTION_MANIFEST_MODE=authoritative`):** (1) the host has
logged `jurisdiction_manifest_agrees` at start-up and from the OPEN-330 daily check for a full weekly cycle,
with no unexplained `jurisdiction_manifest_disagrees`; (2) `git -C <openstates_root> log -1` shows the host's
ddp-open-states checkout is current on `main` (pull it first on the EC2 broker host: nothing does it
automatically); (3) the deployed `sync_schedule.yaml` on that host has all six lists; (4) rollback is removing
the env var and restarting. Do the Mac first, the EC2 broker host second.

## Jurisdiction drift check (`pipelines/jurisdiction_drift_check.py`, OPEN-330)

A daily job that compares THIS host's jurisdiction lists with ddp-open-states' `jurisdictions.yaml` and alerts through
`post_alert` (source `jurisdiction_drift_check`). It checks the live `sync_schedule.yaml` dict (hand-edited per host),
the host's own `DDP_OPENSTATES_JURISDICTIONS` and `LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST` (only when the host sets
them: unset is not drift), and whether the manifest can answer at all: a missing, unreadable, or out-of-date checkout is
reported as "cannot verify" (the EC2 bind mount is never auto-pulled), because a monitor that cannot see must alert.
The alert repeats only when the findings change or after `jurisdiction_drift_check.repeat_days` (7). Default off per host
(`JURISDICTION_DRIFT_CHECK_ENABLED=true`), registered outside `_register_openstates_scrape_jobs` like the fork check.
Manual run: `POST /trigger/openstates-scrape/drift-check`. It reuses `jurisdiction_enrollment.config_disagreements`
(OPEN-328), so its logs are the weekly-review evidence for the dual-read. It cannot read the broker's env (another
host); that value is checked with `check-jurisdiction-drift.py --live` in ddp-open-states. `.github/workflows/
jurisdiction-drift.yml` runs the ddp-open-states drift script on ddp-sync PRs, advisory only (see that file).

**What each host's job can and cannot see.** It reads only its own process's settings: the Mac's ddp-sync (its LegBot
allowlist; it sets no `DDP_OPENSTATES_JURISDICTIONS`), the EC2 broker host's ddp-sync (both, if set). It cannot see the
**broker's** `DDP_OPENSTATES_JURISDICTIONS` (another container with its own `.env`): that is checked with
`check-jurisdiction-drift.py --live broker-prod:DDP_OPENSTATES_JURISDICTIONS=<value>` and by the broker's own shadow
check (OPEN-329). An unset env list and one accidentally set empty look the same to the settings layer, so both are
"skipped"; the job names the skipped lists in its log line and flow status (`env_lists_not_set_on_this_host`) so the gap
is visible. An exception inside the job posts a (non-de-duplicated) "could not run" alert. If Slack succeeds but the Redis
status write fails, the de-duplication cannot persist and the alert repeats daily until Redis is back (the Redis warning
is logged).

**First-run checklist (per host, before leaving `JURISDICTION_DRIFT_CHECK_ENABLED=true` on):** pull the host's
ddp-open-states checkout (so the manifest has `replica_routing` and `legbot_replica`), set the flag, restart,
`POST /trigger/openstates-scrape/drift-check`, then confirm the `jurisdiction_drift_check: done` log line and
`ddp:flow:jurisdiction_drift_check`, and (once, on purpose) that a deliberately wrong list produces the Slack alert and
that correcting it clears the signature. Mac first.

## Fork upstream check (`pipelines/fork_upstream_check.py`)

- **`run_fork_upstream_check_job(config)`** — Daily (01:30 UTC, `openstates_scrape.fork_upstream_check` in `sync_schedule.yaml`). For each DDP fork (people, openstates-core, openstates-scrapers, api-v3) fetches the fork's `main` and its upstream's `main` over public https into a job-owned bare cache (`<OPENSTATES_ROOT>/logs/fork-upstream-cache/`, no credentials, never the live working trees) and counts upstream commits the fork lacks. Writes `<OPENSTATES_ROOT>/logs/fork-upstream-status.json` (per repo: `behind_count`, `oldest_age_days`, `titles`, `areas_changed`, `sensitive_paths_changed` for workflows/migrations/dependency files) for a reviewer to read, and flow status `fork_upstream_check` (shown by `/health`). Alerts through `post_alert` when the oldest missing commit is 3+ days old (tiers 3/7/14 days, then weekly) or a fetch fails (nights 1, 3, then weekly); the previous status file is the de-dup memory. Never merges, opens PRs or pushes. Registered only when `FORK_UPSTREAM_CHECK_ENABLED=true` on the host (default off; Mac Studio only). It has its own registration step (`_register_fork_upstream_check_job`) and does **not** depend on `OPENSTATES_SCRAPE_ENABLED`: the Mac has scraping off, and until PR #213 the check was tucked inside the scrape registration and silently never started. Do not move it back. Manual: `POST /trigger/openstates-scrape/fork-check`. This is the stalled-sync check SYNC-104 asked for. Alerts use `SLACK_BOT_TOKEN`, which the service gets from `ddp-agents/.env` through `scripts/start-ddp-sync.sh`; ddp-sync's own `.env` does not hold it, so a hand-run test must load it the same way. Delivery verified 2026-10-13 (a labelled test reached #automation-errors as CodeBot).

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
| `POST /trigger/verify-org-citations` | Calls `verify_org_citations(...)`, any number of items, one after another (SYNC-100). Synchronous by default; `background: true` (SYNC-106) answers 202 with a `run_id` and runs it in the background, one background run at a time (a second waits; a synchronous request is one citation and may overlap a background run, by design), recording progress in Redis (`ddp:verify_org_citations:run:*`, its own namespace, not the zombie-watchdog's) | Checks whether a cited page shows an organization holds a position on a bill and stores the answer, via `verify_and_store_position` (the same tail `find_bill_positions` research uses). The broker write is an upsert on (bill version, org, position, citation_url): a citation not on the bill is added, one that is has its verification updated, and an inconclusive answer never overwrites a settled row. For a manual batch, a visitor-flagged re-check, or a user-submitted citation. Costs one Claude call per item whether or not it was checked before |
| `GET /trigger/verify-org-citations/{run_id}` | Reads the Redis run record | SYNC-106: `status` (queued / running / finished / failed), `total`, `processed` (items handled in the order sent, so an interrupted run resumes by resending from there), outcome `counts`, timestamps, `error`; 404 if unknown; kept 7 days. A `running` record whose `updated_at` stops moving died with the process; nothing restarts it |
| `POST /trigger/slack-citations` | Runs `slack_citations.run_slack_citations_job(...)` once in the background (202) | SYNC-127: the nightly Slack #legislation pull, now, with the yaml `slack_citations` block as it stands (`mode: report_only` logs what it would send and writes nothing; `write` sends through `run_verify_org_citations_in_background`, so `GET /trigger/verify-org-citations/{run_id}` shows progress under the run id the `slack_citations_summary` log line names). 409 on a host whose `SLACK_CITATIONS_ENABLED` is off |
| `POST /trigger/docker-cleanup` | Runs `docker_cleanup.run_docker_cleanup_job(settings.docker_prune_script, ...)` once in the background (202) | SYNC-119: ddp-agents' `deployment/scripts/docker-prune.sh` now, with the scheduled run's timeout and failure alert. 409 on a host whose `DOCKER_CLEANUP_ENABLED` is off |

## sync_schedule.yaml config blocks

Key config paths referenced in code (don't hardcode — always read from `self._sync_config`):

| Path | Default | Effect |
|---|---|---|
| `bill_sync.webflow_status.enabled` | `true` | Flow 1 on/off |
| `bill_sync.version_check.enabled` | `true` | Flow 2 (Pinecone) on/off |
| `openstates_archive.knowledge_base_embedding.enabled` / `.jurisdictions` | `false` in code, **`true` in the checked-in file since 2026-10-05** / `[fl, us, va, mi, wa, az, ut]` | SYNC-83 embedding hook on/off and enrolled jurisdictions; a host only acts if it also has `KNOWLEDGE_BASE_INDEX_NAME` |
| `openstates_archive.knowledge_base_embedding.backfill.blackout_start_utc` / `.blackout_end_utc` | `"04:45"` / `"07:00"` | SYNC-90 backfill pauses inside this UTC window (the 05:00 archive start) |
| `openstates_archive.bill_search_refresh.enabled` / `.jurisdictions` | `false` in code, **`true` in the checked-in file since 2026-10-05** / `[us, fl, mi, az, va, wa, ut, nc]` | SYNC-87 bill-search refresh hook on/off and enrolled jurisdictions; a host only acts if it also has `RDS_OPENSTATES_API_BASE` and its key |
| `openstates_scrape.secondary.jurisdictions` | `[va, mi, ma, ut, az, nc]` in the checked-in file | The Sunday secondary batch, and (SYNC-57) the states `POST /trigger/openstates-scrape/{state}` accepts on their own; `fl`/`wa`/`usa` are batch targets instead |
| `slack_citations.*` | `enabled: true`, `mode: report_only`, `start_date`, `lookback_days: 30`, `thread_lookback_days: 730`, `max_citations_per_run: 200`, `sync_time_utc: "09:00"` | SYNC-127 nightly Slack pull; runs only where the host's `SLACK_CITATIONS_ENABLED=true` (default off) |
| `docker_cleanup.*` | `enabled: true`, `sync_time_utc: "06:30"` (finishes, timeout included, before the weekly launchd prune's Sunday 08:00), `timeout_seconds: 1800` | SYNC-119 nightly Docker cleanup (ddp-agents `docker-prune.sh`, path `DOCKER_PRUNE_SCRIPT`); runs only where the host's `DOCKER_CLEANUP_ENABLED=true` (default off; the Mac). The script owns every rule, its log and its own alert; the job adds the process-group timeout and a `post_alert` on a non-zero exit |
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
