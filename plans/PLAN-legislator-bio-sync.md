# PLAN: Legislator Bio + Contact Sync

**Status:** ✅ **Phase 1 + Phase 2.5 (data-extraction layer) SHIPPED 2026-04-30.** Phase 1 federal sync (32 FL records) + state-leg baseline sync + APScheduler wiring + Audit B + Phase 2.5 enrichment (`openstates-id` URL field, FL `official-website` extraction via per-state override registry) + round-17 review fixes (scheduler-completion metric, job-id-collision cleanup, edge-case tests, rollback playbook, editor sign-off criteria). 122-test suite. Seventeen pm-review rounds + four production-discovery iterations (chamber→seat data-model, URL-typed ID fields, Zapier Mustache→pre-formatted strings, term-date ChurnPATCH). The `legislator_bio_sync` block in `config/sync_schedule.yaml` defaults to `enabled: false` — operator flips after editor verification + monitoring window. Carry-overs: per-state photo-upload pipeline (Phase 3); state-leg social handles + term dates (deferred — OpenStates probe confirmed they're not in `/people` v3); orphan-set instability investigation; `--undo-last-run` mass-revert capability.
**Created:** 2026-04-29
**Repo:** ddp-sync
**Target:** Phase 1 in ~2 weeks; Phase 2 in 4–6 weeks; Phases 3–4 in backlog

---

## Goal

Enrich the existing Webflow **Legislators** CMS collection with biographical, contact, term, and social-media fields for federal and state legislators. Source data comes from authoritative free + already-paid feeds; published to the live DDP website via Webflow CMS.

## Scope

**In scope (Phase 1–2):**
- All 535 current US Congress members (federal)
- US Congress members departed since 2023-01-01 (~173 members — the realistic backfill set)
- State legislators in the 7 currently active jurisdictions: FL, VA, WA, UT, AZ, MI, MA
- Robust handling of editor-created CMS items (DDP is about to bulk-create hundreds of state-legislator entries for post-session scorecards)

**Out of scope (Phase 1):**
- Narrative biographical prose (deferred to Phase 4)
- State coverage beyond the 7 active jurisdictions (planned for end-of-2026 expansion)
- Photo asset upload to Webflow (Phase 3)
- Ballotpedia data ingestion (deferred indefinitely; we store the slug as a reference link only)

---

## Sources

| Source | Auth | Use |
|---|---|---|
| `unitedstates/congress-legislators` (`legislators-current.yaml`, `legislators-historical.yaml`) | None (public) | Federal current + historical bio, contact, terms, IDs |
| `unitedstates/congress-legislators` (`legislators-social-media.yaml`) | None | Federal social handles |
| OpenStates `/people` v3 | `OPENSTATES_API_KEY` (existing 30k/day tier) | All state legislators + current federal members; provides `bioguide` via `other_identifiers` |
| Congress.gov API | `CONGRESS_API_KEY` (existing) | Reserved for portrait quality + leadership metadata; not strictly required for Phase 1 |

**References:**
- https://github.com/unitedstates/congress-legislators
- https://docs.openstates.org/api-v3/

---

## Existing infrastructure (reused — do not duplicate)

Repo audit (2026-04-29) confirms these components already exist; Phase 1 extends them rather than creating parallel implementations.

| Component | Location | What it provides | Status |
|---|---|---|---|
| `WebflowLookupService` | `services/webflow_lookup.py` | Webflow PATCH (despite the "Lookup" name — it's the write service). Used by `pipelines/bill_version.py` Flow 1. Has API-key fallback (`webflow_scheduler_api_key` → `webflow_votebot_api_key`). | ✅ EXTENDED (commit 2852a36): added rate-limiter, 429/Retry-After + WebflowRateLimitError, `update_legislator_fields()`, `create_legislator_draft()`, field-existence tolerance via cached schema lookup. Rename to `WebflowWriteService` deferred. |
| `RateLimitConfig` + `_apply_rate_limit()` | inline in `pipelines/legislator_sync.py` and `pipelines/bill_sync.py` | Sleep-based rate limiter for OpenStates calls; loads from `rate_limit:` config block. Was duplicated. | ✅ EXTRACTED (commit 2852a36) to `services/rate_limiter.py`. Both pipelines migrated. New module adds `asyncio.Lock`-guarded `apply()` (concurrency safe) and `enforced_sleeps` observability counter. |
| `push_alert_to_zapier()` | `pipelines/voatz_brevo.py` | Posts run summary to a configured Zapier webhook | ⏳ TO REUSE in bio-sync run summaries (step 7). Pattern is the established alerting mechanism for this repo. |
| `ZAPIER_WEBHOOK_URL` | `config.py` + `.env.example` | Already configured | No change. |
| `dry_run` semantics | `sync/types.py::SyncOptions`, `api/routes/sync_unified.py` | Standard pattern across the repo | Bio sync follows the same convention. |
| `sync_schedule.yaml` `notifications:` block | `config/sync_schedule.yaml` | Declared but **never wired** ("future use" — `on_failure`, `on_large_changes`) | ⏳ Bio sync becomes the first consumer in step 7. |

**What does NOT exist yet (gaps still to build):**
- Cross-process / Redis-backed rate coordination — deferred to Phase 2+ when multi-worker becomes a need (single-worker assumption documented).
- Sentry / CloudWatch metrics / on-call escalation — out of scope for this plan; structured-metric breadcrumbs added to `RateLimitConfig.from_yaml` so a future infra-level alerting layer can pick them up.
- Durable PATCH-diff archive — Phase 1 keeps Redis 30-day TTL only; S3 + run-id-range deferred to Phase 2+.

**What's been built (commit refs):** see "Implementation status" below.

---

## Implementation status (as of 2026-04-29)

Phase 1 steps 1, 2, 3a, 3b have landed across two commits. Steps 4–9 are still pending.

| Phase 1 step | Module / artifact | Commit | Status |
|---|---|---|---|
| 1a | `services/rate_limiter.py` (new) | 2852a36 | ✅ Concurrency-safe via `asyncio.Lock`; structured-metric breadcrumb on fallback (round-5 fix); `from_yaml` never raises. |
| 1b | `pipelines/legislator_sync.py` (migrated) | 2852a36 | ✅ Imports from shared module; `_apply_rate_limit` retained as thin alias. |
| 1c | `pipelines/bill_sync.py` (migrated) | 2852a36 | ✅ Same migration shape as legislator_sync. |
| 2a | Error types + result dataclasses | 2852a36 | ✅ `WebflowError`, `WebflowRateLimitError`, `WebflowPatchResult`, `WebflowCreateResult`. |
| 2b | `WebflowLookupService` extension | 2852a36 + 24d058e | ✅ Shared limiter, `_send_with_backoff`, `update_legislator_fields()`, `create_legislator_draft()`, fail-closed schema partition (round-5 fix). |
| 2c | `update_bill_fields` migration | 2852a36 | ✅ Kept legacy bool contract; routes through new helpers internally. No caller migration needed. |
| 2d | Pre-merge staging smoke for `/trigger/bill-status-sync?dry_run=true` | — | ⏳ Required before any production deploy. |
| 3a | `services/congress_legislators.py` (new) | 2852a36 + 24d058e | ✅ Bioguide-indexed in-memory cache; YAML parse via `asyncio.to_thread()` (round-5 fix) so 8.6 MB historical doesn't block event loop. |
| 3b | `services/openstates_people.py` (new) | 24d058e | ✅ `OpenStatesPeopleClient` + `OpenStatesPerson` dataclass + `OpenStatesError` / `OpenStatesRateLimitError`. Verified against live API. |
| 4 | `pipelines/legislator_bio.py` orchestrator | (forthcoming) | ✅ `LegislatorBioPipeline` + `BioSyncOptions` + `BioSyncReport` + `CMSLegislator` + cardinal-rule helpers (`is_empty`, `should_write`, `split_email_field`). Federal sync end-to-end (OpenStates primary → bioguide-id fallback for departed federal). Multi-signal merge detection scaffolded. State path is a clear Phase 2 stub. Smoke-tested against mocked sources for Rick Scott (live federal — 17 fields), Karen Bass (historical federal via bioguide fallback — 7 fields), state record (orphan — correct). |
| 4a | `WebflowLookupService.iter_legislator_items()` | (forthcoming) | ✅ New paginated CMS reader that returns raw upstream dicts. Uses the read-scope key. Has 200-page safety valve. Used by the orchestrator to build the CMS index and to scan for auto-create candidates. |
| 4b | App-startup pre-warm of congress-legislators YAML | (forthcoming) | ✅ `app.py::lifespan` fires `asyncio.create_task(source.warm_cache())` so trigger endpoints never cold-start (round-6 fix). The orchestrator awaits `warm_cache()` defensively (idempotent). |
| 4c | Foundation test suite | (forthcoming) | ✅ `tests/test_legislator_bio_foundation.py` — 31 tests covering RateLimiter concurrency, RateLimitConfig fallback contract, OpenStates dict/string jurisdiction shapes, is_federal heuristic, extract_other_id non-dict guard, is_empty / should_write / split_email_field, schema-cache TTL + stale-reuse, fail-closed `_partition_payload`. All pass. |
| 5 | `/trigger/legislator-bio-sync` endpoint | (forthcoming) | ✅ FastAPI POST handler in `api/routes/triggers.py`. Query params: `dry_run`, `auto_create`, `jurisdiction`, `target`, `limit`, `historical_since`, `audit_only`. **Round-7 ALB-timeout safety gate**: returns 503 + `Retry-After: 60` when `app.state.congress_legislators._warmed` is False. Param validation (400 on bad target/audit_only/historical_since). Audit-only short-circuit returns `{audit, status: not_implemented}` until step 6 lands. Reuses pre-warmed source from app.state — no double-parse. |
| 5a | `split_email_field` round-7 hardening | (forthcoming) | ✅ Case-insensitive scheme matching; `mailto:` unwraps to a real email; whitespace stripped. Three new tests pin the behavior. |
| 5b | Endpoint test suite | (forthcoming) | ✅ `tests/test_trigger_legislator_bio_sync.py` — 10 tests covering: 503-on-not-warmed, 503 on missing app.state, 400 on invalid params, audit-only short-circuits, happy-path wiring, default options, exception → 500. |
| 6 | Audits A and C | (forthcoming) | ✅ `audit_federal_join_keys()` (Audit A — federal records lacking both join keys) and `audit_state_join_keys(jurisdiction=None)` (Audit C — state records lacking openstatesid, optional state filter) implemented as methods on `LegislatorBioPipeline`. New `AuditReport` + `AuditEntry` dataclasses. Trigger endpoint's `audit_only=A\|C` no longer stubs — runs the real audit and returns the report. Both audits wrap WebflowError as `aborted=True` with `abort_reason` rather than raising, so editors get a partial report. New `state_code()` helper on `CMSLegislator` (looks at `state-code` then `state` fields). |
| 6a | Audit test suite | (forthcoming) | ✅ 8 audit tests: A flags only federal-with-no-keys, A skips state, A doesn't flag records with one key, A handles empty federal set; C flags state-no-openstatesid, C filters by jurisdiction (case-insensitive), C scans all states when no jurisdiction; A aborts gracefully on WebflowError. Plus 4 endpoint integration tests for the audit-only paths. Total suite: **55 tests, all pass.** |
| 7 | Run-summary alerting via Zapier | 4427dd3 + c492891 + round-11 hot-loop fix | ✅ `push_bio_sync_alert(webhook_url, report, *, large_changes_threshold=100)` mirrors `voatz_brevo.push_alert_to_zapier`. Wired into `LegislatorBioPipeline.run()` via `try/finally`; alert fires on **every non-dry-run completion including aborts**. Payload includes `on_failure` and `on_large_changes` threshold flags + the threshold value itself for Zapier-side routing. **Round-9 follow-ups:** jurisdiction cache 1h TTL + stale-on-empty-refresh reuse + `metric=webflow.jurisdiction_mapping_empty` breadcrumb. **Round-10 follow-ups:** `asyncio.Lock` serializes concurrent refresh attempts; `large_changes_threshold` extracted to a named constant + tunable parameter; `metric=legislator_bio_sync.alert_sent` on successful POST closes the SLA-dashboard gap. **Round-11 fix (real bug):** stale-reuse path now bumps the cache timestamp, preventing a hot-loop on Webflow where every call after TTL expiry would re-fire a failing fetch during a sustained outage. |
| 7a | Alerting + cache test suite | 4427dd3 + c492891 + round-11 | ✅ 6 alert-function tests; 4 `run()` integration tests; 4 cache tests (TTL, stale-reuse on empty, empty-mapping breadcrumb, lock-serializes-concurrent-refresh); 2 round-10 tests (threshold-tunable, success-metric-emitted); **2 round-11 tests** (no-hot-loop on sustained failure, lock-serializes-failing-fetch with 10 concurrent callers). **Total suite: 82 tests, all pass.** |
| 8 | Orchestrator-internal `run()` integration tests | a0d2300 + 0209c4c + round-14 | ✅ `tests/test_legislator_bio_orchestrator.py` — 13 integration tests covering full-pass `run()` flows: federal happy path, bioguide-fallback (Karen Bass), state Phase-2 stub, dry-run, per-record error, rate-limit abort with alert, jurisdiction filter, lock-release-on-raising-fetch, locked_fields exclusion, large_changes_threshold end-to-end, mixed-success PATCHes. **Round-14 additions:** (12) mass-blank-prevention — upstream None on populated CMS field → field NOT in PATCH (verifies both layers of defense: payload-build None-strip + diff-time `is_empty` check); (13) `==` threshold edge-case — at exactly threshold, `on_large_changes=False` (strict-`>` semantics pinned). `_build_pipeline()` fixture uses `MagicMock(spec=...)` on service mocks. |
| 8a | Round-12/13/14 polish bundled into step 8 | a0d2300 + 0209c4c + round-14 | ✅ Doc-comment on `get_jurisdiction_mapping`; lock-release contract test; locked_fields/threshold/mixed-success/mass-blank/equals-threshold integration tests; `MagicMock(spec=...)` on service mocks; threshold test imports `DEFAULT_LARGE_CHANGES_THRESHOLD` constant rather than hardcoding (round-14 fix to allow ops to tune without breaking tests). |
| 9 | Dry-run + 1 live PATCH on low-stakes record | f235665 + 350b0d6 + 1c23eb2 + 8653d24 | ✅ **Shipped 2026-04-30.** Pre-flight clean. Live execution surfaced three production-discovery iterations (each addressed atomically): (a) chamber→seat data-model correction, (b) URL-typed ID field construction, (c) Zapier Mustache→pre-formatted warning strings. First live PATCH on Mike Haridopolos succeeded; full FL delegation run (32 records) PATCHed cleanly with `errors: []`. See "Step-9 production rollout findings" below for the journey + carry-overs. |

**Round-5 fixes applied (in commit 24d058e):**

| Fix | Location | Description |
|---|---|---|
| Fail-closed schema fetch | `WebflowLookupService._partition_payload` | Schema-fetch failure now propagates as `WebflowError` instead of silently passing the unfiltered payload through. Prevents masking transient `/collections/{id}` 5xx as item-level 4xx avalanches. |
| Off-loop YAML parse | `CongressLegislatorsSource._fetch_or_cache_one` | The 8.6 MB historical YAML parse moved to `asyncio.to_thread()`. Wall-clock parse is slower (~55s vs 13s) but the event loop stays responsive — verified concurrent heartbeat ticks during the parse window. |
| Structured-metric breadcrumb | `RateLimitConfig.from_yaml` | Fallback paths emit `metric=rate_limiter.config_fallback` with `reason=file_not_found|parse_error` so infra alerting can fire when production silently regresses to defaults. |

**Round-6 fixes applied (forthcoming commit):**

| Fix | Location | Description |
|---|---|---|
| App-startup pre-warm | `app.py::lifespan` | `asyncio.create_task(source.warm_cache())` fires at app startup so trigger endpoints never cold-start. Prevents the 30s ALB idle timeout that the round-5 off-loop parse exposed (parse is now ~55s wall-clock). Pre-warm is fire-and-forget; orchestrator awaits `warm_cache()` defensively (idempotent). |
| Stale-schema cache | `WebflowLookupService._get_field_slugs` | Cache entries gain a 1-hour TTL. On expiry we attempt a refresh; if it fails AND we have a stale entry, we reuse it with a warning + `metric=webflow.schema_stale_reuse`. If we have no entry at all, the failure propagates per the fail-closed contract. Recovers from transient `/collections/{id}` 5xx during a Webflow incident without dead-stopping all writes. |
| Defensive `extract_other_id` | `OpenStatesPeopleClient.extract_other_id` | Skip non-dict entries in `other_identifiers`. OpenStates has historically returned bare strings during at least one upstream incident; this prevents an `AttributeError` on `.get()`. |
| `iter_jurisdiction` max-page valve | `OpenStatesPeopleClient.iter_jurisdiction` | New `max_pages` constructor parameter (default 200) prevents a runaway loop if the API's pagination semantics break. Largest real jurisdiction is US Congress at 535 members → ~11 pages at per_page=50, well under the cap. |
| Foundation test suite | `tests/test_legislator_bio_foundation.py` (new) | 20 tests pinning round-3 through round-6 fixes + live-data discoveries: RateLimiter concurrency lock, RateLimitConfig.from_yaml fallback contract, OpenStatesPerson dict/string jurisdiction shapes, is_federal heuristic, extract_other_id non-dict guard, schema-cache TTL + stale-reuse paths, fail-closed _partition_payload. Run with `pytest tests/`. |

**Live-data discoveries during step-3b smoke testing:**

- OpenStates returns `jurisdiction` as a **dict** (`{"name": "...", "id": "...", "classification": "..."}`), not a flat string. `OpenStatesPerson.from_api()` accepts both shapes for safety.
- Federal members' `current_role.division_id` contains `/state:XX` because they represent specific states. Naive division-id check would mis-classify them as state. `is_federal` uses `jurisdiction_name == "United States"` instead.
- These are documented in the `services/openstates_people.py` `is_federal` docstring and reflected in the "New code modules → openstates_people" section below.

**Round-8 fixes + data-model correction (forthcoming commit):**

The round-8 reviewer flagged silent-false-negative risk in Audit C's jurisdiction filter. User clarified the actual data model: **legislators in the Webflow CMS are mapped against a separate Jurisdiction CMS collection via a multi-reference field**. There is no flat `state-code` or `state` field on the Legislators collection. The audit code was looking at flat fields that don't exist in production.

| Fix | Location | Description |
|---|---|---|
| Jurisdiction-mapping cache | `WebflowLookupService.get_jurisdiction_mapping()` (new) | Fetches the Jurisdictions CMS collection once and returns `{ref_id → 2-letter state code}`. Cached on the service instance for the lifetime of the process. Empty dict on missing collection-id config or fetch failure. Uses the existing `_jurisdiction_cache` pattern from `WebflowSource` (ingestion-side) — same field-name probing (`state-code`, `code`, `abbreviation`, name-prefix). |
| State-code clamping | `WebflowLookupService._normalize_state_code()` (new static) | Coerces value to **exactly two uppercase ASCII letters or None** (was previously ≥2 chars, which let "Florida" → "FLORIDA" leak through and break exact-match jurisdiction filters). |
| Resolver helper | `WebflowLookupService.resolve_jurisdiction_ref()` (new static) | Accepts None / list[str] (multi-reference) / single ref-id / already-2-letter code. Returns normalized 2-letter code or None. **US/federal jurisdiction returns None** because the orchestrator detects federal members via the chamber heuristic. |
| `CMSLegislator.state_code` precomputed | `pipelines/legislator_bio.py` | Was a method that read flat fields. Now a `state_code` field on the dataclass, populated at `from_webflow_item()` time via the optional `jurisdiction_resolver` callable. **No flat-field fallback** — the data model is reference-based. |
| Pipeline jurisdiction resolver | `LegislatorBioPipeline._build_jurisdiction_resolver()` (new) | Builds the resolver once at the start of `audit_*` and `_process_cms_records` / `_discover_and_create`. Jurisdictions collection is fetched once per pipeline lifetime (cached in WebflowLookupService). |
| `is_federal` chamber variants | `_FEDERAL_CHAMBER_VALUES` constant (superseded 2026-04-30; see seat-resolver fix below) | Round-8 low-severity #4: previous heuristic missed `"U.S. Senate"`, `"House of Representatives"`, `"Congress"`, etc. Originally matched a frozenset of common variants case-insensitively after `.strip()`. **Replaced** in the seat-resolver fix because production has no flat `chamber` field at all — federal/state classification lives on a multi-reference `seat` field. |
| Audit C unresolvable-jurisdiction handling | `audit_state_join_keys()` | When the resolver returns None (unknown ref-id, missing jurisdiction, or "US"), `state_code=None`. With a jurisdiction filter set, those records are excluded; with no filter, they are scanned and flagged if missing `openstatesid`. **Tests pin both behaviors** so editors know which audit-only mode surfaces unresolvable records. |
| Test fixtures updated | `tests/test_legislator_bio_foundation.py` | `_cms_item` now takes a `jurisdiction_ref` (list or string ref-id) matching the production multi-reference shape. `_make_pipeline_with_items` mocks `webflow.get_jurisdiction_mapping`. **9 new tests added; total suite: 64 tests, all pass.** |

**Seat-resolver fix (round-16, 2026-04-30):**

First live Audit A run on production returned `total_scanned: 0`. Investigation: every CMS record's `chamber` field was empty, so `is_federal` was False on every record, and Audit A skipped them all. User clarified the actual data model: there is **no flat `chamber` field on Legislators**. Federal/state classification lives on a multi-reference field called `seat` pointing at a separate **Seats CMS collection** (collection ID `655288ef928edb1283067286`) with exactly four items, slugs `us-house`, `us-senate`, `state-house`, `state-senate`.

The fix mirrors the round-8 jurisdiction-resolver shape but is much simpler because the Seats collection is small and semantically fixed (every US state is bicameral except Nebraska's unicameral state senate, which still classifies as state). Hardcoding the federal seat ref-IDs avoids a per-run Webflow fetch + cache layer for a 4-item collection.

| Fix | Location | Description |
|---|---|---|
| Federal seat ref-ID set | `_FEDERAL_SEAT_REF_IDS` constant (`pipelines/legislator_bio.py`) | Frozenset of the two federal Seats CMS item IDs (us-house, us-senate). Replaces `_FEDERAL_CHAMBER_VALUES`. |
| Seat-slug display map | `_SEAT_REF_TO_SLUG` constant | All 4 ref-ID → slug pairs. Used by `CMSLegislator.seat_slugs` so audit reports surface a stable, readable slug. |
| Seat-shape normalizer | `_normalize_seat_refs()` helper | Accepts `None` / single-string / list[str], returns `list[str]`. Defensive against API shape variations. |
| `CMSLegislator.seat_refs` field | `pipelines/legislator_bio.py` | New dataclass field. `is_federal = any(r in _FEDERAL_SEAT_REF_IDS for r in seat_refs)`. The chamber-string read is gone; only the multi-reference `seat` field is consulted. |
| `CMSLegislator.seat_slugs` property | `pipelines/legislator_bio.py` | Maps `seat_refs` → kebab-case slugs via `_SEAT_REF_TO_SLUG`. Drops unknown refs silently. |
| `AuditEntry.chamber` → `AuditEntry.seat: list[str]` | `pipelines/legislator_bio.py` + `tests/test_trigger_legislator_bio_sync.py` | AuditEntry's flagged-record summary now carries a slug list (e.g. `["us-senate"]`) instead of a chamber string. Both audits populate from `cms.seat_slugs`. |
| Test fixture compat shim | `_TEST_SEAT_REFS` in both test files | The `_cms_item` / `_cms` helpers' `chamber=` kwarg ("Senate"/"House"/"upper"/"lower") is translated to the appropriate seat ref-ID list. Existing tests didn't need rewriting. |
| Editor checklist updated | `plans/webflow-legislator-fields.md` | "Do NOT touch" list: dropped `chamber`, added `seat` (multi-ref → Seats) with the federal/state slug semantics documented. |
| Tests | `tests/test_legislator_bio_foundation.py` | Replaced `test_cms_legislator_is_federal_chamber_variants` + `test_cms_legislator_is_federal_state_chamber_values_excluded` with `test_cms_legislator_is_federal_via_seat_ref_id` + `test_cms_legislator_state_seat_refs_not_federal` + `test_cms_legislator_seat_field_accepts_string_or_list_or_none` + `test_cms_legislator_seat_slugs_resolves_known_ref_ids`. **97 tests pass.** |

**Tradeoffs accepted:** if the Seats CMS items are ever deleted and re-created, their item IDs will change and `_FEDERAL_SEAT_REF_IDS` will need a one-line update. This is a much rarer event than the jurisdiction list growing (US has 50 states + DC + 5 territories — actively edited). For a 4-item, semantically-fixed collection, the runtime fetch overhead + cache layer + new env var aren't justified.

---

## Step-9 production rollout findings (2026-04-30)

Phase 1 went live tonight. The PLAN/code passed 15 pm-review rounds and a 97-test suite, but the journey from "ready to deploy" to "shipped" surfaced three production-only realities — none caught by tests because they each depended on the live Webflow schema or live Zapier behavior. Documented here so future syncs starting from a planning doc don't re-walk them.

**Iteration 1 — `chamber` field doesn't exist (commit f235665):**
- **Symptom:** First Audit A run returned `total_scanned: 0` despite the CMS having 32+ federal records.
- **Cause:** PLAN assumed a flat `chamber: str` field on Legislators. Production has a multi-reference `seat` field → Seats CMS (4 items: `us-house`, `us-senate`, `state-house`, `state-senate`).
- **Fix:** Replaced `_FEDERAL_CHAMBER_VALUES` with `_FEDERAL_SEAT_REF_IDS` (frozenset of 2 federal Seats item IDs). Renamed `AuditEntry.chamber` → `AuditEntry.seat: list[str]`. Test fixture's `chamber=` kwarg translates to seat ref-IDs, so existing test cases didn't need rewriting.
- **Generalizable lesson:** PLAN documents drift from production CMS schemas. Always confirm field slugs/types/shapes against live data before writing CMS-touching code. Don't assume what "Do NOT touch" lists in editor checklists actually exist.

**Iteration 2 — URL-typed ID fields rejected plain slugs (commits 350b0d6 + 1c23eb2):**
- **Symptom:** First live PATCH on 5 records all failed with `HTTP 400 — "Expected value to be a valid URL string: 'Mike Haridopolos'"`.
- **Cause:** Editor created `ballotpedia-slug` and `govtrack-id` as Link/URL field types, even though the editor checklist said Plain text. We were sending bare slugs/numeric IDs. Webflow PATCH is **atomic** — a single field validation failure rejects the entire payload, even fields that would otherwise validate. So one URL-typed ID field broke ALL the records' PATCHes.
- **Layered diagnostic gap:** `WebflowError`'s `__str__` was only emitting the status code, not the response body. Body was captured as `error_detail` attribute but never surfaced. **Fix (commit 350b0d6):** include `error_detail` in the exception's str representation so it shows up in API responses + logs.
- **Fix (commit 1c23eb2):** construct canonical URLs in the orchestrator: `https://ballotpedia.org/{slug.replace(' ', '_')}` (the unitedstates YAML occasionally writes the value with spaces, e.g. Mike Haridopolos's freshman entry) and `https://www.govtrack.us/congress/members/{id}`. Per-field decision; we know which fields are URL-typed in production now (confirmed list: `openstates-id`, `ballotpedia-slug`, `govtrack-id`, `contact-form-url`, `official-website`, `photo-source-url`).
- **Generalizable lesson:** Webflow PATCH atomicity means one field-type mismatch surfaces all-or-nothing. Diagnostic iteration is normal — each iteration may surface the next mismatch.

**Iteration 3 — Zapier doesn't support Mustache section conditionals (commit 8653d24):**
- **Symptom:** Slack message rendered with literal `{{#on_failure}}...{{/on_failure}}` text or empty-line placeholders.
- **Cause:** Zapier's Slack action only does flat `{{field}}` interpolation; Mustache section syntax (`{{#flag}}`) is not supported.
- **Fix:** Pre-format two warning strings in the bio-sync payload — `failure_warning` and `large_changes_warning` — set to descriptive text or empty string based on the flags. Slack template drops them in unconditionally; empty strings collapse the line.
- **Generalizable lesson:** Don't push template logic into Zapier; pre-format strings at the source where you already have all the context. Keeps Zapier zaps as dumb interpolators.

**Production run summary:**
```
cms_items_seen: 224
items_resolved_via_openstates: 221
items_resolved_via_bioguide_fallback: 0
patched: 32 (full FL federal delegation)
created: 0
potential_merges: 0
upstream_orphans: 3
errors: []
aborted: false
```
- 192 state records resolved via OpenStates and correctly skipped via Phase-2 stub
- 32 federal records all PATCHed cleanly (every `dropped_fields: []`, no field-existence misses)
- Mike Haridopolos's second-pass diff was just `term-end` + `term-start` (he was fully patched on his solo single-record run earlier — confirms cardinal rule preserves work between runs and only churns genuinely-changed upstream values)

**Carry-overs into Phase 2:**

| Item | What | Where to handle |
|---|---|---|
| 3 state-leg orphans | `dean-black-fl0015`, `dave-smith-fl0038`, `david-silvers-fl0089` — populated `openstates_id` but upstream returned None. Almost certainly former FL state legislators OpenStates dropped. No bioguide fallback (federal-only). Will appear as orphans on every run. | Phase 2: add `bio-sync-locked` field or "manually-marked-historical" flag editors can toggle so these are treated as orphan-by-design, not orphan-by-stale-id. |
| Editor verification | Spot-check the 32 patched records visually in Webflow Designer. | Operator task. |
| Scheduler enable | `legislator_bio_sync.enabled: true` in `sync_schedule.yaml` after editor sign-off + monitoring window. | Operator task once verification passes. |
| Phase 2 state-leg sync | Replace the Phase-2 stub in `_sync_one_record` with the real state-leg path. | New work; PLAN already scopes this in §Phasing. |
| Editor checklist drift | `webflow-legislator-fields.md` had `ballotpedia-slug` + `govtrack-id` typed as Plain text; production was URL. Doc updated; could be worth a check on the other 19 fields' actual types. | If a Phase-2 schema audit endpoint is built, surface this comparison automatically. |

**What worked well (validates the PLAN's design):**
- Per-record error isolation: when iteration 2 broke 5 records' PATCHes, the orchestrator captured each as a separate error and continued — the run wasn't aborted, and we got 5 error bodies in one shot, not just the first one before giving up.
- Cardinal rule (don't blank populated fields): Mike Haridopolos's second-pass diff was much shorter than his first pass; only fields that genuinely changed upstream were sent. No churn on stable fields.
- Schema-cache field-existence tolerance: `dropped_fields: []` on every record means the cache is fetching the live schema and not silently dropping fields. If a field hadn't been added in Webflow, it would appear in `dropped_fields` and the operator would know.
- ALB-timeout safety gate (503 + Retry-After 60): kicked in correctly after the first deploy; user retried and pre-warm completion log appeared.
- Atomic data-model correction: the chamber→seat fix was a single commit (f235665) that mirrored the round-8 jurisdiction-resolver pattern. Test fixtures absorbed the change via a translation table, so no test-by-test rewrite was needed.

---

## Phase 2.5 — OpenStates probe findings + data-extraction layer (2026-04-30)

### Probe results: what's actually in OpenStates `/people` v3 for state legs

Ran `scripts/probe_openstates_state_legs.py` against 10 FL state legislators. Output is the source of truth for Phase 2.5 scope decisions; pre-probe assumptions about what was extractable were partly wrong.

**Reliably populated (worth extracting):**
- `name`, `given_name`, `family_name`, `party`, `gender` — 100%
- `email` — 100% (real `@flhouse.gov` emails for all 10 — NOT contact-form URLs like federal)
- `image` — 100% (myfloridahouse.gov-hosted JPGs)
- `birth_date` — 50% (5 of 10) — partial coverage; gracefully None when missing
- `openstates_url` — 100% (Phase-2.5 NEW — wired to `openstates-id` URL field on Webflow)
- Capitol office (`name`, `voice`, `address`) — 100%
- District office — 90%

**Confirmed NOT available — earlier Phase-2.5 plans were wrong:**
- **Social handles** (twitter/facebook/instagram/youtube) — zero matching URLs in `links[]` across all 10 records. OpenStates simply doesn't carry social media for FL state legs. **Decision:** drop the "parse `links[]` for socials" plan; defer to Phase 3 if/when a different upstream source is found.
- **`current_role.start_date`/`end_date`** — only contains `title`, `org_classification`, `district`, `division_id`. **Decision:** state-leg term dates are unavailable from `/people` v3; defer until a different OpenStates endpoint is probed (or accept "no term dates for state").
- **`roles[]` term history** — not returned by `/people` even with default include set. **Decision:** as above.
- **`biography`** — Phase-0 finding confirmed (0 of 10).
- **`other_identifiers`** — Phase-0 finding confirmed (0 of 10 for state).

**Extractable per-state but not generic:**
- `official-website` — no link has note=`"homepage"` or `"official"`. The closest is note=`"member detail page"` (9 of 10) or first link with a known FL House/Senate host. **Decision:** add a per-state override registry; FL-specific override extracts via the note + host fallback. Other states will need their own overrides as we onboard them.

### Phase 2.5 implementation summary

| Change | Where | Rationale |
|---|---|---|
| `openstates_url` field on `OpenStatesPerson` | `services/openstates_people.py` | Probe confirmed 100% population; previously discarded into `.raw` only. |
| `openstates-id` (URL-typed CMS field) populated | `_build_federal_payload` + `_build_state_payload` | Federal AND state get a "see this person on OpenStates" link with one upstream field. |
| `_STATE_PAYLOAD_OVERRIDES` registry | module-level in `pipelines/legislator_bio.py` | State-by-state extensions without polluting the default state-builder. Default = pass-through; FL = `official-website` extraction via "member detail page" note + host fallback. |
| `_fl_state_override` | module-level | Picks `links[]` entry with note `"member detail page"` (9-of-10 coverage), falls back to first link with host in `_FL_OFFICIAL_WEBSITE_HOSTS`. Best-effort; orchestrator logs + continues if override raises. |

### Round-17 review fixes folded in

- **Scheduler-completion metric event** — `metric=legislator_bio_sync.scheduled_run_completed` log event with `success` flag (False on aborted, exceptions, OR errors > 0). Closes the observability gap that the Zapier alert (which fires from the pipeline's own try/finally — same path for HTTP and scheduler) doesn't fully cover.
- **Job-id-collision cleanup** — `scheduler.start()` removes both `daily_legislator_bio_sync` and `weekly_legislator_bio_sync` ids before re-registering. Prevents stale cron from a previous frequency continuing to run alongside the new one across config reloads.
- **State-path edge-case tests** — None birth_date, populated-OpenStates with no capitol office, FL override happy path + host-fallback path, non-FL state skips override (registry keying confirmed).
- **`openstates-id` wiring tests** — both federal and state paths get the field when `os_record.openstates_url` is populated.
- **Rollback playbook** — added to §Rollback procedure with per-record vs mass-revert distinction; `--undo-last-run` capability listed as backlog.
- **Editor sign-off acceptance criteria** — table in §Rollout sequence with field-by-field expectations + sample-size threshold.

### Post-Phase-2.5 ChurnPATCH fixes (2026-04-30)

First post-Phase-2.5 production dry-run revealed two more storage-format-vs-payload mismatches:

**`openstates-id` URL trailing slash:** OpenStates' `openstates_url` ends with `/` (e.g. `https://openstates.org/person/x-xxx/`). Webflow's URL field strips the trailing slash on storage, so every run saw `cms = "...x"` vs `upstream = "...x/"` → diff → re-PATCH. Fix: `.rstrip("/")` before sending. 222 of 224 records were churning (the 2 that stuck were ones where OpenStates happened to not emit a trailing slash). Also fixes a similar pattern that would arise if any future URL-typed field's upstream value carries a trailing slash that Webflow drops.

**`email` lowercase normalization:** Webflow's email field lowercases on storage. FL House emails were already lowercase upstream so the probe didn't catch this; FL Senate (and likely other states) had mixed-case emails that Webflow normalized, churning every run. Fix: `email.lower()` before sending. 189 of 192 state records were churning (the 3 that stuck likely had URL-shaped emails routed to `contact-form-url` instead).

**Generalized lesson:** Webflow URL and email field types apply normalization on storage. When we add a new write target, the storage format may differ from what we send, surfacing only in the second run as ChurnPATCH. Mitigation pattern: send the canonical/storage form (lowercase email, no-trailing-slash URL) at payload-build time so the diff round-trips correctly.

3 new tests pin both fixes (mixed-case email lowercased, lowercase email no-churn-on-rerun, openstates-id no-churn-when-cms-no-trailing-slash).

### What we deliberately deferred

- **Social handles for state legs** — confirmed not available from OpenStates. Phase 3 if a different upstream source is found.
- **State-leg term dates** — confirmed not available from OpenStates `/people` v3. Could probe `/orgs/{id}/memberships` or similar in a future iteration; for now, accept "no term dates for state" and note that the cardinal rule preserves any editor-populated values.
- **`--undo-last-run` mass-revert capability** — backlogged. Pre-deploy guardrails (dry-run + Audit B + per-record error isolation) carry the safety load until then.
- **Per-state photo-upload pipeline** — Phase 3 (per the original PLAN). Currently we just store the OpenStates `image` URL; quality/stability varies but the URL-typed field accepts it.

---

## Phase 0 probe findings (completed 2026-04-29)

These findings shape the design below.

| Question | Finding |
|---|---|
| Does OpenStates `other_identifiers` include `bioguide` for federal? | ✅ Yes (verified for Rick Scott — full ID set returned: bioguide, fec, govtrack, opensecrets, ballotpedia, wikidata, votesmart, lis) |
| Does OpenStates retain departed federal members? | ❌ No — Karen Bass (left Congress Jan 2023) returns 0 results from `/people?jurisdiction=us&name=Karen Bass`. **Bioguide-id fallback is mandatory for federal historical entries.** |
| State `other_identifiers` coverage | Mostly empty — FL/AZ samples returned `[]`; MA only `legacy_openstates`. Cross-source IDs are federal-only in practice. |
| State PII risk | Clean — capitol/district *offices* only, government emails (`@flhouse.gov` etc.), no home addresses |
| Federal PII risk | Clean — capitol offices, DC phones, contact forms |
| Federal `email` quirk | Often a contact-form URL, not an email (Rick Scott's `email`: `https://www.rickscott.senate.gov/contact/contact`) — sync detects URL shape and routes to `contact-form-url` |
| Photo source variance | Federal: clean predictable URL (`unitedstates.github.io/images/congress/450x550/{bioguide}.jpg`). State: per-state CDNs of varying reliability (FL House CMS, MA legislature, AZ third-party Apptegy). Justifies isolating photo upload as Phase 3. |
| `bio.birthday` availability | 100% for federal current + historical; mixed for state (MA had it, FL/AZ did not). Store year-only on CMS. |

**ID-field coverage on 536 current federal members:**

| ID | Coverage | Notes |
|---|---|---|
| `bioguide` | 100% | Universal join key |
| `wikidata` | 100% | |
| `govtrack` | 100% | |
| `fec` | 99% | List-typed; skip for now |
| `opensecrets` | 97% | |
| `ballotpedia` | 97% | |
| `votesmart` | 97% | Skip — no current use |
| `thomas` | 41% | Skip — sparse |
| `lis` | 18% | Skip — sparse |

---

## Identity & join strategy

Single primary join key — **`openstatesid`** — for all jurisdictions (state + federal current). One sync code path, no per-jurisdiction branching.

`bioguide-id` is **stored** on every federal record but **only joined on as a fallback** for federal CMS entries that no longer resolve in OpenStates (i.e., departed members):

```
                              ┌─ Found in OpenStates?  → use openstatesid
Federal CMS entry lookup  ────┤
                              └─ Not found?            → fall back to bioguide-id,
                                                         join to legislators-historical.yaml
```

State legislators only ever use `openstatesid` (no historical YAML for state).

**Why no separate DDP-internal ID:** The Webflow item ID + slug already serve as DDP's persistent identity. Adding a third key would duplicate identity infrastructure already in place.

**Cross-source IDs (stored, not joined):** `bioguide-id`, `wikidata-id`, `opensecrets-id`, `ballotpedia-slug`, `govtrack-id`. Populated automatically. Used by the website for outbound reference links to those platforms — even though we don't ingest data from those sources yet, we get the IDs for free from the unitedstates dataset.

---

## Field precedence & overwrite policy (Phase 1 prerequisite)

This was marked deferred in an earlier draft; pm-review correctly flagged that it must be decided **before any write**, because the wrong rule can silently erase editor work.

### The cardinal rule: never blank a populated CMS field with an empty upstream value

```python
# Values empirically observed as "no data" markers in OpenStates / unitedstates
# feeds. Phase 0 probe saw None, missing key, and "" for unpopulated state-leg
# birth_date / biography. The remainder are defensive guards against placeholder
# sentinels that may appear later (per pm-review round 2 recommendation).
EMPTY_VALUES = {
    None, "",
    "-", "—",
    "N/A", "n/a", "NA", "na",
    "TBD", "tbd",
    "UNKNOWN", "unknown",
    "null", "NULL",
}

def is_empty(value) -> bool:
    if value in EMPTY_VALUES:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, (list, dict)) and not value:
        return True
    return False

def should_write(field_name: str, cms_value, upstream_value) -> bool:
    if is_empty(upstream_value):
        return False                  # never overwrite with empty
    if cms_value == upstream_value:
        return False                  # no-op, skip
    if field_name in LOCKED_FIELDS:
        return False                  # editor-curated, never sync-overwrite
    return True
```

**Numeric zero is intentionally excluded from `EMPTY_VALUES`.** A `district` of `0` (some at-large jurisdictions) is a valid value, not an empty. Per-field validators can override this behavior if a field's `0` is meaningful-as-empty (none currently identified).

**Discovered sentinels:** Phase 1 dry-run output explicitly logs every value that triggers `is_empty()` so editors can review whether the rule needs to be tightened or loosened. New sentinels get added to `EMPTY_VALUES` as evidence accumulates — not pre-emptively guessed.

Editors can fill a field manually; if upstream loses that data later, the CMS keeps the editor's value. The cost is some "drift" between CMS and upstream, but the alternative — silently blanking editor work — is the larger trust risk.

### Source precedence per field

When **both** unitedstates and OpenStates have a value for the same federal member, this matrix decides:

| Field | Federal precedence | State precedence |
|---|---|---|
| Name (display) | unitedstates `name.official_full` | OpenStates `name` |
| Birth year | unitedstates `bio.birthday` (year) | OpenStates `birth_date` (year) |
| Gender | unitedstates `bio.gender` | OpenStates `gender` |
| Capitol phone / address | unitedstates `terms[-1]` | OpenStates office where `classification=capitol` |
| District phone / address | (n/a) | OpenStates office where `classification=district` |
| Contact form URL | unitedstates `terms[-1].contact_form` | OpenStates `email` if URL-shaped |
| Email | (federal usually blank) | OpenStates `email` if email-shaped |
| Official website | unitedstates `terms[-1].url` | OpenStates `links[].url` where `note=website` |
| Term start / end | unitedstates `terms[0].start` / `terms[-1].end` | OpenStates `current_role` (no full term history) |
| Photo source URL | derived from bioguide-id | OpenStates `image` |
| Twitter / Facebook / Instagram / YouTube | unitedstates social-media YAML | OpenStates `other_identifiers` (rarely populated for state) |
| All cross-source IDs | unitedstates `id` block | OpenStates `other_identifiers` |

Content team sign-off on this matrix is a Phase 1 prerequisite, not deferrable.

### Multi-office address resolution

Sources may return >1 office. Selection rules:

- **Federal:** always use `terms[-1]` (latest term) for office address/phone. If multiple offices exist in one term, prefer the one named "Capitol Office" or fall back to the first entry.
- **State:** prefer `classification=capitol` for `phone-capitol` / `office-address-capitol`; prefer `classification=district` for `phone-district` / `office-address-district`. If a state has only one office (most common), populate the matching slot and leave the other blank.
- **Multiple district offices** (e.g., Rick Scott has 7 FL district offices): pick the first one returned by OpenStates. Phase 1 limitation; Phase 2 may add a `district-offices` repeater field if editors push for it.

### Per-field sync opt-out

- **Repo-level (Phase 1):** `legislator_bio_sync.locked_fields: [official-website, contact-form-url]` in `sync_schedule.yaml` disables those fields for all records. Used when DDP wants editor-only control over a field globally.
- **Per-record (deferred):** if usage shows individual records need protection, Phase 2 can add a `bio-sync-locked` boolean on the Legislators collection.

---

## Architecture

Mirrors the bill-sync **Flow 1 / Flow 2 decoupling** pattern (`pipelines/bill_version.py`, documented in `TROUBLESHOOTING.md` → "Data Flow Decoupling 2026-03-11").

```
                        ┌── unitedstates YAMLs (federal current + historical + social)
fetch_all_sources ──────┤
                        └── OpenStates /people    (state + federal current)
                                      │
                                      ▼
                          build_legislator_payloads
                                      │
                  ┌───────────────────┴──────────────────┐
                  ▼                                      ▼
          Flow A: Webflow CMS                    Flow B: Pinecone
       (NEW — bio/contact/term/social)        (existing — RAG content)
        PATCH-or-create-as-draft                  no behavior change
```

Either flow can fail independently without blocking the other — same guarantee the bill sync provides today.

---

## New code modules

### `src/ddp_sync/services/congress_legislators.py`

YAML fetcher with on-disk cache (24h TTL). Cache lives at `~/.cache/ddp-sync/congress-legislators/{filename}` to avoid re-downloading the 8.6 MB historical file every run.

```python
class CongressLegislatorsSource:
    """Loads and caches the unitedstates/congress-legislators YAML dataset."""

    BASE_URL = "https://unitedstates.github.io/congress-legislators"
    CACHE_TTL_SECONDS = 24 * 60 * 60
    CACHE_DIR = Path.home() / ".cache" / "ddp-sync" / "congress-legislators"
    FILES = (
        "legislators-current.yaml",
        "legislators-historical.yaml",
        "legislators-social-media.yaml",
    )

    async def warm_cache(self) -> None: ...
    async def get_by_bioguide(self, bioguide_id: str) -> CongressLegislator | None: ...
    async def iter_current(self) -> AsyncIterator[CongressLegislator]: ...
    async def iter_historical_since(
        self, end_date: date
    ) -> AsyncIterator[CongressLegislator]:
        """Historical members whose last term ended on/after end_date."""

@dataclass
class CongressLegislator:
    bioguide_id: str
    name: dict       # {first, last, middle?, official_full}
    bio: dict        # {birthday, gender}
    terms: list[dict]
    ids: dict        # {bioguide, wikidata, opensecrets, ballotpedia, govtrack, fec, ...}
    social: dict     # {twitter, facebook, instagram, youtube, ...}
```

### `src/ddp_sync/services/openstates_people.py` — IMPLEMENTED

Async client over OpenStates v3 `/people`. Takes a shared `RateLimiter` so bio-sync can coordinate its OpenStates budget with future pipelines. Distinguishes 404 (returns None — bioguide-id fallback signal) from hard failures (raises `OpenStatesRateLimitError` on persistent 429, `OpenStatesError` on other non-2xx).

```python
class OpenStatesError(Exception): ...
class OpenStatesRateLimitError(OpenStatesError): ...

@dataclass
class OpenStatesPerson:
    openstates_id: str       # ocd-person/...
    name: str
    family_name, given_name, party, gender, birth_date, death_date
    email, image, biography
    jurisdiction_name: str | None    # "United States" for federal, state name otherwise
    current_role: dict
    other_identifiers, other_names, links, sources, offices: list[dict]
    raw: dict                # full upstream record

    def get_other_id(self, scheme: str) -> str | None: ...
    @property
    def chamber(self) -> str | None: ...      # upper / lower
    @property
    def district(self) -> str | None: ...
    @property
    def is_federal(self) -> bool:
        """True iff jurisdiction_name == 'United States'.
        Probe-confirmed: federal members' division_id contains /state:XX
        because they represent specific states — naive division-id check
        would mis-classify them as state."""

class OpenStatesPeopleClient:
    BASE_URL = "https://v3.openstates.org"
    INCLUDE_PARAMS = ("other_names", "other_identifiers", "links", "sources", "offices")

    def __init__(self, api_key, rate_limiter, *, max_retry_attempts=3, per_page=50): ...

    async def fetch_by_id(self, openstates_id) -> OpenStatesPerson | None:
        """Returns parsed record on 2xx, None on 404. Raises on persistent 429
        or other non-2xx."""

    async def iter_jurisdiction(self, jurisdiction) -> AsyncIterator[OpenStatesPerson]:
        """Paginated /people?jurisdiction=X. Always passes the full include set."""

    @staticmethod
    def extract_other_id(person: dict, scheme: str) -> str | None: ...
```

Verified against the live API (4 smoke tests with the dev key): Rick Scott returns with full ID set + `is_federal=True`; nonexistent ID returns None; `extract_other_id` static helper works; `iter_jurisdiction("fl")` yields state legislators paginated.

### `src/ddp_sync/pipelines/legislator_bio.py` — IMPLEMENTED (commit c555f38)

Orchestrator. For each CMS legislator: look up upstream (OpenStates first; bioguide-id fallback for federal historical), build a field payload via the source-precedence matrix, diff against the current CMS state via `should_write()` / `is_empty()`, PATCH (or append to dry-run report). Optional auto-create discovers upstream-only members and either creates drafts or flags potential merges.

State legislator handling is a clear Phase 2 stub (logged + skipped, not silent no-op).

**Module-level helpers** (cardinal-rule logic — see `Field precedence & overwrite policy` section):
- `is_empty(value)` — recognizes the `EMPTY_VALUES` sentinels, whitespace-only strings, and empty containers. Container-safe (lists/dicts checked structurally before the membership test, since they'd be unhashable). Numeric zero intentionally preserved.
- `should_write(field_name, cms_value, upstream_value, *, locked_fields=())` — applies the cardinal rule plus locked-fields opt-out.
- `split_email_field(value)` → `(email, contact_form_url)` — URL-shaped values route to `contact-form-url`; bare emails to `email`.

**Public API:**

```python
@dataclass
class BioSyncOptions:
    target: Literal["all", "webflow", "pinecone"] = "all"
    jurisdiction: str | None = None        # None = all configured. "us" for federal.
    auto_create: bool = False
    dry_run: bool = False
    limit: int = 0
    historical_since: date = date(2023, 1, 1)
    locked_fields: tuple[str, ...] = ()


@dataclass
class BioSyncReport:
    cms_items_seen: int = 0
    items_resolved_via_openstates: int = 0
    items_resolved_via_bioguide_fallback: int = 0
    would_patch: list[dict]
    would_create: list[dict]
    potential_merges: list[dict]
    upstream_orphans: list[dict]
    errors: list[str]
    aborted: bool = False
    abort_reason: str | None = None


class LegislatorBioPipeline:
    def __init__(self, *, settings=None, webflow=None, congress=None,
                 openstates=None, openstates_rate_limiter=None): ...

    async def run(self, options: BioSyncOptions) -> BioSyncReport:
        await self.congress.warm_cache()           # idempotent; pre-warmed at startup
        await self._process_cms_records(options, report)
        if options.auto_create:
            await self._discover_and_create(options, report)
        return report
```

**Error-handling contract:** rate-limit errors (`WebflowRateLimitError`, `OpenStatesRateLimitError`) abort the run cleanly — `report.aborted=True`, `abort_reason` set, partial state returned. Per-record `WebflowError` / `OpenStatesError` append to `report.errors` with `f"{slug}: {type(e).__name__}: {e}"` and the run continues. Unhandled exceptions are caught at the per-record boundary, logged via `logger.exception`, and added to `errors[]`.

**Federal payload builder** (`_build_federal_payload`) implements the source-precedence matrix from the PLAN: unitedstates wins for federal-specific fields (term span, capitol office, social, cross-source IDs, photo URL derived from bioguide); OpenStates fills any gaps (capitol office fallback when unitedstates is missing it; email/contact-form-url routing via `split_email_field`).

**State→federal merge detection** (`_find_merge_candidate`): multi-signal scoring requiring ≥2 signals — `name_match` (last-name + first-initial), `birth_year_match`, `bioguide_match` (decisive — counts as score=2). Auto-create skips candidates with a flagged merge; the orchestrator surfaces them in `report.potential_merges` for editor review.

**Phase 1 interim — 3 signals shipped (round-7 plan correction):** the original spec called for 5 signals (Jaro-Winkler full-name similarity ≥0.85; term-continuity within 2 years). Implementation ships the 3 listed above; the two deferred signals would require a new dependency (`jellyfish` or similar for Jaro-Winkler) plus the orchestrator carrying state-leg term-end data through the CMS index. False-positive rate measurement during the rollout dry-run will tell us whether the missing signals are needed for Phase 1 or can wait for Phase 2. **Editor communication note:** when the auto-create discovery first runs, surface a clear caveat in the dry-run report header that merge candidates are flagged from a 3-signal interim heuristic and may have higher false-positive rate than the eventual 5-signal version.

Smoke-tested end-to-end with mocked sources: Rick Scott (federal current — 17 fields PATCH'd), Karen Bass (historical federal via bioguide fallback — 7 fields), state record (correctly orphaned).

### Webflow PATCH support — extend `WebflowLookupService`

The existing `services/webflow_lookup.py::WebflowLookupService` is **already the Webflow write service** (despite its misleading name). It's used by `pipelines/bill_version.py` for Flow 1 status PATCHes. **No new service class is needed.** We extend it.

What's missing in the existing service: rate-limiter, 429/Retry-After handling, draft-create endpoint, field-existence tolerance. Adding these here means **bill sync inherits all four improvements** automatically.

**Webflow tier:** 120 req/min on the current plan. Process-local in-instance limiter at **≤60 req/min** for the shared write service leaves 60 req/min headroom for other writers (the unified-sync API, ad-hoc trigger calls). Cross-process / multi-worker coordination is out of scope for Phase 1 — single ddp-sync deployment, one APScheduler leader, so the in-process limiter is sufficient.

```python
# services/webflow_lookup.py — proposed extensions

class WebflowLookupService:
    # Existing: __init__, update_bill_fields, update_bill_gov_url

    def __init__(self, settings=None, *, max_requests_per_minute: int = 60):
        ...
        # Shared limiter — protects ALL pipelines using this service in the
        # same Python worker (bill sync, bio sync, future writers).
        self._limiter = TokenBucketLimiter(
            rate=max_requests_per_minute / 60.0,
            capacity=max_requests_per_minute,
        )
        self._legislators_collection_id = self.settings.webflow_legislators_collection_id

    async def _patch_with_backoff(self, url, headers, payload) -> httpx.Response:
        """Shared PATCH wrapper — limiter, 429 retry, Retry-After honoring.
        Raises WebflowRateLimitError if 429 persists after final retry.
        Non-2xx responses other than 429 are returned for caller to inspect."""
        last_resp = None
        for attempt in range(3):
            await self._limiter.acquire()
            last_resp = await self._client.patch(url, headers=headers, json=payload)
            if last_resp.status_code != 429:
                return last_resp
            wait = float(last_resp.headers.get("Retry-After", 2 ** attempt))
            await asyncio.sleep(wait + random.uniform(0, 0.5))   # jitter
        raise WebflowRateLimitError(
            f"Webflow returned 429 after 3 retries: {url}",
            response=last_resp,
        )

    # NEW for bio sync:

    async def update_legislator_fields(
        self,
        webflow_id: str,
        field_data: dict,
        *,
        publish: bool = True,
    ) -> WebflowPatchResult:
        """PATCH a Legislators item. Returns a WebflowPatchResult capturing
        success/failure and dropped fields. Raises WebflowRateLimitError on
        persistent 429; raises WebflowError on other non-2xx — callers MUST
        treat these as run failures and surface in BioSyncReport.errors."""

    async def create_legislator_draft(self, field_data: dict) -> WebflowCreateResult:
        """POST /collections/{id}/items with isDraft=true. Returns the new
        webflow_id on success; raises WebflowError on non-2xx."""
```

### Error contract (round-3 addition)

```python
class WebflowError(Exception):
    """Base for any non-success Webflow response."""
    def __init__(self, message: str, *, response: httpx.Response | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code if response is not None else None

class WebflowRateLimitError(WebflowError):
    """Raised when Webflow returns 429 after the configured retry budget is exhausted."""

@dataclass
class WebflowPatchResult:
    success: bool                  # True iff response was 2xx
    webflow_id: str
    dropped_fields: set[str]       # fields that don't exist in the collection
    status_code: int
    error_detail: str | None = None
```

**Caller contract:**
- `update_legislator_fields()` raises on non-2xx (including persistent 429). Callers in `pipelines/legislator_bio.py` catch `WebflowError`, log via structlog, and append to `BioSyncReport.errors`. The run continues for the next CMS item — one bad PATCH does not abort the whole run.
- `WebflowPatchResult.success=False` only ever appears when the PATCH succeeded HTTP-wise but had partial issues (e.g., `dropped_fields` non-empty for incremental schema rollout). Never a silent error mask.
- `bill_version.py`'s existing `update_bill_fields()` is migrated to the same contract (raises on non-2xx instead of returning False) — this is a behavior change for bill sync; **migration plan** in "Backwards compatibility" below.

### Backwards compatibility (round-4 revision)

`WebflowLookupService.__init__` gains a new `max_requests_per_minute` keyword argument (default 60). All existing call sites use the no-argument form, so this is purely additive.

**On the `update_bill_fields` migration** — round-3 review recommended changing its return contract from `bool` to a raising `WebflowPatchResult`. After implementation, this was reconsidered: the existing callers in `pipelines/bill_version.py:264-285, 373-401` already wrap the call in `try/except + bool check`, which is **semantically equivalent** to the raising contract. Forcing the migration would touch the bill-sync code without a behavior change. Decision:

- `update_bill_fields()` keeps the legacy `bool` return contract — **no caller migration needed.**
- It now routes through the new `_patch_with_backoff()` helper internally, so it inherits the rate-limiter and 429 retry. `WebflowRateLimitError` raised inside the helper is caught by the existing `try/except` and converted to `False` — exactly what the legacy callers expect.
- New methods `update_legislator_fields()` and `create_legislator_draft()` use the raising contract for bio-sync's needs (where one PATCH failing should land in `BioSyncReport.errors` rather than be papered over with a False).

This satisfies the round-3 reviewer's underlying concern (no silent data loss; persistent 429 always raises somewhere) without forcing any change to existing pipelines.

**Renaming `WebflowLookupService` → `WebflowWriteService`** is a pure rename and held as a follow-up commit. Touches every import site (`pipelines/bill_version.py:264, 373` and others); not blocking for Phase 1.

### Single-worker assumption (round-3 addition)

The in-process limiter is sufficient **only if there is exactly one Python process making Webflow writes at a time**. Phase 1 explicitly assumes:
- One ddp-sync deployment (one EC2 host, one systemd unit `ddp-sync.service`)
- APScheduler runs in a single worker (single uvicorn process — confirm with `ps aux | grep uvicorn` after deploy; multi-worker uvicorn would breach this assumption)
- Ad-hoc CLI / `/trigger` endpoint invocations against the same host go through the same `WebflowLookupService` instance (same process), so they share the limiter naturally
- `sync_unified.py`'s leader-election (Redis-based) ensures only one worker runs scheduled jobs

**Guardrail:** at process startup, log a warning if `UVICORN_WORKERS > 1` is detected. Documented in deployment README.

**CLI dry-run + scheduled job overlap:** they share the same Python process and same `WebflowLookupService` singleton, so they share the limiter. Two simultaneous bursts cannot exceed the configured per-process budget.

**When this assumption breaks:** multi-worker, multi-host, or multi-region deployments need a Redis-backed cross-process limiter. This is documented as a Phase 2+ requirement gated on infrastructure changes — not a current need.

### OpenStates client + shared rate-limiter (round-3 revision)

Round-3 review reversed the earlier deferral: `RateLimitConfig` extraction is a **Phase 1 deliverable**, not a follow-up.

**Phase 1 step (atomic with the new modules):**
1. Create `services/rate_limiter.py` exposing `RateLimitConfig` (dataclass, behavior-preserving move) + a new stateful `RateLimiter` class wrapping `apply()` (was `_apply_rate_limit`)
2. Migrate `pipelines/legislator_sync.py` and `pipelines/bill_sync.py` to import from `services/rate_limiter.py` (their inline copies are deleted; method calls update from `self._apply_rate_limit()` to `self.rate_limiter.apply()`)
3. New `services/openstates_people.py` uses the same shared module — three callers, one source of truth
4. CI runs existing legislator-sync + bill-sync tests against the migrated code before merge

This avoids the "sideways import" coupling round-3 flagged (`legislator_bio.py` importing from `legislator_sync.py`) and eliminates the existing duplication in one motion. Bumps `Retry-After` honoring from partial (`fetch_sponsored_bills` only) to all OpenStates calls. OpenStates capped ≤50 req/min for bio sync.

**Concurrency safety (round-4 fix):** `RateLimiter.apply()` holds an `asyncio.Lock` for the whole compute-and-sleep window so `asyncio.gather()`-style concurrency cannot bypass the inter-call gap by reading `_last_request_time` before another task updates it. The limiter exposes an `enforced_sleeps` counter for runtime observability. None of the current call sites use `gather()` over rate-limited operations — they're sequential awaits in for-loops — so this is future-proofing.

**API consistency (round-4 fix):** `CongressLegislatorsSource` exposes only async public methods. `get_by_bioguide()`, `iter_current()`, `iter_historical_since()` all auto-warm the cache on first call so callers don't have to track warm state. The previous sync `get_by_bioguide()` that raised on unwarmed cache has been removed in favor of the async signature.

**`from_yaml` contract (round-4 fix):** `RateLimitConfig.from_yaml()` **never raises** — returns defaults on missing file, malformed YAML, or missing keys. Existing pipelines depend on this (their prior inline `_load_rate_limit_config` did the same). Documented in the docstring.

### State→federal transition detection (strengthened from pm-review)

Initial last-name + state heuristic is too weak (false-positives on common surnames, misses on hyphenation/marriage). Replaced with a multi-signal scoring approach: a candidate is flagged only when **two or more** signals match.

```python
def find_merge_candidate(
    new_member: CongressLegislator,
    cms_index: CMSIndex,
) -> tuple[CMSItem | None, list[str]]:
    """Returns (candidate, matched_signals). Candidate only flagged if score >= 2."""
    state = new_member.terms[-1]["state"]
    new_full = (new_member.name.get("official_full") or
                f"{new_member.name['first']} {new_member.name['last']}").lower()
    new_birth_year = _year_from_iso(new_member.bio.get("birthday"))
    new_first_term_start = new_member.terms[0]["start"]

    best, best_signals = None, []
    for cms_item in cms_index.by_state(state):
        signals = []
        # Signal 1: family name AND first-name initial match (filters "Smith" noise)
        if (cms_item.family_name.lower() == new_member.name["last"].lower()
                and cms_item.given_name[:1].lower() == new_member.name["first"][:1].lower()):
            signals.append("name_match")
        # Signal 2: full-name similarity >= 0.85 (Jaro-Winkler, handles hyphenations)
        if jaro_winkler(cms_item.full_name.lower(), new_full) >= 0.85:
            signals.append("name_similarity")
        # Signal 3: birth year matches (federal always has it; state sometimes)
        if new_birth_year and cms_item.birth_year == new_birth_year:
            signals.append("birth_year_match")
        # Signal 4: bioguide-id explicitly present and matches
        if cms_item.bioguide_id and cms_item.bioguide_id == new_member.ids["bioguide"]:
            signals.append("bioguide_match")    # decisive; treat as score=2
        # Signal 5: term continuity (state-leg ends before/around federal term start)
        if cms_item.term_end and abs((cms_item.term_end - new_first_term_start).days) <= 730:
            signals.append("term_continuity")

        score = 2 if "bioguide_match" in signals else len(signals)
        if score >= 2 and len(signals) > len(best_signals):
            best, best_signals = cms_item, signals

    return best, best_signals
```

Editor sees: *"Potential merge: existing CMS item 'Jane Smith-Garcia (FL state senate)' looks like the same person as new federal member 'Jane Smith (FL US House)'. Matched signals: name_similarity, birth_year_match. Review and merge if so."*

The dry-run report surfaces all candidates; auto-create is skipped for any flagged item. False-positive rate will be measured during the rollout — see Testing & rollout section.

### Federal `email` URL detection

```python
def split_email_field(value: str) -> tuple[str | None, str | None]:
    """Returns (email, contact_form_url). At most one is non-None."""
    if not value:
        return None, None
    if value.startswith(("http://", "https://")):
        return None, value
    return value, None
```

---

## Configuration

Extends `config/sync_schedule.yaml`:

```yaml
legislator_bio_sync:
  enabled: true
  sync_time_utc: "06:00"            # piggyback on existing weekly slot
  frequency: weekly
  sync_day: sunday

  # Federal: pull all 535 current + departed since 2023-01-01
  federal:
    enabled: true
    historical_since: "2023-01-01"
    auto_create: false              # editors review before publishing

  # State: per-jurisdiction toggles
  state:
    enabled: true
    auto_create: false
    jurisdictions:                  # empty = inherit active_jurisdictions
      - FL
      - VA
      - WA
      - UT
      - AZ
      - MI
      - MA

  # Source data refresh
  unitedstates_cache_ttl_hours: 24
```

---

## API endpoints

New trigger endpoint (mirrors the existing `/trigger/bill-version-check` pattern):

```
POST /ddp-sync/v1/trigger/legislator-bio-sync
```

**Query params:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `dry_run` | bool | false | Preview the diff without writing |
| `auto_create` | bool | (config) | Override per-run |
| `jurisdiction` | str | (all configured) | Limit to one state code (e.g. `FL`, `us`) |
| `target` | enum | `all` | `all` / `webflow` / `pinecone` |
| `limit` | int | 0 | Cap items processed (0 = unlimited) |
| `historical_since` | date (YYYY-MM-DD) | `2023-01-01` | Federal historical backfill cutoff |
| `audit_only` | enum | none | `A` / `C` — skip the sync, return just the named audit report |

**Returns:** `BioSyncReport` JSON.

**Use cases:**
- Pre-bulk-create dry run: editors run `?dry_run=true&jurisdiction=FL` before bulk-creating FL state-leg CMS entries to preview what would be PATCHed
- On-demand backfill: `?jurisdiction=us&historical_since=2023-01-01` to pull post-2022 departed members
- Single-state testing during rollout

### ALB-timeout safety gate (round-7 fix)

The orchestrator's first call awaits `congress.warm_cache()`, which on a cold container takes ~55s wall-clock (8.6 MB historical YAML parse, off-loop via `asyncio.to_thread`). The startup pre-warm (round-6 fix) usually finishes before any trigger arrives, but a request can race the pre-warm task on a freshly-scaled-up container.

Mitigation: the trigger endpoint **gates on the warm flag** and returns **HTTP 503 with `Retry-After: 60`** when the cache is not yet warmed:

```python
@router.post("/trigger/legislator-bio-sync")
async def trigger_legislator_bio_sync(request: Request, ...):
    source = getattr(request.app.state, "congress_legislators", None)
    if source is None or not source._warmed:
        raise HTTPException(
            status_code=503,
            detail="Bio-sync source still warming up; retry in ~60s",
            headers={"Retry-After": "60"},
        )
    # ... rest of handler
```

This is honest about the actual state and avoids a 30s ALB idle timeout on the cold-start path. The cost is a brief unavailability window (typically <60s after a deploy or fresh-container start). Acceptable trade-off for Phase 1; if the window becomes operationally annoying, Phase 2+ can persist the parsed object to disk for instant load.

### Audit-only mode

The `audit_only` query param skips the sync and returns just an audit report:
- `audit_only=A`: federal join-key coverage (the orchestrator scans all federal CMS records and lists those missing both `openstatesid` and `bioguide-id`)
- `audit_only=C`: pre-existing state CMS records lacking `openstatesid`, scoped by `jurisdiction` if provided

**Audit B** was originally scoped as "editor toolchain only" but moved into ddp-sync's trigger endpoint (2026-04-30, post-Phase-1-ship) so the scheduler can be unblocked without standing up a separate toolchain. Same shape as A and C: `POST /trigger/legislator-bio-sync?audit_only=B` returns an `AuditReport`. See `LegislatorBioPipeline.audit_bulk_import_readiness`.

---

## Webflow CMS schema additions

**See companion document `plans/webflow-legislator-fields.md`** for the editor-facing field-add checklist (21 new fields, all optional).

The sync code will **gracefully no-op on any field not yet present** in Webflow — it catches `field not found` errors during PATCH and logs a warning. This means the field rollout can happen incrementally without breaking sync.

---

## Pre-flight audits (must pass before first live write)

Two audits gate the rollout. Both run as commands inside the new pipeline (`--audit-only` flag) and produce reports for editor review.

### Audit A — Federal join-key coverage

For every CMS legislator with `chamber` in (Senate, House) — i.e., federal — verify that **at least one** of `openstatesid` or `bioguide-id` is populated. If both are missing, the sync cannot resolve the upstream record without a name-match (which has known false-positive risk).

```python
async def audit_federal_join_keys(cms_index) -> AuditReport:
    """Surface federal CMS records that lack both join keys."""
    missing = [item for item in cms_index.federal()
               if not item.openstatesid and not item.bioguide_id]
    return AuditReport(
        total_federal=cms_index.federal_count(),
        missing_both_keys=missing,
    )
```

**Action on findings:** editors manually populate `bioguide-id` (preferred — stable join key) for each flagged record before auto-create is enabled. Single-pass remediation; should be a one-time clean-up.

### Audit B — Bulk-import readiness

Before the first scheduled run after editors complete the bulk-create of state-legislator scorecards, run `--audit-only` to verify:
- Every newly-created CMS legislator has `openstatesid` populated
- No two CMS records share an `openstatesid` (would indicate a duplicate created during bulk-import)

Sequencing rule: **first scheduled sync runs only after Audit B passes.** Before that, only manual dry-run via the trigger endpoint is permitted.

### Audit C — Pre-existing state CMS records lacking `openstatesid`

Audit B catches duplicates introduced *during* the bulk-create. Audit C catches the orthogonal case: state CMS records that **already** existed before the bulk-create and never had `openstatesid` populated. Without this audit, auto-create could silently duplicate a pre-existing record by creating a new draft for the same person.

```python
async def audit_state_join_keys(cms_index) -> AuditReport:
    """Surface state CMS records without openstatesid (pre-existing, not bulk-import)."""
    missing = [item for item in cms_index.state()
               if not item.openstatesid]
    return AuditReport(
        total_state=cms_index.state_count(),
        missing_openstatesid=missing,
    )
```

**Action on findings:** editors populate `openstatesid` (preferred) or mark records as `bio-sync-skip: true` (Phase 2 field) before auto-create is enabled per jurisdiction. **Auto-create cannot be enabled for a jurisdiction until Audit C is clean for that jurisdiction.**

---

## Run-summary alerting

Reuses the existing repo pattern: `pipelines/voatz_brevo.py::push_alert_to_zapier()` posting to `ZAPIER_WEBHOOK_URL`.

After each scheduled run (not dry-run), the bio sync posts a summary:

```python
push_alert_to_zapier(settings.zapier_webhook_url, [{
    "alert_type": "legislator_bio_sync_complete",
    "items_seen": report.cms_items_seen,
    "patched": len(report.would_patch),       # actual patches when not dry-run
    "created": len(report.would_create),
    "potential_merges": len(report.potential_merges),
    "upstream_orphans": len(report.upstream_orphans),
    "errors": len(report.errors),
}])
```

Threshold-based alert escalation (`on_failure`, `on_large_changes`) follows the `notifications:` block in `sync_schedule.yaml`. Note: this block is currently scaffolded but not wired up anywhere in the repo — bio sync is the first consumer. Wiring it up here means future pipelines can adopt the same convention.

**Out of scope for this plan:** Sentry, CloudWatch metrics, on-call escalation. Those are a separate cross-cutting initiative; bio sync should not unilaterally introduce a new monitoring stack.

---

## Testing & rollout

### Unit tests (Phase 1 deliverable)

| Test | Asserts |
|---|---|
| `test_should_write_skips_empty_upstream` | An editor-filled CMS field is preserved when upstream is null/`""` |
| `test_should_write_skips_sentinel_values` | `"-"`, `"N/A"`, `"TBD"`, whitespace-only, `[]`, `{}` all treated as empty |
| `test_should_write_preserves_numeric_zero` | `district=0` (at-large) is NOT treated as empty; gets written |
| `test_should_write_skips_locked_field` | Fields in `LOCKED_FIELDS` are never overwritten |
| `test_graceful_patch_drops_unknown_field` | PATCH against a CMS without the new field logs a warning and continues |
| `test_webflow_429_honors_retry_after` | 429 response with `Retry-After: 5` sleeps 5s + jitter and retries |
| `test_webflow_429_persistent_raises` | After 3 failed retries, raises `WebflowRateLimitError` (does not silently return) |
| `test_webflow_non_2xx_bubbles_to_run_summary` | Persistent 429 / 4xx during a bio-sync run lands in `BioSyncReport.errors`, with `errors_count > 0` in the Zapier summary |
| `test_webflow_limiter_shared_across_pipelines` | Two pipelines using the same `WebflowLookupService` instance share the rate budget |
| `test_audit_c_finds_state_records_lacking_openstatesid` | Fixture CMS index with mixed records → audit returns only state records missing `openstatesid` |
| `test_bill_sync_migrated_signature_compat` | Existing `update_bill_fields()` callers in `bill_version.py` work with the new raising contract (asserts no regression) |
| `test_split_email_field` | URL-shaped `email` routes to `contact-form-url`; bare email routes to `email` |
| `test_extract_other_id` | OpenStates `bioguide` extraction from `other_identifiers` |
| `test_find_merge_candidate_score_threshold` | Single-signal matches don't flag; ≥2 signals do; bioguide-only is decisive |
| `test_office_resolution_capitol_vs_district` | Multi-office records are split correctly into capitol/district fields |
| `test_historical_lookup_uses_in_memory_index` | YAML loaded once, indexed by bioguide-id; lookups are O(1) |
| `test_openstates_429_honors_retry_after` | Retry-After header is respected; exponential backoff on subsequent failures |

### Synthetic merge-detection validation

Pick 2–3 known historical state→federal transitions (e.g., a recent state legislator who ascended to Congress in 2023+) and run `find_merge_candidate` against staged CMS records. Measure:
- True-positive rate: should flag 100%
- False-positive rate on a sample of 100 unrelated state legislators: target <5%

### Rollout sequence

| Step | Gate |
|---|---|
| 1. Schema fields added to Webflow | Field-add checklist signed off by content team |
| 2. Audit A passes (federal join-key coverage) | All federal CMS records have ≥1 join key |
| 3. Audit C passes per jurisdiction (state join-key coverage) | All pre-existing state CMS records have `openstatesid` |
| 4. Phase 1 code deployed with `enabled: false` | Smoke test: trigger endpoint returns 200, scheduler shows job registered |
| 5. Dry-run against 5 federal members | Manually inspect the diff in `would_patch[]`; review `is_empty()` log to confirm rule isn't too aggressive |
| 6. Live PATCH against 1 low-stakes federal record | Editor confirms the CMS shows the expected updates |
| 7. Audit B passes | Bulk-create complete; no duplicate openstatesid |
| 8. Enable scheduler (`enabled: true`); first scheduled run | Watch logs for 429s, field-not-found warnings, orphan counts; first Zapier summary posts |
| 9. Phase 2: extend to 7 state jurisdictions | Per-state dry-run + per-state Audit C before each is enabled |

### Rollback procedure

Webflow does not have a CMS revision history exposed via API, so true rollback isn't free. The bio sync's per-record error isolation + cardinal rule (don't blank populated fields) limit blast radius even when individual record PATCHes are wrong, but a bad bulk write still requires manual remediation.

**Operator playbook when a scheduled run produces unexpected writes:**

1. **Stop further runs immediately.** Edit `config/sync_schedule.yaml` and set `legislator_bio_sync.enabled: false`, then `sudo systemctl restart ddp-sync`. The scheduler's job-id-cleanup preamble removes the registered cron on restart.
2. **Identify scope of the bad run.** Check `sudo journalctl -u ddp-sync --since "1 hour ago" | grep "legislator_bio_sync.scheduled_run_completed"` for the run's `success`, `errors`, and `patched` counts. Cross-reference the Zapier alert's run-summary payload (every non-dry-run posts one).
3. **Get the record-level diff** from the run's structured logs: `journalctl -u ddp-sync --since "<start>" | grep "Updated legislator in Webflow CMS"`. Each record's PATCH logs the field set written. The schema-cache check guarantees no unknown fields were sent.
4. **For per-record revert:** the orchestrator does NOT currently snapshot pre-PATCH CMS state. Manual revert means an editor visiting the affected record in the Webflow Designer and restoring known-good values. Pre-deploy step: when bringing up new write paths (e.g., new bio fields, new state path), seed a sample record's pre-state so the team has a manual revert reference.
5. **For mass-revert:** an `--undo-last-run` capability is scoped but not built (Phase-2.5 backlog item). Until shipped, mass-revert is impractical at >5 records — design choice favors prevention (cardinal rule + dry-run + Audit B + per-record error isolation) over after-the-fact undo.
6. **Drafts:** auto-created drafts can be bulk-deleted via the Webflow API by filtering on `created_by=ddp-sync`. Phase 1 doesn't enable auto-create by default.

**Pre-deploy guardrails for risky changes:**
- Always dry-run with `?dry_run=true&limit=5` before flipping the scheduler `enabled: true` after any change to payload-build code or YAML knobs.
- Phase 2.5+ payload changes (new fields, new state-leg overrides) get probe→test→one-record-live → full-run sequence per the §Step-9 runbook pattern.
- Before adding a new state to the per-state override registry, run the OpenStates probe script (`scripts/probe_openstates_state_legs.py`) against that jurisdiction first to confirm assumptions.

### Editor sign-off acceptance criteria (Phase 1 → scheduler enable gate)

Editors confirming "Phase 1 is good to ramp" should verify these on a sample of 5+ records (mix of federal House, federal Senate, state lower, state upper):

| Field | Source | Expected | Failure mode if wrong |
|---|---|---|---|
| `bioguide-id` | unitedstates YAML | 7-character ID like `H001098` | Wrong/empty would break federal-historical fallback |
| `birth-year` | unitedstates / OpenStates | 4-digit year, plausible (1920-2010) | Wrong birthday → privacy concern; year-only is intentional |
| `term-start` / `term-end` | unitedstates current term (federal) | ISO datetime `2025-01-03T00:00:00.000Z` | Date-only string would re-PATCH every run (ChurnPATCH — fixed in commit af55f38) |
| `phone-capitol` | unitedstates / OpenStates capitol office | E.164 or formatted US phone | Wrong → editor / public misdirected |
| `office-address-capitol` | unitedstates / OpenStates capitol office | Full street address with state abbr + ZIP | OpenStates address quality varies for some states |
| `email` (state) | OpenStates direct | `@<jurisdiction>.gov` email or contact-form URL routed correctly | URL accidentally placed in email field instead of contact-form-url |
| `contact-form-url` (federal) | OpenStates → routed via `split_email_field` | `https://...senate.gov/contact/contact` style | Email accidentally placed here instead of email field |
| `official-website` | unitedstates (federal) / FL override (FL state) | URL to legislator's official page | Wrong page or empty when expected populated |
| `photo-source-url` | bioguide-derived (federal) / OpenStates `image` (state) | Working URL | 404 / wrong person — quality issue, not a sync issue |
| `ballotpedia-slug` | unitedstates `id.ballotpedia`, URL-constructed | `https://ballotpedia.org/<Name_With_Underscores>` | Bare slug or display name with spaces would 400 (fixed in 1c23eb2) |
| `govtrack-id` | unitedstates `id.govtrack`, URL-constructed | `https://www.govtrack.us/congress/members/<id>` | Bare numeric ID would 400 (fixed in 1c23eb2) |
| `openstates-id` (Phase 2.5) | OpenStates `openstates_url` field | `https://openstates.org/person/<slug>/` | Empty when OpenStates has the record |
| social handles (federal only) | unitedstates social YAML | Bare handle (no `@`, no URL) | URL or `@`-prefixed value depending on Webflow field type |
| `seat` (multi-ref) | NOT touched by sync | Pre-set by editor pointing into Seats CMS | Sync would mis-classify federal/state if blank |

Acceptance threshold: **0 wrong values out of 5 sample records** for federal happy-path. State path's birth_date / official-website / social handles are coverage-dependent (50% / partial / 0% from probe); zero-coverage absences are not failures.

If sample reveals systemic issue: stop, file a follow-up, do not enable scheduler until fixed. If sample reveals isolated issue (1 of 5 records): investigate the specific record's upstream data; the bio sync is unlikely the cause.

### Step-9 operational runbook

All commands assume `$DEPLOY_URL` and `$DDP_SYNC_API_KEY` are set in the operator's shell. Each gate must pass before the next step.

**0. Push + deploy (operator).**
- `git push origin main` — 14 commits ahead of origin at end of round-15.
- Deploy with scheduler `enabled: false` (config flag — already the default).
- Sanity: `curl -fsS -H "X-API-Key: $DDP_SYNC_API_KEY" "$DEPLOY_URL/health"` returns 200.
- Regression smoke: `curl -fsSX POST -H "X-API-Key: $DDP_SYNC_API_KEY" "$DEPLOY_URL/trigger/bill-status-sync?dry_run=true"` — confirms the existing pipeline still works post-deploy.

**1. Audit A (federal join-key coverage).** Must return zero unresolvable federal records before any sync runs.
```
curl -X POST -H "X-API-Key: $DDP_SYNC_API_KEY" "$DEPLOY_URL/trigger/legislator-bio-sync?audit_only=A" | jq .
```
- Expected: `unresolvable: []`. Any entry blocks step 2 until editors backfill `openstatesid` or `bioguide-id`.
- If 503 (`Retry-After: 60`): pre-warm task is still parsing 8.6 MB historical YAML. Wait 60s and retry.

**2. Dry-run against 5 federal members.**
```
curl -X POST -H "X-API-Key: $DDP_SYNC_API_KEY" "$DEPLOY_URL/trigger/legislator-bio-sync?dry_run=true&jurisdiction=us&limit=5" | jq .
```
- Inspect `would_patch[]` — every entry should be a sensible bio improvement, not a churn-PATCH.
- Inspect `would_create[]` — should be empty (auto_create not set).
- Inspect `errors[]` — should be empty.
- Inspect `is_empty()` breadcrumbs — confirm no populated CMS fields are about to be blanked. If any are, **stop** and tighten `EMPTY_VALUES`.
- Editor sign-off on the diff before continuing.

**3. Live PATCH against ONE low-stakes federal record. ⚠ Modifies production CMS — operator must explicitly confirm before running.**

The endpoint does not yet support a per-record selector — `limit=1` will PATCH whichever record sorts first off the iterator. Identify that record from step 2's output (it is the first entry in `would_patch[]`). Confirm it is low-stakes (e.g., a junior House member, no leadership role, no recent press around their CMS page). If you need to target a different record specifically, that requires adding a `target_slug` parameter — flag it and run a small follow-up PR before this step.

```
curl -X POST -H "X-API-Key: $DDP_SYNC_API_KEY" "$DEPLOY_URL/trigger/legislator-bio-sync?jurisdiction=us&limit=1&dry_run=false" | jq .
```
- Expected: `report.patched == 1`, `errors == []`, Zapier alert posts a summary, `metric=legislator_bio_sync.alert_sent` appears in logs.
- Editor visually verifies the live CMS record now shows the expected fields.

**4. Phase 1 sign-off.** With step 3 verified, Phase 1 is complete. Subsequent steps belong to the existing rollout-sequence table above (Audit B → enable scheduler → Phase 2).

---

## Phasing

**Phase 1 — Federal sync (target: 2 weeks)**

Round-3 review surfaced two scope additions to land **before** any new modules: the rate-limiter extraction and the `WebflowLookupService` error-contract migration. Both touch existing pipelines and need to land atomically with their migrations to avoid split-deploy breakage.

1. **Foundation refactor (atomic, one PR):**
   a. Create `services/rate_limiter.py` with `RateLimitConfig` + new stateful `RateLimiter` class
   b. Migrate `pipelines/legislator_sync.py` and `pipelines/bill_sync.py` to import from there (delete inline copies)
   c. Run existing legislator-sync + bill-sync tests; no behavior change expected
2. **Webflow service extension (atomic, one PR) — IMPLEMENTED:**
   a. ✅ `WebflowError` / `WebflowRateLimitError` / `WebflowPatchResult` / `WebflowCreateResult` defined
   b. ✅ `WebflowLookupService` extended with shared `RateLimiter` (≤60 req/min), `_patch_with_backoff()` / `_post_with_backoff()` (Retry-After honoring + WebflowRateLimitError on persistent 429), `update_legislator_fields()`, `create_legislator_draft()`, field-existence tolerance via cached collection-schema lookup
   c. ✅ `update_bill_fields()` keeps its legacy `bool` contract — routes through `_patch_with_backoff` internally so it inherits rate-limiting and 429 retry without forcing a caller migration. See "Backwards compatibility" section.
   d. Pre-merge smoke: `/trigger/bill-status-sync?dry_run=true` against staging — TODO before deploy
3. **New modules — IMPLEMENTED:**
   a. ✅ `services/congress_legislators.py` — bioguide-indexed in-memory cache; YAML parse via `asyncio.to_thread()` (round-5 fix) so 8.6 MB historical file doesn't block event loop
   b. ✅ `services/openstates_people.py` — async client + `OpenStatesPerson` dataclass; uses shared `RateLimiter`; full `Retry-After` honoring; 404 returns None; persistent 429 raises `OpenStatesRateLimitError`; `extract_other_id` skips non-dict entries (round-6 defensive); `iter_jurisdiction` has `max_pages=200` safety valve (round-6)
4. **Orchestrator + supporting pieces — IMPLEMENTED (commit c555f38):**
   a. ✅ `pipelines/legislator_bio.py` — `LegislatorBioPipeline`, `BioSyncOptions`, `BioSyncReport`, `CMSLegislator`. Module-level `is_empty` / `should_write` / `split_email_field`. Federal end-to-end. State path is a clear Phase 2 stub.
   b. ✅ `WebflowLookupService.iter_legislator_items()` — paginated CMS read iterator (read scope, 200-page safety valve)
   c. ✅ `app.py::lifespan` pre-warms congress-legislators YAML at startup (round-6 fix) so trigger endpoints never cold-start
   d. ✅ `WebflowLookupService._get_field_slugs` — 1h TTL + stale-on-failure reuse with `metric=webflow.schema_stale_reuse` (round-6)
   e. ✅ `tests/test_legislator_bio_foundation.py` — 31 tests pinning rounds 3-6 fixes + live-data discoveries; all pass
5. **Trigger endpoint — IMPLEMENTED:**
   a. ✅ `POST /ddp-sync/v1/trigger/legislator-bio-sync` in `api/routes/triggers.py`
   b. ✅ Round-7 ALB-timeout safety gate: 503 + `Retry-After: 60` when `app.state.congress_legislators._warmed` is False
   c. ✅ Param validation; `audit_only=A|C` short-circuit
   d. ✅ `split_email_field` round-7 hardening (case-insensitive scheme; `mailto:` unwrap; whitespace strip)
   e. ✅ 10 endpoint tests in `tests/test_trigger_legislator_bio_sync.py`
   f. ✅ README.md updated with the new trigger row + 503-on-warming note
6. **Audits A and C — IMPLEMENTED:**
   a. ✅ `LegislatorBioPipeline.audit_federal_join_keys()` (Audit A) — surfaces federal CMS records lacking BOTH `openstatesid` and `bioguide-id`
   b. ✅ `LegislatorBioPipeline.audit_state_join_keys(jurisdiction=None)` (Audit C) — surfaces state records lacking `openstatesid`; case-insensitive jurisdiction filter
   c. ✅ Both audits wrap WebflowError as `aborted=True` with `abort_reason` (partial report rather than raising)
   d. ✅ `AuditReport` + `AuditEntry` dataclasses. New `CMSLegislator.state_code()` helper.
   e. ✅ Trigger endpoint `audit_only` runs the real audit (replaced the not-implemented stub)
   f. ✅ 8 audit-function tests + 4 endpoint integration tests. **Total suite: 55 tests, all pass.**
7. **Run-summary alerting via Zapier — IMPLEMENTED:**
   a. ✅ `push_bio_sync_alert(webhook_url, report)` in `pipelines/legislator_bio.py`. Mirrors the existing `voatz_brevo.push_alert_to_zapier` pattern (sync `requests`, 30s timeout, never raises).
   b. ✅ Wired into `LegislatorBioPipeline.run()` via `try/finally` so the alert fires on **every non-dry-run completion including aborts** (the case editors most need to know about).
   c. ✅ Payload includes `on_failure` (errors > 0 OR aborted) and `on_large_changes` (patched + created > 100) threshold flags for Zapier-side routing.
   d. ✅ Round-9 follow-ups bundled: jurisdiction cache gains 1h TTL + stale-on-empty-refresh reuse + `metric=webflow.jurisdiction_mapping_empty` breadcrumb. Tests pin all three.
   e. ✅ 13 new tests; **total suite: 77 tests, all pass**.
8. **Orchestrator-internal `run()` integration tests — IMPLEMENTED:**
   a. ✅ New `tests/test_legislator_bio_orchestrator.py` — 8 integration tests covering full-pass `run()` flows: federal-via-OpenStates happy path, bioguide-fallback for departed federal (Karen Bass case), state record Phase-2 stub, dry-run, per-record WebflowError, rate-limit-aborted-with-alert, jurisdiction filter, jurisdiction-cache lock-release-on-raising-fetch.
   b. ✅ `_build_pipeline()` test fixture for fully-mocked end-to-end runs.
   c. ✅ Round-12 polish bundled: doc-comment on `get_jurisdiction_mapping` returned dict + lock-release contract test.
   d. ✅ Round-13 safety-valve integration tests: `locked_fields` excludes named fields from PATCH; `large_changes_threshold` end-to-end → real Zapier payload reflects `on_large_changes=True`; mixed-success PATCHes (2 succeed + 1 WebflowError) records per-record results correctly. `MagicMock(spec=...)` tightening on service mocks catches future signature drift.
   e. ✅ Round-14 follow-ups: mass-blank-prevention end-to-end (upstream None on populated CMS field → field NOT in PATCH, exercising both the payload-None-strip and diff-time `is_empty` defense layers); `==` threshold edge-case test pinning the strict-`>` semantics (`on_large_changes=False` at exactly threshold); large_changes test imports `DEFAULT_LARGE_CHANGES_THRESHOLD` instead of hardcoding 100, so ops can tune the constant without breaking the test. **Total suite: 95 tests, all pass.**

### Threshold semantics (round-14 doc)

**`on_large_changes` flag fires when `patched + created > DEFAULT_LARGE_CHANGES_THRESHOLD` (strictly greater).** At exactly the threshold, the flag is False. Pinned by `test_run_large_changes_alert_does_not_fire_at_exact_threshold`.
9. ⏳ Dry-run against 5 federal members → 1 live PATCH on low-stakes record (last step)

**Phase 2 — State coverage (target: 2–4 weeks after Phase 1)**
1. Extend orchestrator to handle 7 active jurisdictions
2. State-leg field mapping (handle empty `other_identifiers`, missing `birth_date`)
3. Auto-create gating per-jurisdiction (default false)
4. Schedule integration into weekly Sunday job
5. Deploy + monitor first scheduled run; tune backoff if 429s appear

**Phase 3 — Photo asset upload (TBD)**
1. Detect upstream image change via `photo-source-url` field
2. Download → upload to Webflow Assets API
3. Patch the `image` reference field on the legislator
4. Per-source error handling (state photo CDNs are unreliable; AZ uses third-party Apptegy)

**Phase 4 — Narrative bios (TBD)**
Re-evaluate if Phases 1–3 leave real gaps. Options at that time: Ballotpedia API (paid), curated wiki content, Wikipedia summary extraction, manual editor input.

---

## Risks & constraints

1. **OpenStates 429 rate limiting** — Already a known issue (`TROUBLESHOOTING.md`). Bio sync adds ~535 federal calls + ~7 jurisdiction iterations + ~750 state member calls per week. At 500ms delay this is ~11 minutes total, well under the 30k/day budget. If 429s appear, bump `delay_between_bills_ms` to 600–700.

2. **Webflow API rate limits** — Plan tier is **120 req/min**. The shared in-process limiter on `WebflowLookupService` is set to ≤60 req/min for bio-sync (and bill-sync inherits the same limiter when running in the same Python worker), leaving 60 req/min headroom for unified-sync API calls and ad-hoc triggers. PATCH-only-changed-fields keeps payloads small. With ~1300 total legislators worst case, ~22 minutes for full pass at the rate cap. **Cross-process / multi-worker rate coordination is not implemented — single ddp-sync deployment with one APScheduler leader makes process-local sufficient.** If that assumption changes (multi-worker, multi-region), Redis-backed coordination would be needed.

3. **State photo source instability** (deferred to Phase 3) — AZ photos come from third-party Apptegy CDN; not under state control. Re-upload pipeline will need retries + skip-on-fail semantics.

4. **Editor merges for state→federal transitions** — Detection prevents accidental duplicates but requires manual editor action. Frequency: a handful per election cycle. Acceptable.

5. **`bio.birthday` privacy optics** — Even though publicly available via Bioguide, full DOB feels personal. Store **year-only** on CMS; ignore the day/month from the source.

6. **Webflow field count cap** — Plans typically cap collections at 60 fields. Adding 21 brings the Legislators collection to ~33 (existing 12 + new 21). Should be fine; verify before rollout.

7. **Concurrency with imminent bulk-editor import** — Editors are about to bulk-create hundreds of state-legislator CMS records. Sequencing rule: **first scheduled run is gated on Audit B** (every new record has `openstatesid`, no duplicates). Until Audit B passes, only manual dry-runs via the trigger endpoint are permitted. See "Pre-flight audits" and "Rollout sequence" sections.

8. **Historical YAML hot-loop risk** — `legislators-historical.yaml` is 8.6 MB. Naive per-record scan is O(n²) on the lookup. Mitigation: `CongressLegislatorsSource.warm_cache()` builds an in-memory `dict[bioguide_id, record]` once per run; all lookups are O(1) thereafter.

---

## Open questions deferred

1. **What happens when a state-leg's `openstatesid` goes stale?** — When a state legislator leaves office, there's no historical YAML for state. Plan: tag as `upstream_orphan` and surface in the report; editors decide whether to mark the CMS item as past-tenure or remove. Phase 2 may add a `term-end-detected` heuristic (e.g., openstatesid 404s for 3+ consecutive weekly runs → auto-set `term-end`).

2. **Schedule cadence** — Weekly may be conservative for bio data that changes slowly. Could shift to bi-weekly later to reduce API spend if needed.

3. **Should we store `religion`?** — In unitedstates dataset but 0% populated for current members. Skip until populated.

4. **Per-record `bio-sync-locked` boolean field** — Phase 1 ships only the repo-level `locked_fields` config opt-out. If usage shows individual records need protection, Phase 2 can add a per-record CMS boolean. Defer until evidence is in.

---

## References

- `README.md` — Architecture section, Pub/sub events, API path conventions
- `TROUBLESHOOTING.md` — "Data Flow Decoupling 2026-03-11" describes the Flow 1 / Flow 2 pattern this design mirrors
- `src/ddp_sync/pipelines/bill_version.py` — reference implementation of decoupled write paths
- `src/ddp_sync/sync/handlers/legislator.py` — existing batch-sync handler (will be extended in Phase 2)
- `src/ddp_sync/pipelines/legislator_sync.py` — existing OpenStates client patterns (rate limiting, retry)
- `src/ddp_sync/ingestion/sources/webflow.py` — existing Webflow read patterns; PATCH support is the main gap to fill
- `src/ddp_sync/services/legislative_calendar.py` — pattern for OpenStates async API client structure
- `config/sync_schedule.yaml` — schedule config to extend
