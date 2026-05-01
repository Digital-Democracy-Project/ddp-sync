# Troubleshooting

Known issues, past bugs, and their resolutions. Entries are grouped by subsystem.

---

## Startup / Infrastructure

### Pinecone crash on startup

**Symptom:** `PineconeConfigurationError` on startup when `PINECONE_API_KEY` is not set.

**Fix (2026-03-11):** Pinecone client is now lazy-initialized on first use. The service starts without Pinecone credentials — health check reports `"pinecone": "not_configured"` instead of crashing.

### Redis health check `_redis` attribute error

**Symptom:** Health endpoint returns 500 with `'RedisStore' object has no attribute '_redis'`.

**Fix (2026-03-11):** The `RedisStore` attribute is `_client`, not `_redis`. Fixed in `health.py` and `app.py` (zombie watchdog).

### `sync_schedule.yaml` not found (non-editable install)

**Symptom:** Scheduler starts with 0 jobs. Log shows config file not found.

**Fix (2026-03-11):** `scheduler.py` now checks both the package-relative path and `Path.cwd() / "config" / "sync_schedule.yaml"`. Run `uvicorn` from the repo root directory so the CWD fallback resolves correctly.

### Python 3.13 editable install fails on macOS

**Symptom:** `pip install -e .` succeeds but `import ddp_sync` fails with `ModuleNotFoundError`.

**Workaround:** Use non-editable install: `pip install .` (requires reinstall after code changes). This is a known issue with `.pth` file processing in Python 3.13 + Homebrew.

---

## Bill Version Sync

### Bills skipped as "not current session" (WA, MI, UT, AZ, MA)

**Symptom:** Nightly bill version check logs "Skipping bill (not current session)" for certain states. Bills never receive status updates.

**Root causes (all fixed 2026-03-11):**

1. **Null `end_date` from OpenStates** (WA, UT) — `_check_live_sessions()` required both dates non-None. Fix: treat `end_date=None` with valid `start_date` as active.
2. **Stale `end_date` for multi-year sessions** (MI) — Session `2025-2026` has `end_date=2025-12-31`. Fix: parse years from identifier and check if current year is in range.
3. **Non-standard session identifiers** (AZ `57th-2nd-regular`, MA `194th`) — Regex `\d{4}` finds no year. Fix: switched to `is_current_session_async()` which queries OpenStates API directly.
4. **Wrong Webflow field name** (all states) — Code read `session-year` (nonexistent). Actual field is `bill-session` (integer). Fix: `str(fields.get("bill-session", ""))`.

**Files:** `services/legislative_calendar.py`, `pipelines/bill_version.py`, `pipelines/bill_sync.py`

### Bill status stuck / not updating in Webflow CMS

**Symptom:** Webflow CMS shows a stale `status` and `status-date` for a bill, even though the nightly bill version check runs successfully and OpenStates has newer actions.

**Root cause (fixed 2026-03-11):** When a bill had no Redis version cache (first run, Redis flush, or newly added bill), `_is_newer_version()` returned `True`, routing the bill through the **new-version path** which requires full text re-ingestion into Pinecone before updating Webflow. If ingestion failed (Pinecone unavailable, PDF extraction error, etc.), the method returned early — **skipping the Webflow status update and Redis cache write**. On the next run the same failure repeated, leaving the bill permanently stale.

**Fix:** The Webflow status update now runs regardless of whether text ingestion succeeds. The Redis cache is also written so the bill isn't stuck in a re-ingestion loop. A new `"partial"` result status tracks cases where status was updated but ingestion failed.

**Files:** `pipelines/bill_version.py`

---

## Data Flow Decoupling (2026-03-11)

### Background

The Webflow CMS status sync (Flow 1) and Pinecone version check (Flow 2) were previously tangled in a single `check_and_update_bill()` method. This meant:
- You couldn't update Webflow without risking a Pinecone re-ingestion
- A Pinecone failure could stall CMS updates
- No way to backfill historical bills to Webflow without triggering version checks

### Architecture after decoupling

`check_and_update_bill()` is now composed of two independent write paths:
- `update_bill_status()` — Flow 1: extracts status, date, chamber, gov-url from OpenStates and PATCHes Webflow
- `check_and_reingest_version()` — Flow 2: compares bill text version against Redis cache, re-ingests to Pinecone if newer

Both share the same fetched OpenStates data (one API call per bill). Either can fail independently without blocking the other.

### New API parameters

- `target`: `"all"` (default), `"webflow"`, or `"pinecone"` — controls which write paths run
- `all_sessions`: bypasses session/jurisdiction filters for backfill operations
- `/trigger/bill-status-sync`: dedicated endpoint for Flow 1 only

### Config changes

`sync_schedule.yaml` now has a `bill_sync` block with `webflow_status.enabled` and `version_check.enabled` sub-configs. Either flow can be disabled independently. The old `sync_time_utc` and `bill_version_check` keys still work as fallbacks.

**Files:** `sync/types.py`, `pipelines/bill_version.py`, `sync/handlers/bill.py`, `scheduler.py`, `api/routes/triggers.py`, `api/routes/sync_unified.py`, `services/redis_store.py`, `config/sync_schedule.yaml`

---

## Hourly Content Update Removed (2026-04-08)

### Background

An "Hourly Content Update" job polled OpenStates and Congress.gov for changes via the `/bills` list endpoint, then attempted generic ingestion into Pinecone. It had been silently failing on every run since deployment.

### Root causes

1. **Invalid OpenStates parameter** — Change detection sent `sort=updated_at` to `/bills`, which is not a valid OpenStates v3 parameter (422 Unprocessable Entity).
2. **Missing required filter** — The ingestion fetch sent `/bills?per_page=50` with no jurisdiction or session filter, which OpenStates v3 requires (400 Bad Request).

Both errors were caught and swallowed, so the job reported "executed successfully" in APScheduler while doing nothing. The daily bill sync (04:00 UTC) already covers this work using individual bill endpoints (`/bills/{jurisdiction}/{session}/{id}`), which work correctly.

**Fix:** Removed the hourly job, its `_run_updates`/`_update_source`/`trigger_update` methods, the `ChangeDetector` class, and the orphaned `change_detection.py` module.

**Files:** `scheduler.py`, `pipelines/change_detection.py` (deleted), `config/sync_schedule.yaml`

---

## Known Non-Critical Issues (observed 2026-04-08)

### OpenStates 429 rate limiting during daily bill sync

**Symptom:** During the 04:00 UTC bill sync, OpenStates returns 429 (Too Many Requests) on some bills. Observed 7 occurrences in a single run, mostly on US federal bills. Some required 2 retries (5s + 10s backoff). All eventually succeeded.

**Cause:** The configured `delay_between_bills_ms: 500` is at the edge of OpenStates' 2 calls/sec tier. Network jitter can push requests over the limit.

**Mitigation:** Retry logic handles this correctly. If 429s increase, bump `delay_between_bills_ms` to 600–700 in `sync_schedule.yaml`.

### Brevo transient 503 on Federal org

**Symptom:** Voatz→Brevo sync fails for the Federal org (~23,587 users) with `503 - upstream connect error or disconnect/reset before headers. reset reason: remote connection failure`.

**Cause:** Transient Brevo API outage. The code correctly skips the org to avoid unreliable diffs. Observed once (13:28 UTC); subsequent runs succeeded.

**Action:** Monitor. No fix needed unless it becomes a recurring pattern.

### Congress API key logged in httpx output

**Symptom:** httpx INFO logs include the full Congress.gov API key in request URLs (e.g., `api_key=9wdd73N...`).

**Impact:** Low — Congress API keys are free and public. But it adds noise and is not best practice.

**Potential fix:** Add a log filter to redact `api_key=` values from httpx output, or move the key to a request header instead of a query parameter.

---

## Legislator Bio Sync

### `/trigger/legislator-bio-sync` returns 503 with `Retry-After: 60`

**Symptom:** First request after `systemctl restart ddp-sync` returns:
```
HTTP/1.1 503 Service Unavailable
Retry-After: 60
{"detail":"Bio-sync source still warming up; retry in ~60s. ..."}
```

**Cause:** Expected. The bio sync depends on the unitedstates/congress-legislators dataset (8.6 MB historical YAML); parsing it takes ~55s. App startup fires `_prewarm_congress_legislators()` as a background task so the very first trigger request after a restart can race the pre-warm. The 503 + `Retry-After` is intentional — it avoids a silent ALB idle timeout.

**Action:** Wait 60s and retry. If the 503 persists past 2 minutes, check `journalctl -u ddp-sync -f` for YAML fetch failures (e.g. GitHub raw-content unreachable from EC2).

### Bio sync downloads YAML on every run

**Symptom:** Each run hits `raw.githubusercontent.com/unitedstates/congress-legislators/...` instead of using the cache.

**Cause:** The cache lives at `~/.cache/ddp-sync/congress-legislators/` with a 24h TTL. If the service runs as `ubuntu`, that resolves to `/home/ubuntu/.cache/...`. If the directory was deleted (e.g. by a disk-cleanup script), the next run re-downloads.

**Action:** No action required — re-download is idempotent and adds ~5s to the run. If you want to confirm cache state: `ls -la /home/ubuntu/.cache/ddp-sync/congress-legislators/`.

### Zapier alert didn't fire after a non-dry-run

**Symptom:** A non-dry-run completed (logs show `metric=legislator_bio_sync.run_completed`) but no Zapier message arrived.

**Diagnostics in order:**
1. `ZAPIER_WEBHOOK_URL` empty in env → alerts disabled by design (same env var as Voatz→Brevo).
2. Logs show `metric=legislator_bio_sync.alert_sent` → POST succeeded; problem is on the Zapier side (zap paused, filter rejected the payload).
3. Logs show `push_bio_sync_alert returned False` or a non-2xx → Zapier endpoint rejected; check the Zap's runtime tab.

The alert is wired in a `try/finally`, so it fires even on aborted runs (rate-limit, Webflow outage). Absence of the alert when neither (1) nor (2) is true is itself a signal worth investigating.

### Editor reports "field not found" PATCH errors

**Symptom:** `would_patch[]` shows an entry but the live PATCH log says "field not found in schema" and skips the field.

**Cause:** The 21 new Webflow Legislator fields (per `plans/webflow-legislator-fields.md`) are added via the Designer; if a field hasn't been added yet, the bio sync detects this via the cached collection schema (1h TTL) and silently skips that field rather than failing the whole record. The check is intentional — partial schema rollout shouldn't block the sync.

**Action:** Confirm the field exists in the Webflow Designer with the slug listed in `webflow-legislator-fields.md`. After the field is added, the schema cache will refresh on the next call (1h max).

### New CMS field added but bio sync writes don't persist

**Symptom:** A new field is added to the Legislators CMS in the Webflow Designer; the bio sync run reports `patched: N` with the new field in `changed_fields[]` and `dropped_fields: []`; but reading the records back shows the field is still empty (`null`).

**Cause (observed 2026-04-30):** Webflow's CMS schema has two layers — the Designer/draft state and the published-site state. New fields are visible in the schema endpoint (`GET /v2/collections/{id}`) immediately, but the items endpoint can't write to them until the **site is republished**. PATCH requests against unpublished fields return 200 but silently ignore the field's value.

**Action:** After adding new CMS fields, click "Publish" on the Webflow site (the top-right Publish button in the Designer). Then re-run the bio sync.

**Diagnostic:** Use `scripts/probe_webflow_legislators.py` to read back the population. If the field shows 0% populated despite `patched: N` from the bio sync, this is the issue.

### Scheduled bio sync logs `metric=legislator_bio_sync.startup_misconfig` at app start

**Symptom:** After restarting ddp-sync, `journalctl` shows an error event:
```
metric=legislator_bio_sync.startup_misconfig
"legislator_bio_sync.upload_photos: true but webflow_assets_read_write_key is NOT configured. ..."
```

**Cause:** `sync_schedule.yaml` has `legislator_bio_sync.upload_photos: true` but the `webflow_assets_read_write_key` is missing from the secret (or is empty). The scheduled run will register normally, but every photo upload will fail with `metric=webflow_assets.config_error` once the cron fires.

**Action:**
1. Confirm the Webflow workspace has a token with `assets:read` and `assets:write` scopes (the existing `webflow_api_token` has only `cms:*` and is insufficient — confirmed 2026-04-30 via 403 OAuthForbidden).
2. Add the token to `ddp-sync/credentials` in AWS Secrets Manager under the key `webflow_assets_read_write_key` (lowercase, with underscores).
3. Restart ddp-sync; the misconfig event should not re-emit.

If you want the bio sync to run WITHOUT photo uploads (e.g., temporarily during a Webflow incident), set `upload_photos: false` in `sync_schedule.yaml` and restart.

### Photo upload pipeline: stale unitedstates/images URLs for federal members

**Symptom:** Run summary shows `photo_uploads_failed > 0` for federal records; per-record errors include "Source image returned 404: https://unitedstates.github.io/images/congress/450x550/{bioguide}.jpg".

**Cause:** The `unitedstates/images` GitHub Pages dataset is community-PR-maintained and lags new federal members significantly. As of 2026-04-30, 4 of 32 FL federal CMS records had bioguide-photos absent (including 2-year incumbents, not just freshmen).

**Mitigation (Phase-4 V1, commit d3909c6):** Federal records now get a congress.gov fallback URL automatically: `https://www.congress.gov/img/member/{bioguide_lower}.jpg`. The `WebflowAssetService` tries the fallback after the primary 404s; first success wins. If both fail, the record's `legislator-image` stays empty and `photo-source-url` Link still has the canonical URL for the website to fall back via hotlinking.

**Action:** No operator action needed — the fallback runs automatically. If both URLs fail for a specific record, an editor can manually upload a photo via the Webflow Designer (the cardinal rule preserves manual uploads).

### Suspected ChurnPATCH on a new field — diagnose schema mismatch first

**Symptom:** A field appears in `changed_fields[]` on every run, even when the upstream value didn't change. Looks like a Webflow-side normalization mismatch (e.g., trailing slash, case).

**Cause (observed 2026-04-30):** Often the field's slug doesn't actually exist in the live CMS schema. The schema cache silently drops the write; CMS still has `null`; next run's diff still sees a mismatch. Looks like churn; is actually "field doesn't exist, every write is silently dropped".

**Diagnostic order:**
1. Run `scripts/probe_webflow_legislators.py` and check the field's population. If 0%, suspect schema mismatch.
2. Run `scripts/probe_webflow_record.py` to dump the live schema. Compare the slug the bio sync writes vs the slug actually in the schema.
3. If slugs match and population is still 0%, do a direct curl PATCH of one record (see `plans/PLAN-legislator-bio-sync.md` §Step-9 operational runbook) — if that works but the bio sync's PATCH doesn't, capture the response body for further diagnosis.

Only after ruling out schema mismatch should you suspect URL-trailing-slash, email-case, or other storage-format normalization issues.

### Audit A returns `total_scanned: 0`

**Symptom:** `POST /trigger/legislator-bio-sync?audit_only=A` returns `total_scanned: 0` even though the CMS has federal records.

**Cause (resolved 2026-04-30):** Initial code read federal/state classification from a flat `chamber: str` field that doesn't exist in production. Live CMS uses a multi-reference `seat` field → Seats CMS (4 items: `us-house`, `us-senate`, `state-house`, `state-senate`). Fixed in commit f235665.

**Action:** Confirm you're running ddp-sync at f235665 or later (`git log --oneline | grep f235665`). If yes and `total_scanned` is still 0, check that the Legislators records actually have their `seat` ref populated.

### Bio-sync HTTP 400 on every PATCH (atomic-payload rejection)

**Symptom:** `errors[]` in the run report shows every record failing with `Webflow PATCH failed: status=400`. No data was actually written.

**Cause:** Webflow PATCH is atomic — a single field validation failure rejects the entire payload, even fields that would otherwise validate. So if one field's value is wrong type (e.g. a plain slug sent to a Link/URL-typed field), every PATCH attempt across every record fails the same way.

**Diagnostic:** Since commit 350b0d6, `WebflowError`'s string representation includes the response body. Re-run and look at `errors[]` — the body will have something like `"param":"<field-slug>","description":"<reason>"`.

**Common causes:**
- Field is URL/Link-typed but value is a bare slug or number (fix: construct a canonical URL in the orchestrator, e.g. commit 1c23eb2 for ballotpedia-slug + govtrack-id)
- Field is Date-typed but value is `"2025-01-03"` not `"2025-01-03T00:00:00.000Z"`
- Field is Number-typed but value is a string

**Action:** Check the 400 body for the offending field, then either: (a) change the field type in Webflow Designer to match what the sync sends, or (b) update the orchestrator to construct the value the field expects.

### State-leg records show as `upstream_orphans` on every run

**Symptom:** Same set of state-leg slugs (e.g. `dean-black-fl0015`) appears in `upstream_orphans[]` on every bio-sync run.

**Cause (Phase 1 design limitation, expected):** OpenStates drops state legislators when they leave office; the bio sync's bioguide fallback is federal-only. Pre-existing state CMS records with stale `openstates_id` will continue to show as orphans until either an editor archives them in the CMS, or Phase 2 adds a `bio-sync-locked` field that editors can toggle to mark "manually historical, expected orphan".

**Action:** Phase 1 — informational only; no automated handling. Phase 2 — add the lock field; tracked in `plans/PLAN-legislator-bio-sync.md` "Carry-overs into Phase 2".

### Zapier Slack template renders empty placeholders or literal Mustache syntax

**Symptom:** The bio-sync Slack alert renders as the voatz-brevo template (with empty `Voters Added` placeholders), or shows literal `{{#on_failure}}` text.

**Cause:** Zapier's Slack action only supports flat `{{field}}` interpolation, not Mustache section conditionals. Both bio-sync and voatz-brevo POST to the same `ZAPIER_WEBHOOK_URL` by design and route via the `alert_type` field in the payload (`user_sync_complete` vs `legislator_bio_sync_complete`).

**Fix (commit 8653d24):** Bio-sync payload now includes pre-formatted `failure_warning` and `large_changes_warning` strings (empty when no warning, descriptive text when active). Drop them in the Slack template unconditionally.

**Action:**
1. Add a Zapier "Paths" step on the webhook trigger; branch on `alert_type`.
2. Path A (`user_sync_complete`) → existing voatz-brevo Slack template.
3. Path B (`legislator_bio_sync_complete`) → new bio-sync Slack template using fields like `{{summary}}`, `{{patched}}`, `{{errors}}`, `{{failure_warning}}`, `{{large_changes_warning}}`.
