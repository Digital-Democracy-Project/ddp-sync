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
