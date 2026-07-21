# DDP-Sync

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An open-source, unified data pipeline service for the Digital Democracy Project.

## Architecture

DDP-Sync handles all scheduled and on-demand data sync operations:

- **OpenStates scrape jobs** (managed via `openstates_scrape` block in `sync_schedule.yaml`):
  - **Patch refresh** (daily 01:00 UTC): runs `apply-local-patches.sh` on the Mac Studio before any scrapes start
  - **FL scrape** (daily 02:00 UTC): all four FL sessions sequentially (2026 → 2026D → 2026E → 2026F share `_data/fl/`); starts first as it takes 12+ hours
  - **WA scrape** (daily 02:30 UTC): staggered 30 min after FL; finishes ~07:30 UTC
  - **USA scrape** (daily 03:00 UTC): House then Senate sequentially (share `_data/usa/`); finishes ~09:00 UTC
  - **Secondary states** (Sunday 02:00 UTC): VA, MI, MA, UT, AZ fanned out concurrently via `asyncio.gather` — independent `_data/` dirs, no conflicts; run alongside FL start
  - **People refresh** (Sunday 10:00 UTC): `git pull` on the people repo + `os-people to-database` for all states; after USA finishes ~09:00
  - Each job is an independent APScheduler `CronTrigger` with `max_instances=1, coalesce=True` — a long-running FL scrape no longer delays WA/USA or causes Sunday jobs to be skipped; times are staggered to spread Mac load across the window
- **Daily bill sync** (04:00 UTC): Shared OpenStates fetch with independent write paths:
  - Flow 1: OpenStates → Webflow CMS (status, status-date, status-chamber, gov-url)
  - Flow 2: OpenStates → Pinecone — on each new version: upserts new bill-text chunks, deletes surplus old chunks (upsert-then-delete-by-ID using `chunk_count` from Redis cache to avoid a zero-chunk availability window), stores a permanent `bill-text-history-{webflow_id}-{version_date}` record, and generates an LLM changelog (`bill-changelog-{webflow_id}-{version_date}`) comparing old vs new text via `gpt-4o-mini`. Changelog generation fails gracefully (stale URLs, OpenAI errors) without blocking the ingest.
  - Either flow can be disabled independently in `sync_schedule.yaml`
- **Legislator sync** (weekly Sun 06:00 UTC): OpenStates → Pinecone
- **Legislator bio sync** (weekly Sun 07:00 UTC, enabled 2026-05-01): unitedstates/congress-legislators + OpenStates → Webflow Legislators CMS (bio, contact, term, social, photo URL fields). Phase-3 photo upload pipeline populates the `legislator-image` (Image type) field with Webflow-hosted assets via the Webflow Assets v2 API. Per-state override registry handles jurisdiction-specific extraction (e.g., FL `official-website` from `links[]` "member detail page").
- **Organization sync** (monthly 1st 08:00 UTC): Webflow → Pinecone
- **Voatz → Brevo user sync** (every 30 min): Voatz → Brevo contact lists
- **Voatz → Brevo full-attribute sync** (monthly 1st 02:00 UTC): Full re-import
- **Webflow CMS batch jobs** (weekly Mon 03:00 UTC): Fill fields, sync refs, detect bill duplicates, merge duplicate organizations
- **API health check** (nightly 09:00 UTC): POSTs to `/get_events` for every configured Voatz org, asserts non-empty JSON results, and fires a Zapier alert on any failure. Manual run: `.venv/bin/python scripts/check_api_health.py [--dry-run]`

### Service Topology

```
nginx (:80/443)
  └── DDP-API (:5000) — Auth gateway + API proxy
        ├── VoteBot (:8000) — Chat/RAG (read-only)
        └── DDP-Sync (:8001) — Data pipelines (this service)
```

DDP-Sync is not exposed externally. All external traffic goes through DDP-API's catch-all proxy (`/votebot/sync/*`, `/votebot/trigger/*`).

### Pub/sub events

DDP-Sync publishes Redis pub/sub events that other services consume. Subscribers handle missed events via their own startup reconciliation; failures publishing are logged but never raised.

| Channel | When | Payload | Consumer |
|---|---|---|---|
| `votebot:cache:invalidate` | After a successful bill text re-ingestion (`chunks_created > 0`) and `set_bill_version()` update | `{"slug": "...", "reason": "bill_version_change", "version_note": "..."}` | VoteBot's button-cache subscriber clears `votebot:button:{slug}:summary` and `votebot:button:{slug}:pros_cons`. See [PLAN-quick-action-buttons.md](https://github.com/Digital-Democracy-Project/votebot/blob/main/plans/PLAN-quick-action-buttons.md) Phase 5. |

DDP-Sync also stores `bill_slug` and `chunk_count` alongside `last_checked` in the `ddp:bill_version:{webflow_id}` Redis record. `bill_slug` lets VoteBot's startup reconciliation map webflow_id → slug without an extra Webflow API call. `chunk_count` enables the daily sync to delete surplus chunks by exact ID when a new bill version produces fewer chunks than the previous one.

## Configuration

- **Production**: AWS Secrets Manager (`ddp-sync/credentials`)
- **Local dev**: `.env` file (copy from `.env.example`)

Config is loaded once at startup. Source priority: Secrets Manager → `.env` → defaults.

### Webflow CMS package

All Webflow CMS batch logic lives in `src/ddp_sync/webflow_cms/` — a self-contained subpackage with `WebflowClient`, service classes (`BillOrgSyncService`, `DuplicateBillsService`, `OrgMergeService`, fill services), models, and utilities. It was previously maintained as a separate repo (`FillWebflowFields`, now deprecated) and absorbed here on 2026-06-03 so all cron-facing code lives in one place.

### Webflow API tokens

DDP-Sync uses two distinct Webflow API tokens with different scopes:

| Secret key | Scopes | Used by |
|---|---|---|
| `webflow_api_token` | `cms:read cms:write` | All Webflow CMS item PATCHes (bills, legislators, organizations) |
| `webflow_assets_read_write_key` | `assets:read assets:write` | Phase-3 photo upload pipeline (`POST /v2/sites/{id}/assets`); without this, `upload_photos: true` runs disable photo uploads with `metric=webflow_assets.config_error` per record |

The `cms:*` token does NOT carry `assets:write` — confirmed via 2026-04-30 production smoke (returned 403 OAuthForbidden on `POST /assets`). Keep them as separate keys so each can be rotated independently and the principle of least privilege is preserved.

### LegBot dispatch (CAMS)

`src/ddp_sync/services/legbot_client.py` (added 2026-07-21, `ddp-agents`' `PLAN-legbot.md` Phase 3) dispatches bill-analysis questions to LegBot — CAMS's legislative bill agent — via CAMS's generic task API, then reads the structured answer directly off CAMS's local filesystem (no HTTP result endpoint, matching how CAMS's own Agent Smith reads that same directory in-process). Only runs from the local Mac Studio `ddp-sync` instance, not EC2 production — same box as CAMS, no WireGuard hop needed.

| Env var | Default | Purpose |
|---|---|---|
| `CAMS_BASE_URL` | `http://localhost:8000` | CAMS's API base URL |
| `CAMS_API_TOKEN` | `""` | Bearer token for CAMS's task API (matches CAMS's own `CAMS_API_TOKEN`) |
| `CAMS_ARTIFACTS_DIR` | `""` (must be set) | Absolute path to `ddp-agents`' `artifacts/` directory — machine-specific, not guessed |

**Not yet wired into any write path** — this client dispatches and returns LegBot's answer only; there's nowhere durable to write it yet (`BillArtifact`, `ddp-infra`'s Phase 6, doesn't exist). See `ddp-infra/PLAN-bill-document-provenance.md` Phase 8 for the eventual write-path design.

## API

All endpoints are prefixed with `/ddp-sync/v1` (set in `src/ddp_sync/app.py` as `API_PREFIX`). The paths in the tables below are **relative to that prefix** — combine them when calling the service.

For example, the unified sync endpoint:
- **From localhost on EC2**: `http://localhost:8001/ddp-sync/v1/sync/unified`
- **From the public proxy** (DDP-API strips its own `/votebot/sync/` prefix and forwards): `https://api.digitaldemocracyproject.org/votebot/sync/unified`

### Sync Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/sync/unified` | Trigger batch or single sync |
| GET | `/sync/unified/status/{id}` | Poll task status |
| POST | `/sync/unified/all` | Trigger sync for all content types |

The `/sync/unified` endpoint accepts optional `target` and `all_sessions` parameters:
- `target`: `"all"` (default), `"webflow"` (CMS only), or `"pinecone"` (vector store only)
- `all_sessions`: `true` to bypass session/jurisdiction filters for backfill

### Trigger Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/trigger/bill-version-check` | Trigger daily bill sync (Flow 1 + Flow 2) |
| POST | `/trigger/bill-status-sync` | Trigger Webflow CMS status sync only (Flow 1) |
| POST | `/trigger/user-sync` | Trigger Voatz → Brevo incremental sync |
| POST | `/trigger/full-sync` | Trigger Voatz → Brevo full-attribute sync |
| POST | `/trigger/legislator-bio-sync` | Trigger legislator bio + contact sync (federal in Phase 1; state in Phase 2) |
| POST | `/trigger/webflow/{job}` | Trigger specific Webflow batch job |
| POST | `/trigger/openstates-scrape/{target}` | Trigger an OpenStates scrape job immediately (returns 202, runs in background) |

`/trigger/openstates-scrape/{target}` — valid targets: `patches`, `fl`, `wa`, `usa`, `secondary`, `people`, `va`, `mi`, `ma`, `ut`, `az`

`/trigger/bill-status-sync` accepts query params: `all_sessions` (bool), `jurisdiction` (str)

`/trigger/legislator-bio-sync` accepts query params:
- `dry_run` (bool) — preview the diff without writing
- `auto_create` (bool) — create drafts for upstream-only members
- `jurisdiction` (str) — `us` for federal or 2-letter state code
- `target` (`all` / `webflow` / `pinecone`)
- `limit` (int) — cap items processed (0 = unlimited)
- `historical_since` (YYYY-MM-DD)
- `audit_only` (`A` = federal join-key coverage / `B` = bulk-import readiness — no missing or duplicate openstatesid / `C` = pre-existing state CMS records lacking openstatesid)
- `strict_schema` (bool) — Phase-3 fail-fast on schema-cache silent drops; flip True for the first run after adding a new write target
- `upload_photos` (bool) — Phase-3 photo upload to Webflow's asset library (populates `legislator-image`); requires `webflow_assets_read_write_key` with `assets:read assets:write` scopes
- `upload_photos_dry_run` (bool) — Phase-3 connectivity smoke; fetches + size-validates source images but skips Webflow asset creation

Returns 503 + `Retry-After: 60` while the unitedstates dataset is still being parsed at app startup (~55s; pre-warm fires at startup).

After non-dry-run completion (including aborted runs) the bio sync POSTs a summary to the Zapier webhook configured via the `ZAPIER_WEBHOOK_URL` env var (same setting used by the Voatz→Brevo sync). Both alerts share the webhook and route via the top-level `alert_type` field — `user_sync_complete` for Voatz→Brevo, `legislator_bio_sync_complete` for bio-sync. Bio-sync payload includes `on_failure` and `on_large_changes` boolean flags + pre-formatted `failure_warning` and `large_changes_warning` strings (empty when not applicable, populated text otherwise — Zapier doesn't support Mustache conditionals, so flatten at the source). Phase-4 photo coverage metrics: `photo_uploads_attempted`, `photo_uploads_succeeded`, `photo_uploads_failed`, `photo_coverage_ratio` (rounded to 3dp; null when no attempts). Set `ZAPIER_WEBHOOK_URL=` (empty) to disable alerts without removing the variable.

The bio sync uses a multi-reference `seat` field on Legislators CMS records to determine federal vs state classification (refs into a Seats CMS with 4 items: `us-house`, `us-senate`, `state-house`, `state-senate`). The two federal seat ref-IDs are hardcoded in `pipelines/legislator_bio.py::_FEDERAL_SEAT_REF_IDS`. If the Seats CMS items are ever recreated, that constant needs a one-line update.

Phase 1 + 2.5 + 3 + 4 (V1) shipped 2026-04-30 → 2026-05-01 against the FL congressional delegation (32) + 192 FL state legs. End-state population (read-only Webflow probe): federal `bioguide-id` / `wikidata-id` / `ballotpedia-slug` / `govtrack-id` / `birth-year` / `gender` / `term-start` / `term-end` / `phone-capitol` / `office-address-capitol` / `contact-form-url` / `official-website` / `photo-source-url` / `open-states-url` all 100%; state `gender` / `open-states-url` 100%, `office-email` 99%, `official-website` 98%, `photo-source-url` 98%, `legislator-image` 96.4%, `birth-year` 71%, capitol contact ~80%. Audit B is wired (`audit_only=B` returns missing-openstatesid records and openstatesid duplicates). Phase-3 photo upload pipeline uploads source images to Webflow's asset library (federal records have a congress.gov fallback when the unitedstates/images dataset 404s). Phase 4 V1 hardened the scheduler-enable path (config wiring for `upload_photos` / `strict_schema`, startup-time scope validation, photo coverage metrics). **The bio sync is enabled in the weekly cron as of 2026-05-01** (`legislator_bio_sync.enabled: true` + `upload_photos: true` in `sync_schedule.yaml`). Phase 4.5/5 backlog: `--undo-last-run` mass-revert; post-upload HEAD probe; Redis cross-run photo dedup; bulk-data state term-history; alternate state social-handles upstream.

**⚠ Schema-change checklist for the Legislators CMS:** when adding a new field to the Webflow Legislators collection, **publish the Webflow site** before running the bio sync. The schema endpoint reflects new fields immediately, but the items endpoint silently ignores writes to unpublished fields (PATCHes return 200 but the value doesn't persist). Verify with `scripts/probe_webflow_legislators.py` after the first sync run.

Available Webflow jobs: `fill-session-code`, `fill-map-url`, `bill-org-sync`, `org-about-parse`, `check-org-missing`, `find-duplicates`, `merge-duplicate-orgs`

`merge-duplicate-orgs` detects exact-name duplicates across the Member Organizations collection (after normalization for "The", "&"→"and", Inc/LLC/Foundation suffixes, punctuation), merges bill references bidirectionally, and deletes the sparse copy. Canonical record is chosen by richness score (bill refs + populated fields). Safe to re-run — a second pass cleans up any stale references that blocked deletion on the first pass.

### API Health Check

Runs nightly at 09:00 UTC via APScheduler. Dynamically generates one check per configured Voatz org (each org is scoped to a jurisdiction) and POSTs to `/get_events` on `api.digitaldemocracyproject.org` with Voatz auth tokens. Asserts HTTP 200, non-empty body, valid JSON, and at least one item in the results list.

**Manual run:**
```bash
.venv/bin/python scripts/check_api_health.py           # run + alert on failure
.venv/bin/python scripts/check_api_health.py --dry-run # run without alerting
```

**Zapier payload** (`alert_type: api_health_check_failed`, fires only on failures):

| Field | Description |
|---|---|
| `on_failure` | Always `true` when the webhook fires |
| `failure_count` / `total_checks` | e.g. `2` / `9` |
| `slack_message` | Pre-formatted mrkdwn string — use as `{{slack_message}}` in Zapier's Slack action |
| `failures_text` | Same content as bullet list, without the header line |
| `failures` | Array of `{name, description, error, status_code, body_preview, duration_ms}` |
| `checked_at` | ISO 8601 timestamp |

Add checks for non-Voatz endpoints in `FALLBACK_CHECKS` in `src/ddp_sync/pipelines/api_health_check.py`.

### Health / Schedule

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (scheduler, Redis, Pinecone, flow status) |
| GET | `/schedule` | Show all scheduled jobs and next run times |

## Deployment

ddp-sync runs on **two hosts**, no leader election — each runs its own scheduler
(`app.py`: "single worker, no leader election"), so their job sets overlap harmlessly:

| Host | Manager | Role |
|------|---------|------|
| **EC2 civic** | systemd (`ddp-sync.service`) | Canonical scheduler for cloud jobs: Webflow CMS, Pinecone, Brevo, legislator-bio |
| **Mac Studio** | **system LaunchDaemon** (`com.ddp.ddp-sync`) | Runs the OpenStates **scrapes** — must be local (subprocesses `run-scrape.sh` against the local Postgres) |

A change to this repo should be deployed to **both** targets.

### EC2 civic (systemd)

```bash
cd /home/ubuntu/ddp-sync
git pull origin main
source .venv/bin/activate
pip install .
sudo systemctl restart ddp-sync
```

### Mac Studio (system LaunchDaemon)

The Mac is administered SSH-only with no reliable Aqua session, so ddp-sync there is a
**system LaunchDaemon** (`/Library/LaunchDaemons/com.ddp.ddp-sync.plist`, source in
`infrastructure/com.ddp.ddp-sync.plist`) — **not** a GUI LaunchAgent, which can't be
reloaded over SSH. Deploy from an admin account:

```bash
cd /Users/agentsmith/Developer/repos/ddp-sync && git pull origin main
sudo launchctl kickstart -k system/com.ddp.ddp-sync   # restart to pick up changes
curl -s http://localhost:8001/ddp-sync/v1/schedule    # verify the six OpenStates jobs
```

Reinstall the plist (after editing it) and full recovery steps are in
`ddp-infra/README.md` → "Restart Procedures".

### Systemd Service

```bash
sudo systemctl status ddp-sync    # Check status
sudo systemctl restart ddp-sync   # Restart
sudo systemctl stop ddp-sync      # Stop
sudo journalctl -u ddp-sync -f    # Stream logs
```

### First-Time Setup

```bash
cd /home/ubuntu
git clone git@github.com:Digital-Democracy-Project/ddp-sync.git
cd ddp-sync
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .

# Verify
python -c "from ddp_sync.config import get_settings; print('OK')"

# Install systemd service
sudo cp infrastructure/ddp-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ddp-sync
sudo systemctl start ddp-sync
```

## Local Development

```bash
# Clone and setup
git clone git@github.com:Digital-Democracy-Project/ddp-sync.git
cd ddp-sync
python3 -m venv .venv
source .venv/bin/activate
pip install ".[dev]"

# Copy env
cp .env.example .env
# Edit .env with your API keys (see votebot/.env and DDP-API/.env for values)

# Run
uvicorn ddp_sync.app:app --host 0.0.0.0 --port 8001 --reload
```

> **Note:** Use `pip install .` (non-editable) instead of `pip install -e .` if you encounter `.pth` file issues with Python 3.13 on macOS. Non-editable install requires re-running `pip install .` after code changes. The `config/sync_schedule.yaml` path resolves from either the package directory or CWD.

## Logs

```bash
# Stream logs
sudo journalctl -u ddp-sync -f

# Filter by time
sudo journalctl -u ddp-sync --since "04:00" --until "05:00" --no-pager

# Errors in last 24 hours
sudo journalctl -u ddp-sync --since "24 hours ago" -p err --no-pager
```

## Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for known issues, past bugs, and their resolutions.

### Webflow CMS field name reference

Key bill fields (actual Webflow API names):

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `session-code` | string | `"2026"`, `"57th-2nd-regular"` | OpenStates session identifier |
| `bill-session` | integer | `2026` | Calendar year — **NOT** `session-year` |
| `open-states-url-2` | string | `https://openstates.org/az/bills/...` | Used for jurisdiction resolution |
| `status` | string | `"Referred to committee"` | Written by Flow 1 (OpenStates → Webflow) |
| `status-date` | string | `"2026-01-20T00:00:00.000Z"` | Written by Flow 1 |
| `status-chamber` | string | `"Senate"` | Chamber of latest action (e.g. "House", "Senate", "Office of the Governor") |
| `gov-url` | string | `https://www.azleg.gov/...` | Official bill text URL, written by Flow 1 |

## Related Repositories

- [DDP-API](https://github.com/Digital-Democracy-Project/ddp-api) — Auth gateway + API proxy
- [VoteBot](https://github.com/Digital-Democracy-Project/votebot) — Chat/RAG service

## License

This project is open source and available under the [MIT License](LICENSE).
