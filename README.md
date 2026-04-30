# DDP-Sync

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An open-source, unified data pipeline service for the Digital Democracy Project.

## Architecture

DDP-Sync handles all scheduled and on-demand data sync operations:

- **Daily bill sync** (04:00 UTC): Shared OpenStates fetch with independent write paths:
  - Flow 1: OpenStates → Webflow CMS (status, status-date, status-chamber, gov-url)
  - Flow 2: OpenStates → Pinecone (bill text re-ingestion on new versions)
  - Either flow can be disabled independently in `sync_schedule.yaml`
- **Legislator sync** (weekly Sun 06:00 UTC): OpenStates → Pinecone
- **Legislator bio sync** (weekly Sun 07:00 UTC, `enabled: false` by default): unitedstates/congress-legislators + OpenStates → Webflow Legislators CMS (bio, contact, term, social, photo URL fields)
- **Organization sync** (monthly 1st 08:00 UTC): Webflow → Pinecone
- **Voatz → Brevo user sync** (every 30 min): Voatz → Brevo contact lists
- **Voatz → Brevo full-attribute sync** (monthly 1st 02:00 UTC): Full re-import
- **Webflow CMS batch jobs** (weekly Mon 03:00 UTC): Fill fields, sync refs, detect duplicates

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

DDP-Sync also stores `bill_slug` alongside `last_checked` in the `ddp:bill_version:{webflow_id}` Redis record so VoteBot's startup reconciliation can map webflow_id → slug without an extra Webflow API call.

## Configuration

- **Production**: AWS Secrets Manager (`ddp-sync/credentials`)
- **Local dev**: `.env` file (copy from `.env.example`)

Config is loaded once at startup. Source priority: Secrets Manager → `.env` → defaults.

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

`/trigger/bill-status-sync` accepts query params: `all_sessions` (bool), `jurisdiction` (str)

`/trigger/legislator-bio-sync` accepts query params: `dry_run` (bool), `auto_create` (bool), `jurisdiction` (str — `us` for federal or state code), `target` (`all` / `webflow` / `pinecone`), `limit` (int), `historical_since` (YYYY-MM-DD), `audit_only` (`A` = federal join-key coverage / `B` = bulk-import readiness (no missing or duplicate openstatesid) / `C` = pre-existing state CMS records lacking openstatesid). Returns 503 + `Retry-After: 60` while the unitedstates dataset is still being parsed at app startup (~55s; pre-warm fires at startup).

After non-dry-run completion (including aborted runs) the bio sync POSTs a summary to the Zapier webhook configured via the `ZAPIER_WEBHOOK_URL` env var (same setting used by the Voatz→Brevo sync). Both alerts share the webhook and route via the top-level `alert_type` field — `user_sync_complete` for Voatz→Brevo, `legislator_bio_sync_complete` for bio-sync. Bio-sync payload includes `on_failure` and `on_large_changes` boolean flags + pre-formatted `failure_warning` and `large_changes_warning` strings (empty when not applicable, populated text otherwise — Zapier doesn't support Mustache conditionals, so flatten at the source). Set `ZAPIER_WEBHOOK_URL=` (empty) to disable alerts without removing the variable.

The bio sync uses a multi-reference `seat` field on Legislators CMS records to determine federal vs state classification (refs into a Seats CMS with 4 items: `us-house`, `us-senate`, `state-house`, `state-senate`). The two federal seat ref-IDs are hardcoded in `pipelines/legislator_bio.py::_FEDERAL_SEAT_REF_IDS`. If the Seats CMS items are ever recreated, that constant needs a one-line update.

Phase 1 sync (federal + state baseline) was shipped 2026-04-30 against the FL congressional delegation + 192 FL state legs. Audit B is wired (`audit_only=B` returns missing-openstatesid records and openstatesid duplicates). The `legislator_bio_sync` block in `config/sync_schedule.yaml` defaults to `enabled: false` — operator flips after editor verification + monitoring window. State-leg payload is intentionally conservative (bio + capitol contact + photo URL); social handles, official-website, and term dates for state legs are deferred to Phase 2.5 pending an OpenStates probe.

Available Webflow jobs: `fill-session-code`, `fill-map-url`, `bill-org-sync`, `org-about-parse`, `check-org-missing`, `find-duplicates`

### Health / Schedule

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (scheduler, Redis, Pinecone, flow status) |
| GET | `/schedule` | Show all scheduled jobs and next run times |

## Deployment

```bash
cd /home/ubuntu/ddp-sync
git pull origin main
source .venv/bin/activate
pip install .
sudo systemctl restart ddp-sync
```

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
- [FillWebflowFields](https://github.com/VotingRightsBrigade/FillWebflowFields) — Webflow CMS management package (`webflow_cms`)

## License

This project is open source and available under the [MIT License](LICENSE).
