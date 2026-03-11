# DDP-Sync

Unified data pipeline service for the Digital Democracy Project.

## Architecture

DDP-Sync handles all scheduled and on-demand data sync operations:

- **Bill version sync** (daily 04:00 UTC): OpenStates → Webflow CMS + Pinecone
- **Legislator sync** (weekly Sun 06:00 UTC): OpenStates → Pinecone
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

## Configuration

- **Production**: AWS Secrets Manager (`ddp-sync/credentials`)
- **Local dev**: `.env` file (copy from `.env.example`)

Config is loaded once at startup. Source priority: Secrets Manager → `.env` → defaults.

## API

All endpoints are prefixed with `/ddp-sync/v1`.

### Sync Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/sync/unified` | Trigger batch or single sync |
| GET | `/sync/unified/status/{id}` | Poll task status |
| POST | `/sync/unified/all` | Trigger sync for all content types |

### Trigger Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/trigger/user-sync` | Trigger Voatz → Brevo incremental sync |
| POST | `/trigger/full-sync` | Trigger Voatz → Brevo full-attribute sync |
| POST | `/trigger/webflow/{job}` | Trigger specific Webflow batch job |

Available Webflow jobs: `fill-session-code`, `fill-map-url`, `bill-org-sync`, `org-about-parse`, `check-org-missing`, `find-duplicates`

### Health / Schedule

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (scheduler, Redis, Pinecone) |
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

## Related Repositories

- [DDP-API](https://github.com/Digital-Democracy-Project/ddp-api) — Auth gateway + API proxy
- [VoteBot](https://github.com/Digital-Democracy-Project/votebot) — Chat/RAG service
- [FillWebflowFields](https://github.com/VotingRightsBrigade/FillWebflowFields) — Webflow CMS management package (`webflow_cms`)
