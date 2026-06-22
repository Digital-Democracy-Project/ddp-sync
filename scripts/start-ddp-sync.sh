#!/usr/bin/env bash
# start-ddp-sync.sh — Start the DDP-Sync FastAPI service with APScheduler.
# Used by the launchd plist (com.ddp.ddp-sync) and can also be run manually.
set -euo pipefail

PROJECT_DIR="/Users/agentsmith/Developer/repos/ddp-sync"
VENV="$PROJECT_DIR/.venv"
LOG="$PROJECT_DIR/logs/ddp-sync.log"

cd "$PROJECT_DIR"

log() { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') [start-ddp-sync] $*"; }

# Load secrets from .env if present
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
fi

# Scheduler must be enabled explicitly so accidental restarts on EC2
# don't result in two schedulers fighting over the same Redis jobs.
export SCHEDULER_ENABLED=true

log "Starting DDP-Sync..."
exec "$VENV/bin/uvicorn" ddp_sync.app:app \
    --host 0.0.0.0 \
    --port 8001 \
    --log-level info
