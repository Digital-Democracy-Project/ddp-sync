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

# Slack bot token for direct health alerts (no Zapier) — canonical source is
# ddp-agents/.env, shared with run-scrape.sh and the bash health monitor.
if [ -z "${SLACK_BOT_TOKEN:-}" ]; then
    SLACK_BOT_TOKEN=$(grep -E '^SLACK_BOT_TOKEN=' /Users/agentsmith/Developer/repos/ddp-agents/.env \
        2>/dev/null | head -1 | cut -d'=' -f2- | tr -d '"'"'" | awk '{print $1}')
    export SLACK_BOT_TOKEN
fi

# SYNC-99: the alerts post as CodeBot (ddp_sync/slack_identity.py). The username defaults to "CodeBot", but
# the icon emoji is configured only in ddp-agents/.env, so copy the two persona variables across the same way
# as the bot token above, and only when they are not already set (a value in this service's own .env wins).
# --- persona vars (the test runs this block)
# printenv, not ${!var}: macOS ships bash 3.2. `|| true`: a missing file or key must not end the script under set -e.
# The value is the rest of the line minus an inline " # comment" and surrounding quotes (it can contain spaces).
for var in CODEBOT_SLACK_USERNAME CODEBOT_SLACK_ICON_EMOJI; do
    if [ -z "$(printenv "$var" || true)" ]; then
        value=$(grep -E "^${var}=" "${AGENTS_ENV:-/Users/agentsmith/Developer/repos/ddp-agents/.env}" 2>/dev/null \
            | head -1 | cut -d'=' -f2- \
            | sed -E -e 's/[[:space:]]+#.*$//' -e 's/[[:space:]]+$//' -e "s/^[\"']//" -e "s/[\"']\$//" || true)
        if [ -n "$value" ]; then
            export "$var=$value"
        fi
    fi
done
# --- end persona vars

# Scheduler must be enabled explicitly so accidental restarts on EC2
# don't result in two schedulers fighting over the same Redis jobs.
export SCHEDULER_ENABLED=true

log "Starting DDP-Sync..."
exec "$VENV/bin/uvicorn" ddp_sync.app:app \
    --host 0.0.0.0 \
    --port 8001 \
    --log-level info
