#!/usr/bin/env bash
# OPEN-193: resolves ddp-sync's production secrets on the HOST (which already has verified
# secretsmanager:GetSecretValue access -- see notes/ops-handoff, 2026-09-02) and writes them as
# plain env vars to infrastructure/../.env, consumed by docker-compose.prod.yml's `env_file:`.
#
# Deliberately NOT done from inside the container: this box's container network can't reach the
# EC2 instance metadata service the way the bare-metal process could, and giving the container
# its own AWS role just to read two secrets is more surface than resolving them here once, where
# access is already proven. Matches config.py's own documented fallback path ("AWS Secrets
# Manager -> .env file -> defaults") -- this populates that .env file, it doesn't bypass it.
#
# Idempotent: re-run any time to pick up a rotated secret. Never prints a secret value -- only
# which keys were written and to where.
set -euo pipefail

REGION="us-east-1"
DDP_SYNC_SECRET_ID="ddp-sync/credentials"
RDS_HOST="ddp-openstates.cvxdhm1ogxug.us-east-1.rds.amazonaws.com"
RDS_PORT="5432"
RDS_DBNAME="openstates"

OUT_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.env"

# The ARN of the RDS-managed secret is NOT kept in this committed file. Resolution order:
#   1. the environment (RDS_CREDENTIALS_SECRET_ARN), so a manual or systemd run can set it;
#   2. the value this script last wrote to .env (gitignored, mode 600, same name the container uses);
#   3. otherwise stop with a clear message before any AWS call.
RDS_SECRET_ID="${RDS_CREDENTIALS_SECRET_ARN:-}"
if [ -z "$RDS_SECRET_ID" ] && [ -f "$OUT_FILE" ]; then
  RDS_SECRET_ID="$(grep -m1 '^RDS_CREDENTIALS_SECRET_ARN=' "$OUT_FILE" | cut -d= -f2- | tr -d '"' || true)"
fi
: "${RDS_SECRET_ID:?[render-env] RDS_CREDENTIALS_SECRET_ARN is not set and not found in $OUT_FILE; set it in the environment or restore it in .env (the ARN was moved out of this script)}"

echo "[render-env] fetching ddp-sync/credentials..."
DDP_SYNC_JSON=$(aws secretsmanager get-secret-value --region "$REGION" --secret-id "$DDP_SYNC_SECRET_ID" --query 'SecretString' --output text)

echo "[render-env] fetching RDS master credential..."
RDS_JSON=$(aws secretsmanager get-secret-value --region "$REGION" --secret-id "$RDS_SECRET_ID" --query 'SecretString' --output text)

export DDP_SYNC_JSON RDS_JSON
python3 - "$OUT_FILE" "$RDS_HOST" "$RDS_PORT" "$RDS_DBNAME" "$RDS_SECRET_ID" <<'PYEOF'
import json
import os
import sys
import urllib.parse

out_file, rds_host, rds_port, rds_dbname, rds_secret_id = sys.argv[1:6]

ddp_sync_json = json.loads(os.environ["DDP_SYNC_JSON"])
rds_json = json.loads(os.environ["RDS_JSON"])

# secret key (snake_case, matches SyncSettings' own field names) -> env var name
# (matches config.py's _load_from_env() / .env.example exactly -- this populates the same
# fallback path the code already documents, not a new convention).
KEY_MAP = {
    "api_key": "DDP_SYNC_API_KEY",
    "openai_api_key": "OPENAI_API_KEY",
    "openai_embedding_model": "OPENAI_EMBEDDING_MODEL",
    "pinecone_api_key": "PINECONE_API_KEY",
    "pinecone_environment": "PINECONE_ENVIRONMENT",
    "pinecone_index_name": "PINECONE_INDEX_NAME",
    "pinecone_namespace": "PINECONE_NAMESPACE",
    "openstates_api_key": "OPENSTATES_API_KEY",
    "congress_api_key": "CONGRESS_API_KEY",
    "brevo_api_key": "BREVO_API_KEY",
    "brevo_rate_limit_rph": "BREVO_RATE_LIMIT_RPH",
    "sync_interval_minutes": "SYNC_INTERVAL_MINUTES",
    "zapier_webhook_url": "ZAPIER_WEBHOOK_URL",
    "webflow_api_token": "WEBFLOW_API_TOKEN",
    "webflow_assets_read_write_key": "WEBFLOW_ASSETS_READ_WRITE_KEY",
    "webflow_site_id": "WEBFLOW_SITE_ID",
    "webflow_bills_collection_id": "WEBFLOW_BILLS_COLLECTION_ID",
    "webflow_jurisdiction_collection_id": "WEBFLOW_JURISDICTION_COLLECTION_ID",
    "webflow_legislators_collection_id": "WEBFLOW_LEGISLATORS_COLLECTION_ID",
    "webflow_categories_collection_id": "WEBFLOW_CATEGORIES_COLLECTION_ID",
    "webflow_organizations_collection_id": "WEBFLOW_ORGANIZATIONS_COLLECTION_ID",
    "webflow_scheduler_api_key": "WEBFLOW_SCHEDULER_API_KEY",
    "webflow_votebot_api_key": "WEBFLOW_VOTEBOT_API_KEY",
    # OPEN-286: reused from ddp-broker-py's existing Slack app token (same workspace, already
    # has chat:write scope, confirmed live against #automation-errors) rather than minting a
    # new one -- _alert_scrape_failure() already reads SLACK_BOT_TOKEN, it was just never set.
    "slack_bot_token": "SLACK_BOT_TOKEN",
    # SYNC-131: the CodeBot persona for those alerts (slack_identity.codebot_identity()). On the Mac,
    # scripts/start-ddp-sync.sh copies these from ddp-agents/.env; EC2 has no such file, so without
    # them its alerts post as CodeBot with the default :robot_face: icon instead of :codebot:.
    "codebot_slack_username": "CODEBOT_SLACK_USERNAME",
    "codebot_slack_icon_emoji": "CODEBOT_SLACK_ICON_EMOJI",
    # SYNC-132: the CAMS token slack_alerts.post_alert presents to CAMS's /api/v1/operator-alerts (owner decision
    # 2026-10-28: the regular CAMS token, no second token). Deliberately NOT stored under the key `cams_api_token`:
    # get_settings() copies every secret key that matches a SyncSettings field, and a set `cams_api_token` is the
    # code's "this host is the Mac" signal (openstates_archive._mac_capable(), replica routing in ingestion/sources/
    # openstates.py), so that key would send EC2's replica reads to localhost:8002 and make it behave like the Mac
    # at the next process start. `operator_alert_token` is not a SyncSettings field; only post_alert reads it.
    "operator_alert_token": "OPERATOR_ALERT_TOKEN",
    # SYNC-59: this EC2 process's Bearer token for calling the Mac Studio's own ddp-sync
    # (/trigger/scraper-session-legbot), to trigger LegBot after a cloud-owned scrape's
    # RDS-load finishes. Must match the Mac's own DDP_SYNC_API_KEY exactly -- a fresh one was
    # generated there for this purpose (the Mac's own inbound key wasn't set before).
    "mac_ddp_sync_api_key": "MAC_DDP_SYNC_API_KEY",
    # SYNC-59/SYNC-65: the credential this EC2 process presents to its own local RDS-backed
    # api-v3 (OPEN-279) when resolving which sessions an archive/scrape run actually touched,
    # closing the "known partial-rollout gap" both tickets' docstrings named -- reusing the
    # same token already loaded into that api-v3's own profiles_profile for ddp-broker
    # (BROKER-41), not a new one.
    "rds_openstates_api_key": "RDS_OPENSTATES_API_KEY",
    # "redis_url" deliberately NOT mapped -- docker-compose.prod.yml's own `environment:`
    # block sets REDIS_URL explicitly (DB 3, avoiding ddp-broker's DB 0/1/2) and Compose gives
    # `environment:` precedence over `env_file:` for the same name, so this would be shadowed
    # anyway; omitting it here avoids the false impression this file controls it.
}

lines = ["# Generated by infrastructure/render-env.sh -- do not edit by hand, do not commit.\n"]
written = []
for secret_key, env_name in KEY_MAP.items():
    if secret_key in ddp_sync_json and ddp_sync_json[secret_key] not in (None, ""):
        value = str(ddp_sync_json[secret_key])
        lines.append(f'{env_name}={value}\n')
        written.append(env_name)

password = urllib.parse.quote(rds_json["password"], safe="")
user = rds_json["username"]
rds_url = f"postgresql://{user}:{password}@{rds_host}:{rds_port}/{rds_dbname}"
lines.append(f'RDS_DATABASE_URL={rds_url}\n')
written.append("RDS_DATABASE_URL")

# OPEN-260: lets consumers resolve a live credential from Secrets Manager at call time
# instead of reusing this rendered (and, on the 7-day rotation cadence, eventually stale)
# RDS_DATABASE_URL. Same secret, just also exposed as its ARN for services/rds_credentials.py.
lines.append(f'RDS_CREDENTIALS_SECRET_ARN={rds_secret_id}\n')
written.append("RDS_CREDENTIALS_SECRET_ARN")
# OPEN-260 (PR #128): the RDS-managed secret itself only has username/password -- resolve_
# rds_database_url() needs host/port/dbname from here instead, same values this script
# already hardcodes above for RDS_DATABASE_URL.
lines.append(f'RDS_HOST={rds_host}\n')
written.append("RDS_HOST")
lines.append(f'RDS_PORT={rds_port}\n')
written.append("RDS_PORT")
lines.append(f'RDS_DBNAME={rds_dbname}\n')
written.append("RDS_DBNAME")

with open(out_file, "w") as f:
    f.writelines(lines)
os.chmod(out_file, 0o600)

print(f"[render-env] wrote {len(written)} keys to {out_file}: {sorted(written)}")
PYEOF
unset DDP_SYNC_JSON RDS_JSON
