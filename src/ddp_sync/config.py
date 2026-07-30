"""
Configuration for DDP-Sync.

Priority: AWS Secrets Manager -> .env file -> defaults.
Production uses Secrets Manager. Local dev uses .env.
"""

import json
import os
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

AWS_SECRET_NAME = os.getenv("AWS_SECRET_NAME", "ddp-sync/credentials")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Track which source was used
_config_source = "unknown"


@dataclass
class SyncSettings:
    """Settings object for sync code. Mirrors fields from VoteBot's Settings
    that the sync/ingestion/updates code actually references."""

    # Auth
    api_key: str = ""
    ddp_api_key: str = ""  # Bearer token for outbound calls to DDP-API

    # OpenAI
    openai_api_key: str = ""
    openai_embedding_model: str = "text-embedding-3-large"

    # Pinecone
    pinecone_api_key: str = ""
    pinecone_environment: str = "us-east-1"
    pinecone_index_name: str = "votebot-large"
    pinecone_namespace: str = "default"

    # External APIs
    openstates_api_key: str = ""
    congress_api_key: str = ""

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Webflow
    webflow_votebot_api_key: str = ""  # Read-only CMS key (CMS:read scope)
    webflow_scheduler_api_key: str = ""  # Write-capable CMS key (CMS:write scope)
    webflow_api_token: str = ""
    webflow_assets_read_write_key: str = ""  # Phase-3: assets:read + assets:write scopes for photo upload pipeline
    webflow_site_id: str = ""
    webflow_bills_collection_id: str = ""
    webflow_jurisdiction_collection_id: str = ""
    webflow_legislators_collection_id: str = ""
    webflow_categories_collection_id: str = ""
    webflow_organizations_collection_id: str = ""

    # Brevo / Voatz (Phase 2)
    brevo_api_key: str = ""
    brevo_rate_limit_rph: int = 36000
    blacklist: list = field(default_factory=list)
    zapier_webhook_url: str = ""
    sync_interval_minutes: int = 30
    organizations: list = field(default_factory=list)

    # Ingestion tuning
    chunk_size: int = 1000
    chunk_overlap: int = 200
    pdf_max_pages: int = 1000

    # App
    environment: str = "production"
    debug: bool = False
    log_level: str = "INFO"

    # CAMS (LegBot dispatch — PLAN-legbot.md Phase 3). Local Mac Studio
    # instance only, per Ramon's 2026-07-20 call to run this dispatching
    # from ddp-sync's local instance, not EC2 production — same-box call,
    # no WireGuard hop needed.
    cams_base_url: str = "http://localhost:8000"
    cams_api_token: str = ""
    # CAMS's artifacts/{task_id}/task_result.json directory — read directly
    # off the shared local filesystem rather than a new HTTP endpoint,
    # matching how Agent Smith's own get_task_artifacts tool already reads
    # this same directory in-process. No default — this is a machine-
    # specific absolute path (ddp-agents' working directory); must be set
    # via CAMS_ARTIFACTS_DIR, not guessed.
    cams_artifacts_dir: str = ""

    # ddp-broker-py (BillArtifact write path — ddp-infra Phase 8). Local Mac
    # Studio dev stack by default (README's port 8080); production points at
    # the real broker host/token via env.
    ddp_broker_api_base: str = "http://localhost:8080"
    ddp_broker_api_token: str = ""

    # Local api-v3 archived-text lookup (ddp-infra PLAN-bill-document-
    # provenance.md, "Real gap found 2026-07-29/30" — OPEN-13). A small,
    # dedicated read against ddp-open-states' own local api-v3 instance
    # (docker-compose.ddp.yml maps its container to host port 8002 on the
    # Mac Studio, same box ddp-sync's LegBot dispatch already runs on — see
    # cams_base_url above), NOT the same thing as openstates_api_key/the
    # public v3.openstates.org API bill_sync.py calls, and NOT the site-wide
    # OPENSTATES_API_BASE cutover PLAN-local-openstates-migration.md scopes.
    # No default key — api-v3's apikey_auth has no dev bypass; must be set
    # (the local dev stack's seeded Profile key is the well-known
    # 00000000-0000-0000-0000-000000000001 sentinel used elsewhere in this
    # project, e.g. ddp-open-states/quality_check.py, start-os-api.sh).
    local_openstates_api_base: str = "http://localhost:8002"
    local_openstates_api_key: str = ""

    # Fields that VoteBot code references but are not relevant to sync
    # Included as no-ops to avoid AttributeError during migration
    openai_model: str = ""
    openai_max_tokens: int = 4096
    openai_temperature: float = 0.7
    max_retrieval_chunks: int = 10
    similarity_threshold: float = 0.7
    request_timeout_seconds: int = 30
    max_concurrent_requests: int = 10
    app_name: str = "ddp-sync"
    app_version: str = "0.1.0"
    api_prefix: str = "/ddp-sync/v1"
    allowed_origins: str = "*"


# Backward-compat alias so moved code can use `from ddp_sync.config import Settings`
Settings = SyncSettings


def _load_from_secrets_manager() -> dict | None:
    global _config_source
    try:
        import boto3
        client = boto3.client("secretsmanager", region_name=AWS_REGION)
        response = client.get_secret_value(SecretId=AWS_SECRET_NAME)
        config = json.loads(response["SecretString"])
        _config_source = "secrets_manager"
        logger.info(f"Loaded config from Secrets Manager: {AWS_SECRET_NAME}")
        return config
    except Exception as e:
        logger.warning(f"Secrets Manager unavailable: {e}")
        return None


def _load_from_env() -> dict:
    """Fallback: build config dict from environment variables."""
    global _config_source
    from dotenv import load_dotenv
    load_dotenv()
    _config_source = "env"
    return {
        "api_key": os.getenv("DDP_SYNC_API_KEY", ""),
        "ddp_api_key": os.getenv("DDP_API_KEY", ""),
        "openai_api_key": os.getenv("OPENAI_API_KEY", ""),
        "openai_embedding_model": os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"),
        "pinecone_api_key": os.getenv("PINECONE_API_KEY", ""),
        "pinecone_environment": os.getenv("PINECONE_ENVIRONMENT", "us-east-1"),
        "pinecone_index_name": os.getenv("PINECONE_INDEX_NAME", "votebot-large"),
        "pinecone_namespace": os.getenv("PINECONE_NAMESPACE", "default"),
        "openstates_api_key": os.getenv("OPENSTATES_API_KEY", ""),
        "congress_api_key": os.getenv("CONGRESS_API_KEY", ""),
        "redis_url": os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        "webflow_votebot_api_key": os.getenv("WEBFLOW_VOTEBOT_API_KEY", ""),
        "webflow_scheduler_api_key": os.getenv("WEBFLOW_SCHEDULER_API_KEY", ""),
        "webflow_api_token": os.getenv("WEBFLOW_API_TOKEN", ""),
        "webflow_assets_read_write_key": os.getenv("WEBFLOW_ASSETS_READ_WRITE_KEY", ""),
        "webflow_site_id": os.getenv("WEBFLOW_SITE_ID", ""),
        "webflow_bills_collection_id": os.getenv("WEBFLOW_BILLS_COLLECTION_ID", ""),
        "webflow_jurisdiction_collection_id": os.getenv("WEBFLOW_JURISDICTION_COLLECTION_ID", ""),
        "webflow_legislators_collection_id": os.getenv("WEBFLOW_LEGISLATORS_COLLECTION_ID", ""),
        "webflow_categories_collection_id": os.getenv("WEBFLOW_CATEGORIES_COLLECTION_ID", ""),
        "webflow_organizations_collection_id": os.getenv("WEBFLOW_ORGANIZATIONS_COLLECTION_ID", ""),
        "brevo_api_key": os.getenv("BREVO_API_KEY", ""),
        "brevo_rate_limit_rph": int(os.getenv("BREVO_RATE_LIMIT_RPH", "36000")),
        "blacklist": [],
        "zapier_webhook_url": os.getenv("ZAPIER_WEBHOOK_URL", ""),
        "sync_interval_minutes": int(os.getenv("SYNC_INTERVAL_MINUTES", "30")),
        "chunk_size": int(os.getenv("CHUNK_SIZE", "1000")),
        "chunk_overlap": int(os.getenv("CHUNK_OVERLAP", "200")),
        "pdf_max_pages": int(os.getenv("PDF_MAX_PAGES", "1000")),
        "environment": os.getenv("ENVIRONMENT", "production"),
        "debug": os.getenv("DEBUG", "false").lower() == "true",
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "cams_base_url": os.getenv("CAMS_BASE_URL", "http://localhost:8000"),
        "cams_api_token": os.getenv("CAMS_API_TOKEN", ""),
        "cams_artifacts_dir": os.getenv("CAMS_ARTIFACTS_DIR", ""),
        "ddp_broker_api_base": os.getenv("DDP_BROKER_API_BASE", "http://localhost:8080"),
        "ddp_broker_api_token": os.getenv("DDP_BROKER_API_TOKEN", ""),
        "local_openstates_api_base": os.getenv("LOCAL_OPENSTATES_API_BASE", "http://localhost:8002"),
        "local_openstates_api_key": os.getenv("LOCAL_OPENSTATES_API_KEY", ""),
    }


@lru_cache
def get_settings() -> SyncSettings:
    """Load config and return a SyncSettings instance.

    Named get_settings() so moved code that calls
    `from ddp_sync.config import get_settings` works with minimal changes.
    """
    raw = _load_from_secrets_manager()
    if raw is None:
        raw = _load_from_env()

    # Build SyncSettings from dict, ignoring unknown keys
    known_fields = {f for f in SyncSettings.__dataclass_fields__}
    filtered = {k: v for k, v in raw.items() if k in known_fields}
    return SyncSettings(**filtered)


def get_config_source() -> str:
    """Return which config source was used (for health check)."""
    # Ensure config has been loaded
    get_settings()
    return _config_source
