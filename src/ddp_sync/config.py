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
    # Public v3.openstates.org API base. Configurable so it matches the
    # local_openstates_api_base pattern below -- SYNC-6 needs this to be
    # overridable so BillSyncService.fetch_bill_from_openstates() can be
    # pointed at a different base in tests without patching a class
    # constant.
    openstates_api_base: str = "https://v3.openstates.org"
    # Jurisdictions to route to the local OpenStates replica instead of the
    # public API (SYNC-6). Deliberately the *same* env var name
    # ddp-broker-py's settings.py reads (DDP_OPENSTATES_JURISDICTIONS) so
    # both services can be pointed at one shared value -- ddp-sync does NOT
    # hardcode its own default jurisdiction list here, to avoid drifting
    # from whatever broker is actually routing. Empty by default (unlike
    # broker's hard-required env var): this is a new var for ddp-sync, and
    # until ops sets it to match broker's value, falling back to "always
    # public API" is the same behavior as before this change existed --
    # safer than refusing to boot.
    ddp_openstates_jurisdictions: list = field(default_factory=list)

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

    # On-demand single-bill dispatch dev/prod broker routing (SYNC-10).
    # Distinct from ddp_broker_api_base above -- every other caller in this
    # process (SYNC-9's batch pipeline, concept-statement dispatch) targets
    # one process-wide broker via that setting. This endpoint is the one
    # call path that must pick between *two* ddp-broker-py instances per
    # request, depending on which environment the original ddp-next caller
    # belongs to (a trusted signal ddp-api stamps onto the forwarded
    # request -- see API-5 -- not something this endpoint trusts from the
    # caller's own request body). Dev defaults to the same local Mac Studio
    # broker ddp_broker_api_base already defaults to, since that's where the
    # real dev ddp-broker-py instance runs today. Prod has no default --
    # must be set explicitly once a real production ddp-broker-py exists.
    ondemand_broker_api_base_dev: str = "http://localhost:8080"
    ondemand_broker_api_token_dev: str = ""
    ondemand_broker_api_base_prod: str = ""
    ondemand_broker_api_token_prod: str = ""

    # Local api-v3 archived-text lookup (ddp-infra PLAN-bill-document-
    # provenance.md, "Real gap found 2026-07-29/30" — OPEN-13). A small,
    # dedicated read against ddp-open-states' own local api-v3 instance
    # (docker-compose.ddp.yml maps its container to host port 8002 on the
    # Mac Studio, same box ddp-sync's LegBot dispatch already runs on — see
    # cams_base_url above), NOT the same thing as openstates_api_key/the
    # public v3.openstates.org API bill_sync.py calls by default.
    # Originally NOT the site-wide OPENSTATES_API_BASE cutover
    # PLAN-local-openstates-migration.md scopes (Pinecone re-keying,
    # VoteBot, universal ingestion) -- that migration is still out of
    # scope. SYNC-6 (2026-08-11) *does* now reuse this same base/key pair
    # for BillSyncService.fetch_bill_from_openstates(), but only for
    # jurisdictions listed in ddp_openstates_jurisdictions below -- a
    # narrow, jurisdiction-scoped routing decision mirroring
    # ddp-broker-py's _get_client_for_jurisdiction(), not the universal
    # cutover.
    # No default key — api-v3's apikey_auth has no dev bypass; must be set
    # (the local dev stack's seeded Profile key is the well-known
    # 00000000-0000-0000-0000-000000000001 sentinel used elsewhere in this
    # project, e.g. ddp-open-states/quality_check.py, start-os-api.sh).
    local_openstates_api_base: str = "http://localhost:8002"
    local_openstates_api_key: str = ""

    # How long legbot_client.py's _dispatch_and_await polls before giving up on a
    # LegBot task. Generous by design, not a tight deadline -- MLX/local-model
    # compute has no per-call cost the way cloud API tokens do, so there's no
    # cost-control reason to cut a still-running task off early. Raised from a
    # hardcoded 120s (LegBot's own latency_estimate is ~60s, so 120s looked like
    # reasonable headroom on paper) after a real 2026-08-15 dev run found 4 of 8
    # bill-artifact dispatches timing out at exactly 120s under back-to-back
    # sequential load on the same local backend -- a guardrail that cost real
    # data (four artifacts silently failed) for no actual cost savings.
    legbot_dispatch_timeout_seconds: float = 1200.0

    # Cap on how many organizations bill_organization_position_research.py's
    # generate_and_store_bill_organization_positions will verify/write per
    # invocation -- a safety valve against a malformed or hallucinated
    # find_bill_positions response driving an unbounded number of
    # verify_bill_position calls, not a realistic per-bill estimate. Raised
    # from a hardcoded 20 (picked from the one real validation run to date,
    # which found 6 organizations for one bill) after a real report that some
    # bills draw hundreds of real supporting/opposing organizations -- the old
    # cap would have silently discarded the vast majority of a high-profile
    # bill's real positions with only a log warning to show for it.
    org_research_max_organizations: int = 500

    # How many bills run_legbot_pipeline (session_pipeline_runner.py)
    # processes concurrently, instead of one at a time. Bounded, not
    # unbounded -- _process_bill() also fires several non-MLX HTTP calls per
    # bill (a broker coverage check, OpenStates version/archived-text
    # lookups, ensure_bill_exists) that would otherwise all fire at once for
    # however many bills a session has -- potentially thousands -- for no
    # real throughput benefit once only a handful of MLX-LM pool instances
    # (ddp-agents' own LEGBOT_MLX_MAX_INSTANCES) can usefully be busy at the
    # same time. Added after a real live run confirmed the pipeline was
    # strictly sequential: with AGENTS-33/34's demand-based, memory-gated
    # MLX-LM/MLX-VLM instance pool now live in ddp-agents, a caller that only
    # ever has one request in flight can never trigger that pool's scale-up
    # at all, regardless of how much memory or how many bills are queued --
    # defeating the whole point of building it for bulk session backfills.
    #
    # Default is 1 (AGENTS-37, 2026-08-18) -- NOT the 4 this setting first
    # shipped with. The very first real dispatch run under concurrency=4
    # (FL's 2026E session, 20 bills) found that value actively harmful on
    # real hardware: LEGBOT_MLX_MAX_INSTANCES was configured at 3 (one below
    # 4, so the pool was oversubscribed from the first request), and worse,
    # the 3 real MLX-LM instances that did run concurrently thrashed rather
    # than parallelized -- per-bill bill_summary generation went from a
    # 30-50s single-instance sequential baseline to 20-47+ minutes under
    # 3-way concurrent load, a 20-50x slowdown. Confirmed directly against
    # dev ddp-broker-py's own database: zero new BillArtifact rows landed
    # from that run. This matches a risk ddp-agents' own AGENTS-30/33 design
    # docs already flagged as open ("not yet live-validated against real
    # Metal/GPU hardware") -- this was that validation, and the result was
    # bad. Until real concurrent MLX-LM throughput on the target hardware is
    # actually benchmarked and shown safe, defaulting to genuinely sequential
    # behavior (1) is the only value known not to make things worse than the
    # pre-concurrency baseline. Operators may still opt into a higher value
    # via SESSION_PIPELINE_CONCURRENCY once that validation happens -- this
    # is a default change, not a removal of the configurability itself.
    #
    # Does NOT by itself resolve the still-open Phase 8 concern
    # (prioritizing this batch pipeline's own concurrency against Agent
    # Smith's interactive traffic specifically), nor the still-open question
    # of whether legbot_dispatch_timeout_seconds should change given real
    # per-instance latency can apparently exceed 20 minutes under
    # contention -- both remain open, tracked on AGENTS-37.
    session_pipeline_concurrency: int = 1

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
        "openstates_api_base": os.getenv("OPENSTATES_API_BASE", "https://v3.openstates.org"),
        "ddp_openstates_jurisdictions": [
            j.strip().upper()
            for j in os.getenv("DDP_OPENSTATES_JURISDICTIONS", "").split(",")
            if j.strip()
        ],
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
        "ondemand_broker_api_base_dev": os.getenv("ONDEMAND_BROKER_API_BASE_DEV", "http://localhost:8080"),
        "ondemand_broker_api_token_dev": os.getenv("ONDEMAND_BROKER_API_TOKEN_DEV", ""),
        "ondemand_broker_api_base_prod": os.getenv("ONDEMAND_BROKER_API_BASE_PROD", ""),
        "ondemand_broker_api_token_prod": os.getenv("ONDEMAND_BROKER_API_TOKEN_PROD", ""),
        "local_openstates_api_base": os.getenv("LOCAL_OPENSTATES_API_BASE", "http://localhost:8002"),
        "local_openstates_api_key": os.getenv("LOCAL_OPENSTATES_API_KEY", ""),
        "legbot_dispatch_timeout_seconds": float(
            os.getenv("LEGBOT_DISPATCH_TIMEOUT_SECONDS", "1200")
        ),
        "org_research_max_organizations": int(
            os.getenv("LEGBOT_ORG_RESEARCH_MAX_ORGANIZATIONS", "500")
        ),
        "session_pipeline_concurrency": int(os.getenv("SESSION_PIPELINE_CONCURRENCY", "1")),
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
