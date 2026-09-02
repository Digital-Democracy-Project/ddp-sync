"""
Configuration for DDP-Sync.

Priority: AWS Secrets Manager -> .env file -> defaults.
Production uses Secrets Manager. Local dev uses .env.
"""

import json
import math
import os
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Optional


def _positive_float(raw: str | None, *, default: float, name: str) -> float:
    """Parse an operator-supplied float that must be > 0 and finite.

    Falls back to `default` on anything unusable rather than raising: a typo
    in a .env should not stop the service starting, and the log line says
    exactly what was ignored. SYNC-39 -- the value this guards is a poll
    interval, where 0 or a negative would busy-loop against CAMS rather than
    poll briskly.
    """
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        logging.getLogger(__name__).warning(
            "%s=%r is not a number; using %s", name, raw, default)
        return default
    if not math.isfinite(value) or value <= 0:
        logging.getLogger(__name__).warning(
            "%s=%r must be a finite number greater than 0; using %s",
            name, raw, default)
        return default
    return value

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

    # SYNC-51 (found building OPEN-193): scheduler.py's start() registers every job it knows
    # about unconditionally the moment a process starts, with zero coordination against any
    # other host also running this same image -- true when ddp-sync ran on exactly one host,
    # not true the moment a second host (the EC2 instance running ddp-broker/api-v3) runs it
    # too. A single blanket on/off switch for the whole scheduler was tried first and rejected:
    # it can only ever be all-or-nothing per host, and the Mac's own start-ddp-sync.sh already
    # needs the scheduler running for real, so a blanket default-off would have required every
    # existing host's startup script to opt back in correctly -- fragile, and the wrong shape
    # regardless once a host needs SOME jobs but not others (this EC2 instance wants
    # openstates_scrape_enabled but not voatz_sync_enabled/webflow_batch_enabled/etc).
    #
    # Every flag below defaults to True -- preserving current behavior exactly, on every
    # existing host, with zero .env changes required there. A host opts a specific task OUT
    # by setting its flag to false in its own .env; sync_schedule.yaml's own per-job `enabled`
    # keys (checked-in, identical across every deployment) are untouched by this and still
    # apply underneath -- both must be true for tasks that have both.
    bill_sync_enabled: bool = True
    legislator_sync_enabled: bool = True
    legislator_bio_sync_enabled: bool = True
    organization_sync_enabled: bool = True
    voatz_sync_enabled: bool = True
    webflow_batch_enabled: bool = True
    votebot_eval_enabled: bool = True
    api_health_check_enabled: bool = True
    openstates_scrape_enabled: bool = True
    openstates_archive_enabled: bool = True
    mi_cookie_publish_enabled: bool = True

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

    # SYNC-39: how often _dispatch_and_await asks CAMS whether a task has
    # finished. It was a module constant of 5 in legbot_client.py.
    #
    # The dominant cost is latency on every call, not cache loss. Measured on
    # VA 2026S1 (2026-08-27), from CAMS's own mlx_artifact_started ->
    # mlx_artifact_complete timestamps, which are not quantised by this poll:
    #
    #     real generation, warm call   median 1.72s   p90 3.57s   (n=220)
    #     real prefill, cold           median 0.80s   p90 3.11s   (n=88)
    #     client-observed per call     median 5.10s   min 5.10s
    #
    # So roughly 3.4s of every call was this loop sleeping, on 220 calls in an
    # 830s run. That waste is unconditional -- it happens whether or not any
    # cache is ever lost.
    #
    # There is a second, weaker effect. A bill does not learn its own call
    # finished for up to one interval, then spends about another on a broker
    # write and a coverage check before dispatching again, leaving its worker
    # idle while it still needs it; a competing bill polling on the same
    # rhythm claims it, and MLXWorkerSupervisor cannot tell an idle-but-still-
    # needed worker from a free one. That accounted for 13 of 18 cold
    # concept_statements prefills.
    #
    # Do NOT over-claim that effect. Shortening the interval shortens the
    # competitor's cadence too, so the ratio may be preserved and the steal
    # rate may not improve at all. Whether it does is an open question this
    # value exists to let someone answer -- and on a corpus like VA 2026S1 a
    # lost prefill costs under a second anyway. It costs much more on a large
    # bill, where prefill is genuinely expensive, which is where cache
    # retention still matters.
    #
    # 1.0 is chosen to cut the per-call latency, which is the part that is
    # measured and certain. Configurable rather than a constant per this
    # repo's own no-hardcoded-config rule, so it can be tuned and measured
    # without a code change.
    legbot_poll_interval_seconds: float = 1.0

    # AGENTS-42 (2026-08-19): a single 45-call incident (one crashed MLX
    # request wedging LegBot's single-instance MLX pool, ddp-agents'
    # legbot/reasoning.py _MLXInstancePool) burned ~14h of wall-clock time --
    # every task queued behind the stuck request had no way to tell "waiting
    # for a turn" apart from "actively generating" (both read as CAMS task
    # status "running"), so each one burned its full
    # legbot_dispatch_timeout_seconds just sitting in queue with zero chance
    # of ever getting one. legbot_client.py's _dispatch_and_await now splits
    # its poll loop into two phases: this generous ceiling governs from
    # dispatch until ddp-agents' new mlx_generation_started_at marker
    # (cams/api/routes.py TaskResponse) is first observed non-empty --  at
    # that point legbot_dispatch_timeout_seconds takes back over, measured
    # from when generation actually began. Sized to cover a full batch's
    # worth of legitimate queueing (not a tight guardrail, matching this
    # module's existing dispatch-timeout philosophy above) without being
    # unboundable -- a task that never starts generating within this window
    # (whether genuinely still stuck in queue, a non-MLX-routed task that's
    # otherwise hung, or an older CAMS deployed without this field yet) is
    # still given up on and cancelled, not waited on forever.
    legbot_queue_wait_timeout_seconds: float = 3600.0

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

    # SYNC-48: independent enable flag for an automated scraper-completion
    # caller of run_legbot_pipeline (pipelines/scraper_triggered_legbot.py).
    # Deliberately NOT the same switch as LEGBOT_ENABLED (ddp-agents/CAMS
    # side) -- that one is the last-resort "stop everything, including
    # manual Agent Smith dispatches" switch and isn't meant to be toggled
    # routinely. This flag pauses only the automated path; manual dispatches
    # via /trigger/bill-artifact-generation are unaffected either way.
    # Defaults to disabled: no automated caller exists yet (see that
    # module's own docstring for what's still a named follow-up), so there
    # is nothing for an operator to opt into today.
    legbot_scrape_completion_trigger_enabled: bool = False

    # SYNC-48: TTL for the overlap-rejection lock in
    # scraper_triggered_legbot.py, keyed per (jurisdiction, session). A
    # coarse ceiling, not a correctness mechanism -- see that module's own
    # docstring for why a lock that occasionally expires early (letting a
    # second run start) or late (rejecting one extra trigger) is safe by
    # construction, not something this TTL needs to get exactly right.
    # Sized generously: real throughput after SYNC-37/38/39's 2026-08-27
    # measurements was ~25s/bill on a 20-bill session (see PLAN-legbot.md),
    # so a few hundred-bill session is on the order of an hour or two at
    # session_pipeline_concurrency's default of 1 (sequential) -- 4 hours
    # leaves real headroom above that without being unboundable.
    legbot_scrape_completion_trigger_lock_ttl_seconds: int = 14400

    # SYNC-50: what a real scraper-completion trigger dispatches once it has
    # resolved which session(s) a scrape run touched. Deliberately its own
    # setting, not a reuse of config/sync_schedule.yaml's session_pipeline_batch
    # job config -- that job hand-picks one fixed (jurisdiction, session) pair on
    # a schedule; this fires for whichever jurisdiction/session a real scrape
    # just resolved, so its defaults have to be jurisdiction-agnostic. Defaults
    # to every recognized artifact type (session_pipeline_runner.ALL_ARTIFACT_TYPES)
    # -- a real 24/7 pipeline has no principled reason to leave one out by default.
    legbot_scrape_completion_trigger_artifact_types: list = field(default_factory=list)

    # SYNC-50: per-trigger bill limit, same "no real ceiling needed" reasoning
    # SYNC-9's own limit field already documents (run_legbot_pipeline dispatches
    # sequentially, and MLX concurrency protection already lives one layer down)
    # -- sized generously above any single tracked jurisdiction's real session
    # size (Virginia's own 2026 regular session: 3,637 bills).
    legbot_scrape_completion_trigger_limit: int = 10000

    # SYNC-50: concept_statements is already part of the standard automated flow
    # elsewhere (session_pipeline_batch's own include_concept_statements) --
    # true by default here for the same reason. include_org_research has no
    # equivalent setting: ddp-infra PLAN-legbot.md §32 Gate 1 item 4 (2026-09-01)
    # decided that explicitly out of scope for automated dispatch, not a knob to
    # reintroduce here -- see _maybe_trigger_legbot_for_scrape's own call site.
    legbot_scrape_completion_trigger_include_concept_statements: bool = True

    # SYNC-50: safety bound on how many bills resolve_touched_sessions() will
    # scan (paginating the local api-v3 instance) before giving up on finding
    # every touched session for one scrape run. Not a limit on what LegBot
    # dispatches -- purely how far this one resolution step looks before
    # settling for whatever sessions it has already found, so a jurisdiction
    # with an unexpectedly enormous single-run diff can't turn session
    # resolution itself into an unbounded scan.
    legbot_scrape_completion_trigger_resolution_max_bills: int = 500

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
        "bill_sync_enabled": os.getenv("BILL_SYNC_ENABLED", "true").lower() == "true",
        "legislator_sync_enabled": os.getenv("LEGISLATOR_SYNC_ENABLED", "true").lower() == "true",
        "legislator_bio_sync_enabled": os.getenv("LEGISLATOR_BIO_SYNC_ENABLED", "true").lower() == "true",
        "organization_sync_enabled": os.getenv("ORGANIZATION_SYNC_ENABLED", "true").lower() == "true",
        "voatz_sync_enabled": os.getenv("VOATZ_SYNC_ENABLED", "true").lower() == "true",
        "webflow_batch_enabled": os.getenv("WEBFLOW_BATCH_ENABLED", "true").lower() == "true",
        "votebot_eval_enabled": os.getenv("VOTEBOT_EVAL_ENABLED", "true").lower() == "true",
        "api_health_check_enabled": os.getenv("API_HEALTH_CHECK_ENABLED", "true").lower() == "true",
        "openstates_scrape_enabled": os.getenv("OPENSTATES_SCRAPE_ENABLED", "true").lower() == "true",
        "openstates_archive_enabled": os.getenv("OPENSTATES_ARCHIVE_ENABLED", "true").lower() == "true",
        "mi_cookie_publish_enabled": os.getenv("MI_COOKIE_PUBLISH_ENABLED", "true").lower() == "true",
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
        "legbot_queue_wait_timeout_seconds": float(
            os.getenv("LEGBOT_QUEUE_WAIT_TIMEOUT_SECONDS", "3600")
        ),
        # Guarded because this is operator-editable and 0 or a negative would
        # turn the poll loop into a hot loop against CAMS rather than merely
        # polling briskly. Falls back to the default rather than raising: a
        # typo in a .env should not stop the pipeline starting.
        "legbot_poll_interval_seconds": _positive_float(
            os.getenv("LEGBOT_POLL_INTERVAL_SECONDS"), default=1.0,
            name="LEGBOT_POLL_INTERVAL_SECONDS",
        ),
        "org_research_max_organizations": int(
            os.getenv("LEGBOT_ORG_RESEARCH_MAX_ORGANIZATIONS", "500")
        ),
        "session_pipeline_concurrency": int(os.getenv("SESSION_PIPELINE_CONCURRENCY", "1")),
        "legbot_scrape_completion_trigger_enabled": (
            os.getenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED", "false").lower() == "true"
        ),
        "legbot_scrape_completion_trigger_lock_ttl_seconds": int(
            os.getenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_LOCK_TTL_SECONDS", "14400")
        ),
        # SYNC-50. Default mirrors session_pipeline_runner.ALL_ARTIFACT_TYPES exactly --
        # not imported from there to avoid a pipelines-importing-into-config cycle; that
        # module's own assertion (ARTIFACT_DISPATCH_ORDER == ALL_ARTIFACT_TYPES) is what
        # keeps this literal from silently drifting unnoticed if a type is ever added.
        "legbot_scrape_completion_trigger_artifact_types": [
            t.strip()
            for t in os.getenv(
                "LEGBOT_SCRAPE_COMPLETION_TRIGGER_ARTIFACT_TYPES",
                "bill_summary,bill_pros_cons,bill_vote_yes_frame,bill_vote_no_frame,"
                "bill_supporting_orgs,bill_opposing_orgs,bill_impact_analysis,"
                "bill_topics,bill_changelog",
            ).split(",")
            if t.strip()
        ],
        "legbot_scrape_completion_trigger_limit": int(
            os.getenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_LIMIT", "10000")
        ),
        "legbot_scrape_completion_trigger_include_concept_statements": (
            os.getenv(
                "LEGBOT_SCRAPE_COMPLETION_TRIGGER_INCLUDE_CONCEPT_STATEMENTS", "true"
            ).lower()
            == "true"
        ),
        "legbot_scrape_completion_trigger_resolution_max_bills": int(
            os.getenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_RESOLUTION_MAX_BILLS", "500")
        ),
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
