"""On-demand trigger endpoints for scheduled jobs."""

import asyncio
import logging
from dataclasses import asdict
from datetime import date

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field

from ddp_sync.api.auth import api_key_auth
from ddp_sync.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/trigger/user-sync")
async def trigger_user_sync(token: str = Depends(api_key_auth)):
    """Trigger incremental Voatz -> Brevo user sync."""
    try:
        from ddp_sync.pipelines.voatz_brevo import run_sync_job
        await asyncio.get_event_loop().run_in_executor(None, run_sync_job)
        return {"status": "completed", "job": "user_sync"}
    except Exception as e:
        logger.error(f"User sync trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/full-sync")
async def trigger_full_sync(token: str = Depends(api_key_auth)):
    """Trigger full-attribute Voatz -> Brevo sync."""
    try:
        from ddp_sync.pipelines.voatz_brevo import run_full_sync_job
        await asyncio.get_event_loop().run_in_executor(None, run_full_sync_job)
        return {"status": "completed", "job": "full_sync"}
    except Exception as e:
        logger.error(f"Full sync trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/bill-version-check")
async def trigger_bill_version_check(token: str = Depends(api_key_auth)):
    """Trigger the daily bill version check (status updates to Webflow CMS)."""
    try:
        from ddp_sync.scheduler import get_scheduler
        scheduler = get_scheduler()
        if not scheduler:
            raise HTTPException(status_code=503, detail="Scheduler not initialized")
        result = await scheduler.trigger_openstates_sync(force_all=False)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Bill version check trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/bill-status-sync")
async def trigger_bill_status_sync(
    all_sessions: bool = False,
    jurisdiction: str | None = None,
    token: str = Depends(api_key_auth),
):
    """Sync OpenStates → Webflow CMS status fields only (no Pinecone).

    Lightweight alternative to bill-version-check that only updates
    status, status-date, status-chamber, and gov-url in Webflow CMS.

    Query params:
        all_sessions: Bypass session filters for backfill (default false)
        jurisdiction: Filter to a single state code (e.g. FL)
    """
    try:
        from ddp_sync.scheduler import get_scheduler
        scheduler = get_scheduler()
        if not scheduler:
            raise HTTPException(status_code=503, detail="Scheduler not initialized")
        result = await scheduler.trigger_bill_status_sync(
            all_sessions=all_sessions,
            jurisdiction=jurisdiction,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Bill status sync trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class BillArtifactGenerationRequest(BaseModel):
    """Request body for POST /trigger/bill-artifact-generation.

    No field has a default -- every cost-relevant parameter must be an
    explicit, reviewed choice per call, mirroring run_legbot_pipeline's own
    "no silent defaults" discipline (pipelines/session_pipeline_runner.py).
    """

    jurisdiction_iso2: str = Field(..., description="Two-letter state code, e.g. 'FL'.")
    session_code: str = Field(..., description="Legislative session identifier, e.g. '2026F'.")
    artifact_types: list[str] = Field(
        ...,
        description=(
            "BillArtifact types to fill in for this run (from the types "
            "session_pipeline_runner.ALL_ARTIFACT_TYPES supports -- "
            "bill_summary, bill_pros_cons, bill_changelog, etc.). Start "
            "with a small subset, not all of them at once."
        ),
    )
    include_org_research: bool = Field(
        ...,
        description=(
            "Whether to also dispatch Organization Position Research for "
            "bills not yet researched. THE ONLY PARAMETER HERE THAT COSTS "
            "MONEY: this is the one dispatch that leaves the local MLX model "
            "for a metered cloud API with server-side web search. Measured "
            "2026-08-28 on FL 2026E: 17 calls, $7.26 -- about $0.43 per bill "
            "researched, against $0 for all nine artifact_types combined. "
            "Scales per bill, so a 200-bill session is on the order of $85. "
            "Already-researched bills are skipped and cost nothing, which is "
            "why the figure is per bill researched rather than per bill "
            "considered."
        ),
    )
    include_concept_statements: bool = Field(
        ...,
        description=(
            "Whether to also generate ConceptStatementSet rows (LegBot's "
            "concept_statements question type) for bills without an "
            "existing published set (SYNC-31)."
        ),
    )
    limit: int = Field(
        ...,
        description=(
            "Max bills to consider this run (must be >= 1). No upper ceiling: "
            "run_legbot_pipeline dispatches sequentially anyway, and real "
            "concurrent-load protection for MLX already exists one layer "
            "down -- CAMS's own _mlx_semaphore (ddp-agents/src/legbot/"
            "reasoning.py, shipped 2026-08-04) serializes every MLX call it "
            "receives from any caller, this batch included, so simultaneous "
            "requests queue safely instead of contending for the GPU. This "
            "endpoint is still synchronous, though (see the route's own "
            "docstring): a very large limit means a very long-held HTTP "
            "request, which is a timeout consideration for the caller, not "
            "a cost or concurrency one."
        ),
    )
    retry_failed: bool = Field(
        ...,
        description=(
            "Whether to re-dispatch artifacts whose stored status is "
            "'failed' (SYNC-42). Without this, a failed artifact is skipped "
            "by every later run forever -- so a failure caused by a bug, an "
            "outage or a prompt change since fixed can only be cleared by "
            "deleting rows in the broker by hand. Reaches 'failed' and "
            "nothing else: a 'complete' artifact is never re-dispatched at "
            "any value of this flag, and neither are 'pending'/'processing'. "
            "Retrying costs real inference and rewrites real rows, so like "
            "every other cost-relevant field here it has no default. Pair it "
            "with dry_run=true first -- the preview honours it and lists "
            "exactly what a real retry would dispatch."
        ),
    )
    dry_run: bool = Field(False, description="Preview scope without dispatching anything.")


def _resolve_batch_broker_target(environment: str | None) -> tuple[str | None, str | None]:
    """Resolve /trigger/bill-artifact-generation's own broker target from an
    optional X-DDP-Environment header -- distinct from
    _resolve_ondemand_broker_target below (used by the on-demand single-bill
    endpoints), which requires the header and is meant only for calls
    forwarded through ddp-api's own trusted proxy. This endpoint predates
    that header entirely and has real existing callers (the
    session_pipeline_batch scheduled job, direct operator calls) that never
    set it -- a missing/omitted header must keep this endpoint's original
    behavior (None, None), letting run_legbot_pipeline fall through to
    whatever DDP_BROKER_API_BASE/DDP_BROKER_API_TOKEN are globally
    configured, rather than erroring the way the on-demand endpoints do.
    """
    if environment is None:
        return None, None
    if environment not in ("dev", "prod"):
        raise HTTPException(
            status_code=400,
            detail="X-DDP-Environment, if set, must be 'dev' or 'prod'.",
        )
    settings = get_settings()
    if environment == "dev":
        return settings.ondemand_broker_api_base_dev, settings.ondemand_broker_api_token_dev
    if not settings.ondemand_broker_api_base_prod:
        raise HTTPException(
            status_code=503,
            detail="ONDEMAND_BROKER_API_BASE_PROD is not configured -- production "
            "ddp-broker-py routing isn't set up on this instance yet.",
        )
    return settings.ondemand_broker_api_base_prod, settings.ondemand_broker_api_token_prod


@router.post("/trigger/bill-artifact-generation")
async def trigger_bill_artifact_generation(
    body: BillArtifactGenerationRequest,
    x_ddp_environment: str | None = Header(default=None),
    token: str = Depends(api_key_auth),
):
    """Fill in missing BillArtifact rows for every bill in one jurisdiction/session.

    run_legbot_pipeline's (pipelines/session_pipeline_runner.py) first real
    production caller (SYNC-9) -- ddp-infra's PLAN-bill-document-
    provenance.md "Step 1, scoped version". Synchronous, like
    /trigger/bill-version-check: returns the full result payload so an
    operator can review exactly what was generated/skipped/failed before
    running a broader batch.

    `limit` has no upper ceiling (removed 2026-08-15 -- the previous hard
    cap of 25 was meant to protect against concurrent load on a shared
    CAMS/LegBot/MLX backend, but that protection already exists one layer
    down (CAMS's own `_mlx_semaphore`, `ddp-agents/src/legbot/reasoning.py`,
    shipped 2026-08-04, serializes every MLX call it receives regardless of
    caller) and this pipeline dispatches sequentially anyway; MLX inference
    also has no per-call cost, unlike a metered cloud API. See `limit`'s own
    field description above for what remains a real consideration -- this
    endpoint's synchronous request/response shape means a very large
    `limit` is a client/proxy timeout question, not a cost or concurrency
    one.

    x_ddp_environment: optional 'dev'/'prod' X-DDP-Environment header --
    mirrors the on-demand single-bill endpoints' own environment switch
    (_resolve_ondemand_broker_target below), so a caller can point one run
    at dev ddp-broker-py without touching this instance's global
    DDP_BROKER_API_BASE/DDP_BROKER_API_TOKEN, which every OTHER broker-
    writing path in this service also reads (including real scheduled
    production jobs) -- added after a live incident where testing a full
    session run required a manual, global .env swap to avoid writing to
    production, with no per-request way to ask for dev instead. Omitted
    (the default) preserves this endpoint's original behavior unchanged.
    """
    if body.limit < 1:
        raise HTTPException(
            status_code=400,
            detail=f"limit must be >= 1, got {body.limit}.",
        )

    broker_api_base, broker_api_token = _resolve_batch_broker_target(x_ddp_environment)

    from ddp_sync.pipelines.session_pipeline_runner import run_legbot_pipeline

    try:
        return await run_legbot_pipeline(
            body.jurisdiction_iso2,
            body.session_code,
            body.artifact_types,
            body.include_org_research,
            body.limit,
            include_concept_statements=body.include_concept_statements,
            retry_failed=body.retry_failed,
            dry_run=body.dry_run,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Bill artifact generation trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Which of the two configured ddp-broker-py instances (dev vs. prod) an
# on-demand single-bill dispatch writes its BillArtifact to (SYNC-10) --
# keyed by the trusted X-DDP-Environment header ddp-api's /trigger/* proxy
# stamps onto the forwarded request based on which API key made the call
# (API-5), never a value trusted from the caller's own request body.
def _resolve_ondemand_broker_target(environment: str | None) -> tuple[str, str]:
    if environment not in ("dev", "prod"):
        raise HTTPException(
            status_code=400,
            detail=(
                "Missing or invalid X-DDP-Environment header (must be 'dev' or "
                "'prod') -- this endpoint only accepts calls forwarded through "
                "ddp-api's proxy, which stamps this header from the calling "
                "API key's own environment tag (see API-5)."
            ),
        )
    settings = get_settings()
    if environment == "dev":
        return settings.ondemand_broker_api_base_dev, settings.ondemand_broker_api_token_dev
    if not settings.ondemand_broker_api_base_prod:
        raise HTTPException(
            status_code=503,
            detail="ONDEMAND_BROKER_API_BASE_PROD is not configured -- production "
            "ddp-broker-py routing isn't set up on this instance yet.",
        )
    return settings.ondemand_broker_api_base_prod, settings.ondemand_broker_api_token_prod


class LegBotAnalyzeBillRequest(BaseModel):
    """Request body for POST /trigger/legbot-analyze-bill.

    No field has a default -- same "every consequential param is a
    conscious choice" discipline as BillArtifactGenerationRequest above.
    bill_source is caller-supplied here (unlike the batch pipeline, where it
    always comes from trusted internal candidate-listing code) -- ddp-sync
    itself never fetches it; it's passed straight through to CAMS/LegBot's
    task API as a plain string, the same as every other dispatch in this
    codebase. Any URL-fetch safety for it is CAMS/LegBot's own ingest-path
    responsibility (already shared across every analyze_bill caller), not
    something this endpoint duplicates.
    """

    bill_openstates_id: str = Field(..., description="OpenStates bill ID.")
    jurisdiction: str = Field(..., description="Two-letter state code, e.g. 'FL'.")
    session_code: str = Field(..., description="Legislative session identifier, e.g. '2026F'.")
    bill_source: str = Field(..., description="URL to the bill's PDF/HTML, or its raw text.")
    artifact_type: str = Field(
        ...,
        description=(
            "One of the BillArtifact types "
            "(session_pipeline_runner.ALL_ARTIFACT_TYPES) -- bill_summary, "
            "bill_pros_cons, bill_changelog, etc."
        ),
    )


@router.post("/trigger/legbot-analyze-bill", status_code=202)
async def trigger_legbot_analyze_bill(
    body: LegBotAnalyzeBillRequest,
    background_tasks: BackgroundTasks,
    x_ddp_environment: str | None = Header(default=None),
    token: str = Depends(api_key_auth),
):
    """Dispatch an on-demand single-bill LegBot analysis (SYNC-10).

    ddp-next's interactive "explain this bill"/"pros and cons" UX --
    distinct from /trigger/bill-artifact-generation above, which fills in a
    whole jurisdiction/session batch. Reuses the exact same dispatch ->
    ddp-broker-py write path as that batch pipeline
    (bill_artifact_generation.py), just for one bill.

    Mac-Studio-only by construction: CAMS/LegBot dispatch is a same-box
    call (legbot_client.py reads CAMS's result off the local filesystem,
    with no network equivalent), so this endpoint only ever works correctly
    when ddp-sync itself is running on the same host as CAMS. Host-guards
    on CAMS_BASE_URL/CAMS_ARTIFACTS_DIR being configured (503, not a
    confusing stack trace) rather than assuming the caller reached the
    right instance -- see ddp-api's own routing-fix ticket (the
    /trigger/legbot-analyze-bill path needs a scoped override to reach this
    instance specifically; not yet built, tracked separately).

    Async, pending-row + background-task shape: writes an initial `pending`
    BillArtifact row via dispatch_and_record_bill_artifact, then dispatches
    to LegBot in the background and returns 202 immediately -- ddp-next
    polls ddp-broker-py's BillArtifact row (via ddp-api's existing /broker
    proxy) until status is no longer pending, rather than blocking this
    request on LegBot's own response time.
    """
    settings = get_settings()
    if not settings.cams_base_url or not settings.cams_artifacts_dir:
        raise HTTPException(
            status_code=503,
            detail=(
                "CAMS_BASE_URL/CAMS_ARTIFACTS_DIR not configured on this "
                "instance -- this endpoint only works on the same host as "
                "CAMS (Mac Studio), not a co-located ddp-sync instance "
                "elsewhere."
            ),
        )

    from ddp_sync.pipelines.session_pipeline_runner import ALL_ARTIFACT_TYPES

    if body.artifact_type not in ALL_ARTIFACT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unrecognized artifact_type: {body.artifact_type!r}. "
            f"Must be one of {sorted(ALL_ARTIFACT_TYPES)}.",
        )

    broker_api_base, broker_api_token = _resolve_ondemand_broker_target(x_ddp_environment)

    from ddp_sync.services.local_openstates_client import get_current_version_identity

    version = await get_current_version_identity(body.bill_openstates_id)
    if version is None:
        raise HTTPException(
            status_code=404,
            detail=f"No archived version found for bill_openstates_id={body.bill_openstates_id!r}.",
        )

    from ddp_sync.pipelines.bill_artifact_generation import dispatch_and_record_bill_artifact

    background_tasks.add_task(
        dispatch_and_record_bill_artifact,
        bill_openstates_id=body.bill_openstates_id,
        jurisdiction=body.jurisdiction,
        session_code=body.session_code,
        version_date=version["version_date"],
        version_note=version["version_note"],
        bill_source=body.bill_source,
        artifact_type=body.artifact_type,
        broker_api_base=broker_api_base,
        broker_api_token=broker_api_token,
    )

    return {
        "status": "pending",
        "bill_openstates_id": body.bill_openstates_id,
        "artifact_type": body.artifact_type,
        "environment": x_ddp_environment,
    }


class LegBotAnalyzeBillFullRequest(BaseModel):
    """Request body for POST /trigger/legbot-analyze-bill-full (SYNC-15).

    The single-parameter "run everything for this bill" counterpart to
    LegBotAnalyzeBillRequest above -- one call for all 8 artifact types plus
    org position research, instead of 8+ separate calls. gov_id has no
    default (required, unlike artifact_types) -- see
    run_single_bill_full's own docstring for why it can't be derived or
    skipped. include_org_research and include_concept_statements (SYNC-31)
    still have no default, matching this codebase's "every cost-relevant
    parameter is a conscious choice" discipline used everywhere else in
    this file -- only artifact_types gets a real default (all of them),
    since that's this endpoint's whole reason to exist.
    """

    bill_openstates_id: str = Field(..., description="OpenStates bill ID.")
    jurisdiction: str = Field(..., description="Two-letter state code, e.g. 'FL'.")
    session_code: str = Field(..., description="Legislative session identifier, e.g. '2026F'.")
    gov_id: str = Field(..., description="Bill's gov_id, e.g. 'SB2B' -- required for the broker coverage check.")
    bill_source: str = Field(..., description="URL to the bill's PDF/HTML, or its raw text.")
    artifact_types: list[str] | None = Field(
        None,
        description=(
            "BillArtifact types to run for this bill. Omit or pass null "
            "for all of ALL_ARTIFACT_TYPES -- this endpoint's whole "
            "point is not having to enumerate them."
        ),
    )
    include_org_research: bool = Field(
        ...,
        description=(
            "Whether to also dispatch Organization Position Research for this "
            "bill. THE ONLY PARAMETER HERE THAT COSTS MONEY -- roughly $0.43 "
            "per bill against $0 for every artifact type, because it is the "
            "one dispatch that uses a metered cloud API with server-side web "
            "search rather than the local MLX model. See "
            "BillArtifactGenerationRequest.include_org_research for the "
            "measurement this came from."
        ),
    )
    include_concept_statements: bool = Field(
        ...,
        description=(
            "Whether to also generate a ConceptStatementSet for this bill, "
            "if none is already published (SYNC-31)."
        ),
    )
    retry_failed: bool = Field(
        ...,
        description=(
            "Whether to re-dispatch artifacts whose stored status is "
            "'failed' (SYNC-42). Without this, a failed artifact is skipped "
            "by every later run forever -- so a failure caused by a bug, an "
            "outage or a prompt change since fixed can only be cleared by "
            "deleting rows in the broker by hand. Reaches 'failed' and "
            "nothing else: a 'complete' artifact is never re-dispatched at "
            "any value of this flag, and neither are 'pending'/'processing'. "
            "Retrying costs real inference and rewrites real rows, so like "
            "every other cost-relevant field here it has no default. Pair it "
            "with dry_run=true first -- the preview honours it and lists "
            "exactly what a real retry would dispatch."
        ),
    )
    dry_run: bool = Field(False, description="Preview scope without dispatching anything.")


@router.post("/trigger/legbot-analyze-bill-full")
async def trigger_legbot_analyze_bill_full(
    body: LegBotAnalyzeBillFullRequest,
    x_ddp_environment: str | None = Header(default=None),
    token: str = Depends(api_key_auth),
):
    """Run every requested artifact type (default: all 8) plus optional org
    research for one bill, in a single call (SYNC-15).

    Synchronous, like /trigger/bill-artifact-generation -- returns the full
    per-bill result payload rather than the dispatch-and-poll shape
    /trigger/legbot-analyze-bill uses. That endpoint's async/202 design
    exists because it's ddp-next's public-facing interactive UX with a real
    user waiting; this one is operator/backfill-facing (at most 10 real
    dispatches -- 9 artifact types + org research -- for one bill), which
    comfortably fits the same synchronous convention the batch endpoint
    already established for potentially many more dispatches than that.

    Same Mac-Studio-only construction as /trigger/legbot-analyze-bill --
    see that endpoint's own docstring for why (CAMS/LegBot dispatch reads
    results off local disk, no network equivalent).
    """
    settings = get_settings()
    if not settings.cams_base_url or not settings.cams_artifacts_dir:
        raise HTTPException(
            status_code=503,
            detail=(
                "CAMS_BASE_URL/CAMS_ARTIFACTS_DIR not configured on this "
                "instance -- this endpoint only works on the same host as "
                "CAMS (Mac Studio), not a co-located ddp-sync instance "
                "elsewhere."
            ),
        )

    broker_api_base, broker_api_token = _resolve_ondemand_broker_target(x_ddp_environment)

    from ddp_sync.pipelines.session_pipeline_runner import run_single_bill_full

    try:
        result = await run_single_bill_full(
            bill_openstates_id=body.bill_openstates_id,
            jurisdiction_iso2=body.jurisdiction,
            session_code=body.session_code,
            gov_id=body.gov_id,
            bill_source=body.bill_source,
            artifact_types=body.artifact_types,
            include_org_research=body.include_org_research,
            include_concept_statements=body.include_concept_statements,
            retry_failed=body.retry_failed,
            dry_run=body.dry_run,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    result["environment"] = x_ddp_environment
    return result


@router.post("/trigger/legislator-bio-sync")
async def trigger_legislator_bio_sync(
    request: Request,
    dry_run: bool = False,
    auto_create: bool | None = None,
    jurisdiction: str | None = None,
    target: str = "all",
    limit: int = 0,
    historical_since: str | None = None,
    audit_only: str | None = None,
    strict_schema: bool = False,
    upload_photos: bool = False,
    upload_photos_dry_run: bool = False,
    token: str = Depends(api_key_auth),
):
    """Trigger the legislator bio + contact sync.

    See plans/PLAN-legislator-bio-sync.md for the full design. Phase 1
    federal-only; state path is a clear stub.

    Query params:
        dry_run:          Preview the diff without writing.
        auto_create:      Create drafts for upstream-only members. Defaults
                          to the per-jurisdiction config (currently false).
        jurisdiction:     Filter to one state code ("FL", "WA", ..., "us"
                          for federal). Default: all configured.
        target:           "all" / "webflow" / "pinecone".
        limit:            Cap items processed. 0 = unlimited.
        historical_since: Federal historical backfill cutoff (YYYY-MM-DD).
                          Default: 2023-01-01.
        audit_only:       Skip the sync and return just an audit report.
                          Values: "A" (federal join-key coverage),
                          "B" (bulk-import readiness — every record has
                          openstatesid + no duplicates), "C" (pre-existing
                          state CMS records lacking openstatesid).
        strict_schema:    Phase-3 validation flag. When True, any payload
                          field that the schema cache would silently drop
                          (slug missing from the live CMS collection)
                          becomes a per-record error. Default False;
                          flip True for the first deploy after adding a
                          new write target so missing slugs surface
                          instead of silently no-op'ing.
        upload_photos:    Phase-3 photo-upload pipeline. When True, the
                          orchestrator fetches the source image from
                          photo-source-url and uploads to Webflow's
                          asset library, populating the legislator-image
                          (Image type) field. Default False; flip True
                          after editor verification of a sample. Skipped
                          when CMS already has a legislator-image set
                          (cardinal rule). Per-record upload failures
                          are logged + isolated, don't abort the run.
        upload_photos_dry_run: Phase-3 connectivity smoke. When True
                          (with upload_photos=True), the source image
                          is fetched + size-validated + hashed but the
                          Webflow asset-creation step is skipped.
                          Lets operators smoke-test source-CDN
                          reachability without consuming Webflow's
                          asset rate limit or storage. No-op when
                          upload_photos is False.

    Returns: BioSyncReport JSON.

    503 if the congress-legislators source isn't yet warmed at app
    startup — retry in ~60s. Set Retry-After header.
    """
    # ALB-timeout safety gate (round-7 fix). The startup pre-warm task
    # in app.py::lifespan usually finishes before any trigger arrives,
    # but a request can race the pre-warm on a freshly-scaled-up
    # container. Returning 503 with Retry-After is honest about the
    # actual state and avoids a silent 30s ALB idle timeout on the cold
    # path.
    source = getattr(request.app.state, "congress_legislators", None)
    if source is None or not source._warmed:
        logger.warning(
            "legislator-bio-sync trigger arrived before pre-warm complete; "
            "returning 503 Retry-After=60"
        )
        raise HTTPException(
            status_code=503,
            detail=(
                "Bio-sync source still warming up; retry in ~60s. "
                "(8.6 MB historical YAML parse runs once at app startup.)"
            ),
            headers={"Retry-After": "60"},
        )

    # Validate target
    if target not in ("all", "webflow", "pinecone"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid target '{target}'. Must be all/webflow/pinecone.",
        )

    # Audit-only short-circuits the sync (step 6: Audits A and C; Audit B
    # added before scheduler enable).
    if audit_only is not None:
        audit_code = audit_only.upper()
        if audit_code not in ("A", "B", "C"):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid audit_only='{audit_only}'. Use 'A' (federal "
                    "join-key), 'B' (bulk-import readiness — no missing or "
                    "duplicate openstatesid), or 'C' (pre-existing state "
                    "lacking openstatesid)."
                ),
            )
        from ddp_sync.pipelines.legislator_bio import LegislatorBioPipeline
        try:
            pipeline = LegislatorBioPipeline(congress=source)
            if audit_code == "A":
                report = await pipeline.audit_federal_join_keys()
            elif audit_code == "B":
                report = await pipeline.audit_bulk_import_readiness()
            else:  # "C"
                report = await pipeline.audit_state_join_keys(
                    jurisdiction=jurisdiction,
                )
            return asdict(report)
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("legislator-bio-sync audit failed")
            raise HTTPException(status_code=500, detail=str(e))

    # Parse historical_since
    try:
        if historical_since:
            since = date.fromisoformat(historical_since)
        else:
            since = date(2023, 1, 1)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid historical_since '{historical_since}': {e}",
        )

    # Build options + run
    from ddp_sync.pipelines.legislator_bio import (
        BioSyncOptions,
        LegislatorBioPipeline,
    )

    # auto_create defaults: currently always false until per-jurisdiction
    # config wiring lands (step 7). Editors can flip explicitly via the
    # query param.
    effective_auto_create = bool(auto_create) if auto_create is not None else False

    options = BioSyncOptions(
        target=target,  # type: ignore[arg-type]
        jurisdiction=jurisdiction,
        auto_create=effective_auto_create,
        dry_run=dry_run,
        limit=limit,
        historical_since=since,
        strict_schema=strict_schema,
        upload_photos=upload_photos,
        upload_photos_dry_run=upload_photos_dry_run,
    )

    try:
        # Reuse the pre-warmed source from app.state so we don't pay
        # the parse cost again.
        pipeline = LegislatorBioPipeline(congress=source)
        report = await pipeline.run(options)
        return asdict(report)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("legislator-bio-sync trigger failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger/votebot-eval")
async def trigger_votebot_eval(
    days: int = 7,
    token: str = Depends(api_key_auth),
):
    """Trigger an on-demand votebot eval run.

    See plans/PLAN-eval-and-cache-hit-logging.md §3.4 for the full design.

    Query params:
        days: Window passed to evaluate_production.py --days. Bounded by
              the YAML config's ``max_days`` (default 30). Out-of-range
              returns 400.

    Status codes:
        200 — run completed successfully (returns headline + regressions).
        400 — days outside [1, max_days].
        409 — another run is currently in flight (returns current_run_id
              + lock TTL).
        500 — unexpected error.
        503 — votebot path is invalid (Phase 1 not deployed, EC2 path
              moved, etc.).
    """
    from ddp_sync.pipelines.votebot_eval import (
        run_votebot_eval,
        DEFAULT_MAX_DAYS,
    )
    from ddp_sync.scheduler import get_scheduler

    scheduler = get_scheduler()
    yaml_config = (
        scheduler._sync_config.get("votebot_eval") if scheduler else None
    )
    max_days = (yaml_config or {}).get("max_days", DEFAULT_MAX_DAYS)
    if not isinstance(days, int) or days < 1 or days > max_days:
        raise HTTPException(
            status_code=400,
            detail=f"days must be in [1, {max_days}], got {days}",
        )

    try:
        result = await run_votebot_eval(
            days=days,
            yaml_config=yaml_config,
            trigger="manual",
        )
    except Exception as e:
        logger.exception("votebot-eval trigger failed unexpectedly")
        raise HTTPException(status_code=500, detail=str(e))

    if result.get("success"):
        return result

    err = result.get("error")
    if err == "already_running":
        raise HTTPException(
            status_code=409,
            detail={
                "error": "already_running",
                "current_run_id": result.get("current_run_id"),
            },
        )
    if err == "votebot_path_invalid":
        raise HTTPException(
            status_code=503,
            detail={
                "error": "votebot_path_invalid",
                "message": result.get("detail"),
            },
        )
    # Other failures (timeout, subprocess_nonzero, redis_unavailable, parse_error)
    raise HTTPException(status_code=500, detail=result)


@router.post("/trigger/webflow/{job_name}")
async def trigger_webflow_job(job_name: str, token: str = Depends(api_key_auth)):
    """Trigger a specific Webflow CMS batch job."""
    from ddp_sync.pipelines import webflow_batch

    job_map = {
        "fill-session-code": webflow_batch.run_webflow_fill_session_code,
        "fill-map-url": webflow_batch.run_webflow_fill_map_url,
        "bill-org-sync": webflow_batch.run_webflow_bill_org_sync,
        "org-about-parse": webflow_batch.run_webflow_org_about_parse,
        "check-org-missing": webflow_batch.run_webflow_check_org_missing,
        "find-duplicates": webflow_batch.run_webflow_find_duplicates,
        "merge-duplicate-orgs": webflow_batch.run_webflow_merge_duplicate_orgs,
    }

    func = job_map.get(job_name)
    if not func:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown job: {job_name}. Available: {', '.join(job_map.keys())}"
        )

    try:
        await asyncio.get_event_loop().run_in_executor(None, func)
        return {"status": "completed", "job": f"webflow_{job_name}"}
    except Exception as e:
        logger.error(f"Webflow {job_name} trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# OpenStates scrape triggers
# ---------------------------------------------------------------------------

# Targets that map to a named job function.
_OPENSTATES_JOB_TARGETS = {"patches", "fl", "wa", "usa", "secondary", "people"}

# Individual secondary-state codes accepted as single-jurisdiction triggers.
_OPENSTATES_SINGLE_JURISDICTION = {"va", "mi", "ma", "ut", "az"}


@router.post("/trigger/openstates-scrape/{target}")
async def trigger_openstates_scrape(
    target: str,
    background_tasks: BackgroundTasks,
    token: str = Depends(api_key_auth),
):
    """Trigger an OpenStates scrape job immediately, without waiting for its cron.

    Returns 202 Accepted immediately; the job runs in the background and logs
    to ~/Developer/repos/ddp-open-states/logs/scraper.log plus ddp-sync's
    structured log. Flow status is written to Redis under ddp:flow:openstates_*.

    Targets:
        patches     — run apply-local-patches.sh (idempotent, ~30s)
        fl          — all FL sessions sequentially (2026, 2026D, 2026E, 2026F)
        wa          — WA scrape + import
        usa         — USA lower then upper sequentially
        secondary   — VA, MI, MA, UT, AZ concurrently
        people      — git pull people repo + os-people to-database for all states
        va|mi|ma|ut|az — single secondary-state scrape + import
    """
    from ddp_sync.pipelines.openstates_scrape import (
        run_patch_refresh_job,
        run_fl_scrapes_job,
        run_wa_scrape_job,
        run_usa_scrapes_job,
        run_secondary_scrapes_job,
        run_people_refresh_job,
        run_single_scrape_job,
    )
    from ddp_sync.scheduler import get_scheduler

    scheduler = get_scheduler()
    config = scheduler._sync_config.get("openstates_scrape", {}) if scheduler else {}

    if target in _OPENSTATES_JOB_TARGETS:
        job_map = {
            "patches": run_patch_refresh_job,
            "fl": run_fl_scrapes_job,
            "wa": run_wa_scrape_job,
            "usa": run_usa_scrapes_job,
            "secondary": run_secondary_scrapes_job,
            "people": run_people_refresh_job,
        }
        background_tasks.add_task(job_map[target], config)
        return {"status": "started", "target": target}

    if target in _OPENSTATES_SINGLE_JURISDICTION:
        background_tasks.add_task(run_single_scrape_job, target, config)
        return {"status": "started", "target": target}

    available = sorted(_OPENSTATES_JOB_TARGETS | _OPENSTATES_SINGLE_JURISDICTION)
    raise HTTPException(
        status_code=404,
        detail=f"Unknown target '{target}'. Available: {', '.join(available)}",
    )


@router.post("/trigger/openstates-archive/{target}")
async def trigger_openstates_archive(
    target: str,
    background_tasks: BackgroundTasks,
    token: str = Depends(api_key_auth),
):
    """Trigger OpenStates bill-document archiving immediately, without waiting for its cron.

    Split out from the scrape trigger (2026-07-31) — archiving is now fully independent of
    scraping (see openstates_archive in sync_schedule.yaml), so it gets its own trigger too.
    Returns 202 Accepted immediately; the job runs in the background and logs to
    ~/Developer/repos/ddp-open-states/logs/scraper.log plus ddp-sync's structured log. Flow
    status is written to Redis under ddp:flow:openstates_archive.

    Targets:
        all                — every jurisdiction in openstates_archive.jurisdictions, concurrently
        <jurisdiction abbr> — a single jurisdiction from that same list (see sync_schedule.yaml)
    """
    from ddp_sync.pipelines.openstates_archive import (
        DEFAULT_ARCHIVE_JURISDICTIONS,
        run_archive_jobs,
        run_single_archive_job,
    )
    from ddp_sync.scheduler import get_scheduler

    scheduler = get_scheduler()
    config = scheduler._sync_config.get("openstates_archive", {}) if scheduler else {}
    # Read from config rather than a separate hardcoded set here -- a second copy of this
    # list silently went stale when ma/al/us were added to config 2026-08-10 (this endpoint
    # kept 404ing on them while the scheduler and run_archive_jobs' own default both knew
    # about the new jurisdictions already).
    jurisdictions = set(config.get("jurisdictions", DEFAULT_ARCHIVE_JURISDICTIONS))

    if target == "all":
        background_tasks.add_task(run_archive_jobs, config)
        return {"status": "started", "target": target}

    if target in jurisdictions:
        background_tasks.add_task(run_single_archive_job, target, config)
        return {"status": "started", "target": target}

    available = sorted(jurisdictions | {"all"})
    raise HTTPException(
        status_code=404,
        detail=f"Unknown target '{target}'. Available: {', '.join(available)}",
    )
