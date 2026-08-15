"""Session-targeted batch runner -- ddp-infra's PLAN-bill-document-provenance.md,
"Step 1, scoped version" (approved 2026-08-01 after 3 rounds of /pm-review).

Not the full 100k-bill backlog job "Processing pipeline shape" describes
elsewhere in that plan -- that job still needs real concurrency control and
weeks of runtime for the full historical corpus, neither of which this
module takes on. This is a much smaller, immediately buildable gap: there
was no way to point this pipeline at "every bill in session X" and have it
fill in whatever's missing -- every one of the 8 BillArtifact types plus
Organization Position Research was a single-bill, single-type manual call.
Motivated directly by trying to do exactly that against FL's real 2026F
session (2026-08-01) and finding the answer was no.

Sequential, no concurrency control -- matches this pipeline's actual state
everywhere else. Fills only absent rows -- no freshness/version-currency
check.

Two real callers now (SYNC-9): the on-demand API route
(``POST /trigger/bill-artifact-generation``, api/routes/triggers.py) and
``run_scheduled_session_pipeline`` below, registered as the
``session_pipeline_batch`` APScheduler job (scheduler.py) -- shipped
``enabled: false`` in sync_schedule.yaml pending ddp-infra's own Phase 8
(concurrency cap, prioritization vs. interactive Agent Smith traffic):
verified 2026-08-14 directly against ``PLAN-legbot.md`` and
``PLAN-bill-document-provenance.md`` that this still doesn't exist anywhere
in that pipeline (a plain sequential loop, no semaphore) -- see
``session_pipeline_batch``'s own YAML comment for the full status. Phase 6
(``BillArtifact`` dedup key) is separately confirmed live since 2026-07-26,
so this pipeline's actual writes are safe; what's gated is unattended
*broad* production batch volume specifically, not this module's existence
as a real caller.
"""

from __future__ import annotations

import resource
import sys
import time
import uuid

import structlog

from ddp_sync.pipelines.bill_artifact_generation import (
    generate_and_store_bill_artifact,
    generate_and_store_bill_changelog,
)
from ddp_sync.pipelines.bill_organization_position_research import (
    generate_and_store_bill_organization_positions,
)
from ddp_sync.services.broker_client import (
    BrokerClientError,
    get_bill_artifacts,
    get_bill_organization_positions_status,
)
from ddp_sync.services.local_openstates_client import (
    get_current_version_identity,
    list_current_session_bill_candidates,
)

logger = structlog.get_logger()

# The recognized BillArtifact types this plan ships -- matches
# bill_artifact_generation.py's own _ARTIFACT_TYPE_TO_QUESTION_TYPE keys
# (8 types, including bill_topics -- SYNC-1) plus bill_changelog, which
# dispatches through a separate function (generate_and_store_bill_changelog)
# because it needs a prior version's text + diff, not a single bill_source.
# Name kept as ALL_8_ARTIFACT_TYPES (not renamed to ALL_9) -- SYNC-1's own
# ticket ties any rename to the future default-artifact-set flip, not to
# this recognition-gate fix; bill_topics is deliberately NOT part of any
# default artifact_types list yet (see config/sync_schedule.yaml's
# session_pipeline_batch, which hand-picks its own small subset -- nothing
# here auto-widens to "every recognized type").
ALL_8_ARTIFACT_TYPES = frozenset({
    "bill_summary",
    "bill_pros_cons",
    "bill_vote_yes_frame",
    "bill_vote_no_frame",
    "bill_supporting_orgs",
    "bill_opposing_orgs",
    "bill_impact_analysis",
    "bill_topics",
    "bill_changelog",
})


def _peak_memory_mb() -> float:
    """Peak resident-set size for this process so far, in MB -- stdlib
    `resource` only, no new dependency for this basic metric. ru_maxrss'
    unit differs by platform: bytes on macOS/BSD, KB on Linux.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


async def _process_bill(
    candidate: dict,
    *,
    jurisdiction_iso2: str,
    session_code: str,
    artifact_types: list[str],
    include_org_research: bool,
    dry_run: bool,
    run_id: str,
) -> dict:
    gov_id = candidate["gov_id"]
    bill_openstates_id = candidate["bill_openstates_id"]
    bill_started = time.monotonic()
    result = {
        "gov_id": gov_id,
        "artifacts_generated": [],
        "artifacts_skipped_present": [],
        "artifacts_failed": [],
        "artifacts_skipped_failed_previously": [],
        "artifact_durations_seconds": {},
        "org_research_dispatched": False,
        "org_research_skipped_reason": None,
        "org_research_duration_seconds": None,
        "duration_seconds": None,
        "error": None,
    }

    try:
        coverage = await get_bill_artifacts(
            jurisdiction=jurisdiction_iso2, session_code=session_code, gov_id=gov_id
        )
    except BrokerClientError as exc:
        # A coverage-check failure for one bill shouldn't abort the batch --
        # isolated to this bill's own result entry.
        result["error"] = f"coverage_check_failed: {exc}"
        result["duration_seconds"] = time.monotonic() - bill_started
        return result

    existing = coverage["artifacts"] if coverage else {}
    needs_dispatch = []
    for artifact_type in artifact_types:
        status = (existing.get(artifact_type) or {}).get("status")
        if status == "failed":
            result["artifacts_skipped_failed_previously"].append(artifact_type)
        elif status is not None:
            # complete, pending, or processing -- has a row, don't retry.
            result["artifacts_skipped_present"].append(artifact_type)
        else:
            needs_dispatch.append(artifact_type)

    # Only resolve the bill's current version identity if something actually
    # needs it -- a bill fully covered for every requested artifact_type
    # (and already researched, or org research not requested) never needs
    # this extra lookup.
    version = None
    if needs_dispatch or include_org_research:
        version = await get_current_version_identity(bill_openstates_id)

    for artifact_type in needs_dispatch:
        if dry_run:
            result["artifacts_generated"].append(artifact_type)
            continue
        if version is None:
            logger.warning(
                "session_pipeline_no_version_identity",
                run_id=run_id, gov_id=gov_id, artifact_type=artifact_type,
            )
            result["artifacts_failed"].append(artifact_type)
            continue
        dispatch_started = time.monotonic()
        try:
            if artifact_type == "bill_changelog":
                await generate_and_store_bill_changelog(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction_iso2,
                    session_code=session_code,
                    version_date=version["version_date"],
                    version_note=version["version_note"],
                )
            else:
                await generate_and_store_bill_artifact(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction_iso2,
                    session_code=session_code,
                    version_date=version["version_date"],
                    version_note=version["version_note"],
                    bill_source=candidate["live_url_fallback"],
                    artifact_type=artifact_type,
                )
            result["artifacts_generated"].append(artifact_type)
        except Exception as exc:
            # Broad on purpose: LegBotDispatchError, BrokerClientError, and
            # bill_changelog's own ArchivedVersionMismatchError are all
            # "one artifact type's dispatch failed" from this orchestrator's
            # point of view -- no bill_changelog-specific handling needed,
            # per this design's own reasoning for why a version mismatch
            # can't cause a stuck batch (see the PLAN doc).
            logger.warning(
                "session_pipeline_artifact_dispatch_failed",
                run_id=run_id, gov_id=gov_id, artifact_type=artifact_type,
                error=str(exc),
            )
            result["artifacts_failed"].append(artifact_type)
        finally:
            result["artifact_durations_seconds"][artifact_type] = round(
                time.monotonic() - dispatch_started, 3
            )

    if include_org_research:
        try:
            org_status = await get_bill_organization_positions_status(
                bill_openstates_id=bill_openstates_id
            )
        except BrokerClientError as exc:
            result["org_research_skipped_reason"] = f"status_check_failed: {exc}"
        else:
            if org_status["has_rows"]:
                result["org_research_skipped_reason"] = "already_researched"
            elif dry_run:
                result["org_research_dispatched"] = True
            elif version is None:
                result["org_research_skipped_reason"] = "no_current_version_resolved"
            else:
                org_started = time.monotonic()
                try:
                    await generate_and_store_bill_organization_positions(
                        bill_openstates_id=bill_openstates_id,
                        jurisdiction=jurisdiction_iso2,
                        session_code=session_code,
                        version_date=version["version_date"],
                        version_note=version["version_note"],
                        bill_source=candidate["live_url_fallback"],
                        gov_id=gov_id,
                        bill_title=version["bill_title"],
                    )
                    result["org_research_dispatched"] = True
                except Exception as exc:
                    logger.warning(
                        "session_pipeline_org_research_dispatch_failed",
                        run_id=run_id, gov_id=gov_id, error=str(exc),
                    )
                    result["org_research_skipped_reason"] = f"dispatch_failed: {exc}"
                finally:
                    result["org_research_duration_seconds"] = round(
                        time.monotonic() - org_started, 3
                    )

    result["duration_seconds"] = round(time.monotonic() - bill_started, 3)

    return result


async def run_legbot_pipeline(
    jurisdiction_iso2: str,
    session_code: str,
    artifact_types: list[str],
    include_org_research: bool,
    limit: int,
    *,
    dry_run: bool = False,
) -> dict:
    """Fill in whatever's missing, for every bill in one jurisdiction/session.

    artifact_types, include_org_research, and limit all have NO default --
    the caller must decide explicitly, for every parameter with real cost
    implications. One call can trigger up to 10 real dispatches (9 artifacts
    + org research) per bill.

    dry_run=True runs the same coverage checks as a real run but skips every
    dispatch call, returning the same result shape with nothing actually
    generated -- lets an operator see real scope before spending anything.
    Not a mandatory first step; just cheap to call.

    Raises:
        ValueError: jurisdiction_iso2/session_code empty, limit <= 0, or
            artifact_types empty/contains an unrecognized type -- ordinary
            function-signature hygiene, checked before any lister/dispatch
            call.

    Basic performance metrics, added for real visibility during the first
    live runs against actual sessions -- wall-clock timing (stdlib `time`)
    and peak RSS (stdlib `resource`, no new dependency): per-dispatch and
    per-bill duration, plus one run-level duration/peak-memory pair.
    Informational only, not a new observability subsystem -- everything is
    plain fields on the same result dict/log events this design already
    specifies, not a separate metrics store.

    Returns:
        {
            "bills_considered": int,   # how many candidates were actually looked at
            "bills_processed": int,    # always equal to bills_considered today
                                        # (no concurrency, no early-abort reason exists)
            "truncated": bool,         # True if more bills exist beyond bills_considered
            "duration_seconds": float, # whole-run wall-clock time
            "peak_memory_mb": float,   # this process' peak RSS so far, at run end
            "results": [               # one entry per bill, in the order processed
                {
                    "gov_id": str,
                    "artifacts_generated": [str],
                    "artifacts_skipped_present": [str],
                    "artifacts_failed": [str],
                    "artifacts_skipped_failed_previously": [str],
                    "artifact_durations_seconds": {artifact_type: float},  # dispatched types only
                    "org_research_dispatched": bool,
                    "org_research_skipped_reason": str | None,
                    "org_research_duration_seconds": float | None,
                    "duration_seconds": float,  # this bill's total processing time
                    "error": str | None,
                },
                ...
            ],
        }
    """
    if not jurisdiction_iso2:
        raise ValueError("jurisdiction_iso2 is required")
    if not session_code:
        raise ValueError("session_code is required")
    if limit <= 0:
        raise ValueError("limit must be a positive integer")
    if not artifact_types:
        raise ValueError("artifact_types must be non-empty")
    unrecognized = set(artifact_types) - ALL_8_ARTIFACT_TYPES
    if unrecognized:
        raise ValueError(f"Unrecognized artifact_types: {sorted(unrecognized)}")

    run_id = str(uuid.uuid4())
    run_started = time.monotonic()
    logger.info(
        "session_pipeline_run_start",
        run_id=run_id,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
        artifact_types=list(artifact_types),
        include_org_research=include_org_research,
        limit=limit,
        dry_run=dry_run,
        peak_memory_mb=round(_peak_memory_mb(), 1),
    )

    # Request limit + 1 so truncated is actually computable: if more than
    # `limit` candidates come back, we know more exist beyond what's
    # considered, without misreporting a specific further count (a
    # limit + 1 probe only proves "more than limit exist").
    candidates = await list_current_session_bill_candidates(
        jurisdiction_iso2, session_code=session_code, limit=limit + 1
    )
    truncated = len(candidates) > limit
    candidates = candidates[:limit]
    bills_considered = len(candidates)

    results = []
    for candidate in candidates:
        bill_result = await _process_bill(
            candidate,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
            artifact_types=artifact_types,
            include_org_research=include_org_research,
            dry_run=dry_run,
            run_id=run_id,
        )
        results.append(bill_result)
        logger.info("session_pipeline_bill_complete", run_id=run_id, **bill_result)

    summary = {
        "bills_considered": bills_considered,
        "bills_processed": len(results),
        "truncated": truncated,
        "duration_seconds": round(time.monotonic() - run_started, 3),
        "peak_memory_mb": round(_peak_memory_mb(), 1),
        "results": results,
    }
    logger.info(
        "session_pipeline_run_end",
        run_id=run_id,
        bills_considered=bills_considered,
        bills_processed=len(results),
        truncated=truncated,
        duration_seconds=summary["duration_seconds"],
        peak_memory_mb=summary["peak_memory_mb"],
    )
    return summary


# Required session_pipeline_batch YAML keys -- deliberately no defaults
# invented here either, same "every cost-relevant param is a conscious
# choice" discipline as run_legbot_pipeline's own signature above. A
# scheduled run missing any of these fails loudly (logged + returned as a
# failure dict), rather than silently falling back to "every bill, every
# artifact type."
_REQUIRED_BATCH_CONFIG_KEYS = ("jurisdiction_iso2", "session_code", "artifact_types", "limit")


async def run_scheduled_session_pipeline(config: dict) -> dict:
    """Scheduler-driven wrapper around run_legbot_pipeline -- the
    ``session_pipeline_batch`` YAML block's own scheduled batch job (SYNC-9).

    Args:
        config: this job's own ``session_pipeline_batch`` block
            (sync_schedule.yaml). Required keys: jurisdiction_iso2,
            session_code, artifact_types, limit. Optional:
            include_org_research (default False), dry_run (default False).

    Returns:
        run_legbot_pipeline's own result dict on success. On a missing
        required key or a value run_legbot_pipeline itself rejects (e.g.
        an unrecognized artifact_type), returns
        ``{"success": False, "error": "invalid_config", ...}`` instead --
        never raises, so a misconfigured YAML block can't crash the
        scheduler process or take down other scheduled jobs with it.
    """
    missing = [key for key in _REQUIRED_BATCH_CONFIG_KEYS if not config.get(key)]
    if missing:
        logger.error("session_pipeline_batch_invalid_config", missing_keys=missing)
        return {"success": False, "error": "invalid_config", "missing_keys": missing}

    try:
        return await run_legbot_pipeline(
            config["jurisdiction_iso2"],
            config["session_code"],
            config["artifact_types"],
            config.get("include_org_research", False),
            config["limit"],
            dry_run=config.get("dry_run", False),
        )
    except ValueError as exc:
        logger.error("session_pipeline_batch_invalid_config", error=str(exc))
        return {"success": False, "error": "invalid_config", "detail": str(exc)}
