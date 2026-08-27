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

Bounded-concurrency, not sequential -- changed 2026-08-18 after a real live
run confirmed the pipeline processed every bill strictly one at a time,
regardless of real spare capacity. ddp-agents' AGENTS-33/34 shipped a
demand-based, memory-gated MLX-LM/MLX-VLM instance pool specifically to let
CAMS scale up under genuine concurrent load -- but a caller that only ever
has one request in flight can never trigger that pool's scale-up at all, no
matter how much memory is free. `run_legbot_pipeline` runs up to
`SESSION_PIPELINE_CONCURRENCY` (default 1 as of AGENTS-37 -- see
`SyncSettings.session_pipeline_concurrency`'s own docstring for why the
default dropped from this feature's original 4 -- env-configurable higher
once real concurrent MLX-LM throughput is validated) bills' own
`_process_bill` calls at once (`asyncio.Semaphore`-bounded, not unbounded
`asyncio.gather` -- see that setting's own docstring for why an actual cap
still matters even with the pool's own backpressure). Fills only absent
rows -- no freshness/version-currency check.

Two real callers now (SYNC-9): the on-demand API route
(``POST /trigger/bill-artifact-generation``, api/routes/triggers.py) and
``run_scheduled_session_pipeline`` below, registered as the
``session_pipeline_batch`` APScheduler job (scheduler.py) -- shipped
``enabled: false`` in sync_schedule.yaml pending ddp-infra's own Phase 8
(prioritization vs. interactive Agent Smith traffic specifically -- this
change fixes "bills stack up one at a time no matter what capacity exists,"
not "this batch job might still contend with interactive traffic under real
load," which is a separate, still-open concern). Phase 6 (``BillArtifact``
dedup key) is separately confirmed live since 2026-07-26, so this pipeline's
actual writes are safe; what's gated is unattended *broad* production batch
volume specifically, not this module's existence as a real caller.

SYNC-31: ``include_concept_statements`` folds ``ConceptStatementSet``
generation (previously the standalone, since-retired ``concept_statement_
dispatch.py`` weekly job -- see SYNC-32) into this same per-bill batch,
reusing its candidate enumeration and ``ensure_bill_exists()`` call. Kept as
its own boolean option, the same shape as ``include_org_research``, rather
than folded into ``artifact_types``/``ALL_ARTIFACT_TYPES`` -- a
``ConceptStatementSet`` row has no ``BillVersion`` at all and a coarser,
published-only dedup rule (no "failed" status exists for it), neither of
which the generic per-artifact-type coverage loop below (keyed on
``get_bill_artifacts``, which never returns this type) could express
correctly.
"""

from __future__ import annotations

import asyncio
import resource
import sys
import time
import uuid

import structlog

from ddp_sync.config import get_settings
from ddp_sync.pipelines.bill_artifact_generation import (
    generate_and_store_bill_artifact,
    generate_and_store_bill_changelog,
)
from ddp_sync.pipelines.bill_organization_position_research import (
    generate_and_store_bill_organization_positions,
)
from ddp_sync.pipelines.concept_statement_dispatch import (
    dispatch_and_store_concept_statements,
)
from ddp_sync.services.broker_client import (
    BrokerClientError,
    ensure_bill_exists,
    get_bill_artifacts,
    get_bill_organization_positions_status,
    get_concept_statement_set,
)
from ddp_sync.services.local_openstates_client import (
    get_archived_bill_text,
    get_current_version_identity,
    list_current_session_bill_candidates,
)

logger = structlog.get_logger()

# The recognized BillArtifact types this plan ships -- matches
# bill_artifact_generation.py's own _ARTIFACT_TYPE_TO_QUESTION_TYPE keys
# (8 types, including bill_topics -- SYNC-1) plus bill_changelog, which
# dispatches through a separate function (generate_and_store_bill_changelog)
# because it needs a prior version's text + diff, not a single bill_source.
# Renamed from ALL_8_ARTIFACT_TYPES (SYNC-38). The count had been wrong since
# bill_changelog joined -- there are nine members, not eight. The old comment
# here deferred the rename to SYNC-1's default-artifact-set flip; SYNC-1 closed
# 2026-08-15 without the rename happening, so the deferral had simply outlived
# the thing it was waiting on.
#
# The count is dropped rather than corrected to nine, so it cannot go stale the
# same way a third time.
#
# bill_topics is deliberately NOT part of any default artifact_types list yet
# (see config/sync_schedule.yaml's session_pipeline_batch, which hand-picks
# its own small subset -- nothing here auto-widens to "every recognized
# type").
ALL_ARTIFACT_TYPES = frozenset({
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

# SYNC-38: the order these are dispatched in, which is a property of the
# pipeline rather than of however the caller wrote its request body.
#
# ALL_ARTIFACT_TYPES above stays a frozenset -- it exists to validate
# membership, and a set is the right shape for that. But a set has no order,
# so before this constant the dispatch order came from the caller's own
# artifact_types list, and therefore so did which bill's KV cache survived.
#
# The order encodes one rule: everything that can share a bill's prefilled
# cache goes before the one thing that cannot.
#
#   - The 8 standard types all render the same bill text and share its
#     prefix, so after the first call they are cache hits. Measured on VA
#     2026S1 (2026-08-27): 90-100% warm.
#   - concept_statements shares that prefix too, and is dispatched separately
#     (SYNC-31) rather than as an artifact_type -- see the dispatch below.
#   - bill_changelog cannot share it. It builds a two-input prompt (prior
#     version text + diff), so it always takes its own cache key. Running it
#     before concept_statements evicted the bill's cache and made concept
#     statements pay a full prefill: 3 warm out of 20 on that same run,
#     against 90-100% for everything else. It goes last, always.
ARTIFACT_DISPATCH_ORDER: tuple[str, ...] = (
    "bill_summary",
    "bill_pros_cons",
    "bill_vote_yes_frame",
    "bill_vote_no_frame",
    "bill_supporting_orgs",
    "bill_opposing_orgs",
    "bill_impact_analysis",
    "bill_topics",
    # last, and deliberately: its own cache key evicts the bill's.
    "bill_changelog",
)

# The one type above that cannot reuse a bill's prefilled cache. Named rather
# than written as a literal at the two places that care, so the reason lives
# in one place.
_OWN_CACHE_KEY_TYPES = frozenset({"bill_changelog"})

assert set(ARTIFACT_DISPATCH_ORDER) == ALL_ARTIFACT_TYPES, (
    "ARTIFACT_DISPATCH_ORDER and ALL_ARTIFACT_TYPES have drifted apart; a "
    "type recognised but never ordered would silently never dispatch"
)



def _peak_memory_mb() -> float:
    """Peak resident-set size for this process so far, in MB -- stdlib
    `resource` only, no new dependency for this basic metric. ru_maxrss'
    unit differs by platform: bytes on macOS/BSD, KB on Linux.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


async def _run_org_research(
    *,
    bill_openstates_id: str,
    gov_id: str,
    jurisdiction_iso2: str,
    session_code: str,
    version: dict | None,
    dry_run: bool,
    run_id: str,
    broker_api_base: str | None,
    broker_api_token: str | None,
) -> dict:
    """One bill's organisation research, as a self-contained unit of work.

    Lifted out of _process_bill unchanged (SYNC-37) so it can run as a task
    alongside the artifact loop instead of blocking it. It returns the three
    result fields it used to assign directly and the caller merges them, so
    the per-bill result dict keeps exactly the shape every downstream reader
    already expects.

    It still catches its own dispatch failures, as it always did -- a bill
    whose organisation research fails keeps its artifacts. That mattered when
    this was inline and matters more now: an exception escaping into a task
    nobody handles becomes an unhandled-exception warning and a silently lost
    result.
    """
    out = {
        "org_research_dispatched": False,
        "org_research_skipped_reason": None,
        "org_research_duration_seconds": None,
    }
    try:
        org_status = await get_bill_organization_positions_status(
            bill_openstates_id=bill_openstates_id,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except BrokerClientError as exc:
        out["org_research_skipped_reason"] = f"status_check_failed: {exc}"
        return out

    if org_status["has_rows"]:
        out["org_research_skipped_reason"] = "already_researched"
        return out
    if dry_run:
        out["org_research_dispatched"] = True
        return out
    if version is None:
        out["org_research_skipped_reason"] = "no_current_version_resolved"
        return out

    org_started = time.monotonic()
    try:
        await generate_and_store_bill_organization_positions(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction_iso2,
            session_code=session_code,
            version_date=version["version_date"],
            version_note=version["version_note"],
            gov_id=gov_id,
            bill_title=version["bill_title"],
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        out["org_research_dispatched"] = True
    except Exception as exc:
        logger.warning(
            "session_pipeline_org_research_dispatch_failed",
            run_id=run_id, gov_id=gov_id, error=str(exc),
        )
        out["org_research_skipped_reason"] = f"dispatch_failed: {exc}"
    finally:
        out["org_research_duration_seconds"] = round(time.monotonic() - org_started, 3)
    return out


async def _process_bill(
    candidate: dict,
    **kwargs,
) -> dict:
    """One bill, with its organisation-research task guaranteed cleaned up.

    SYNC-37 gave _process_bill_inner a task that outlives individual awaits,
    and a task needs an owner: one that runs on the normal return, on an
    exception, and on the whole run being cancelled mid-bill (which has
    happened, twice, under memory pressure). Rather than wrap 130 lines of
    existing, unchanged bill processing in a try block -- or split it into a
    helper taking fifteen parameters purely to give `finally` a body -- the
    ownership lives here, and the inner function hands the task up through
    `org_holder` the moment it creates one.
    """
    org_holder: dict = {}
    try:
        return await _process_bill_inner(candidate, org_holder=org_holder, **kwargs)
    finally:
        org_task = org_holder.get("task")
        if org_task is not None:
            # Consume it unconditionally, not only while it is still
            # pending. /pm-review caught the difference: a task that has
            # already finished *with an exception* is `done()`, and skipping
            # it there leaves that exception unretrieved -- an asyncio
            # warning at collection time and a result quietly discarded,
            # which is the exact failure this cleanup exists to prevent.
            if not org_task.done():
                org_task.cancel()
            try:
                await org_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                # Cannot displace whatever exception is already on its way
                # out: an `except` inside `finally` only suppresses what it
                # catches itself. So a bill that failed for its own reason
                # still reports that reason, and a failure belonging to the
                # org task is logged as its own event rather than
                # impersonating the bill's.
                logger.warning(
                    "session_pipeline_org_research_task_failed",
                    run_id=kwargs.get("run_id"),
                    gov_id=candidate.get("gov_id"),
                    error=str(exc),
                )


async def _process_bill_inner(
    candidate: dict,
    *,
    org_holder: dict,
    jurisdiction_iso2: str,
    session_code: str,
    artifact_types: list[str],
    include_org_research: bool,
    include_concept_statements: bool,
    dry_run: bool,
    run_id: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
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
        "concept_statements_dispatched": False,
        "concept_statements_skipped_reason": None,
        "concept_statements_duration_seconds": None,
        "duration_seconds": None,
        "error": None,
    }

    try:
        coverage = await get_bill_artifacts(
            jurisdiction=jurisdiction_iso2,
            session_code=session_code,
            gov_id=gov_id,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
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

    # SYNC-38: canonical order, not the caller's. See ARTIFACT_DISPATCH_ORDER.
    needs_dispatch.sort(key=ARTIFACT_DISPATCH_ORDER.index)
    cache_sharing = [t for t in needs_dispatch if t not in _OWN_CACHE_KEY_TYPES]
    own_cache_key = [t for t in needs_dispatch if t in _OWN_CACHE_KEY_TYPES]


    # Only resolve the bill's current version identity if something actually
    # needs it -- a bill fully covered for every requested artifact_type
    # (and already researched, or org research not requested) never needs
    # this extra lookup.
    version = None
    if needs_dispatch or include_org_research:
        version = await get_current_version_identity(bill_openstates_id)

    # SYNC-21 (PLAN-local-openstates-migration.md §3.6): make sure a Bill row
    # exists in ddp-broker-py before dispatching any real LegBot analysis for
    # this bill -- write_bill_artifact/write_bill_organization_position only
    # ever attach to an EXISTING Bill row, by design, so without this a real
    # dispatch's output is silently discarded for any bill Voatz/Webflow
    # hasn't already brought in. `version is not None` here already implies
    # needs_dispatch or include_org_research was true above (version is only
    # ever resolved in that case) -- this bill was never going to be
    # skipped, so `ensure` is worth attempting.
    #
    # get_archived_bill_text below is a NEW, additional call, purely as a
    # gate -- generate_and_store_bill_artifact's own internal per-artifact-
    # type archived-text check (_resolve_bill_source) is completely
    # unchanged and still runs exactly as it does today. A bill with version
    # metadata but no archived text yet (most jurisdictions besides FL,
    # today) must never trigger `ensure` or get a stub created for it --
    # placing this check any earlier (e.g. right after
    # get_current_version_identity, before confirming text is actually
    # archived) would do exactly that for most non-FL bills in a session.
    if not dry_run and version is not None:
        archived_text = await get_archived_bill_text(bill_openstates_id)
        if archived_text:
            try:
                await ensure_bill_exists(
                    jurisdiction=jurisdiction_iso2,
                    session_code=session_code,
                    gov_id=gov_id,
                    title=version["bill_title"],
                    chamber_classification=version["chamber_classification"],
                    jurisdiction_classification=version["jurisdiction_classification"],
                    bill_openstates_id=bill_openstates_id,
                    broker_api_base=broker_api_base,
                    broker_api_token=broker_api_token,
                )
            except BrokerClientError as exc:
                # Prevents this bill's LegBot dispatch entirely (both
                # artifact generation and org research below) -- a stable,
                # named result category distinct from artifacts_failed's
                # per-artifact-type failures, so a systemic problem (the
                # broker endpoint itself down/misconfigured -- every bill in
                # the run would show this same reason) reads differently
                # from a handful of bills each individually lacking archived
                # text.
                logger.warning(
                    "session_pipeline_ensure_bill_failed",
                    run_id=run_id, gov_id=gov_id, error=str(exc),
                )
                result["error"] = f"ensure_failed: {exc}"
                result["duration_seconds"] = round(time.monotonic() - bill_started, 3)
                return result

    # SYNC-37: organisation research starts here and is awaited at the very
    # end, instead of running between the artifact loop and concept
    # statements.
    #
    # It used to block. Traced live on the WA 2025-2026 run of 2026-08-27:
    # eight artifacts five seconds apart, every one a warm cache hit, then
    # ~45 seconds of nothing while this bill waited on a network call, then
    # concept_statements paying a full re-read because the idle worker had
    # been correctly given to another bill in the meantime. Six of seven
    # full-artifact bills paid 2-3 prefills instead of one.
    #
    # This is the latest point it can start: `version` has been resolved and
    # `ensure_bill_exists` has run, and both of those can still return early
    # above -- launching before them would leave a task to clean up on paths
    # that currently just return.
    #
    # No new concurrency machinery: run_legbot_pipeline already runs bills
    # concurrently under a Semaphore, and this is one more await on the same
    # loop.
    org_task: asyncio.Task | None = None
    if include_org_research:
        org_task = asyncio.create_task(_run_org_research(
            bill_openstates_id=bill_openstates_id,
            gov_id=gov_id,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
            version=version,
            dry_run=dry_run,
            run_id=run_id,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        ))
        # The wrapper owns cleanup, and it can only clean up a task it can
        # see -- this is the handoff.
        org_holder["task"] = org_task

    # SYNC-38: one dispatch body, invoked in two phases with concept
    # statements between them. A closure rather than a helper taking a dozen
    # parameters purely to relocate code that has not otherwise changed.
    #
    # `continue` became `return`: inside the old loop it meant 'skip to the
    # next artifact_type', which per-artifact is the same thing.
    async def _dispatch_artifact(artifact_type: str) -> None:
        if dry_run:
            result["artifacts_generated"].append(artifact_type)
            return  # next artifact_type
        if version is None:
            logger.warning(
                "session_pipeline_no_version_identity",
                run_id=run_id, gov_id=gov_id, artifact_type=artifact_type,
            )
            result["artifacts_failed"].append(artifact_type)
            return  # next artifact_type
        dispatch_started = time.monotonic()
        try:
            if artifact_type == "bill_changelog":
                artifact_result = await generate_and_store_bill_changelog(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction_iso2,
                    session_code=session_code,
                    version_date=version["version_date"],
                    version_note=version["version_note"],
                    broker_api_base=broker_api_base,
                    broker_api_token=broker_api_token,
                )
            else:
                artifact_result = await generate_and_store_bill_artifact(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction_iso2,
                    session_code=session_code,
                    version_date=version["version_date"],
                    version_note=version["version_note"],
                    artifact_type=artifact_type,
                    broker_api_base=broker_api_base,
                    broker_api_token=broker_api_token,
                )
            # SYNC-24: a normal (non-raising) return doesn't mean success --
            # generate_and_store_bill_artifact/_changelog deliberately return
            # normally with a written status="failed" row for a legitimate
            # decline (insufficient_information, no_archived_bill_text,
            # no_valid_topics, no_archived_changelog_inputs), so the actual
            # returned status has to be inspected. Anything other than
            # "complete" -- including "failed" or a missing/malformed status
            # from an outdated mock -- is treated as a failure rather than
            # ever being counted as generated.
            if artifact_result.get("status") == "complete":
                result["artifacts_generated"].append(artifact_type)
            else:
                result["artifacts_failed"].append(artifact_type)
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

    # Phase 1: everything that shares the bill's prefilled cache.
    for artifact_type in cache_sharing:
        await _dispatch_artifact(artifact_type)

    # SYNC-31: ConceptStatementSet generation, folded in from the now-retired
    # standalone concept_statement_dispatch.py weekly job (SYNC-32). No
    # `version` dependency at all -- unlike every other artifact_type/org
    # research above, a ConceptStatementSet row has no BillVersion FK, so
    # this never needs get_current_version_identity to have resolved
    # anything. Its own dedup check (get_concept_statement_set) is
    # deliberately coarser than get_bill_artifacts' -- "does a *published*
    # set exist at all," with no version-currency concept -- and preserved
    # exactly as concept_statement_dispatch.py's own standalone job already
    # implemented it, per this ticket's own "relocation, not a rewrite"
    # scope. There is also no "failed" ConceptStatementSet status to skip on
    # a later run -- insufficient_information (or no archived text at all)
    # means dispatch_and_store_concept_statements wrote nothing, recorded
    # here as concept_statements_skipped_reason="nothing_to_publish", not as
    # a member of artifacts_failed (which is BillArtifact-status-shaped).
    if include_concept_statements:
        try:
            existing_concept_set = await get_concept_statement_set(
                gov_id=gov_id,
                jurisdiction_iso2=jurisdiction_iso2,
                session_code=session_code,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        except BrokerClientError as exc:
            result["concept_statements_skipped_reason"] = f"status_check_failed: {exc}"
        else:
            if existing_concept_set is not None:
                result["concept_statements_skipped_reason"] = "already_published"
            elif dry_run:
                result["concept_statements_dispatched"] = True
            else:
                concept_started = time.monotonic()
                try:
                    concept_result = await dispatch_and_store_concept_statements(
                        gov_id=gov_id,
                        bill_openstates_id=bill_openstates_id,
                        jurisdiction_iso2=jurisdiction_iso2,
                        session_code=session_code,
                        bill_source=candidate.get("live_url_fallback", ""),
                        broker_api_base=broker_api_base,
                        broker_api_token=broker_api_token,
                    )
                except Exception as exc:
                    logger.warning(
                        "session_pipeline_concept_statements_dispatch_failed",
                        run_id=run_id, gov_id=gov_id, error=str(exc),
                    )
                    result["concept_statements_skipped_reason"] = f"dispatch_failed: {exc}"
                else:
                    if concept_result is None:
                        result["concept_statements_skipped_reason"] = "nothing_to_publish"
                    else:
                        result["concept_statements_dispatched"] = True
                finally:
                    result["concept_statements_duration_seconds"] = round(
                        time.monotonic() - concept_started, 3
                    )

    # Phase 2: bill_changelog last, after concept statements have used the
    # cache. Its two-input prompt takes its own key, so running it earlier is
    # what made concept_statements pay a full prefill (SYNC-38).
    #
    # This is deliberately NOT wrapped in a try/finally, though /pm-review
    # asked. Moving changelog after the concept block does create a path where
    # it never dispatches -- but only one, and it is not a path where the
    # answer matters. Every *contained* concept failure is caught above
    # (BrokerClientError on the status check, broad Exception on the dispatch),
    # and phase 2 still runs; there is a test for exactly that. The only escape
    # is an unexpected exception from get_concept_statement_set, which
    # propagates out of _process_bill, out of asyncio.gather (no
    # return_exceptions) and ends the entire run -- at which point whether this
    # one bill's changelog was written before everything stopped is not a
    # meaningful difference, and the next run's coverage check re-dispatches it
    # anyway. A try/finally awaiting inside a possibly-cancelled scope would be
    # real added risk in exchange for that.
    for artifact_type in own_cache_key:
        await _dispatch_artifact(artifact_type)

    # SYNC-37: collect the organisation research launched before the artifact
    # loop. By now it has almost always finished during work that used to
    # wait for it; if it has not, this is the same wait as before, just at
    # the end. `_run_org_research` handles its own failures and returns the
    # same three fields that used to be assigned inline, so the result dict
    # below is identical in shape and content to what it was.
    if org_task is not None:
        result.update(await org_task)

    result["duration_seconds"] = round(time.monotonic() - bill_started, 3)

    return result


async def run_legbot_pipeline(
    jurisdiction_iso2: str,
    session_code: str,
    artifact_types: list[str],
    include_org_research: bool,
    limit: int,
    *,
    include_concept_statements: bool,
    dry_run: bool = False,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Fill in whatever's missing, for every bill in one jurisdiction/session.

    artifact_types, include_org_research, include_concept_statements, and
    limit all have NO default -- the caller must decide explicitly, for
    every parameter with real cost implications. One call can trigger up to
    11 real dispatches (9 artifacts + org research + concept statements) per
    bill. include_concept_statements is keyword-only (unlike the other
    three, which are positional-or-keyword) -- added after those three
    already had real positional callers (SYNC-9/SYNC-15); every caller must
    still pass it explicitly, just as an explicit keyword rather than a new
    required positional slot spliced into an existing argument order.

    broker_api_base/broker_api_token: optional per-call override threaded to
    every bill's _process_bill call. None (the default) preserves this
    function's existing behavior for its only real caller today (SYNC-9's
    batch pipeline, which always targets the one shared broker instance via
    settings) -- SYNC-15's single-bill full-run endpoint is the first caller
    to pass these explicitly.

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

    Bounded concurrency: up to `SyncSettings.session_pipeline_concurrency`
    (default 1 as of AGENTS-37, env `SESSION_PIPELINE_CONCURRENCY`) bills'
    own _process_bill calls run at once -- see that setting's own docstring
    and this module's own docstring for why the default isn't higher out of
    the box. `session_pipeline_bill_complete` is still logged per-bill, as
    each one actually finishes
    (interleaved under concurrency, not batched at the end).

    Returns:
        {
            "bills_considered": int,   # how many candidates were actually looked at
            "bills_processed": int,    # always equal to bills_considered today
                                        # (no early-abort reason exists)
            "truncated": bool,         # True if more bills exist beyond bills_considered
            "duration_seconds": float, # whole-run wall-clock time
            "peak_memory_mb": float,   # this process' peak RSS so far, at run end
            "results": [               # one entry per bill, in candidate order
                                        # (asyncio.gather preserves input order
                                        # regardless of completion order under
                                        # concurrency)
                {
                    "gov_id": str,
                    "artifacts_generated": [str],  # dispatch returned status="complete"
                    "artifacts_skipped_present": [str],
                    "artifacts_failed": [str],     # SYNC-24: covers BOTH a raised
                                                    # dispatch/persistence exception AND a
                                                    # normal return with a written
                                                    # status="failed" row -- e.g. LegBot
                                                    # legitimately declining via
                                                    # insufficient_information. Both are
                                                    # "this artifact_type produced no
                                                    # usable content" from this summary's
                                                    # point of view; distinguishing an
                                                    # outage from a legitimate decline
                                                    # requires reading the underlying
                                                    # BillArtifact row's own
                                                    # failure_stage/failure_reason.
                    "artifacts_skipped_failed_previously": [str],
                    "artifact_durations_seconds": {artifact_type: float},  # dispatched types only
                    "org_research_dispatched": bool,
                    "org_research_skipped_reason": str | None,
                    "org_research_duration_seconds": float | None,
                    "concept_statements_dispatched": bool,
                    "concept_statements_skipped_reason": str | None,
                    "concept_statements_duration_seconds": float | None,
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
    unrecognized = set(artifact_types) - ALL_ARTIFACT_TYPES
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
        include_concept_statements=include_concept_statements,
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

    # Bounded concurrency (2026-08-18): several bills' own _process_bill
    # calls run at once instead of strictly one at a time, up to
    # session_pipeline_concurrency -- see this module's own docstring and
    # that setting's docstring for why a caller that only ever has one
    # request in flight can never exercise ddp-agents' AGENTS-33/34 MLX
    # instance pool at all. A semaphore, not a bare asyncio.gather over
    # every candidate at once: _process_bill() fires several non-MLX HTTP
    # calls per bill (broker coverage check, OpenStates lookups,
    # ensure_bill_exists) that a genuinely unbounded fan-out would send all
    # at once for however many bills a session has -- potentially thousands
    # -- for no throughput benefit once only a handful of MLX-LM instances
    # can usefully be busy at the same time.
    semaphore = asyncio.Semaphore(get_settings().session_pipeline_concurrency)

    async def _process_bill_bounded(candidate: dict) -> dict:
        async with semaphore:
            bill_result = await _process_bill(
                candidate,
                jurisdiction_iso2=jurisdiction_iso2,
                session_code=session_code,
                artifact_types=artifact_types,
                include_org_research=include_org_research,
                include_concept_statements=include_concept_statements,
                dry_run=dry_run,
                run_id=run_id,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        # Outside the semaphore's `async with` -- release this bill's slot
        # for the next queued one as soon as the real work finishes, rather
        # than holding it through this log call too.
        logger.info("session_pipeline_bill_complete", run_id=run_id, **bill_result)
        return bill_result

    results = list(await asyncio.gather(*(_process_bill_bounded(c) for c in candidates)))

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


async def run_single_bill_full(
    *,
    bill_openstates_id: str,
    jurisdiction_iso2: str,
    session_code: str,
    gov_id: str,
    bill_source: str,
    artifact_types: list[str] | None,
    include_org_research: bool,
    include_concept_statements: bool,
    dry_run: bool = False,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Run every requested artifact type (default: all of
    ALL_ARTIFACT_TYPES) plus optional org research and concept-statement
    generation for ONE caller-specified bill, in a single call -- SYNC-15
    (include_concept_statements added SYNC-31).

    The single-bill counterpart to run_legbot_pipeline: that function lists
    candidates across a whole jurisdiction/session and loops _process_bill
    over each; this function skips the listing step entirely and calls
    _process_bill directly with one caller-built candidate, since the caller
    already knows exactly which bill they want. Reuses _process_bill's
    existing coverage-check skip logic unchanged -- this is not a
    force-regenerate mode, already-present artifacts are still skipped.

    artifact_types: None means "all of them" (ALL_ARTIFACT_TYPES) -- the
    one place in this module a missing value gets a real default rather than
    being rejected, since "run everything for this bill" is this function's
    whole reason to exist. An empty list is still rejected, same as
    run_legbot_pipeline, since that's never a meaningful request either way.

    gov_id: required, not derived from bill_openstates_id -- _process_bill's
    coverage check (get_bill_artifacts) is keyed by (jurisdiction, session,
    gov_id), not by bill_openstates_id alone, so this can't be skipped or
    inferred without an extra lookup this function deliberately avoids (the
    caller already has this from wherever they got bill_openstates_id).

    Raises:
        ValueError: bill_openstates_id/jurisdiction_iso2/session_code/gov_id/
            bill_source empty, or artifact_types contains an unrecognized
            type -- same signature-hygiene discipline as run_legbot_pipeline.

    Returns:
        _process_bill's own per-bill result dict (see run_legbot_pipeline's
        docstring for its shape) plus "run_id".
    """
    if not bill_openstates_id:
        raise ValueError("bill_openstates_id is required")
    if not jurisdiction_iso2:
        raise ValueError("jurisdiction_iso2 is required")
    if not session_code:
        raise ValueError("session_code is required")
    if not gov_id:
        raise ValueError("gov_id is required")
    if not bill_source:
        raise ValueError("bill_source is required")

    resolved_artifact_types = (
        list(ALL_ARTIFACT_TYPES) if artifact_types is None else artifact_types
    )
    if not resolved_artifact_types:
        raise ValueError("artifact_types must be non-empty")
    unrecognized = set(resolved_artifact_types) - ALL_ARTIFACT_TYPES
    if unrecognized:
        raise ValueError(f"Unrecognized artifact_types: {sorted(unrecognized)}")

    run_id = str(uuid.uuid4())
    logger.info(
        "single_bill_full_run_start",
        run_id=run_id,
        bill_openstates_id=bill_openstates_id,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
        gov_id=gov_id,
        artifact_types=resolved_artifact_types,
        include_org_research=include_org_research,
        include_concept_statements=include_concept_statements,
        dry_run=dry_run,
    )

    candidate = {
        "gov_id": gov_id,
        "bill_openstates_id": bill_openstates_id,
        "live_url_fallback": bill_source,
    }
    result = await _process_bill(
        candidate,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
        artifact_types=resolved_artifact_types,
        include_org_research=include_org_research,
        include_concept_statements=include_concept_statements,
        dry_run=dry_run,
        run_id=run_id,
        broker_api_base=broker_api_base,
        broker_api_token=broker_api_token,
    )
    result["run_id"] = run_id
    logger.info("single_bill_full_run_end", run_id=run_id, **{k: v for k, v in result.items() if k != "run_id"})
    return result


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
            include_org_research (default False), include_concept_statements
            (default False, SYNC-31), dry_run (default False).

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
            include_concept_statements=config.get("include_concept_statements", False),
            dry_run=config.get("dry_run", False),
        )
    except ValueError as exc:
        logger.error("session_pipeline_batch_invalid_config", error=str(exc))
        return {"success": False, "error": "invalid_config", "detail": str(exc)}


