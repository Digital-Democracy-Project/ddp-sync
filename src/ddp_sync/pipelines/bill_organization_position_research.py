"""Organization Position Research — ddp-infra's PLAN-bill-document-provenance.md
Phase 8 (approved 2026-08-01 after 4 rounds of /pm-review).

Wires up LegBot's find_bill_positions (real web search for named organizations'
actual public positions on a bill, with citations) as a two-step process,
always: every position find_bill_positions reports gets independently
re-checked via verify_bill_position before it's stored. Writes one
BillOrganizationPosition row per organization found — deliberately not a
BillArtifact, since one dispatch can report zero to many organizations, each
with its own independent verification outcome.

Scope note, same shape as bill_artifact_generation.py's own scope note: this
module researches and stores organization positions for ONE caller-supplied
bill. It is manually invocable only — no scheduler registration, no batch job,
matching the plan's explicit "on-demand only" decision. Not part of Phase 8's
Pinecone dual-write requirement either — that requirement is specific to
BillArtifact's prose content for VoteBot's RAG retrieval; this pipeline's
structured, per-organization findings were never in scope for that, and
adding it would be scope beyond what was approved.
"""

from __future__ import annotations

import uuid

import structlog

# Cross-module reuse of a "private" helper is intentional here, not an
# oversight -- the approved design explicitly calls for reusing
# _resolve_bill_source() from bill_artifact_generation.py unchanged, rather
# than duplicating its archived-text-preferred logic in this module too.
from ddp_sync.config import get_settings
from ddp_sync.pipelines.bill_artifact_generation import _resolve_bill_source
from ddp_sync.services.broker_client import (
    BrokerClientError,
    write_bill_organization_position,
    write_bill_organization_research_run,
)
from ddp_sync.services.legbot_client import (
    LegBotDispatchError,
    dispatch_bill_position_verification,
    dispatch_bill_question,
)

logger = structlog.get_logger()


def _build_claim(*, org_name: str, position: str, gov_id: str, bill_title: str, jurisdiction: str, session_code: str) -> str:
    """Default claim template for verify_bill_position — includes enough bill
    identity for the model to judge confidently against an arbitrary webpage,
    not just a bare organization+verb sentence. Exact wording is still an
    open item per the approved design — needs empirical testing against real
    citations, which needs this code to exist first. This is a starting
    point, not asserted as final.
    """
    return f"{org_name} {position}s {gov_id} ({bill_title}, {jurisdiction} {session_code})"


async def generate_and_store_bill_organization_positions(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    gov_id: str,
    bill_title: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> list[dict]:
    """Research and store real, named-organization positions for one bill.

    Manually invocable only — no scheduler registration, matching the
    approved design's "on-demand only" decision.

    No bill_source parameter -- removed along with _resolve_bill_source's
    live-URL fallback (see that function's own docstring). If nothing is
    archived for this bill, this returns an empty list without ever
    dispatching find_bill_positions, same as the "found nothing" case below.

    broker_api_base/broker_api_token: optional per-call override threaded to
    every broker_client write below, same shape as
    generate_and_store_bill_artifact's own (SYNC-10) -- None (the default)
    preserves existing behavior; SYNC-15's single-bill full-run endpoint
    passes these through so org research lands on the same dev/prod broker
    instance the other artifact types do.

    Flow:
      1. Resolve bill_source from ddp-open-states' own archive -- the only
         source, same as the other 8 artifact types. Returns early if
         nothing is archived.
      2. Dispatch find_bill_positions once. If it reports
         insufficient_information or an empty positions list, return an
         empty list immediately — no rows written, no "checked, found
         nothing" marker for this on-demand phase.
      3. Cap to the first settings.org_research_max_organizations positions
         found.
      4. For each organization: dispatch verify_bill_position, then write one
         BillOrganizationPosition row. A verify-dispatch failure or a
         broker-write failure for one organization is isolated to that
         organization — logged, recorded on that row where possible, and the
         loop continues rather than aborting the whole run.

    Returns:
        One summary dict per organization found (not per row successfully
        written — a broker-write failure still gets an entry here, with
        outcome="broker_write_failed"), so an operator can see exactly which
        organizations succeeded and which failed, and at which stage:
        {"org_name": str, "position": str, "outcome": "written" |
        "verification_failed" | "broker_write_failed", "position_id": int | None}.
    """
    invocation_id = str(uuid.uuid4())
    resolved_bill_source = await _resolve_bill_source(bill_openstates_id)
    if resolved_bill_source is None:
        logger.info(
            "No archived bill text -- skipping org research entirely, no dispatch",
            bill_openstates_id=bill_openstates_id,
        )
        return []

    # Longer timeout than dispatch_bill_question's own 120s default --
    # empirically justified, not a guess: a real live validation run against
    # FL SJR 2F (2026-08-01) completed in ~119s server-side, right at the
    # edge of that default, and a client-side spurious timeout fired before
    # the result could be read. find_bill_positions does real web search
    # (potentially several searches), unlike the single-reasoning-call
    # question types that default was calibrated for.
    find_result = await dispatch_bill_question(
        resolved_bill_source, "find_bill_positions", timeout_seconds=240.0
    )
    find_answer = find_result["answer"]
    find_model_name = find_result.get("backend")

    # PLAN-bill-document-provenance.md's "Step 1, scoped version" (approved
    # 2026-08-01): record that research was attempted, right after
    # find_bill_positions returns a real answer -- whether it reports zero
    # organizations or many. Never written if the dispatch above raises
    # instead of reaching this line, so a transient dispatch failure is
    # never mistaken for "researched, found nothing." A failure writing
    # this tracking record itself is isolated, not fatal -- it's a
    # supplementary "has this been checked" marker, not required for the
    # organization findings below to be correct.
    positions_found_count = len(find_answer.get("positions") or [])
    try:
        await write_bill_organization_research_run(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            invocation_id=invocation_id,
            positions_found_count=positions_found_count,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except BrokerClientError as exc:
        logger.warning(
            "Failed to record BillOrganizationResearchRun -- continuing anyway",
            bill_openstates_id=bill_openstates_id,
            invocation_id=invocation_id,
            error=str(exc),
        )

    if find_answer.get("insufficient_information") or not find_answer.get("positions"):
        logger.info(
            "find_bill_positions found nothing — no rows written",
            bill_openstates_id=bill_openstates_id,
            invocation_id=invocation_id,
        )
        return []

    positions = find_answer["positions"]
    max_organizations = get_settings().org_research_max_organizations
    if len(positions) > max_organizations:
        logger.warning(
            "find_bill_positions returned more organizations than the cap — truncating",
            bill_openstates_id=bill_openstates_id,
            invocation_id=invocation_id,
            returned_count=len(positions),
            cap=max_organizations,
        )
        positions = positions[:max_organizations]

    results = []
    for finding in positions:
        org_name = finding["org_name"]
        position = finding["position"]
        citation_url = finding["citation_url"]
        citation_excerpt = finding.get("citation_excerpt", "")

        claim = _build_claim(
            org_name=org_name,
            position=position,
            gov_id=gov_id,
            bill_title=bill_title,
            jurisdiction=jurisdiction,
            session_code=session_code,
        )

        # Failures are isolated per organization, not per batch — a bad
        # citation shouldn't cost the others.
        verify_answer = None
        verify_model_name = None
        write_status = "complete"
        failure_stage = None
        failure_reason = None
        try:
            verify_result = await dispatch_bill_position_verification(citation_url, claim)
            verify_answer = verify_result["answer"]
            verify_model_name = verify_result.get("backend")
        except LegBotDispatchError as exc:
            logger.warning(
                "verify_bill_position failed for one organization — recording a failed row",
                bill_openstates_id=bill_openstates_id,
                invocation_id=invocation_id,
                org_name=org_name,
                error=str(exc),
            )
            write_status = "failed"
            failure_stage = "verification"
            failure_reason = "legbot_dispatch_failed"

        write_kwargs = dict(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            version_date=version_date,
            version_note=version_note,
            invocation_id=invocation_id,
            org_name=org_name,
            position=position,
            citation_url=citation_url,
            citation_excerpt=citation_excerpt,
            find_model_name=find_model_name,
            status=write_status,
            failure_stage=failure_stage,
            failure_reason=failure_reason,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        if verify_answer is not None:
            write_kwargs.update(
                verification_verdict=verify_answer["verdict"],
                verification_insufficient_information=verify_answer.get("insufficient_information", False),
                verification_content_incomplete=verify_answer.get("content_looks_incomplete", False),
                verification_explanation=verify_answer.get("explanation", ""),
                verify_model_name=verify_model_name,
            )
        # else: verification_verdict stays at ddp-broker-py's own "pending"
        # default, verify_* fields stay null -- the failure-row policy.

        try:
            write_result = await write_bill_organization_position(**write_kwargs)
        except BrokerClientError as exc:
            logger.warning(
                "Broker write failed for one organization — continuing with the rest",
                bill_openstates_id=bill_openstates_id,
                invocation_id=invocation_id,
                org_name=org_name,
                error=str(exc),
            )
            results.append({
                "org_name": org_name,
                "position": position,
                "outcome": "broker_write_failed",
                "position_id": None,
            })
            continue

        results.append({
            "org_name": org_name,
            "position": position,
            "outcome": "written" if verify_answer is not None else "verification_failed",
            "position_id": write_result.get("id"),
        })

    return results
