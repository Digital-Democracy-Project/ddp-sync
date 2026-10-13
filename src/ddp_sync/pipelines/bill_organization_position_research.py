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
from collections import Counter

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
    write_organization_content,
)
from ddp_sync.services.legbot_client import (
    LegBotDispatchError,
    dispatch_bill_position_verification,
    dispatch_bill_question,
    dispatch_describe_organization,
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


# SYNC-100: how a rate-limited verify surfaces. LegBot degrades to {"insufficient_information":
# True, "reason": "backend error: <upstream message>"} with no verdict (legbot_client's own
# docstring), so the only signal is the text of the upstream error.
_RATE_LIMIT_MARKERS = ("429", "rate_limit", "rate limit", "529", "overloaded")


def _is_rate_limited(verify_answer: dict) -> bool:
    reason = str(verify_answer.get("reason") or "").lower()
    return not verify_answer.get("verdict") and any(marker in reason for marker in _RATE_LIMIT_MARKERS)


async def verify_and_store_position(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    gov_id: str,
    bill_title: str,
    invocation_id: str,
    org_name: str,
    position: str,
    citation_url: str,
    citation_excerpt: str = "",
    find_model_name: str | None = None,
    skip_write_on_rate_limit: bool = False,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Verify one organization's cited position against its page, then write one row.

    The per-organization tail of generate_and_store_bill_organization_positions, split out
    (SYNC-100) so a caller that already has a finding -- not one from find_bill_positions --
    verifies and stores it through the same code. Failures stay isolated to this
    organization: a verify-dispatch failure is recorded on the row, a broker-write failure
    is reported, and neither raises.

    skip_write_on_rate_limit: when True, a verify answer that was rate limited is reported
    as outcome="retryable" and nothing is written. Off by default so find_bill_positions
    research keeps writing the degraded row it always has.

    Returns {"org_name", "position", "outcome", "position_id", "result",
    "verification_verdict", "attempted_verdict", "organization_id",
    "organization_needs_content"} (the last two are the broker's answer about the organization the
    finding resolved to, SYNC-107; None/False when nothing was written); outcome is one of "written",
    "verification_failed", "broker_write_failed" or "retryable". `attempted_verdict` is what THIS
    check concluded ("confirmed", "not_confirmed", or "pending" when it could not tell; None when it
    never got an answer). `result` ("created"/"updated"/"unchanged") and `verification_verdict` are
    the broker's answer about the STORED row, which differ when an inconclusive check was refused
    permission to overwrite a settled row. Both are None when nothing was written.
    """
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
    attempted_verdict = None
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

    if skip_write_on_rate_limit and verify_answer is not None and _is_rate_limited(verify_answer):
        # SYNC-100: a rate-limited answer says nothing about the citation, so it
        # is reported and not written -- a stored row would stand for "could not
        # be checked" forever, where nothing stored means the next run tries again.
        logger.warning(
            "verify_bill_position was rate limited -- writing nothing so a later run retries",
            bill_openstates_id=bill_openstates_id,
            invocation_id=invocation_id,
            org_name=org_name,
            reason=verify_answer.get("reason"),
        )
        return {
            "org_name": org_name,
            "position": position,
            "outcome": "retryable",
            "position_id": None,
            "result": None,
            "verification_verdict": None,
            "attempted_verdict": None,
            "organization_id": None,
            "organization_needs_content": False,
        }

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
        # SYNC-41: "pending", not "" -- ddp-broker-py serializes this as
        # a ChoiceField over pending/confirmed/not_confirmed, so a blank
        # string is a 400, not a null. "pending" is already this field's
        # established meaning for "verification produced no verdict"
        # (the broker's own test_failed_verification_write_leaves_verdict
        # _pending covers the dispatch-failure case), and it is what the
        # else-branch below already leaves behind.
        #
        # LegBot omits "verdict" whenever it degrades to
        # insufficient_information without reaching the model at all --
        # a backend error, unparseable JSON, an unresolved URL (four
        # such paths in ddp-agents' legbot/handlers.py). Those answers
        # carry "reason" instead of "explanation", so fall back to it:
        # without that, the one field saying *why* the page could not be
        # read is dropped on the floor.
        # `or`, not a .get() default: /pm-review noted that "verdict"
        # can be present-but-null or blank as easily as absent, and both
        # are a broker 400 rather than a KeyError. An *unexpected*
        # non-empty verdict is deliberately still sent through and left
        # to fail the broker's own ChoiceField -- that is one organization
        # losing its write, loudly, rather than model drift being quietly
        # rewritten to "pending" here.
        verdict = verify_answer.get("verdict") or "pending"
        attempted_verdict = verdict
        explanation = verify_answer.get("explanation") or verify_answer.get("reason", "")
        if not verify_answer.get("verdict"):
            logger.warning(
                "verify_bill_position returned no verdict — recording the "
                "row as pending/insufficient rather than failing the bill",
                bill_openstates_id=bill_openstates_id,
                invocation_id=invocation_id,
                org_name=org_name,
                citation_url=citation_url,
                reason=explanation,
            )
        write_kwargs.update(
            verification_verdict=verdict,
            verification_insufficient_information=verify_answer.get("insufficient_information", False),
            verification_content_incomplete=verify_answer.get("content_looks_incomplete", False),
            verification_explanation=explanation,
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
        return {
            "org_name": org_name,
            "position": position,
            "outcome": "broker_write_failed",
            "position_id": None,
            "result": None,
            "verification_verdict": None,
            "attempted_verdict": attempted_verdict,
            "organization_id": None,
            "organization_needs_content": False,
        }

    return {
        "org_name": org_name,
        "position": position,
        "outcome": "written" if verify_answer is not None else "verification_failed",
        "position_id": write_result.get("id"),
        "result": write_result.get("result"),
        "verification_verdict": write_result.get("verification_verdict"),
        "attempted_verdict": attempted_verdict,
        "organization_id": write_result.get("organization_id"),
        "organization_needs_content": bool(write_result.get("organization_needs_content")),
    }


# SYNC-107: the broker Organization fields LegBot's describe_organization answer fills. LegBot's answer
# keys are the broker's own field names (AGENTS-104), so nothing is renamed; its `sources` list is not a
# broker field and is ignored.
_ABOUT_FIELDS = (
    "description",
    "org_type",
    "policy_focus_areas",
    "funding",
    "affiliates",
    "website",
    "email",
    "contact_page_url",
)


def _as_text(value) -> str:
    """A model may answer a list-shaped field (policy_focus_areas, affiliates) as a list; the broker stores text."""
    if isinstance(value, list):
        return "; ".join(str(item).strip() for item in value if str(item).strip())
    return value.strip() if isinstance(value, str) else ""


async def generate_and_store_organization_content(
    *,
    organization_id: int,
    org_name: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Ask LegBot for one organization's "About" content and save it (SYNC-107, with AGENTS-104 and BROKER-204).

    The broker fills only fields that are still empty, so this never overwrites Webflow-sourced or
    staff-edited text. Failures stay isolated to this organization and never raise.

    Returns {"organization_id", "outcome", "written"}; outcome is one of "written", "nothing_written"
    (LegBot had nothing, or the broker found every field already filled), "dispatch_failed" or
    "broker_write_failed".
    """
    outcome = {"organization_id": organization_id, "outcome": "nothing_written", "written": []}
    try:
        result = await dispatch_describe_organization(org_name)
    except LegBotDispatchError as exc:
        logger.warning(
            "describe_organization dispatch failed -- organization left without content",
            organization_id=organization_id, org_name=org_name, error=str(exc),
        )
        outcome["outcome"] = "dispatch_failed"
        return outcome

    answer = result["answer"]
    if answer.get("insufficient_information"):
        return outcome
    content = {field: _as_text(answer.get(field)) for field in _ABOUT_FIELDS}
    if not any(content.values()):
        return outcome

    source = f"legbot:describe_organization:{result.get('backend') or 'unknown'}"
    try:
        written = await write_organization_content(
            organization_id=organization_id,
            content=content,
            source=source,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except BrokerClientError as exc:
        logger.warning(
            "Broker rejected organization content -- continuing",
            organization_id=organization_id, org_name=org_name, error=str(exc),
        )
        outcome["outcome"] = "broker_write_failed"
        return outcome
    outcome["written"] = written.get("written", [])
    if outcome["written"]:
        outcome["outcome"] = "written"
    return outcome


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

    # AGENTS-74 / SYNC-37: no per-call timeout override. This used to pass
    # timeout_seconds=240.0, raised from a then-120s default after a real FL
    # SJR 2F run finished at ~119s and tripped a spurious client-side
    # timeout. That default is now settings.legbot_dispatch_timeout_seconds
    # (1200s), so the override no longer raises anything -- it lowers it.
    #
    # And the defect was never the duration. When this cap fires the call
    # returns *fewer organisations than exist*, and nothing downstream can
    # tell "found 3 organisations" from "found 3 before we cut it off": a
    # silently truncated research pass looks exactly like a thorough one, and
    # the missing organisations are indistinguishable from real absence.
    # Observed calls run 25-45s, nowhere near the cap, and no log line has
    # ever shown it firing -- so this changes nothing in the common case and
    # only matters in the tail, which is precisely the case it corrupted.
    #
    # This is not unbounded. legbot_dispatch_timeout_seconds still applies
    # here, and LEGBOT_STATE_TIMEOUT_S (7200s) bounds the CAMS side. What is
    # gone is an arbitrary inner cap, not the backstops.
    find_result = await dispatch_bill_question(
        resolved_bill_source, "find_bill_positions"
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
        # SYNC-41: a finding is model-produced, so neither its shape nor
        # any of its fields is guaranteed. A malformed entry skips itself
        # rather than raising -- a bare subscript here would abort the whole
        # bill, which is the same failure this ticket fixed below. The
        # isinstance check is not belt-and-braces: `positions` is a JSON
        # list straight out of the model, so a null or a bare string in it
        # would reach .get() and raise AttributeError, costing the bill
        # exactly what the KeyError did (caught in /pm-review).
        if not isinstance(finding, dict):
            logger.warning(
                "find_bill_positions returned a non-object finding — "
                "skipping it, keeping the rest",
                bill_openstates_id=bill_openstates_id,
                invocation_id=invocation_id,
                finding_type=type(finding).__name__,
            )
            results.append({
                "org_name": "",
                "position": "",
                "outcome": "malformed_finding",
                "position_id": None,
            })
            continue

        org_name = finding.get("org_name", "")
        position = finding.get("position", "")
        citation_url = finding.get("citation_url", "")
        citation_excerpt = finding.get("citation_excerpt", "")
        if not (org_name and position and citation_url):
            logger.warning(
                "find_bill_positions returned a finding missing a required "
                "field — skipping it, keeping the rest",
                bill_openstates_id=bill_openstates_id,
                invocation_id=invocation_id,
                present_keys=sorted(finding),
            )
            results.append({
                "org_name": org_name,
                "position": position,
                "outcome": "malformed_finding",
                "position_id": None,
            })
            continue

        results.append(
            await verify_and_store_position(
                bill_openstates_id=bill_openstates_id,
                jurisdiction=jurisdiction,
                session_code=session_code,
                version_date=version_date,
                version_note=version_note,
                gov_id=gov_id,
                bill_title=bill_title,
                invocation_id=invocation_id,
                org_name=org_name,
                position=position,
                citation_url=citation_url,
                citation_excerpt=citation_excerpt,
                find_model_name=find_model_name,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        )

    # SYNC-107: an organization this run just confirmed a position for, and that still has no
    # description (typically a stub the run created), gets its "About" content once. Only here, not in
    # verify_and_store_position: verify_org_citations shares that function and never creates
    # organizations. The first confirmed finding for the organization carries the outcome.
    if get_settings().organization_content_enabled:
        asked: set[int] = set()
        for result in results:
            organization_id = result.get("organization_id")
            if (
                result.get("verification_verdict") != "confirmed"
                or not result.get("organization_needs_content")
                or organization_id in asked
            ):
                continue
            asked.add(organization_id)
            result["about_content"] = await generate_and_store_organization_content(
                organization_id=organization_id,
                org_name=result["org_name"],
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        if asked:
            # One line per bill, so a LegBot or broker contract that fails every organization is
            # visible instead of looking like a quiet success.
            logger.info(
                "organization_about_summary",
                bill_openstates_id=bill_openstates_id,
                invocation_id=invocation_id,
                asked=len(asked),
                outcomes=dict(Counter(r["about_content"]["outcome"] for r in results if "about_content" in r)),
            )

    return results
