"""Phase 8 write path — ddp-infra's PLAN-bill-document-provenance.md.

Connects two pieces that already existed separately but were never wired
together: LegBot dispatch (services/legbot_client.py, shipped 2026-07-21,
dispatch-and-return only) and the BillArtifact ledger (ddp-broker-py Phase 6,
merged 2026-07-26). This module is the plan's step 4: "write the result to
both ddp-broker-py AND Pinecone — both writes are required for the job to
count as done, not ddp-broker-py with Pinecone as an optional follow-up."

bill_summary/bill_pros_cons/bill_vote_yes_frame/bill_vote_no_frame/
bill_supporting_orgs/bill_opposing_orgs/bill_impact_analysis are wired
here — bill_changelog is still gated on LegBot's AC11a diff-format
validation (and has its own dispatch path, dispatch_bill_changelog, not
this one).

Scope note: this module generates and stores ONE artifact for a caller-
supplied bill version. It does NOT implement Phase 8's step 1 ("find bill
versions that don't have the artifacts they need yet") — that requires
BillVersion rows to actually exist, which is Phase 4's job, not yet built.

bill_source resolution (added 2026-07-30, ddp-infra's "Real gap found
2026-07-29/30" design): before dispatching to LegBot, check whether
ddp-open-states already has archived text for this bill's latest version
(local_openstates_client.get_archived_bill_text, OPEN-13) and use it
directly if present -- skipping the live-fetch-and-re-extract LegBot would
otherwise do for a document ddp-open-states already extracted once. Falls
back to the caller-supplied bill_source (a live URL) unchanged when no
archived text is available, exactly as before this change. Scoped to the
7 single-version artifact types this module already handles -- bill_source
here is always resolved for a bill's *current* version, so this never
applies to bill_changelog (dispatch_bill_changelog, bill_version.py's own
path), which needs the *prior* version's text specifically and isn't
touched by this change.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog

from ddp_sync.ingestion.metadata import DocumentMetadata
from ddp_sync.ingestion.pipeline import IngestionPipeline
from ddp_sync.services.broker_client import write_bill_artifact
from ddp_sync.services.legbot_client import dispatch_bill_question
from ddp_sync.services.local_openstates_client import get_archived_bill_text

logger = structlog.get_logger()

# artifact_type -> LegBot question_type (config/legbot_questions.yaml, ddp-agents)
_ARTIFACT_TYPE_TO_QUESTION_TYPE = {
    "bill_summary": "summary_500char",
    "bill_pros_cons": "pros_cons",
    "bill_vote_yes_frame": "vote_yes_frame",
    "bill_vote_no_frame": "vote_no_frame",
    "bill_supporting_orgs": "supporting_orgs",
    "bill_opposing_orgs": "opposing_orgs",
    "bill_impact_analysis": "impact_analysis",
}

# artifact_types whose answer is already plain text under an answer["text"]
# key (config/legbot_questions.yaml's output_shape) — flattened identically.
_TEXT_ANSWER_ARTIFACT_TYPES = {"bill_summary", "bill_vote_yes_frame", "bill_vote_no_frame"}

# artifact_types whose answer is a single list under answer["org_types"]
# ([{type, reason}, ...], config/legbot_questions.yaml) — flattened identically.
_ORG_TYPES_ANSWER_ARTIFACT_TYPES = {"bill_supporting_orgs", "bill_opposing_orgs"}


def _content_from_answer(artifact_type: str, answer: dict) -> str:
    """Flatten LegBot's structured answer into BillArtifact.content.

    bill_summary/bill_vote_yes_frame/bill_vote_no_frame's answer is already
    plain text. pros_cons/supporting_orgs/opposing_orgs/bill_impact_analysis'
    answers are structured — stored as a JSON string rather than inventing a
    Markdown format the plan doesn't specify; a consumer rendering these
    artifact types should json.loads() the content back.
    """
    if artifact_type in _TEXT_ANSWER_ARTIFACT_TYPES:
        return answer["text"]
    if artifact_type == "bill_pros_cons":
        return json.dumps({"pros": answer["pros"], "cons": answer["cons"]})
    if artifact_type in _ORG_TYPES_ANSWER_ARTIFACT_TYPES:
        return json.dumps({"org_types": answer["org_types"]})
    if artifact_type == "bill_impact_analysis":
        return json.dumps({
            "affected_parties": answer["affected_parties"],
            "fiscal_or_programmatic_effects": answer["fiscal_or_programmatic_effects"],
            "effective_date": answer["effective_date"],
        })
    raise ValueError(f"Unsupported artifact_type for content extraction: {artifact_type}")


async def _resolve_bill_source(bill_openstates_id: str, live_url_fallback: str) -> str:
    """Prefer ddp-open-states' already-archived text over a live-fetch URL.

    Checks the local api-v3 instance for already-archived, already-extracted
    text for this bill's latest version (OPEN-13). If present, LegBot is
    handed that text directly instead of a URL it would otherwise have to
    download and extract itself. Falls back to live_url_fallback --
    ddp-sync's existing behavior -- when no archived text is available,
    exactly as if this function didn't exist.
    """
    archived_text = await get_archived_bill_text(bill_openstates_id)
    if archived_text:
        logger.info(
            "Using ddp-open-states' archived bill text -- skipping LegBot live fetch",
            bill_openstates_id=bill_openstates_id,
        )
        return archived_text
    return live_url_fallback


async def generate_and_store_bill_artifact(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    bill_source: str,
    artifact_type: str,
) -> dict:
    """Dispatch to LegBot, then persist the result to ddp-broker-py and Pinecone.

    A Pinecone failure does NOT abort the write — it's recorded as
    pinecone_synced_at=None on the BillArtifact row instead (Phase 6's
    "stale for search" state), so a health-monitor/re-sync pass can retry
    without regenerating content. A LegBot answer flagged
    insufficient_information is recorded as a failed row (failure_stage=
    generation), not silently dropped, per Phase 6's failure-tracking design.

    Genuinely unrecoverable failures — LegBot unreachable/timed out
    (LegBotDispatchError), or ddp-broker-py rejecting/unreachable
    (BrokerClientError) — propagate to the caller rather than being
    swallowed; there's no BillArtifact row to record them on in the second
    case, and no point creating a placeholder failed row from a dispatch
    that produced no answer at all in the first.

    Returns:
        The BillArtifact row as ddp-broker-py's API reports it.
    """
    if artifact_type not in _ARTIFACT_TYPE_TO_QUESTION_TYPE:
        raise ValueError(f"Unsupported artifact_type for Phase 8 dispatch: {artifact_type}")

    question_type = _ARTIFACT_TYPE_TO_QUESTION_TYPE[artifact_type]
    resolved_bill_source = await _resolve_bill_source(bill_openstates_id, bill_source)
    dispatch_result = await dispatch_bill_question(resolved_bill_source, question_type)
    answer = dispatch_result["answer"]
    model_name = dispatch_result.get("backend")

    if answer.get("insufficient_information"):
        logger.info(
            "LegBot reported insufficient_information — recording a failed artifact",
            bill_openstates_id=bill_openstates_id,
            artifact_type=artifact_type,
        )
        return await write_bill_artifact(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            version_date=version_date,
            version_note=version_note,
            artifact_type=artifact_type,
            content="",
            status="failed",
            failure_stage="generation",
            failure_reason="insufficient_information",
            model_name=model_name,
        )

    content = _content_from_answer(artifact_type, answer)

    pinecone_synced_at = None
    try:
        pipeline = IngestionPipeline()
        metadata = DocumentMetadata(
            document_id=f"bill-artifact-{artifact_type}-{bill_openstates_id}-{version_date}",
            document_type=f"bill-artifact-{artifact_type}",
            source="Digital Democracy Project",
            jurisdiction=jurisdiction,
            bill_id=bill_openstates_id,
            extra={"session_code": session_code, "version_note": version_note},
        )
        await pipeline.ingest_document(content=content, metadata=metadata, skip_duplicates=False)
        pinecone_synced_at = datetime.now(timezone.utc).isoformat()
    except Exception:
        logger.exception(
            "Pinecone ingest failed for bill artifact — writing to ddp-broker-py "
            "anyway (pinecone_synced_at stays null, so it's visible as stale "
            "and re-syncable rather than silently missing)",
            bill_openstates_id=bill_openstates_id,
            artifact_type=artifact_type,
        )

    return await write_bill_artifact(
        bill_openstates_id=bill_openstates_id,
        jurisdiction=jurisdiction,
        session_code=session_code,
        version_date=version_date,
        version_note=version_note,
        artifact_type=artifact_type,
        content=content,
        status="complete",
        model_name=model_name,
        pinecone_synced_at=pinecone_synced_at,
    )
