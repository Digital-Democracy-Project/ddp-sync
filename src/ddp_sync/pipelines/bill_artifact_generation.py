"""Phase 8 write path — ddp-infra's PLAN-bill-document-provenance.md.

Connects two pieces that already existed separately but were never wired
together: LegBot dispatch (services/legbot_client.py, shipped 2026-07-21,
dispatch-and-return only) and the BillArtifact ledger (ddp-broker-py Phase 6,
merged 2026-07-26). This module is the plan's step 4: "write the result to
both ddp-broker-py AND Pinecone — both writes are required for the job to
count as done, not ddp-broker-py with Pinecone as an optional follow-up."

Only bill_summary/bill_pros_cons are wired here, matching legbot_client's own
current scope — bill_changelog is still gated on LegBot's AC11a diff-format
validation, and bill_impact_analysis has no settled output schema yet (see
the plan's Phase 8 section).

Scope note: this module generates and stores ONE artifact for a caller-
supplied bill version. It does NOT implement Phase 8's step 1 ("find bill
versions that don't have the artifacts they need yet") — that requires
BillVersion rows to actually exist, which is Phase 4's job, not yet built.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog

from ddp_sync.ingestion.metadata import DocumentMetadata
from ddp_sync.ingestion.pipeline import IngestionPipeline
from ddp_sync.services.broker_client import write_bill_artifact
from ddp_sync.services.legbot_client import dispatch_bill_question

logger = structlog.get_logger()

# artifact_type -> LegBot question_type (config/legbot_questions.yaml, ddp-agents)
_ARTIFACT_TYPE_TO_QUESTION_TYPE = {
    "bill_summary": "summary_500char",
    "bill_pros_cons": "pros_cons",
}


def _content_from_answer(artifact_type: str, answer: dict) -> str:
    """Flatten LegBot's structured answer into BillArtifact.content.

    bill_summary's answer is already plain text. pros_cons' answer is
    structured (separate pros/cons lists) — stored as a JSON string rather
    than inventing a Markdown format the plan doesn't specify; a consumer
    rendering this artifact_type should json.loads() it back.
    """
    if artifact_type == "bill_summary":
        return answer["text"]
    if artifact_type == "bill_pros_cons":
        return json.dumps({"pros": answer["pros"], "cons": answer["cons"]})
    raise ValueError(f"Unsupported artifact_type for content extraction: {artifact_type}")


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
    (BrokerWriteError) — propagate to the caller rather than being
    swallowed; there's no BillArtifact row to record them on in the second
    case, and no point creating a placeholder failed row from a dispatch
    that produced no answer at all in the first.

    Returns:
        The BillArtifact row as ddp-broker-py's API reports it.
    """
    if artifact_type not in _ARTIFACT_TYPE_TO_QUESTION_TYPE:
        raise ValueError(f"Unsupported artifact_type for Phase 8 dispatch: {artifact_type}")

    question_type = _ARTIFACT_TYPE_TO_QUESTION_TYPE[artifact_type]
    dispatch_result = await dispatch_bill_question(bill_source, question_type)
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
