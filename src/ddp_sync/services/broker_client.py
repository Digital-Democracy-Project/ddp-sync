"""Writes generated bill artifacts to ddp-broker-py — ddp-infra's
PLAN-bill-document-provenance.md Phase 8 (the "write to ddp-broker-py" half
of step 4; see ddp_sync.pipelines.bill_artifact_generation for the other
half, the Pinecone write).

Goes through ddp-broker-py's HTTP API rather than a direct DB connection —
ddp-broker-py owns BillVersion/BillArtifact (Phase 1/6); this is a writer,
not a second owner of that data, matching the plan's Phase 9 note preferring
the broker API over a direct RDS connection for exactly this kind of write.
"""

from __future__ import annotations

import httpx
import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

_REQUEST_TIMEOUT_SECONDS = 30.0


class BrokerWriteError(Exception):
    """Raised when writing a BillArtifact to ddp-broker-py fails outright
    (bad request, auth failure, or the broker being unreachable) — never
    swallowed into a fake success."""


async def write_bill_artifact(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    artifact_type: str,
    content: str,
    origin: str = "ai_generated",
    status: str = "complete",
    model_name: str | None = None,
    model_version: str | None = None,
    prompt_version: str | None = None,
    prompt_hash: str | None = None,
    generation_config: dict | None = None,
    failure_stage: str | None = None,
    failure_reason: str | None = None,
    chunk_count: int = 0,
    pinecone_synced_at: str | None = None,
) -> dict:
    """Create or update a BillArtifact row in ddp-broker-py.

    Identifies the target BillVersion by its natural key (bill + version_date
    + version_note) rather than a Django PK ddp-sync has no reason to know —
    ddp-broker-py's API resolves/creates that row itself. Idempotent: the
    same (bill_version, artifact_type, model_version, prompt_version)
    combination upserts rather than duplicates, matching BillArtifact's own
    uniqueness constraint (Phase 6) — safe to retry after a network failure.

    Returns:
        The written row as ddp-broker-py's API reports it, e.g.
        {"id": 42, "created": True}.

    Raises:
        BrokerWriteError: ddp-broker-py rejected the write or was unreachable.
    """
    settings = get_settings()
    if not settings.ddp_broker_api_base:
        raise BrokerWriteError(
            "DDP_BROKER_API_BASE is not configured — cannot write BillArtifact."
        )

    payload = {
        "bill_openstates_id": bill_openstates_id,
        "jurisdiction": jurisdiction,
        "session_code": session_code,
        "version_date": version_date,
        "version_note": version_note,
        "artifact_type": artifact_type,
        "content": content,
        "origin": origin,
        "status": status,
        "model_name": model_name,
        "model_version": model_version,
        "prompt_version": prompt_version,
        "prompt_hash": prompt_hash,
        "generation_config": generation_config,
        "failure_stage": failure_stage,
        "failure_reason": failure_reason,
        "chunk_count": chunk_count,
        "pinecone_synced_at": pinecone_synced_at,
    }
    headers = {"Authorization": f"Bearer {settings.ddp_broker_api_token}"}

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post(
                f"{settings.ddp_broker_api_base}/api/bill-artifacts/",
                headers=headers,
                json=payload,
            )
        except httpx.RequestError as exc:
            raise BrokerWriteError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerWriteError(
            f"ddp-broker-py rejected the BillArtifact write "
            f"({resp.status_code}): {resp.text}"
        )

    result = resp.json()
    logger.info(
        "BillArtifact written",
        bill_openstates_id=bill_openstates_id,
        artifact_type=artifact_type,
        artifact_id=result.get("id"),
        created=result.get("created"),
    )
    return result
