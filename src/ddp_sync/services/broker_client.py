"""Reads/writes bill data in ddp-broker-py — ddp-infra's
PLAN-bill-document-provenance.md Phases 4 and 8.

- write_bill_artifact: the "write to ddp-broker-py" half of Phase 8's step 4;
  see ddp_sync.pipelines.bill_artifact_generation for the other half, the
  Pinecone write.
- get_latest_bill_version / write_bill_version: the read/write pair Phase 4's
  Redis-dropping redesign uses instead of Redis for "have we seen this bill
  version before."

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


class BrokerClientError(Exception):
    """Raised when a ddp-broker-py request fails outright (bad request, auth
    failure, or the broker being unreachable) — never swallowed into a fake
    success or a silent None."""


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
        BrokerClientError: ddp-broker-py rejected the write or was unreachable.
    """
    settings = get_settings()
    if not settings.ddp_broker_api_base:
        raise BrokerClientError(
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
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
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


async def get_latest_bill_version(bill_openstates_id: str) -> dict | None:
    """Read the latest version ddp-broker-py has recorded for a bill —
    Phase 4's replacement for a Redis lookup.

    Returns:
        {"version_date", "version_note", "text_url", "media_type",
        "chunk_count"} if a version has been recorded, or None if this bill
        has never been seen before. None is the expected, common first-time
        case, not an error — callers should treat it the same way the old
        Redis-miss path did ("no prior version, so nothing to compare
        against yet").

    Raises:
        BrokerClientError: ddp-broker-py rejected the request or was
            unreachable — a real failure, distinct from "not found."
    """
    settings = get_settings()
    if not settings.ddp_broker_api_base:
        raise BrokerClientError(
            "DDP_BROKER_API_BASE is not configured — cannot read BillVersion."
        )

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.get(
                f"{settings.ddp_broker_api_base}/api/bill-versions/latest/",
                params={"bill_openstates_id": bill_openstates_id},
            )
        except httpx.RequestError as exc:
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
            f"ddp-broker-py rejected the BillVersion read "
            f"({resp.status_code}): {resp.text}"
        )

    result = resp.json()
    if not result.pop("found", False):
        return None
    return result


async def write_bill_version(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    text_url: str = "",
    media_type: str = "",
    chunk_count: int = 0,
    pinecone_ingested: bool = False,
) -> dict:
    """Record a bill version in ddp-broker-py — the write half of Phase 4's
    redesign. Without this, get_latest_bill_version would always return None
    and every bill would look "new" on every run.

    Identifies the target BillVersion by its natural key (bill + version_date
    + version_note), same as write_bill_artifact — idempotent, safe to retry.

    Returns:
        {"id": ..., "created": bool}.

    Raises:
        BrokerClientError: ddp-broker-py rejected the write (e.g. no matching
            Bill exists yet for this bill_openstates_id) or was unreachable.
    """
    settings = get_settings()
    if not settings.ddp_broker_api_base:
        raise BrokerClientError(
            "DDP_BROKER_API_BASE is not configured — cannot write BillVersion."
        )

    payload = {
        "bill_openstates_id": bill_openstates_id,
        "jurisdiction": jurisdiction,
        "session_code": session_code,
        "version_date": version_date,
        "version_note": version_note,
        "text_url": text_url,
        "media_type": media_type,
        "chunk_count": chunk_count,
        "pinecone_ingested": pinecone_ingested,
    }
    headers = {"Authorization": f"Bearer {settings.ddp_broker_api_token}"}

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post(
                f"{settings.ddp_broker_api_base}/api/bill-versions/",
                headers=headers,
                json=payload,
            )
        except httpx.RequestError as exc:
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
            f"ddp-broker-py rejected the BillVersion write "
            f"({resp.status_code}): {resp.text}"
        )

    result = resp.json()
    logger.info(
        "BillVersion written",
        bill_openstates_id=bill_openstates_id,
        version_date=version_date,
        version_note=version_note,
        version_id=result.get("id"),
        created=result.get("created"),
    )
    return result
