"""Reads/writes bill data in ddp-broker-py — ddp-infra's
PLAN-bill-document-provenance.md Phases 4 and 8, and
PLAN-bill-concept-polling.md Phase 0.

- write_bill_artifact: the "write to ddp-broker-py" half of Phase 8's step 4;
  see ddp_sync.pipelines.bill_artifact_generation for the other half, the
  Pinecone write.
- get_latest_bill_version / write_bill_version: the read/write pair Phase 4's
  Redis-dropping redesign uses instead of Redis for "have we seen this bill
  version before."
- get_concept_statement_set / create_concept_statement_set: the read/write
  pair for ConceptStatementSet (PLAN-bill-concept-polling.md §0.3, built in
  ddp-broker-py PR #247) — see ddp_sync.pipelines.concept_statement_dispatch
  for the caller.

Goes through ddp-broker-py's HTTP API rather than a direct DB connection —
ddp-broker-py owns BillVersion/BillArtifact/ConceptStatementSet; this is a
writer, not a second owner of that data, matching the plan's Phase 9 note
preferring the broker API over a direct RDS connection for exactly this
kind of write.
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
    compare_version_date: str | None = None,
    compare_version_note: str | None = None,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Create or update a BillArtifact row in ddp-broker-py.

    Identifies the target BillVersion by its natural key (bill + version_date
    + version_note) rather than a Django PK ddp-sync has no reason to know —
    ddp-broker-py's API resolves/creates that row itself. Idempotent: the
    same (bill_version, artifact_type, model_version, prompt_version)
    combination upserts rather than duplicates, matching BillArtifact's own
    uniqueness constraint (Phase 6) — safe to retry after a network failure.

    compare_version_date/compare_version_note are only ever set by
    generate_and_store_bill_changelog (artifact_type=bill_changelog) — the
    prior version this artifact's changelog diffs against. ddp-broker-py
    resolves them to a BillVersion FK with a strict lookup, scoped to the
    same bill; a non-existent, self-referential, or ambiguous pair is
    rejected with a 400, surfacing here as BrokerClientError same as any
    other rejected write — no special handling needed for it.

    broker_api_base/broker_api_token override the process-wide
    settings.ddp_broker_api_base/ddp_broker_api_token for this one call —
    added for SYNC-10's on-demand single-bill endpoint, which is the one
    caller that must target one of two different ddp-broker-py instances
    (dev vs. prod) per request rather than whichever broker this process is
    configured for. None (the default) preserves every existing caller's
    behavior unchanged.

    Returns:
        The written row as ddp-broker-py's API reports it, e.g.
        {"id": 42, "created": True}.

    Raises:
        BrokerClientError: ddp-broker-py rejected the write or was unreachable.
    """
    settings = get_settings()
    resolved_api_base = broker_api_base if broker_api_base is not None else settings.ddp_broker_api_base
    resolved_api_token = broker_api_token if broker_api_token is not None else settings.ddp_broker_api_token
    if not resolved_api_base:
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
        "compare_version_date": compare_version_date,
        "compare_version_note": compare_version_note,
    }
    headers = {"Authorization": f"Bearer {resolved_api_token}"}

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post(
                f"{resolved_api_base}/api/bill-artifacts/",
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


async def write_bill_organization_position(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    invocation_id: str,
    org_name: str,
    position: str,
    citation_url: str,
    citation_excerpt: str = "",
    find_model_name: str | None = None,
    find_model_version: str | None = None,
    find_prompt_version: str | None = None,
    find_prompt_hash: str | None = None,
    find_generation_config: dict | None = None,
    find_code_commit_sha: str | None = None,
    verification_verdict: str = "pending",
    verification_insufficient_information: bool = False,
    verification_content_incomplete: bool = False,
    verification_explanation: str = "",
    verify_model_name: str | None = None,
    verify_model_version: str | None = None,
    verify_prompt_version: str | None = None,
    verify_prompt_hash: str | None = None,
    verify_generation_config: dict | None = None,
    verify_code_commit_sha: str | None = None,
    status: str = "complete",
    failure_stage: str | None = None,
    failure_reason: str | None = None,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Create a BillOrganizationPosition row in ddp-broker-py — ddp-infra's
    PLAN-bill-document-provenance.md Phase 8, "Organization Position
    Research" (approved 2026-08-01 after 4 rounds of /pm-review).

    Identifies the target BillVersion by its natural key, same as
    write_bill_artifact. Unlike write_bill_artifact, this always creates a
    new row — never an upsert. Every dispatch is a new finding; a history
    of individual findings, not a single current-value slot.

    broker_api_base/broker_api_token: optional per-call override, see
    get_bill_artifacts' own docstring for why (SYNC-15).

    Returns:
        {"id": <int>} — the created row's id.

    Raises:
        BrokerClientError: ddp-broker-py rejected the write or was
            unreachable.
    """
    settings = get_settings()
    resolved_api_base = broker_api_base if broker_api_base is not None else settings.ddp_broker_api_base
    resolved_api_token = broker_api_token if broker_api_token is not None else settings.ddp_broker_api_token
    if not resolved_api_base:
        raise BrokerClientError(
            "DDP_BROKER_API_BASE is not configured — cannot write BillOrganizationPosition."
        )

    payload = {
        "bill_openstates_id": bill_openstates_id,
        "jurisdiction": jurisdiction,
        "session_code": session_code,
        "version_date": version_date,
        "version_note": version_note,
        "invocation_id": invocation_id,
        "org_name": org_name,
        "position": position,
        "citation_url": citation_url,
        "citation_excerpt": citation_excerpt,
        "find_model_name": find_model_name,
        "find_model_version": find_model_version,
        "find_prompt_version": find_prompt_version,
        "find_prompt_hash": find_prompt_hash,
        "find_generation_config": find_generation_config,
        "find_code_commit_sha": find_code_commit_sha,
        "verification_verdict": verification_verdict,
        "verification_insufficient_information": verification_insufficient_information,
        "verification_content_incomplete": verification_content_incomplete,
        "verification_explanation": verification_explanation,
        "verify_model_name": verify_model_name,
        "verify_model_version": verify_model_version,
        "verify_prompt_version": verify_prompt_version,
        "verify_prompt_hash": verify_prompt_hash,
        "verify_generation_config": verify_generation_config,
        "verify_code_commit_sha": verify_code_commit_sha,
        "status": status,
        "failure_stage": failure_stage,
        "failure_reason": failure_reason,
    }
    headers = {"Authorization": f"Bearer {resolved_api_token}"}

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post(
                f"{resolved_api_base}/api/bill-organization-positions/",
                headers=headers,
                json=payload,
            )
        except httpx.RequestError as exc:
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
            f"ddp-broker-py rejected the BillOrganizationPosition write "
            f"({resp.status_code}): {resp.text}"
        )

    result = resp.json()
    logger.info(
        "BillOrganizationPosition written",
        bill_openstates_id=bill_openstates_id,
        org_name=org_name,
        verification_verdict=verification_verdict,
        position_id=result.get("id"),
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


async def get_concept_statement_set(
    *,
    gov_id: str,
    jurisdiction_iso2: str,
    session_code: str,
) -> dict | None:
    """Read the current *published* ConceptStatementSet for a bill, if any
    (ddp-infra PLAN-bill-concept-polling.md §1.1's public read endpoint,
    GET /api/concept-statements/ — unauthenticated, resolves
    ConceptStatementSet.objects.current(...)).

    Returns:
        None when no *published* set exists yet for this bill identity.
        This does NOT mean "never dispatched" — a set can also exist in
        `pending` or `rejected` status, neither of which this endpoint
        ever surfaces (§0.3's public-read rule: only published sets are
        ever returned publicly). See
        ddp_sync.pipelines.concept_statement_dispatch's own docstring for
        what that means for the batch job's dedup logic. Otherwise, the
        resolved set's fields as ddp-broker-py's API reports them
        (including its own `id`, `statements`, vote tallies, etc).

    Raises:
        BrokerClientError: ddp-broker-py rejected the request or was
            unreachable — a real failure, distinct from "not found."
    """
    settings = get_settings()
    if not settings.ddp_broker_api_base:
        raise BrokerClientError(
            "DDP_BROKER_API_BASE is not configured — cannot read ConceptStatementSet."
        )

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.get(
                f"{settings.ddp_broker_api_base}/api/concept-statements/",
                params={
                    "gov_id": gov_id,
                    "jurisdiction": jurisdiction_iso2,
                    "session": session_code,
                },
            )
        except httpx.RequestError as exc:
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
            f"ddp-broker-py rejected the ConceptStatementSet read "
            f"({resp.status_code}): {resp.text}"
        )

    result = resp.json()
    if not result.pop("found", False):
        return None
    return result


async def create_concept_statement_set(
    *,
    gov_id: str,
    jurisdiction_iso2: str,
    session_code: str,
    statements: list[str],
    source_document_url: str = "",
    model_name: str | None = None,
) -> dict:
    """Create a new ConceptStatementSet row (ddp-infra
    PLAN-bill-concept-polling.md §0.3/§0.4) — POST
    /api/concept-statement-sets/, authenticated the same way
    write_bill_artifact already is (Bearer ddp_broker_api_token).

    Unlike write_bill_artifact, this never upserts. ConceptStatementSet is
    immutable-once-created by design (§0.3: "regeneration creates a new
    row, it never edits an existing one") — every call to this function
    creates a brand-new row, always landing in `status="pending"`
    (ddp-broker-py's own default), regardless of whether a set already
    exists for this bill identity. Whether a new row is actually warranted
    is the *caller's* decision (see
    ddp_sync.pipelines.concept_statement_dispatch's dedup note) — this
    function has no opinion and performs no existence check of its own.

    Returns:
        The created row as ddp-broker-py's API reports it, e.g.
        {"id": 12, "gov_id": ..., "status": "pending", "generated_at": ...}.

    Raises:
        BrokerClientError: ddp-broker-py rejected the write or was
            unreachable.
    """
    settings = get_settings()
    if not settings.ddp_broker_api_base:
        raise BrokerClientError(
            "DDP_BROKER_API_BASE is not configured — cannot write ConceptStatementSet."
        )

    payload = {
        "gov_id": gov_id,
        "jurisdiction_iso2": jurisdiction_iso2,
        "session_code": session_code,
        "statements": statements,
        "source_document_url": source_document_url,
        "model_name": model_name,
    }
    headers = {"Authorization": f"Bearer {settings.ddp_broker_api_token}"}

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post(
                f"{settings.ddp_broker_api_base}/api/concept-statement-sets/",
                headers=headers,
                json=payload,
            )
        except httpx.RequestError as exc:
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
            f"ddp-broker-py rejected the ConceptStatementSet write "
            f"({resp.status_code}): {resp.text}"
        )

    result = resp.json()
    logger.info(
        "ConceptStatementSet written",
        gov_id=gov_id,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
        set_id=result.get("id"),
    )
    return result


async def get_bill_artifacts(
    *,
    jurisdiction: str,
    session_code: str,
    gov_id: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict | None:
    """Read BillArtifact coverage (any status, including failed) for a bill —
    PLAN-bill-document-provenance.md's "Step 1, scoped version" (approved
    2026-08-01 after 3 rounds of /pm-review).

    Calls GET /api/bill-artifacts/status/, NOT the public .../current/
    endpoint — that one only ever returns status=complete rows, so it
    can't tell "has a failed row" apart from "has no row at all" (a real
    gap found implementing this design, not anticipated by it as written).
    Service-token-authenticated, mirroring the auth split between this
    app's public reads and service-only writes/status checks.

    broker_api_base/broker_api_token: optional per-call override, same shape
    as write_bill_artifact's own (SYNC-10) -- None (the default) preserves
    existing behavior for every caller that doesn't need per-call broker
    routing (SYNC-9's batch pipeline); SYNC-15's single-bill full-run
    endpoint passes these through so its coverage check lands on the same
    dev/prod broker instance its writes do.

    Returns:
        None if no Bill/BillVersion exists for this identity yet (the
        {"found": false} case) -- mirrors get_latest_bill_version's/
        get_concept_statement_set's exact pattern. Otherwise
        {"bill_version_id": int, "artifacts": {artifact_type: {"status": str}}}
        -- every artifact_type with any row at all for this bill's current
        version, keyed by type, regardless of status.

    Raises:
        BrokerClientError: ddp-broker-py rejected the request or was
            unreachable — a real failure, distinct from "not found."
    """
    settings = get_settings()
    resolved_api_base = broker_api_base if broker_api_base is not None else settings.ddp_broker_api_base
    resolved_api_token = broker_api_token if broker_api_token is not None else settings.ddp_broker_api_token
    if not resolved_api_base:
        raise BrokerClientError(
            "DDP_BROKER_API_BASE is not configured — cannot read BillArtifact status."
        )

    headers = {"Authorization": f"Bearer {resolved_api_token}"}

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.get(
                f"{resolved_api_base}/api/bill-artifacts/status/",
                headers=headers,
                params={"jurisdiction": jurisdiction, "session": session_code, "gov_id": gov_id},
            )
        except httpx.RequestError as exc:
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
            f"ddp-broker-py rejected the BillArtifact status read "
            f"({resp.status_code}): {resp.text}"
        )

    result = resp.json()
    if not result.pop("found", False):
        return None
    return result


async def get_bill_organization_positions_status(
    *,
    bill_openstates_id: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Check whether a bill's organization positions have been researched at
    all yet — PLAN-bill-document-provenance.md's "Step 1, scoped version"
    (approved 2026-08-01 after 3 rounds of /pm-review).

    Calls GET /api/bill-organization-positions/status/. Keyed by
    bill_openstates_id alone (globally unique), not the
    (jurisdiction, session, gov_id) triple most other reads in this module
    use — this status check isn't BillVersion-scoped, see
    BillOrganizationResearchRun's own model docstring for why.

    broker_api_base/broker_api_token: optional per-call override, see
    get_bill_artifacts' own docstring for why (SYNC-15).

    Returns:
        {"has_rows": bool, "row_count": int}. has_rows is true if this bill
        has ever had a completed find_bill_positions run (regardless of how
        many organizations it found) OR already has real
        BillOrganizationPosition rows from before this tracking existed.
        There's no "not found" case here, unlike get_bill_artifacts/
        get_latest_bill_version — a bill with no research at all simply
        reads {"has_rows": False, "row_count": 0}.

    Raises:
        BrokerClientError: ddp-broker-py rejected the request or was
            unreachable.
    """
    settings = get_settings()
    resolved_api_base = broker_api_base if broker_api_base is not None else settings.ddp_broker_api_base
    resolved_api_token = broker_api_token if broker_api_token is not None else settings.ddp_broker_api_token
    if not resolved_api_base:
        raise BrokerClientError(
            "DDP_BROKER_API_BASE is not configured — cannot read organization-research status."
        )

    headers = {"Authorization": f"Bearer {resolved_api_token}"}

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.get(
                f"{resolved_api_base}/api/bill-organization-positions/status/",
                headers=headers,
                params={"bill_openstates_id": bill_openstates_id},
            )
        except httpx.RequestError as exc:
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
            f"ddp-broker-py rejected the organization-research status read "
            f"({resp.status_code}): {resp.text}"
        )

    return resp.json()


async def write_bill_organization_research_run(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    invocation_id: str,
    positions_found_count: int,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Record that organization-position research was attempted for a bill —
    PLAN-bill-document-provenance.md's "Step 1, scoped version" (approved
    2026-08-01 after 3 rounds of /pm-review).

    Called by generate_and_store_bill_organization_positions right after
    find_bill_positions returns a real answer, whether it reports zero
    organizations or many — never called if that dispatch itself fails or
    times out, so a transient failure is never mistaken for "researched,
    found nothing."

    broker_api_base/broker_api_token: optional per-call override, see
    get_bill_artifacts' own docstring for why (SYNC-15).

    Returns:
        {"id": <int>} — the created row's id.

    Raises:
        BrokerClientError: ddp-broker-py rejected the write or was
            unreachable.
    """
    settings = get_settings()
    resolved_api_base = broker_api_base if broker_api_base is not None else settings.ddp_broker_api_base
    resolved_api_token = broker_api_token if broker_api_token is not None else settings.ddp_broker_api_token
    if not resolved_api_base:
        raise BrokerClientError(
            "DDP_BROKER_API_BASE is not configured — cannot write BillOrganizationResearchRun."
        )

    payload = {
        "bill_openstates_id": bill_openstates_id,
        "jurisdiction": jurisdiction,
        "session_code": session_code,
        "invocation_id": invocation_id,
        "positions_found_count": positions_found_count,
    }
    headers = {"Authorization": f"Bearer {resolved_api_token}"}

    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post(
                f"{resolved_api_base}/api/bill-organization-research-runs/",
                headers=headers,
                json=payload,
            )
        except httpx.RequestError as exc:
            raise BrokerClientError(f"ddp-broker-py unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise BrokerClientError(
            f"ddp-broker-py rejected the BillOrganizationResearchRun write "
            f"({resp.status_code}): {resp.text}"
        )

    result = resp.json()
    logger.info(
        "BillOrganizationResearchRun written",
        bill_openstates_id=bill_openstates_id,
        run_id=result.get("id"),
        positions_found_count=positions_found_count,
    )
    return result
