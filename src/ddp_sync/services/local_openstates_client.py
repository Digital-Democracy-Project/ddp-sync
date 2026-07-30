"""Local api-v3 archived-text lookup — ddp-infra's
PLAN-bill-document-provenance.md, Phase 8's "Real gap found 2026-07-29/30:
bill_source is always a live-fetched URL, never the archive Phase 1 already
saved" (design decided 2026-07-30).

ddp-open-states permanently archives the full extracted text of every bill
version it scrapes (BillVersionDocument.raw_text, live in production since
2026-07-25). Until OPEN-13 (Digital-Democracy-Project/api-v3#2, merged
2026-07-30, commit 0683aa23) shipped, nothing outside ddp-open-states itself
could read that text back out — every caller, including this pipeline, had
to resolve a live document URL and hand it to LegBot, which re-downloaded
and re-extracted the identical document from scratch on every dispatch.
OPEN-13 exposes that already-archived raw_text on api-v3's single-bill
detail route; this module is the one, narrow client that reads it.

Deliberately NOT the site-wide OPENSTATES_API_BASE cutover
PLAN-local-openstates-migration.md scopes (Pinecone re-keying, VoteBot,
universal ingestion) — that's a much bigger, explicitly out-of-scope
migration. This mirrors ddp-broker-py's own existing shape for "a small,
second client pointed at a local OpenStates instance for one purpose"
(DDPOpenStates / OpenStatesService._get_client_for_jurisdiction,
fetch/interfaces/OpenStates/openstates_service.py:1282-1296) without reusing
any of that (different repo, different auth) — just the same shape.

Caveat inherited directly from OPEN-13's own scope (api-v3/api/bills.py's
BillPagination.postprocess_includes): raw_text is only populated for a
bill's *latest* version, and only on this single-bill detail endpoint
(never the paginated /bills list/search endpoint). That's sufficient for
bill_summary/bill_pros_cons/bill_impact_analysis/bill_vote_yes_frame/
bill_vote_no_frame/bill_supporting_orgs/bill_opposing_orgs, which only need
a bill's current text — it does NOT cover bill_changelog, which needs the
*prior* version's text specifically (a further, not-yet-scoped extension).
Do not call this for changelog generation; bill_version.py's
_generate_and_ingest_changelog has its own prior-version live-fetch path,
untouched by this module.
"""

from __future__ import annotations

import httpx
import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

# Same-box call (Mac Studio, alongside CAMS/LegBot per cams_base_url's own
# convention) -- not a WireGuard hop, so this stays short. A slow/hung local
# api-v3 shouldn't stall bill_source resolution for long before falling back.
_REQUEST_TIMEOUT_SECONDS = 10.0


async def get_archived_bill_text(bill_openstates_id: str) -> str | None:
    """Look up already-archived text for a bill's latest version from the
    local api-v3 instance (ddp-open-states' Phase 1 permanent archive).

    Args:
        bill_openstates_id: bare UUID (no "ocd-bill/" prefix), matching
            ddp-broker-py's Bill.openstates_id / BillVersionSyncService's
            _extract_bill_openstates_id convention -- callers already have
            this on hand, no extra lookup needed to call this function.

    Returns:
        The archived raw_text for the bill's latest version if
        ddp-open-states has already scraped, archived, and extracted it,
        else None. None covers every "not available" case identically --
        not an archived jurisdiction (everything but FL today), an FL bill
        not yet archived, the bill not found at all, local api-v3
        unreachable, or any other request/parsing failure. Deliberately
        never raises: this is a pre-check for an optimization (skip a
        redundant re-fetch LegBot would otherwise do), not a required read
        -- any failure here must fall back to ddp-sync's existing live-URL
        bill_source path exactly as if this function didn't exist, never
        abort artifact generation.
    """
    settings = get_settings()
    if not settings.local_openstates_api_base:
        return None

    params: dict[str, str] = {"include": "versions"}
    if settings.local_openstates_api_key:
        params["apikey"] = settings.local_openstates_api_key

    url = f"{settings.local_openstates_api_base}/bills/ocd-bill/{bill_openstates_id}"

    try:
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
            resp = await client.get(url, params=params)
    except httpx.RequestError as exc:
        logger.warning(
            "Local api-v3 unreachable -- falling back to live-fetch bill_source",
            bill_openstates_id=bill_openstates_id,
            error=str(exc),
        )
        return None

    if resp.status_code == 404:
        # Bill not present in the local archive at all -- the common case
        # today (non-archived jurisdiction, or never scraped there).
        return None
    if resp.status_code >= 400:
        logger.warning(
            "Local api-v3 rejected the bill-detail read -- falling back to "
            "live-fetch bill_source",
            bill_openstates_id=bill_openstates_id,
            status_code=resp.status_code,
        )
        return None

    try:
        data = resp.json()
    except ValueError:
        logger.warning(
            "Local api-v3 returned a non-JSON response -- falling back to "
            "live-fetch bill_source",
            bill_openstates_id=bill_openstates_id,
        )
        return None

    # api-v3's own postprocess_includes attaches raw_text to at most one
    # link (the latest version's preferred PDF-over-HTML link) -- scanning
    # every version/link for the first non-empty raw_text is equivalent to
    # re-deriving "latest" ourselves, and simpler.
    for version in data.get("versions") or []:
        for link in version.get("links") or []:
            raw_text = link.get("raw_text")
            if raw_text:
                return raw_text

    return None


async def list_current_session_bill_candidates(
    jurisdiction_iso2: str,
    *,
    limit: int,
) -> list[dict]:
    """List bill identities for a jurisdiction's current session, read from
    the local api-v3 instance's paginated bill list -- the bill-enumeration
    half of ddp-infra PLAN-bill-concept-polling.md §0.4's scheduled batch
    dispatch job (get_archived_bill_text above is that same job's per-bill
    *text* lookup, called once per candidate this function returns).

    Deliberately reads the *local*, unthrottled api-v3 instance (same
    host/auth this module already needs for get_archived_bill_text) rather
    than the live, rate-limited v3.openstates.org API bill_sync.py's
    curated Webflow-bill pipeline hits -- this job has no Webflow-curated
    bill list to iterate in the first place (that's the whole point of
    building this on /explore's reach, not BillArtifact's), and there's no
    reason to spend OpenStates' 30k-calls/day quota enumerating identities
    a dependency this file already has can answer for free.

    "Current session" is resolved via OpenStatesSource.
    get_current_session_identifier() -- the same session-resolution call
    bill_sync.py's own is_current_session_async() already makes -- not a
    new session-detection mechanism.

    Args:
        jurisdiction_iso2: two-letter state code (e.g. "fl", "wa").
        limit: maximum number of candidates to return (the batch job's own
            max_bills_per_run cap, or whatever remains of it).

    Returns:
        A list of dicts, each {"gov_id", "session_code",
        "live_url_fallback"} -- gov_id is the bare UUID (ocd-bill/ prefix
        stripped, matching get_archived_bill_text's own convention);
        live_url_fallback is the bill's first recorded source URL, handed
        to dispatch_and_store_concept_statements as the live-fetch
        fallback bill_source (used only when get_archived_bill_text finds
        nothing archived for this same gov_id).

        Deliberately does NOT request raw_text here -- that's the
        paginated /bills list's own documented gap (OPEN-13/BROKER-8, see
        get_archived_bill_text's docstring); resolving each candidate's
        actual bill_source (archived vs. live) stays get_archived_bill_text's
        job, not this one's.

        Never raises: any failure (no current session resolvable, local
        api-v3 unreachable/rejecting/non-JSON) returns an empty list, same
        never-abort-the-run posture as get_archived_bill_text -- a listing
        failure for one jurisdiction should skip that jurisdiction for this
        run, not crash the whole batch.
    """
    if limit <= 0:
        return []

    # Imported locally to avoid a module-level import cycle risk (this
    # module is imported by services/*, ingestion/sources/openstates is
    # imported by pipelines/* -- both are leaves today, but this keeps the
    # dependency direction local and explicit rather than assumed).
    from ddp_sync.ingestion.sources.openstates import OpenStatesSource

    session_code = await OpenStatesSource().get_current_session_identifier(jurisdiction_iso2)
    if not session_code:
        logger.warning(
            "No current session identifier resolved -- skipping jurisdiction "
            "for this concept-statement batch run",
            jurisdiction_iso2=jurisdiction_iso2,
        )
        return []

    settings = get_settings()
    if not settings.local_openstates_api_base:
        return []

    params: dict[str, str | int] = {
        "jurisdiction": jurisdiction_iso2.lower(),
        "session": session_code,
        "per_page": limit,
    }
    if settings.local_openstates_api_key:
        params["apikey"] = settings.local_openstates_api_key

    url = f"{settings.local_openstates_api_base}/bills"

    try:
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
            resp = await client.get(url, params=params)
    except httpx.RequestError as exc:
        logger.warning(
            "Local api-v3 unreachable -- skipping jurisdiction for this "
            "concept-statement batch run",
            jurisdiction_iso2=jurisdiction_iso2,
            error=str(exc),
        )
        return []

    if resp.status_code >= 400:
        logger.warning(
            "Local api-v3 rejected the bill-list read -- skipping "
            "jurisdiction for this concept-statement batch run",
            jurisdiction_iso2=jurisdiction_iso2,
            status_code=resp.status_code,
        )
        return []

    try:
        data = resp.json()
    except ValueError:
        logger.warning(
            "Local api-v3 returned a non-JSON bill-list response -- "
            "skipping jurisdiction for this concept-statement batch run",
            jurisdiction_iso2=jurisdiction_iso2,
        )
        return []

    candidates = []
    for bill in data.get("results", []) or []:
        raw_id = bill.get("id", "") or ""
        gov_id = raw_id.rsplit("/", 1)[-1] if raw_id else ""
        if not gov_id:
            continue
        sources = bill.get("sources") or []
        live_url_fallback = sources[0].get("url", "") if sources else ""
        candidates.append({
            "gov_id": gov_id,
            "session_code": session_code,
            "live_url_fallback": live_url_fallback,
        })

    return candidates[:limit]
