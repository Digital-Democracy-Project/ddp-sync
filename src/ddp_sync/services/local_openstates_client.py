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

SYNC-6 (2026-08-11) added a second, narrower reuse of that same shape:
BillSyncService.fetch_bill_from_openstates() (pipelines/bill_sync.py) now
also routes to settings.local_openstates_api_base/local_openstates_api_key
directly (not through this module — different endpoint shape,
/bills/{jurisdiction}/{session}/{bill_id} vs this module's
/bills/ocd-bill/{uuid}), but only for jurisdictions listed in
settings.ddp_openstates_jurisdictions, mirroring
_get_client_for_jurisdiction() itself rather than this module's narrower
archived-text lookup. Still not the universal cutover this docstring
originally disclaimed — just one more jurisdiction-scoped caller of the
same local replica.

Caveat inherited directly from OPEN-13's own scope (api-v3/api/bills.py's
BillPagination.postprocess_includes): raw_text is only populated for a
bill's *latest* version, and only on this single-bill detail endpoint
(never the paginated /bills list/search endpoint). That's sufficient for
bill_summary/bill_pros_cons/bill_impact_analysis/bill_vote_yes_frame/
bill_vote_no_frame/bill_supporting_orgs/bill_opposing_orgs, which only need
a bill's current text — use get_archived_bill_text for those.

bill_changelog needs the *prior* version's text plus a diff, not just
get_archived_bill_text's latest-only raw_text — use
get_archived_changelog_inputs below instead, which reads api-v3's
extended response (ddp-infra "excellent news" fix, 2026-07-30):
diff_from_previous_version is precomputed and permanently stored by
openstates-core's archive_bill_versions() at scrape time (2026-07-20), and
api-v3 now surfaces it, plus the immediately-previous version's own
raw_text, on the same single-bill detail endpoint.
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

    # api-v3's postprocess_includes attaches raw_text to both the latest version's
    # preferred link AND (since the bill_changelog fix, 2026-07-30) the version
    # immediately before it -- so this can no longer just return the first non-empty
    # raw_text found across any version, that risks returning the *previous* version's
    # text instead of latest's. SYNC-16: api-v3 now resolves and orders "latest"/
    # "previous" itself via openstates-core's audited stage classifier (OPEN-92) and
    # returns `versions` with the correctly-ordered entries last -- take versions[-1]
    # directly rather than re-deriving order here via a naive (date, note) sort (the
    # naive sort silently agreed with api-v3's own former bug, OPEN-92's own
    # discovery -- neither side had actually verified the other).
    versions = data.get("versions") or []
    if not versions:
        return None
    latest = versions[-1]
    for link in latest.get("links") or []:
        raw_text = link.get("raw_text")
        if raw_text:
            return raw_text

    return None


async def get_current_version_identity(bill_openstates_id: str) -> dict | None:
    """Resolve a bill's current (latest) version_date/version_note plus its
    title, from the same single-bill detail endpoint get_archived_bill_text
    already reads -- ddp-infra's "Step 1, scoped version" (approved
    2026-08-01 after 3 rounds of /pm-review).

    A real gap found building Step 1's orchestrator, not enumerated by the
    reviewed design as written: list_current_session_bill_candidates
    returns gov_id/bill_openstates_id/session_code/live_url_fallback, but
    generate_and_store_bill_artifact and generate_and_store_bill_changelog
    both require the current version_date/version_note as an explicit
    argument (the natural key they write against), and
    generate_and_store_bill_organization_positions needs bill_title for its
    claim template -- none of which the lister returns. Rather than
    duplicate get_archived_bill_text's own "take versions[-1] as latest"
    logic a third time in this file, this is a small, separate function
    reusing the identical fetch, computing the same `latest` this module
    already computes twice elsewhere, just exposing different fields off it.

    Returns:
        {"version_date": str, "version_note": str, "bill_title": str,
        "chamber_classification": str, "jurisdiction_classification": str} if
        the bill has at least one version archived, else None -- same
        never-abort posture as get_archived_bill_text: a caller that can't
        resolve this for one bill should skip that bill for this run, not
        abort the whole batch.

        chamber_classification/jurisdiction_classification (SYNC-21,
        PLAN-local-openstates-migration.md §3.6): additive keys, read off the
        same already-parsed `data` this function's other fields already come
        from (`data["from_organization"]["classification"]` /
        `data["jurisdiction"]["classification"]` -- the same two fields
        ddp-broker-py's OpenStatesService._get_chamber_for_bill reads off its
        own bill_data object) -- no new network call, no change to any
        existing key, so every existing caller is unaffected.
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
            "Local api-v3 unreachable -- cannot resolve current version identity",
            bill_openstates_id=bill_openstates_id,
            error=str(exc),
        )
        return None

    if resp.status_code >= 400:
        logger.warning(
            "Local api-v3 rejected the bill-detail read -- cannot resolve "
            "current version identity",
            bill_openstates_id=bill_openstates_id,
            status_code=resp.status_code,
        )
        return None

    try:
        data = resp.json()
    except ValueError:
        logger.warning(
            "Local api-v3 returned a non-JSON response -- cannot resolve "
            "current version identity",
            bill_openstates_id=bill_openstates_id,
        )
        return None

    versions = data.get("versions") or []
    if not versions:
        return None
    # SYNC-16: api-v3 orders `versions` with the correctly-resolved latest entry last
    # (OPEN-92) -- take it directly rather than re-deriving order here.
    latest = versions[-1]

    return {
        "version_date": latest.get("date", ""),
        "version_note": latest.get("note", ""),
        "bill_title": data.get("title", ""),
        "chamber_classification": (data.get("from_organization") or {}).get("classification", ""),
        "jurisdiction_classification": (data.get("jurisdiction") or {}).get("classification", ""),
    }


async def get_archived_changelog_inputs(bill_openstates_id: str) -> dict | None:
    """Look up already-archived old_bill_source + diff_source for bill_changelog, from the
    local api-v3 instance (ddp-open-states' Phase 1 permanent archive).

    Reads the same single-bill detail endpoint get_archived_bill_text does, but pulls the two
    fields bill_changelog specifically needs instead of just latest's raw_text:
    - old_bill_source: the version immediately before latest's own raw_text.
    - diff_source: latest's diff_from_previous_version -- a difflib.unified_diff() of latest's
      text against that same immediately-previous version's text, precomputed and permanently
      stored by openstates-core's archive_bill_versions() at scrape time (2026-07-20), not
      re-derived here.

    Args:
        bill_openstates_id: bare UUID (no "ocd-bill/" prefix), same convention as
            get_archived_bill_text.

    Returns:
        {"old_bill_source": str, "diff_source": str, "old_version_date": str,
        "old_version_note": str, "latest_version_date": str, "latest_version_note": str} if
        old_bill_source and diff_source are both archived and non-empty, else None -- covering
        every "not available" case identically (fewer than two versions, latest not archived,
        previous not archived, no diff computed yet e.g. previous was the first version ever
        archived, local api-v3 unreachable/rejecting/non-JSON, or the bill not found at all).
        The four *_date/*_note fields are the same values api-v3 already resolved as
        "latest"/"previous" (ddp-infra's bill_changelog design, added 2026-08-01; SYNC-16
        stopped re-deriving that resolution on this side) -- surfaced so a caller can
        verify it's generating a changelog for the version api-v3 actually resolved as
        latest, not a stale or different one. Deliberately never raises, same posture as
        get_archived_bill_text:
        this is a pre-check for an optimization, not a required read -- any failure here must
        fall back to bill_version.py's existing live-refetch-and-diff path exactly as if this
        function didn't exist, never abort changelog generation.
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
            "Local api-v3 unreachable -- falling back to live-refetch changelog inputs",
            bill_openstates_id=bill_openstates_id,
            error=str(exc),
        )
        return None

    if resp.status_code >= 400:
        # Covers 404 (bill not in the local archive at all, the common case today) and any
        # other rejection identically -- both fall back the same way.
        return None

    try:
        data = resp.json()
    except ValueError:
        logger.warning(
            "Local api-v3 returned a non-JSON response -- falling back to live-refetch "
            "changelog inputs",
            bill_openstates_id=bill_openstates_id,
        )
        return None

    versions = data.get("versions") or []
    if len(versions) < 2:
        return None

    # SYNC-16: api-v3 resolves and orders "latest"/"previous" itself via
    # openstates-core's audited stage classifier (OPEN-92) and returns `versions`
    # with those two entries last, in order -- take them directly rather than
    # re-deriving order here via a naive (date, note) sort.
    latest, previous = versions[-1], versions[-2]

    diff_source = latest.get("diff_from_previous_version")
    if not diff_source:
        return None

    old_bill_source = None
    for link in previous.get("links") or []:
        raw_text = link.get("raw_text")
        if raw_text:
            old_bill_source = raw_text
            break
    if not old_bill_source:
        return None

    return {
        "old_bill_source": old_bill_source,
        "diff_source": diff_source,
        "old_version_date": previous.get("date", ""),
        "old_version_note": previous.get("note", ""),
        "latest_version_date": latest.get("date", ""),
        "latest_version_note": latest.get("note", ""),
    }


# api-v3's own enforced ceiling on a single page's size (confirmed live,
# 2026-08-01: per_page=50 rejected with "invalid per_page, must be in
# [1, 20]"). Used to cap the actual per-request page size regardless of
# what the caller's own `limit` is -- `limit` is a total-candidates cap,
# not a page size (see the pagination fix below).
_API_V3_MAX_PER_PAGE = 20


async def list_current_session_bill_candidates(
    jurisdiction_iso2: str,
    *,
    session_code: str | None = None,
    limit: int,
) -> list[dict]:
    """List bill identities for a jurisdiction's session, read from the
    local api-v3 instance's paginated bill list -- the bill-enumeration half
    of ddp-infra PLAN-bill-concept-polling.md §0.4's scheduled batch
    dispatch job (get_archived_bill_text above is that same job's per-bill
    *text* lookup, called once per candidate this function returns), and
    also the bill-enumeration half of "Step 1, scoped version" (session-
    targeted batch runner, approved 2026-08-01 after 3 rounds of
    /pm-review).

    Deliberately reads the *local*, unthrottled api-v3 instance (same
    host/auth this module already needs for get_archived_bill_text) rather
    than the live, rate-limited v3.openstates.org API bill_sync.py's
    curated Webflow-bill pipeline hits -- this job has no Webflow-curated
    bill list to iterate in the first place (that's the whole point of
    building this on /explore's reach, not BillArtifact's), and there's no
    reason to spend OpenStates' 30k-calls/day quota enumerating identities
    a dependency this file already has can answer for free.

    Args:
        jurisdiction_iso2: two-letter state code (e.g. "fl", "wa"; case
            doesn't matter here -- normalized to uppercase before querying,
            see the real bug fix below).
        session_code: an explicit session identifier (e.g. "2026F"), for
            Step 1's session-targeted use. When omitted (the original,
            still-supported caller in concept_statement_dispatch.py),
            "current session" is resolved via
            OpenStatesSource.get_current_session_identifier() -- the same
            session-resolution call bill_sync.py's own
            is_current_session_async() already makes, not a new
            session-detection mechanism. Fully backward-compatible: the
            existing caller passes nothing, behavior is unchanged.
        limit: maximum number of candidates to return, across however many
            pages that takes -- NOT a single request's page size (see the
            real pagination bug fixed below).

    Returns:
        A list of dicts, each {"gov_id", "bill_openstates_id", "session_code",
        "live_url_fallback"}. **`gov_id` and `bill_openstates_id` are two
        different identities, not interchangeable** -- fixed 2026-07-30
        after live testing found every real dispatch through this path
        failed `ConceptStatementSet.gov_id`'s `max_length=20` (a bare UUID
        is 36 characters). `gov_id` is the bill's short public identifier
        (`Bill.identifier`, e.g. "SJR 2F"), matching every other `gov_id` in
        this plan (`BillPromotionRequest`, `ConceptStatementSet`, the public
        read/vote API); `bill_openstates_id` is the bare UUID (`ocd-bill/`
        prefix stripped), needed only for `get_archived_bill_text`'s archive
        lookup, which is keyed on that UUID, not the identifier.
        live_url_fallback is the bill's first recorded source URL, handed to
        dispatch_and_store_concept_statements as the live-fetch fallback
        bill_source (used only when get_archived_bill_text finds nothing
        archived for this bill).

        Deliberately does NOT request raw_text here -- that's the
        paginated /bills list's own documented gap (OPEN-13/BROKER-8, see
        get_archived_bill_text's docstring); resolving each candidate's
        actual bill_source (archived vs. live) stays get_archived_bill_text's
        job, not this one's.

        Deduplicated by bill_openstates_id (SYNC-23): a live run found the
        same bill appearing twice in one call's results, traced to this
        function's own pagination sending a shrinking per_page across pages
        (fixed below) -- this dedup is a second, independent safety net
        against api-v3's default sort having no secondary tiebreaker key,
        which could still land a tied-sort-value bill on two pages even with
        per_page held fixed.

        Never raises: any failure (no current session resolvable, local
        api-v3 unreachable/rejecting/non-JSON) returns an empty list, same
        never-abort-the-run posture as get_archived_bill_text -- a listing
        failure for one jurisdiction should skip that jurisdiction for this
        run, not crash the whole batch.
    """
    if limit <= 0:
        return []

    resolved_session_code = session_code
    if resolved_session_code is None:
        # Imported locally to avoid a module-level import cycle risk (this
        # module is imported by services/*, ingestion/sources/openstates is
        # imported by pipelines/* -- both are leaves today, but this keeps
        # the dependency direction local and explicit rather than assumed).
        from ddp_sync.ingestion.sources.openstates import OpenStatesSource

        resolved_session_code = await OpenStatesSource().get_current_session_identifier(
            jurisdiction_iso2
        )
        if not resolved_session_code:
            logger.warning(
                "No current session identifier resolved -- skipping jurisdiction "
                "for this batch run",
                jurisdiction_iso2=jurisdiction_iso2,
            )
            return []

    settings = get_settings()
    if not settings.local_openstates_api_base:
        return []

    url = f"{settings.local_openstates_api_base}/bills"

    # Real, separate bug found live while building Step 1, 2026-08-01: this
    # sent jurisdiction_iso2.lower() ("fl"), but api-v3's own jurisdiction
    # filter (a 2-letter value routes through the `us` package's
    # lookup(abbr=...), which is case-sensitive) only matches the uppercase
    # form -- confirmed live, "fl" returns 0 results, "FL" returns the real
    # 5. This function has shipped and been on a schedule since before this
    # fix (PLAN-bill-concept-polling.md §0.4) -- the existing scheduled
    # caller has likely been silently returning zero candidates every run.
    base_params: dict[str, str] = {"jurisdiction": jurisdiction_iso2.upper()}
    base_params["session"] = resolved_session_code
    if settings.local_openstates_api_key:
        base_params["apikey"] = settings.local_openstates_api_key

    # Real, separate pagination bug found designing Step 1: per_page=limit
    # was sent as the *page size* to a single GET -- silently under-covering
    # any session with more bills than `limit`. `limit` means total
    # candidates collected across pages, not a per-request page size --
    # loop against api-v3's own pagination.max_page, capped at
    # _API_V3_MAX_PER_PAGE per request (confirmed live: api-v3 rejects
    # per_page > 20).
    #
    # Real, separate duplicate-candidate bug found live 2026-08-18 (SYNC-23):
    # per_page here used to be recomputed every iteration as `limit -
    # len(candidates)`, shrinking on later pages. api-v3's own pagination
    # (api-v3/api/pagination.py Pagination.paginate) computes each request's
    # offset as `(page - 1) * per_page` and `max_page` from THAT SAME
    # request's per_page -- both derived fresh per call, not fixed
    # server-side. Varying per_page across pages while treating `page` as a
    # stable cursor breaks that offset math: e.g. limit=25 against a
    # same-jurisdiction/session total of 22 bills fetches page=1
    # per_page=20 (items 1-20, max_page=ceil(22/20)=2), then page=2
    # per_page=5 (offset=(2-1)*5=5 -- items 6-10, NOT 21-22) -- re-fetching 5
    # bills already collected on page 1 as if they were new. per_page is now
    # decided ONCE, before the loop, and held fixed for every page this call
    # makes, so offset math stays consistent across however many pages are
    # needed. The dedup below is an additional, independent safety net (api-
    # v3's default sort has no secondary tiebreaker key, so bills with a tied
    # sort value could still land on two pages even with per_page held
    # fixed) -- not a substitute for this fix, since the per_page bug above
    # was confirmed as a real, reproducible cause on its own.
    candidates: list[dict] = []
    seen_bill_openstates_ids: set[str] = set()
    duplicates_dropped = 0
    page = 1
    per_page = str(min(_API_V3_MAX_PER_PAGE, limit))
    while len(candidates) < limit:
        params = dict(base_params)
        params["per_page"] = per_page
        params["page"] = str(page)

        try:
            async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
                resp = await client.get(url, params=params)
        except httpx.RequestError as exc:
            logger.warning(
                "Local api-v3 unreachable -- skipping jurisdiction for this "
                "batch run",
                jurisdiction_iso2=jurisdiction_iso2,
                error=str(exc),
            )
            return candidates

        if resp.status_code >= 400:
            logger.warning(
                "Local api-v3 rejected the bill-list read -- skipping "
                "jurisdiction for this batch run",
                jurisdiction_iso2=jurisdiction_iso2,
                status_code=resp.status_code,
            )
            return candidates

        try:
            data = resp.json()
        except ValueError:
            logger.warning(
                "Local api-v3 returned a non-JSON bill-list response -- "
                "skipping jurisdiction for this batch run",
                jurisdiction_iso2=jurisdiction_iso2,
            )
            return candidates

        for bill in data.get("results", []) or []:
            gov_id = (bill.get("identifier") or "").strip()
            raw_id = bill.get("id", "") or ""
            bill_openstates_id = raw_id.rsplit("/", 1)[-1] if raw_id else ""
            if not gov_id or not bill_openstates_id:
                continue
            if bill_openstates_id in seen_bill_openstates_ids:
                duplicates_dropped += 1
                continue
            seen_bill_openstates_ids.add(bill_openstates_id)
            sources = bill.get("sources") or []
            live_url_fallback = sources[0].get("url", "") if sources else ""
            candidates.append({
                "gov_id": gov_id,
                "bill_openstates_id": bill_openstates_id,
                "session_code": resolved_session_code,
                "live_url_fallback": live_url_fallback,
            })

        pagination = data.get("pagination") or {}
        max_page = pagination.get("max_page", page)
        if page >= max_page or len(candidates) >= limit:
            break
        page += 1

    if duplicates_dropped:
        # Surfaces the residual api-v3 sort-tie case (no secondary
        # tiebreaker key on its default sort) even after the per_page fix
        # above -- dedup silently returning fewer than `limit` unique
        # candidates, with no other signal that api-v3's own pagination is
        # still occasionally unstable, would otherwise be invisible.
        logger.warning(
            "list_current_session_bill_candidates dropped duplicate "
            "candidates across pages",
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=resolved_session_code,
            duplicates_dropped=duplicates_dropped,
            unique_candidates=len(candidates),
        )

    return candidates[:limit]
