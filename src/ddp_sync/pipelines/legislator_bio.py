"""Legislator bio + contact sync orchestrator.

Step 4 of Phase 1 (see plans/PLAN-legislator-bio-sync.md). Ties together:
- services/congress_legislators (federal — unitedstates YAML dataset)
- services/openstates_people    (state + current federal)
- services/webflow_lookup       (Webflow CMS read + write)

Run flow:
1. Read all Legislators CMS items (one Webflow read pass).
2. For each CMS item:
   a. Resolve upstream:
      - Federal current: try OpenStates by openstatesid → bioguide-id fallback
      - Federal historical (no openstatesid): bioguide-id → congress-legislators
      - State: OpenStates by openstatesid (Phase 2 — stubbed in Phase 1)
   b. Build a field payload using the source-precedence matrix from the PLAN.
   c. Diff against current CMS values via ``should_write()`` / ``is_empty()``.
   d. PATCH (or append to dry-run report).
3. Optional: discover upstream-only legislators and create drafts when
   ``auto_create=true``, with multi-signal merge detection to flag
   state→federal transitions for editor review.

Phase 1 scope is **federal only**. State coverage lands in Phase 2 (extends
``_resolve_upstream`` and ``_build_payload``); the state branch is a clear
TODO marker rather than a silent no-op.

Error handling:
- WebflowError / OpenStatesError per record append to BioSyncReport.errors;
  the run continues for the next CMS item.
- WebflowRateLimitError or OpenStatesRateLimitError aborts the run (429
  storms can't be papered over) — the report is returned with the partial
  state and the error.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, Literal

import requests
import structlog

from ddp_sync.config import Settings, get_settings
from ddp_sync.services.congress_legislators import (
    CongressLegislator,
    CongressLegislatorsSource,
)
from ddp_sync.services.openstates_people import (
    OpenStatesError,
    OpenStatesPeopleClient,
    OpenStatesPerson,
    OpenStatesRateLimitError,
)
from ddp_sync.services.rate_limiter import RateLimiter
from ddp_sync.services.webflow_assets import (
    AssetReference,
    WebflowAssetError,
    WebflowAssetService,
)
from ddp_sync.services.webflow_lookup import (
    WebflowError,
    WebflowLookupService,
    WebflowRateLimitError,
)

logger = structlog.get_logger()


# ---------- Cardinal rule: empty-value handling ----------

EMPTY_VALUES: set = {
    None, "",
    "-", "—",
    "N/A", "n/a", "NA", "na",
    "TBD", "tbd",
    "UNKNOWN", "unknown",
    "null", "NULL",
}


def is_empty(value: Any) -> bool:
    """Return True if ``value`` is one of our recognized empty sentinels.

    Numeric zero is intentionally excluded — a `district` of 0 (used by
    some at-large jurisdictions) is a valid value, not an empty.

    Container types (list/dict) are checked structurally before the
    EMPTY_VALUES membership test so we don't `in`-test an unhashable.
    """
    if isinstance(value, (list, dict)):
        return not value
    if isinstance(value, str) and not value.strip():
        return True
    if value in EMPTY_VALUES:
        return True
    return False


def should_write(
    field_name: str,
    cms_value: Any,
    upstream_value: Any,
    *,
    locked_fields: Iterable[str] = (),
) -> bool:
    """Cardinal rule: never blank a populated CMS field with empty upstream.

    Returns True iff the upstream value is non-empty AND differs from the
    CMS value AND the field is not in the editor's locked-fields list.
    """
    if is_empty(upstream_value):
        return False
    if cms_value == upstream_value:
        return False
    if field_name in locked_fields:
        return False
    return True


# ---------- Run-summary alerting (step 7) ----------


# Default threshold for the on_large_changes escalation flag in the Zapier
# payload (round-10 fix: was a magic number embedded inline). Editors can
# tune this by passing ``large_changes_threshold=`` to push_bio_sync_alert.
# Phase 2 may move this to ``sync_schedule.yaml::notifications`` for runtime
# tuning without a code change; for Phase 1 the named constant is the
# single source of truth.
DEFAULT_LARGE_CHANGES_THRESHOLD = 100


def push_bio_sync_alert(
    webhook_url: str,
    report: "BioSyncReport",
    *,
    large_changes_threshold: int = DEFAULT_LARGE_CHANGES_THRESHOLD,
) -> bool:
    """POST a legislator-bio-sync run summary to the configured Zapier webhook.

    Mirrors ``pipelines/voatz_brevo.py::push_alert_to_zapier`` — sync HTTP
    via the ``requests`` library (Zapier is fire-and-forget; latency
    doesn't matter), 30s timeout, never raises (returns ``bool``).

    The webhook URL comes from ``settings.zapier_webhook_url`` which the
    config layer populates from the ``ZAPIER_WEBHOOK_URL`` env var (same
    wiring used by ``voatz_brevo.run_sync_job``).

    Bio sync becomes the first consumer of the scaffolded ``notifications:``
    block in ``sync_schedule.yaml`` (PLAN run-summary alerting section).
    Threshold-based escalation routing is handled Zapier-side based on
    the ``on_failure`` and ``on_large_changes`` flags in the payload.

    Caller is responsible for skipping this on dry-runs (no real writes
    happened, so no editor-facing alert needed).
    """
    if not webhook_url:
        logger.error(
            "No Zapier webhook URL configured for bio-sync alert",
            metric="legislator_bio_sync.alert_skipped",
            reason="no_webhook_url",
        )
        return False

    patched = len(report.would_patch)
    created = len(report.would_create)
    merges = len(report.potential_merges)
    orphans = len(report.upstream_orphans)
    errors = len(report.errors)

    # Threshold flags Zapier can route on. on_failure and on_large_changes
    # mirror the names in sync_schedule.yaml::notifications.
    on_failure = errors > 0 or report.aborted
    on_large_changes = (patched + created) > large_changes_threshold

    # Pre-formatted warning lines. Zapier doesn't support Mustache
    # conditional sections — only flat {{field}} interpolation — so the
    # Slack template just drops these in unconditionally. Empty string
    # when no warning, so the line collapses cleanly.
    failure_warning = (
        "⚠️ Failure flag set — investigate" if on_failure else ""
    )
    large_changes_warning = (
        f"⚠️ Large-changes flag set ({patched} + {created} > "
        f"{large_changes_threshold})"
        if on_large_changes else ""
    )

    # Phase-4: photo-upload coverage metrics. Computed only when at
    # least one upload was attempted (otherwise ratio is undefined and
    # we send 0/null). Surfaced both as raw counters and a coverage
    # ratio for dashboarding.
    photo_attempted = report.photo_uploads_attempted
    photo_succeeded = report.photo_uploads_succeeded
    photo_failed = report.photo_uploads_failed
    photo_coverage_ratio = (
        round(photo_succeeded / photo_attempted, 3)
        if photo_attempted > 0 else None
    )

    payload = {
        "alert_type": "legislator_bio_sync_complete",
        "summary": (
            f"items_seen={report.cms_items_seen} "
            f"patched={patched} created={created} "
            f"merges={merges} orphans={orphans} errors={errors} "
            f"photos={photo_succeeded}/{photo_attempted}"
        ),
        "items_seen": report.cms_items_seen,
        "items_resolved_via_openstates": (
            report.items_resolved_via_openstates
        ),
        "items_resolved_via_bioguide_fallback": (
            report.items_resolved_via_bioguide_fallback
        ),
        "patched": patched,
        "created": created,
        "potential_merges": merges,
        "upstream_orphans": orphans,
        "errors": errors,
        "aborted": report.aborted,
        "abort_reason": report.abort_reason,
        "on_failure": on_failure,
        "on_large_changes": on_large_changes,
        "large_changes_threshold": large_changes_threshold,
        "failure_warning": failure_warning,
        "large_changes_warning": large_changes_warning,
        "photo_uploads_attempted": photo_attempted,
        "photo_uploads_succeeded": photo_succeeded,
        "photo_uploads_failed": photo_failed,
        "photo_coverage_ratio": photo_coverage_ratio,
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=30)
        if 200 <= response.status_code < 300:
            # Round-10 fix: positive-confirm metric for SLA dashboards.
            # alert_skipped + alert_failed metrics already cover the
            # negative paths; this closes the observability gap.
            logger.info(
                "bio-sync Zapier alert sent",
                metric="legislator_bio_sync.alert_sent",
                patched=patched,
                created=created,
                errors=errors,
                aborted=report.aborted,
            )
            return True
        logger.error(
            "Zapier webhook returned non-2xx",
            status_code=response.status_code,
            response=response.text[:200],
            metric="legislator_bio_sync.alert_failed",
        )
    except Exception as e:  # noqa: BLE001 — never raise from this helper
        logger.error(
            "Zapier webhook error",
            error=str(e),
            metric="legislator_bio_sync.alert_failed",
        )
    return False


# ---------- Federal `email` URL detection ----------

def split_email_field(value: Any) -> tuple[str | None, str | None]:
    """Split an OpenStates ``email`` value into (email, contact_form_url).

    Federal members usually have a contact-form URL in this field; state
    members usually have a real email. URL-shaped values route to
    ``contact-form-url``; bare emails route to ``email``. At most one of
    the two is non-None.

    Round-7 hardening: case-insensitive scheme matching plus ``mailto:``
    handling. The bare `mailto:jane@x.gov` form unwraps to a real email,
    not a contact-form URL. Whitespace is stripped. Anything else
    URL-shaped (any scheme://) routes to contact-form-url.
    """
    if not value or not isinstance(value, str):
        return None, None
    stripped = value.strip()
    if not stripped:
        return None, None
    lower = stripped.lower()
    if lower.startswith("mailto:"):
        # Strip the "mailto:" prefix; anything after is a real email
        return stripped.split(":", 1)[1] or None, None
    if lower.startswith(("http://", "https://")):
        return None, stripped
    return stripped, None


# ---------- Options + report ----------


@dataclass
class BioSyncOptions:
    target: Literal["all", "webflow", "pinecone"] = "all"
    jurisdiction: str | None = None     # None = all configured. "us" for federal.
    auto_create: bool = False
    dry_run: bool = False
    limit: int = 0                       # 0 = unlimited
    historical_since: date = field(default_factory=lambda: date(2023, 1, 1))
    locked_fields: tuple[str, ...] = ()
    # Phase-3: when True, any payload field that the schema cache would
    # silently drop (because the slug doesn't exist in the live CMS
    # collection) is treated as a per-record error. Default False so
    # ongoing scheduled runs tolerate partial schema rollouts; flip True
    # for the first deploy after adding a new write target so missing
    # slugs surface as errors instead of silently no-op'ing.
    strict_schema: bool = False
    # Phase-3: when True, the orchestrator fetches the source image
    # from photo-source-url and uploads to Webflow's asset library,
    # populating the legislator-image (Image type) field. Default False
    # because the upload flow is opt-in and per-state photo CDN
    # stability varies; operator flips True after editor verification
    # of a sample. Per-record upload failures are isolated (logged +
    # continue, don't abort the run); the photo-source-url Link field
    # still carries the original URL even when upload fails.
    upload_photos: bool = False
    # Phase-3 connectivity smoke: when True (and upload_photos is True),
    # fetch + size-validate + hash the source image but skip the actual
    # Webflow asset creation. Lets operators smoke-test source-CDN
    # reachability without consuming Webflow's asset rate limit or
    # storage. Implies upload_photos=True; no-op when upload_photos
    # is False.
    upload_photos_dry_run: bool = False


@dataclass
class BioSyncReport:
    cms_items_seen: int = 0
    items_resolved_via_openstates: int = 0
    items_resolved_via_bioguide_fallback: int = 0
    would_patch: list[dict] = field(default_factory=list)
    would_create: list[dict] = field(default_factory=list)
    potential_merges: list[dict] = field(default_factory=list)
    upstream_orphans: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    aborted: bool = False
    abort_reason: str | None = None
    # Phase-4: photo-upload coverage metrics. Populated only when
    # options.upload_photos is True. attempted = number of records the
    # orchestrator tried to upload for (CMS legislator-image was empty
    # AND a source URL was available); succeeded / failed split between
    # them. failed records are also surfaced individually in errors[].
    photo_uploads_attempted: int = 0
    photo_uploads_succeeded: int = 0
    photo_uploads_failed: int = 0


# ---------- CMS index helpers ----------


# Federal-detection: Legislators have a multi-reference `seat` field
# pointing to the Seats CMS collection (4 items: U.S. House, U.S. Senate,
# State House, State Senate). We hardcode the federal ref-IDs rather than
# fetching the Seats collection at runtime — the collection is small and
# semantically fixed (every US state is bicameral except Nebraska's
# unicameral state senate). If item IDs ever change, update this constant.
#
# The full ref-ID → slug mapping is exposed via _SEAT_REF_TO_SLUG so the
# audit reports can surface a human-readable seat slug per record.
_SEAT_REF_TO_SLUG: dict[str, str] = {
    "66316e20ae88354aed5df702": "us-house",
    "66316e0956dc73af879134b4": "us-senate",
    "655288ef928edb1283067463": "state-house",
    "655288ef928edb12830673e8": "state-senate",
}
_FEDERAL_SEAT_REF_IDS: frozenset[str] = frozenset({
    "66316e20ae88354aed5df702",  # us-house
    "66316e0956dc73af879134b4",  # us-senate
})


# Sentinel for "photo upload was tried and is permanently disabled for
# this run" — set on the orchestrator's ``self.assets`` field after a
# config-error so subsequent records skip without re-running init.
_NULL_ASSET_SERVICE_SENTINEL = object()


# ---------- Per-state payload overrides (Phase 2.5) ----------
#
# OpenStates carries some fields per-state that need jurisdiction-specific
# extraction. The default state-builder is generic; overrides here
# augment the payload for specific states. Each override receives the
# OpenStatesPerson and returns a partial payload dict (or empty dict).
#
# The override is best-effort: if it raises, the orchestrator logs a
# warning and continues with the default payload. Don't put critical
# fields here — keep the override scoped to enrichment fields the
# default state builder can't get generically.

_FL_OFFICIAL_WEBSITE_HOSTS = (
    "myfloridahouse.gov",
    "www.myfloridahouse.gov",
    "flsenate.gov",
    "www.flsenate.gov",
)
_FL_PROFILE_LINK_NOTES = ("member detail page",)


def _fl_state_override(os_record: "OpenStatesPerson") -> dict:
    """FL-specific enrichment.

    Extracts ``official-website`` from ``links[]`` by picking the
    "member detail page" note when present (9-of-10 coverage in the
    2026-04-30 probe), falling back to the first link with a known
    FL-legislature host. Other states' override functions can mirror
    this shape.
    """
    payload: dict = {}
    links = os_record.links or []

    chosen: str | None = None
    for link in links:
        note = (link.get("note") or "").strip().lower()
        if note in _FL_PROFILE_LINK_NOTES:
            chosen = link.get("url")
            break
    if not chosen:
        for link in links:
            url = link.get("url") or ""
            try:
                from urllib.parse import urlparse
                if urlparse(url).netloc.lower() in _FL_OFFICIAL_WEBSITE_HOSTS:
                    chosen = url
                    break
            except Exception:  # noqa: BLE001
                continue
    if chosen:
        payload["official-website"] = chosen
    return payload


_STATE_PAYLOAD_OVERRIDES: dict[str, Callable[["OpenStatesPerson"], dict]] = {
    "FL": _fl_state_override,
}


def _normalize_seat_refs(value: Any) -> list[str]:
    """Normalize the upstream ``seat`` field into a list of ref-ID strings.

    Multi-reference fields in Webflow's v2 API typically return a list of
    item-id strings, but we accept None / single-string / list shapes
    defensively.
    """
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str) and v]
    return []


@dataclass
class CMSLegislator:
    """Convenience wrapper around a Webflow Legislators item.

    Captures the join keys and any field values the orchestrator needs to
    diff against. The full upstream item is preserved as ``raw_item`` for
    any downstream code that needs other fields.

    ``state_code`` is precomputed at construction time from the
    Legislators' ``jurisdiction`` multi-reference field, resolved via the
    optional ``jurisdiction_resolver`` callable. The resolver is built
    once per orchestrator/audit run from the Jurisdictions CMS collection
    (see ``WebflowLookupService.get_jurisdiction_mapping``).
    """

    webflow_id: str
    slug: str
    name: str
    openstates_id: str | None
    bioguide_id: str | None
    is_federal: bool
    state_code: str | None
    seat_refs: list[str]
    raw_fields: dict
    raw_item: dict

    @property
    def seat_slugs(self) -> list[str]:
        """Resolve seat ref-IDs to their kebab-case slugs.

        Unknown ref-IDs are dropped silently; this is purely a display aid
        for audit reports.
        """
        return [
            _SEAT_REF_TO_SLUG[r]
            for r in self.seat_refs
            if r in _SEAT_REF_TO_SLUG
        ]

    @classmethod
    def from_webflow_item(
        cls,
        item: dict,
        *,
        jurisdiction_resolver: Callable[[Any], str | None] | None = None,
    ) -> "CMSLegislator":
        """Build a CMSLegislator from a raw Webflow CMS item.

        ``jurisdiction_resolver(value)`` should accept the upstream
        ``jurisdiction`` field (None, ref-id string, list of ref-ids, or
        already-2-letter code) and return a normalized two-letter state
        code or None for federal/unresolvable. Use
        ``WebflowLookupService.resolve_jurisdiction_ref`` partial-applied
        to the cached mapping.

        When ``jurisdiction_resolver`` is None the state_code is None —
        the data model is reference-based and no flat fallback is
        attempted (round-8 fix). Audit C without a resolver will see
        every record as having no state code, which is the safer
        behavior than silent false-negatives.

        Federal/state classification reads the multi-reference ``seat``
        field and matches each ref-ID against ``_FEDERAL_SEAT_REF_IDS``.
        Records with no seat assigned have ``is_federal=False`` and will
        be treated as state by the orchestrator (and skipped by Audit A).
        """
        fields = item.get("fieldData") or {}
        seat_refs = _normalize_seat_refs(fields.get("seat"))
        is_federal = any(r in _FEDERAL_SEAT_REF_IDS for r in seat_refs)
        state_code: str | None = None
        if jurisdiction_resolver is not None:
            try:
                state_code = jurisdiction_resolver(fields.get("jurisdiction"))
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "jurisdiction_resolver raised; treating as unresolved",
                    error=str(e),
                )
                state_code = None
        return cls(
            webflow_id=item.get("id") or "",
            slug=fields.get("slug") or "",
            name=fields.get("name") or "",
            openstates_id=(fields.get("openstatesid") or None),
            bioguide_id=(fields.get("bioguide-id") or None),
            is_federal=is_federal,
            state_code=state_code,
            seat_refs=seat_refs,
            raw_fields=fields,
            raw_item=item,
        )


# ---------- Audit reports (step 6) ----------


@dataclass
class AuditEntry:
    """A single CMS record flagged by an audit run.

    ``seat`` is a list of kebab-case slugs from the Seats CMS (e.g.
    ``["us-senate"]``) — empty when the record has no seat assigned.
    Using slugs keeps audit reports stable even if the Seats CMS display
    text gets re-cased or punctuation-normalized.

    ``reason`` is set by Audit B to distinguish the two failure classes
    (``missing_openstatesid`` vs ``duplicate_openstatesid:<id>``).
    Audits A and C leave it unset.
    """

    webflow_id: str
    slug: str
    name: str
    seat: list[str] = field(default_factory=list)
    state_code: str | None = None
    openstates_id: str | None = None
    bioguide_id: str | None = None
    reason: str | None = None


@dataclass
class AuditReport:
    """Outcome of a single audit run.

    ``audit_name`` is one of "A" (federal join-key coverage) or
    "C" (pre-existing state records lacking openstatesid). Audit B (post-
    bulk-import duplicate detection) runs from the editor toolchain.
    """

    audit_name: str
    total_scanned: int
    flagged_count: int
    flagged: list[AuditEntry] = field(default_factory=list)
    jurisdiction: str | None = None
    aborted: bool = False
    abort_reason: str | None = None


# ---------- Pipeline ----------


class LegislatorBioPipeline:
    """Orchestrator for the legislator bio + contact sync.

    Hold a single instance per process to share the underlying
    WebflowLookupService rate-limiter across calls.
    """

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        webflow: WebflowLookupService | None = None,
        congress: CongressLegislatorsSource | None = None,
        openstates: OpenStatesPeopleClient | None = None,
        openstates_rate_limiter: RateLimiter | None = None,
        assets: "WebflowAssetService | None" = None,
    ):
        self.settings = settings or get_settings()
        self.webflow = webflow or WebflowLookupService(self.settings)
        self.congress = congress or CongressLegislatorsSource()
        if openstates is None:
            api_key = self.settings.openstates_api_key
            if not api_key:
                raise ValueError(
                    "OPENSTATES_API_KEY is required for legislator bio sync"
                )
            self.openstates = OpenStatesPeopleClient(
                api_key,
                openstates_rate_limiter or RateLimiter(),
                openstates_api_base=self.settings.openstates_api_base,
                local_openstates_api_base=self.settings.local_openstates_api_base,
                local_openstates_api_key=self.settings.local_openstates_api_key,
                ddp_openstates_jurisdictions=self.settings.ddp_openstates_jurisdictions,
            )
        else:
            self.openstates = openstates
        # Phase-3 photo upload service. Lazy-initialized on first use
        # if the orchestrator was constructed without one and
        # upload_photos is enabled — keeps the cold path cheap.
        self.assets = assets

    # ---------- Public entry point ----------

    async def run(self, options: BioSyncOptions) -> BioSyncReport:
        """Run a sync pass. Returns a BioSyncReport (always — even on abort)."""
        report = BioSyncReport()
        logger.info(
            "Starting legislator bio sync",
            jurisdiction=options.jurisdiction,
            auto_create=options.auto_create,
            dry_run=options.dry_run,
            target=options.target,
            limit=options.limit,
        )

        # Pre-warm the federal source so per-record lookups are O(1).
        # Idempotent — if app.py pre-warmed at startup this is a no-op.
        await self.congress.warm_cache()

        try:
            try:
                await self._process_cms_records(options, report)
            except (WebflowRateLimitError, OpenStatesRateLimitError) as e:
                report.aborted = True
                report.abort_reason = f"Rate-limit error aborted run: {e}"
                logger.error(
                    "Bio sync aborted on rate limit",
                    error=str(e),
                    processed=report.cms_items_seen,
                )
                return report

            if options.auto_create:
                try:
                    await self._discover_and_create(options, report)
                except (
                    WebflowRateLimitError, OpenStatesRateLimitError
                ) as e:
                    report.aborted = True
                    report.abort_reason = (
                        f"Rate-limit error during auto-create: {e}"
                    )
                    logger.error(
                        "Bio sync auto-create aborted",
                        error=str(e),
                    )
                    return report

            logger.info(
                "Legislator bio sync complete",
                seen=report.cms_items_seen,
                patched=len(report.would_patch),
                created=len(report.would_create),
                merges=len(report.potential_merges),
                orphans=len(report.upstream_orphans),
                errors=len(report.errors),
            )
            return report
        finally:
            # Step 7 — post run-summary alert to Zapier on every non-dry-run
            # completion (including aborts — those are exactly what editors
            # need to know about). Skipped for dry-runs since no real writes
            # happened. Run the sync HTTP call off the event loop via
            # asyncio.to_thread; webhook is fire-and-forget so we don't
            # block the caller on it.
            if not options.dry_run:
                webhook = getattr(self.settings, "zapier_webhook_url", "")
                if webhook:
                    try:
                        await asyncio.to_thread(
                            push_bio_sync_alert, webhook, report
                        )
                    except Exception as e:  # noqa: BLE001
                        # push_bio_sync_alert never raises, but defend
                        # against asyncio.to_thread surprises so we don't
                        # mask a real error from the run.
                        logger.error(
                            "Zapier alert task crashed",
                            error=str(e),
                        )
                else:
                    logger.info(
                        "Skipping Zapier alert; ZAPIER_WEBHOOK_URL not set",
                    )

    # ---------- Processing ----------

    async def _process_cms_records(
        self,
        options: BioSyncOptions,
        report: BioSyncReport,
    ) -> None:
        """Iterate every Legislators CMS item and apply the sync logic."""
        resolver = await self._build_jurisdiction_resolver()
        async for item in self.webflow.iter_legislator_items():
            cms = CMSLegislator.from_webflow_item(
                item, jurisdiction_resolver=resolver,
            )
            report.cms_items_seen += 1

            # Honor jurisdiction filter
            if options.jurisdiction:
                if not self._matches_jurisdiction(cms, options.jurisdiction):
                    continue

            # Honor limit
            if (
                options.limit
                and (
                    len(report.would_patch)
                    + len(report.errors)
                    >= options.limit
                )
            ):
                break

            try:
                await self._sync_one_record(cms, options, report)
            except WebflowRateLimitError:
                raise  # bubble — abort the run
            except OpenStatesRateLimitError:
                raise
            except (WebflowError, OpenStatesError) as e:
                report.errors.append(
                    f"{cms.slug or cms.webflow_id}: {type(e).__name__}: {e}"
                )
                logger.warning(
                    "Bio sync error on record",
                    webflow_id=cms.webflow_id,
                    name=cms.name,
                    error=str(e),
                )
            except Exception as e:  # noqa: BLE001
                report.errors.append(
                    f"{cms.slug or cms.webflow_id}: unhandled: {e}"
                )
                logger.exception(
                    "Bio sync unhandled error",
                    webflow_id=cms.webflow_id,
                )

    async def _sync_one_record(
        self,
        cms: CMSLegislator,
        options: BioSyncOptions,
        report: BioSyncReport,
    ) -> None:
        # Resolve upstream
        federal_record = None
        os_record = None
        resolved = False

        if cms.openstates_id:
            try:
                os_record = await self.openstates.fetch_by_id(cms.openstates_id)
            except OpenStatesError as e:
                # transient-ish; log and continue with bioguide fallback if federal
                logger.warning(
                    "OpenStates fetch failed; will try bioguide fallback if federal",
                    webflow_id=cms.webflow_id,
                    openstates_id=cms.openstates_id,
                    error=str(e),
                )
            if os_record is not None:
                resolved = True
                report.items_resolved_via_openstates += 1

        # Federal-only fallback path
        if not resolved and cms.is_federal and cms.bioguide_id:
            federal_record = await self.congress.get_by_bioguide(cms.bioguide_id)
            if federal_record is not None:
                resolved = True
                report.items_resolved_via_bioguide_fallback += 1

        # If resolved via OpenStates AND it's federal, also fetch the
        # unitedstates record by bioguide for richer fields (term history,
        # social handles, full ID set). The federal source-precedence matrix
        # in the PLAN says unitedstates wins for federal-specific fields.
        if (
            resolved
            and os_record is not None
            and os_record.is_federal
            and federal_record is None
        ):
            bg = (
                cms.bioguide_id
                or os_record.get_other_id("bioguide")
            )
            if bg:
                federal_record = await self.congress.get_by_bioguide(bg)

        if not resolved:
            report.upstream_orphans.append({
                "webflow_id": cms.webflow_id,
                "slug": cms.slug,
                "name": cms.name,
                "openstates_id": cms.openstates_id,
                "bioguide_id": cms.bioguide_id,
            })
            return

        # Build payload + diff
        if cms.is_federal:
            payload = self._build_federal_payload(
                cms, federal_record, os_record
            )
        else:
            # State path: OpenStates-only payload (no unitedstates equivalent
            # for state legs). Defensive: os_record is always non-None here
            # because the bioguide fallback at line 680 is federal-only —
            # an unresolved state record short-circuits to upstream_orphans
            # before reaching this branch.
            if os_record is None:
                return
            payload = self._build_state_payload(cms, os_record)

        # Phase-3 photo upload (opt-in via options.upload_photos):
        # uploads the source image into Webflow's asset library and
        # populates legislator-image (Image-typed). Skipped when CMS
        # already has the field populated (cardinal rule preserves
        # editor-managed values; subsequent runs no-op cleanly).
        if (
            options.upload_photos
            and not cms.raw_fields.get("legislator-image")
            and payload.get("photo-source-url")
        ):
            asset_value = await self._maybe_upload_photo(
                cms, payload["photo-source-url"], report,
                dry_run=options.upload_photos_dry_run,
            )
            if asset_value is not None:
                payload["legislator-image"] = asset_value

        changed = self._diff_payload(
            cms.raw_fields, payload, options.locked_fields
        )
        if not changed:
            return

        if options.dry_run or options.target == "pinecone":
            report.would_patch.append({
                "webflow_id": cms.webflow_id,
                "name": cms.name,
                "changed_fields": sorted(changed.keys()),
            })
            return

        # Live PATCH. Phase-3 strict_schema (round-18 fix): the
        # update_legislator_fields service-side check raises BEFORE
        # sending the PATCH when any payload slug is missing from the
        # live CMS schema. Avoids the partial-write state the original
        # post-PATCH check left behind.
        result = await self.webflow.update_legislator_fields(
            cms.webflow_id, changed,
            strict_schema=options.strict_schema,
        )
        report.would_patch.append({
            "webflow_id": cms.webflow_id,
            "name": cms.name,
            "changed_fields": sorted(changed.keys()),
            "dropped_fields": sorted(result.dropped_fields),
        })

    # ---------- Phase-3 photo upload helper ----------

    async def _maybe_upload_photo(
        self,
        cms: "CMSLegislator",
        source_url: str,
        report: BioSyncReport,
        *,
        dry_run: bool = False,
    ) -> dict | None:
        """Upload the source image into Webflow's asset library.

        Returns the Image-field-shaped dict (``{fileId, url, alt}``) on
        success, or ``None`` on any failure (logged + recorded in the
        run's errors list, but does NOT abort the record's PATCH; the
        photo-source-url Link field still gets the original URL so the
        website can fall back to hotlinking).

        Lazy-initializes ``self.assets`` on first use so the cold path
        of upload-photos-disabled runs doesn't pay the construction
        cost.
        """
        if self.assets is None:
            # Phase-3: assets uses a dedicated key with the assets:read +
            # assets:write scopes (separate from the cms:* token used for
            # item PATCHes). The general webflow_api_token doesn't carry
            # assets:write — confirmed via 2026-04-30 production smoke
            # which returned 403 OAuthForbidden on POST /assets.
            #
            # Round-19 fix: fail FAST on missing dedicated key rather
            # than silently falling back to the cms token (which would
            # re-trigger the 403 mid-run on every record). The reviewer
            # flagged the silent-fallback as a high-severity safety
            # gap. Operator who enabled upload_photos but didn't
            # configure the dedicated key gets a single clear error
            # logged once for the whole run.
            assets_token = self.settings.webflow_assets_read_write_key
            if not assets_token:
                logger.error(
                    "Photo upload disabled: webflow_assets_read_write_key "
                    "is not configured. Add a Webflow API token with "
                    "assets:read + assets:write scopes to the secret as "
                    "`webflow_assets_read_write_key`. The cms:* token is "
                    "insufficient (returns 403 OAuthForbidden on POST "
                    "/assets).",
                    metric="webflow_assets.config_error",
                )
                self.assets = _NULL_ASSET_SERVICE_SENTINEL  # type: ignore[assignment]
                return None
            try:
                self.assets = WebflowAssetService(
                    api_token=assets_token,
                    site_id=self.settings.webflow_site_id,
                )
            except ValueError as e:
                # Missing site_id config — log once + give up for this
                # run (don't keep retrying)
                logger.error(
                    "Photo upload disabled: WebflowAssetService init failed",
                    error=str(e),
                    metric="webflow_assets.config_error",
                )
                self.assets = _NULL_ASSET_SERVICE_SENTINEL  # type: ignore[assignment]
                return None
        if self.assets is _NULL_ASSET_SERVICE_SENTINEL:
            return None

        # Phase-4: federal records get a congress.gov fallback URL
        # because the unitedstates/images community dataset has gaps
        # for new freshmen + some non-current bioguides (4 of 32 FL
        # federal records 404'd in production 2026-04-30). Congress.gov
        # uses lowercase bioguide-id in the path. State records have no
        # universal fallback (per-state CDNs vary too much).
        fallback_urls: tuple[str, ...] = ()
        if cms.is_federal and cms.bioguide_id:
            fallback_urls = (
                f"https://www.congress.gov/img/member/"
                f"{cms.bioguide_id.lower()}.jpg",
            )

        # Phase-4: count this attempt (regardless of outcome). The
        # success/failure split lands below.
        report.photo_uploads_attempted += 1

        try:
            ref = await self.assets.upload_from_url(
                source_url,
                fallback_urls=fallback_urls,
                alt_text=cms.name,
                dry_run=dry_run,
            )
        except WebflowAssetError as e:
            report.errors.append(
                f"{cms.slug or cms.webflow_id}: photo upload failed: {e}"
            )
            report.photo_uploads_failed += 1
            logger.warning(
                "Photo upload failed; skipping legislator-image",
                webflow_id=cms.webflow_id,
                source_url=source_url,
                error=str(e),
                metric="webflow_assets.upload_failed",
            )
            return None
        except Exception as e:  # noqa: BLE001
            report.errors.append(
                f"{cms.slug or cms.webflow_id}: photo upload unhandled: {e}"
            )
            report.photo_uploads_failed += 1
            logger.exception(
                "Photo upload unhandled error",
                webflow_id=cms.webflow_id,
                source_url=source_url,
            )
            return None
        # dry_run mode returns None from upload_from_url; the caller
        # treats that the same as "no asset to set" — payload skips
        # legislator-image. The fetch + size-check + hash already
        # validated source-CDN reachability. Count as success because
        # the source-fetch flow worked, even though no asset was made.
        if ref is None:
            report.photo_uploads_succeeded += 1
            return None
        report.photo_uploads_succeeded += 1
        return ref.to_image_field_value()

    # ---------- Federal payload builder ----------

    def _build_federal_payload(
        self,
        cms: CMSLegislator,
        federal: CongressLegislator | None,
        os_record: OpenStatesPerson | None,
    ) -> dict:
        """Build a Webflow PATCH payload for a federal legislator.

        Source-precedence matrix from the PLAN: ``unitedstates`` wins for
        federal-specific fields (terms, IDs, contact, social). OpenStates
        is the fallback when the unitedstates record is missing a value.
        """
        payload: dict[str, Any] = {}

        # OpenStates profile URL — applies to federal too (URL-typed
        # `open-states-url` field on the CMS; one-line "see this person
        # on OpenStates" link). Phase-2.5 addition; populated from
        # OpenStates' `openstates_url` field.
        # Trailing slash is stripped because Webflow's URL field
        # round-trips without it; without this normalization every run
        # would re-PATCH (ChurnPATCH observed 2026-04-30 on 222/224
        # records after the Phase-2.5 ship).
        if os_record is not None and os_record.openstates_url:
            payload["open-states-url"] = os_record.openstates_url.rstrip("/")

        # Cross-source IDs (mostly federal-only — the unitedstates dataset
        # publishes them all with high coverage, so prefer that source).
        if federal is not None:
            ids = federal.ids or {}
            payload["bioguide-id"] = ids.get("bioguide")
            payload["wikidata-id"] = ids.get("wikidata")
            payload["opensecrets-id"] = ids.get("opensecrets")
            # ballotpedia-slug and govtrack-id are URL-typed in the live
            # Webflow CMS even though the upstream values are bare slugs /
            # numeric IDs. We construct the canonical URLs so the field
            # accepts the value (and the website can render the link
            # directly). The unitedstates YAML occasionally writes the
            # ballotpedia value with spaces instead of underscores
            # (observed for a 2025-elected freshman); normalize that.
            bp = ids.get("ballotpedia")
            if bp:
                payload["ballotpedia-slug"] = (
                    f"https://ballotpedia.org/{bp.replace(' ', '_')}"
                )
            gov_track = ids.get("govtrack")
            if gov_track is not None:
                payload["govtrack-id"] = (
                    f"https://www.govtrack.us/congress/members/{gov_track}"
                )

        # Bio (year-only birth — privacy)
        if federal is not None:
            bd = (federal.bio or {}).get("birthday")
            payload["birth-year"] = self._year_from_iso(bd)
            payload["gender"] = (federal.bio or {}).get("gender")
        elif os_record is not None:
            payload["birth-year"] = self._year_from_iso(os_record.birth_date)
            payload["gender"] = os_record.gender

        # Term span
        if federal is not None and federal.terms:
            payload["term-start"] = self._date_to_webflow_iso(
                federal.first_term.get("start")
            )
            payload["term-end"] = self._date_to_webflow_iso(
                federal.latest_term.get("end")
            )
            payload["seniority-rank"] = federal.latest_term.get("state_rank")

        # Capitol office, phone, contact form, official website
        if federal is not None and federal.terms:
            latest = federal.latest_term
            payload["phone-capitol"] = latest.get("phone")
            payload["office-address-capitol"] = latest.get("address")
            payload["contact-form-url"] = latest.get("contact_form")
            payload["official-website"] = latest.get("url")

        # If the unitedstates dataset didn't have one, fall back to OpenStates
        if os_record is not None:
            if not payload.get("phone-capitol"):
                # Prefer the office classified as capitol
                cap = self._first_office(os_record.offices, "capitol")
                if cap:
                    payload.setdefault("phone-capitol", cap.get("voice"))
                    payload.setdefault(
                        "office-address-capitol", cap.get("address")
                    )
            email, form = split_email_field(os_record.email)
            if email and not payload.get("office-email"):
                # Webflow Email field type lowercases on storage;
                # send canonical form to avoid ChurnPATCH.
                payload["office-email"] = email.lower()
            if form and not payload.get("contact-form-url"):
                payload["contact-form-url"] = form

        # Social media (federal-only data; from unitedstates social YAML)
        if federal is not None:
            social = federal.social or {}
            payload["twitter-handle"] = social.get("twitter")
            payload["facebook-handle"] = social.get("facebook")
            payload["instagram-handle"] = social.get("instagram")
            payload["youtube-handle"] = social.get("youtube")

        # Photo URL (derive from bioguide for federal)
        bioguide = (
            (federal.bioguide_id if federal else None)
            or cms.bioguide_id
            or (
                os_record.get_other_id("bioguide")
                if os_record is not None else None
            )
        )
        if bioguide:
            payload["photo-source-url"] = (
                f"https://unitedstates.github.io/images/congress/450x550/"
                f"{bioguide}.jpg"
            )

        # Strip None values so should_write doesn't see them as upstream
        return {k: v for k, v in payload.items() if v is not None}

    def _build_state_payload(
        self,
        cms: "CMSLegislator",
        os_record: OpenStatesPerson,
    ) -> dict:
        """Build a Webflow PATCH payload for a state legislator (Phase 2).

        OpenStates-only sourcing — state legs don't appear in the
        unitedstates dataset. ``other_identifiers`` is empty for state
        members per the Phase-0 probe, so the federal-only ID fields
        (bioguide, wikidata, opensecrets, ballotpedia, govtrack) and
        the bioguide-derived ``photo-source-url`` are all skipped.

        Phase-2 baseline scope (intentionally conservative — fields that
        are unreliably populated upstream for state legs are left for a
        future iteration with measured data):
          - birth-year, gender
          - phone-capitol, office-address-capitol (from offices[capitol])
          - email / contact-form-url (via split_email_field)
          - photo-source-url (from OpenStates image; per-state CDN URLs
            vary in stability, but the field is URL-typed and accepts
            any URL — quality issues are a separate concern)

        Confirmed not in OpenStates `/people` v3 for state legs (per the
        2026-04-30 probe of 10 FL state legislators):
          - twitter/facebook/instagram/youtube — zero matching URLs in
            ``links[]`` across 10 sampled records; OpenStates simply
            doesn't carry social media for FL state legs.
          - ``current_role`` has only ``title``, ``org_classification``,
            ``district``, ``division_id`` — no start_date / end_date.
          - ``roles[]`` term-history is not returned by /people even
            with default includes.
          - ``other_identifiers`` is empty (no bioguide/wikidata/etc).
          - ``biography`` is empty.

        ``official-website`` IS extractable per-state but the
        link-classification is jurisdiction-specific (FL House uses
        note="member detail page"; other states will differ). Per-state
        overrides happen via ``_STATE_PAYLOAD_OVERRIDES`` so the default
        builder stays generic.
        """
        payload: dict[str, Any] = {}

        # OpenStates profile URL → open-states-url (URL-typed CMS field).
        # Trailing slash stripped to match Webflow's storage format
        # (see _build_federal_payload comment for ChurnPATCH context).
        if os_record.openstates_url:
            payload["open-states-url"] = os_record.openstates_url.rstrip("/")

        payload["birth-year"] = self._year_from_iso(os_record.birth_date)
        payload["gender"] = os_record.gender

        cap = self._first_office(os_record.offices, "capitol")
        if cap:
            payload["phone-capitol"] = cap.get("voice")
            payload["office-address-capitol"] = cap.get("address")

        email, form = split_email_field(os_record.email)
        if email:
            # Lowercase the local-part too — Webflow's Email field type
            # lowercases on storage, so upstream mixed-case emails
            # (observed for some FL Senate records) churned every run
            # when sent verbatim. Email addresses are treated as
            # case-insensitive in practice (RFC 5321 allows case-
            # sensitive local-parts but mainstream MTAs don't).
            #
            # Goes to `office-email` (the official .gov address). The
            # separate `campaign-email` field is editor-managed; the
            # bio sync has no upstream source for it.
            payload["office-email"] = email.lower()
        if form:
            payload["contact-form-url"] = form

        if os_record.image:
            payload["photo-source-url"] = os_record.image

        # Per-state overrides for jurisdiction-specific extraction (e.g.
        # FL extracts official-website from links[] by host pattern).
        # Default registry is empty; overrides return a partial payload
        # dict that gets merged in. None-strip happens after merge.
        override = _STATE_PAYLOAD_OVERRIDES.get(cms.state_code or "")
        if override is not None:
            try:
                extra = override(os_record)
                if extra:
                    payload.update(extra)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "State payload override raised; ignoring",
                    state_code=cms.state_code,
                    error=str(e),
                )

        # Strip None values so should_write doesn't see them as upstream
        return {k: v for k, v in payload.items() if v is not None}

    @staticmethod
    def _year_from_iso(value: Any) -> int | None:
        if not value or not isinstance(value, str) or len(value) < 4:
            return None
        try:
            return int(value[:4])
        except ValueError:
            return None

    @staticmethod
    def _date_to_webflow_iso(value: Any) -> str | None:
        """Coerce a date-only string (YYYY-MM-DD) into Webflow's Date-field
        storage format (``YYYY-MM-DDT00:00:00.000Z``).

        Webflow Date fields round-trip as full ISO 8601 with milliseconds +
        Z suffix. The unitedstates YAML stores term dates as date-only
        strings; without coercion, ``should_write`` sees the date-only
        upstream value as different from the ISO-datetime CMS value on
        every run and re-PATCHes — a ChurnPATCH that hot-loops the
        Webflow rate limiter on every scheduled run.
        """
        if not value or not isinstance(value, str):
            return None
        s = value.strip()
        if not s:
            return None
        if "T" in s:
            # Already datetime-shaped; assume the upstream knows what it's
            # doing and pass through.
            return s
        return f"{s}T00:00:00.000Z"

    @staticmethod
    def _first_office(
        offices: list[dict],
        classification: str,
    ) -> dict | None:
        for o in offices or []:
            if (o or {}).get("classification") == classification:
                return o
        return None

    # ---------- Diff ----------

    def _diff_payload(
        self,
        cms_fields: dict,
        upstream_payload: dict,
        locked_fields: Iterable[str],
    ) -> dict:
        """Return the subset of ``upstream_payload`` that should be PATCHed.

        Applies the cardinal rule per field: never blanks a populated CMS
        value with an empty upstream value, never overwrites locked fields,
        skips no-ops.
        """
        locked_set = set(locked_fields)
        changed: dict[str, Any] = {}
        for field_name, upstream_value in upstream_payload.items():
            cms_value = cms_fields.get(field_name)
            if should_write(
                field_name, cms_value, upstream_value, locked_fields=locked_set
            ):
                changed[field_name] = upstream_value
        return changed

    # ---------- Auto-create + merge detection ----------

    async def _discover_and_create(
        self,
        options: BioSyncOptions,
        report: BioSyncReport,
    ) -> None:
        """Find federal legislators in the unitedstates dataset that are NOT
        in the CMS, and either flag potential merges or create drafts.

        Phase 1 federal-only. State auto-create is Phase 2 scope.
        """
        # Index existing CMS entries by bioguide-id for O(1) membership check.
        cms_bioguides: set[str] = set()
        cms_index: dict[str, list[CMSLegislator]] = {}  # by state code
        resolver = await self._build_jurisdiction_resolver()
        async for item in self.webflow.iter_legislator_items():
            cms = CMSLegislator.from_webflow_item(
                item, jurisdiction_resolver=resolver,
            )
            if cms.bioguide_id:
                cms_bioguides.add(cms.bioguide_id)
            # State-keyed index for merge candidate scan
            if cms.state_code:
                cms_index.setdefault(cms.state_code, []).append(cms)

        async for legislator in self.congress.iter_current():
            if legislator.bioguide_id in cms_bioguides:
                continue
            # Honor historical_since for backfill discovery — only relevant if
            # we extend this loop to iter_historical_since later.
            candidate, signals = self._find_merge_candidate(
                legislator, cms_index
            )
            if candidate:
                report.potential_merges.append({
                    "candidate_webflow_id": candidate.webflow_id,
                    "candidate_name": candidate.name,
                    "new_bioguide_id": legislator.bioguide_id,
                    "new_name": (
                        legislator.name.get("official_full")
                        or legislator.bioguide_id
                    ),
                    "matched_signals": signals,
                })
                continue
            if options.dry_run:
                report.would_create.append({
                    "bioguide_id": legislator.bioguide_id,
                    "name": (
                        legislator.name.get("official_full")
                        or legislator.bioguide_id
                    ),
                })
                continue
            # Live create-as-draft
            payload = self._build_federal_payload(
                CMSLegislator(
                    webflow_id="",
                    slug="",
                    name=(
                        legislator.name.get("official_full")
                        or legislator.bioguide_id
                    ),
                    openstates_id=None,
                    bioguide_id=legislator.bioguide_id,
                    is_federal=True,
                    state_code=None,
                    raw_fields={},
                    raw_item={},
                ),
                legislator,
                None,
            )
            payload["name"] = (
                legislator.name.get("official_full")
                or legislator.bioguide_id
            )
            result = await self.webflow.create_legislator_draft(payload)
            report.would_create.append({
                "bioguide_id": legislator.bioguide_id,
                "name": payload["name"],
                "webflow_id": result.webflow_id,
            })

    def _find_merge_candidate(
        self,
        new_member: CongressLegislator,
        cms_index: dict[str, list[CMSLegislator]],
    ) -> tuple[CMSLegislator | None, list[str]]:
        """Multi-signal scoring (PLAN ``State→federal transition detection``).

        Returns (candidate, matched_signals) where candidate is None unless
        the score is >= 2 (or a decisive bioguide match).
        """
        if not new_member.terms:
            return None, []
        state = (new_member.latest_term.get("state") or "").upper()
        candidates_in_state = cms_index.get(state, [])
        if not candidates_in_state:
            return None, []

        new_full = (
            new_member.name.get("official_full")
            or f"{new_member.name.get('first', '')} {new_member.name.get('last', '')}"
        ).strip().lower()
        new_birth_year = self._year_from_iso((new_member.bio or {}).get("birthday"))
        new_first_start = new_member.first_term.get("start")

        best: CMSLegislator | None = None
        best_signals: list[str] = []
        for cms in candidates_in_state:
            signals: list[str] = []
            cms_name = (cms.name or "").strip().lower()
            cms_last = cms_name.rsplit(" ", 1)[-1] if cms_name else ""
            cms_first_initial = cms_name[:1] if cms_name else ""
            new_last = (new_member.name.get("last") or "").lower()
            new_first_initial = (new_member.name.get("first") or "").lower()[:1]

            if cms_last == new_last and cms_first_initial == new_first_initial:
                signals.append("name_match")
            if new_birth_year:
                cms_birth_year = (
                    cms.raw_fields.get("birth-year") if cms.raw_fields else None
                )
                if cms_birth_year and int(cms_birth_year) == new_birth_year:
                    signals.append("birth_year_match")
            if (
                cms.bioguide_id
                and new_member.bioguide_id == cms.bioguide_id
            ):
                signals.append("bioguide_match")

            score = 2 if "bioguide_match" in signals else len(signals)
            if score >= 2 and len(signals) > len(best_signals):
                best = cms
                best_signals = signals
        return best, best_signals

    # ---------- Jurisdiction resolver ----------

    async def _build_jurisdiction_resolver(
        self,
    ) -> Callable[[Any], str | None]:
        """Return a callable that resolves a ``jurisdiction`` field value
        (ref-id, list-of-ref-ids, or already-2-letter code) to a
        normalized two-letter state code or None.

        The Jurisdictions CMS collection is fetched once per
        WebflowLookupService instance and cached. Calling this method
        again on the same pipeline returns a resolver bound to the same
        cached mapping.
        """
        mapping = await self.webflow.get_jurisdiction_mapping()

        def resolve(value: Any) -> str | None:
            return WebflowLookupService.resolve_jurisdiction_ref(
                value, mapping
            )

        return resolve

    # ---------- Audits (step 6) ----------

    async def audit_federal_join_keys(self) -> AuditReport:
        """Audit A — Federal join-key coverage.

        Surfaces every federal CMS record that lacks BOTH ``openstatesid``
        and ``bioguide-id``. Without at least one of these the orchestrator
        cannot resolve the upstream record (a name-based lookup has known
        false-positive risk and is intentionally not used).

        **Action on findings:** editors manually populate ``bioguide-id``
        (preferred — stable across re-elections) before auto-create is
        enabled. Single-pass remediation.

        Wraps any WebflowError as ``aborted=True`` so the endpoint can
        return a partial report rather than raising.
        """
        flagged: list[AuditEntry] = []
        total = 0
        resolver = await self._build_jurisdiction_resolver()
        try:
            async for item in self.webflow.iter_legislator_items():
                cms = CMSLegislator.from_webflow_item(
                    item, jurisdiction_resolver=resolver,
                )
                if not cms.is_federal:
                    continue
                total += 1
                if not cms.openstates_id and not cms.bioguide_id:
                    flagged.append(
                        AuditEntry(
                            webflow_id=cms.webflow_id,
                            slug=cms.slug,
                            name=cms.name,
                            seat=cms.seat_slugs,
                            state_code=cms.state_code,
                            openstates_id=None,
                            bioguide_id=None,
                        )
                    )
        except WebflowError as e:
            return AuditReport(
                audit_name="A",
                total_scanned=total,
                flagged_count=len(flagged),
                flagged=flagged,
                aborted=True,
                abort_reason=f"{type(e).__name__}: {e}",
            )
        return AuditReport(
            audit_name="A",
            total_scanned=total,
            flagged_count=len(flagged),
            flagged=flagged,
        )

    async def audit_state_join_keys(
        self,
        jurisdiction: str | None = None,
    ) -> AuditReport:
        """Audit C — Pre-existing state CMS records lacking ``openstatesid``.

        Catches the case Audit B misses: state CMS records that already
        existed before the bulk-create push and never had their
        ``openstatesid`` populated. Without this audit, auto-create could
        silently duplicate a pre-existing record by creating a new draft
        for the same person.

        ``jurisdiction`` filters to a single state code (e.g. "FL"); when
        omitted, all non-federal records are scanned.

        **Action on findings:** editors populate ``openstatesid`` (preferred)
        or mark the record with the future ``bio-sync-locked`` field
        (Phase 2) before per-jurisdiction auto-create is enabled.

        Wraps any WebflowError as ``aborted=True``.
        """
        flagged: list[AuditEntry] = []
        total = 0
        wanted = WebflowLookupService._normalize_state_code(jurisdiction) if jurisdiction else None
        resolver = await self._build_jurisdiction_resolver()
        try:
            async for item in self.webflow.iter_legislator_items():
                cms = CMSLegislator.from_webflow_item(
                    item, jurisdiction_resolver=resolver,
                )
                if cms.is_federal:
                    continue
                if wanted and cms.state_code != wanted:
                    continue
                total += 1
                if not cms.openstates_id:
                    flagged.append(
                        AuditEntry(
                            webflow_id=cms.webflow_id,
                            slug=cms.slug,
                            name=cms.name,
                            seat=cms.seat_slugs,
                            state_code=cms.state_code,
                            openstates_id=None,
                            bioguide_id=cms.bioguide_id,
                        )
                    )
        except WebflowError as e:
            return AuditReport(
                audit_name="C",
                total_scanned=total,
                flagged_count=len(flagged),
                flagged=flagged,
                jurisdiction=wanted,
                aborted=True,
                abort_reason=f"{type(e).__name__}: {e}",
            )
        return AuditReport(
            audit_name="C",
            total_scanned=total,
            flagged_count=len(flagged),
            flagged=flagged,
            jurisdiction=wanted,
        )

    async def audit_bulk_import_readiness(self) -> AuditReport:
        """Audit B — Bulk-import readiness.

        Run before enabling the bio-sync scheduler. Surfaces two failure
        classes that would break a scheduled sync:

        1. **Records missing ``openstatesid``** — the bulk-create flow is
           supposed to populate it on every new record. Missing values
           mean the editor toolchain skipped some, OR the records are
           pre-existing entries Audit C should also catch. Flagged with
           ``reason="missing_openstatesid"``.

        2. **Duplicate ``openstatesid``** — two or more CMS records share
           the same OpenStates id. The bulk-create flow silently created
           a second draft for someone who already had a CMS entry. Each
           offending record is flagged with
           ``reason="duplicate_openstatesid:<the-shared-id>"`` so editors
           can group them in the report.

        Federal records get the same checks (the strict bulk-import rule
        is "every record has openstatesid", with bioguide-id as an
        additional join key, not an alternative).

        Wraps any WebflowError as ``aborted=True``.
        """
        flagged: list[AuditEntry] = []
        total = 0
        # ref_id → list of records (for duplicate detection in second pass)
        by_openstates_id: dict[str, list[AuditEntry]] = {}
        resolver = await self._build_jurisdiction_resolver()
        try:
            async for item in self.webflow.iter_legislator_items():
                cms = CMSLegislator.from_webflow_item(
                    item, jurisdiction_resolver=resolver,
                )
                total += 1
                entry = AuditEntry(
                    webflow_id=cms.webflow_id,
                    slug=cms.slug,
                    name=cms.name,
                    seat=cms.seat_slugs,
                    state_code=cms.state_code,
                    openstates_id=cms.openstates_id,
                    bioguide_id=cms.bioguide_id,
                )
                if not cms.openstates_id:
                    flagged.append(
                        replace(entry, reason="missing_openstatesid")
                    )
                    continue
                by_openstates_id.setdefault(cms.openstates_id, []).append(
                    entry
                )
        except WebflowError as e:
            return AuditReport(
                audit_name="B",
                total_scanned=total,
                flagged_count=len(flagged),
                flagged=flagged,
                aborted=True,
                abort_reason=f"{type(e).__name__}: {e}",
            )

        # Second pass: surface every record that shares an openstatesid
        # with another. Each entry's reason carries the conflicting id so
        # editors can group them.
        for os_id, entries in by_openstates_id.items():
            if len(entries) > 1:
                for e in entries:
                    flagged.append(
                        replace(e, reason=f"duplicate_openstatesid:{os_id}")
                    )

        return AuditReport(
            audit_name="B",
            total_scanned=total,
            flagged_count=len(flagged),
            flagged=flagged,
        )

    # ---------- Helpers ----------

    @staticmethod
    def _matches_jurisdiction(cms: CMSLegislator, jurisdiction: str) -> bool:
        """Filter CMS records by jurisdiction option ('us' or state code).

        Compares against the precomputed ``cms.state_code`` (resolved
        from the Jurisdictions multi-reference field). State-code
        comparison is exact-uppercase; "us"/"US" matches federal records.
        """
        wanted = (jurisdiction or "").strip().upper()
        if wanted == "US":
            return cms.is_federal
        return cms.state_code == wanted
