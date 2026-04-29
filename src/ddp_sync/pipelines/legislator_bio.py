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

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Literal

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


# ---------- CMS index helpers ----------


@dataclass
class CMSLegislator:
    """Convenience wrapper around a Webflow Legislators item.

    Captures the join keys and any field values the orchestrator needs to
    diff against. The full upstream item is preserved as ``raw`` for any
    downstream code that needs other fields.
    """

    webflow_id: str
    slug: str
    name: str
    openstates_id: str | None
    bioguide_id: str | None
    is_federal: bool
    raw_fields: dict
    raw_item: dict

    @classmethod
    def from_webflow_item(cls, item: dict) -> "CMSLegislator":
        fields = item.get("fieldData") or {}
        chamber = (fields.get("chamber") or "").lower()
        is_federal = chamber in {"senate", "house", "us senate", "us house"}
        return cls(
            webflow_id=item.get("id") or "",
            slug=fields.get("slug") or "",
            name=fields.get("name") or "",
            openstates_id=(fields.get("openstatesid") or None),
            bioguide_id=(fields.get("bioguide-id") or None),
            is_federal=is_federal,
            raw_fields=fields,
            raw_item=item,
        )


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
            )
        else:
            self.openstates = openstates

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
            except (WebflowRateLimitError, OpenStatesRateLimitError) as e:
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

    # ---------- Processing ----------

    async def _process_cms_records(
        self,
        options: BioSyncOptions,
        report: BioSyncReport,
    ) -> None:
        """Iterate every Legislators CMS item and apply the sync logic."""
        async for item in self.webflow.iter_legislator_items():
            cms = CMSLegislator.from_webflow_item(item)
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
            # Phase 2 work — state path. Skip with a clear marker for now.
            logger.info(
                "Skipping state legislator (Phase 2 work)",
                webflow_id=cms.webflow_id,
                name=cms.name,
            )
            return

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

        # Live PATCH
        result = await self.webflow.update_legislator_fields(
            cms.webflow_id, changed
        )
        report.would_patch.append({
            "webflow_id": cms.webflow_id,
            "name": cms.name,
            "changed_fields": sorted(changed.keys()),
            "dropped_fields": sorted(result.dropped_fields),
        })

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

        # Cross-source IDs (mostly federal-only — the unitedstates dataset
        # publishes them all with high coverage, so prefer that source).
        if federal is not None:
            ids = federal.ids or {}
            payload["bioguide-id"] = ids.get("bioguide")
            payload["wikidata-id"] = ids.get("wikidata")
            payload["opensecrets-id"] = ids.get("opensecrets")
            payload["ballotpedia-slug"] = ids.get("ballotpedia")
            gov_track = ids.get("govtrack")
            if gov_track is not None:
                payload["govtrack-id"] = str(gov_track)

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
            payload["term-start"] = federal.first_term.get("start")
            payload["term-end"] = federal.latest_term.get("end")
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
            if email and not payload.get("email"):
                payload["email"] = email
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

    @staticmethod
    def _year_from_iso(value: Any) -> int | None:
        if not value or not isinstance(value, str) or len(value) < 4:
            return None
        try:
            return int(value[:4])
        except ValueError:
            return None

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
        async for item in self.webflow.iter_legislator_items():
            cms = CMSLegislator.from_webflow_item(item)
            if cms.bioguide_id:
                cms_bioguides.add(cms.bioguide_id)
            # State-keyed index for merge candidate scan
            state = (cms.raw_fields.get("state-code") or "").upper() or None
            if state:
                cms_index.setdefault(state, []).append(cms)

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

    # ---------- Helpers ----------

    @staticmethod
    def _matches_jurisdiction(cms: CMSLegislator, jurisdiction: str) -> bool:
        """Filter CMS records by jurisdiction option ('us' or state code)."""
        wanted = (jurisdiction or "").lower()
        if wanted == "us":
            return cms.is_federal
        # State match — the orchestrator's CMS reader doesn't currently
        # populate a normalized state code, so this is a heuristic over the
        # raw fields. Phase 2 may add an explicit state-code field.
        state = (
            cms.raw_fields.get("state-code")
            or cms.raw_fields.get("state")
            or ""
        ).lower()
        return state == wanted
