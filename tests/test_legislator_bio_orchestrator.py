"""Phase 1 step 8: end-to-end integration tests for ``LegislatorBioPipeline.run()``.

These exercise the full orchestrator flow with the source dependencies
mocked out at the module-boundary level (Webflow / OpenStates / congress-
legislators). Foundation tests in test_legislator_bio_foundation.py cover
the individual pieces; this file ensures they compose correctly.

Coverage:
- happy path via OpenStates (federal current member)
- bioguide-fallback path (departed federal member; OpenStates returns None)
- state record gets logged + skipped (Phase 2 stub)
- dry-run emits no PATCHes and no Zapier alert
- per-record WebflowError continues the run; lands in report.errors
- WebflowRateLimitError aborts the run cleanly; alert STILL fires with
  on_failure=True (the editor-most-needs-this-alert case)
- jurisdiction filter "us" passes only federal records
- limit option caps processed records

Plus: round-12 follow-up — a test that pins lock-release behavior when
``_fetch_jurisdiction_mapping_fresh`` raises an unexpected exception
(the implementation has try/except inside the fetch, so it returns {}
on errors; the test ensures the lock still releases via ``async with``
even if the contract is ever broken).
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.pipelines.legislator_bio import (
    DEFAULT_LARGE_CHANGES_THRESHOLD,
    BioSyncOptions,
    BioSyncReport,
    LegislatorBioPipeline,
)
from ddp_sync.services.congress_legislators import CongressLegislator
from ddp_sync.services.openstates_people import (
    OpenStatesError,
    OpenStatesPerson,
    OpenStatesRateLimitError,
)
from ddp_sync.services.webflow_lookup import (
    WebflowError,
    WebflowLookupService,
    WebflowPatchResult,
    WebflowRateLimitError,
)


# ---------- Fixtures ----------


_DEFAULT_JURIS_MAPPING = {
    "juris-fl": "FL",
    "juris-va": "VA",
    "juris-ma": "MA",
    "juris-us": "US",
}


_TEST_SEAT_REFS = {
    "Senate": ["66316e0956dc73af879134b4"],   # us-senate (federal)
    "House": ["66316e20ae88354aed5df702"],    # us-house (federal)
    "upper": ["655288ef928edb12830673e8"],    # state-senate
    "lower": ["655288ef928edb1283067463"],    # state-house
}


def _cms(
    *,
    item_id: str,
    name: str = "Test",
    chamber: str = "Senate",
    openstatesid: str | None = None,
    bioguide_id: str | None = None,
    jurisdiction_ref: list | str | None = None,
    extra_fields: dict | None = None,
) -> dict:
    fields: dict[str, Any] = {
        "name": name,
        "slug": name.lower().replace(" ", "-"),
        "seat": _TEST_SEAT_REFS.get(chamber, []),
    }
    if openstatesid:
        fields["openstatesid"] = openstatesid
    if bioguide_id:
        fields["bioguide-id"] = bioguide_id
    if jurisdiction_ref is not None:
        fields["jurisdiction"] = jurisdiction_ref
    if extra_fields:
        fields.update(extra_fields)
    return {"id": item_id, "fieldData": fields}


def _os_person(
    *,
    openstates_id: str = "ocd-person/x",
    name: str = "Test",
    party: str = "Republican",
    chamber: str = "upper",
    state: str = "FL",
    bioguide: str | None = None,
    is_federal: bool = False,
    email: str | None = None,
    capitol_phone: str | None = None,
    capitol_address: str | None = None,
    birth_date: str | None = None,
    gender: str | None = None,
    image: str | None = None,
) -> OpenStatesPerson:
    raw: dict[str, Any] = {
        "id": openstates_id,
        "name": name,
        "party": party,
        "current_role": {
            "org_classification": chamber,
            "district": "1",
            "division_id": f"ocd-division/country:us/state:{state.lower()}",
        },
    }
    if is_federal:
        raw["jurisdiction"] = {"name": "United States"}
    else:
        raw["jurisdiction"] = {"name": "Florida"}
    if bioguide:
        raw["other_identifiers"] = [{"scheme": "bioguide", "identifier": bioguide}]
    if email:
        raw["email"] = email
    if birth_date:
        raw["birth_date"] = birth_date
    if gender:
        raw["gender"] = gender
    if image:
        raw["image"] = image
    if capitol_phone or capitol_address:
        raw["offices"] = [{
            "classification": "capitol",
            "voice": capitol_phone,
            "address": capitol_address,
        }]
    return OpenStatesPerson.from_api(raw)


def _fed(
    *,
    bioguide: str = "X001",
    first: str = "Test",
    last: str = "Person",
    birthday: str | None = "1960-01-01",
    state: str = "FL",
    term_start: str = "2023-01-03",
    term_end: str = "2027-01-03",
    type_: str = "sen",
    contact_form: str | None = None,
    phone: str = "202-000-0000",
    address: str = "100 Hart Senate Office Building",
    url: str = "https://example.senate.gov",
    twitter: str | None = None,
    facebook: str | None = None,
    is_historical: bool = False,
) -> CongressLegislator:
    name: dict[str, Any] = {
        "first": first,
        "last": last,
        "official_full": f"{first} {last}",
    }
    bio: dict[str, Any] = {"gender": "M"}
    if birthday:
        bio["birthday"] = birthday
    term: dict[str, Any] = {
        "type": type_,
        "start": term_start,
        "end": term_end,
        "state": state,
        "phone": phone,
        "address": address,
        "url": url,
        "state_rank": "junior",
    }
    if contact_form:
        term["contact_form"] = contact_form
    social: dict[str, Any] = {}
    if twitter:
        social["twitter"] = twitter
    if facebook:
        social["facebook"] = facebook
    return CongressLegislator(
        bioguide_id=bioguide,
        name=name,
        bio=bio,
        terms=[term],
        ids={
            "bioguide": bioguide,
            "wikidata": "Q123",
            "opensecrets": "N00012345",
            "ballotpedia": f"{first} {last}",
            "govtrack": 412345,
        },
        social=social,
        is_historical=is_historical,
    )


def _build_pipeline(
    *,
    cms_items: list[dict],
    openstates_responses: dict[str, OpenStatesPerson | None] | None = None,
    openstates_errors: dict[str, Exception] | None = None,
    federal_records: dict[str, CongressLegislator] | None = None,
    jurisdiction_mapping: dict[str, str] | None = None,
    webhook_url: str = "",
    patch_recorder: list | None = None,
    patch_error: Exception | None = None,
    patch_func=None,  # round-13: callable override for mixed-success cases
):
    """Wire up a fully-mocked LegislatorBioPipeline for run() tests.

    ``MagicMock(spec=...)`` is used on the service mocks so attribute access
    for methods that don't exist on the real class raises (round-13 fix —
    catches signature drift between the test fixture and production code).
    """
    from ddp_sync.services.congress_legislators import CongressLegislatorsSource
    from ddp_sync.services.openstates_people import OpenStatesPeopleClient

    openstates_responses = openstates_responses or {}
    openstates_errors = openstates_errors or {}
    federal_records = federal_records or {}
    jurisdiction_mapping = (
        jurisdiction_mapping if jurisdiction_mapping is not None
        else _DEFAULT_JURIS_MAPPING
    )

    async def fake_iter():
        for item in cms_items:
            yield item

    # spec= catches drift if iter_legislator_items / get_jurisdiction_mapping /
    # update_legislator_fields ever get renamed or dropped on the real class.
    webflow = MagicMock(spec=WebflowLookupService)
    webflow.iter_legislator_items = fake_iter
    webflow.get_jurisdiction_mapping = AsyncMock(return_value=jurisdiction_mapping)

    if patch_func is not None:
        webflow.update_legislator_fields = patch_func
    elif patch_error is not None:
        webflow.update_legislator_fields = AsyncMock(side_effect=patch_error)
    else:
        async def fake_patch(webflow_id, fields, *, publish=True, api_key=None):
            if patch_recorder is not None:
                patch_recorder.append((webflow_id, dict(fields)))
            return WebflowPatchResult(success=True, webflow_id=webflow_id)
        webflow.update_legislator_fields = fake_patch

    async def fake_fetch_by_id(osid):
        if osid in openstates_errors:
            raise openstates_errors[osid]
        return openstates_responses.get(osid)
    openstates = MagicMock(spec=OpenStatesPeopleClient)
    openstates.fetch_by_id = AsyncMock(side_effect=fake_fetch_by_id)

    congress = MagicMock(spec=CongressLegislatorsSource)
    congress.warm_cache = AsyncMock()
    async def fake_get_by_bioguide(bg):
        return federal_records.get(bg)
    congress.get_by_bioguide = AsyncMock(side_effect=fake_get_by_bioguide)
    async def empty_iter():
        if False:
            yield None
    congress.iter_current = empty_iter

    settings = MagicMock()
    settings.openstates_api_key = "fake-key"
    settings.zapier_webhook_url = webhook_url

    return LegislatorBioPipeline(
        settings=settings,
        webflow=webflow,
        congress=congress,
        openstates=openstates,
    )


# ---------- Happy paths ----------


@pytest.mark.asyncio
async def test_run_federal_via_openstates_full_flow():
    """Federal current member: OpenStates resolves, congress-legislators
    enriches via bioguide. Expected federal fields land in PATCH payload."""
    cms = [_cms(
        item_id="wf-1",
        name="Rick Scott",
        chamber="Senate",
        openstatesid="ocd-person/rs",
        jurisdiction_ref=["juris-us"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/rs",
        name="Rick Scott",
        chamber="upper",
        state="FL",
        bioguide="S001217",
        is_federal=True,
        email="https://www.rickscott.senate.gov/contact/contact",
    )
    fed = _fed(
        bioguide="S001217", first="Rick", last="Scott",
        birthday="1952-12-01",
        twitter="SenRickScott", facebook="RickScottSenOffice",
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/rs": os_record},
        federal_records={"S001217": fed},
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.aborted is False
    assert report.cms_items_seen == 1
    assert report.items_resolved_via_openstates == 1
    assert report.items_resolved_via_bioguide_fallback == 0
    assert len(patches) == 1
    webflow_id, fields = patches[0]
    assert webflow_id == "wf-1"
    # Federal fields populated from unitedstates dataset
    assert fields["bioguide-id"] == "S001217"
    assert fields["wikidata-id"] == "Q123"
    assert fields["birth-year"] == 1952
    assert fields["twitter-handle"] == "SenRickScott"
    # term-start coerced to Webflow Date format to prevent ChurnPATCH
    # (Webflow Date fields store as ISO-datetime; date-only would diff
    # against stored value on every run).
    assert fields["term-start"] == "2023-01-03T00:00:00.000Z"
    # Federal email-as-URL routed to contact-form-url
    assert fields.get("contact-form-url") == "https://www.rickscott.senate.gov/contact/contact"
    assert "email" not in fields  # bare email field stays empty for federal
    # ballotpedia-slug and govtrack-id are URL-typed fields in the live
    # Webflow CMS; the orchestrator constructs canonical URLs (and
    # normalizes ballotpedia value's spaces → underscores).
    assert fields["ballotpedia-slug"] == "https://ballotpedia.org/Rick_Scott"
    assert fields["govtrack-id"] == "https://www.govtrack.us/congress/members/412345"


@pytest.mark.asyncio
async def test_run_federal_bioguide_fallback_for_departed_member():
    """Departed federal: OpenStates returns None (or 404 caller path);
    orchestrator falls back to bioguide-id → congress-legislators.
    Karen Bass case from the Phase-0 probe."""
    cms = [_cms(
        item_id="wf-2",
        name="Karen Bass",
        chamber="House",
        bioguide_id="B001270",
        jurisdiction_ref=["juris-us"],
    )]
    fed = _fed(
        bioguide="B001270", first="Karen", last="Bass",
        birthday="1953-10-03",
        term_start="2011-01-05", term_end="2023-01-03",
        type_="rep",
        is_historical=True,
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={},  # nothing
        federal_records={"B001270": fed},
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.cms_items_seen == 1
    assert report.items_resolved_via_openstates == 0
    assert report.items_resolved_via_bioguide_fallback == 1
    assert len(patches) == 1
    _, fields = patches[0]
    # bioguide-id is already on the CMS record (it's the join key here)
    # so it's correctly deduped out of the PATCH per the cardinal rule.
    # The genuinely-new fields from the federal record:
    assert fields["birth-year"] == 1953
    assert fields["term-start"] == "2011-01-05T00:00:00.000Z"
    assert fields["term-end"] == "2023-01-03T00:00:00.000Z"
    assert fields["gender"] == "M"


@pytest.mark.asyncio
async def test_run_state_record_with_no_writable_fields_no_ops():
    """A state record where OpenStates resolves but has no populated bio
    fields produces an empty payload — no PATCH, but resolved successfully
    (not flagged as orphan). Confirms the diff-then-skip path works for
    state legs, not just federal."""
    cms = [_cms(
        item_id="wf-3",
        name="FL state rep",
        chamber="lower",
        openstatesid="ocd-person/fl-1",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-1",
        name="FL state rep",
        chamber="lower",
        state="FL",
        is_federal=False,
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-1": os_record},
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.cms_items_seen == 1
    assert report.items_resolved_via_openstates == 1
    assert report.upstream_orphans == []
    assert len(patches) == 0  # nothing to write; cardinal-rule no-op
    assert report.errors == []


@pytest.mark.asyncio
async def test_run_state_legislator_full_flow():
    """State legs get bio + contact + photo PATCHed from OpenStates only.
    No federal-only IDs (bioguide, wikidata, opensecrets, ballotpedia,
    govtrack); no social handles (Phase-2.5); no term dates."""
    cms = [_cms(
        item_id="wf-fl-1",
        name="Jane State",
        chamber="lower",
        openstatesid="ocd-person/fl-1",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-1",
        name="Jane State",
        chamber="lower",
        state="FL",
        is_federal=False,
        birth_date="1972-05-04",
        gender="F",
        email="jane@myfloridahouse.gov",
        capitol_phone="850-555-0100",
        capitol_address="402 House Office Building, Tallahassee, FL",
        image="https://www.flhouse.gov/Sections/Representatives/photos/jane.jpg",
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-1": os_record},
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.aborted is False
    assert report.errors == []
    assert len(patches) == 1
    webflow_id, fields = patches[0]
    assert webflow_id == "wf-fl-1"

    # State-sourced fields are populated
    assert fields["birth-year"] == 1972
    assert fields["gender"] == "F"
    assert fields["email"] == "jane@myfloridahouse.gov"
    assert fields["phone-capitol"] == "850-555-0100"
    assert fields["office-address-capitol"] == (
        "402 House Office Building, Tallahassee, FL"
    )
    assert fields["photo-source-url"] == (
        "https://www.flhouse.gov/Sections/Representatives/photos/jane.jpg"
    )

    # Federal-only IDs are NOT in the state payload
    for federal_id in (
        "bioguide-id", "wikidata-id", "opensecrets-id",
        "ballotpedia-slug", "govtrack-id",
    ):
        assert federal_id not in fields, (
            f"federal-only id {federal_id} leaked into state payload"
        )

    # Phase-2.5 follow-ups not yet implemented for state
    for deferred in (
        "twitter-handle", "facebook-handle", "instagram-handle",
        "youtube-handle", "term-start", "term-end", "seniority-rank",
        "official-website",
    ):
        assert deferred not in fields, (
            f"{deferred} should be Phase-2.5 work, not in baseline state payload"
        )


@pytest.mark.asyncio
async def test_run_state_legislator_email_url_routes_to_contact_form_url():
    """Some state legs (rare, but exists) have a contact-form URL in the
    OpenStates email field; same split_email_field treatment as federal."""
    cms = [_cms(
        item_id="wf-fl-2",
        name="State Form",
        chamber="lower",
        openstatesid="ocd-person/fl-2",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-2",
        chamber="lower",
        state="FL",
        is_federal=False,
        email="https://www.flhouse.gov/contact/form",
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-2": os_record},
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    _, fields = patches[0]
    assert fields["contact-form-url"] == "https://www.flhouse.gov/contact/form"
    assert "email" not in fields


@pytest.mark.asyncio
async def test_run_state_legislator_no_capitol_office_skips_phone_and_address():
    """When OpenStates returns no capitol office, the orchestrator
    omits phone-capitol + office-address-capitol entirely (cardinal rule
    won't blank populated CMS values via empty upstream)."""
    cms = [_cms(
        item_id="wf-fl-3",
        name="No Office",
        chamber="upper",
        openstatesid="ocd-person/fl-3",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-3",
        chamber="upper",
        state="FL",
        is_federal=False,
        birth_date="1980-03-15",
        # No capitol_phone / capitol_address → no offices entry
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-3": os_record},
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    _, fields = patches[0]
    assert fields["birth-year"] == 1980
    assert "phone-capitol" not in fields
    assert "office-address-capitol" not in fields


@pytest.mark.asyncio
async def test_run_dry_run_emits_no_patches_no_alert():
    """Dry-run: would_patch populated; no PATCH calls; no Zapier alert."""
    cms = [_cms(
        item_id="wf-4",
        name="Rick",
        chamber="Senate",
        openstatesid="ocd-person/rs",
        jurisdiction_ref=["juris-us"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/rs",
        chamber="upper", state="FL",
        bioguide="S001217", is_federal=True,
    )
    fed = _fed(bioguide="S001217")
    patches: list = []
    alerts: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/rs": os_record},
        federal_records={"S001217": fed},
        webhook_url="https://example.com/zap",  # set, but should NOT be called
        patch_recorder=patches,
    )
    with patch(
        "ddp_sync.pipelines.legislator_bio.push_bio_sync_alert",
        new=lambda url, report, **kw: alerts.append((url, report)) or True,
    ):
        report = await pipeline.run(BioSyncOptions(dry_run=True))

    assert len(patches) == 0
    assert len(report.would_patch) == 1     # diff was computed
    assert alerts == []                      # NO Zapier on dry-run


# ---------- Error paths ----------


@pytest.mark.asyncio
async def test_run_per_record_webflow_error_continues_run():
    """One record fails with WebflowError; orchestrator logs to errors[]
    and continues with the next record."""
    cms = [
        _cms(item_id="wf-good", name="Good", chamber="Senate",
             openstatesid="ocd-person/good", jurisdiction_ref=["juris-us"]),
        _cms(item_id="wf-bad", name="Bad", chamber="Senate",
             openstatesid="ocd-person/bad", jurisdiction_ref=["juris-us"]),
    ]
    os_responses = {
        "ocd-person/good": _os_person(
            openstates_id="ocd-person/good", chamber="upper", state="FL",
            bioguide="G001", is_federal=True),
        "ocd-person/bad": _os_person(
            openstates_id="ocd-person/bad", chamber="upper", state="FL",
            bioguide="B001", is_federal=True),
    }
    feds = {
        "G001": _fed(bioguide="G001"),
        "B001": _fed(bioguide="B001"),
    }
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses=os_responses,
        federal_records=feds,
        patch_error=WebflowError("simulated 4xx"),
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))

    # Both records attempted; both PATCHes failed
    assert report.cms_items_seen == 2
    assert report.aborted is False    # per-record errors don't abort
    assert len(report.errors) == 2
    assert "WebflowError" in report.errors[0]


@pytest.mark.asyncio
async def test_run_aborts_cleanly_on_rate_limit():
    """WebflowRateLimitError aborts the run (rate-limit storms can't be
    papered over). report.aborted=True; partial state preserved; alert
    STILL fires."""
    cms = [_cms(
        item_id="wf-1", chamber="Senate",
        openstatesid="ocd-person/rs", jurisdiction_ref=["juris-us"],
    )]
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={
            "ocd-person/rs": _os_person(
                openstates_id="ocd-person/rs", chamber="upper",
                bioguide="S001", is_federal=True),
        },
        federal_records={"S001": _fed(bioguide="S001")},
        webhook_url="https://example.com/zap",
        patch_error=WebflowRateLimitError("persistent 429"),
    )
    alerts: list = []
    with patch(
        "ddp_sync.pipelines.legislator_bio.push_bio_sync_alert",
        new=lambda url, report, **kw: alerts.append(report) or True,
    ):
        report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.aborted is True
    assert "Rate-limit error" in (report.abort_reason or "")
    # Alert fires on aborted run — editors most need to know about this
    assert len(alerts) == 1
    assert alerts[0].aborted is True


# ---------- Filtering ----------


@pytest.mark.asyncio
async def test_run_jurisdiction_filter_us_only_processes_federal():
    """jurisdiction='us' filter excludes state records before processing."""
    cms = [
        _cms(item_id="wf-fed", chamber="Senate",
             openstatesid="ocd-person/fed",
             jurisdiction_ref=["juris-us"]),
        _cms(item_id="wf-st", chamber="lower",
             openstatesid="ocd-person/st",
             jurisdiction_ref=["juris-fl"]),
    ]
    os_responses = {
        "ocd-person/fed": _os_person(
            openstates_id="ocd-person/fed", chamber="upper",
            bioguide="F001", is_federal=True),
        "ocd-person/st": _os_person(
            openstates_id="ocd-person/st", chamber="lower", is_federal=False),
    }
    feds = {"F001": _fed(bioguide="F001")}
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses=os_responses,
        federal_records=feds,
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False, jurisdiction="us"))

    # Only the federal record was processed
    assert report.cms_items_seen == 2  # both seen at iteration time
    # State filter excluded the state record before resolution
    assert report.items_resolved_via_openstates == 1
    assert len(patches) == 1
    assert patches[0][0] == "wf-fed"


# ---------- Jurisdiction-cache lock release on raising fetch (round-12 follow-up) ----------


@pytest.mark.asyncio
async def test_jurisdiction_cache_lock_releases_when_fetch_raises():
    """Round-12 follow-up: the implementation has try/except inside
    _fetch_jurisdiction_mapping_fresh so it returns {} on errors and never
    raises. But if a future code path or test stub bypasses that and lets
    an exception propagate, the asyncio.Lock must still release (via
    `async with` __aexit__) so subsequent callers aren't deadlocked.
    """
    settings = MagicMock()
    settings.webflow_scheduler_api_key = "sched"
    settings.webflow_votebot_api_key = "votebot"
    settings.webflow_jurisdiction_collection_id = "juris"
    settings.webflow_bills_collection_id = "b"
    settings.webflow_legislators_collection_id = "l"
    svc = WebflowLookupService(settings)

    # Force the fetch to raise (bypassing the inner try/except)
    async def raising_fetch():
        raise RuntimeError("simulated fetch crash")
    svc._fetch_jurisdiction_mapping_fresh = raising_fetch

    # First call: raises out of the lock — but the lock should release
    with pytest.raises(RuntimeError):
        await svc.get_jurisdiction_mapping()

    # Second call should be able to acquire the lock (proving the first
    # caller released it). Replace the fetch with a successful stub.
    async def good_fetch():
        return {"juris-fl": "FL"}
    svc._fetch_jurisdiction_mapping_fresh = good_fetch

    # If the lock were stuck, this would deadlock or timeout; passing
    # confirms the lock released.
    result = await svc.get_jurisdiction_mapping()
    assert result == {"juris-fl": "FL"}


# ---------- Round-13 follow-ups: safety-valve integration coverage ----------


@pytest.mark.asyncio
async def test_run_locked_fields_excluded_from_patch():
    """Round-13 fix: the locked_fields option flows through run() to the
    diff and excludes the named fields from the PATCH even when upstream
    has different values. Pins the editor opt-out contract — the cardinal
    rule's ``LOCKED_FIELDS`` skip applied via ``BioSyncOptions.locked_fields``
    rather than a global config.
    """
    cms = [_cms(
        item_id="wf-1",
        chamber="Senate",
        openstatesid="ocd-person/x",
        jurisdiction_ref=["juris-us"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/x",
        chamber="upper",
        bioguide="X001",
        is_federal=True,
    )
    fed = _fed(
        bioguide="X001",
        birthday="1960-01-01",
        twitter="LockedTwitter",
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/x": os_record},
        federal_records={"X001": fed},
        patch_recorder=patches,
    )

    # Lock birth-year and twitter-handle. Upstream HAS them (1960 and
    # LockedTwitter), but they should be excluded from the PATCH.
    report = await pipeline.run(BioSyncOptions(
        dry_run=False,
        locked_fields=("birth-year", "twitter-handle"),
    ))

    assert report.aborted is False
    assert len(patches) == 1
    _, fields = patches[0]
    # Locked fields excluded
    assert "birth-year" not in fields, (
        "birth-year was in locked_fields but appeared in PATCH payload"
    )
    assert "twitter-handle" not in fields, (
        "twitter-handle was in locked_fields but appeared in PATCH payload"
    )
    # Other federal fields still get patched (verifies the lock is
    # surgical, not a wholesale skip)
    assert fields.get("wikidata-id") == "Q123"
    assert fields.get("term-start") == "2023-01-03T00:00:00.000Z"


def _build_n_federal_records(n: int):
    """Helper for threshold tests — generates ``n`` federal CMS records
    with matching OpenStates and congress-legislators responses."""
    cms = [
        _cms(
            item_id=f"wf-{i}",
            name=f"Senator {i}",
            chamber="Senate",
            openstatesid=f"ocd-person/sen-{i}",
            jurisdiction_ref=["juris-us"],
        )
        for i in range(n)
    ]
    os_responses = {
        f"ocd-person/sen-{i}": _os_person(
            openstates_id=f"ocd-person/sen-{i}",
            chamber="upper",
            bioguide=f"S{i:06d}",
            is_federal=True,
        )
        for i in range(n)
    }
    federal_records = {
        f"S{i:06d}": _fed(bioguide=f"S{i:06d}", first=f"Sen{i}", last="X")
        for i in range(n)
    }
    return cms, os_responses, federal_records


@pytest.mark.asyncio
async def test_run_large_changes_alert_fires_when_threshold_exceeded():
    """Round-13 fix: a run() that PATCHes more than
    DEFAULT_LARGE_CHANGES_THRESHOLD records correctly fires the Zapier
    alert with on_large_changes=True.

    Exercises the full chain run() → push_bio_sync_alert → requests.post,
    so the actual default threshold is in play. ``THRESHOLD + 1`` records
    is a deliberate just-over-threshold setup. Round-14 fix: the test
    imports ``DEFAULT_LARGE_CHANGES_THRESHOLD`` rather than hard-coding
    100, so ops can tune the constant without breaking this test.
    """
    n = DEFAULT_LARGE_CHANGES_THRESHOLD + 1
    cms, os_responses, federal_records = _build_n_federal_records(n)
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses=os_responses,
        federal_records=federal_records,
        webhook_url="https://example.com/zap",
    )

    # Capture the actual Zapier payload via requests.post (the real
    # push_bio_sync_alert serializes through this).
    captured: dict = {}

    def fake_post(url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        m = MagicMock()
        m.status_code = 200
        return m

    with patch("ddp_sync.pipelines.legislator_bio.requests.post", new=fake_post):
        report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.aborted is False
    assert len(report.would_patch) == n
    # The Zapier alert was fired with on_large_changes=True
    assert "json" in captured, "Zapier alert was not POSTed"
    payload = captured["json"]
    assert payload["patched"] == n
    assert payload["on_large_changes"] is True, (
        f"expected on_large_changes=True for {n} patches > threshold "
        f"{DEFAULT_LARGE_CHANGES_THRESHOLD}, got payload={payload}"
    )
    assert payload["large_changes_threshold"] == DEFAULT_LARGE_CHANGES_THRESHOLD


@pytest.mark.asyncio
async def test_run_large_changes_alert_does_not_fire_at_exact_threshold():
    """Round-14 fix: pin the strict-``>`` semantics. At exactly
    ``DEFAULT_LARGE_CHANGES_THRESHOLD`` records, ``on_large_changes`` is
    ``False`` — the alert flag fires only when count > threshold, not
    >=. Documents the boundary case so a future refactor can't silently
    flip the comparison.
    """
    n = DEFAULT_LARGE_CHANGES_THRESHOLD  # exactly at threshold
    cms, os_responses, federal_records = _build_n_federal_records(n)
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses=os_responses,
        federal_records=federal_records,
        webhook_url="https://example.com/zap",
    )

    captured: dict = {}

    def fake_post(url, json=None, timeout=None):
        captured["json"] = json
        m = MagicMock()
        m.status_code = 200
        return m

    with patch("ddp_sync.pipelines.legislator_bio.requests.post", new=fake_post):
        report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert len(report.would_patch) == n
    payload = captured["json"]
    assert payload["patched"] == n
    # The boundary case: == threshold, NOT > threshold → flag stays False
    assert payload["on_large_changes"] is False, (
        f"expected on_large_changes=False for exactly {n} patches "
        f"(== threshold); got True (semantics flipped to >=?)"
    )


@pytest.mark.asyncio
async def test_run_mixed_success_patches_records_per_record_results():
    """Round-13 fix: a run with some successful PATCHes and some failed
    PATCHes correctly records 2 successes + 1 error. Prior error tests
    forced ALL PATCHes to fail; this is the more realistic production
    scenario where Webflow returns 422/4xx on a specific bad record but
    succeeds on the others.
    """
    cms = [
        _cms(item_id="wf-good", name="Good Record", chamber="Senate",
             openstatesid="ocd-person/good", jurisdiction_ref=["juris-us"]),
        _cms(item_id="wf-bad", name="Bad Record", chamber="Senate",
             openstatesid="ocd-person/bad", jurisdiction_ref=["juris-us"]),
        _cms(item_id="wf-also-good", name="Also Good", chamber="Senate",
             openstatesid="ocd-person/also-good", jurisdiction_ref=["juris-us"]),
    ]
    os_responses = {
        "ocd-person/good": _os_person(
            openstates_id="ocd-person/good", chamber="upper",
            bioguide="G001", is_federal=True),
        "ocd-person/bad": _os_person(
            openstates_id="ocd-person/bad", chamber="upper",
            bioguide="B001", is_federal=True),
        "ocd-person/also-good": _os_person(
            openstates_id="ocd-person/also-good", chamber="upper",
            bioguide="GG001", is_federal=True),
    }
    feds = {
        "G001": _fed(bioguide="G001"),
        "B001": _fed(bioguide="B001"),
        "GG001": _fed(bioguide="GG001"),
    }

    # Selective patch: 2xx for the good ones, WebflowError for the bad one.
    patch_calls: list = []

    async def selective_patch(webflow_id, fields, *, publish=True, api_key=None):
        patch_calls.append(webflow_id)
        if webflow_id == "wf-bad":
            raise WebflowError("422 unprocessable on this record")
        return WebflowPatchResult(success=True, webflow_id=webflow_id)

    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses=os_responses,
        federal_records=feds,
        patch_func=selective_patch,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))

    # All 3 attempted
    assert len(patch_calls) == 3, (
        f"expected 3 PATCH attempts, got {patch_calls}"
    )
    # Per-record errors don't abort the run
    assert report.aborted is False
    # 2 successful → 2 entries in would_patch
    assert len(report.would_patch) == 2
    succeeded = {p["webflow_id"] for p in report.would_patch}
    assert succeeded == {"wf-good", "wf-also-good"}
    # 1 error recorded — the orchestrator formats per-record errors as
    # f"{slug or webflow_id}: {type(e).__name__}: {e}"
    assert len(report.errors) == 1
    err = report.errors[0]
    assert "WebflowError" in err
    assert "422" in err
    assert "bad-record" in err  # slug derived from "Bad Record"


@pytest.mark.asyncio
async def test_run_does_not_blank_populated_cms_field_with_empty_upstream():
    """Round-14 fix: the cardinal 'never blank a populated CMS field with
    empty upstream' rule is pinned end-to-end through ``run()``.

    This is the mass-blank-prevention scenario the reviewer flagged: if
    an upstream API schema shift caused birth-year to start coming through
    as ``None``, would the orchestrator silently overwrite the editor-
    populated CMS values with empty? No — two layers of defense prevent
    this:
      1. ``_build_federal_payload`` strips None values before the diff
      2. ``_diff_fields`` (via ``should_write`` / ``is_empty``) skips
         empty upstream values

    This test exercises both layers via run(). The federal record has
    None for ``birthday`` and no twitter handle; the CMS record has
    populated values for ``birth-year`` and ``twitter-handle``. The
    PATCH must not touch those fields. Other federal-source fields
    (``wikidata-id``, ``opensecrets-id``) WILL be patched, confirming
    the orchestrator did process the record (this isn't a no-op skip).
    """
    cms = [_cms(
        item_id="wf-1",
        name="Editor Populated",
        chamber="Senate",
        openstatesid="ocd-person/x",
        jurisdiction_ref=["juris-us"],
        extra_fields={
            # Editor populated these manually:
            "birth-year": "1965",
            "twitter-handle": "EditorPopulated",
        },
    )]
    os_record = _os_person(
        openstates_id="ocd-person/x",
        chamber="upper",
        bioguide="X001",
        is_federal=True,
    )
    # Federal source: None for the protected fields, populated for others
    fed = _fed(
        bioguide="X001",
        birthday=None,    # ← upstream None (e.g. API schema shift)
        twitter=None,     # ← upstream None (e.g. handle deleted)
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/x": os_record},
        federal_records={"X001": fed},
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.aborted is False
    # The orchestrator DID process the record (verifies this isn't a
    # no-op pass that would also incidentally avoid blanking).
    assert len(patches) == 1, (
        f"expected one PATCH (other fields differ), got {patches}"
    )
    _, fields = patches[0]

    # Mass-blank prevention: the populated CMS fields are NOT blanked
    # by None upstream
    assert "birth-year" not in fields, (
        f"mass-blank: birth-year was None upstream but appeared in PATCH "
        f"payload — would have blanked editor-populated '1965'. fields={fields}"
    )
    assert "twitter-handle" not in fields, (
        f"mass-blank: twitter-handle was None upstream but appeared in "
        f"PATCH payload — would have blanked editor-populated handle. "
        f"fields={fields}"
    )
    # Other federal fields ARE present (confirms processing happened)
    assert fields.get("wikidata-id") == "Q123"
    assert fields.get("opensecrets-id") == "N00012345"


@pytest.mark.asyncio
async def test_run_no_churn_on_term_dates_when_cms_already_has_iso_datetime():
    """ChurnPATCH prevention: if a previous run wrote term-start /
    term-end as Webflow's ISO-datetime format (the storage shape), a
    second run should NOT re-PATCH those fields just because the
    upstream YAML reports the same date in date-only form.

    Background: production rollout (2026-04-30) exposed that all 32 FL
    federal records were re-PATCHing term-start + term-end on every run
    after the first successful write. Cause: unitedstates YAML stores
    `"2025-01-03"` (date-only) but Webflow Date fields round-trip as
    `"2025-01-03T00:00:00.000Z"`. should_write's plain `==` comparison
    saw a diff every run. Fix: orchestrator coerces the upstream
    date-only string to Webflow's storage format BEFORE the diff.
    """
    cms = [_cms(
        item_id="wf-1",
        name="Already Patched",
        chamber="Senate",
        openstatesid="ocd-person/x",
        jurisdiction_ref=["juris-us"],
        extra_fields={
            # CMS already has term dates in Webflow's ISO-datetime
            # storage shape from a previous successful PATCH.
            "term-start": "2023-01-03T00:00:00.000Z",
            "term-end":   "2029-01-03T00:00:00.000Z",
        },
    )]
    os_record = _os_person(
        openstates_id="ocd-person/x",
        chamber="upper",
        bioguide="X001",
        is_federal=True,
    )
    # Upstream YAML reports the same dates in date-only form (the
    # canonical unitedstates shape).
    fed = _fed(
        bioguide="X001",
        term_start="2023-01-03",
        term_end="2029-01-03",
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/x": os_record},
        federal_records={"X001": fed},
        patch_recorder=patches,
    )

    report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.aborted is False
    # If churn-prevention works, term-start + term-end should NOT
    # appear in any patch payload (other fields might if they differ).
    if patches:
        _, fields = patches[0]
        assert "term-start" not in fields, (
            f"ChurnPATCH: term-start was re-PATCHed despite matching "
            f"upstream date. fields={fields}"
        )
        assert "term-end" not in fields, (
            f"ChurnPATCH: term-end was re-PATCHed despite matching "
            f"upstream date. fields={fields}"
        )
