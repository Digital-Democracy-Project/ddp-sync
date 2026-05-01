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
        async def fake_patch(webflow_id, fields, *, publish=True, api_key=None, strict_schema=False):
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
    assert "office-email" not in fields  # federal email-as-URL routes to contact-form-url, not office-email
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
    assert fields["office-email"] == "jane@myfloridahouse.gov"
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
    assert "office-email" not in fields


@pytest.mark.asyncio
async def test_run_state_legislator_missing_birth_date_does_not_set_birth_year():
    """Phase-2.5 edge case: state legs with no birth_date in OpenStates
    (5-of-10 in the FL probe) should not produce a birth-year field at
    all (None gets stripped by the build's filter)."""
    cms = [_cms(
        item_id="wf-fl-bd",
        chamber="lower",
        openstatesid="ocd-person/fl-bd",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-bd",
        chamber="lower",
        state="FL",
        is_federal=False,
        # birth_date intentionally omitted (the OpenStates probe found
        # 50% of FL state legs have it null)
        gender="F",
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-bd": os_record},
        patch_recorder=patches,
    )
    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    if patches:
        _, fields = patches[0]
        assert "birth-year" not in fields, (
            "birth-year should not appear when birth_date is empty/None"
        )
        assert fields.get("gender") == "F"


@pytest.mark.asyncio
async def test_run_state_legislator_with_openstates_url_writes_openstates_id():
    """Phase-2.5: openstates_url → openstates-id (URL-typed CMS field)."""
    cms = [_cms(
        item_id="wf-fl-url",
        chamber="lower",
        openstatesid="ocd-person/fl-url",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-url",
        chamber="lower",
        state="FL",
        is_federal=False,
        gender="M",
    )
    # Manually set openstates_url on the dataclass (the test fixture
    # doesn't yet support it as a kwarg, but the field exists post-
    # Phase-2.5 update).
    os_record.openstates_url = "https://openstates.org/person/jane-state-x/"
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-url": os_record},
        patch_recorder=patches,
    )
    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    _, fields = patches[0]
    # Trailing slash stripped to match Webflow's URL-field storage
    # format and prevent ChurnPATCH (post-Phase-2.5 fix).
    assert fields["open-states-url"] == "https://openstates.org/person/jane-state-x"


@pytest.mark.asyncio
async def test_run_state_legislator_email_lowercased_to_match_webflow_storage():
    """ChurnPATCH prevention: Webflow's email field lowercases on storage.
    If we send mixed-case (some FL Senate emails were observed mixed-case
    on the 2026-04-30 production run), every subsequent run sees a diff
    and re-PATCHes. Lowercase before sending so the value round-trips."""
    cms = [_cms(
        item_id="wf-fl-mixed",
        chamber="upper",
        openstatesid="ocd-person/fl-mixed",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-mixed",
        chamber="upper",
        state="FL",
        is_federal=False,
        email="Senator.Smith@flsenate.gov",   # ← mixed case from upstream
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-mixed": os_record},
        patch_recorder=patches,
    )
    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    _, fields = patches[0]
    # Lowercase before sending; the cardinal-rule diff matches Webflow's
    # stored form on the next run.
    assert fields["office-email"] == "senator.smith@flsenate.gov"


@pytest.mark.asyncio
async def test_run_state_legislator_email_lowercase_no_churn_on_rerun():
    """When CMS already has the lowercase email and upstream returns the
    same value (mixed-case or lowercase), no re-PATCH happens."""
    cms = [_cms(
        item_id="wf-fl-stable",
        chamber="lower",
        openstatesid="ocd-person/fl-stable",
        jurisdiction_ref=["juris-fl"],
        extra_fields={
            "office-email": "stable.rep@flhouse.gov",  # already lowercase in CMS
        },
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-stable",
        chamber="lower",
        state="FL",
        is_federal=False,
        email="Stable.Rep@FLHOUSE.GOV",   # upstream returns mixed case
    )
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-stable": os_record},
        patch_recorder=patches,
    )
    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    if patches:
        _, fields = patches[0]
        assert "office-email" not in fields, (
            f"ChurnPATCH: office-email was re-PATCHed despite CMS already having "
            f"the lowercased form. fields={fields}"
        )


@pytest.mark.asyncio
async def test_run_state_legislator_openstates_url_no_churn_when_cms_has_no_trailing_slash():
    """ChurnPATCH prevention for openstates-id (URL-typed): Webflow strips
    trailing slash on storage; we strip before sending so the round-trip
    matches."""
    cms = [_cms(
        item_id="wf-fl-osurl",
        chamber="lower",
        openstatesid="ocd-person/fl-osurl",
        jurisdiction_ref=["juris-fl"],
        extra_fields={
            # CMS already has the no-slash form from a previous run.
            "open-states-url": "https://openstates.org/person/jane-x",
        },
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-osurl",
        chamber="lower",
        state="FL",
        is_federal=False,
    )
    # Upstream returns with trailing slash (canonical OpenStates form).
    os_record.openstates_url = "https://openstates.org/person/jane-x/"
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-osurl": os_record},
        patch_recorder=patches,
    )
    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    if patches:
        _, fields = patches[0]
        assert "open-states-url" not in fields, (
            f"ChurnPATCH: open-states-url re-PATCHed despite CMS having "
            f"the slug-stripped form. fields={fields}"
        )


@pytest.mark.asyncio
async def test_run_state_legislator_fl_override_extracts_official_website():
    """Phase-2.5: FL state-payload override picks the link with note
    ``member detail page`` (or first link to a known FL host) for
    official-website. Other states get default pass-through (no FL
    override applied)."""
    from ddp_sync.services.openstates_people import OpenStatesPerson
    cms = [_cms(
        item_id="wf-fl-ow",
        chamber="lower",
        openstatesid="ocd-person/fl-ow",
        jurisdiction_ref=["juris-fl"],
    )]
    raw = {
        "id": "ocd-person/fl-ow",
        "name": "Jane FL",
        "current_role": {
            "org_classification": "lower", "district": "57",
        },
        "jurisdiction": {"name": "Florida"},
        "links": [
            {"url": "https://www.flhouse.gov/Sections/Representatives/details.aspx?MemberId=4885", "note": ""},
            {"url": "https://myfloridahouse.gov/Sections/Representatives/details.aspx?MemberId=4885&LegislativeTermId=90", "note": "member detail page"},
            {"url": "https://www.flhouse.gov/Sections/Representatives/details.aspx?MemberId=4885", "note": ""},
        ],
    }
    os_record = OpenStatesPerson.from_api(raw)
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-ow": os_record},
        patch_recorder=patches,
    )
    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    _, fields = patches[0]
    # Override picks the "member detail page" note when present
    assert fields["official-website"] == (
        "https://myfloridahouse.gov/Sections/Representatives/details.aspx?MemberId=4885&LegislativeTermId=90"
    )


@pytest.mark.asyncio
async def test_run_state_legislator_fl_override_falls_back_to_known_host():
    """When no link has the 'member detail page' note, the FL override
    falls back to the first link with a known FL legislature host."""
    from ddp_sync.services.openstates_people import OpenStatesPerson
    cms = [_cms(
        item_id="wf-fl-host",
        chamber="upper",
        openstatesid="ocd-person/fl-host",
        jurisdiction_ref=["juris-fl"],
    )]
    raw = {
        "id": "ocd-person/fl-host",
        "name": "Jane FL Sen",
        "current_role": {"org_classification": "upper"},
        "jurisdiction": {"name": "Florida"},
        "links": [
            {"url": "https://example.com/random", "note": ""},
            {"url": "https://www.flsenate.gov/Senators/S29", "note": ""},
        ],
    }
    os_record = OpenStatesPerson.from_api(raw)
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-host": os_record},
        patch_recorder=patches,
    )
    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    _, fields = patches[0]
    assert fields["official-website"] == (
        "https://www.flsenate.gov/Senators/S29"
    )


@pytest.mark.asyncio
async def test_run_state_legislator_non_fl_state_skips_fl_override():
    """States without an override entry get the default state payload
    (no official-website extraction). Confirms the override registry
    is keyed correctly."""
    from ddp_sync.services.openstates_people import OpenStatesPerson
    cms = [_cms(
        item_id="wf-ma-1",
        chamber="lower",
        openstatesid="ocd-person/ma-1",
        jurisdiction_ref=["juris-ma"],
    )]
    raw = {
        "id": "ocd-person/ma-1",
        "name": "MA Rep",
        "current_role": {"org_classification": "lower"},
        "jurisdiction": {"name": "Massachusetts"},
        "links": [
            {"url": "https://malegislature.gov/Legislators/Profile/X", "note": "member detail page"},
        ],
    }
    os_record = OpenStatesPerson.from_api(raw)
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/ma-1": os_record},
        patch_recorder=patches,
    )
    report = await pipeline.run(BioSyncOptions(dry_run=False))
    assert report.errors == []
    if patches:
        _, fields = patches[0]
        # MA has no entry in _STATE_PAYLOAD_OVERRIDES, so even with
        # a "member detail page" link, the default state builder
        # doesn't extract official-website.
        assert "official-website" not in fields


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

    async def selective_patch(webflow_id, fields, *, publish=True, api_key=None, strict_schema=False):
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
async def test_run_upload_photos_off_does_not_call_assets():
    """Default behavior: assets service not invoked when upload_photos=False.
    photo-source-url Link still populates as before."""
    from unittest.mock import MagicMock, AsyncMock
    cms = [_cms(
        item_id="wf-fl-1", chamber="lower",
        openstatesid="ocd-person/fl-1",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-1", chamber="lower",
        state="FL", is_federal=False,
        image="https://www.flhouse.gov/photo.jpg",
    )
    fake_assets = MagicMock()
    fake_assets.upload_from_url = AsyncMock()
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-1": os_record},
    )
    pipeline.assets = fake_assets
    report = await pipeline.run(BioSyncOptions(
        dry_run=True, upload_photos=False,
    ))
    assert report.errors == []
    fake_assets.upload_from_url.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_upload_photos_on_uploads_and_populates_legislator_image():
    """Phase-3: with upload_photos=True, asset service is called and
    legislator-image is populated in the PATCH payload."""
    from unittest.mock import MagicMock, AsyncMock
    from ddp_sync.services.webflow_assets import AssetReference
    cms = [_cms(
        item_id="wf-fl-1", chamber="lower",
        openstatesid="ocd-person/fl-1",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-1", name="Jane FL",
        chamber="lower", state="FL", is_federal=False,
        image="https://www.flhouse.gov/photo.jpg",
    )
    fake_assets = MagicMock()
    fake_assets.upload_from_url = AsyncMock(return_value=AssetReference(
        asset_id="asset-123", hosted_url="https://cdn.webflow.com/asset-123.jpg",
        alt_text="Jane FL",
    ))
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-1": os_record},
        patch_recorder=patches,
    )
    pipeline.assets = fake_assets
    report = await pipeline.run(BioSyncOptions(
        dry_run=False, upload_photos=True,
    ))
    assert report.errors == []
    fake_assets.upload_from_url.assert_awaited_once()
    _, fields = patches[0]
    assert fields["legislator-image"] == {
        "fileId": "asset-123",
        "url": "https://cdn.webflow.com/asset-123.jpg",
        "alt": "Jane FL",
    }


@pytest.mark.asyncio
async def test_run_upload_photos_fails_fast_when_assets_key_missing():
    """Round-19 fix: when upload_photos=True but
    webflow_assets_read_write_key is not configured, the orchestrator
    skips photo upload (with a clear logged error) instead of silently
    falling back to webflow_api_token — which would have re-triggered
    the 403 OAuthForbidden the operator already saw in production.
    """
    cms = [_cms(
        item_id="wf-fl-1", chamber="lower",
        openstatesid="ocd-person/fl-1",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-1", chamber="lower",
        state="FL", is_federal=False,
        image="https://www.flhouse.gov/photo.jpg",
    )
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-1": os_record},
    )
    # Simulate the misconfiguration: cms token set, assets key blank.
    pipeline.settings = MagicMock()
    pipeline.settings.webflow_api_token = "cms-token-abc"
    pipeline.settings.webflow_assets_read_write_key = ""
    pipeline.settings.webflow_site_id = "site-id"
    pipeline.assets = None  # force lazy-init path

    report = await pipeline.run(BioSyncOptions(
        dry_run=False, upload_photos=True,
    ))
    # Run completes (per-record error isolation); no assets created
    assert report.aborted is False
    # legislator-image is NOT in any patch (upload was disabled)
    for entry in report.would_patch:
        assert "legislator-image" not in entry.get("changed_fields", [])


@pytest.mark.asyncio
async def test_run_upload_photos_skips_when_cms_already_has_image():
    """Cardinal-rule preserves existing legislator-image — no upload
    attempted, asset service never called."""
    from unittest.mock import MagicMock, AsyncMock
    cms = [_cms(
        item_id="wf-fl-1", chamber="lower",
        openstatesid="ocd-person/fl-1",
        jurisdiction_ref=["juris-fl"],
        extra_fields={
            # CMS already has an editor-uploaded image
            "legislator-image": {
                "fileId": "existing-asset",
                "url": "https://cdn.webflow.com/existing.jpg",
                "alt": "",
            },
        },
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-1", chamber="lower",
        state="FL", is_federal=False,
        image="https://www.flhouse.gov/photo.jpg",
    )
    fake_assets = MagicMock()
    fake_assets.upload_from_url = AsyncMock()
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-1": os_record},
    )
    pipeline.assets = fake_assets
    report = await pipeline.run(BioSyncOptions(
        dry_run=False, upload_photos=True,
    ))
    assert report.errors == []
    fake_assets.upload_from_url.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_upload_photos_passes_congress_gov_fallback_for_federal():
    """Phase-4: federal records get a congress.gov fallback URL for the
    photo upload (because unitedstates/images dataset has gaps for new
    freshmen). State records get no fallback (per-state CDNs vary)."""
    from unittest.mock import MagicMock, AsyncMock
    from ddp_sync.services.webflow_assets import AssetReference

    cms_fed = [_cms(
        item_id="wf-fed-1", chamber="Senate",
        openstatesid="ocd-person/x", bioguide_id="X001",
        jurisdiction_ref=["juris-us"],
    )]
    fed = _fed(bioguide="X001")
    os_record = _os_person(
        openstates_id="ocd-person/x", chamber="upper",
        bioguide="X001", is_federal=True,
    )
    fake_assets = MagicMock()
    fake_assets.upload_from_url = AsyncMock(return_value=AssetReference(
        asset_id="asset-fed", hosted_url="https://cdn/fed.jpg",
        alt_text="Fed Test",
    ))
    pipeline = _build_pipeline(
        cms_items=cms_fed,
        openstates_responses={"ocd-person/x": os_record},
        federal_records={"X001": fed},
    )
    pipeline.assets = fake_assets

    await pipeline.run(BioSyncOptions(dry_run=False, upload_photos=True))

    fake_assets.upload_from_url.assert_awaited_once()
    call_kwargs = fake_assets.upload_from_url.call_args.kwargs
    fallback_urls = call_kwargs.get("fallback_urls") or ()
    assert len(fallback_urls) == 1
    # Lowercase bioguide in the path
    assert fallback_urls[0] == "https://www.congress.gov/img/member/x001.jpg"


@pytest.mark.asyncio
async def test_run_upload_photos_no_fallback_for_state_records():
    """State records get no fallback URLs (per-state CDNs vary too much
    for a universal fallback). The upload either works on the primary
    OpenStates `image` URL or fails per-record."""
    from unittest.mock import MagicMock, AsyncMock
    from ddp_sync.services.webflow_assets import AssetReference

    cms_state = [_cms(
        item_id="wf-st-1", chamber="lower",
        openstatesid="ocd-person/y",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/y", chamber="lower",
        state="FL", is_federal=False,
        image="https://www.flhouse.gov/photo.jpg",
    )
    fake_assets = MagicMock()
    fake_assets.upload_from_url = AsyncMock(return_value=AssetReference(
        asset_id="asset-st", hosted_url="https://cdn/st.jpg",
        alt_text="State Test",
    ))
    pipeline = _build_pipeline(
        cms_items=cms_state,
        openstates_responses={"ocd-person/y": os_record},
    )
    pipeline.assets = fake_assets

    await pipeline.run(BioSyncOptions(dry_run=False, upload_photos=True))

    fake_assets.upload_from_url.assert_awaited_once()
    call_kwargs = fake_assets.upload_from_url.call_args.kwargs
    fallback_urls = call_kwargs.get("fallback_urls") or ()
    assert fallback_urls == ()  # state path: no fallback


@pytest.mark.asyncio
async def test_run_upload_photos_dry_run_skips_legislator_image_field():
    """Phase-3 round-18: upload_photos_dry_run fetches/validates the
    source image but skips the actual Webflow asset creation. The
    legislator-image field is NOT populated in the payload because no
    asset was created. Useful for connectivity smoke testing without
    consuming Webflow's asset rate limit."""
    from unittest.mock import MagicMock, AsyncMock
    cms = [_cms(
        item_id="wf-fl-1", chamber="lower",
        openstatesid="ocd-person/fl-1",
        jurisdiction_ref=["juris-fl"],
    )]
    os_record = _os_person(
        openstates_id="ocd-person/fl-1", chamber="lower",
        state="FL", is_federal=False,
        image="https://www.flhouse.gov/photo.jpg",
    )
    fake_assets = MagicMock()
    # Simulate dry_run mode returning None
    fake_assets.upload_from_url = AsyncMock(return_value=None)
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/fl-1": os_record},
        patch_recorder=patches,
    )
    pipeline.assets = fake_assets
    report = await pipeline.run(BioSyncOptions(
        dry_run=False, upload_photos=True, upload_photos_dry_run=True,
    ))
    # upload_from_url was called with dry_run=True
    fake_assets.upload_from_url.assert_awaited_once()
    call_kwargs = fake_assets.upload_from_url.call_args.kwargs
    assert call_kwargs.get("dry_run") is True
    # Other fields PATCH normally; legislator-image is NOT in the payload
    # (no asset was actually created, so we have nothing to set).
    assert report.errors == []
    if patches:
        _, fields = patches[0]
        assert "legislator-image" not in fields


@pytest.mark.asyncio
async def test_run_upload_photos_failure_isolated_does_not_abort():
    """Per-record photo upload failure is logged + recorded as a
    per-record error; the rest of the payload still PATCHes; the run
    continues to subsequent records."""
    from unittest.mock import MagicMock, AsyncMock
    from ddp_sync.services.webflow_assets import WebflowAssetError
    cms = [
        _cms(item_id="wf-1", name="A", chamber="lower",
             openstatesid="ocd-person/a",
             jurisdiction_ref=["juris-fl"]),
        _cms(item_id="wf-2", name="B", chamber="lower",
             openstatesid="ocd-person/b",
             jurisdiction_ref=["juris-fl"]),
    ]
    os_a = _os_person(
        openstates_id="ocd-person/a", chamber="lower",
        state="FL", is_federal=False,
        image="https://broken.example.com/photo.jpg",
    )
    os_b = _os_person(
        openstates_id="ocd-person/b", chamber="lower",
        state="FL", is_federal=False,
        image="https://www.flhouse.gov/good.jpg",
    )
    from ddp_sync.services.webflow_assets import AssetReference

    async def upload_side_effect(source_url, *, alt_text="", dry_run=False, fallback_urls=()):
        if "broken" in source_url:
            raise WebflowAssetError("404 fetching source image")
        return AssetReference(
            asset_id="asset-ok", hosted_url="https://cdn.webflow.com/ok.jpg",
            alt_text=alt_text,
        )

    fake_assets = MagicMock()
    fake_assets.upload_from_url = AsyncMock(side_effect=upload_side_effect)
    patches: list = []
    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={
            "ocd-person/a": os_a, "ocd-person/b": os_b,
        },
        patch_recorder=patches,
    )
    pipeline.assets = fake_assets
    report = await pipeline.run(BioSyncOptions(
        dry_run=False, upload_photos=True,
    ))
    # First record's photo upload failed → in errors but not aborted
    assert any("photo upload" in e for e in report.errors)
    assert report.aborted is False
    # Both records got PATCH'd; A's PATCH didn't include legislator-image
    # (upload failed); B's did.
    assert len(patches) == 2
    a_fields = next(f for wf, f in patches if wf == "wf-1")
    b_fields = next(f for wf, f in patches if wf == "wf-2")
    assert "legislator-image" not in a_fields
    assert b_fields["legislator-image"]["fileId"] == "asset-ok"


@pytest.mark.asyncio
async def test_run_strict_schema_off_tolerates_dropped_fields():
    """Phase-3: by default, schema-cache drops are tolerated (run continues
    with the kept fields, dropped reported in would_patch entry)."""
    cms = [_cms(
        item_id="wf-1", chamber="Senate",
        openstatesid="ocd-person/x",
        jurisdiction_ref=["juris-us"],
    )]
    fed = _fed(bioguide="X001")
    os_record = _os_person(
        openstates_id="ocd-person/x", chamber="upper",
        bioguide="X001", is_federal=True,
    )

    async def patch_with_drops(webflow_id, fields, *, publish=True, api_key=None, strict_schema=False):
        # Simulate the schema cache dropping one field
        return WebflowPatchResult(
            success=True, webflow_id=webflow_id,
            dropped_fields={"some-missing-slug"},
        )

    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/x": os_record},
        federal_records={"X001": fed},
        patch_func=patch_with_drops,
    )
    report = await pipeline.run(
        BioSyncOptions(dry_run=False, strict_schema=False),
    )
    # Strict off: tolerated, no errors
    assert report.errors == []
    assert report.aborted is False
    # would_patch entry surfaces the dropped fields for visibility
    assert report.would_patch[0]["dropped_fields"] == ["some-missing-slug"]


@pytest.mark.asyncio
async def test_run_strict_schema_on_raises_on_dropped_fields():
    """Phase-3 round-18: strict_schema enforcement happens BEFORE the
    PATCH inside update_legislator_fields, raising WebflowError when
    any payload slug is missing. Avoids the partial-write state that
    post-PATCH enforcement left behind. The orchestrator catches the
    error per-record (existing isolation), records it in report.errors,
    and continues to the next record.
    """
    from ddp_sync.services.webflow_lookup import WebflowError
    cms = [_cms(
        item_id="wf-1", chamber="Senate",
        openstatesid="ocd-person/x",
        jurisdiction_ref=["juris-us"],
    )]
    fed = _fed(bioguide="X001")
    os_record = _os_person(
        openstates_id="ocd-person/x", chamber="upper",
        bioguide="X001", is_federal=True,
    )

    # Simulate update_legislator_fields raising on strict_schema with
    # missing slugs (matches the real service-side enforcement).
    async def patch_strict(webflow_id, fields, *, publish=True,
                            api_key=None, strict_schema=False):
        if strict_schema:
            raise WebflowError(
                "strict_schema: payload fields not in live CMS "
                "collection schema: ['office-email', 'open-states-url']"
            )
        return WebflowPatchResult(success=True, webflow_id=webflow_id)

    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/x": os_record},
        federal_records={"X001": fed},
        patch_func=patch_strict,
    )
    report = await pipeline.run(
        BioSyncOptions(dry_run=False, strict_schema=True),
    )
    # Strict on: per-record error captured
    assert len(report.errors) == 1
    err = report.errors[0]
    assert "strict_schema" in err
    assert "office-email" in err and "open-states-url" in err
    # Run still completes (per-record isolation); doesn't abort
    assert report.aborted is False


@pytest.mark.asyncio
async def test_run_strict_schema_on_clean_run_no_errors():
    """Strict mode is a no-op when every payload field exists in the
    live CMS schema."""
    cms = [_cms(
        item_id="wf-1", chamber="Senate",
        openstatesid="ocd-person/x",
        jurisdiction_ref=["juris-us"],
    )]
    fed = _fed(bioguide="X001")
    os_record = _os_person(
        openstates_id="ocd-person/x", chamber="upper",
        bioguide="X001", is_federal=True,
    )
    # Default _build_pipeline returns dropped_fields=set() (empty)

    pipeline = _build_pipeline(
        cms_items=cms,
        openstates_responses={"ocd-person/x": os_record},
        federal_records={"X001": fed},
    )
    report = await pipeline.run(
        BioSyncOptions(dry_run=False, strict_schema=True),
    )
    assert report.errors == []
    assert report.aborted is False


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
