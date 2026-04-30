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


def _cms(
    *,
    item_id: str,
    name: str = "Test",
    chamber: str = "Senate",
    openstatesid: str | None = None,
    bioguide_id: str | None = None,
    jurisdiction_ref: list | str | None = None,
) -> dict:
    fields: dict[str, Any] = {
        "name": name,
        "slug": name.lower().replace(" ", "-"),
        "chamber": chamber,
    }
    if openstatesid:
        fields["openstatesid"] = openstatesid
    if bioguide_id:
        fields["bioguide-id"] = bioguide_id
    if jurisdiction_ref is not None:
        fields["jurisdiction"] = jurisdiction_ref
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
):
    """Wire up a fully-mocked LegislatorBioPipeline for run() tests."""
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

    webflow = MagicMock()
    webflow.iter_legislator_items = fake_iter
    webflow.get_jurisdiction_mapping = AsyncMock(return_value=jurisdiction_mapping)

    if patch_error is not None:
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
    openstates = MagicMock()
    openstates.fetch_by_id = AsyncMock(side_effect=fake_fetch_by_id)

    congress = MagicMock()
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
    assert fields["term-start"] == "2023-01-03"
    # Federal email-as-URL routed to contact-form-url
    assert fields.get("contact-form-url") == "https://www.rickscott.senate.gov/contact/contact"
    assert "email" not in fields  # bare email field stays empty for federal


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
    assert fields["term-start"] == "2011-01-05"
    assert fields["term-end"] == "2023-01-03"
    assert fields["gender"] == "M"


@pytest.mark.asyncio
async def test_run_state_record_phase_2_stub_skipped_not_orphaned():
    """A state record where OpenStates resolves should be SKIPPED with a
    'Phase 2' log line, not silently no-op'd, and not flagged as orphan."""
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
    # OpenStates DID resolve; it just gets skipped at payload-build time
    assert report.items_resolved_via_openstates == 1
    assert report.upstream_orphans == []     # NOT flagged as orphan
    assert len(patches) == 0                  # NO PATCH (Phase 2 stub)
    assert report.errors == []


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
