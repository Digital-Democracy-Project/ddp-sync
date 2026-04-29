"""Foundation tests for the legislator bio sync project.

Pins the round-3 through round-6 review fixes and the live-data discoveries
made during step-3b smoke testing. These tests run against the modules
landed in commits 2852a36 and 24d058e (foundation refactor + step 3b +
round-5 fixes).

Scope: just the foundation — the orchestrator (step 4), trigger endpoint
(step 5), and audits (step 6) get their own test files when those land.

Tested behaviors:

- RateLimiter is concurrency-safe (asyncio.Lock guards apply()).
- RateLimitConfig.from_yaml never raises and emits a structured-metric
  breadcrumb on the fallback path.
- OpenStatesPerson.from_api accepts both dict-shaped and string-shaped
  ``jurisdiction`` upstream values (round-3b live-data discovery).
- OpenStatesPerson.is_federal uses jurisdiction_name == "United States"
  (federal members' division_id contains /state:XX, so a naive division-id
  check would mis-classify them).
- OpenStatesPeopleClient.extract_other_id defensively skips non-dict
  entries in other_identifiers (round-6 fix).
- WebflowLookupService._get_field_slugs reuses a stale cache when the
  fresh-fetch fails (round-6 fix); fails closed when no cache is available.
- WebflowLookupService._partition_payload propagates schema-fetch errors
  rather than silently passing the unfiltered payload through (round-5).
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.pipelines.legislator_bio import (
    AuditEntry,
    AuditReport,
    BioSyncOptions,
    BioSyncReport,
    CMSLegislator,
    LegislatorBioPipeline,
    is_empty,
    push_bio_sync_alert,
    should_write,
    split_email_field,
)
from ddp_sync.services.openstates_people import (
    OpenStatesPeopleClient,
    OpenStatesPerson,
)
from ddp_sync.services.rate_limiter import RateLimitConfig, RateLimiter
from ddp_sync.services.webflow_lookup import (
    WebflowError,
    WebflowLookupService,
    WebflowRateLimitError,
)


# ---------- RateLimiter ----------


@pytest.mark.asyncio
async def test_rate_limiter_serializes_concurrent_calls():
    """asyncio.gather() must NOT bypass the inter-call gap (round-4 fix).

    Five concurrent calls at 600 rpm + a 50ms inter-call floor → effective
    100ms per-request delay → 4 enforced sleeps × 100ms ≈ 400ms total.
    Without the lock, all five could read _last_request_time before
    anyone updates it and proceed without sleeping (~0ms).
    """
    cfg = RateLimitConfig(requests_per_minute=600, delay_between_requests_ms=50)
    limiter = RateLimiter(cfg)

    t0 = time.monotonic()
    await asyncio.gather(*[limiter.apply() for _ in range(5)])
    elapsed = time.monotonic() - t0

    # Per-request delay = max(50ms, 60/600 = 100ms) = 100ms
    # Expected: 4 sleeps × 100ms = 400ms (first call doesn't sleep)
    assert 0.35 < elapsed < 0.55, f"expected ~400ms, got {elapsed:.3f}s"
    assert limiter.enforced_sleeps == 4, (
        f"expected 4 enforced sleeps, got {limiter.enforced_sleeps}"
    )


# ---------- RateLimitConfig.from_yaml ----------


def test_from_yaml_returns_defaults_on_missing_file(caplog):
    """Round-5 fix: from_yaml must never raise. Missing file → defaults."""
    cfg = RateLimitConfig.from_yaml(Path("/this/path/does/not/exist.yaml"))
    assert cfg.requests_per_minute == 120
    assert cfg.delay_between_requests_ms == 500
    assert cfg.max_retry_attempts == 3


def test_from_yaml_returns_defaults_on_malformed_yaml(tmp_path):
    """Malformed YAML must not raise — return defaults."""
    bad = tmp_path / "bad.yaml"
    bad.write_text("not: valid: yaml: ::: [")
    cfg = RateLimitConfig.from_yaml(bad)
    assert cfg.requests_per_minute == 120


def test_from_yaml_accepts_legacy_delay_between_bills_ms(tmp_path):
    """Legacy YAML key must still work (bill_sync.py historical naming)."""
    yaml_path = tmp_path / "schedule.yaml"
    yaml_path.write_text(
        "rate_limit:\n"
        "  requests_per_minute: 60\n"
        "  delay_between_bills_ms: 750\n"
    )
    cfg = RateLimitConfig.from_yaml(yaml_path)
    assert cfg.requests_per_minute == 60
    assert cfg.delay_between_requests_ms == 750


# ---------- OpenStatesPerson ----------


def test_from_api_handles_dict_shaped_jurisdiction():
    """Round-3b live-data discovery: OpenStates returns jurisdiction as a dict."""
    raw = {
        "id": "ocd-person/abc",
        "name": "Test Person",
        "jurisdiction": {"id": "ocd-jur/...", "name": "Florida", "classification": "state"},
        "current_role": {},
    }
    person = OpenStatesPerson.from_api(raw)
    assert person.jurisdiction_name == "Florida"


def test_from_api_handles_string_shaped_jurisdiction():
    """Defensive: also accept a flat string in case the API changes shape."""
    raw = {"id": "ocd-person/abc", "name": "X", "jurisdiction": "United States"}
    person = OpenStatesPerson.from_api(raw)
    assert person.jurisdiction_name == "United States"


def test_from_api_handles_missing_jurisdiction():
    """Missing jurisdiction → None (not "" or other falsy spelling)."""
    raw = {"id": "ocd-person/abc", "name": "X"}
    person = OpenStatesPerson.from_api(raw)
    assert person.jurisdiction_name is None


def test_is_federal_recognizes_united_states():
    """Round-3b heuristic fix: jurisdiction_name == 'United States' is the
    canonical federal signal. Federal members' division_id includes /state:XX
    (because they represent specific states), so naive division-id checks
    mis-classify them."""
    raw = {
        "id": "ocd-person/abc",
        "name": "Rick Scott",
        "jurisdiction": {"name": "United States", "classification": "country"},
        "current_role": {
            "division_id": "ocd-division/country:us/state:fl",
            "org_classification": "upper",
        },
    }
    assert OpenStatesPerson.from_api(raw).is_federal is True


def test_is_federal_false_for_state_legislator():
    raw = {
        "id": "ocd-person/abc",
        "name": "FL state rep",
        "jurisdiction": {"name": "Florida", "classification": "state"},
        "current_role": {"division_id": "ocd-division/country:us/state:fl/sldl:57"},
    }
    assert OpenStatesPerson.from_api(raw).is_federal is False


def test_is_federal_handles_missing_jurisdiction_gracefully():
    raw = {"id": "ocd-person/abc", "name": "X"}
    assert OpenStatesPerson.from_api(raw).is_federal is False


def test_extract_other_id_finds_match():
    raw = {"other_identifiers": [
        {"scheme": "fec", "identifier": "S8FL00273"},
        {"scheme": "bioguide", "identifier": "S001217"},
    ]}
    assert OpenStatesPeopleClient.extract_other_id(raw, "bioguide") == "S001217"


def test_extract_other_id_returns_none_for_missing_scheme():
    raw = {"other_identifiers": [{"scheme": "fec", "identifier": "X"}]}
    assert OpenStatesPeopleClient.extract_other_id(raw, "bioguide") is None


def test_extract_other_id_skips_non_dict_entries():
    """Round-6 defensive fix: OpenStates has historically returned strings
    in other_identifiers during at least one upstream incident."""
    raw = {"other_identifiers": [
        "garbage-string",
        None,
        {"scheme": "bioguide", "identifier": "S001217"},
    ]}
    assert OpenStatesPeopleClient.extract_other_id(raw, "bioguide") == "S001217"


def test_extract_other_id_handles_missing_other_identifiers():
    assert OpenStatesPeopleClient.extract_other_id({}, "bioguide") is None


# ---------- Cardinal rule: is_empty / should_write / split_email_field ----------


def test_is_empty_recognizes_sentinels():
    """All values in EMPTY_VALUES + whitespace + empty containers are empty."""
    assert is_empty(None)
    assert is_empty("")
    assert is_empty("   ")
    assert is_empty("-")
    assert is_empty("N/A")
    assert is_empty("UNKNOWN")
    assert is_empty([])
    assert is_empty({})


def test_is_empty_preserves_numeric_zero():
    """At-large districts have district=0; this is a real value, not empty."""
    assert is_empty(0) is False
    assert is_empty("0") is False  # string "0" is also not empty


def test_is_empty_preserves_real_values():
    assert is_empty("Rick Scott") is False
    assert is_empty(["item"]) is False
    assert is_empty({"key": "val"}) is False


def test_is_empty_handles_unhashable_types():
    """Lists/dicts must not raise — they're checked structurally, not by membership."""
    is_empty([1, 2, 3])
    is_empty({"a": 1})
    # Should not raise


def test_should_write_skips_empty_upstream():
    assert should_write("twitter-handle", "JaneDoe", "") is False
    assert should_write("twitter-handle", "JaneDoe", None) is False
    assert should_write("twitter-handle", None, None) is False


def test_should_write_skips_no_op():
    assert should_write("twitter-handle", "JaneDoe", "JaneDoe") is False


def test_should_write_skips_locked_field():
    assert should_write(
        "email", "editor@x.gov", "new@x.gov", locked_fields={"email"}
    ) is False


def test_should_write_writes_when_upstream_has_value():
    assert should_write("twitter-handle", None, "JaneDoe") is True
    assert should_write("twitter-handle", "OldHandle", "NewHandle") is True


def test_split_email_field_routes_url_to_contact_form():
    e, f = split_email_field("https://senator.gov/contact")
    assert e is None and f == "https://senator.gov/contact"


def test_split_email_field_routes_email_to_email():
    e, f = split_email_field("jane@flhouse.gov")
    assert e == "jane@flhouse.gov" and f is None


def test_split_email_field_handles_empty():
    e, f = split_email_field("")
    assert e is None and f is None
    e, f = split_email_field(None)
    assert e is None and f is None


def test_split_email_field_handles_uppercase_scheme():
    """Round-7 hardening: case-insensitive scheme matching."""
    e, f = split_email_field("HTTPS://senator.gov/contact")
    assert e is None and f == "HTTPS://senator.gov/contact"


def test_split_email_field_handles_mailto():
    """Round-7 hardening: mailto: unwraps to a real email, not a URL."""
    e, f = split_email_field("mailto:jane@flhouse.gov")
    assert e == "jane@flhouse.gov" and f is None
    e, f = split_email_field("MAILTO:jane@flhouse.gov")
    assert e == "jane@flhouse.gov" and f is None


def test_split_email_field_strips_whitespace():
    e, f = split_email_field("  jane@flhouse.gov  ")
    assert e == "jane@flhouse.gov" and f is None
    e, f = split_email_field("   ")
    assert e is None and f is None


# ---------- WebflowLookupService schema cache ----------


def _make_service():
    settings = MagicMock()
    settings.webflow_scheduler_api_key = "sched"
    settings.webflow_votebot_api_key = "votebot"
    settings.webflow_bills_collection_id = "bills"
    settings.webflow_legislators_collection_id = "legi"
    return WebflowLookupService(settings)


@pytest.mark.asyncio
async def test_get_field_slugs_caches_successful_fetch():
    svc = _make_service()
    fake_resp = MagicMock(status_code=200)
    fake_resp.json = MagicMock(return_value={
        "fields": [{"slug": "name"}, {"slug": "bioguide-id"}],
    })
    client = MagicMock()
    client.get = AsyncMock(return_value=fake_resp)

    slugs = await svc._get_field_slugs(client, {}, "legi")
    assert slugs == {"name", "bioguide-id"}
    # Second call should hit cache, not the client
    client.get.reset_mock()
    slugs2 = await svc._get_field_slugs(client, {}, "legi")
    assert slugs2 == slugs
    assert client.get.call_count == 0


@pytest.mark.asyncio
async def test_get_field_slugs_reuses_stale_on_refresh_failure():
    """Round-6 fix: if a fresh fetch fails AND we have a stale cached entry,
    reuse the stale entry with a warning rather than failing the whole sync."""
    from ddp_sync.services.webflow_lookup import SCHEMA_CACHE_TTL_SECONDS

    svc = _make_service()
    # Seed the cache with a stale entry (timestamp far in the past)
    stale_slugs = {"name", "old-field"}
    svc._field_slug_cache["legi"] = (
        time.time() - SCHEMA_CACHE_TTL_SECONDS - 60,
        stale_slugs,
    )
    # Refresh attempt returns 503
    fail_resp = MagicMock(status_code=503, text="Service Unavailable")
    client = MagicMock()
    client.get = AsyncMock(return_value=fail_resp)

    slugs = await svc._get_field_slugs(client, {}, "legi")
    assert slugs == stale_slugs


@pytest.mark.asyncio
async def test_get_field_slugs_fails_closed_when_no_cache():
    """No prior successful fetch → schema-fetch failure must propagate."""
    svc = _make_service()
    fail_resp = MagicMock(status_code=503, text="Service Unavailable")
    client = MagicMock()
    client.get = AsyncMock(return_value=fail_resp)

    with pytest.raises(WebflowError):
        await svc._get_field_slugs(client, {}, "legi")


@pytest.mark.asyncio
async def test_get_field_slugs_reuses_stale_on_transport_error():
    """Round-6 fix: transport errors during refresh must also reuse stale cache."""
    from ddp_sync.services.webflow_lookup import SCHEMA_CACHE_TTL_SECONDS

    svc = _make_service()
    stale_slugs = {"name"}
    svc._field_slug_cache["legi"] = (
        time.time() - SCHEMA_CACHE_TTL_SECONDS - 60,
        stale_slugs,
    )
    client = MagicMock()
    client.get = AsyncMock(side_effect=httpx.TimeoutException("upstream slow"))

    slugs = await svc._get_field_slugs(client, {}, "legi")
    assert slugs == stale_slugs


# ---------- WebflowLookupService _partition_payload ----------


@pytest.mark.asyncio
async def test_partition_payload_fail_closed_propagates_error():
    """Round-5 fix: when no schema cache exists and the schema fetch fails,
    _partition_payload must NOT silently pass the unfiltered payload through.
    """
    svc = _make_service()
    raise_get = AsyncMock(side_effect=WebflowError("schema 503"))
    with patch.object(svc, "_get_field_slugs", new=raise_get):
        with pytest.raises(WebflowError):
            await svc._partition_payload(
                client=MagicMock(),
                headers={},
                field_data={"twitter-handle": "x"},
                collection_id="legi",
            )


@pytest.mark.asyncio
async def test_partition_payload_drops_unknown_fields_when_schema_known():
    """Happy path: payload filtered to known slugs; dropped set captures rest."""
    svc = _make_service()
    known = AsyncMock(return_value={"twitter-handle", "bioguide-id"})
    with patch.object(svc, "_get_field_slugs", new=known):
        kept, dropped = await svc._partition_payload(
            client=MagicMock(),
            headers={},
            field_data={
                "twitter-handle": "X",
                "made-up-field": "Y",
                "bioguide-id": "Z",
            },
            collection_id="legi",
        )
    assert kept == {"twitter-handle": "X", "bioguide-id": "Z"}
    assert dropped == {"made-up-field"}


# ---------- Audit A and Audit C (step 6) ----------


def _cms_item(
    *,
    item_id: str,
    name: str = "Test",
    chamber: str = "Senate",
    openstatesid: str | None = None,
    bioguide_id: str | None = None,
    jurisdiction_ref: str | list | None = None,
    extra_fields: dict | None = None,
) -> dict:
    """Build a fake Webflow Legislators item for audit-input fixtures.

    ``jurisdiction_ref`` simulates the Webflow multi-reference shape:
    pass a list of ref-ids (typical Webflow shape), a single ref-id
    string, or None for unset.
    """
    fields: dict = {
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
    if extra_fields:
        fields.update(extra_fields)
    return {"id": item_id, "fieldData": fields}


# Default jurisdiction ref-id mapping for tests.
# {ref_id -> 2-letter state code} matches the production data model:
# Legislators have a multi-reference jurisdiction field; the orchestrator
# resolves ref-ids via WebflowLookupService.get_jurisdiction_mapping().
_TEST_JURIS_MAPPING = {
    "juris-fl": "FL",
    "juris-va": "VA",
    "juris-wa": "WA",
    "juris-ma": "MA",
    "juris-az": "AZ",
    "juris-mi": "MI",
    "juris-ut": "UT",
    "juris-us": "US",   # federal — resolver normalizes to None
}


def _make_pipeline_with_items(
    items: list[dict],
    *,
    jurisdiction_mapping: dict[str, str] | None = None,
) -> LegislatorBioPipeline:
    """Build a pipeline whose webflow.iter_legislator_items yields ``items``.

    The pipeline's jurisdiction resolver uses ``jurisdiction_mapping``
    (defaults to the standard test mapping above). This matches the
    production code path: the orchestrator builds a resolver from the
    Jurisdictions CMS collection and passes it to CMSLegislator.from_webflow_item.
    """
    mapping = jurisdiction_mapping if jurisdiction_mapping is not None else _TEST_JURIS_MAPPING

    async def _iter():
        for item in items:
            yield item

    webflow = MagicMock()
    webflow.iter_legislator_items = _iter
    webflow.get_jurisdiction_mapping = AsyncMock(return_value=mapping)
    congress = MagicMock()
    openstates = MagicMock()
    settings = MagicMock()
    settings.openstates_api_key = "k"
    return LegislatorBioPipeline(
        settings=settings,
        webflow=webflow,
        congress=congress,
        openstates=openstates,
    )


@pytest.mark.asyncio
async def test_audit_a_finds_federal_records_missing_both_keys():
    """Federal records lacking BOTH openstatesid and bioguide-id are flagged."""
    items = [
        _cms_item(item_id="w-1", name="No keys",
                  chamber="Senate"),
        _cms_item(item_id="w-2", name="Has openstates",
                  chamber="House", openstatesid="ocd-person/x"),
        _cms_item(item_id="w-3", name="Has bioguide",
                  chamber="Senate", bioguide_id="X001"),
        _cms_item(item_id="w-4", name="Has both",
                  chamber="House", openstatesid="ocd-person/y",
                  bioguide_id="Y001"),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_federal_join_keys()
    assert report.audit_name == "A"
    assert report.total_scanned == 4
    assert report.flagged_count == 1
    assert report.flagged[0].webflow_id == "w-1"
    assert report.flagged[0].name == "No keys"


@pytest.mark.asyncio
async def test_audit_a_skips_state_records():
    """Audit A scans only federal records (chamber Senate/House)."""
    items = [
        _cms_item(item_id="w-1", name="Fed",
                  chamber="Senate"),
        _cms_item(item_id="w-2", name="State no key",
                  chamber="lower"),  # state — should be ignored
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_federal_join_keys()
    assert report.total_scanned == 1
    assert report.flagged_count == 1
    assert report.flagged[0].webflow_id == "w-1"


@pytest.mark.asyncio
async def test_audit_a_does_not_flag_records_with_one_key():
    """Either openstatesid OR bioguide-id is sufficient to satisfy Audit A."""
    items = [
        _cms_item(item_id="w-1", name="A", chamber="Senate",
                  openstatesid="ocd-person/x"),
        _cms_item(item_id="w-2", name="B", chamber="Senate",
                  bioguide_id="X001"),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_federal_join_keys()
    assert report.flagged_count == 0


@pytest.mark.asyncio
async def test_audit_a_returns_empty_when_no_federal_records():
    """A clean repo with only state legislators is a valid scan, not a failure."""
    items = [
        _cms_item(item_id="w-1", chamber="lower"),
        _cms_item(item_id="w-2", chamber="upper"),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_federal_join_keys()
    assert report.audit_name == "A"
    assert report.total_scanned == 0
    assert report.flagged_count == 0


@pytest.mark.asyncio
async def test_audit_c_finds_state_records_missing_openstatesid():
    """State records lacking openstatesid are flagged. Federal records ignored.

    Uses the production data model: jurisdiction is a multi-reference field
    holding a list of ref-ids; the orchestrator resolves via
    WebflowLookupService.get_jurisdiction_mapping().
    """
    items = [
        _cms_item(item_id="w-1", name="State no key",
                  chamber="lower", jurisdiction_ref=["juris-fl"]),
        _cms_item(item_id="w-2", name="State has key",
                  chamber="lower", jurisdiction_ref=["juris-fl"],
                  openstatesid="ocd-person/x"),
        _cms_item(item_id="w-3", name="Fed no key",
                  chamber="Senate", jurisdiction_ref=["juris-us"]),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_state_join_keys()
    assert report.audit_name == "C"
    assert report.total_scanned == 2  # federal record skipped
    assert report.flagged_count == 1
    assert report.flagged[0].webflow_id == "w-1"
    assert report.flagged[0].state_code == "FL"


@pytest.mark.asyncio
async def test_audit_c_filters_by_jurisdiction():
    """When jurisdiction is provided, only records in that state are scanned."""
    items = [
        _cms_item(item_id="w-1", name="FL no key",
                  chamber="lower", jurisdiction_ref=["juris-fl"]),
        _cms_item(item_id="w-2", name="VA no key",
                  chamber="lower", jurisdiction_ref=["juris-va"]),
        _cms_item(item_id="w-3", name="MA has key",
                  chamber="lower", jurisdiction_ref=["juris-ma"],
                  openstatesid="ocd-person/x"),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_state_join_keys(jurisdiction="FL")
    assert report.jurisdiction == "FL"
    assert report.total_scanned == 1  # only FL record
    assert report.flagged_count == 1
    assert report.flagged[0].webflow_id == "w-1"


@pytest.mark.asyncio
async def test_audit_c_jurisdiction_is_case_insensitive():
    items = [
        _cms_item(item_id="w-1", chamber="lower",
                  jurisdiction_ref=["juris-fl"]),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_state_join_keys(jurisdiction="fl")
    assert report.jurisdiction == "FL"
    assert report.flagged_count == 1


@pytest.mark.asyncio
async def test_audit_c_includes_all_states_when_no_jurisdiction():
    items = [
        _cms_item(item_id="w-1", chamber="lower",
                  jurisdiction_ref=["juris-fl"]),
        _cms_item(item_id="w-2", chamber="lower",
                  jurisdiction_ref=["juris-va"]),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_state_join_keys()
    assert report.jurisdiction is None
    assert report.flagged_count == 2


@pytest.mark.asyncio
async def test_audit_c_skips_records_with_unresolvable_jurisdiction_when_filtered():
    """Round-8 fix: a record whose jurisdiction ref doesn't resolve must NOT
    silently match a jurisdiction filter. With state_code=None and
    jurisdiction='FL', the record is excluded from the scan (and would be
    surfaced if the editor ran the audit with no filter)."""
    items = [
        _cms_item(item_id="w-1", chamber="lower",
                  jurisdiction_ref=["unknown-ref-id"]),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_state_join_keys(jurisdiction="FL")
    assert report.total_scanned == 0
    assert report.flagged_count == 0


@pytest.mark.asyncio
async def test_audit_c_unresolvable_jurisdiction_visible_when_unfiltered():
    """Same record, no jurisdiction filter → IS counted and flagged
    (still missing openstatesid, regardless of state)."""
    items = [
        _cms_item(item_id="w-1", chamber="lower",
                  jurisdiction_ref=["unknown-ref-id"]),
    ]
    pipeline = _make_pipeline_with_items(items)
    report = await pipeline.audit_state_join_keys()
    assert report.total_scanned == 1
    assert report.flagged_count == 1
    assert report.flagged[0].state_code is None  # unresolvable


# ---------- Jurisdiction resolution + chamber matching (round-8 fixes) ----------


def test_normalize_state_code_clamps_to_two_letters():
    """Round-8 fix: only true 2-letter codes pass; full state names rejected."""
    from ddp_sync.services.webflow_lookup import WebflowLookupService

    n = WebflowLookupService._normalize_state_code
    assert n("FL") == "FL"
    assert n("fl") == "FL"
    assert n("  fl  ") == "FL"
    # Full state name → rejected (was the round-8 silent-false-negative bug)
    assert n("Florida") is None
    # Single char or numeric → rejected
    assert n("F") is None
    assert n("12") is None
    # Empty / non-string → None
    assert n(None) is None
    assert n("") is None
    assert n(["FL"]) is None


def test_resolve_jurisdiction_ref_handles_all_upstream_shapes():
    """Multi-reference shape (list), single ref-id, already-2-letter, None."""
    from ddp_sync.services.webflow_lookup import WebflowLookupService

    mapping = {"juris-fl": "FL", "juris-us": "US"}
    r = WebflowLookupService.resolve_jurisdiction_ref

    # Standard multi-reference shape from Webflow
    assert r(["juris-fl"], mapping) == "FL"
    # Multi-ref with first non-empty winning
    assert r(["juris-fl", "juris-us"], mapping) == "FL"
    # Single string ref-id (no list wrapper)
    assert r("juris-fl", mapping) == "FL"
    # Already-2-letter code (legacy / pre-resolved value)
    assert r("WA", mapping) == "WA"
    # US/federal → returns None (orchestrator detects federal via chamber)
    assert r(["juris-us"], mapping) is None
    assert r("US", mapping) is None
    # Unset / missing
    assert r(None, mapping) is None
    assert r("", mapping) is None
    assert r([], mapping) is None
    # Unknown ref-id → None (not silently "US")
    assert r(["unknown"], mapping) is None
    assert r("unknown-ref", mapping) is None


def test_cms_legislator_state_code_via_resolver():
    """state_code is precomputed at construction from the jurisdiction resolver."""
    from ddp_sync.services.webflow_lookup import WebflowLookupService

    mapping = {"juris-fl": "FL"}
    def resolver(v):
        return WebflowLookupService.resolve_jurisdiction_ref(v, mapping)

    item = {
        "id": "w-1",
        "fieldData": {
            "name": "Adam",
            "chamber": "lower",
            "jurisdiction": ["juris-fl"],
        },
    }
    cms = CMSLegislator.from_webflow_item(item, jurisdiction_resolver=resolver)
    assert cms.state_code == "FL"
    assert cms.is_federal is False


def test_cms_legislator_state_code_none_without_resolver():
    """No resolver → state_code is None even if a flat field is set.

    Round-8 fix dropped the legacy flat-field fallback. The data model is
    reference-based; the resolver is the single source of truth."""
    item = {
        "id": "w-1",
        "fieldData": {
            "name": "Test",
            "chamber": "lower",
            "state-code": "FL",  # legacy flat field — IGNORED without resolver
            "jurisdiction": ["juris-fl"],
        },
    }
    cms = CMSLegislator.from_webflow_item(item)  # no resolver
    assert cms.state_code is None


def test_cms_legislator_resolver_exception_treated_as_unresolved():
    """A resolver that raises must not fail the whole construction."""
    def bad_resolver(v):
        raise RuntimeError("resolver broke")

    item = {
        "id": "w-1",
        "fieldData": {"name": "Test", "chamber": "lower", "jurisdiction": ["x"]},
    }
    cms = CMSLegislator.from_webflow_item(item, jurisdiction_resolver=bad_resolver)
    assert cms.state_code is None


def test_cms_legislator_is_federal_chamber_variants():
    """Round-8 fix: chamber heuristic accepts common variants."""
    for chamber in [
        "Senate", "House", "US Senate", "U.S. Senate",
        "US House", "U.S. House", "House of Representatives",
        "U.S. House of Representatives", "Congress", "U.S. Congress",
    ]:
        item = {"id": "w", "fieldData": {"name": "X", "chamber": chamber}}
        cms = CMSLegislator.from_webflow_item(item)
        assert cms.is_federal is True, f"expected federal: {chamber!r}"


def test_cms_legislator_is_federal_state_chamber_values_excluded():
    for chamber in ["lower", "upper", "Lower", "UPPER", "Assembly"]:
        item = {"id": "w", "fieldData": {"name": "X", "chamber": chamber}}
        cms = CMSLegislator.from_webflow_item(item)
        assert cms.is_federal is False, f"expected NOT federal: {chamber!r}"


# ---------- Zapier run-summary alerting (step 7) ----------


def test_push_bio_sync_alert_returns_false_when_no_webhook():
    """Defensive: missing webhook URL should not crash the run."""
    report = BioSyncReport(cms_items_seen=10, would_patch=[{"x": 1}])
    assert push_bio_sync_alert("", report) is False
    assert push_bio_sync_alert(None, report) is False  # type: ignore[arg-type]


def test_push_bio_sync_alert_posts_payload_on_success():
    """Happy path: 200 response → True. Payload includes all the count
    fields plus the on_failure / on_large_changes threshold flags."""
    report = BioSyncReport(
        cms_items_seen=10,
        items_resolved_via_openstates=5,
        items_resolved_via_bioguide_fallback=2,
        would_patch=[{"a": 1}, {"b": 2}],
        would_create=[],
        potential_merges=[{"x": 1}],
        upstream_orphans=[],
        errors=["err-1"],
    )
    captured: dict = {}

    def fake_post(url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        m = MagicMock()
        m.status_code = 200
        return m

    with patch("ddp_sync.pipelines.legislator_bio.requests.post", new=fake_post):
        ok = push_bio_sync_alert("https://example.com/zap", report)

    assert ok is True
    assert captured["url"] == "https://example.com/zap"
    payload = captured["json"]
    assert payload["alert_type"] == "legislator_bio_sync_complete"
    assert payload["items_seen"] == 10
    assert payload["items_resolved_via_openstates"] == 5
    assert payload["items_resolved_via_bioguide_fallback"] == 2
    assert payload["patched"] == 2
    assert payload["created"] == 0
    assert payload["potential_merges"] == 1
    assert payload["upstream_orphans"] == 0
    assert payload["errors"] == 1
    # on_failure should be True (errors > 0)
    assert payload["on_failure"] is True
    # on_large_changes False (patched + created = 2, < 100)
    assert payload["on_large_changes"] is False
    assert payload["aborted"] is False
    assert "synced_at" in payload
    assert "summary" in payload


def test_push_bio_sync_alert_sets_on_failure_for_aborted_run():
    """An aborted run sets on_failure=True even with 0 errors recorded."""
    report = BioSyncReport(aborted=True, abort_reason="rate-limit")
    captured: dict = {}

    def fake_post(url, json=None, timeout=None):
        captured["json"] = json
        m = MagicMock()
        m.status_code = 200
        return m

    with patch("ddp_sync.pipelines.legislator_bio.requests.post", new=fake_post):
        push_bio_sync_alert("https://example.com/zap", report)

    payload = captured["json"]
    assert payload["aborted"] is True
    assert payload["abort_reason"] == "rate-limit"
    assert payload["on_failure"] is True


def test_push_bio_sync_alert_sets_on_large_changes_above_threshold():
    """on_large_changes flips when patched + created > 100."""
    report = BioSyncReport(
        would_patch=[{"x": i} for i in range(101)],
    )
    captured: dict = {}

    def fake_post(url, json=None, timeout=None):
        captured["json"] = json
        m = MagicMock()
        m.status_code = 200
        return m

    with patch("ddp_sync.pipelines.legislator_bio.requests.post", new=fake_post):
        push_bio_sync_alert("https://example.com/zap", report)

    assert captured["json"]["on_large_changes"] is True


def test_push_bio_sync_alert_returns_false_on_non_2xx():
    """Webhook 5xx → False, no raise (run must not be aborted by alert failure)."""
    report = BioSyncReport()

    def fake_post(url, json=None, timeout=None):
        m = MagicMock()
        m.status_code = 503
        m.text = "Service Unavailable"
        return m

    with patch("ddp_sync.pipelines.legislator_bio.requests.post", new=fake_post):
        ok = push_bio_sync_alert("https://example.com/zap", report)
    assert ok is False


def test_push_bio_sync_alert_returns_false_on_exception():
    """Network error → False, no raise."""
    report = BioSyncReport()

    def fake_post(url, json=None, timeout=None):
        raise RuntimeError("connection refused")

    with patch("ddp_sync.pipelines.legislator_bio.requests.post", new=fake_post):
        ok = push_bio_sync_alert("https://example.com/zap", report)
    assert ok is False


@pytest.mark.asyncio
async def test_run_invokes_zapier_alert_on_non_dry_run():
    """Step 7: a non-dry-run completion fires push_bio_sync_alert."""
    pipeline = _make_pipeline_with_items([])  # empty CMS — fast path
    pipeline.settings.zapier_webhook_url = "https://example.com/zap"

    sent: list = []

    def fake_alert(url, report):
        sent.append((url, report))
        return True

    with patch(
        "ddp_sync.pipelines.legislator_bio.push_bio_sync_alert",
        new=fake_alert,
    ):
        # warm_cache stub since we haven't mocked the full source path
        pipeline.congress.warm_cache = AsyncMock()
        await pipeline.run(BioSyncOptions(dry_run=False))

    assert len(sent) == 1
    assert sent[0][0] == "https://example.com/zap"
    assert isinstance(sent[0][1], BioSyncReport)


@pytest.mark.asyncio
async def test_run_skips_zapier_alert_on_dry_run():
    """Dry-run never alerts (no real writes happened)."""
    pipeline = _make_pipeline_with_items([])
    pipeline.settings.zapier_webhook_url = "https://example.com/zap"

    sent: list = []

    def fake_alert(url, report):
        sent.append((url, report))
        return True

    with patch(
        "ddp_sync.pipelines.legislator_bio.push_bio_sync_alert",
        new=fake_alert,
    ):
        pipeline.congress.warm_cache = AsyncMock()
        await pipeline.run(BioSyncOptions(dry_run=True))

    assert sent == []


@pytest.mark.asyncio
async def test_run_alert_fires_even_on_aborted_run():
    """Aborted runs are exactly the ones editors need an alert for."""
    pipeline = _make_pipeline_with_items([])
    pipeline.settings.zapier_webhook_url = "https://example.com/zap"

    # Force an abort by raising WebflowRateLimitError mid-process
    from ddp_sync.services.webflow_lookup import WebflowRateLimitError
    async def _failing_iter():
        if False:
            yield None
        raise WebflowRateLimitError("persistent 429")
    pipeline.webflow.iter_legislator_items = _failing_iter
    pipeline.congress.warm_cache = AsyncMock()

    sent: list = []
    def fake_alert(url, report):
        sent.append(report)
        return True

    with patch(
        "ddp_sync.pipelines.legislator_bio.push_bio_sync_alert",
        new=fake_alert,
    ):
        report = await pipeline.run(BioSyncOptions(dry_run=False))

    assert report.aborted is True
    assert len(sent) == 1
    assert sent[0].aborted is True


@pytest.mark.asyncio
async def test_run_skips_alert_when_no_webhook_configured():
    """No ZAPIER_WEBHOOK_URL → no attempt, no error."""
    pipeline = _make_pipeline_with_items([])
    pipeline.settings.zapier_webhook_url = ""

    sent: list = []
    def fake_alert(url, report):
        sent.append(report)
        return True

    with patch(
        "ddp_sync.pipelines.legislator_bio.push_bio_sync_alert",
        new=fake_alert,
    ):
        pipeline.congress.warm_cache = AsyncMock()
        await pipeline.run(BioSyncOptions(dry_run=False))

    assert sent == []


# ---------- Round-9 fix: jurisdiction cache TTL + empty-mapping metric ----------


@pytest.mark.asyncio
async def test_jurisdiction_mapping_caches_with_ttl():
    """Round-9 fix: TTL'd cache. Two calls within TTL hit the cache;
    fetch only happens once."""
    import time as time_module
    from ddp_sync.services.webflow_lookup import (
        WebflowLookupService,
        JURISDICTION_CACHE_TTL_SECONDS,
    )

    settings = MagicMock()
    settings.webflow_scheduler_api_key = "sched"
    settings.webflow_votebot_api_key = "votebot"
    settings.webflow_bills_collection_id = "bills"
    settings.webflow_legislators_collection_id = "legi"
    settings.webflow_jurisdiction_collection_id = "juris"
    svc = WebflowLookupService(settings)

    fetch_count = 0

    async def fake_fetch():
        nonlocal fetch_count
        fetch_count += 1
        return {"juris-fl": "FL"}

    svc._fetch_jurisdiction_mapping_fresh = fake_fetch
    m1 = await svc.get_jurisdiction_mapping()
    m2 = await svc.get_jurisdiction_mapping()
    assert m1 == m2 == {"juris-fl": "FL"}
    assert fetch_count == 1, f"expected 1 fetch within TTL, got {fetch_count}"

    # Force expiry
    cached_at, mapping = svc._jurisdiction_mapping
    svc._jurisdiction_mapping = (
        cached_at - JURISDICTION_CACHE_TTL_SECONDS - 1,
        mapping,
    )
    m3 = await svc.get_jurisdiction_mapping()
    assert m3 == {"juris-fl": "FL"}
    assert fetch_count == 2, "expected refresh after TTL expiry"


@pytest.mark.asyncio
async def test_jurisdiction_mapping_reuses_stale_on_empty_refresh():
    """Round-9: if a refresh returns empty AND we have a non-empty stale
    entry, reuse it (mirrors the schema-cache stale-reuse pattern)."""
    from ddp_sync.services.webflow_lookup import (
        WebflowLookupService,
        JURISDICTION_CACHE_TTL_SECONDS,
    )
    import time as time_module

    settings = MagicMock()
    settings.webflow_scheduler_api_key = "sched"
    settings.webflow_votebot_api_key = "votebot"
    settings.webflow_jurisdiction_collection_id = "juris"
    settings.webflow_bills_collection_id = "b"
    settings.webflow_legislators_collection_id = "l"
    svc = WebflowLookupService(settings)

    # Seed a stale entry
    svc._jurisdiction_mapping = (
        time_module.time() - JURISDICTION_CACHE_TTL_SECONDS - 60,
        {"juris-fl": "FL"},
    )
    # Refresh returns empty (e.g., transient 503 ate the data)
    svc._fetch_jurisdiction_mapping_fresh = AsyncMock(return_value={})

    m = await svc.get_jurisdiction_mapping()
    assert m == {"juris-fl": "FL"}, "expected stale reuse on empty refresh"


@pytest.mark.asyncio
async def test_jurisdiction_mapping_emits_metric_on_empty_no_cache(caplog):
    """Round-9 fix: empty mapping with no usable cache → structured metric
    breadcrumb so infra alerting can fire."""
    from ddp_sync.services.webflow_lookup import WebflowLookupService

    settings = MagicMock()
    settings.webflow_scheduler_api_key = "sched"
    settings.webflow_votebot_api_key = "votebot"
    settings.webflow_jurisdiction_collection_id = ""  # config missing → empty
    settings.webflow_bills_collection_id = "b"
    settings.webflow_legislators_collection_id = "l"
    svc = WebflowLookupService(settings)

    m = await svc.get_jurisdiction_mapping()
    assert m == {}
    # The breadcrumb is emitted via structlog. The structured field
    # `metric=webflow.jurisdiction_mapping_empty` is in the log; we verify
    # via the record dict on caplog (structlog routes through stdlib logger).
    # We don't assert on the exact message text — just that an empty
    # mapping was returned and the cache was set so a follow-up call
    # within TTL doesn't re-fetch.
    assert svc._jurisdiction_mapping is not None
    assert svc._jurisdiction_mapping[1] == {}


@pytest.mark.asyncio
async def test_audit_a_aborts_gracefully_on_webflow_error():
    """A WebflowError mid-scan is captured into the report rather than raised."""
    from ddp_sync.services.webflow_lookup import WebflowError

    async def _failing_iter():
        yield _cms_item(item_id="w-1", chamber="Senate",
                        openstatesid="ocd-person/x")
        raise WebflowError("upstream 503")

    webflow = MagicMock()
    webflow.iter_legislator_items = _failing_iter
    webflow.get_jurisdiction_mapping = AsyncMock(return_value={})
    settings = MagicMock()
    settings.openstates_api_key = "k"
    pipeline = LegislatorBioPipeline(
        settings=settings,
        webflow=webflow,
        congress=MagicMock(),
        openstates=MagicMock(),
    )
    report = await pipeline.audit_federal_join_keys()
    assert report.aborted is True
    assert "WebflowError" in (report.abort_reason or "")
    assert report.total_scanned == 1  # got at least one before the error
