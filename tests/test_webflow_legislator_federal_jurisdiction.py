"""Tests for SYNC-76: WebflowSource._resolve_jurisdiction() must classify federal
Congress members as jurisdiction "US" regardless of their CMS `jurisdiction`
reference, which points at their represented home state, not their OpenStates
jurisdiction.
"""

from __future__ import annotations

from ddp_sync.config import SyncSettings
from ddp_sync.ingestion.sources.webflow import WebflowSource

_US_HOUSE_SEAT_REF = "66316e20ae88354aed5df702"
_US_SENATE_SEAT_REF = "66316e0956dc73af879134b4"
_STATE_HOUSE_SEAT_REF = "655288ef928edb1283067463"


def _make_source() -> WebflowSource:
    return WebflowSource(settings=SyncSettings())


def test_resolve_jurisdiction_federal_house_seat_overrides_home_state_ref():
    source = _make_source()
    source._jurisdiction_cache["california-ref-id"] = "CA"

    assert source._resolve_jurisdiction("california-ref-id", seat=[_US_HOUSE_SEAT_REF]) == "US"


def test_resolve_jurisdiction_federal_senate_seat_overrides_home_state_ref():
    source = _make_source()
    source._jurisdiction_cache["new-jersey-ref-id"] = "NJ"

    assert source._resolve_jurisdiction("new-jersey-ref-id", seat=_US_SENATE_SEAT_REF) == "US"


def test_resolve_jurisdiction_state_seat_still_resolves_home_state():
    source = _make_source()
    source._jurisdiction_cache["florida-ref-id"] = "FL"

    assert source._resolve_jurisdiction("florida-ref-id", seat=[_STATE_HOUSE_SEAT_REF]) == "FL"


def test_resolve_jurisdiction_no_seat_falls_back_to_existing_behavior():
    source = _make_source()
    source._jurisdiction_cache["washington-ref-id"] = "WA"

    assert source._resolve_jurisdiction("washington-ref-id") == "WA"
    assert source._resolve_jurisdiction(None) == "US"


def test_process_legislator_item_federal_member_gets_us_jurisdiction():
    source = _make_source()
    source._jurisdiction_cache["california-ref-id"] = "CA"

    item = {
        "id": "item-1",
        "fieldData": {
            "name": "Test Representative",
            "openstatesid": "abc-123",
            "jurisdiction": "california-ref-id",
            "seat": [_US_HOUSE_SEAT_REF],
            "post-body": "Some scorecard content long enough to pass extraction.",
        },
    }

    doc = source._process_legislator_item(item)

    assert doc is not None
    assert doc.metadata.jurisdiction == "US"
