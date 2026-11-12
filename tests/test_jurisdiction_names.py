"""OPEN-420 / SYNC-157: source names for jurisdictions come from the manifest, with no per-state table.

The names label the source of ingested documents, so they must not change for the tracked states (pinned below against
the real manifest when a checkout is pointed at, and against a stand-in manifest otherwise).
"""

from __future__ import annotations

import os

import pytest
import yaml

from ddp_sync.ingestion.sources.openstates import OpenStatesSource
from ddp_sync.pipelines.bill_sync import BillSyncService
from ddp_sync.pipelines.legislator_sync import LegislatorSyncService
from ddp_sync.services import jurisdiction_names
from ddp_sync.services.jurisdiction_names import source_name

# opt in: point OPENSTATES_ROOT at a ddp-open-states checkout on main (read at import; other tests clear the env var)
_REAL_CHECKOUT = os.environ.get("OPENSTATES_ROOT", "")

# The names in the knowledge base today (OPEN-420's table, deleted by SYNC-157): must not change.
EXISTING = {
    "us": "US Congress",
    "fl": "Florida Legislature",
    "wa": "Washington Legislature",
    "va": "Virginia Legislature",
    "mi": "Michigan Legislature",
    "ma": "Massachusetts Legislature",
    "ut": "Utah Legislature",
    "az": "Arizona Legislature",
    "al": "Alabama Legislature",
    "nc": "North Carolina Legislature",
    "pa": "Pennsylvania Legislature",
    "ne": "Nebraska Legislature",
    "la": "Louisiana Legislature",
}

_NAMES = {"us": "United States (Congress)", "fl": "Florida", "wa": "Washington", "va": "Virginia", "mi": "Michigan",
          "ma": "Massachusetts", "ut": "Utah", "az": "Arizona", "al": "Alabama", "nc": "North Carolina",
          "pa": "Pennsylvania", "ne": "Nebraska", "la": "Louisiana", "zz": "Zedland"}


@pytest.fixture
def manifest_root(tmp_path, monkeypatch):
    """A stand-in shared list with the real manifest's names (and one new state)."""
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    je._logged.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({c: {"name": n} for c, n in _NAMES.items()}))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    return str(tmp_path)


@pytest.mark.parametrize("code,expected", sorted(EXISTING.items()))
def test_the_existing_names_are_unchanged(manifest_root, code, expected):
    assert source_name(code) == expected
    assert source_name(code.upper()) == expected


def test_a_state_added_to_the_shared_list_needs_no_edit_here(manifest_root):
    assert source_name("zz") == "Zedland Legislature"


def test_the_federal_label_is_not_the_manifest_name(manifest_root):
    assert source_name("us") == source_name("usa") == "US Congress"  # the manifest says "United States (Congress)"


def test_an_unreadable_shared_list_falls_back_to_the_code(tmp_path, monkeypatch):
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path / "nowhere"))
    assert source_name("fl") == "FL Legislature"
    assert source_name("us") == "US Congress"


def test_unknown_and_empty_codes(manifest_root):
    assert source_name("qq") == "QQ Legislature"
    assert source_name("") == "State Legislature"
    assert source_name(None) == "State Legislature"


def test_there_is_no_per_state_table_any_more():
    """SYNC-157: no dict keyed by state codes in this module (the manifest is the one list)."""
    tables = [v for v in vars(jurisdiction_names).values() if isinstance(v, dict) and "fl" in v]
    assert tables == []


@pytest.mark.parametrize("cls", [BillSyncService, LegislatorSyncService, OpenStatesSource])
def test_all_three_call_sites_read_the_one_function(manifest_root, cls):
    for code, expected in EXISTING.items():
        assert cls._get_source_name(object(), code) == expected
    assert cls._get_source_name(object(), "") == "State Legislature"


def test_every_jurisdiction_in_the_real_manifest_has_a_real_name(monkeypatch):
    if not _REAL_CHECKOUT or not os.path.exists(os.path.join(_REAL_CHECKOUT, "jurisdictions.yaml")):
        pytest.skip("set OPENSTATES_ROOT to a ddp-open-states checkout to run this")
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", _REAL_CHECKOUT)
    manifest = yaml.safe_load(open(os.path.join(_REAL_CHECKOUT, "jurisdictions.yaml")))
    assert {code: source_name(code) for code in manifest if code in EXISTING} == {
        code: name for code, name in EXISTING.items() if code in manifest}
    fallback = sorted(code for code in manifest if source_name(code) == f"{code.upper()} Legislature")
    assert fallback == [], f"these have no name in the shared list: {fallback}"
