"""OPEN-420: one table of source names for jurisdictions, used by all three call sites.

The three copies in bill_sync, legislator_sync and the OpenStates source were identical and none had `nc`. The
names label the source of ingested documents, so they must not change for the existing states (pinned below), and
the next enrolled state must not be forgotten (checked against the manifest when a checkout is pointed at).
"""

from __future__ import annotations

import os

import pytest
import yaml

from ddp_sync.ingestion.sources.openstates import OpenStatesSource
from ddp_sync.pipelines.bill_sync import BillSyncService
from ddp_sync.pipelines.legislator_sync import LegislatorSyncService
from ddp_sync.services.jurisdiction_names import JURISDICTION_SOURCE_NAMES, source_name

# opt in: point OPENSTATES_ROOT at a ddp-open-states checkout on main (read at import; other tests clear the env var)
_REAL_CHECKOUT = os.environ.get("OPENSTATES_ROOT", "")

# The names the three copies carried before OPEN-420 (their wording is data in the knowledge base): must not change.
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
    "ca": "California Legislature",
    "ny": "New York Legislature",
    "tx": "Texas Legislature",
}


# OPEN-478/479/480: pilot #2's states carry their own names, not the "<CODE> Legislature" fallback.
PILOT_2 = {"pa": "Pennsylvania Legislature", "ne": "Nebraska Legislature", "la": "Louisiana Legislature"}


@pytest.fixture
def manifest_root(tmp_path, monkeypatch):
    """A tiny shared list standing in for the ddp-open-states checkout (SYNC-146: names come from it)."""
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    je._logged.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({
        "pa": {"name": "Pennsylvania"}, "ne": {"name": "Nebraska"}, "la": {"name": "Louisiana"},
        "zz": {"name": "Zedland"}, "us": {"name": "United States (Congress)"}, "fl": {"name": "FL from the manifest"},
    }))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    return str(tmp_path)


@pytest.mark.parametrize("code,expected", sorted(PILOT_2.items()))
def test_pilot_2_states_keep_their_table_names_even_when_the_shared_list_cannot_be_read(tmp_path, monkeypatch, code, expected):
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path / "nowhere"))
    assert JURISDICTION_SOURCE_NAMES[code] == expected and source_name(code) == expected


def test_a_state_added_to_the_shared_list_needs_no_edit_here(manifest_root):
    assert source_name("zz") == "Zedland Legislature"


def test_the_table_wins_over_the_manifest_so_existing_names_never_change(manifest_root):
    assert source_name("us") == "US Congress"          # the manifest says "United States (Congress)"
    assert source_name("fl") == "Florida Legislature"  # the manifest here says something else


def test_an_unreadable_shared_list_keeps_the_old_fallback(tmp_path, monkeypatch):
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path / "nowhere"))
    assert source_name("zz") == "ZZ Legislature"


@pytest.mark.parametrize("code,expected", sorted(EXISTING.items()))
def test_the_existing_names_are_unchanged(code, expected):
    assert source_name(code) == expected
    assert source_name(code.upper()) == expected


def test_nc_is_labelled_like_the_others_not_by_the_fallback():
    assert source_name("nc") == "North Carolina Legislature"
    assert source_name("nc") != "NC Legislature"


def test_unknown_and_empty_codes_keep_the_old_fallbacks():
    assert source_name("zz") == "ZZ Legislature"
    assert source_name("") == "State Legislature"
    assert source_name(None) == "State Legislature"


def test_the_table_is_the_existing_names_plus_nc_and_pilot_2_and_nothing_else():
    """States added AFTER these come from the shared list (SYNC-146), not from a new table line."""
    assert JURISDICTION_SOURCE_NAMES == {**EXISTING, "nc": "North Carolina Legislature", **PILOT_2}


@pytest.mark.parametrize("cls", [BillSyncService, LegislatorSyncService, OpenStatesSource])
def test_all_three_call_sites_read_the_one_table(cls):
    """The three classes delegate: none carries a table of its own any more."""
    assert not hasattr(cls, "JURISDICTION_SOURCE_NAMES")
    for code in JURISDICTION_SOURCE_NAMES:
        assert cls._get_source_name(object(), code) == JURISDICTION_SOURCE_NAMES[code]
    assert cls._get_source_name(object(), "nc") == "North Carolina Legislature"
    assert cls._get_source_name(object(), "") == "State Legislature"


def test_every_jurisdiction_in_the_manifest_has_a_real_name_not_the_code_fallback(monkeypatch):
    """SYNC-146: a state missing from the table is labelled from the shared list, so none may fall back to
    "<CODE> Legislature" (that would only happen if the manifest could not be read or had no name)."""
    if not _REAL_CHECKOUT or not os.path.exists(os.path.join(_REAL_CHECKOUT, "jurisdictions.yaml")):
        pytest.skip("set OPENSTATES_ROOT to a ddp-open-states checkout to run this")
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", _REAL_CHECKOUT)
    manifest = yaml.safe_load(open(os.path.join(_REAL_CHECKOUT, "jurisdictions.yaml")))
    fallback = sorted(code for code in manifest if source_name(code) == f"{code.upper()} Legislature")
    assert fallback == [], f"these have no name in the shared list or the table: {fallback}"
