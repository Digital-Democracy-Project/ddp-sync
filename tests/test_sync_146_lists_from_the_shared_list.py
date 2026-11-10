"""SYNC-146: a state added to the shared list (ddp-open-states jurisdictions.yaml) needs no ddp-sync edit.

The cloud-run ownership / S3-memory gates take their answer from the shared list in `authoritative` mode; `shadow` mode
(the default) and an unreadable shared list keep the yaml's own answer exactly as before. SYNC-152: who is scraped has
no yaml list at all any more, so the scraped states are the shared list's in either mode (`enrolled_scrape_states`).
A tiny manifest in a temp dir stands in for the ddp-open-states checkout.
"""

from __future__ import annotations

import pytest
import yaml

from ddp_sync.pipelines.openstates_scrape import (
    _cloud_path_owns,
    _memory_backend_enabled,
    enrolled_scrape_states,
)
from ddp_sync.services import jurisdiction_enrollment as je


def _entry(tier="secondary", status="live", cloud_path=True):
    return {
        "tier": tier, "status": status,
        "archive": {"enabled": True, "scheduled": status == "live"},
        "enrollment": {"embedding": False, "search_refresh": False, "cloud_path": cloud_path},
    }


# The nine live states as they are today (tier, cloud_path), plus the three pilot #2 states and a paused one.
MANIFEST = {
    "us": _entry("primary"), "fl": _entry("primary"), "wa": _entry("primary"),
    "va": _entry(), "mi": _entry(), "ut": _entry(), "az": _entry(), "ma": _entry(), "nc": _entry(),
    "pa": _entry(status="scraping"), "ne": _entry(status="scraping"), "la": _entry(status="scraping"),
    "al": _entry(status="paused", cloud_path=False),
}
LIVE_SECONDARY = ["az", "ma", "mi", "nc", "ut", "va"]  # what sync_schedule.yaml's secondary list holds today for them


def _config(root, secondary=None, cloud=None, memory=None):
    return {
        "openstates_root": root,
        "secondary": {"enabled": True, "jurisdictions": list(secondary if secondary is not None else LIVE_SECONDARY)},
        "cloud_path": {
            "enabled": True,
            "jurisdictions": list(cloud if cloud is not None else ["usa", "fl", "wa", *LIVE_SECONDARY]),
            "memory_backend_jurisdictions": list(memory or []),
        },
    }


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    je._cache.clear()
    je._logged.clear()
    monkeypatch.delenv("OPENSTATES_ROOT", raising=False)
    monkeypatch.setenv(je.MODE_ENV, "authoritative")


@pytest.fixture
def root(tmp_path):
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(MANIFEST))
    return str(tmp_path)


# --- a new state needs no yaml entry ---------------------------------------------------------------------


def test_a_scraping_state_is_scraped_with_no_yaml_entry(root):
    got = enrolled_scrape_states(_config(root))
    assert {"pa", "ne", "la"} <= set(got)                      # not in the yaml list above, still scheduled
    assert "al" not in got                                     # paused: on no schedule
    assert set(LIVE_SECONDARY) <= set(got)


def test_a_probing_state_is_not_scraped(root, tmp_path):
    data = dict(MANIFEST, zz=_entry(status="probing"))
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(data))
    assert "zz" not in enrolled_scrape_states(_config(root))


def test_cloud_path_ownership_and_the_s3_memory_floor_follow_the_shared_list(root):
    cfg = _config(root)
    for code in ("pa", "ne", "la"):                            # none is in the yaml cloud list
        assert _cloud_path_owns(code, cfg) is True
        assert _memory_backend_enabled(code, cfg) is True      # owned => the S3 backend, no memory entry needed
    assert _cloud_path_owns("al", cfg) is False                # manifest: cloud_path false


def test_a_state_the_manifest_takes_off_the_cloud_path_stops_being_owned_but_keeps_a_listed_memory_floor(root, tmp_path):
    data = dict(MANIFEST, pa=_entry(status="scraping", cloud_path=False))
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(data))
    je._cache.clear()
    cfg = _config(root, cloud=["pa"], memory=[])
    assert _cloud_path_owns("pa", cfg) is False                # the manifest wins in authoritative mode
    assert _memory_backend_enabled("pa", cfg) is False         # nothing remembers it unless it was listed first
    cfg_listed = _config(root, cloud=["pa"], memory=["pa"])
    assert _memory_backend_enabled("pa", cfg_listed) is True   # the rollback rule: the yaml list is the memory


def test_the_federal_state_is_usa_on_the_cloud_side(root):
    assert _cloud_path_owns("usa", _config(root)) is True
    assert _cloud_path_owns("us", _config(root)) is False


# --- nothing changes for the nine live states ---------------------------------------------------------------


def test_the_nine_live_states_get_exactly_the_answers_the_yaml_gave(root):
    cfg = _config(root)
    yaml_cloud = {"usa", "fl", "wa", *LIVE_SECONDARY}
    for code in ["usa", "fl", "wa", *LIVE_SECONDARY]:
        assert _cloud_path_owns(code, cfg) is True
        assert code in yaml_cloud
    assert {"usa", "fl", "wa", *LIVE_SECONDARY} <= set(enrolled_scrape_states(cfg))


# --- fallbacks: shadow mode and an unreadable shared list keep today's behaviour ------------------------------


def test_shadow_mode_is_the_yaml_exactly_for_the_gates_that_still_have_a_yaml_list(root, monkeypatch):
    monkeypatch.delenv(je.MODE_ENV)
    cfg = _config(root)
    assert {"pa", "ne", "la"} <= set(enrolled_scrape_states(cfg))  # SYNC-152: no yaml list to fall back to
    assert _cloud_path_owns("pa", cfg) is False
    assert _memory_backend_enabled("pa", cfg) is False


def test_an_unreadable_shared_list_falls_back_to_the_yaml(tmp_path):
    cfg = _config(str(tmp_path / "nowhere"))
    assert enrolled_scrape_states(cfg) is None   # SYNC-152: "cannot say", never a guessed list
    assert _cloud_path_owns("mi", cfg) is True
    assert _cloud_path_owns("pa", cfg) is False


def test_the_memory_floor_with_an_unreadable_shared_list_is_the_yaml_exactly(tmp_path):
    cfg = _config(str(tmp_path / "nowhere"), memory=["pa"])
    assert _memory_backend_enabled("mi", cfg) is True      # on the yaml cloud list
    assert _memory_backend_enabled("pa", cfg) is True      # on the yaml memory floor
    assert _memory_backend_enabled("ne", cfg) is False     # on neither: the manifest cannot add it when unreadable
