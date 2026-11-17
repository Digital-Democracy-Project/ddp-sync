"""SYNC-172: the weekly states run on their own days instead of all starting Sunday 02:00 UTC.

Config only: every weekly state got its own `openstates_scrape.timing` entry. These tests read the REAL
`config/sync_schedule.yaml` and check what the scheduler makes of it: a weekly state on a non-Sunday day, the
nightly states unchanged, every `day` a real weekday name (the scheduler falls back to Sunday on anything else,
silently), and dynamic_cadence still lifting a non-Sunday weekly state to nightly and back. WHO is scraped stays the
manifest's: a temp manifest stands in for ddp-open-states here, and the SYNC-152 guard test checks the real one.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import yaml

from ddp_sync.pipelines import openstates_scrape as scrape
from ddp_sync.scheduler import _DAY_ABBR, DEFAULT_CONFIG_PATH, UpdateScheduler
from ddp_sync.services import jurisdiction_enrollment as je

REAL = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())["openstates_scrape"]
TIMING = REAL["timing"]


def _entry():
    return {"status": "live", "archive": {"enabled": True, "scheduled": True}, "enrollment": {"cloud_path": True}}


@pytest.fixture(autouse=True)
def manifest(tmp_path, monkeypatch):
    """Every state the real timing map names is scraped (usa is spelled `us` in the manifest)."""
    je._cache.clear()
    je._logged.clear()
    monkeypatch.delenv(je.MODE_ENV, raising=False)
    codes = {"us" if j == "usa" else j: _entry() for j in TIMING}
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(codes))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))


def _registered(cadences=None) -> UpdateScheduler:
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    sched = UpdateScheduler(settings=settings, config_path=DEFAULT_CONFIG_PATH)
    sched._register_openstates_scrape_jobs(cadences)
    return sched


def _trigger(sched, j) -> tuple[str, str, str]:
    f = {x.name: str(x) for x in sched.scheduler.get_job(f"openstates_scrape_{j}").trigger.fields}
    return f["day_of_week"], f["hour"], f["minute"]


def test_state_timing_gives_a_weekly_state_its_own_non_sunday_day():
    assert scrape.state_timing(REAL, "ma") == {"floor": "weekly", "day": "monday", "hour": 12, "minute": 0, "sessions": []}
    assert scrape.state_timing(REAL, "la")["day"] == "thursday"


def test_the_nightly_states_are_unchanged():
    assert scrape.state_timing(REAL, "wa") == {"floor": "nightly", "day": "sunday", "hour": 2, "minute": 30, "sessions": []}
    assert scrape.state_timing(REAL, "usa") == {
        "floor": "nightly", "day": "sunday", "hour": 3, "minute": 0,
        "sessions": ["119 chamber=lower", "119 chamber=upper"],
    }
    assert scrape.state_timing(REAL, "pa") == {"floor": "nightly", "day": "sunday", "hour": 4, "minute": 0, "sessions": []}


def test_every_day_in_the_real_file_is_a_weekday_the_scheduler_knows():
    """`_DAY_ABBR.get(day, "sun")`: a misspelt day would quietly put a state back on Sunday."""
    days = {j: t.get("day") for j, t in TIMING.items() if t.get("day") is not None}
    days["(defaults)"] = REAL["defaults"]["day"]
    assert {j: d for j, d in days.items() if d not in _DAY_ABBR} == {}
    weekly_without_day = [j for j, t in TIMING.items() if t.get("floor", "weekly") == "weekly" and "day" not in t]
    assert weekly_without_day == []


def test_massachusetts_runs_alone_on_its_day():
    """MA walks every bill (10-16 h); no other weekly state shares its day."""
    weekly = {j: scrape.state_timing(REAL, j) for j in TIMING if scrape.state_timing(REAL, j)["floor"] == "weekly"}
    assert [j for j, t in weekly.items() if t["day"] == weekly["ma"]["day"]] == ["ma"]


# The agreed layout (SYNC-172), as the scheduler must register it: (day_of_week, hour, minute). "*" = nightly.
EXPECTED = {
    "ma": ("mon", "12", "0"),
    "va": ("tue", "12", "0"), "ut": ("tue", "12", "0"),
    "az": ("wed", "12", "0"), "nc": ("wed", "12", "0"),
    "la": ("thu", "12", "0"), "ne": ("thu", "12", "0"),
    "al": ("fri", "12", "0"), "ga": ("fri", "12", "0"),
    "nv": ("sat", "12", "0"), "or": ("sat", "12", "0"),
    "fl": ("sun", "2", "0"), "mi": ("sun", "2", "0"),
    "wa": ("*", "2", "30"), "usa": ("*", "3", "0"), "pa": ("*", "4", "0"),
}


def test_the_scheduler_registers_exactly_the_agreed_layout():
    """pm-review: every state, not a sample, so a wrong-but-valid day for one state cannot slip through."""
    assert set(TIMING) == set(EXPECTED)
    sched = _registered()
    assert {j: _trigger(sched, j) for j in EXPECTED} == EXPECTED
    assert {j for j in EXPECTED if EXPECTED[j][0] == "*"} == {j for j in TIMING if TIMING[j]["floor"] == "nightly"}


def test_dynamic_cadence_lifts_a_non_sunday_state_to_nightly_and_back_to_its_own_day():
    sched = _registered()
    sched._register_openstates_scrape_jobs({"la": "nightly"})
    assert _trigger(sched, "la") == ("*", "12", "0")
    assert sched._openstates_cadence["la"] == "nightly"

    sched._register_openstates_scrape_jobs({})
    assert _trigger(sched, "la") == ("thu", "12", "0")
    assert sched._openstates_cadence["la"] == "weekly"
