"""Tests for OPEN-128's timeout half: MA's full walk must fit comfortably inside its ceiling.

Removing MA's sponsor-date filter (openstates-scrapers, ma/bills.py) makes every MA run a full
walk of ~11,500 bills. A real one measured 341 minutes against the 6h `default` MA previously
fell under -- a 19-minute margin on a walk paced by malegislature.gov's own variable ~2s
per-page response time.

The consequence of crossing it is worse than a normal timeout. `_run_scrape` uses
subprocess.run(timeout=...), which kills run-scrape.sh from outside its own process, so the
script's cleanup, marker writes and on_failure() alerting never run -- and openstates-core has
already wiped the jurisdiction's data directory at scrape start. A timeout kill discards the
whole run's work silently.
"""
from unittest import mock

import pytest

import ddp_sync.pipelines.openstates_scrape as os_scrape
from ddp_sync.pipelines.openstates_scrape import SCRAPE_TIMEOUT_S

# The longest real MA full walk observed in scraper.log, in seconds (341 minutes).
MEASURED_MA_FULL_WALK_S = 341 * 60


def _ceiling(jurisdiction: str) -> int:
    return SCRAPE_TIMEOUT_S.get(jurisdiction, SCRAPE_TIMEOUT_S["default"])


def test_ma_has_its_own_ceiling_rather_than_the_default():
    # The bug this guards is quiet: MA absent from the dict silently inherits `default`, and
    # nothing in the scrape path would say so.
    assert "ma" in SCRAPE_TIMEOUT_S


def test_ma_ceiling_clears_the_measured_full_walk_with_real_headroom():
    """Not merely "above the measured walk" -- comfortably above it.

    A ceiling only just above a measured duration is what MA already had, and the walk got
    longer. 1.5x is the minimum that means anything for a run whose pace depends on a third
    party's response time.
    """
    assert _ceiling("ma") >= MEASURED_MA_FULL_WALK_S * 1.5, (
        f"MA ceiling {_ceiling('ma')}s gives only "
        f"{_ceiling('ma') / MEASURED_MA_FULL_WALK_S:.2f}x the measured "
        f"{MEASURED_MA_FULL_WALK_S}s full walk"
    )


def test_the_old_default_would_not_have_been_enough():
    # Pins the reason this change exists. If someone later deletes MA's entry, this documents
    # what they would be reverting to and why it is not adequate.
    assert SCRAPE_TIMEOUT_S["default"] < MEASURED_MA_FULL_WALK_S * 1.1


def test_ma_does_not_exceed_the_longest_jurisdiction():
    # FL is the genuine long pole (12+ hours regularly). MA should not quietly become the
    # loosest ceiling in the table -- if it ever needs to, that is a deliberate decision and
    # chunking is the better answer.
    assert _ceiling("ma") <= _ceiling("fl")


def test_other_jurisdictions_are_untouched():
    # This change must not widen anyone else's ceiling as a side effect. USA's 4h in particular
    # is load-bearing for OPEN-87's retry gate, which calls it the tightest in the table.
    assert _ceiling("fl") == 16 * 3600
    assert _ceiling("wa") == 8 * 3600
    assert _ceiling("usa") == 4 * 3600
    assert SCRAPE_TIMEOUT_S["default"] == 6 * 3600


def test_jurisdictions_without_an_entry_still_get_the_default():
    # The lookup contract itself, since MA's whole problem was inheriting it unnoticed.
    assert _ceiling("az") == SCRAPE_TIMEOUT_S["default"]
    assert _ceiling("va") == SCRAPE_TIMEOUT_S["default"]


# --- the production path, not a copy of the lookup -----------------------------------
#
# pm-review's point: every test above reimplements `SCRAPE_TIMEOUT_S.get(j, default)` in this
# file, so they would all still pass if the production resolution diverged from it. These two
# assert on the value actually handed to the process runner.

@pytest.mark.asyncio
async def test_ma_resolves_to_twelve_hours_at_the_invocation_path():
    seen = {}

    def fake_run(cmd, env, timeout, cwd=None):
        seen["timeout"] = timeout
        return 0, b"", b"", False

    with mock.patch.object(os_scrape, "_run_with_group_kill", fake_run):
        await os_scrape._run_scrape("ma", None, "/tmp/openstates", None, {})

    assert seen["timeout"] == 12 * 3600
    assert seen["timeout"] >= MEASURED_MA_FULL_WALK_S * 1.5


@pytest.mark.asyncio
async def test_an_entry_less_jurisdiction_resolves_to_the_default_at_the_invocation_path():
    seen = {}

    def fake_run(cmd, env, timeout, cwd=None):
        seen["timeout"] = timeout
        return 0, b"", b"", False

    with mock.patch.object(os_scrape, "_run_with_group_kill", fake_run):
        await os_scrape._run_scrape("az", None, "/tmp/openstates", None, {})

    assert seen["timeout"] == SCRAPE_TIMEOUT_S["default"]
