"""Tests for OPEN-139: alert when a jurisdiction that was filing bills stops filing them.

Three units and their wiring:

* scrape_key_for       -- reproduces run-scrape.sh's SCRAPE_KEY, pinned against the real
                          filenames sitting in ddp-open-states/logs/last-run today, so a change
                          on either side of that cross-language coupling breaks a test rather
                          than silently zeroing a metric.
* read_filing_counts   -- reads the .imported marker, and refuses to turn an unmeasured run
                          into a measured zero.
* should_alert_quiet   -- the actual decision.

Plus the wiring into _check_sustained_blocking, which now records one richer history entry per
run and runs two independent checks over it (OPEN-22's block escalation and this).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    DEFAULT_QUIET_WINDOW,
    _check_sustained_blocking,
    read_filing_counts,
    scrape_key_for,
    should_alert_quiet,
)


# -- scrape_key_for: the cross-language coupling with run-scrape.sh --------------------------
#
# Expected values are the ACTUAL filenames observed in ddp-open-states/logs/last-run on
# 2026-08-23, not values derived from reading the shell. That is the point of the test.

@pytest.mark.parametrize(
    "label,expected",
    [
        ("az", "az"),
        ("wa", "wa"),
        ("fl session=2026", "fl_session_2026"),
        ("fl session=2026F", "fl_session_2026F"),
        ("fl session=2025A", "fl_session_2025A"),
        ("usa session=119 chamber=lower", "usa_session_119_chamber_lower"),
        ("usa session=119 chamber=upper", "usa_session_119_chamber_upper"),
    ],
)
def test_scrape_key_matches_real_marker_filenames(label, expected):
    assert scrape_key_for(label) == expected


# -- read_filing_counts ---------------------------------------------------------------------


def _write_marker(tmp_path, key, contents):
    d = tmp_path / "logs" / "last-run"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{key}.imported").write_text(contents)
    return str(tmp_path)


def test_reads_a_real_measurement(tmp_path):
    root = _write_marker(tmp_path, "va", "ok:37:83:168:incremental\n")
    assert read_filing_counts(root, "va") == {
        "bills_new": 37,
        "bills_updated": 83,
        "bills_noop": 168,
        "mode": "incremental",
    }


def test_reads_a_genuine_zero(tmp_path):
    """A measured zero is data, and must come back as 0 rather than None."""
    root = _write_marker(tmp_path, "ut", "ok:0:0:0:incremental\n")
    assert read_filing_counts(root, "ut")["bills_new"] == 0


def test_arizonas_stuck_run_reads_as_zero_new(tmp_path):
    """AZ's real shape: 895 bills collected, none of them new."""
    root = _write_marker(tmp_path, "az", "ok:0:0:895:incremental\n")
    counts = read_filing_counts(root, "az")
    assert counts["bills_new"] == 0
    assert counts["bills_noop"] == 895


def test_unparsed_status_is_not_a_zero(tmp_path):
    """The whole reason the marker carries a status field. `unparsed` must not become 0."""
    root = _write_marker(tmp_path, "ma", "unparsed::::incremental\n")
    assert read_filing_counts(root, "ma") is None


def test_missing_file_is_not_a_zero(tmp_path):
    root = _write_marker(tmp_path, "va", "ok:1:1:1:incremental\n")
    assert read_filing_counts(root, "never-ran") is None


def test_malformed_lines_are_not_zeroes(tmp_path):
    for junk in ("ok:1:2:3\n", "ok:a:b:c:incremental\n", "\n", "garbage\n"):
        root = _write_marker(tmp_path, "x", junk)
        assert read_filing_counts(root, "x") is None, junk


# -- should_alert_quiet ---------------------------------------------------------------------


def _h(*new_counts):
    """History from a list of bills_new values. None means the run was not measured."""
    return [
        {"success": True} if n is None else {"success": True, "bills_new": n}
        for n in new_counts
    ]


def test_alerts_when_a_filing_jurisdiction_goes_quiet():
    assert should_alert_quiet(_h(40, 55, 30, 0, 0, 0), 3) is True


def test_silent_while_still_filing():
    assert should_alert_quiet(_h(40, 55, 30, 0, 12, 0), 3) is False


def test_silent_when_the_most_recent_run_found_bills():
    assert should_alert_quiet(_h(40, 0, 0, 0, 7), 3) is False


def test_silent_before_the_window_is_full():
    assert should_alert_quiet(_h(40, 0, 0), 3) is False


def test_never_alerts_on_a_jurisdiction_that_has_never_filed():
    """Out-of-session all along, or never successfully collected. Either way this is not a
    regression, and alerting weekly forever would train people to ignore the alert."""
    assert should_alert_quiet(_h(0, 0, 0, 0, 0, 0), 3) is False


def test_empty_history_is_silent():
    assert should_alert_quiet([], 3) is False


def test_unmeasured_runs_are_skipped_not_counted_as_zero():
    """Three unmeasured runs must not add up to three quiet runs. Here the only measured runs
    are 40 then 0 -- one zero, not three, so nothing fires."""
    assert should_alert_quiet(_h(40, None, None, None, 0), 3) is False


def test_unmeasured_runs_do_not_break_a_real_quiet_streak():
    """Measured: 40, 0, 0, 0 -> three consecutive measured zeroes after real filing."""
    assert should_alert_quiet(_h(40, 0, None, 0, None, 0), 3) is True


def test_default_window_is_three():
    assert DEFAULT_QUIET_WINDOW == 3


# -- wiring into _check_sustained_blocking --------------------------------------------------


@pytest.mark.asyncio
async def test_filing_counts_are_folded_into_the_history_record(tmp_path):
    root = _write_marker(tmp_path, "va", "ok:12:5:60:incremental\n")
    store = AsyncMock()
    store.get_run_history.return_value = []
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store):
        await _check_sustained_blocking(
            "openstates_secondary_scrapes",
            ["va"],
            [{"success": True, "jurisdiction": "va"}],
            {"openstates_root": root, "filing_activity": {"enabled": True}},
        )
    record = store.append_run_history.call_args.args[2]
    assert record["bills_new"] == 12
    assert record["bills_updated"] == 5
    assert record["scrape_mode"] == "incremental"


@pytest.mark.asyncio
async def test_a_failed_run_records_no_filing_figures(tmp_path):
    """A failed run's marker is either absent or left over from an earlier run. Either way it is
    not a measurement of this run, and must not enter the history as one."""
    root = _write_marker(tmp_path, "mi", "ok:99:0:0:incremental\n")
    store = AsyncMock()
    store.get_run_history.return_value = []
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store):
        await _check_sustained_blocking(
            "openstates_secondary_scrapes",
            ["mi"],
            [{"success": False, "jurisdiction": "mi", "failure_reason": "waf_block"}],
            {"openstates_root": root, "filing_activity": {"enabled": True}},
        )
    record = store.append_run_history.call_args.args[2]
    assert "bills_new" not in record


@pytest.mark.asyncio
async def test_alerts_when_enabled_and_the_history_says_quiet(tmp_path):
    root = _write_marker(tmp_path, "az", "ok:0:0:895:incremental\n")
    store = AsyncMock()
    store.get_run_history.return_value = _h(40, 30, 0, 0, 0)
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store), \
         patch("ddp_sync.pipelines.openstates_scrape._alert_quiet_jurisdiction") as alert:
        await _check_sustained_blocking(
            "openstates_secondary_scrapes",
            ["az"],
            [{"success": True, "jurisdiction": "az"}],
            {"openstates_root": root, "filing_activity": {"enabled": True, "quiet_window": 3}},
        )
    alert.assert_called_once_with("az", 3)


@pytest.mark.asyncio
async def test_disabled_by_default_does_not_alert(tmp_path):
    """The config ships disabled for the first cycle; absent config must behave the same."""
    root = _write_marker(tmp_path, "az", "ok:0:0:895:incremental\n")
    store = AsyncMock()
    store.get_run_history.return_value = _h(40, 30, 0, 0, 0)
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store), \
         patch("ddp_sync.pipelines.openstates_scrape._alert_quiet_jurisdiction") as alert:
        await _check_sustained_blocking(
            "openstates_secondary_scrapes",
            ["az"],
            [{"success": True, "jurisdiction": "az"}],
            {"openstates_root": root},
        )
    alert.assert_not_called()
