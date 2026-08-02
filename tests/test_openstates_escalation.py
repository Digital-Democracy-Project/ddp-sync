"""Tests for OPEN-22: detect and escalate a sustained (multi-week) MI blocking pattern.

Covers AC6's three required cases (a single failure does not escalate, a sustained pattern
does, a recovered run resets the streak cleanly) plus the failure-reason classification
(AC0b) and the rolling-history wiring (AC0) that feeds should_escalate().
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    _check_sustained_blocking,
    _run_scrape,
    classify_failure_reason,
    should_escalate,
)

WAF_STDERR_TAIL = (
    "openstates.exceptions.ScrapeError: MI bill scrape aborted: 3 consecutive WAF blocks "
    "detected fetching bill pages -- legislature.mi.gov is likely blocking this run entirely"
)


# -- classify_failure_reason (AC0b) --


def test_classify_timeout():
    assert classify_failure_reason("timeout", "") == "timeout"


def test_classify_waf_block_from_stderr():
    assert classify_failure_reason("exit_code_1", WAF_STDERR_TAIL) == "waf_block"


def test_classify_waf_block_is_case_insensitive():
    assert classify_failure_reason("exit_code_1", WAF_STDERR_TAIL.upper()) == "waf_block"


def test_classify_nonzero_exit_other_without_waf_markers():
    assert classify_failure_reason("exit_code_1", "some unrelated traceback") == "nonzero_exit_other"


def test_classify_network_error_for_generic_exception_string():
    assert classify_failure_reason("FileNotFoundError: run-scrape.sh not found", "") == "network_error"


# -- should_escalate (AC1-AC3, AC5) — pure function, no Redis needed --


def _history(*failure_reasons: str | None) -> list[dict]:
    """Build a fake history list, oldest first, one record per arg (None = success)."""
    return [
        {"timestamp": f"t{i}", "success": r is None, "failure_reason": r}
        for i, r in enumerate(failure_reasons)
    ]


def test_single_failure_does_not_escalate():
    # AC3: 1 of the last 4 runs blocked must not trigger escalation.
    history = _history(None, None, None, "waf_block")
    assert should_escalate(history, window=4, threshold=3) is False


def test_sustained_pattern_escalates():
    # 3 of the last 4 runs WAF-blocked -- a sustained pattern, not a one-off.
    history = _history("waf_block", "waf_block", None, "waf_block")
    assert should_escalate(history, window=4, threshold=3) is True


def test_fully_blocked_window_escalates():
    history = _history("waf_block", "waf_block", "waf_block", "waf_block")
    assert should_escalate(history, window=4, threshold=3) is True


def test_recovered_run_resets_streak_cleanly():
    # AC5: a bad 3-block streak, then MI recovers. AC1 checks "most/all of the recent window"
    # -- a ratio, not a strict "did the very last run block" flag -- so one recovery run
    # doesn't have to *instantly* clear the alert while the window still holds 3-of-4 blocked
    # (that's still a real, current pattern worth flagging). It must clear once the window has
    # genuinely moved past the bad streak, with no separate streak-counter state to remember
    # to reset -- the ratio falls out of the same rolling history automatically.
    still_within_window = _history("waf_block", "waf_block", "waf_block", None)
    assert should_escalate(still_within_window, window=4, threshold=3) is True

    window_has_moved_on = _history("waf_block", "waf_block", "waf_block", None, None)
    assert should_escalate(window_has_moved_on, window=4, threshold=3) is False


def test_short_history_below_threshold_count_never_escalates():
    # Fewer total runs than threshold -- can't possibly reach threshold blocked entries yet.
    history = _history("waf_block", "waf_block")
    assert should_escalate(history, window=4, threshold=3) is False


def test_only_last_window_entries_considered():
    # 4 old blocks fall outside a window of 2; only the last two (both successes) count.
    history = _history("waf_block", "waf_block", "waf_block", "waf_block", None, None)
    assert should_escalate(history, window=2, threshold=1) is False


# -- _run_scrape: failure_reason is attached to the result (AC0b plumbing) --


@pytest.mark.asyncio
async def test_run_scrape_attaches_waf_block_failure_reason():
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(1, b"", WAF_STDERR_TAIL.encode(), False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = await _run_scrape("mi", None, "/fake/root", timeout_s=10)

    assert result["success"] is False
    assert result["failure_reason"] == "waf_block"


@pytest.mark.asyncio
async def test_run_scrape_attaches_nonzero_exit_other_for_unrelated_failure():
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(1, b"", b"some unrelated stderr", False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = await _run_scrape("mi", None, "/fake/root", timeout_s=10)

    assert result["success"] is False
    assert result["failure_reason"] == "nonzero_exit_other"


@pytest.mark.asyncio
async def test_run_scrape_success_has_no_failure_reason():
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        return_value=(0, b"", b"", False),
    ):
        result = await _run_scrape("mi", None, "/fake/root", timeout_s=10)

    assert result["success"] is True
    assert result.get("failure_reason") is None


# -- _check_sustained_blocking wiring (AC0, AC2) --


@pytest.mark.asyncio
async def test_check_sustained_blocking_alerts_when_pattern_detected():
    mock_redis = AsyncMock()
    mock_redis.get_run_history.return_value = _history(
        "waf_block", "waf_block", None, "waf_block"
    )

    with (
        patch(
            "ddp_sync.services.redis_store.get_redis_store", return_value=mock_redis
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_sustained_block") as mock_alert,
    ):
        await _check_sustained_blocking(
            "openstates_secondary_scrapes",
            ["mi"],
            [{"success": False, "failure_reason": "waf_block"}],
            {"secondary": {"escalation": {"window_size": 4, "threshold": 3}}},
        )

    mock_redis.append_run_history.assert_called_once()
    mock_alert.assert_called_once()
    assert mock_alert.call_args.args[0] == "mi"


@pytest.mark.asyncio
async def test_check_sustained_blocking_does_not_alert_on_single_failure():
    mock_redis = AsyncMock()
    mock_redis.get_run_history.return_value = _history(None, None, None, "waf_block")

    with (
        patch(
            "ddp_sync.services.redis_store.get_redis_store", return_value=mock_redis
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_sustained_block") as mock_alert,
    ):
        await _check_sustained_blocking(
            "openstates_secondary_scrapes",
            ["mi"],
            [{"success": False, "failure_reason": "waf_block"}],
            {"secondary": {"escalation": {"window_size": 4, "threshold": 3}}},
        )

    mock_alert.assert_not_called()


@pytest.mark.asyncio
async def test_check_sustained_blocking_uses_config_defaults_when_absent():
    mock_redis = AsyncMock()
    mock_redis.get_run_history.return_value = _history(
        "waf_block", "waf_block", "waf_block", "waf_block"
    )

    with (
        patch(
            "ddp_sync.services.redis_store.get_redis_store", return_value=mock_redis
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_sustained_block") as mock_alert,
    ):
        await _check_sustained_blocking(
            "openstates_secondary_scrapes",
            ["mi"],
            [{"success": False, "failure_reason": "waf_block"}],
            config=None,
        )

    mock_alert.assert_called_once()


@pytest.mark.asyncio
async def test_check_sustained_blocking_never_raises_on_redis_error():
    mock_redis = AsyncMock()
    mock_redis.append_run_history.side_effect = ConnectionError("redis down")

    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=mock_redis):
        await _check_sustained_blocking(  # must not raise
            "openstates_secondary_scrapes",
            ["mi"],
            [{"success": False, "failure_reason": "waf_block"}],
            {"secondary": {"escalation": {"window_size": 4, "threshold": 3}}},
        )
