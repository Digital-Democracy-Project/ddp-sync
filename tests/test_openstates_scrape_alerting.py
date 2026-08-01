"""Tests for openstates_scrape.py's failure-alerting behavior (2026-08-01).

ddp-sync's own per-jurisdiction scrape timeout used to be completely silent: a
subprocess.TimeoutExpired only logged an ERROR and wrote to an un-surfaced Redis
key. These tests pin down the three cases that matter for that fix:

- Timeout: alerts (run-scrape.sh's own process was killed before it could alert
  itself).
- Ordinary nonzero exit (run-scrape.sh's own deliberate `exit 1`): does NOT
  alert again here -- run-scrape.sh already fired its own Slack/CAMS alert from
  inside the process via its `trap ... ERR`.
- Signal-killed (negative returncode, e.g. OOM killer or an operator's `kill`):
  DOES alert -- a signal delivered to run-scrape.sh's own process can't be
  caught by its `trap ... ERR`, so nothing alerted on that one either.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ddp_sync.pipelines.openstates_scrape import _run_scrape


@pytest.mark.asyncio
async def test_timeout_alerts_once():
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(-9, b"", b"", True),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await _run_scrape("ma", None, "/fake/root", timeout_s=10)

    assert result["success"] is False
    assert result["error"] == "timeout"
    mock_alert.assert_called_once()
    assert "timed out" in mock_alert.call_args.args[1]


@pytest.mark.asyncio
async def test_ordinary_nonzero_exit_does_not_alert():
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(1, b"", b"some stderr", False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await _run_scrape("ma", None, "/fake/root", timeout_s=10)

    assert result["success"] is False
    assert result["error"] == "exit_code_1"
    mock_alert.assert_not_called()


@pytest.mark.asyncio
async def test_signal_killed_negative_returncode_alerts():
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(-15, b"", b"", False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await _run_scrape("ma", None, "/fake/root", timeout_s=10)

    assert result["success"] is False
    assert result["error"] == "exit_code_-15"
    mock_alert.assert_called_once()
    assert "signal 15" in mock_alert.call_args.args[1]


@pytest.mark.asyncio
async def test_success_does_not_alert():
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(0, b"", b"", False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await _run_scrape("ma", None, "/fake/root", timeout_s=10)

    assert result["success"] is True
    mock_alert.assert_not_called()


@pytest.mark.asyncio
async def test_subprocess_start_failure_alerts():
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            side_effect=FileNotFoundError("run-scrape.sh not found"),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await _run_scrape("ma", None, "/fake/root", timeout_s=10)

    assert result["success"] is False
    mock_alert.assert_called_once()


def test_alert_scrape_failure_never_raises_when_slack_returns_non_ok():
    with (
        patch.dict("os.environ", {"SLACK_BOT_TOKEN": "fake-token"}, clear=False),
        patch("ddp_sync.pipelines.openstates_scrape.requests.post") as mock_post,
    ):
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {"ok": False, "error": "channel_not_found"}

        from ddp_sync.pipelines.openstates_scrape import _alert_scrape_failure

        _alert_scrape_failure("ma", "timed out after 21600s", 21605.3)  # must not raise


def test_alert_scrape_failure_never_raises_when_slack_request_errors():
    with (
        patch.dict("os.environ", {"SLACK_BOT_TOKEN": "fake-token"}, clear=False),
        patch(
            "ddp_sync.pipelines.openstates_scrape.requests.post",
            side_effect=ConnectionError("network down"),
        ),
    ):
        from ddp_sync.pipelines.openstates_scrape import _alert_scrape_failure

        _alert_scrape_failure("ma", "timed out after 21600s", 21605.3)  # must not raise


def test_alert_scrape_failure_never_raises_when_cams_returns_error():
    with patch.dict(
        "os.environ",
        {"SLACK_BOT_TOKEN": "", "CAMS_API_TOKEN": "fake-cams-token"},
        clear=False,
    ), patch("ddp_sync.pipelines.openstates_scrape.requests.post") as mock_post:
        mock_post.return_value.ok = False
        mock_post.return_value.status_code = 500

        from ddp_sync.pipelines.openstates_scrape import _alert_scrape_failure

        _alert_scrape_failure("ma", "timed out after 21600s", 21605.3)  # must not raise
