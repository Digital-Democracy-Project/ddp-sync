"""Tests for SYNC-2: archive-run process-group kill + failure alerting parity.

_run_archive() used to invoke run-archive.sh with a bare subprocess.run(timeout=...).
That only kills the direct child (the wrapper script) on TimeoutExpired --
start_new_session=True makes the wrapper the leader of its own process group, but
nothing then targets that group, so its grandchildren (os-text-extract archive <state>
and its tee) survived, orphaned, in the detached session. Real incident 2026-08-07: MI's
4h archive timeout killed only the wrapper, and the orphaned archiver kept running
headless for ~24h more with nobody told, because run-archive.sh's own ERR trap -- which
owns the Slack #automation-errors alert and CAMS failure report -- never got a chance to
run either.

Two fixes, mirroring the scrape pipeline's own hardening (openstates_scrape.py's
_run_with_group_kill / _alert_scrape_failure):

  1. _run_archive now goes through _run_with_group_kill (reused directly, not
     reimplemented), which killpg()s the whole process group on timeout.
  2. _alert_archive_failure posts to Slack + CAMS for the three cases run-archive.sh's
     own in-process ERR trap cannot see: a timeout kill, a signal delivered straight to
     the wrapper's own process, and an exception raised before/while invoking the
     subprocess at all. An ordinary positive-exit-code failure is NOT re-alerted here --
     run-archive.sh's own trap already fired for that one, from inside the process.

The bulk of these tests mock _run_with_group_kill for determinism (the group-kill
mechanism itself -- the watchdog thread, the killpg, the stall detection -- is already
exercised end-to-end against real subprocesses in test_openstates_scrape_stall.py, and
reusing the same function means nothing here needs to re-prove that). One test
(test_a_timed_out_archive_run_kills_the_whole_process_group_and_alerts) is the evidence-
bar exception: it drives a real subprocess through a real timeout and checks `ps` and a
grandchild-survival marker directly, plus that an alert path fires.
"""

from __future__ import annotations

import subprocess
import time
import uuid
from unittest.mock import patch

import pytest

from ddp_sync.pipelines.openstates_archive import (
    ARCHIVE_TIMEOUT_S,
    _alert_archive_failure,
    _run_archive,
)


@pytest.mark.asyncio
async def test_timeout_alerts_once():
    with (
        patch(
            "ddp_sync.pipelines.openstates_archive._run_with_group_kill",
            return_value=(-9, b"", b"", True, False),
        ),
        patch("ddp_sync.pipelines.openstates_archive._alert_archive_failure") as mock_alert,
    ):
        result = await _run_archive("mi", "/fake/root", timeout_s=10)

    assert result["success"] is False
    assert result["error"] == "timeout"
    mock_alert.assert_called_once()
    assert mock_alert.call_args.args[0] == "mi"
    assert "timed out" in mock_alert.call_args.args[1]


@pytest.mark.asyncio
async def test_ordinary_nonzero_exit_does_not_alert():
    """run-archive.sh's own ERR trap already fired its Slack/CAMS alert from inside the
    process for a plain `exit 1` -- alerting again here would double-page."""
    with (
        patch(
            "ddp_sync.pipelines.openstates_archive._run_with_group_kill",
            return_value=(1, b"", b"some stderr", False, False),
        ),
        patch("ddp_sync.pipelines.openstates_archive._alert_archive_failure") as mock_alert,
    ):
        result = await _run_archive("mi", "/fake/root", timeout_s=10)

    assert result["success"] is False
    assert result["error"] == "exit_code_1"
    mock_alert.assert_not_called()


@pytest.mark.asyncio
async def test_signal_killed_negative_returncode_alerts():
    """A signal delivered straight to run-archive.sh's own process (OOM killer, an
    operator's `kill`, another supervisor) can't be caught by its own ERR trap."""
    with (
        patch(
            "ddp_sync.pipelines.openstates_archive._run_with_group_kill",
            return_value=(-15, b"", b"", False, False),
        ),
        patch("ddp_sync.pipelines.openstates_archive._alert_archive_failure") as mock_alert,
    ):
        result = await _run_archive("mi", "/fake/root", timeout_s=10)

    assert result["success"] is False
    assert result["error"] == "exit_code_-15"
    mock_alert.assert_called_once()
    assert "signal 15" in mock_alert.call_args.args[1]


@pytest.mark.asyncio
async def test_success_does_not_alert():
    with (
        patch(
            "ddp_sync.pipelines.openstates_archive._run_with_group_kill",
            return_value=(0, b"", b"", False, False),
        ),
        patch("ddp_sync.pipelines.openstates_archive._alert_archive_failure") as mock_alert,
    ):
        result = await _run_archive("mi", "/fake/root", timeout_s=10)

    assert result["success"] is True
    mock_alert.assert_not_called()


@pytest.mark.asyncio
async def test_subprocess_start_failure_alerts():
    """Something failed before/while invoking the subprocess itself (e.g. a missing
    script or openstates_root) -- run-archive.sh never started, so it never had a
    chance to alert either."""
    with (
        patch(
            "ddp_sync.pipelines.openstates_archive._run_with_group_kill",
            side_effect=FileNotFoundError("run-archive.sh not found"),
        ),
        patch("ddp_sync.pipelines.openstates_archive._alert_archive_failure") as mock_alert,
    ):
        result = await _run_archive("mi", "/fake/root", timeout_s=10)

    assert result["success"] is False
    mock_alert.assert_called_once()


def test_alert_archive_failure_never_raises_when_slack_returns_non_ok():
    with (
        patch.dict("os.environ", {"SLACK_BOT_TOKEN": "fake-token"}, clear=False),
        patch("ddp_sync.pipelines.openstates_archive.requests.post") as mock_post,
    ):
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {"ok": False, "error": "channel_not_found"}

        _alert_archive_failure("mi", "timed out after 14400s", 14405.3)  # must not raise


def test_alert_archive_failure_never_raises_when_slack_request_errors():
    with (
        patch.dict("os.environ", {"SLACK_BOT_TOKEN": "fake-token"}, clear=False),
        patch(
            "ddp_sync.pipelines.openstates_archive.requests.post",
            side_effect=ConnectionError("network down"),
        ),
    ):
        _alert_archive_failure("mi", "timed out after 14400s", 14405.3)  # must not raise


def test_alert_archive_failure_never_raises_when_cams_returns_error():
    with patch.dict(
        "os.environ",
        {"SLACK_BOT_TOKEN": "", "CAMS_API_TOKEN": "fake-cams-token"},
        clear=False,
    ), patch("ddp_sync.pipelines.openstates_archive.requests.post") as mock_post:
        mock_post.return_value.ok = False
        mock_post.return_value.status_code = 500

        _alert_archive_failure("mi", "timed out after 14400s", 14405.3)  # must not raise


def test_alert_archive_failure_includes_jurisdiction_in_the_message():
    """The ticket's own acceptance line: the alert must name the jurisdiction.

    CAMS_API_TOKEN is forced empty here, not left to whatever the ambient environment
    happens to have -- this repo's own .env sets a real one, and when it's present
    _alert_archive_failure() makes a SECOND requests.post call (to CAMS, using data=
    rather than json=). mock_post.call_args reflects the LAST call, so without this the
    test passes or fails depending on suite run order / whether .env got loaded first,
    rather than on anything this test is actually about (the Slack message content).
    """
    with (
        patch.dict(
            "os.environ",
            {"SLACK_BOT_TOKEN": "fake-token", "CAMS_API_TOKEN": ""},
            clear=False,
        ),
        patch("ddp_sync.pipelines.openstates_archive.requests.post") as mock_post,
    ):
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {"ok": True}

        _alert_archive_failure("mi", "timed out after 14400s", 14405.3)

        sent = mock_post.call_args.kwargs["json"]["text"]
        assert "mi" in sent


# --- mi's own timeout ceiling (ticket part 3) --------------------------------------


def test_mi_has_its_own_ceiling_rather_than_the_default():
    # MI silently inheriting `default` (4h) is the bug: ~8k docs at ~10s/doc via
    # os-text-extract is 20h+, so a healthy run gets killed partway through every day.
    assert "mi" in ARCHIVE_TIMEOUT_S
    assert ARCHIVE_TIMEOUT_S["mi"] > ARCHIVE_TIMEOUT_S["default"]


def test_mi_ceiling_clears_the_measured_backlog():
    measured_backlog_seconds = 8000 * 10  # ~8k docs at ~10s/doc
    assert ARCHIVE_TIMEOUT_S["mi"] >= measured_backlog_seconds


def test_other_archive_ceilings_are_untouched():
    assert ARCHIVE_TIMEOUT_S["fl"] == 16 * 3600
    assert ARCHIVE_TIMEOUT_S["wa"] == 8 * 3600
    assert ARCHIVE_TIMEOUT_S["us"] == 24 * 3600
    assert ARCHIVE_TIMEOUT_S["default"] == 4 * 3600


@pytest.mark.asyncio
async def test_mi_resolves_to_its_own_ceiling_at_the_invocation_path():
    seen = {}

    def fake_run(cmd, env, timeout, *args, **kwargs):
        seen["timeout"] = timeout
        return 0, b"", b"", False, False

    with patch(
        "ddp_sync.pipelines.openstates_archive._run_with_group_kill",
        side_effect=fake_run,
    ):
        await _run_archive("mi", "/fake/root")

    assert seen["timeout"] == ARCHIVE_TIMEOUT_S["mi"]


# --- evidence bar: a real subprocess, a real timeout, a real kill -------------------


@pytest.mark.asyncio
async def test_a_timed_out_archive_run_kills_the_whole_process_group_and_alerts(tmp_path):
    """Drives _run_archive() against a real run-archive.sh stand-in that backgrounds a
    grandchild -- exactly the shape of run-archive.sh spawning
    `os-text-extract archive <state>` and its `tee` -- and forces a real timeout.

    Confirms both halves of the ticket: (a) the process group actually dies, checked
    two ways (a live `ps` scan for a unique sentinel in the grandchild's own command
    line, and a marker file the grandchild would write if its `sleep` ever completed),
    and (b) an alert path is exercised.
    """
    sentinel = f"sync2-evidence-{uuid.uuid4().hex}"
    marker = tmp_path / "grandchild-survived"
    script = tmp_path / "run-archive.sh"
    script.write_text(
        "#!/bin/bash\n"
        f'bash -c \': {sentinel}; sleep 3; touch "{marker}"\' &\n'
        "wait\n"
    )
    script.chmod(0o755)

    with patch(
        "ddp_sync.pipelines.openstates_archive._alert_archive_failure"
    ) as mock_alert:
        result = await _run_archive("mi", str(tmp_path), timeout_s=1)

    # (a) the process group actually died.
    ps_output = subprocess.run(
        ["ps", "aux"], capture_output=True, text=True, check=False
    ).stdout
    surviving = [
        line for line in ps_output.splitlines() if sentinel in line and "grep" not in line
    ]
    assert surviving == [], f"orphaned process(es) still in ps: {surviving}"

    time.sleep(3.5)  # let the grandchild's sleep(3) elapse if it somehow survived
    assert not marker.exists(), "grandchild archiver process survived the timeout kill"

    # (b) an alert path was exercised.
    assert result["success"] is False
    assert result["error"] == "timeout"
    mock_alert.assert_called_once()
    assert mock_alert.call_args.args[0] == "mi"
    assert "timed out" in mock_alert.call_args.args[1]
