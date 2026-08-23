"""Tests for the patch-refresh and people-refresh jobs' failure alerting (OPEN-127).

Both jobs used to swallow their own timeouts entirely: `except subprocess.TimeoutExpired`
logged one ERROR line and returned, with no Slack alert, no CAMS record, and not even a
`_write_flow_status` write (unlike the nonzero-exit branch right above it). They also used
`subprocess.run(timeout=...)`, which kills only the direct child -- so a timeout orphaned the
git operations and `os-people to-database` runs that were doing the actual work.

Note the deliberate asymmetry with `_run_scrape` (see test_openstates_scrape_alerting.py):
there, an ordinary nonzero exit does NOT alert, because run-scrape.sh fires its own Slack/CAMS
alert from inside the process via `trap ... ERR`. `apply-local-patches.sh` and
`run-people-refresh.sh` have no ERR trap and no alerting of their own at all -- verified, zero
matches for trap/Slack/CAMS in either script -- so here the nonzero-exit branch alerts too.
Without it, a failed patch refresh is exactly as silent as the timeout was.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    run_patch_refresh_job,
    run_people_refresh_job,
)

CONFIG = {"openstates_root": "/fake/root"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job,label",
    [(run_patch_refresh_job, "patch refresh"), (run_people_refresh_job, "people refresh")],
)
async def test_timeout_alerts(job, label):
    """A timeout is the case that was 100% silent before OPEN-127."""
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(-9, b"", b"", True),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()) as st,
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await job(CONFIG)

    assert result["success"] is False
    assert result["error"] == "timeout"
    mock_alert.assert_called_once()
    assert mock_alert.call_args.args[0] == label
    assert "timed out" in mock_alert.call_args.args[1]
    # The timeout branch also never wrote a flow status before; it should now.
    st.assert_awaited_once()
    assert st.await_args.args[1]["status"] == "failed"
    assert st.await_args.args[1]["error"] == "timeout"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job,label",
    [(run_patch_refresh_job, "patch refresh"), (run_people_refresh_job, "people refresh")],
)
async def test_nonzero_exit_alerts_because_these_scripts_do_not_self_alert(job, label):
    """Deliberately unlike _run_scrape -- see this module's docstring."""
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(1, b"", b"boom", False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await job(CONFIG)

    assert result["success"] is False
    assert result["error"] == "exit_code_1"
    mock_alert.assert_called_once()
    assert mock_alert.call_args.args[0] == label


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job", [run_patch_refresh_job, run_people_refresh_job]
)
async def test_success_does_not_alert(job):
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(0, b"", b"", False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await job(CONFIG)

    assert result["success"] is True
    mock_alert.assert_not_called()


@pytest.mark.asyncio
async def test_both_jobs_go_through_the_group_kill_helper():
    """Guards the actual orphaning fix, not just the alerting.

    people_refresh previously passed start_new_session=True to subprocess.run, which
    _run_with_group_kill's own docstring explains is insufficient on its own. If either job
    regresses to a bare subprocess.run, this fails.
    """
    for job in (run_patch_refresh_job, run_people_refresh_job):
        with (
            patch(
                "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
                return_value=(0, b"", b"", False),
            ) as mock_helper,
            patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()),
            patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
        ):
            await job(CONFIG)
        mock_helper.assert_called_once()


def test_group_kill_cwd_param_is_backward_compatible():
    """The new cwd argument must not change behaviour for the existing three-arg callers."""
    import inspect

    from ddp_sync.pipelines.openstates_scrape import _run_with_group_kill

    params = inspect.signature(_run_with_group_kill).parameters
    assert list(params) == ["cmd", "env", "timeout", "cwd"]
    assert params["cwd"].default is None
