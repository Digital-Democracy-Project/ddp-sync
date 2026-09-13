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
            return_value=(-9, b"", b"", True, False),
        ),
        patch(
            "ddp_sync.pipelines.openstates_scrape.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
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
            return_value=(1, b"", b"boom", False, False),
        ),
        patch(
            "ddp_sync.pipelines.openstates_scrape.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
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
            return_value=(0, b"", b"", False, False),
        ),
        patch(
            "ddp_sync.pipelines.openstates_scrape.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await job(CONFIG)

    assert result["success"] is True
    mock_alert.assert_not_called()


@pytest.mark.asyncio
async def test_people_refresh_refuses_without_running_when_rds_unresolvable():
    """OPEN-285: os-people needs a real DATABASE_URL (resolved live, matching _run_load's own
    OPEN-260 pattern) -- confirm this refuses up front, before ever invoking the script, rather
    than letting os-people fall through to a localhost default that fails per-state instead."""
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        ) as mock_helper,
        patch(
            "ddp_sync.pipelines.openstates_scrape.resolve_rds_database_url",
            return_value=(None, "RDS_CREDENTIALS_SECRET_ARN not set -- refusing to guess which secret to read"),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()) as st,
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = await run_people_refresh_job(CONFIG)

    assert result["success"] is False
    assert "cannot resolve an RDS target" in result["error"]
    mock_helper.assert_not_called()
    # This refusal path is deliberately silent on Slack/CAMS (matching _run_load's own
    # preflight refusal) -- it's a config problem to surface via flow-status, not treated as
    # the same class of failure as a real scrape/refresh going wrong mid-run.
    mock_alert.assert_not_called()
    st.assert_awaited_once()
    assert st.await_args.args[1]["status"] == "failed"


@pytest.mark.asyncio
async def test_people_refresh_passes_resolved_url_as_database_url_env():
    """The whole point of the fix -- os-people must actually see the live-resolved RDS URL,
    not the container's own inherited (RDS-less) environment."""
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(0, b"", b"", False, False),
        ) as mock_helper,
        patch(
            "ddp_sync.pipelines.openstates_scrape.resolve_rds_database_url",
            return_value=("postgresql://user:pass@rds-host/openstates", ""),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = await run_people_refresh_job(CONFIG)

    assert result["success"] is True
    passed_env = mock_helper.call_args.args[1]
    assert passed_env["DATABASE_URL"] == "postgresql://user:pass@rds-host/openstates"


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
                return_value=(0, b"", b"", False, False),
            ) as mock_helper,
            patch(
                "ddp_sync.pipelines.openstates_scrape.resolve_rds_database_url",
                return_value=("postgresql://rds/openstates", ""),
            ),
            patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()),
            patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
        ):
            await job(CONFIG)
        mock_helper.assert_called_once()


def test_group_kill_optional_params_are_backward_compatible():
    """Every argument after `timeout` must stay optional for the existing three-arg callers.

    Updated for OPEN-155, which added progress_dir/stall_seconds. Deliberately asserts the
    *property* that matters -- three-arg calls still work and everything else defaults to None --
    rather than an exact parameter list, which pinned the signature so tightly that adding an
    optional argument failed the test without anything actually breaking.
    """
    import inspect

    from ddp_sync.pipelines.openstates_scrape import _run_with_group_kill

    params = inspect.signature(_run_with_group_kill).parameters
    assert list(params)[:3] == ["cmd", "env", "timeout"]
    for name in list(params)[3:]:
        assert params[name].default is None, f"{name} must be optional"
