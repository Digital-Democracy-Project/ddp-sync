"""Scheduler wiring tests for GrantBot's monthly funder-scrape trigger
(SYNC-36/AGENTS-54). Mirrors test_scheduler_session_pipeline_batch.py's
pattern (temp YAML config file + UpdateScheduler); unlike that job, this one
has no required keys with no sensible default, so there's no
missing-required-key case here.
"""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.scheduler import UpdateScheduler


def _scheduler_with_yaml(yaml_text: str) -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write(textwrap.dedent(yaml_text))
        path = Path(f.name)
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    return UpdateScheduler(settings=settings, config_path=path)


@pytest.mark.asyncio
async def test_disabled_by_default_no_job_registered():
    """Config omits grantbot_scrape entirely -- no job, no crash."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
          webflow_status: {enabled: true}
          version_check: {enabled: true}
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "grantbot_scrape" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_explicitly_disabled_no_job_registered():
    sched = _scheduler_with_yaml(
        """
        grantbot_scrape:
          enabled: false
          frequency: monthly
          day_of_month: 1
          sync_time_utc: "07:00"
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "grantbot_scrape" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_enabled_monthly_registers_cron_job():
    sched = _scheduler_with_yaml(
        """
        grantbot_scrape:
          enabled: true
          frequency: monthly
          day_of_month: 1
          sync_time_utc: "07:00"
        """
    )
    sched.start()
    try:
        jobs = {j.id: j for j in sched.scheduler.get_jobs()}
        assert "grantbot_scrape" in jobs
        job = jobs["grantbot_scrape"]
        assert job.func.__name__ == "_grantbot_scrape_wrapper"
        assert job.next_run_time is not None
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_enabled_with_only_defaults_still_registers():
    """Every field has a sensible default -- an `enabled: true` block with
    nothing else must still register (no required-key validation gate,
    unlike session_pipeline_batch)."""
    sched = _scheduler_with_yaml(
        """
        grantbot_scrape:
          enabled: true
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "grantbot_scrape" in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_registered_job_calls_run_grantbot_scrape_job_scheduled():
    sched = _scheduler_with_yaml(
        """
        grantbot_scrape:
          enabled: true
          frequency: monthly
          day_of_month: 1
          sync_time_utc: "07:00"
        """
    )
    with patch(
        "ddp_sync.pipelines.grantbot_scrape.run_grantbot_scrape_job",
        new=AsyncMock(return_value={"success": True, "status": "started"}),
    ) as mock_run:
        sched.start()
        try:
            jobs = {j.id: j for j in sched.scheduler.get_jobs()}
            job = jobs["grantbot_scrape"]
            await job.func()
        finally:
            sched.stop()

    mock_run.assert_awaited_once()
    assert mock_run.await_args.kwargs["trigger"] == "scheduled"
