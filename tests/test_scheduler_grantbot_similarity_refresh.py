"""Scheduler wiring for GrantBot's twice-yearly grantee-similarity refresh trigger (AGENTS-128).
Same temp-YAML + UpdateScheduler pattern as test_scheduler_grantbot_scrape.py."""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.scheduler import UpdateScheduler

_YAML = """
    grantbot_similarity_refresh:
      enabled: true
      frequency: semiannual
      months: [3, 9]
      day_of_month: 15
      sync_time_utc: "07:00"
    """


def _scheduler_with_yaml(yaml_text: str, *, flag: bool = True) -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(yaml_text))
        path = Path(f.name)
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    settings.grantbot_similarity_refresh_enabled = flag
    return UpdateScheduler(settings=settings, config_path=path)


def _jobs(sched):
    return {j.id: j for j in sched.scheduler.get_jobs()}


@pytest.mark.asyncio
async def test_no_config_block_registers_nothing():
    sched = _scheduler_with_yaml("bill_sync:\n  sync_time_utc: '04:00'\n")
    sched.start()
    try:
        assert "grantbot_similarity_refresh" not in _jobs(sched)
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_explicitly_disabled_registers_nothing():
    sched = _scheduler_with_yaml(_YAML.replace("enabled: true", "enabled: false"))
    sched.start()
    try:
        assert "grantbot_similarity_refresh" not in _jobs(sched)
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_a_host_can_opt_out_with_its_env_flag():
    sched = _scheduler_with_yaml(_YAML, flag=False)
    sched.start()
    try:
        assert "grantbot_similarity_refresh" not in _jobs(sched)
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_registers_a_cron_job_for_the_middle_of_march_and_september_at_0700_utc():
    sched = _scheduler_with_yaml(_YAML)
    sched.start()
    try:
        job = _jobs(sched)["grantbot_similarity_refresh"]
        fields = {f.name: str(f) for f in job.trigger.fields}
        assert fields["month"] == "3,9" and fields["day"] == "15"
        assert fields["hour"] == "7" and fields["minute"] == "0"
        assert job.max_instances == 1 and job.next_run_time is not None
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_enabled_with_only_defaults_still_registers_with_the_same_months():
    sched = _scheduler_with_yaml("grantbot_similarity_refresh:\n  enabled: true\n")
    sched.start()
    try:
        job = _jobs(sched)["grantbot_similarity_refresh"]
        fields = {f.name: str(f) for f in job.trigger.fields}
        assert fields["month"] == "3,9" and fields["day"] == "15"
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_it_runs_before_the_monthly_scrape_on_the_first_of_the_next_month():
    """The refresh takes a day or two; the 15th leaves ample time before the funder scrape on the 1st."""
    sched = _scheduler_with_yaml(_YAML + "\n    grantbot_scrape:\n      enabled: true\n      day_of_month: 1\n")
    sched.start()
    try:
        jobs = _jobs(sched)
        assert str(next(f for f in jobs["grantbot_similarity_refresh"].trigger.fields if f.name == "day")) == "15"
        assert str(next(f for f in jobs["grantbot_scrape"].trigger.fields if f.name == "day")) == "1"
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_the_registered_job_calls_the_refresh_pipeline_as_scheduled():
    sched = _scheduler_with_yaml(_YAML)
    with patch(
        "ddp_sync.pipelines.grantbot_scrape.run_grantbot_similarity_refresh_job",
        new=AsyncMock(return_value={"success": True, "status": "started"}),
    ) as mock_run:
        sched.start()
        try:
            await _jobs(sched)["grantbot_similarity_refresh"].func()
        finally:
            sched.stop()
    mock_run.assert_awaited_once()
    assert mock_run.await_args.kwargs["trigger"] == "scheduled"
