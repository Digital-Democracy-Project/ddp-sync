"""Scheduler wiring tests for the concept-statement dispatch batch job
(ddp-infra PLAN-bill-concept-polling.md §0.4). Mirrors
test_scheduler_bio_sync.py's pattern exactly (temp YAML config file +
UpdateScheduler), since this job's config shape (enabled/frequency/
sync_day/sync_time_utc) is copied from legislator_bio_sync's own.
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
    """Config omits concept_statement_dispatch entirely -- no job, no crash."""
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
        assert "weekly_concept_statement_dispatch" not in ids
        assert "daily_concept_statement_dispatch" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_explicitly_disabled_no_job_registered():
    sched = _scheduler_with_yaml(
        """
        concept_statement_dispatch:
          enabled: false
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "11:00"
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "weekly_concept_statement_dispatch" not in ids
        assert "daily_concept_statement_dispatch" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_enabled_weekly_registers_cron_job():
    sched = _scheduler_with_yaml(
        """
        concept_statement_dispatch:
          enabled: true
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "11:00"
          max_bills_per_run: 25
        """
    )
    sched.start()
    try:
        jobs = {j.id: j for j in sched.scheduler.get_jobs()}
        assert "weekly_concept_statement_dispatch" in jobs
        job = jobs["weekly_concept_statement_dispatch"]
        assert job.func.__name__ == "_concept_statement_dispatch_wrapper"
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_enabled_daily_registers_daily_job():
    sched = _scheduler_with_yaml(
        """
        concept_statement_dispatch:
          enabled: true
          frequency: daily
          sync_time_utc: "11:00"
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "daily_concept_statement_dispatch" in ids
        assert "weekly_concept_statement_dispatch" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_frequency_toggle_does_not_leave_stale_job():
    sched = _scheduler_with_yaml(
        """
        concept_statement_dispatch:
          enabled: true
          frequency: daily
          sync_time_utc: "11:00"
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "daily_concept_statement_dispatch" in ids
    finally:
        sched.stop()

    sched2 = _scheduler_with_yaml(
        """
        concept_statement_dispatch:
          enabled: true
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "11:00"
        """
    )
    sched2.start()
    try:
        ids = {j.id for j in sched2.scheduler.get_jobs()}
        assert "weekly_concept_statement_dispatch" in ids
        assert "daily_concept_statement_dispatch" not in ids
    finally:
        sched2.stop()


@pytest.mark.asyncio
async def test_jurisdictions_default_to_active_jurisdictions_top_level_list():
    """No `jurisdictions` override in the job's own block -> falls back to
    sync_schedule.yaml's top-level active_jurisdictions list."""
    sched = _scheduler_with_yaml(
        """
        active_jurisdictions:
          - FL
          - WA
        concept_statement_dispatch:
          enabled: true
          frequency: daily
          sync_time_utc: "11:00"
        """
    )
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.run_concept_statement_batch_job",
        new=AsyncMock(return_value={}),
    ) as mock_run:
        sched.start()
        try:
            jobs = {j.id: j for j in sched.scheduler.get_jobs()}
            job = jobs["daily_concept_statement_dispatch"]
            await job.func()
        finally:
            sched.stop()

    mock_run.assert_awaited_once()
    assert mock_run.await_args.kwargs["jurisdictions"] == ["FL", "WA"]


@pytest.mark.asyncio
async def test_jurisdictions_override_takes_precedence_over_active_jurisdictions():
    sched = _scheduler_with_yaml(
        """
        active_jurisdictions:
          - FL
          - WA
        concept_statement_dispatch:
          enabled: true
          frequency: daily
          sync_time_utc: "11:00"
          jurisdictions:
            - AZ
        """
    )
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.run_concept_statement_batch_job",
        new=AsyncMock(return_value={}),
    ) as mock_run:
        sched.start()
        try:
            jobs = {j.id: j for j in sched.scheduler.get_jobs()}
            job = jobs["daily_concept_statement_dispatch"]
            await job.func()
        finally:
            sched.stop()

    assert mock_run.await_args.kwargs["jurisdictions"] == ["AZ"]


@pytest.mark.asyncio
async def test_max_bills_per_run_passed_through_to_the_job_config():
    sched = _scheduler_with_yaml(
        """
        concept_statement_dispatch:
          enabled: true
          frequency: daily
          sync_time_utc: "11:00"
          max_bills_per_run: 7
        """
    )
    with patch(
        "ddp_sync.pipelines.concept_statement_dispatch.run_concept_statement_batch_job",
        new=AsyncMock(return_value={}),
    ) as mock_run:
        sched.start()
        try:
            jobs = {j.id: j for j in sched.scheduler.get_jobs()}
            job = jobs["daily_concept_statement_dispatch"]
            await job.func()
        finally:
            sched.stop()

    passed_config = mock_run.await_args.args[0]
    assert passed_config["max_bills_per_run"] == 7
