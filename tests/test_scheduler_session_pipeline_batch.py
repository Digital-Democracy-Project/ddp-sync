"""Scheduler wiring tests for the session-targeted BillArtifact batch job
(SYNC-9). Mirrors test_scheduler_concept_statement_dispatch.py's pattern
(temp YAML config file + UpdateScheduler), with an added case this job
needs and concept_statement_dispatch doesn't: required config keys with no
sensible default (jurisdiction_iso2, session_code, artifact_types, limit).
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
    """Config omits session_pipeline_batch entirely -- no job, no crash."""
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
        assert "session_pipeline_batch" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_explicitly_disabled_no_job_registered():
    sched = _scheduler_with_yaml(
        """
        session_pipeline_batch:
          enabled: false
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "13:00"
          jurisdiction_iso2: "FL"
          session_code: "2026F"
          artifact_types: [bill_summary]
          limit: 10
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "session_pipeline_batch" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_enabled_but_missing_required_key_not_registered():
    """enabled: true with no artifact_types -- must not register a job that
    would fail every time it fires."""
    sched = _scheduler_with_yaml(
        """
        session_pipeline_batch:
          enabled: true
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "13:00"
          jurisdiction_iso2: "FL"
          session_code: "2026F"
          limit: 10
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "session_pipeline_batch" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_enabled_weekly_registers_cron_job():
    sched = _scheduler_with_yaml(
        """
        session_pipeline_batch:
          enabled: true
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "13:00"
          jurisdiction_iso2: "FL"
          session_code: "2026F"
          artifact_types: [bill_summary, bill_pros_cons]
          limit: 10
        """
    )
    sched.start()
    try:
        jobs = {j.id: j for j in sched.scheduler.get_jobs()}
        assert "session_pipeline_batch" in jobs
        job = jobs["session_pipeline_batch"]
        assert job.func.__name__ == "_session_pipeline_batch_wrapper"
        assert job.next_run_time is not None
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_enabled_daily_registers_job():
    sched = _scheduler_with_yaml(
        """
        session_pipeline_batch:
          enabled: true
          frequency: daily
          sync_time_utc: "13:00"
          jurisdiction_iso2: "FL"
          session_code: "2026F"
          artifact_types: [bill_summary]
          limit: 5
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "session_pipeline_batch" in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_registered_job_calls_run_scheduled_session_pipeline_with_full_config():
    sched = _scheduler_with_yaml(
        """
        session_pipeline_batch:
          enabled: true
          frequency: daily
          sync_time_utc: "13:00"
          jurisdiction_iso2: "FL"
          session_code: "2026F"
          artifact_types: [bill_summary, bill_pros_cons]
          include_org_research: true
          limit: 10
        """
    )
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_scheduled_session_pipeline",
        new=AsyncMock(return_value={"bills_considered": 0}),
    ) as mock_run:
        sched.start()
        try:
            jobs = {j.id: j for j in sched.scheduler.get_jobs()}
            job = jobs["session_pipeline_batch"]
            await job.func()
        finally:
            sched.stop()

    mock_run.assert_awaited_once()
    passed_config = mock_run.await_args.args[0]
    assert passed_config["jurisdiction_iso2"] == "FL"
    assert passed_config["session_code"] == "2026F"
    assert passed_config["artifact_types"] == ["bill_summary", "bill_pros_cons"]
    assert passed_config["include_org_research"] is True
    assert passed_config["limit"] == 10
