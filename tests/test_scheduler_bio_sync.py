"""Scheduler wiring tests for the legislator bio sync.

Light-touch tests — the scheduler module has no other test coverage in
this repo, and the bio-sync block is mechanical (mirrors legislator_sync
exactly). These pin the enabled/disabled gating and the runner-method
wiring so the cron schema doesn't drift.
"""

from __future__ import annotations

import os
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

import pytest

from ddp_sync.scheduler import UpdateScheduler


def _scheduler_with_yaml(yaml_text: str) -> UpdateScheduler:
    """Build an UpdateScheduler using a temp config file."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write(textwrap.dedent(yaml_text))
        path = Path(f.name)
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    return UpdateScheduler(settings=settings, config_path=path)


@pytest.mark.asyncio
async def test_legislator_bio_sync_disabled_no_job_registered():
    """Default config has enabled: false → no bio-sync job."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
          webflow_status: {enabled: true}
          version_check: {enabled: true}
        legislator_bio_sync:
          enabled: false
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "07:00"
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "weekly_legislator_bio_sync" not in ids
        assert "daily_legislator_bio_sync" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_legislator_bio_sync_enabled_weekly_registers_cron_job():
    """enabled: true + frequency: weekly → weekly_legislator_bio_sync job."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
          webflow_status: {enabled: true}
          version_check: {enabled: true}
        legislator_bio_sync:
          enabled: true
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "07:00"
        """
    )
    sched.start()
    try:
        jobs = {j.id: j for j in sched.scheduler.get_jobs()}
        assert "weekly_legislator_bio_sync" in jobs
        # Confirm the job's callable is the orchestrator runner, not a
        # leftover legislator_sync handler.
        job = jobs["weekly_legislator_bio_sync"]
        assert job.func.__name__ == "_run_legislator_bio_sync"
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_legislator_bio_sync_enabled_daily_registers_daily_job():
    """frequency: daily → daily_legislator_bio_sync (no day_of_week)."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
          webflow_status: {enabled: true}
          version_check: {enabled: true}
        legislator_bio_sync:
          enabled: true
          frequency: daily
          sync_time_utc: "07:00"
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "daily_legislator_bio_sync" in ids
        assert "weekly_legislator_bio_sync" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_run_legislator_bio_sync_invokes_orchestrator_with_yaml_options():
    """The runner reads YAML knobs into BioSyncOptions and calls run().

    Doesn't actually run the orchestrator (mocked) — pins the wiring
    between sync_schedule.yaml and BioSyncOptions construction.
    """
    sched = _scheduler_with_yaml(
        """
        legislator_bio_sync:
          enabled: true
          jurisdiction: us
          auto_create: false
          historical_since: "2024-01-01"
          target: webflow
        """
    )
    fake_report = MagicMock()
    fake_report.aborted = False
    fake_report.cms_items_seen = 224
    fake_report.would_patch = []
    fake_report.would_create = []
    fake_report.errors = []
    fake_report.abort_reason = None
    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.run = AsyncMock(return_value=fake_report)
        result = await sched._run_legislator_bio_sync()

    assert result["success"] is True
    assert result["items_seen"] == 224
    instance.run.assert_awaited_once()
    options = instance.run.call_args.args[0]
    assert options.target == "webflow"
    assert options.jurisdiction == "us"
    assert options.auto_create is False
    assert options.dry_run is False
    assert str(options.historical_since) == "2024-01-01"


@pytest.mark.asyncio
async def test_run_legislator_bio_sync_logs_and_returns_failure_on_unhandled_exception():
    """If the orchestrator raises, the runner logs + returns success=False
    rather than propagating into APScheduler's job-loop."""
    sched = _scheduler_with_yaml(
        """
        legislator_bio_sync:
          enabled: true
        """
    )
    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.run = AsyncMock(side_effect=RuntimeError("simulated"))
        result = await sched._run_legislator_bio_sync()

    assert result["success"] is False
    assert "simulated" in result["error"]


@pytest.mark.asyncio
async def test_scheduler_path_emits_run_completed_metric_for_external_monitoring():
    """Round-17 fix: scheduler runner emits a structured-log metric event
    that external monitoring can scrape.

    The Zapier alert is fired by ``LegislatorBioPipeline.run()``'s
    try/finally — same code path whether triggered by HTTP or the
    scheduler — but external monitoring (CloudWatch / Datadog) needs
    a separate structured-log signal. ``alert_sent`` covers the
    Zapier-success path; ``scheduled_run_completed`` is the
    independent scheduler-path signal.
    """
    sched = _scheduler_with_yaml("legislator_bio_sync: {enabled: true}")
    fake_report = MagicMock()
    fake_report.aborted = False
    fake_report.cms_items_seen = 50
    fake_report.would_patch = [{"x": 1}, {"x": 2}]
    fake_report.would_create = []
    fake_report.errors = []
    fake_report.abort_reason = None

    captured_kwargs: list[dict] = []
    fake_logger = MagicMock()
    fake_logger.info = lambda msg, **kw: captured_kwargs.append(kw)
    fake_logger.warning = lambda msg, **kw: None
    fake_logger.exception = lambda msg, **kw: captured_kwargs.append(kw)

    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline, patch("ddp_sync.scheduler.logger", new=fake_logger):
        instance = MockPipeline.return_value
        instance.run = AsyncMock(return_value=fake_report)
        result = await sched._run_legislator_bio_sync()

    assert result["success"] is True
    metric_events = [
        kw for kw in captured_kwargs
        if kw.get("metric") == "legislator_bio_sync.scheduled_run_completed"
    ]
    assert metric_events, (
        f"scheduler did not emit scheduled_run_completed metric; "
        f"captured: {captured_kwargs}"
    )
    # success flag is True for clean run
    assert metric_events[0].get("success") is True


@pytest.mark.asyncio
async def test_scheduler_path_run_completed_metric_marks_failure_on_aborted_run():
    """When the orchestrator returns aborted=True, the metric event's
    success flag is False even though the runner itself didn't raise."""
    sched = _scheduler_with_yaml("legislator_bio_sync: {enabled: true}")
    fake_report = MagicMock()
    fake_report.aborted = True
    fake_report.abort_reason = "simulated rate-limit"
    fake_report.cms_items_seen = 5
    fake_report.would_patch = []
    fake_report.would_create = []
    fake_report.errors = []

    captured_kwargs: list[dict] = []
    fake_logger = MagicMock()
    fake_logger.info = lambda msg, **kw: captured_kwargs.append(kw)
    fake_logger.warning = lambda msg, **kw: None
    fake_logger.exception = lambda msg, **kw: captured_kwargs.append(kw)

    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline, patch("ddp_sync.scheduler.logger", new=fake_logger):
        instance = MockPipeline.return_value
        instance.run = AsyncMock(return_value=fake_report)
        result = await sched._run_legislator_bio_sync()

    # Aborted run completes the wrapper; result.success=False
    assert result["success"] is False
    metric_events = [
        kw for kw in captured_kwargs
        if kw.get("metric") == "legislator_bio_sync.scheduled_run_completed"
    ]
    assert metric_events, "metric event missing on aborted run"
    assert metric_events[0].get("success") is False


@pytest.mark.asyncio
async def test_scheduler_frequency_toggle_does_not_leave_stale_job():
    """Round-17 fix: toggling frequency (daily↔weekly) at config reload
    must not leave the previous frequency's job registered alongside the
    new one. The scheduler.start() preamble removes both possible job
    ids before re-registering."""
    sched = _scheduler_with_yaml(
        """
        legislator_bio_sync:
          enabled: true
          frequency: daily
          sync_time_utc: "07:00"
        """
    )
    sched.start()
    try:
        ids_after_first_start = {j.id for j in sched.scheduler.get_jobs()}
        assert "daily_legislator_bio_sync" in ids_after_first_start
        assert "weekly_legislator_bio_sync" not in ids_after_first_start

        # Simulate a config flip: stop, swap config, restart. (We
        # construct a new scheduler since stop() shuts down the
        # underlying AsyncIOScheduler.)
    finally:
        sched.stop()

    sched2 = _scheduler_with_yaml(
        """
        legislator_bio_sync:
          enabled: true
          frequency: weekly
          sync_day: sunday
          sync_time_utc: "07:00"
        """
    )
    sched2.start()
    try:
        ids = {j.id for j in sched2.scheduler.get_jobs()}
        # Only the new frequency's job exists; the stale one is gone.
        assert "weekly_legislator_bio_sync" in ids
        assert "daily_legislator_bio_sync" not in ids
    finally:
        sched2.stop()


@pytest.mark.asyncio
async def test_run_legislator_bio_sync_invalid_historical_since_falls_back():
    """Bad historical_since string → falls back to 2023-01-01, doesn't crash."""
    sched = _scheduler_with_yaml(
        """
        legislator_bio_sync:
          enabled: true
          historical_since: "not-a-date"
        """
    )
    fake_report = MagicMock()
    fake_report.aborted = False
    fake_report.cms_items_seen = 0
    fake_report.would_patch = []
    fake_report.would_create = []
    fake_report.errors = []
    fake_report.abort_reason = None
    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.run = AsyncMock(return_value=fake_report)
        await sched._run_legislator_bio_sync()

    options = instance.run.call_args.args[0]
    assert str(options.historical_since) == "2023-01-01"
