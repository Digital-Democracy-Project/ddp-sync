"""OPEN-286: a missing SLACK_BOT_TOKEN must be loud at startup, not just a warning
buried inside a scrape-failure path nobody hits until something actually fails (MA's
real 2026-09-06 load failure went unnoticed for a week -- see notes/ops-handoff). Light-
touch, matching this module's own established convention (test_scheduler_per_task_flags.py)."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ddp_sync.scheduler import UpdateScheduler

# caplog captures stdlib logging, which structlog wraps differently here (see this
# repo's own test_votebot_eval_yaml_validation.py note on the same mismatch) -- patching
# the module logger directly is the reliable way to assert a structlog call happened.


def _settings(environment: str) -> MagicMock:
    settings = MagicMock()
    settings.environment = environment
    settings.sync_interval_minutes = 30
    settings.bill_sync_enabled = False
    settings.legislator_sync_enabled = False
    settings.legislator_bio_sync_enabled = False
    settings.organization_sync_enabled = False
    settings.voatz_sync_enabled = False
    settings.webflow_batch_enabled = False
    settings.votebot_eval_enabled = False
    settings.api_health_check_enabled = False
    settings.openstates_scrape_enabled = False
    settings.openstates_archive_enabled = False
    settings.mi_cookie_publish_enabled = False
    settings.session_pipeline_batch_enabled = False
    return settings


def _scheduler(environment: str) -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("sync_time_utc: '04:00'\n")
        path = Path(f.name)
    return UpdateScheduler(settings=_settings(environment), config_path=path)


@pytest.mark.asyncio
async def test_production_with_no_slack_token_logs_critical(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    sched = _scheduler("production")
    with patch("ddp_sync.scheduler.logger") as mock_logger:
        sched.start()
        assert mock_logger.critical.called
        (msg,), _ = mock_logger.critical.call_args
        assert "SLACK_BOT_TOKEN" in msg
    sched.stop()


@pytest.mark.asyncio
async def test_production_with_slack_token_set_stays_quiet(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-real-token")
    sched = _scheduler("production")
    with patch("ddp_sync.scheduler.logger") as mock_logger:
        sched.start()
        assert not mock_logger.critical.called
    sched.stop()


@pytest.mark.asyncio
async def test_non_production_with_no_slack_token_stays_quiet(monkeypatch):
    """Dev/test environments are expected to run without a real Slack token --
    only production should ever raise this alarm."""
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    sched = _scheduler("development")
    with patch("ddp_sync.scheduler.logger") as mock_logger:
        sched.start()
        assert not mock_logger.critical.called
    sched.stop()
