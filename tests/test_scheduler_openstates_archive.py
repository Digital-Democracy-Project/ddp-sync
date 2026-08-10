"""Scheduler wiring tests for the OpenStates bill-document archive jobs.

Changed 2026-08-10 from a single daily job fanning out to every jurisdiction
concurrently, to one weekly job per jurisdiction (see sync_schedule.yaml's
openstates_archive.schedule map) -- added so `us`'s much larger backlog can't
dominate shared CPU/network/DDP-HOT I/O on a day it shares with anything else.
These tests pin the per-jurisdiction registration so that shape doesn't drift.
"""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

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


_BASE_YAML = """
openstates_archive:
  enabled: true
  sync_time_utc: "05:00"
  jurisdictions:
    - fl
    - ut
    - az
    - wa
    - va
    - mi
    - ma
    - al
    - us
  schedule:
    fl: monday
    ut: monday
    az: tuesday
    wa: wednesday
    va: wednesday
    mi: thursday
    ma: friday
    al: saturday
    us: sunday
"""


@pytest.mark.asyncio
async def test_openstates_archive_disabled_no_jobs_registered():
    """enabled: false -> no per-jurisdiction jobs."""
    sched = _scheduler_with_yaml(
        """
        openstates_archive:
          enabled: false
          jurisdictions: [fl, us]
          schedule: {fl: monday, us: sunday}
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert not any(i.startswith("openstates_archive") for i in ids)
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_openstates_archive_registers_one_job_per_jurisdiction():
    """Each jurisdiction in the list gets its own job id, not one shared batch job."""
    sched = _scheduler_with_yaml(_BASE_YAML)
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        for jurisdiction in ("fl", "ut", "az", "wa", "va", "mi", "ma", "al", "us"):
            assert f"openstates_archive_{jurisdiction}" in ids
        # The old single batch-job id must not linger.
        assert "openstates_archive" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_openstates_archive_us_runs_on_its_own_day_alone():
    """`us` is the only jurisdiction scheduled for Sunday -- the whole point
    of the per-jurisdiction split is that federal never shares a day."""
    sched = _scheduler_with_yaml(_BASE_YAML)
    sched.start()
    try:
        jobs = {j.id: j for j in sched.scheduler.get_jobs()}
        sunday_jobs = [
            jid for jid, j in jobs.items()
            if jid.startswith("openstates_archive_")
            and "sun" in str(j.trigger)
        ]
        assert sunday_jobs == ["openstates_archive_us"]
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_openstates_archive_jurisdiction_missing_from_schedule_defaults_to_sunday():
    """A jurisdiction present in `jurisdictions` but absent from `schedule`
    doesn't crash registration -- falls back to Sunday."""
    sched = _scheduler_with_yaml(
        """
        openstates_archive:
          enabled: true
          sync_time_utc: "05:00"
          jurisdictions: [fl, nd]
          schedule:
            fl: monday
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "openstates_archive_fl" in ids
        assert "openstates_archive_nd" in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_openstates_archive_job_invokes_run_single_archive_job_for_its_own_jurisdiction():
    """Each job's callable calls run_single_archive_job with its own jurisdiction,
    not the whole list -- the actual fix for the concurrent-fan-out contention.

    Patches before start() -- _register_openstates_archive_jobs() imports
    run_single_archive_job once at registration time (not lazily per-call), so
    the closure binds whatever the module attribute pointed to when start() ran.
    """
    sched = _scheduler_with_yaml(_BASE_YAML)
    mock_run = AsyncMock(return_value={"success": True})
    with patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job",
        new=mock_run,
    ):
        sched.start()
        try:
            jobs = {j.id: j for j in sched.scheduler.get_jobs()}
            await jobs["openstates_archive_us"].func()
            mock_run.assert_awaited_once()
            assert mock_run.call_args.args[0] == "us"

            mock_run.reset_mock()
            await jobs["openstates_archive_fl"].func()
            mock_run.assert_awaited_once()
            assert mock_run.call_args.args[0] == "fl"
        finally:
            sched.stop()
