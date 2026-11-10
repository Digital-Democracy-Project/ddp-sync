"""OPEN-494: Pennsylvania is nightly at 04:00 UTC.

SYNC-152 moved PA from the `primary` block to `openstates_scrape.timing.pa` and gave it the same per-state pipeline job as
every other scraped state (`openstates_scrape_pa`). Drives the real UpdateScheduler (real APScheduler) against the
committed config/sync_schedule.yaml and a temp manifest; only the scrape call itself is faked.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from ddp_sync.scheduler import DEFAULT_CONFIG_PATH, UpdateScheduler
from ddp_sync.services import jurisdiction_enrollment as je


@pytest.fixture
def sched(tmp_path, monkeypatch):
    je._cache.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(
        {"pa": {"status": "scraping"}, "va": {"status": "live"}, "ne": {"status": "scraping"}}))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    s = UpdateScheduler(settings=settings, config_path=DEFAULT_CONFIG_PATH)
    s._register_openstates_scrape_jobs()
    return s


def _fields(sched, job_id) -> dict[str, str]:
    job = sched.scheduler.get_job(job_id)
    assert job is not None, job_id
    return {f.name: str(f) for f in job.trigger.fields}


def test_the_real_sync_schedule_has_pa_nightly_at_0400(sched):
    f = _fields(sched, "openstates_scrape_pa")
    assert (f["hour"], f["minute"], f["day_of_week"]) == ("4", "0", "*")


def test_states_with_no_timing_entry_run_weekly_on_sunday_at_0200(sched):
    for j in ("va", "ne"):
        f = _fields(sched, f"openstates_scrape_{j}")
        assert (f["hour"], f["minute"], f["day_of_week"]) == ("2", "0", "sun")


@pytest.mark.asyncio
async def test_the_pa_job_runs_the_pa_pipeline_only(sched):
    calls: list[str] = []

    async def _fake_run_scrape(jurisdiction, *_a, **_kw):
        calls.append(jurisdiction)
        return {"success": True, "jurisdiction": jurisdiction}

    with patch("ddp_sync.pipelines.openstates_scrape._run_scrape", new=_fake_run_scrape), patch(
        "ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()
    ), patch("ddp_sync.pipelines.openstates_scrape._check_sustained_blocking", new=AsyncMock()), patch(
        "ddp_sync.pipelines.openstates_archive.archive_after_scrape", new=AsyncMock(return_value=True)
    ):
        await sched.scheduler.get_job("openstates_scrape_pa").func()
    assert calls == ["pa"]
