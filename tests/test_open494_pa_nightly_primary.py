"""OPEN-494: Pennsylvania is a nightly primary state at 04:00 UTC, and not in the Sunday secondary batch.

Drives the real UpdateScheduler (real APScheduler, real temp config); only the scrape call itself is faked.
The last class reads the committed config/sync_schedule.yaml, so the lineup Ramon decided is what is pinned.
"""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.scheduler import DEFAULT_CONFIG_PATH, UpdateScheduler

_YAML = """
    openstates_scrape:
      enabled: true
      openstates_root: "/tmp/openstates-does-not-exist"
      patch_refresh:
        enabled: false
      primary:
        fl: {{enabled: true, sync_time_utc: "02:00"}}
        wa: {{enabled: true, sync_time_utc: "02:30"}}
        usa: {{enabled: true, sync_time_utc: "03:00"}}
        pa: {{enabled: {pa_enabled}, sync_time_utc: "04:00"}}
      secondary:
        enabled: true
        sync_day: sunday
        sync_time_utc: "02:00"
        jurisdictions: [va, ne]
      people_refresh:
        enabled: false
"""


def _scheduler(pa_enabled: str = "true", path: Path | None = None) -> UpdateScheduler:
    if path is None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(textwrap.dedent(_YAML.format(pa_enabled=pa_enabled)))
            path = Path(f.name)
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    sched = UpdateScheduler(settings=settings, config_path=path)
    sched._register_openstates_scrape_jobs()
    return sched


def _job(sched, job_id):
    return next((j for j in sched.scheduler.get_jobs() if j.id == job_id), None)


def _fields(job) -> dict[str, str]:
    return {f.name: str(f) for f in job.trigger.fields}


async def _scraped_by(job) -> list[str]:
    calls: list[str] = []

    async def _fake_run_scrape(jurisdiction, *_a, **_kw):
        calls.append(jurisdiction)
        return {"success": True, "jurisdiction": jurisdiction}

    with patch("ddp_sync.pipelines.openstates_scrape._run_scrape", new=_fake_run_scrape), patch(
        "ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()
    ), patch("ddp_sync.pipelines.openstates_scrape._check_sustained_blocking", new=AsyncMock()):
        await job.func()
    return calls


class TestPrimaryStateJob:
    def test_pa_is_registered_nightly_at_0400_utc(self):
        job = _job(_scheduler(), "openstates_primary_scrape_pa")
        assert job is not None
        f = _fields(job)
        assert (f["hour"], f["minute"]) == ("4", "0")
        assert f["day_of_week"] == "*", "nightly: no weekday restriction"

    def test_pa_is_not_in_the_sunday_batch(self):
        batch = _job(_scheduler(), "openstates_secondary_scrapes")
        assert _fields(batch)["day_of_week"] == "sun"
        assert batch is not None

    @pytest.mark.asyncio
    async def test_the_nightly_job_scrapes_only_pa_and_the_batch_never_scrapes_it(self):
        sched = _scheduler()
        assert await _scraped_by(_job(sched, "openstates_primary_scrape_pa")) == ["pa"]
        assert await _scraped_by(_job(sched, "openstates_secondary_scrapes")) == ["va", "ne"]

    def test_a_disabled_primary_state_gets_no_job(self):
        assert _job(_scheduler(pa_enabled="false"), "openstates_primary_scrape_pa") is None

    def test_florida_washington_usa_timing_is_untouched(self):
        sched = _scheduler()
        got = {i: (_fields(_job(sched, i))["hour"], _fields(_job(sched, i))["minute"])
               for i in ("openstates_fl_scrape", "openstates_wa_scrape", "openstates_usa_scrape")}
        assert got == {
            "openstates_fl_scrape": ("2", "0"),
            "openstates_wa_scrape": ("2", "30"),
            "openstates_usa_scrape": ("3", "0"),
        }

    def test_a_leftover_escalated_secondary_job_for_pa_is_removed(self):
        sched = _scheduler()
        sched.scheduler.add_job(lambda: None, "interval", hours=1, id="openstates_secondary_scrape_pa")
        sched._register_openstates_scrape_jobs()
        assert _job(sched, "openstates_secondary_scrape_pa") is None


class TestCommittedConfig:
    def test_the_real_sync_schedule_has_pa_nightly_0400_and_out_of_the_sunday_batch(self):
        sched = _scheduler(path=DEFAULT_CONFIG_PATH)
        f = _fields(_job(sched, "openstates_primary_scrape_pa"))
        assert (f["hour"], f["minute"], f["day_of_week"]) == ("4", "0", "*")
        batch_cfg = sched._sync_config["openstates_scrape"]["secondary"]["jurisdictions"]
        assert "pa" not in batch_cfg
        assert {"ne", "la"} <= set(batch_cfg)
