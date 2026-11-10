"""SYNC-153: no archive job is registered on a schedule.

Until SYNC-153 every archive jurisdiction had its own weekly job on its own day (`openstates_archive.schedule`, 2026-08-10),
a leftover backstop from before the after-scrape archive (OPEN-291). A state is now archived only right after its own scrape
(inside its openstates_scrape_<state> pipeline job) or by the manual archive trigger. Even a host whose sync_schedule.yaml
still has the old `jurisdictions` / `schedule` keys registers nothing.
"""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ddp_sync.scheduler import DEFAULT_CONFIG_PATH, UpdateScheduler


def _scheduler(path: Path) -> UpdateScheduler:
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    return UpdateScheduler(settings=settings, config_path=path)


@pytest.mark.asyncio
async def test_an_old_style_archive_block_registers_no_archive_job():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent("""
            bill_sync:
              sync_time_utc: "04:00"
            openstates_archive:
              enabled: true
              sync_time_utc: "05:00"
              jurisdictions: [fl, us, al]
              schedule: {fl: monday, us: sunday, al: saturday}
        """))
        path = Path(f.name)
    sched = _scheduler(path)
    sched.start()
    try:
        assert not [j.id for j in sched.scheduler.get_jobs() if j.id.startswith("openstates_archive")]
    finally:
        sched.stop()


def test_the_committed_config_has_no_archive_list_or_days():
    import yaml

    block = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())["openstates_archive"]
    assert "schedule" not in block and "jurisdictions" not in block and "sync_time_utc" not in block
