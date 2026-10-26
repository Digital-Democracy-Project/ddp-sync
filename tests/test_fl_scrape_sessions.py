"""SYNC-130: the FL job scrapes the sessions in the checked-in schedule, in order, 2027 included.

Nothing else schedules a Florida session: if 2027 is not in `openstates_scrape.primary.fl.sessions`, its bills
are never scraped.
"""

from unittest.mock import AsyncMock, patch

import pytest
import yaml

from ddp_sync.pipelines import openstates_scrape
from ddp_sync.scheduler import DEFAULT_CONFIG_PATH


@pytest.fixture
def config():
    with open(DEFAULT_CONFIG_PATH) as f:
        return yaml.safe_load(f)["openstates_scrape"]


@pytest.mark.asyncio
async def test_the_fl_job_runs_each_scheduled_session_in_order_ending_with_2027(config):
    run = AsyncMock(return_value={"success": True})
    with patch.object(openstates_scrape, "_run_scrape", new=run), \
            patch.object(openstates_scrape, "_write_flow_status", new=AsyncMock()):
        result = await openstates_scrape.run_fl_scrapes_job(config)
    assert [c.args[1] for c in run.await_args_list] == [
        "session=2026", "session=2026D", "session=2026E", "session=2026F", "session=2027"]
    assert result["success"] is True
