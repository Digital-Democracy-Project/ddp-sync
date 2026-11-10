"""SYNC-130: the FL job is one run with no session argument, so the scraper picks the active sessions.

It used to loop over a hand-kept `primary.fl.sessions` list, and 2027 went unscraped because nobody added it.
SYNC-152: FL is one state pipeline like any other; its `timing.fl` entry has no sessions.
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


def test_the_checked_in_schedule_lists_no_fl_sessions(config):
    assert "sessions" not in config["timing"]["fl"]
    assert openstates_scrape.state_timing(config, "fl")["sessions"] == []


def _bookkeeping():
    return patch.object(openstates_scrape, "_check_sustained_blocking", new=AsyncMock()), patch(
        "ddp_sync.pipelines.openstates_archive.archive_after_scrape", new=AsyncMock(return_value=None)
    )


@pytest.mark.asyncio
async def test_the_fl_job_runs_once_without_a_session(config):
    run = AsyncMock(return_value={"success": True})
    status = AsyncMock()
    history, archive = _bookkeeping()
    with patch.object(openstates_scrape, "_run_scrape", new=run), \
            patch.object(openstates_scrape, "_write_flow_status", new=status), history, archive:
        result = await openstates_scrape.run_state_pipeline("fl", config)
    assert [c.args[:2] for c in run.await_args_list] == [("fl", None)]
    assert result["success"] is True
    assert status.await_args.args[1]["status"] == "completed"


@pytest.mark.asyncio
async def test_a_failed_fl_run_is_recorded_as_failed(config):
    status = AsyncMock()
    history, archive = _bookkeeping()
    with patch.object(openstates_scrape, "_run_scrape", new=AsyncMock(return_value={"success": False})), \
            patch.object(openstates_scrape, "_write_flow_status", new=status), history, archive, \
            pytest.raises(openstates_scrape.PipelineFailed):
        await openstates_scrape.run_state_pipeline("fl", config)
    assert status.await_args.args[1]["status"] == "failed"
