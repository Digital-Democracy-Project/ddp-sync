"""BROKER-541: after a successful weekly people refresh, ddp-sync tells ddp-broker-py once for each state the refresh
imported (the manifest's `enrollment.people_refresh`), so a roster change reaches that state's scorecards without
waiting for its next scrape. Same switch and sending rules as the after-scrape signal (BROKER-537), its own debounce
name, and it never changes the people refresh's own result.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import run_people_refresh_job
from ddp_sync.services import broker_client
from ddp_sync.services.broker_client import BrokerClientError
from tests import sync157_manifest

ON = {"broker_scorecard_signal": {"enabled": True}}
SETTINGS = SimpleNamespace(ddp_broker_api_base="https://broker.test", ddp_broker_api_token="tok")


@pytest.fixture(autouse=True)
def manifest(tmp_path, monkeypatch):
    # three states imported by the weekly refresh, the federal one among them (the manifest spells it "us")
    sync157_manifest.use(tmp_path, monkeypatch, **sync157_manifest.only("enrollment", "people_refresh", ["us", "fl", "ut"]))


class _FakeRedis:
    def __init__(self):
        self.keys: set[str] = set()

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.keys:
            return None
        self.keys.add(key)
        return True


@pytest.fixture
def redis():
    fake = _FakeRedis()
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=fake)):
        yield fake


@pytest.fixture
def settings():
    with patch("ddp_sync.config.get_settings", return_value=SETTINGS), patch.object(
        broker_client, "get_settings", return_value=SETTINGS
    ):
        yield SETTINGS


@pytest.fixture
def send():
    with patch("ddp_sync.services.broker_client.request_scorecard_refresh", new=AsyncMock()) as mock:
        yield mock


async def _people_refresh(config, returncode=0, timed_out=False):
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        return_value=(returncode, b"", b"boom" if returncode else b"", timed_out, False),
    ), patch(
        "ddp_sync.pipelines.openstates_scrape.resolve_rds_database_url",
        return_value=("postgresql://rds/openstates", ""),
    ), patch("ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()), patch(
        "ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"
    ):
        return await run_people_refresh_job(config)


def _sent(send):
    return sorted(call.args[0] for call in send.await_args_list)


@pytest.mark.asyncio
async def test_a_successful_refresh_signals_each_state_it_imported_once(settings, redis, send):
    result = await _people_refresh(ON)
    assert result["success"] is True
    assert _sent(send) == ["fl", "us", "ut"]
    assert send.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("config", [None, {}, {"broker_scorecard_signal": {"enabled": False}}])
async def test_with_the_switch_off_nothing_is_sent(settings, redis, send, config):
    result = await _people_refresh(config)
    assert result["success"] is True
    send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode,timed_out", [(1, False), (-9, True)])
async def test_a_failed_or_timed_out_refresh_sends_nothing(settings, redis, send, returncode, timed_out):
    result = await _people_refresh(ON, returncode=returncode, timed_out=timed_out)
    assert result["success"] is False
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_one_states_failure_does_not_stop_the_others_or_the_job(settings, redis, send):
    async def _send(code):
        if code == "fl":
            raise BrokerClientError("broker down")

    send.side_effect = _send
    result = await _people_refresh(ON)
    assert result == {"success": True, "duration_seconds": result["duration_seconds"]}
    assert _sent(send) == ["fl", "us", "ut"]


@pytest.mark.asyncio
async def test_a_state_whose_code_cannot_be_mapped_does_not_stop_the_others(settings, redis, send):
    real = __import__("ddp_sync.pipelines.openstates_archive", fromlist=["_archive_jurisdiction_code"])._archive_jurisdiction_code

    def _map(code):
        if code == "fl":
            raise ValueError("bad code")
        return real(code)

    with patch("ddp_sync.pipelines.openstates_archive._archive_jurisdiction_code", side_effect=_map):
        result = await _people_refresh(ON)
    assert result["success"] is True
    assert _sent(send) == ["us", "ut"]


@pytest.mark.asyncio
async def test_a_scrape_signal_sent_minutes_before_does_not_swallow_this_one(settings, redis, send):
    """The scrape signal may have read the roster before this import finished: this one has its own debounce name."""
    redis.keys.add("ddp:broker_scorecard_signal:ut")
    await _people_refresh(ON)
    assert "ut" in _sent(send)
    assert {"ddp:broker_scorecard_signal_people:ut", "ddp:broker_scorecard_signal_people:fl",
            "ddp:broker_scorecard_signal_people:us"} <= redis.keys


@pytest.mark.asyncio
async def test_without_a_broker_address_or_token_nothing_is_sent_and_the_job_still_succeeds(redis, send):
    unset = SimpleNamespace(ddp_broker_api_base="", ddp_broker_api_token="")
    with patch("ddp_sync.config.get_settings", return_value=unset):
        result = await _people_refresh(ON)
    assert result["success"] is True
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_state_not_enrolled_for_the_people_refresh_is_not_signalled(tmp_path, monkeypatch, settings, redis, send):
    sync157_manifest.use(tmp_path, monkeypatch, **sync157_manifest.only("enrollment", "people_refresh", ["ut"]))
    await _people_refresh(ON)
    assert _sent(send) == ["ut"]
