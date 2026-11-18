"""BROKER-537: after a successful scrape, ddp-sync tells ddp-broker-py, which reads that state's changed bills and roster
and refreshes the scorecards they change. Off by default, no per-state list (the broker decides which states it
tracks), sent the same way as the bill-event signal (debounced, best effort), and never changes the scrape's result.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from ddp_sync.pipelines.openstates_scrape import (
    PipelineFailed,
    _maybe_signal_broker_bill_events,
    _maybe_signal_broker_scorecard_refresh,
    run_state_pipeline,
)
from ddp_sync.services import broker_client
from ddp_sync.services.broker_client import BrokerClientError, request_scorecard_refresh
from tests import sync157_manifest

CFG = {"broker_scorecard_signal": {"enabled": True}}
SETTINGS = SimpleNamespace(ddp_broker_api_base="https://broker.test", ddp_broker_api_token="tok")


@pytest.fixture(autouse=True)
def manifest(tmp_path, monkeypatch):
    # nobody is enrolled in the bill-event signal: the scorecard signal must not depend on that list
    sync157_manifest.use(tmp_path, monkeypatch, **sync157_manifest.only("enrollment", "broker_bill_event_signal", []))


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


def _scrape(success: bool, jurisdiction: str = "ut"):
    return patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        new=AsyncMock(return_value={"success": success, "jurisdiction": jurisdiction}),
    )


def _no_archive():
    return patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=AsyncMock(return_value=True))


async def _pipeline(jurisdiction="ut", config=CFG):
    with patch("ddp_sync.pipelines.openstates_scrape._check_sustained_blocking", new=AsyncMock()), patch(
        "ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()
    ):
        return await run_state_pipeline(jurisdiction, config)


# ── through run_state_pipeline ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_successful_scrape_sends_one_signal_for_any_state(settings, redis, send):
    # va is on no list at all: the broker, not ddp-sync, decides which states have scorecards
    with _scrape(True, "va"), _no_archive():
        result = await _pipeline("va")
    assert result["success"] is True
    send.assert_awaited_once_with("va")


@pytest.mark.asyncio
async def test_a_failed_scrape_sends_nothing(settings, redis, send):
    with _scrape(False), _no_archive(), pytest.raises(PipelineFailed):
        await _pipeline()
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_signal_goes_before_the_archive(settings, redis):
    order = []

    async def _archive(j, cfg):
        order.append("archive")
        return True

    with _scrape(True), patch(
        "ddp_sync.services.broker_client.request_scorecard_refresh",
        new=AsyncMock(side_effect=lambda j: order.append("signal")),
    ), patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=_archive):
        await _pipeline()
    assert order == ["signal", "archive"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [BrokerClientError("ddp-broker-py rejected the scorecard refresh (401): no"), httpx.ReadTimeout("slow")],
)
async def test_a_broker_failure_never_changes_the_scrape_result(settings, redis, failure):
    with _scrape(True), _no_archive() as archive, patch(
        "ddp_sync.services.broker_client.request_scorecard_refresh", new=AsyncMock(side_effect=failure)
    ):
        result = await _pipeline()
    assert result["success"] is True
    archive.assert_awaited_once()


@pytest.mark.asyncio
async def test_both_signals_are_sent_when_both_are_on(settings, redis, send, tmp_path, monkeypatch):
    sync157_manifest.use(tmp_path, monkeypatch, **sync157_manifest.only("enrollment", "broker_bill_event_signal", ["ut"]))
    both = {**CFG, "broker_bill_event_signal": {"enabled": True}}
    with _scrape(True), _no_archive(), patch(
        "ddp_sync.services.broker_client.request_bill_event_check", new=AsyncMock()
    ) as events:
        await _pipeline(config=both)
    events.assert_awaited_once_with("ut")
    send.assert_awaited_once_with("ut")  # its own debounce key: the first signal does not swallow the second


@pytest.mark.asyncio
async def test_the_hook_itself_never_raises(settings, redis):
    with patch("ddp_sync.services.broker_client.request_scorecard_refresh",
               new=AsyncMock(side_effect=BrokerClientError("rejected (503)"))), \
            patch("ddp_sync.pipelines.openstates_scrape.logger") as log:
        await _maybe_signal_broker_scorecard_refresh("ut", CFG)
    log.warning.assert_called_once()
    assert log.warning.call_args.args[0] == "broker_scorecard_signal_failed"


# ── the switch ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("config", [None, {}, {"broker_scorecard_signal": {"enabled": False}},
                                    {"broker_bill_event_signal": {"enabled": True}}])
async def test_off_unless_its_own_switch_is_on(settings, redis, send, config):
    await _maybe_signal_broker_scorecard_refresh("ut", config)
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_bill_event_switch_does_not_send_this_signal_nor_this_one_that(settings, redis, send):
    with patch("ddp_sync.services.broker_client.request_bill_event_check", new=AsyncMock()) as events:
        await _maybe_signal_broker_bill_events("ut", CFG)
    events.assert_not_awaited()
    send.assert_not_awaited()


def test_the_checked_in_schedule_ships_it_off_inside_the_block_the_pipeline_receives():
    import yaml

    from ddp_sync.scheduler import DEFAULT_CONFIG_PATH

    with open(DEFAULT_CONFIG_PATH) as f:
        block = yaml.safe_load(f)["openstates_scrape"]["broker_scorecard_signal"]
    assert block == {"enabled": False}  # switched on after the broker release that carries BROKER-537 (after A5)


@pytest.mark.asyncio
async def test_usa_is_sent_as_the_brokers_us(settings, redis, send):
    await _maybe_signal_broker_scorecard_refresh("usa", CFG)
    send.assert_awaited_once_with("us")


@pytest.mark.asyncio
async def test_unset_token_skips_loudly(redis, send):
    unset = SimpleNamespace(ddp_broker_api_base="https://broker.test", ddp_broker_api_token="")
    with patch("ddp_sync.config.get_settings", return_value=unset), patch(
        "ddp_sync.pipelines.openstates_scrape.logger"
    ) as log:
        await _maybe_signal_broker_scorecard_refresh("ut", CFG)
    send.assert_not_awaited()
    log.warning.assert_called_once_with("broker_scorecard_signal_not_configured", jurisdiction="ut")


# ── the debounce ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_two_runs_in_quick_succession_send_one_signal(settings, redis, send):
    await _maybe_signal_broker_scorecard_refresh("ut", CFG)
    await _maybe_signal_broker_scorecard_refresh("ut", CFG)
    send.assert_awaited_once_with("ut")
    assert redis.keys == {"ddp:broker_scorecard_signal:ut"}


@pytest.mark.asyncio
async def test_the_debounce_fails_open_when_redis_errors(settings, send):
    broken = SimpleNamespace(_client=SimpleNamespace(set=AsyncMock(side_effect=ConnectionError("down"))))
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=broken):
        await _maybe_signal_broker_scorecard_refresh("ut", CFG)
    send.assert_awaited_once_with("ut")


# ── broker_client.request_scorecard_refresh ─────────────────────────────────────────


def _mock_post(response=None, error=None):
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.post = AsyncMock(return_value=response, side_effect=error)
    return patch.object(broker_client.httpx, "AsyncClient", return_value=client), client


@pytest.mark.asyncio
@pytest.mark.parametrize("status,body", [(202, {"jurisdiction": "UT", "queued": True}),
                                         (200, {"jurisdiction": "NC", "queued": False})])
async def test_client_posts_the_jurisdiction_with_the_service_token(settings, status, body):
    ctx, client = _mock_post(httpx.Response(status, json=body))
    with ctx:
        await request_scorecard_refresh("ut")  # a state the broker does not track (200) is not an error
    client.post.assert_awaited_once_with(
        "https://broker.test/api/scorecards/refresh-after-scrape/",
        headers={"Authorization": "Bearer tok"},
        json={"jurisdiction": "ut"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 400, 401, 404, 503])
async def test_client_raises_with_the_status_on_a_rejection(settings, status):
    ctx, _ = _mock_post(httpx.Response(status, text="nope"))
    with ctx, pytest.raises(BrokerClientError, match=f"scorecard refresh \\({status}\\)"):
        await request_scorecard_refresh("ut")


@pytest.mark.asyncio
async def test_client_raises_when_the_broker_is_unreachable(settings):
    ctx, _ = _mock_post(error=httpx.ConnectError("refused"))
    with ctx, pytest.raises(BrokerClientError, match="unreachable"):
        await request_scorecard_refresh("ut")
