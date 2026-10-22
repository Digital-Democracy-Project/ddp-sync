"""SYNC-128: after a successful scrape, ddp-sync asks ddp-broker-py (BROKER-300) to run its
bill-event check for that jurisdiction. Opt-in per jurisdiction, best effort, never changes the
scrape's own result.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from ddp_sync.pipelines.openstates_scrape import (
    _maybe_signal_broker_bill_events,
    _run_scrape,
)
from ddp_sync.services import broker_client
from ddp_sync.services.broker_client import BrokerClientError, request_bill_event_check

CFG = {"broker_bill_event_signal": {"enabled": True, "jurisdictions": ["UT", "us"]}}
SETTINGS = SimpleNamespace(ddp_broker_api_base="https://broker.test", ddp_broker_api_token="tok")


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
    with patch("ddp_sync.services.broker_client.request_bill_event_check", new=AsyncMock()) as mock:
        yield mock


def _scrape(success: bool):
    return patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        new=AsyncMock(return_value={"success": success, "jurisdiction": "ut"}),
    )


def _no_archive_hook():
    return patch("ddp_sync.pipelines.openstates_archive.maybe_trigger_archive_after_scrape", new=AsyncMock())


# ── through _run_scrape ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_successful_scrape_of_an_enrolled_jurisdiction_sends_one_signal(settings, redis, send):
    with _scrape(True), _no_archive_hook():
        result = await _run_scrape("ut", None, "/fake/root", config=CFG)
    assert result["success"] is True
    send.assert_awaited_once_with("ut")


@pytest.mark.asyncio
async def test_failed_scrape_sends_nothing(settings, redis, send):
    with _scrape(False), _no_archive_hook():
        await _run_scrape("ut", None, "/fake/root", config=CFG)
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_goes_before_the_archive_hook(settings, redis):
    order = []
    with _scrape(True), patch(
        "ddp_sync.services.broker_client.request_bill_event_check",
        new=AsyncMock(side_effect=lambda j: order.append("signal")),
    ), patch(
        "ddp_sync.pipelines.openstates_archive.maybe_trigger_archive_after_scrape",
        new=AsyncMock(side_effect=lambda j: order.append("archive")),
    ):
        await _run_scrape("ut", None, "/fake/root", config=CFG)
    assert order == ["signal", "archive"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [BrokerClientError("ddp-broker-py rejected the bill-event check (401): no"), httpx.ReadTimeout("slow")],
)
async def test_a_broker_failure_never_changes_the_scrape_result(settings, redis, failure):
    with _scrape(True), _no_archive_hook() as archive, patch(
        "ddp_sync.services.broker_client.request_bill_event_check", new=AsyncMock(side_effect=failure)
    ):
        result = await _run_scrape("ut", None, "/fake/root", config=CFG)
    assert result["success"] is True
    archive.assert_awaited_once()  # the archive hook still runs


# ── the gate ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config",
    [
        None,
        {},
        {"broker_bill_event_signal": {"enabled": False, "jurisdictions": ["ut"]}},
        {"broker_bill_event_signal": {"enabled": True, "jurisdictions": ["va"]}},
    ],
)
async def test_off_by_default_and_unenrolled_jurisdictions_send_nothing(settings, redis, send, config):
    await _maybe_signal_broker_bill_events("ut", config)
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_usa_is_sent_as_the_brokers_us(settings, redis, send):
    await _maybe_signal_broker_bill_events("usa", CFG)
    send.assert_awaited_once_with("us")


@pytest.mark.asyncio
async def test_unset_token_skips_loudly(redis, send):
    unset = SimpleNamespace(ddp_broker_api_base="https://broker.test", ddp_broker_api_token="")
    with patch("ddp_sync.config.get_settings", return_value=unset), patch(
        "ddp_sync.pipelines.openstates_scrape.logger"
    ) as log:
        await _maybe_signal_broker_bill_events("ut", CFG)
    send.assert_not_awaited()
    log.warning.assert_called_once_with("broker_bill_event_signal_not_configured", jurisdiction="ut")


# ── the debounce ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_two_sessions_in_quick_succession_send_one_signal(settings, redis, send):
    await _maybe_signal_broker_bill_events("usa", CFG)
    await _maybe_signal_broker_bill_events("usa", CFG)
    send.assert_awaited_once_with("us")


@pytest.mark.asyncio
async def test_debounce_fails_open_when_redis_errors(settings, send):
    broken = SimpleNamespace(_client=SimpleNamespace(set=AsyncMock(side_effect=ConnectionError("down"))))
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=broken):
        await _maybe_signal_broker_bill_events("ut", CFG)
    send.assert_awaited_once_with("ut")


# ── broker_client.request_bill_event_check ──────────────────────────────────────────


def _mock_post(response=None, error=None):
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.post = AsyncMock(return_value=response, side_effect=error)
    return patch.object(broker_client.httpx, "AsyncClient", return_value=client), client


@pytest.mark.asyncio
async def test_client_posts_the_jurisdiction_with_the_service_token(settings):
    ctx, client = _mock_post(httpx.Response(202, json={"status": "queued"}))
    with ctx:
        await request_bill_event_check("ut")
    client.post.assert_awaited_once_with(
        "https://broker.test/api/openstates-bill-events/check/",
        headers={"Authorization": "Bearer tok"},
        json={"jurisdiction": "ut"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 503])
async def test_client_raises_with_the_status_on_a_rejection(settings, status):
    ctx, _ = _mock_post(httpx.Response(status, text="nope"))
    with ctx, pytest.raises(BrokerClientError, match=f"\\({status}\\)"):
        await request_bill_event_check("ut")


@pytest.mark.asyncio
async def test_client_raises_when_the_broker_is_unreachable(settings):
    ctx, _ = _mock_post(error=httpx.ConnectError("refused"))
    with ctx, pytest.raises(BrokerClientError, match="unreachable"):
        await request_bill_event_check("ut")
