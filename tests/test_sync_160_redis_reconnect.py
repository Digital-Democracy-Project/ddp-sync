"""SYNC-160: ddp-sync used to try Redis once, at startup. On 2026-11-13 the Mac's ddp-sync came up 49 s
before its Redis container and refused every scraper-triggered LegBot run (503, redis_unavailable)
for 1.5 days, with no alert. `reconnect_until_available` retries until Redis answers, alerts once if
it is still missing after `alert_after`, and clears the pipeline locks as startup would have."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.scraper_triggered_legbot import trigger_scraper_session_pipeline
from ddp_sync.services import redis_store as rs_module
from ddp_sync.services.redis_store import (
    PIPELINE_LOCK_PREFIX,
    RedisStore,
    reconnect_until_available,
)
from tests.test_scraper_triggered_legbot import FakeRedisClient

# SYNC-186: the LegBot gate reads the stand-in manifest (tests/conftest.py), not this host's checkout.
pytestmark = pytest.mark.usefixtures("legbot_stand_in_manifest")


class _FakeRedisWithScan(FakeRedisClient):
    """The trigger's SET-NX/GET/DELETE/EXPIRE plus the SCAN/DELETE that clear_state_locks uses."""

    async def scan_iter(self, match=None):
        prefix = (match or "").rstrip("*")
        for key in list(self.store):
            if key.startswith(prefix):
                yield key

    async def delete(self, *keys):
        return sum(1 for k in keys if self.store.pop(k, None) is not None)


class _Clock:
    def __init__(self, step: float):
        self.now = 0.0
        self.step = step

    def __call__(self) -> float:
        value = self.now
        self.now += self.step
        return value


@pytest.fixture
def store(monkeypatch):
    """A real RedisStore as the process singleton, with no client (Redis was not up at startup)."""
    store = RedisStore()
    monkeypatch.setattr(rs_module, "_redis_store", store)
    return store


def _connect_after(store: RedisStore, failures: int, client) -> AsyncMock:
    """A connect() that fails `failures` times, then publishes `client`."""
    calls = {"n": 0}

    async def connect():
        calls["n"] += 1
        store._client = client if calls["n"] > failures else None

    mock = AsyncMock(side_effect=connect)
    store.connect = mock
    return mock


def _enabled_settings() -> SyncSettings:
    return SyncSettings(
        legbot_scrape_completion_trigger_enabled=True,
        legbot_scrape_completion_trigger_lock_ttl_seconds=14400,
    )


async def _trigger():
    return await trigger_scraper_session_pipeline(
        "PA", "2025-2026", ["bill_summary"], False, 10,
        include_concept_statements=False, retry_failed=False,
    )


@pytest.mark.asyncio
async def test_redis_arriving_late_is_used_without_a_restart(store, monkeypatch):
    """Acceptance 1: refused while Redis is missing, accepted once it comes up, no restart."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings", _enabled_settings
    )
    client = _FakeRedisWithScan()
    # a lock left by a run that died with the previous process (what startup's clear would remove)
    client.store[f"{PIPELINE_LOCK_PREFIX}ma"] = b"old-run"
    connect = _connect_after(store, failures=2, client=client)

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "bills_processed": 3}),
    ):
        assert await _trigger() == {"success": False, "error": "redis_unavailable"}

        await reconnect_until_available(interval=0, alert_after=10_000)

        result = await _trigger()

    assert connect.await_count == 3
    assert store._client is client
    assert result["success"] is True and result["bills_processed"] == 3
    # the stale pipeline lock was cleared on reconnect, as at startup (SYNC-153)
    assert f"{PIPELINE_LOCK_PREFIX}ma" not in client.store


@pytest.mark.asyncio
async def test_one_alert_when_redis_is_still_missing_after_alert_after(store, monkeypatch):
    """Acceptance 2: one operator alert, not one per retry."""
    post_alert = MagicMock(return_value=True)
    monkeypatch.setattr("ddp_sync.slack_alerts.post_alert", post_alert)
    _connect_after(store, failures=10, client=_FakeRedisWithScan())

    await reconnect_until_available(interval=0, alert_after=300, clock=_Clock(step=60))

    assert post_alert.call_count == 1
    kwargs = post_alert.call_args.kwargs
    assert kwargs["source"] == "redis_store.unavailable"
    assert kwargs["dedupe_key"] == "ddp-sync-redis-unavailable"
    assert "LegBot" in post_alert.call_args.args[0]


@pytest.mark.asyncio
async def test_no_alert_when_redis_arrives_within_alert_after(store, monkeypatch):
    """The 2026-11-13 case: Redis 49 s late. It connects on a retry and nobody is alerted."""
    post_alert = MagicMock(return_value=True)
    monkeypatch.setattr("ddp_sync.slack_alerts.post_alert", post_alert)
    _connect_after(store, failures=1, client=_FakeRedisWithScan())

    await reconnect_until_available(interval=0, alert_after=300, clock=_Clock(step=30))

    post_alert.assert_not_called()
    assert store._client is not None


@pytest.mark.asyncio
async def test_a_failing_alert_does_not_stop_the_retries(store, monkeypatch):
    post_alert = MagicMock(side_effect=RuntimeError("route down"))
    monkeypatch.setattr("ddp_sync.slack_alerts.post_alert", post_alert)
    client = _FakeRedisWithScan()
    _connect_after(store, failures=6, client=client)

    await reconnect_until_available(interval=0, alert_after=60, clock=_Clock(step=60))

    assert post_alert.call_count == 1
    assert store._client is client


@pytest.mark.asyncio
async def test_returns_at_once_when_the_startup_connect_succeeded(store):
    store._client = _FakeRedisWithScan()
    connect = _connect_after(store, failures=0, client=store._client)

    await reconnect_until_available(interval=0)

    connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_connect_publishes_a_client_only_after_its_ping_succeeds(store, monkeypatch):
    """A retry runs while the app serves requests: no caller may see a client whose ping has not
    answered, and a failed attempt is closed rather than left open every 30 s."""
    seen_during_ping = []

    class _Client:
        def __init__(self, ok: bool):
            self.ok = ok
            self.closed = False

        async def ping(self):
            seen_during_ping.append(store._client)
            if not self.ok:
                raise ConnectionError("Connection refused")
            return True

        async def close(self):
            self.closed = True

    failing, working = _Client(ok=False), _Client(ok=True)
    clients = iter([failing, working])
    monkeypatch.setattr("redis.asyncio.from_url", lambda *a, **k: next(clients))

    await store.connect()
    assert store._client is None and failing.closed is True

    await store.connect()
    assert store._client is working
    assert seen_during_ping == [None, None]
