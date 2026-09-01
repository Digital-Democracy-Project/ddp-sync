"""Unit tests for SYNC-48's scraper-triggered LegBot dispatch wrapper.

Covers: overlap rejection while a trigger is in flight, safe recovery after
that trigger releases (no permanent omission, no stale lock), lock release
on both success and pipeline-exception paths, the independent enable flag,
and per-(jurisdiction, session) lock scoping.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.scraper_triggered_legbot import (
    _lock_key,
    trigger_scraper_session_pipeline,
)


class FakeRedisClient:
    """Mirrors test_votebot_eval.py's own FakeRedisClient -- same minimal
    SET-NX-EX/GET/DELETE surface, duplicated here rather than shared since
    the original isn't exported from a conftest fixture either."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.set_ex_history: list[tuple[str, int]] = []

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return False
        self.store[key] = value if isinstance(value, bytes) else str(value).encode()
        if ex is not None:
            self.set_ex_history.append((key, ex))
        return True

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0


@pytest.fixture
def fake_redis_store():
    rs = MagicMock()
    rs._client = FakeRedisClient()
    return rs


def _enabled_settings(**overrides) -> SyncSettings:
    defaults = dict(
        session_pipeline_scraper_trigger_enabled=True,
        session_pipeline_scraper_trigger_lock_ttl_seconds=14400,
    )
    defaults.update(overrides)
    return SyncSettings(**defaults)


@pytest.mark.asyncio
async def test_disabled_flag_returns_trigger_disabled_without_touching_redis(
    fake_redis_store, monkeypatch
):
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(session_pipeline_scraper_trigger_enabled=False),
    )
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        result = await trigger_scraper_session_pipeline(
            "FL", "2026E", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )
    assert result == {"success": False, "error": "trigger_disabled"}
    mock_run.assert_not_awaited()
    assert fake_redis_store._client.store == {}


@pytest.mark.asyncio
async def test_redis_unavailable_returns_error(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(),
    )
    unavailable = MagicMock()
    unavailable._client = None
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: unavailable
    )
    result = await trigger_scraper_session_pipeline(
        "FL", "2026E", ["bill_summary"], False, 10,
        include_concept_statements=False, retry_failed=False,
    )
    assert result == {"success": False, "error": "redis_unavailable"}


@pytest.mark.asyncio
async def test_redis_error_during_lock_acquisition_returns_error_not_raise(
    monkeypatch,
):
    """PM review (SYNC-48): the acquire call itself must not raise either --
    a Redis timeout/connection drop there must still return redis_unavailable
    rather than propagating out of a function documented as never raising."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(),
    )
    flaky = MagicMock()
    flaky._client = AsyncMock()
    flaky._client.set = AsyncMock(side_effect=ConnectionError("redis dropped"))
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: flaky
    )

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        result = await trigger_scraper_session_pipeline(
            "FL", "2026E", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )

    assert result == {"success": False, "error": "redis_unavailable"}
    mock_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_overlap_rejected_while_a_trigger_is_in_flight(
    fake_redis_store, monkeypatch
):
    """AC1 (SYNC-48): a duplicate trigger for the same jurisdiction/session
    is rejected outright -- no queueing, no coalescing -- while one is
    already in flight, and never touches run_legbot_pipeline at all."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(),
    )
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    fake_redis_store._client.store[_lock_key("FL", "2026E")] = b"existing-run-id"

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(),
    ) as mock_run:
        result = await trigger_scraper_session_pipeline(
            "FL", "2026E", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )

    assert result["success"] is False
    assert result["error"] == "already_running"
    assert result["current_run_id"] == "existing-run-id"
    mock_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_different_session_is_not_blocked_by_an_unrelated_lock(
    fake_redis_store, monkeypatch
):
    """Lock is scoped per (jurisdiction_iso2, session_code) -- a trigger for
    a different session must proceed normally even while FL/2026E is locked."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(),
    )
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    fake_redis_store._client.store[_lock_key("FL", "2026E")] = b"existing-run-id"

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 0, "results": []}),
    ) as mock_run:
        result = await trigger_scraper_session_pipeline(
            "WA", "2026", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )

    assert result["success"] is True
    mock_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_lock_is_released_after_a_normal_completion_so_the_next_trigger_proceeds(
    fake_redis_store, monkeypatch
):
    """No permanent omission (AC1): once an in-flight run finishes normally,
    its lock is gone -- a later trigger for the same jurisdiction/session is
    not rejected and does reach run_legbot_pipeline."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(),
    )
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 3, "results": []}),
    ) as mock_run:
        first = await trigger_scraper_session_pipeline(
            "FL", "2026E", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )
        assert first["success"] is True
        assert _lock_key("FL", "2026E") not in fake_redis_store._client.store

        second = await trigger_scraper_session_pipeline(
            "FL", "2026E", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )

    assert second["success"] is True
    assert mock_run.await_count == 2


@pytest.mark.asyncio
async def test_lock_is_released_even_when_the_pipeline_call_raises(
    fake_redis_store, monkeypatch
):
    """AC2 (SYNC-48): a run that dies mid-flight (simulated here as
    run_legbot_pipeline raising) must not leave a stale lock that blocks the
    next trigger forever."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(),
    )
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(side_effect=RuntimeError("simulated cams reload mid-dispatch")),
    ):
        result = await trigger_scraper_session_pipeline(
            "FL", "2026E", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )

    assert result["success"] is False
    assert result["error"] == "pipeline_error"
    assert "simulated cams reload" in result["detail"]
    assert _lock_key("FL", "2026E") not in fake_redis_store._client.store


@pytest.mark.asyncio
async def test_lock_ttl_uses_the_configured_setting_not_a_hardcoded_value(
    fake_redis_store, monkeypatch
):
    """Even in the crash case a `finally` block never runs at all (e.g. a
    hard process kill, not just a raised exception), the lock is not
    permanent -- it expires on its own via this TTL. Confirms the TTL
    actually used is the configured setting, not a hardcoded fallback."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(session_pipeline_scraper_trigger_lock_ttl_seconds=999),
    )
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 0, "results": []}),
    ):
        await trigger_scraper_session_pipeline(
            "FL", "2026E", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )

    lock_sets = [
        ex for (key, ex) in fake_redis_store._client.set_ex_history
        if key == _lock_key("FL", "2026E")
    ]
    assert lock_sets == [999]


@pytest.mark.asyncio
async def test_success_result_merges_run_id_with_the_pipeline_result(
    fake_redis_store, monkeypatch
):
    monkeypatch.setattr(
        "ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
        lambda: _enabled_settings(),
    )
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )

    with patch(
        "ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
        new=AsyncMock(return_value={"bills_considered": 7, "bills_processed": 7}),
    ):
        result = await trigger_scraper_session_pipeline(
            "FL", "2026E", ["bill_summary"], False, 10,
            include_concept_statements=False, retry_failed=False,
        )

    assert result["success"] is True
    assert result["bills_considered"] == 7
    assert result["bills_processed"] == 7
    assert "run_id" in result and result["run_id"]


def test_default_enable_flag_is_off():
    """SYNC-48: disabled by default -- no automated caller exists yet, so
    there is nothing today to opt into."""
    assert SyncSettings().session_pipeline_scraper_trigger_enabled is False


def test_default_lock_ttl_is_a_finite_positive_number():
    assert SyncSettings().session_pipeline_scraper_trigger_lock_ttl_seconds > 0
