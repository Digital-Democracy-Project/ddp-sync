"""Tests for RedisStore.append_run_history/get_run_history (OPEN-22 AC0).

Additive alongside set_flow_status/get_flow_status (untouched, still tested only implicitly
elsewhere) -- these exercise the actual RPUSH/LTRIM/EXPIRE/LRANGE semantics against a minimal
in-memory fake client, since nothing previously tested redis_store.py directly.
"""

from __future__ import annotations

import pytest

from ddp_sync.services.redis_store import RedisStore


class _FakeListRedis:
    """Minimal in-memory stand-in for the subset of redis.asyncio used by
    append_run_history/get_run_history."""

    def __init__(self):
        self._lists: dict[str, list[str]] = {}
        self._ttls: dict[str, int] = {}

    async def rpush(self, key: str, value: str) -> int:
        self._lists.setdefault(key, []).append(value)
        return len(self._lists[key])

    async def ltrim(self, key: str, start: int, end: int) -> None:
        values = self._lists.get(key, [])
        # Redis LTRIM semantics: negative indices count from the end, end is inclusive.
        length = len(values)
        norm_start = start if start >= 0 else max(length + start, 0)
        norm_end = end if end >= 0 else length + end
        self._lists[key] = values[norm_start : norm_end + 1]

    async def expire(self, key: str, ttl: int) -> None:
        self._ttls[key] = ttl

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        values = self._lists.get(key, [])
        length = len(values)
        norm_start = start if start >= 0 else max(length + start, 0)
        norm_end = end if end >= 0 else length + end
        return values[norm_start : norm_end + 1]


@pytest.fixture
def store():
    s = RedisStore()
    s._client = _FakeListRedis()
    return s


@pytest.mark.asyncio
async def test_append_then_get_round_trips(store):
    await store.append_run_history("openstates_secondary_scrapes", "mi", {"success": True})

    history = await store.get_run_history("openstates_secondary_scrapes", "mi")

    assert history == [{"success": True}]


@pytest.mark.asyncio
async def test_history_is_capped_at_max_len_oldest_dropped_first(store):
    for i in range(5):
        await store.append_run_history(
            "openstates_secondary_scrapes", "mi", {"run": i}, max_len=3
        )

    history = await store.get_run_history("openstates_secondary_scrapes", "mi")

    # Oldest two (run 0, run 1) trimmed off; last 3 retained in append order.
    assert history == [{"run": 2}, {"run": 3}, {"run": 4}]


@pytest.mark.asyncio
async def test_different_jurisdictions_do_not_share_history(store):
    await store.append_run_history("openstates_secondary_scrapes", "mi", {"j": "mi"})
    await store.append_run_history("openstates_secondary_scrapes", "va", {"j": "va"})

    assert await store.get_run_history("openstates_secondary_scrapes", "mi") == [{"j": "mi"}]
    assert await store.get_run_history("openstates_secondary_scrapes", "va") == [{"j": "va"}]


@pytest.mark.asyncio
async def test_get_run_history_returns_empty_list_when_redis_unavailable():
    store = RedisStore()
    store._client = None

    assert await store.get_run_history("openstates_secondary_scrapes", "mi") == []


@pytest.mark.asyncio
async def test_append_run_history_no_ops_when_redis_unavailable():
    store = RedisStore()
    store._client = None

    await store.append_run_history("openstates_secondary_scrapes", "mi", {"x": 1})  # must not raise


@pytest.mark.asyncio
async def test_append_run_history_never_raises_on_client_error(store):
    class _BrokenRedis(_FakeListRedis):
        async def rpush(self, key, value):
            raise ConnectionError("redis down")

    store._client = _BrokenRedis()

    await store.append_run_history("openstates_secondary_scrapes", "mi", {"x": 1})  # must not raise
