"""Tests for RedisStore.set_legbot_task/get_legbot_task/delete_legbot_task/
get_all_legbot_task_ids (SYNC-61).

Same convention as test_redis_store_run_history.py: exercise the actual
SET/GET/DELETE/KEYS semantics against a minimal in-memory fake client, since
nothing previously tested redis_store.py's task-tracking methods directly
(confirmed: set_sync_task/get_sync_task, the closest existing precedent,
have no direct test coverage either).
"""

from __future__ import annotations

import json

import pytest

from ddp_sync.services.redis_store import RedisStore


class _FakeKeyValueRedis:
    """Minimal in-memory stand-in for the subset of redis.asyncio used by
    the legbot task-tracking methods."""

    def __init__(self):
        self._values: dict[str, str] = {}
        self._ttls: dict[str, int] = {}

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self._values[key] = value
        if ex is not None:
            self._ttls[key] = ex

    async def get(self, key: str) -> str | None:
        return self._values.get(key)

    async def delete(self, key: str) -> int:
        existed = key in self._values
        self._values.pop(key, None)
        self._ttls.pop(key, None)
        return 1 if existed else 0

    async def keys(self, pattern: str) -> list[str]:
        # Only ever called here with a trailing "*" -- a plain prefix match
        # is sufficient, no real glob support needed for this fake.
        assert pattern.endswith("*")
        prefix = pattern[:-1]
        return [k for k in self._values if k.startswith(prefix)]


@pytest.fixture
def store():
    s = RedisStore()
    s._client = _FakeKeyValueRedis()
    return s


@pytest.mark.asyncio
async def test_set_then_get_round_trips(store):
    data = {
        "artifact_type": "bill_changelog",
        "bill_openstates_id": "abc",
        "target_artifact_id": 198773,
        "dispatched_at": "2026-09-11T12:00:00+00:00",
    }
    await store.set_legbot_task("task-1", data)

    result = await store.get_legbot_task("task-1")

    assert result == data


@pytest.mark.asyncio
async def test_get_unknown_task_returns_none(store):
    assert await store.get_legbot_task("never-set") is None


@pytest.mark.asyncio
async def test_delete_removes_the_record(store):
    await store.set_legbot_task("task-2", {"bill_openstates_id": "xyz"})

    await store.delete_legbot_task("task-2")

    assert await store.get_legbot_task("task-2") is None


@pytest.mark.asyncio
async def test_delete_unknown_task_does_not_raise(store):
    await store.delete_legbot_task("was-never-there")  # must not raise


@pytest.mark.asyncio
async def test_get_all_task_ids_lists_every_tracked_task(store):
    await store.set_legbot_task("task-a", {"x": 1})
    await store.set_legbot_task("task-b", {"x": 2})

    ids = await store.get_all_legbot_task_ids()

    assert sorted(ids) == ["task-a", "task-b"]


@pytest.mark.asyncio
async def test_get_all_task_ids_excludes_unrelated_keys(store):
    """The prefix scope matters -- this must never pick up an unrelated
    Redis key that happens to also exist (e.g. a sync task, a bill-version
    cache entry)."""
    await store.set_legbot_task("task-a", {"x": 1})
    store._client._values["ddp:sync:task:unrelated"] = json.dumps({"y": 2})

    ids = await store.get_all_legbot_task_ids()

    assert ids == ["task-a"]


@pytest.mark.asyncio
async def test_set_stores_with_the_configured_ttl(store):
    await store.set_legbot_task("task-ttl", {"x": 1})

    key = f"{store.LEGBOT_TASK_PREFIX}task-ttl"
    assert store._client._ttls[key] == store.LEGBOT_TASK_TTL


@pytest.mark.asyncio
async def test_every_method_no_ops_gracefully_when_redis_unavailable():
    """Same graceful-fallback convention as every other method in this
    module -- confirmed here rather than assumed, since these are new."""
    s = RedisStore()
    s._client = None

    await s.set_legbot_task("t", {"x": 1})  # must not raise
    assert await s.get_legbot_task("t") is None
    await s.delete_legbot_task("t")  # must not raise
    assert await s.get_all_legbot_task_ids() == []


@pytest.mark.asyncio
async def test_set_never_raises_on_client_error():
    class _BrokenRedis(_FakeKeyValueRedis):
        async def set(self, *args, **kwargs):
            raise RuntimeError("connection reset")

    s = RedisStore()
    s._client = _BrokenRedis()

    await s.set_legbot_task("t", {"x": 1})  # must not raise


@pytest.mark.asyncio
async def test_get_never_raises_on_client_error():
    class _BrokenRedis(_FakeKeyValueRedis):
        async def get(self, *args, **kwargs):
            raise RuntimeError("connection reset")

    s = RedisStore()
    s._client = _BrokenRedis()

    assert await s.get_legbot_task("t") is None
