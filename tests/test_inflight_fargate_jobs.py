"""Tests for inflight_fargate_jobs.py (OPEN-251)."""

from __future__ import annotations

from unittest.mock import patch

from ddp_sync.pipelines import inflight_fargate_jobs as ifj


class FakeRedisClient:
    def __init__(self, *, raise_on=None):
        self._hashes: dict[str, dict[str, str]] = {}
        self._raise_on = raise_on or set()

    def _maybe_raise(self, op):
        if op in self._raise_on:
            raise RuntimeError(f"simulated {op} failure")

    def hset(self, key, field, value):
        self._maybe_raise("hset")
        self._hashes.setdefault(key, {})[field] = value

    def hdel(self, key, field):
        self._maybe_raise("hdel")
        self._hashes.get(key, {}).pop(field, None)

    def hgetall(self, key):
        self._maybe_raise("hgetall")
        return dict(self._hashes.get(key, {}))


def test_record_started_then_list_inflight_round_trips():
    client = FakeRedisClient()
    with patch.object(ifj, "_client", client):
        ifj.record_started(
            "mi-abc123", "mi", "session=119", "arn:task/1", {"cluster": "x"}, "/root"
        )
        records = ifj.list_inflight()

    assert list(records.keys()) == ["mi-abc123"]
    record = records["mi-abc123"]
    assert record["jurisdiction"] == "mi"
    assert record["session_arg"] == "session=119"
    assert record["task_arn"] == "arn:task/1"
    assert record["fargate_cfg"] == {"cluster": "x"}
    assert record["openstates_root"] == "/root"


def test_clear_removes_only_the_named_run_id():
    client = FakeRedisClient()
    with patch.object(ifj, "_client", client):
        ifj.record_started("run-a", "mi", None, "arn:task/a", {}, "/root")
        ifj.record_started("run-b", "fl", None, "arn:task/b", {}, "/root")
        ifj.clear("run-a")
        records = ifj.list_inflight()

    assert list(records.keys()) == ["run-b"]


def test_every_operation_is_a_no_op_when_redis_is_unavailable():
    """A Redis outage must never raise into the caller -- record_started/clear are called from
    inside run_cloud_scrape()'s own hot path, which must never fail over tracking, and
    list_inflight() must degrade to "nothing to reconcile" rather than crashing startup."""
    with patch.object(ifj, "_get_client", return_value=None):
        ifj.record_started("run-a", "mi", None, "arn:task/a", {}, "/root")  # must not raise
        ifj.clear("run-a")  # must not raise
        assert ifj.list_inflight() == {}


def test_write_failure_is_swallowed_and_logged_not_raised():
    client = FakeRedisClient(raise_on={"hset", "hdel", "hgetall"})
    with patch.object(ifj, "_client", client):
        ifj.record_started("run-a", "mi", None, "arn:task/a", {}, "/root")  # must not raise
        ifj.clear("run-a")  # must not raise
        assert ifj.list_inflight() == {}


def test_unparseable_stored_value_is_skipped_not_fatal():
    """A hand-edited or corrupted Hash field must not take down the whole reconciliation pass
    over one bad entry -- the other, well-formed records are still worth resuming."""
    client = FakeRedisClient()
    client._hashes[ifj.INFLIGHT_KEY] = {"good": '{"jurisdiction": "mi"}', "bad": "not-json"}
    with patch.object(ifj, "_client", client):
        records = ifj.list_inflight()

    assert list(records.keys()) == ["good"]
