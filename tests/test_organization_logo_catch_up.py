"""SYNC-170: the weekly organization logo catch-up on the Mac's scheduler.

The scheduler half is checked with the real APScheduler trigger and a fixed clock, the catch-up function faked; the job
half with `backfill_organization_logos` faked and the alert patched.
"""

from __future__ import annotations

import asyncio
import tempfile
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import _load_from_env
from ddp_sync.pipelines import organization_logo
from ddp_sync.pipelines.organization_logo import catch_up_counts, run_organization_logo_catch_up_job
from ddp_sync.scheduler import UpdateScheduler

_P = "ddp_sync.pipelines.organization_logo"
_ALERT = "ddp_sync.slack_alerts.post_alert"

class FakeRedis:
    """Just what the catch-up lease uses: SET NX EX, GET, EXPIRE, DELETE (async, like redis.asyncio)."""

    def __init__(self):
        self.data = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return None
        self.data[key] = value.encode() if isinstance(value, str) else value
        return True

    async def get(self, key):
        return self.data.get(key)

    async def expire(self, key, ttl):
        return key in self.data

    async def delete(self, key):
        return 1 if self.data.pop(key, None) is not None else 0

    async def eval(self, script, numkeys, key, owner, *args):
        """The two owner-checked scripts of redis_store (compare and act in one step)."""
        from ddp_sync.services.redis_store import _EXTEND_SCRIPT, _RELEASE_SCRIPT

        owned = self.data.get(key) == (owner.encode() if isinstance(owner, str) else owner)
        if script == _RELEASE_SCRIPT:
            return 1 if owned and self.data.pop(key, None) is not None else 0
        if script == _EXTEND_SCRIPT:
            return 1 if owned else 0
        raise AssertionError("unexpected script")


@pytest.fixture(autouse=True)
def fake_redis():
    from ddp_sync.services.redis_store import get_redis_store

    client = FakeRedis()
    with patch.object(get_redis_store(), "_client", client):
        yield client


REPORT = {
    "looked": 1200, "due": 4800, "waited_seconds": 0.0, "stopped": None,
    "outcomes": {"commons": 300, "website": 450, "parent": 90, "none": 340, "failed": 20},
    "sources": {"commons": 300, "website:svg": 150, "website:png": 300},
    "rejected": 12, "skipped": {},
}


def _scheduler(on_host: bool, in_yaml: bool, extra: str = "") -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(f"""
            bill_sync:
              sync_time_utc: "04:00"
            organization_logo_catch_up:
              enabled: {str(in_yaml).lower()}
              sync_day: saturday
              sync_time_utc: "16:00"
            """) + extra)
        path = Path(f.name)
    settings = MagicMock(sync_interval_minutes=30, organization_logo_catch_up_enabled=on_host)
    return UpdateScheduler(settings=settings, config_path=path)


def test_the_host_setting_is_off_unless_the_environment_says_true(monkeypatch):
    monkeypatch.delenv("ORGANIZATION_LOGO_CATCH_UP_ENABLED", raising=False)
    assert _load_from_env()["organization_logo_catch_up_enabled"] is False
    monkeypatch.setenv("ORGANIZATION_LOGO_CATCH_UP_ENABLED", "true")
    assert _load_from_env()["organization_logo_catch_up_enabled"] is True


@pytest.mark.parametrize(("on_host", "in_yaml", "registered"),
                         [(True, True, True), (False, True, False), (True, False, False)])
@pytest.mark.asyncio
async def test_the_job_is_scheduled_only_when_the_host_setting_and_the_yaml_are_both_on(on_host, in_yaml, registered):
    sched = _scheduler(on_host, in_yaml)
    sched.start()
    try:
        assert ("organization_logo_catch_up" in {j.id for j in sched.scheduler.get_jobs()}) is registered
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_it_fires_weekly_on_its_day_and_time_and_calls_the_catch_up_with_the_cap():
    sched = _scheduler(True, True, "  max_organizations_per_run: 1200\n")
    run = AsyncMock(return_value=REPORT)
    with patch(f"{_P}.run_organization_logo_catch_up_job", new=run):  # bound when the job is registered
        sched.start()
    try:
        job = sched.scheduler.get_job("organization_logo_catch_up")
        wednesday = datetime(2026, 11, 18, 9, 0, tzinfo=timezone.utc)
        first = job.trigger.get_next_fire_time(None, wednesday)
        assert first == datetime(2026, 11, 21, 16, 0, tzinfo=timezone.utc)  # the Saturday after, 16:00 UTC
        assert job.trigger.get_next_fire_time(first, first.replace(minute=1)) == datetime(
            2026, 11, 28, 16, 0, tzinfo=timezone.utc)  # and a week later, not sooner
        assert job.max_instances == 1
        await job.func()
        run.assert_awaited_once_with(limit=1200)
    finally:
        sched.stop()


def test_a_cap_below_one_is_refused_at_startup():
    sched = _scheduler(True, True, "  max_organizations_per_run: 0\n")
    with pytest.raises(ValueError, match="max_organizations_per_run"):
        sched.start()


@pytest.mark.asyncio
async def test_the_run_calls_the_backfill_with_the_cap_logs_the_counts_and_alerts_nothing_when_it_finishes():
    with patch(f"{_P}.backfill_organization_logos", new=AsyncMock(return_value=REPORT)) as backfill, \
            patch(_ALERT) as alert, patch(f"{_P}.logger") as log:
        result = await run_organization_logo_catch_up_job(limit=1500)

    kwargs = backfill.await_args.kwargs
    assert kwargs["limit"] == 1500 and kwargs["pause_seconds"] == 1.0
    assert (kwargs["older_than_days"], kwargs["dry_run"], kwargs["organization_ids"], kwargs["force"]) == (None, False, None, False)
    assert kwargs["run_id"] == result["run_id"]
    assert {k: v for k, v in result.items() if k != "run_id"} == REPORT
    alert.assert_not_called()
    done = [c for c in log.info.call_args_list if c.args[0] == "organization_logo_catch_up_done"]
    assert len(done) == 1
    assert done[0].kwargs["summary"] == catch_up_counts(REPORT)


def test_the_counts_carry_each_source_the_parent_none_deferred_and_failed():
    line = catch_up_counts({**REPORT, "outcomes": {**REPORT["outcomes"], "deferred": 3}})
    assert line == ("looked at 1200 of 4800 due; logos found (commons 300, website:png 300, website:svg 150); "
                    "from the parent 90; none 340; deferred 3; not judged 0; failed 20")


@pytest.mark.asyncio
async def test_a_run_that_stops_early_posts_one_alert_with_the_counts():
    stopped = {**REPORT, "stopped": "judge_unavailable", "waited_seconds": 14400.0}
    with patch(f"{_P}.backfill_organization_logos", new=AsyncMock(return_value=stopped)), patch(_ALERT) as alert:
        await run_organization_logo_catch_up_job(limit=1500)

    alert.assert_called_once()
    text = alert.call_args.args[0]
    assert "stopped early (judge_unavailable)" in text
    assert catch_up_counts(stopped) in text
    assert alert.call_args.kwargs["source"] == "organization_logo_catch_up"


@pytest.mark.asyncio
async def test_a_run_that_fails_posts_one_alert_and_does_not_raise():
    with patch(f"{_P}.backfill_organization_logos", new=AsyncMock(side_effect=RuntimeError("broker down"))), \
            patch(_ALERT) as alert:
        result = await run_organization_logo_catch_up_job(limit=1500)

    assert result["error"] == "RuntimeError: broker down"
    alert.assert_called_once()
    assert "failed (RuntimeError: broker down)" in alert.call_args.args[0]


@pytest.mark.asyncio
async def test_a_second_start_while_a_run_is_going_does_nothing(fake_redis):
    release = asyncio.Event()

    async def slow_backfill(**_kw):
        await release.wait()
        return REPORT

    with patch(f"{_P}.backfill_organization_logos", new=AsyncMock(side_effect=slow_backfill)) as backfill, \
            patch(_ALERT):
        first = asyncio.create_task(run_organization_logo_catch_up_job(limit=1500))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        second = await asyncio.wait_for(run_organization_logo_catch_up_job(limit=1500), timeout=5)  # never waits
        assert second["skipped"] == "already_running"
        release.set()
        assert (await first)["looked"] == REPORT["looked"]
    assert backfill.await_count == 1
    assert organization_logo.CATCH_UP_LOCK_KEY not in fake_redis.data  # released
