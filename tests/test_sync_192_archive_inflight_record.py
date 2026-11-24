"""SYNC-192: an archive's Fargate task is in the same Redis in-flight record as the load, so a ddp-sync restart during an
archive waits for that task, then runs the follow-ups (LegBot, bill search refresh, embedding) once, instead of dropping
them (found 2026-11-24, OPEN-633 inventory). The ECS client, Redis and every hook are fakes; nothing leaves the process.
"""

from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from ddp_sync.pipelines import cloud_scrape_trigger as cst
from ddp_sync.pipelines import inflight_fargate_jobs as ifj
from ddp_sync.pipelines import openstates_archive as oa
from ddp_sync.services import jurisdiction_enrollment as je
from ddp_sync.services import redis_store
from tests.test_cloud_scrape_trigger import FakeRedisClient, _running_response
from tests.test_openstates_archive_fargate import (
    _FARGATE_CFG,
    FakeEcsClient,
    _run_task_ok,
    _stopped,
)
from tests.test_sync_153_archive_after_scrape_only import FakeRedis

pytestmark = pytest.mark.asyncio

CFG = {"cloud_path": {"fargate": _FARGATE_CFG}, "use_fargate": True}
ARN = "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc"
LOCK_KEY = "ddp:pipeline_lock:pa"
MISSING = {"tasks": [], "failures": [{"arn": ARN, "reason": "MISSING"}]}
HOOKS = ("legbot", "bill_search", "knowledge_base")


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    je._cache.clear()
    je._logged.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({"pa": {"status": "scraping", "archive": {"enabled": True}}}))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    store = FakeRedisClient()
    locks = FakeRedis()
    with (
        patch.object(ifj, "_client", store),
        patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=locks)),
        patch.object(oa, "_load_archive_config_for_scrape_trigger", return_value={"enabled": True}),
        patch.object(oa, "_alert_archive_failure") as alert,
        patch.object(oa, "post_alert") as final_alert,
        patch("ddp_sync.pipelines.openstates_archive.asyncio.sleep", new=AsyncMock()) as sleep,
    ):
        yield SimpleNamespace(store=store, locks=locks, alert=alert, final_alert=final_alert, sleep=sleep)


def records(env):
    return {k: json.loads(v) for k, v in env.store.hgetall(ifj.INFLIGHT_KEY).items()}


def hooks(**side_effects):
    """The three follow-ups as AsyncMocks, patched in for a with-block (an effect may be an exception)."""
    mocks = {n: AsyncMock(side_effect=side_effects.get(n)) for n in HOOKS}
    return mocks, patch.multiple(
        oa,
        _maybe_trigger_legbot_for_archive=mocks["legbot"],
        _maybe_refresh_bill_search=mocks["bill_search"],
        _maybe_embed_knowledge_base=mocks["knowledge_base"],
    )


def stored_record(env, run_id="pa-archive-1", **over):
    rec = {"kind": "archive", "run_id": run_id, "jurisdiction": "pa", "task_arn": ARN, "fargate_cfg": dict(_FARGATE_CFG),
           "archive_started_at": "2026-11-24T10:00:00+00:00", "attempt": 1, "hooks_done": [], **over}
    env.store.hset(ifj.INFLIGHT_KEY, run_id, json.dumps(rec))
    return run_id, rec


def launch(ecs):
    """A normal Fargate archive launch against `ecs`: the RDS login and boto3 are faked."""
    return (patch.object(oa, "resolve_rds_database_url", return_value=("postgresql://rds/x", "")),
            patch.object(oa.boto3, "client", return_value=ecs))


# --- what is recorded, and when it is cleared ------------------------------------------------------------------


async def test_the_record_is_written_after_launch_before_the_wait_and_cleared_after_the_follow_ups(env):
    seen = {}

    def wait(arn, cfg, client, **kw):
        seen["at_wait"] = records(env)
        return 0, ""

    mocks, patcher = hooks()
    mocks["knowledge_base"].side_effect = lambda *a: seen.setdefault("at_embed", records(env))
    rds, boto = launch(FakeEcsClient(run_task_response=_run_task_ok(ARN)))
    with patcher, rds, boto, patch("ddp_sync.pipelines.cloud_scrape_trigger._wait_for_task_stop", wait):
        result = await oa._run_archive_with_hook("pa", config=CFG)

    assert result["success"] is True
    (rec,) = seen["at_wait"].values()
    assert rec["kind"] == "archive" and rec["task_arn"] == ARN and rec["jurisdiction"] == "pa" and rec["attempt"] == 1
    assert rec["run_id"].startswith("pa-archive-") and rec["fargate_cfg"]["cluster"] == _FARGATE_CFG["cluster"]
    assert "postgresql" not in json.dumps(rec)                       # no connection string ever lands in Redis
    (rec2,) = seen["at_embed"].values()                              # still there while the last follow-up runs ...
    assert rec2["hooks_done"] == ["legbot", "bill_search"]           # ... with the finished ones noted
    assert records(env) == {}                                        # ... and gone once all three finished
    assert all(m.await_count == 1 for m in mocks.values())


async def test_a_failed_task_clears_the_record_and_runs_no_follow_up(env):
    mocks, patcher = hooks()
    rds, boto = launch(FakeEcsClient(run_task_response=_run_task_ok(ARN), describe_responses=[_stopped(exit_code=1)]))
    with patcher, rds, boto:
        result = await oa._run_archive_with_hook("pa", config=CFG)
    assert result["success"] is False and records(env) == {}
    assert all(m.await_count == 0 for m in mocks.values())
    env.alert.assert_called_once()                                    # today's per-attempt alert, unchanged


async def test_the_retry_attempt_number_is_what_the_record_gets(env):
    seen = []

    async def attempt(j, cfg):
        seen.append(oa._archive_attempt.get())
        return {"success": False, "error": "x"}

    with patch.object(oa, "run_single_archive_job", attempt):
        assert await oa._archive_with_retry("pa", {"enabled": True}) == (False, 3)
    assert seen == [1, 2, 3]


async def test_a_cancel_while_waiting_keeps_the_record(env):
    """ddp-sync stopping cancels the wait; the record must survive it, or the restart has nothing to resume."""
    started = asyncio.Event()
    loop = asyncio.get_running_loop()

    def wait(*a, **k):
        loop.call_soon_threadsafe(started.set)
        time.sleep(0.3)
        return 0, ""

    rds, boto = launch(FakeEcsClient(run_task_response=_run_task_ok(ARN)))
    with rds, boto, patch("ddp_sync.pipelines.cloud_scrape_trigger._wait_for_task_stop", wait):
        task = asyncio.create_task(oa._run_archive_with_hook("pa", config=CFG))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert [r["kind"] for r in records(env).values()] == ["archive"]


async def test_a_cancel_during_the_follow_ups_keeps_the_record_and_what_finished(env):
    _mocks, patcher = hooks(knowledge_base=asyncio.CancelledError())
    run_id, _ = stored_record(env)
    with patcher, pytest.raises(asyncio.CancelledError):
        await oa._run_archive_followups("pa", oa.datetime.now(oa.timezone.utc), {}, run_id=run_id)
    assert records(env)[run_id]["hooks_done"] == ["legbot", "bill_search"]


async def test_a_failing_follow_up_is_noted_and_does_not_stop_the_others_or_keep_the_record(env):
    mocks, patcher = hooks(bill_search=RuntimeError("search down"))
    run_id, _ = stored_record(env)
    with patcher:
        await oa._run_archive_followups("pa", oa.datetime.now(oa.timezone.utc), {}, run_id=run_id)
    assert all(m.await_count == 1 for m in mocks.values()) and records(env) == {}


async def test_a_local_archive_has_no_record_and_runs_the_follow_ups_as_before(env):
    mocks, patcher = hooks()
    with patcher, patch.object(oa, "_run_archive", new=AsyncMock(return_value={"success": True})):
        await oa._run_archive_with_hook("pa", "/x", config={})
    assert all(m.await_count == 1 for m in mocks.values()) and records(env) == {}


# --- a restart: the resumed path -------------------------------------------------------------------------------


async def test_resume_waits_for_the_recorded_task_launches_nothing_and_runs_each_follow_up_once(env):
    mocks, patcher = hooks()
    run_id, rec = stored_record(env)
    ecs = FakeEcsClient(describe_responses=[_stopped(exit_code=0), _stopped(exit_code=0)])
    with patcher:
        result = await oa.resume_inflight_archive_job(run_id, rec, ecs_client=ecs)
    assert result["success"] is True
    assert ecs.run_task_calls == []                                   # no second archive
    assert all(c["tasks"] == [ARN] for c in ecs.describe_calls)
    assert all(m.await_count == 1 for m in mocks.values())
    assert mocks["legbot"].await_args.args[1].isoformat() == rec["archive_started_at"]   # the original floor, not "now"
    assert records(env) == {}


async def test_a_restart_during_the_last_follow_up_does_not_repeat_the_first_two(env):
    """Process 1 dies (cancelled) inside the embedding; process 2 resumes: LegBot and search are not run again."""
    mocks1, patcher1 = hooks(knowledge_base=asyncio.CancelledError())
    rds, boto = launch(FakeEcsClient(run_task_response=_run_task_ok(ARN), describe_responses=[_stopped(exit_code=0)]))
    with patcher1, rds, boto, pytest.raises(asyncio.CancelledError):
        await oa._run_archive_with_hook("pa", config=CFG)
    assert len(records(env)) == 1

    mocks2, patcher2 = hooks()
    with patcher2, patch.object(oa.boto3, "client", return_value=FakeEcsClient(describe_responses=[_stopped(0)] * 2)):
        await redis_store.clear_state_locks()                         # "startup": locks cleared, then reconcile
        (task,) = cst.reconcile_inflight_fargate_jobs()
        await task
    assert (mocks1["legbot"].await_count, mocks1["bill_search"].await_count) == (1, 1)
    assert (mocks2["legbot"].await_count, mocks2["bill_search"].await_count) == (0, 0)
    assert mocks2["knowledge_base"].await_count == 1
    assert records(env) == {}


async def test_a_resumed_archive_holds_the_state_lock_so_a_second_run_cannot_start(env):
    """Startup cleared the lock; the resume puts it back for the whole run and releases it at the end."""
    held = {}

    async def embed(*a):
        held["during"] = env.locks.data.get(LOCK_KEY)
        held["second_run"] = await redis_store.acquire_state_lock("pa", 60)      # a scheduled or manual run of the state
        held["manual_archive"] = await oa.run_archive_only("pa", {"enabled": True})

    mocks, patcher = hooks()
    mocks["knowledge_base"].side_effect = embed
    run_id, rec = stored_record(env)
    with patcher:
        await redis_store.clear_state_locks()
        assert LOCK_KEY not in env.locks.data
        await oa.resume_inflight_archive_job(run_id, rec, ecs_client=FakeEcsClient(describe_responses=[_stopped(0)] * 2))
    assert held["during"] and held["second_run"] is None
    assert held["manual_archive"] == {"success": False, "jurisdiction": "pa", "skipped": "busy"}
    assert LOCK_KEY not in env.locks.data                                        # released: nothing left wedged


async def test_a_lock_already_held_by_another_run_is_neither_taken_nor_released(env):
    env.locks.data[LOCK_KEY] = "someone-else"
    mocks, patcher = hooks()
    run_id, rec = stored_record(env)
    with patcher:
        result = await oa.resume_inflight_archive_job(run_id, rec, ecs_client=FakeEcsClient(describe_responses=[_stopped(0)] * 2))
    assert result["success"] is True and env.locks.data[LOCK_KEY] == "someone-else"
    assert all(m.await_count == 1 for m in mocks.values())


# --- the stopped or failed task, and the stale record -----------------------------------------------------------


@pytest.mark.parametrize("attempt, runs, sleeps", [(1, 2, [1800, 7200]), (2, 1, [7200]), (3, 0, [])])
async def test_a_failed_resumed_task_alerts_and_continues_the_retries_from_the_next_attempt(env, attempt, runs, sleeps):
    run_id, rec = stored_record(env, attempt=attempt)
    ecs = FakeEcsClient(describe_responses=[_stopped(exit_code=1), _stopped(exit_code=1)])
    mocks, patcher = hooks()
    run = AsyncMock(return_value={"success": False, "error": "x"})
    with patcher, patch.object(oa, "run_single_archive_job", run):
        result = await oa.resume_inflight_archive_job(run_id, rec, ecs_client=ecs)
    assert result["success"] is False and run.await_count == runs
    assert [c.args[0] for c in env.sleep.await_args_list] == sleeps          # today's 30 min / 2 h timing
    env.alert.assert_called_once()                                            # the per-attempt alert for the dead task
    env.final_alert.assert_called_once()                                      # and the usual final one
    assert all(m.await_count == 0 for m in mocks.values()) and records(env) == {}


async def test_a_failed_resumed_task_followed_by_a_good_retry_ends_without_the_final_alert(env):
    run_id, rec = stored_record(env)
    run = AsyncMock(return_value={"success": True})
    with patch.object(oa, "run_single_archive_job", run):
        result = await oa.resume_inflight_archive_job(
            run_id, rec, ecs_client=FakeEcsClient(describe_responses=[_stopped(1), _stopped(1)])
        )
    assert result["success"] is True and run.await_count == 1 and env.final_alert.call_count == 0


async def test_a_stale_record_whose_task_ecs_forgot_is_alerted_and_cleared_not_waited_for(env):
    mocks, patcher = hooks()
    run_id, rec = stored_record(env)
    ecs = FakeEcsClient(describe_responses=[MISSING])                         # one answer only: a second poll would raise
    with patcher:
        result = await oa.resume_inflight_archive_job(run_id, rec, ecs_client=ecs)
    assert result["error"] == "task_unknown" and len(ecs.describe_calls) == 1 and ecs.run_task_calls == []
    assert records(env) == {} and LOCK_KEY not in env.locks.data                # the state is not wedged
    assert all(m.await_count == 0 for m in mocks.values())
    env.alert.assert_called_once()
    assert "no longer knows" in env.alert.call_args.args[1]


@pytest.mark.parametrize("where", ["wait", "config"])
async def test_an_error_before_the_task_is_known_to_have_stopped_keeps_the_record_and_launches_nothing(env, where):
    """pm-review: clearing here would forget a task that may still be running."""
    run_id, rec = stored_record(env)
    ecs = FakeEcsClient(describe_responses=[_running_response()])
    mocks, patcher = hooks()
    boom = {"wait": patch("ddp_sync.pipelines.cloud_scrape_trigger._wait_for_task_stop", side_effect=RuntimeError("throttled")),
            "config": patch.object(oa, "_load_archive_config_for_scrape_trigger", side_effect=RuntimeError("yaml"))}[where]
    with patcher, boom:
        result = await oa.resume_inflight_archive_job(run_id, rec, ecs_client=ecs)
    assert result["success"] is False and run_id in records(env) and ecs.run_task_calls == []
    assert LOCK_KEY not in env.locks.data and all(m.await_count == 0 for m in mocks.values())
    env.alert.assert_called_once()
    assert "kept for the next restart" in env.alert.call_args.args[1]


async def test_a_malformed_record_is_alerted_cleared_and_never_raises(env):
    run_id, rec = stored_record(env)
    del rec["task_arn"]
    result = await oa.resume_inflight_archive_job(run_id, rec, ecs_client=FakeEcsClient())
    assert result["success"] is False and records(env) == {}
    env.alert.assert_called_once()


# --- startup routing, and the gap ---------------------------------------------------------------------------------


async def test_reconcile_sends_an_archive_record_to_the_archive_resume_and_a_scrape_record_to_the_scrape_resume(env):
    stored_record(env, run_id="pa-archive-1")
    env.store.hset(ifj.INFLIGHT_KEY, "ok-1", json.dumps({"jurisdiction": "ok", "task_arn": "t", "fargate_cfg": {},
                                                           "openstates_root": "/r", "session_arg": None}))
    with patch.object(oa, "resume_inflight_archive_job", new=AsyncMock(return_value={})) as archive, \
         patch.object(cst, "resume_inflight_fargate_job", MagicMock(return_value={})) as scrape:
        await asyncio.gather(*cst.reconcile_inflight_fargate_jobs())
    assert [c.args[0] for c in archive.await_args_list] == ["pa-archive-1"]
    assert [c.args[0] for c in scrape.call_args_list] == ["ok-1"]


async def test_a_restart_between_run_task_and_the_record_write_leaves_nothing_to_resume(env):
    """The gap (as the load's): the record is written just after run_task returns. A process that dies in between left
    a running task and no record, so the restart resumes nothing and that run's follow-ups do not run. The state's
    next archive then meets the task's own `_archive_lock` (exit 90) and is retried, as today."""
    mocks, patcher = hooks()
    rds, boto = launch(FakeEcsClient(run_task_response=_run_task_ok(ARN)))
    with patcher, rds, boto, patch.object(ifj, "record_archive_started", side_effect=SystemExit("died here")), \
         pytest.raises(SystemExit):
        await oa._run_archive_with_hook("pa", config=CFG)
    assert records(env) == {}
    assert cst.reconcile_inflight_fargate_jobs() == []
    assert all(m.await_count == 0 for m in mocks.values())


async def test_an_unreachable_redis_changes_nothing_about_a_normal_archive(env):
    mocks, patcher = hooks()
    rds, boto = launch(FakeEcsClient(run_task_response=_run_task_ok(ARN), describe_responses=[_stopped(0)]))
    with patcher, rds, boto, patch.object(ifj, "_get_client", return_value=None):
        result = await oa._run_archive_with_hook("pa", config=CFG)
    assert result["success"] is True and all(m.await_count == 1 for m in mocks.values())
