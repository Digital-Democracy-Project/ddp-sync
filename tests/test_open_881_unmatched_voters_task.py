"""OPEN-881: the after-load unmatched-voter check runs as a launched Fargate task (`cloud_unmatched_voters.py` on
`ddp-scrapers-db`); ddp-sync starts it, waits, reads its JSON from S3, then compares, remembers and alerts exactly as the
local check does. Off by default (the local path is the one-release rollback). The task is in the in-flight record, so a
restart still reads its result. ECS, S3, Redis and Slack are fakes; nothing leaves the process."""

from __future__ import annotations

import asyncio
import io
import json
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.pipelines import cloud_scrape_trigger as cst
from ddp_sync.pipelines import inflight_fargate_jobs as ifj
from ddp_sync.pipelines import unmatched_voter_alert as uva
from ddp_sync.pipelines.openstates_scrape import run_state_pipeline
from tests.test_openstates_archive_fargate import (
    _FARGATE_CFG,
    FakeEcsClient,
    _run_task_ok,
    _stopped,
)
from tests.test_unmatched_voter_alert import _FakeRedis, _state

ARN = "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc"
FARGATE = {**_FARGATE_CFG, "memory_bucket": "bkt", "memory_prefix": "prod", "rds_from_task_secret": True}
CFG = {"unmatched_voter_alert": {"enabled": True, "on_fargate": True}, "cloud_path": {"fargate": FARGATE}}
MISSING = {"tasks": [], "failures": [{"arn": ARN, "reason": "MISSING"}]}


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.reads: list[tuple[str, str]] = []

    def get_object(self, Bucket, Key):
        self.reads.append((Bucket, Key))
        if Key not in self.objects:
            raise RuntimeError(f"NoSuchKey {Key} at db.internal")
        return {"Body": io.BytesIO(self.objects[Key])}


@pytest.fixture
def env():
    """Fake memory Redis (`ddp:unmatched_voters:*`), alert, S3 and ECS. The in-flight store is tests/conftest.py's
    autouse in-memory one (`_no_real_inflight_redis`): none of these tests can reach a real Redis."""
    memory = _FakeRedis()
    s3 = FakeS3()
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=memory)), \
            patch.object(uva, "post_alert", return_value=True) as alert, \
            patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3), \
            patch("boto3.client") as boto:
        yield SimpleNamespace(memory=memory, s3=s3, alert=alert, boto=boto)


def records():
    return {k: json.loads(v) for k, v in ifj._get_client().hgetall(ifj.INFLIGHT_KEY).items()}


def stage_result(env, run_id, state=None):
    env.s3.objects[f"working-tier/me/{run_id}/unmatched-voters.json"] = json.dumps(
        state or _state([("2026", "PUGH of Portland", 812)])
    ).encode()


def ecs_for(env, describe, run_task=None):
    ecs = FakeEcsClient(run_task_response=run_task or _run_task_ok(ARN), describe_responses=describe)
    env.boto.return_value = ecs
    return ecs


def fixed_run_id(run_id="me-unmatched-1"):
    return patch.object(uva, "new_run_id", return_value=run_id)


# --- the launch -----------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_fargate_launches_the_runner_on_the_db_task_definition_and_alerts_from_its_s3_result(env):
    ecs = ecs_for(env, [_stopped(0)])
    stage_result(env, "me-unmatched-1")
    with fixed_run_id():
        await uva.maybe_alert_unmatched_voters("me", CFG)

    (call,) = ecs.run_task_calls
    assert call["taskDefinition"] == "ddp-scrapers-db"  # OPEN-639: the only definition that may carry the login
    override = call["overrides"]["containerOverrides"][0]
    assert override["command"] == ["me", "me-unmatched-1"]
    envs = {e["name"]: e["value"] for e in override["environment"]}
    assert envs["RUNNER_SCRIPT"] == "cloud_unmatched_voters.py" and envs["RUN_ID"] == "me-unmatched-1"
    assert "DATABASE_URL" not in envs  # rds_from_task_secret: the task builds its own login
    assert env.s3.reads == [("bkt", "working-tier/me/me-unmatched-1/unmatched-voters.json")]
    env.alert.assert_called_once()
    text = env.alert.call_args.args[0]
    assert "PUGH of Portland" in text and "812" in text and "postgresql" not in text
    assert json.loads(env.memory.store["ddp:unmatched_voters:me"])["names"] == [["2026", "PUGH of Portland"]]


@pytest.mark.asyncio
async def test_without_the_task_secret_switch_the_login_is_passed_at_launch_as_the_archive_does(env):
    cfg = {**CFG, "cloud_path": {"fargate": {**FARGATE, "rds_from_task_secret": False}}}
    ecs = ecs_for(env, [_stopped(0)])
    stage_result(env, "me-unmatched-1")
    with fixed_run_id(), patch.object(uva, "resolve_rds_database_url", return_value=("postgresql://u:pw@rds/db", None)):
        await uva.maybe_alert_unmatched_voters("me", cfg)
    envs = {e["name"]: e["value"] for e in ecs.run_task_calls[0]["overrides"]["containerOverrides"][0]["environment"]}
    assert envs["DATABASE_URL"] == "postgresql://u:pw@rds/db"


@pytest.mark.asyncio
async def test_off_by_default_runs_the_local_check_and_launches_nothing(env):
    cfg = {"unmatched_voter_alert": {"enabled": True}, "cloud_path": {"fargate": FARGATE}}
    ecs = ecs_for(env, [])
    with patch.object(uva, "read_unmatched", return_value=_state([("2026", "Lewis C", 3)])) as local:
        await uva.maybe_alert_unmatched_voters("me", cfg)
    local.assert_called_once()
    assert ecs.run_task_calls == [] and records() == {}
    env.alert.assert_called_once()


@pytest.mark.asyncio
async def test_the_comparison_with_the_last_run_is_the_same_on_fargate(env):
    env.memory.store["ddp:unmatched_voters:me"] = json.dumps(uva.snapshot(_state([("2026", "PUGH of Portland", 812)])))
    ecs_for(env, [_stopped(0)])
    stage_result(env, "me-unmatched-1")
    with fixed_run_id():
        await uva.maybe_alert_unmatched_voters("me", CFG)
    env.alert.assert_not_called()  # the same name at the same share as last run: silent


# --- the in-flight record ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_task_is_recorded_after_launch_before_the_wait_and_cleared_after_the_alert(env):
    seen = {}
    ecs_for(env, [_stopped(0)])
    stage_result(env, "me-unmatched-1")
    real_wait = cst._wait_for_task_stop

    def wait(*a, **k):
        seen["at_wait"] = records()
        return real_wait(*a, **k)

    env.alert.side_effect = lambda *a, **k: seen.setdefault("at_alert", records()) is not None
    with fixed_run_id(), patch.object(cst, "_wait_for_task_stop", wait):
        await uva.maybe_alert_unmatched_voters("me", CFG)
    (rec,) = seen["at_wait"].values()
    assert rec["kind"] == "unmatched_voters" and rec["task_arn"] == ARN and rec["jurisdiction"] == "me"
    assert rec["run_id"] == "me-unmatched-1" and rec["fargate_cfg"]["cluster"] == FARGATE["cluster"]
    assert "postgresql" not in json.dumps(rec)
    assert len(seen["at_alert"]) == 1  # still recorded while the comparison and alert run
    assert records() == {}  # gone once they have


@pytest.mark.asyncio
async def test_a_cancel_while_waiting_keeps_the_record(env):
    started = threading.Event()
    release = threading.Event()
    ecs_for(env, [])

    def wait(*a, **k):
        started.set()
        release.wait(2)
        return 0, ""

    with fixed_run_id(), patch.object(cst, "_wait_for_task_stop", wait):
        task = asyncio.create_task(uva.maybe_alert_unmatched_voters("me", CFG))
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
    assert [r["kind"] for r in records().values()] == ["unmatched_voters"]


@pytest.mark.asyncio
async def test_a_failed_task_alerts_nothing_remembers_nothing_and_clears_the_record(env):
    ecs_for(env, [_stopped(2)])
    stage_result(env, "me-unmatched-1")  # even a result in S3 is not trusted from a task that exited non-zero
    with fixed_run_id():
        await uva.maybe_alert_unmatched_voters("me", CFG)
    env.alert.assert_not_called()
    assert env.memory.store == {} and records() == {}


@pytest.mark.asyncio
async def test_a_launch_failure_is_logged_without_the_connection_string_and_leaves_no_record(env):
    ecs = FakeEcsClient(run_task_error=RuntimeError("denied for postgresql://load:s3cret@rds.internal:5432/os"))
    env.boto.return_value = ecs
    with fixed_run_id(), patch.object(uva.logger, "warning") as warn:
        await uva.maybe_alert_unmatched_voters("me", CFG)
    logged = json.dumps([[str(a) for a in c.args] + [str(v) for v in c.kwargs.values()] for c in warn.call_args_list])
    assert "unmatched_voter_check_failed" in logged and "s3cret" not in logged and "rds.internal" not in logged
    assert records() == {} and not env.alert.called


@pytest.mark.asyncio
async def test_an_error_while_waiting_is_logged_without_the_connection_string(env):
    class Throttled(FakeEcsClient):
        def describe_tasks(self, **kwargs):
            raise RuntimeError("throttled; env DATABASE_URL=postgresql://load:s3cret@rds.internal:5432/os")

    env.boto.return_value = Throttled(run_task_response=_run_task_ok(ARN))
    with fixed_run_id(), patch.object(uva.logger, "warning") as warn:
        await uva.maybe_alert_unmatched_voters("me", CFG)
    logged = json.dumps([[str(a) for a in c.args] + [str(v) for v in c.kwargs.values()] for c in warn.call_args_list])
    assert "unmatched_voter_check_failed" in logged and "throttled" in logged
    assert "s3cret" not in logged and "rds.internal" not in logged


@pytest.mark.asyncio
@pytest.mark.parametrize("staged", ["absent", "wrong_state", "not_json"])
async def test_a_missing_or_wrong_result_is_a_failed_check_not_an_alert(env, staged):
    ecs_for(env, [_stopped(0)])
    key = "working-tier/me/me-unmatched-1/unmatched-voters.json"
    if staged == "wrong_state":
        env.s3.objects[key] = json.dumps({**_state(), "state": "ky"}).encode()
    elif staged == "not_json":
        env.s3.objects[key] = b"<html>"
    with fixed_run_id():
        await uva.maybe_alert_unmatched_voters("me", CFG)
    env.alert.assert_not_called()
    assert env.memory.store == {} and records() == {}


@pytest.mark.asyncio
async def test_a_broken_fargate_check_never_changes_the_runs_outcome(env):
    from tests.test_unmatched_voter_alert import _scrape

    ecs_for(env, [], run_task=None)
    env.boto.return_value = FakeEcsClient(run_task_error=RuntimeError("capacity"))
    with _scrape(True), patch("ddp_sync.pipelines.openstates_scrape._check_sustained_blocking", new=AsyncMock()), patch(
        "ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()
    ), patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=AsyncMock(return_value=True)):
        result = await run_state_pipeline("me", CFG)
    assert result["success"] is True and records() == {}


# --- after a restart --------------------------------------------------------------------------------------------------


def stored(run_id="me-unmatched-1", **over):
    rec = {"kind": "unmatched_voters", "run_id": run_id, "jurisdiction": "me", "task_arn": ARN,
           "fargate_cfg": dict(FARGATE), **over}
    ifj.record_unmatched_voters_started(run_id, "me", ARN, FARGATE)
    ifj._get_client().hset(ifj.INFLIGHT_KEY, run_id, json.dumps(rec))
    return run_id, rec


@pytest.mark.asyncio
async def test_a_restarted_ddp_sync_waits_for_the_same_task_reads_its_result_and_alerts_without_a_second_launch(env):
    run_id, rec = stored()
    ecs = ecs_for(env, [_stopped(0), _stopped(0)])  # known to ECS, then the wait
    stage_result(env, run_id)
    await uva.resume_inflight_unmatched_voters_job(run_id, rec)
    assert ecs.run_task_calls == []
    env.alert.assert_called_once()
    assert "PUGH of Portland" in env.alert.call_args.args[0]
    assert "ddp:unmatched_voters:me" in env.memory.store and records() == {}


@pytest.mark.asyncio
async def test_a_resumed_check_whose_task_failed_alerts_nothing_and_drops_the_record(env):
    run_id, rec = stored()
    ecs_for(env, [_stopped(1), _stopped(1)])
    await uva.resume_inflight_unmatched_voters_job(run_id, rec)
    env.alert.assert_not_called()
    assert records() == {}


@pytest.mark.asyncio
async def test_a_task_ecs_forgot_is_read_straight_from_s3_when_its_result_is_there(env):
    run_id, rec = stored()
    ecs_for(env, [MISSING])
    stage_result(env, run_id)
    await uva.resume_inflight_unmatched_voters_job(run_id, rec)
    env.alert.assert_called_once()
    assert records() == {}


@pytest.mark.asyncio
async def test_a_task_ecs_forgot_with_no_result_drops_the_record_quietly(env):
    run_id, rec = stored()
    ecs_for(env, [MISSING])
    await uva.resume_inflight_unmatched_voters_job(run_id, rec)
    env.alert.assert_not_called()
    assert records() == {}


@pytest.mark.asyncio
async def test_a_record_missing_its_fields_is_dropped(env):
    run_id, rec = stored()
    del rec["task_arn"]
    await uva.resume_inflight_unmatched_voters_job(run_id, rec)
    assert records() == {} and not env.alert.called


@pytest.mark.asyncio
async def test_startup_sends_the_record_to_the_unmatched_voter_resume_only(env):
    stored()
    with patch.object(uva, "resume_inflight_unmatched_voters_job", new=AsyncMock(return_value=None)) as resume, \
            patch.object(cst, "resume_inflight_fargate_job", MagicMock(return_value={})) as scrape:
        await asyncio.gather(*cst.reconcile_inflight_fargate_jobs())
    assert [c.args[0] for c in resume.await_args_list] == ["me-unmatched-1"]
    scrape.assert_not_called()


def test_the_tests_never_reach_a_real_redis():
    """tests/conftest.py's autouse fixture put an in-memory hash in front of the in-flight store."""
    assert type(ifj._client).__name__ == "_Hash"
