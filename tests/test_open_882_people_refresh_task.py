"""OPEN-882: the weekly people refresh runs as a launched Fargate task (`cloud_people_refresh.py` on `ddp-scrapers-db`);
ddp-sync starts it, waits, reads its PER-STATE JSON from S3, writes the status record, alerts on a failed state and sends
the broker scorecard signal only for the states that worked. Off by default (the local script is the one-release rollback).
The task is in the in-flight record, so a restart still reads its result. ECS, S3, Slack and the broker are fakes; the
in-flight store is tests/conftest.py's autouse in-memory `_no_real_inflight_redis`: nothing here can reach a real Redis,
S3, ECS or database."""

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
from ddp_sync.pipelines import openstates_scrape as oss
from ddp_sync.pipelines import people_refresh_task as prt
from tests.test_openstates_archive_fargate import (
    _FARGATE_CFG,
    FakeEcsClient,
    _run_task_ok,
    _stopped,
)

ARN = "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc"
RUN = "people-refresh-fixed"
KEY = f"working-tier/people-refresh/{RUN}/people-refresh.json"
FARGATE = {**_FARGATE_CFG, "memory_bucket": "bkt", "memory_prefix": "prod", "rds_from_task_secret": True}
CFG = {"people_refresh": {"enabled": True, "on_fargate": True}, "cloud_path": {"fargate": FARGATE},
       "broker_scorecard_signal": {"enabled": True}}
STATES = ["ky", "mi", "us"]
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


def doc(**per_state):
    return {"run_id": RUN, "states": {s: {"status": v} for s, v in per_state.items()}}


@pytest.fixture
def env():
    s3 = FakeS3()
    flow, alert, signal = AsyncMock(), MagicMock(), AsyncMock()
    with patch.object(oss, "_write_flow_status", flow), patch.object(oss, "_alert_scrape_failure", alert), \
            patch("ddp_sync.services.broker_client.request_scorecard_refresh", signal), \
            patch.object(oss, "_send_broker_signal", new=AsyncMock()) as send, \
            patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3), \
            patch("ddp_sync.services.jurisdiction_enrollment.manifest_codes", return_value=list(STATES)), \
            patch.object(prt, "new_run_id", return_value=RUN), \
            patch("boto3.client") as boto:
        yield SimpleNamespace(s3=s3, flow=flow, alert=alert, send=send, boto=boto)


def records():
    return {k: json.loads(v) for k, v in ifj._get_client().hgetall(ifj.INFLIGHT_KEY).items()}


def stage(env, document):
    env.s3.objects[KEY] = json.dumps(document).encode()


def ecs_for(env, describe, run_task=None, ecs_cls=FakeEcsClient):
    ecs = ecs_cls(run_task_response=run_task or _run_task_ok(ARN), describe_responses=describe)
    env.boto.return_value = ecs
    return ecs


def signalled(env):
    return [c.args[1] for c in env.send.await_args_list]  # (name, code, sender)


def status_written(env):
    return env.flow.await_args.args[1]


# --- the launch -------------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_fargate_launches_the_runner_on_the_db_task_definition_with_every_manifest_state(env):
    ecs = ecs_for(env, [_stopped(0)])
    stage(env, doc(ky="done", mi="done", us="done"))
    result = await oss.run_people_refresh_job(CFG)

    (call,) = ecs.run_task_calls
    assert call["taskDefinition"] == "ddp-scrapers-db"  # OPEN-639: the only definition that may carry the login
    override = call["overrides"]["containerOverrides"][0]
    assert override["command"] == [RUN, "ky,mi,us"]
    envs = {e["name"]: e["value"] for e in override["environment"]}
    assert envs["RUNNER_SCRIPT"] == "cloud_people_refresh.py" and envs["RUN_ID"] == RUN
    assert "DATABASE_URL" not in envs  # rds_from_task_secret: the task builds its own login
    assert env.s3.reads == [("bkt", KEY)]
    assert result["success"] is True and result["states_ok"] == STATES
    assert status_written(env)["status"] == "completed" and not env.alert.called


@pytest.mark.asyncio
async def test_without_the_task_secret_switch_the_login_is_passed_at_launch_as_the_archive_does(env):
    cfg = {**CFG, "cloud_path": {"fargate": {**FARGATE, "rds_from_task_secret": False}}}
    ecs = ecs_for(env, [_stopped(0)])
    stage(env, doc(ky="done", mi="done", us="done"))
    with patch.object(prt, "resolve_rds_database_url", return_value=("postgresql://u:pw@rds/db", None)):
        await oss.run_people_refresh_job(cfg)
    envs = {e["name"]: e["value"] for e in ecs.run_task_calls[0]["overrides"]["containerOverrides"][0]["environment"]}
    assert envs["DATABASE_URL"] == "postgresql://u:pw@rds/db"


@pytest.mark.asyncio
@pytest.mark.parametrize("block", [{}, {"enabled": True}, {"on_fargate": False}])
async def test_off_by_default_runs_the_local_script_and_launches_nothing(env, block):
    cfg = {**CFG, "people_refresh": block}
    ecs = ecs_for(env, [])
    with patch.object(oss, "resolve_rds_database_url", return_value=(None, "no rds in this test")):
        result = await oss.run_people_refresh_job(cfg)  # stops at its own first step, before any script
    assert ecs.run_task_calls == [] and records() == {}
    assert result["error"].startswith("cannot resolve an RDS target")


def test_the_shipped_schedule_has_it_off():
    import yaml

    from ddp_sync.scheduler import DEFAULT_CONFIG_PATH

    with open(DEFAULT_CONFIG_PATH) as f:
        block = yaml.safe_load(f)["openstates_scrape"]["people_refresh"]
    assert block["on_fargate"] is False


# --- the per-state result ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_signal_goes_only_to_the_states_that_worked(env):
    ecs_for(env, [_stopped(1)])  # exit 1: some state failed, the result is still there
    stage(env, doc(ky="done", mi="failed", us="done"))
    result = await oss.run_people_refresh_job(CFG)
    assert signalled(env) == ["ky", "us"]  # not mi
    assert result["success"] is False and result["states_failed"] == ["mi"]
    st = status_written(env)
    assert st["status"] == "failed" and st["states_failed"] == ["mi"] and st["states_ok"] == ["ky", "us"]
    env.alert.assert_called_once()
    assert "mi" in env.alert.call_args.args[1] and "ky" not in env.alert.call_args.args[1]


@pytest.mark.asyncio
async def test_a_state_the_result_does_not_list_counts_as_failed(env):
    ecs_for(env, [_stopped(0)])
    stage(env, doc(ky="done", mi="done"))  # us missing
    result = await oss.run_people_refresh_job(CFG)
    assert signalled(env) == ["ky", "mi"] and result["states_failed"] == ["us"]


@pytest.mark.asyncio
async def test_no_signal_when_the_switch_is_off_even_for_good_states(env):
    ecs_for(env, [_stopped(0)])
    stage(env, doc(ky="done", mi="done", us="done"))
    await oss.run_people_refresh_job({**CFG, "broker_scorecard_signal": {"enabled": False}})
    assert signalled(env) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_code", [2, 137])
async def test_a_task_that_left_no_result_fails_every_state_and_signals_none(env, exit_code):
    ecs_for(env, [_stopped(exit_code)])
    stage(env, doc(ky="done", mi="done", us="done"))  # an object from nowhere is not trusted from a task that crashed
    result = await oss.run_people_refresh_job(CFG)
    assert signalled(env) == [] and result["states_failed"] == STATES
    env.alert.assert_called_once()
    assert status_written(env)["status"] == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("staged", ["absent", "other_run", "not_json", "no_states"])
async def test_a_missing_or_wrong_result_fails_every_state(env, staged):
    ecs_for(env, [_stopped(0)])
    if staged == "other_run":
        env.s3.objects[KEY] = json.dumps({**doc(ky="done", mi="done", us="done"), "run_id": "other"}).encode()
    elif staged == "not_json":
        env.s3.objects[KEY] = b"<html>"
    elif staged == "no_states":
        env.s3.objects[KEY] = json.dumps({"run_id": RUN}).encode()
    result = await oss.run_people_refresh_job(CFG)
    assert signalled(env) == [] and result["states_failed"] == STATES and env.alert.call_count == 1


@pytest.mark.asyncio
async def test_no_states_in_the_manifest_is_an_alerted_failure_not_a_quiet_success(env):
    ecs = ecs_for(env, [])
    with patch("ddp_sync.services.jurisdiction_enrollment.manifest_codes", return_value=None):
        result = await oss.run_people_refresh_job(CFG)
    assert result["success"] is False and ecs.run_task_calls == [] and env.alert.call_count == 1
    assert status_written(env)["status"] == "failed"


@pytest.mark.asyncio
async def test_a_launch_failure_alerts_without_the_connection_string_and_leaves_no_record(env):
    env.boto.return_value = FakeEcsClient(run_task_error=RuntimeError("denied for postgresql://load:s3cret@rds.internal:5432/os"))
    result = await oss.run_people_refresh_job(CFG)
    blob = json.dumps([result, status_written(env), [str(a) for a in env.alert.call_args.args]])
    assert "s3cret" not in blob and "rds.internal" not in blob
    assert result["success"] is False and records() == {} and signalled(env) == []


@pytest.mark.asyncio
async def test_an_error_while_waiting_is_reported_without_the_connection_string(env):
    class Throttled(FakeEcsClient):
        def describe_tasks(self, **kwargs):
            raise RuntimeError("throttled; env DATABASE_URL=postgresql://load:s3cret@rds.internal:5432/os")

    ecs_for(env, [], ecs_cls=Throttled)
    result = await oss.run_people_refresh_job(CFG)
    blob = json.dumps([result, status_written(env), [str(a) for a in env.alert.call_args.args]])
    assert "throttled" in blob and "s3cret" not in blob and "rds.internal" not in blob


# --- the in-flight record ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_task_is_recorded_after_launch_before_the_wait_and_cleared_after_the_signal(env):
    seen = {}
    ecs_for(env, [_stopped(0)])
    stage(env, doc(ky="done", mi="done", us="done"))
    real_wait = cst._wait_for_task_stop

    def wait(*a, **k):
        seen["at_wait"] = records()
        return real_wait(*a, **k)

    async def sig(*a, **k):
        seen.setdefault("at_signal", records())

    env.send.side_effect = sig
    with patch.object(cst, "_wait_for_task_stop", wait):
        await oss.run_people_refresh_job(CFG)
    (rec,) = seen["at_wait"].values()
    assert rec["kind"] == "people_refresh" and rec["task_arn"] == ARN and rec["run_id"] == RUN
    assert rec["states"] == STATES and rec["fargate_cfg"]["cluster"] == FARGATE["cluster"]
    assert "postgresql" not in json.dumps(rec)
    assert len(seen["at_signal"]) == 1  # still recorded while the result is acted on
    assert records() == {}


@pytest.mark.asyncio
async def test_a_cancel_while_waiting_keeps_the_record(env):
    started, release = threading.Event(), threading.Event()
    ecs_for(env, [])

    def wait(*a, **k):
        started.set()
        release.wait(2)
        return 0, ""

    with patch.object(cst, "_wait_for_task_stop", wait):
        task = asyncio.create_task(oss.run_people_refresh_job(CFG))
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
    assert [r["kind"] for r in records().values()] == ["people_refresh"]


# --- after a restart --------------------------------------------------------------------------------------------------


def stored(states=None, **over):
    states = states or list(STATES)
    ifj.record_people_refresh_started(RUN, ARN, FARGATE, states)
    rec = {**records()[RUN], **over}
    ifj._get_client().hset(ifj.INFLIGHT_KEY, RUN, json.dumps(rec))
    return rec


@pytest.fixture
def resumed(env):
    with patch.object(prt, "_load_scrape_config", return_value=CFG):
        yield env


@pytest.mark.asyncio
async def test_a_restarted_ddp_sync_waits_for_the_same_task_and_signals_the_good_states_without_a_second_launch(resumed):
    rec = stored()
    ecs = ecs_for(resumed, [_stopped(1), _stopped(1)])  # known to ECS, then the wait
    stage(resumed, doc(ky="failed", mi="done", us="done"))
    await prt.resume_inflight_people_refresh_job(RUN, rec)
    assert ecs.run_task_calls == []
    assert signalled(resumed) == ["mi", "us"] and resumed.alert.call_count == 1
    assert status_written(resumed)["states_failed"] == ["ky"] and records() == {}


@pytest.mark.asyncio
async def test_a_resumed_task_that_crashed_alerts_signals_nothing_and_drops_the_record(resumed):
    rec = stored()
    ecs_for(resumed, [_stopped(2), _stopped(2)])
    await prt.resume_inflight_people_refresh_job(RUN, rec)
    assert signalled(resumed) == [] and resumed.alert.call_count == 1 and records() == {}


@pytest.mark.asyncio
async def test_a_task_ecs_forgot_is_read_straight_from_s3_when_its_result_is_there(resumed):
    rec = stored()
    ecs_for(resumed, [MISSING])
    stage(resumed, doc(ky="done", mi="done", us="done"))
    await prt.resume_inflight_people_refresh_job(RUN, rec)
    assert signalled(resumed) == STATES and records() == {}


@pytest.mark.asyncio
async def test_a_task_ecs_forgot_with_no_result_is_an_alerted_failure_and_drops_the_record(resumed):
    rec = stored()
    ecs_for(resumed, [MISSING])
    await prt.resume_inflight_people_refresh_job(RUN, rec)
    assert signalled(resumed) == [] and resumed.alert.call_count == 1 and records() == {}


@pytest.mark.asyncio
async def test_a_record_missing_its_fields_is_alerted_and_dropped(resumed):
    rec = stored()
    del rec["states"]
    await prt.resume_inflight_people_refresh_job(RUN, rec)
    assert records() == {} and resumed.alert.call_count == 1 and signalled(resumed) == []


@pytest.mark.asyncio
async def test_startup_sends_the_record_to_the_people_refresh_resume_only(env):
    stored()
    with patch.object(prt, "resume_inflight_people_refresh_job", new=AsyncMock(return_value=None)) as resume, \
            patch.object(cst, "resume_inflight_fargate_job", MagicMock(return_value={})) as scrape:
        await asyncio.gather(*cst.reconcile_inflight_fargate_jobs())
    assert [c.args[0] for c in resume.await_args_list] == [RUN]
    scrape.assert_not_called()


def test_the_tests_never_reach_a_real_redis():
    """tests/conftest.py's autouse fixture put an in-memory hash in front of the in-flight store."""
    assert type(ifj._client).__name__ == "_Hash"


# --- the local path still signals every manifest state, as before -----------------------------------------------------


@pytest.mark.asyncio
async def test_the_local_scripts_signal_still_covers_every_manifest_state(env):
    with patch("ddp_sync.services.jurisdiction_enrollment.manifest_codes", return_value=list(STATES)):
        await oss._maybe_signal_broker_scorecard_refresh_after_people(CFG)
    assert signalled(env) == STATES
