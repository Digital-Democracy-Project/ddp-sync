"""
Tests for cloud_scrape_trigger.py (OPEN-193).

Uses a small in-memory fake ECS client rather than botocore.stub.Stubber -- same reasoning
test_cloud_collector.py already gives for S3: `run_task`/`describe_tasks` are the exact
methods this module calls, so a fake keeps these tests fast and focused on this module's own
orchestration logic (launch, poll-until-stopped, exit-code handling, load handoff) rather than
re-verifying botocore's request/response validation.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines import cloud_scrape_trigger as cst


class FakeRedisClient:
    """In-memory stand-in for redis.Redis, just the Hash ops inflight_fargate_jobs.py uses."""

    def __init__(self):
        self._hashes: dict[str, dict[str, str]] = {}

    def ping(self):
        return True

    def hset(self, key, field, value):
        self._hashes.setdefault(key, {})[field] = value

    def hdel(self, key, field):
        self._hashes.get(key, {}).pop(field, None)

    def hgetall(self, key):
        return dict(self._hashes.get(key, {}))


def _fargate_config(**overrides):
    cfg = {
        "cluster": "ddp-scrapers-prototype",
        "task_definition": "ddp-scraper-prototype",
        "subnets": ["subnet-abc"],
        "security_groups": ["sg-abc"],
    }
    cfg.update(overrides)
    return {"cloud_path": {"enabled": True, "jurisdictions": ["mi"], "fargate": cfg}}


class FakeEcsClient:
    """Records run_task/stop_task calls; describe_tasks replays a scripted sequence of
    responses so a test can simulate "still running" polls before "stopped"."""

    def __init__(
        self,
        run_task_response=None,
        run_task_error=None,
        describe_responses=None,
        describe_error=None,
    ):
        self._run_task_response = run_task_response
        self._run_task_error = run_task_error
        self._describe_responses = list(describe_responses or [])
        self._describe_error = describe_error
        self.run_task_calls = []
        self.describe_calls = []
        self.stop_task_calls = []

    def run_task(self, **kwargs):
        self.run_task_calls.append(kwargs)
        if self._run_task_error:
            raise self._run_task_error
        return self._run_task_response

    def describe_tasks(self, **kwargs):
        self.describe_calls.append(kwargs)
        if self._describe_error:
            raise self._describe_error
        return self._describe_responses.pop(0)

    def stop_task(self, **kwargs):
        self.stop_task_calls.append(kwargs)
        return {}


def _stopped_response(*, exit_code=0, container_name="scraper", container_reason="", stopped_reason=""):
    return {
        "tasks": [
            {
                "lastStatus": "STOPPED",
                "stoppedReason": stopped_reason,
                "containers": [
                    {"name": container_name, "exitCode": exit_code, "reason": container_reason}
                ],
            }
        ]
    }


def _running_response():
    return {"tasks": [{"lastStatus": "RUNNING", "containers": []}]}


class FakeSubprocessResult:
    def __init__(self, returncode=0, stderr=b""):
        self.returncode = returncode
        self.stderr = stderr


# ── config validation ───────────────────────────────────────────────────────────────────────


def test_missing_fargate_config_fails_without_touching_ecs():
    ecs = FakeEcsClient()
    with patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert:
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", {"cloud_path": {"enabled": True, "jurisdictions": ["mi"]}},
            ecs_client=ecs,
        )

    assert result["success"] is False
    assert result["failure_reason"] == "config_error"
    assert ecs.run_task_calls == []
    mock_alert.assert_called_once()


# ── launch failures ─────────────────────────────────────────────────────────────────────────


def test_run_task_exception_fails_cleanly():
    ecs = FakeEcsClient(run_task_error=RuntimeError("no capacity"))
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = cst.run_cloud_scrape("mi", None, "/fake/root", _fargate_config(), ecs_client=ecs)

    assert result["success"] is False
    assert "run_task_failed" in result["error"]
    assert "no capacity" in result["error"]


def test_run_task_failures_list_fails_cleanly():
    ecs = FakeEcsClient(
        run_task_response={"failures": [{"reason": "RESOURCE:FARGATE"}], "tasks": []}
    )
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = cst.run_cloud_scrape("mi", None, "/fake/root", _fargate_config(), ecs_client=ecs)

    assert result["success"] is False
    assert "RESOURCE:FARGATE" in result["error"]


# ── collection outcome ──────────────────────────────────────────────────────────────────────


def test_collection_polls_until_stopped_then_loads():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_running_response(), _stopped_response(exit_code=0)],
    )
    captured = {}

    def fake_subprocess(cmd, env):
        captured["cmd"] = cmd
        captured["env"] = env
        return FakeSubprocessResult(returncode=0)

    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("time.sleep"),  # the RUNNING->STOPPED poll would otherwise really sleep
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=fake_subprocess,
        )

    assert result["success"] is True
    assert result["jurisdiction"] == "mi"
    assert "cloud_run_id" in result
    assert len(ecs.describe_calls) == 2

    # Command overrides carried the run_id, so the loader can be told exactly what to load.
    run_task_kwargs = ecs.run_task_calls[0]
    override_env = run_task_kwargs["overrides"]["containerOverrides"][0]["environment"]
    run_id = next(e["value"] for e in override_env if e["name"] == "RUN_ID")
    assert result["cloud_run_id"] == run_id

    assert captured["cmd"] == ["python3", "/fake/root/cloud_loader.py", "mi", run_id]
    assert captured["env"]["DATABASE_URL"] == "postgresql://rds/openstates"


def test_preflight_and_load_resolve_the_credential_independently_not_once_and_reused():
    """pm-review: the design's central claim is that a rotation mid-collection is picked up at
    load time rather than carrying forward whatever the preflight check resolved. A test using
    one constant mocked URL for both calls can't distinguish "resolved twice" from "resolved
    once and cached" -- this uses two DISTINCT URLs (simulating a rotation between the
    preflight check and the load step) and asserts the loader actually receives the second
    one, not the first."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    captured = {}

    def fake_subprocess(cmd, env):
        captured["env"] = env
        return FakeSubprocessResult(returncode=0)

    resolve_calls = []

    def fake_resolve():
        resolve_calls.append(len(resolve_calls))
        # First call (preflight) gets the pre-rotation URL; second call (the actual load)
        # gets the post-rotation URL -- simulating a rotation that happened in between.
        if len(resolve_calls) == 1:
            return "postgresql://pre-rotation/openstates", ""
        return "postgresql://post-rotation/openstates", ""

    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", side_effect=fake_resolve
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=fake_subprocess,
        )

    assert result["success"] is True
    assert len(resolve_calls) == 2
    assert captured["env"]["DATABASE_URL"] == "postgresql://post-rotation/openstates"


def test_assign_public_ip_defaults_to_enabled_for_no_nat_public_subnets():
    """OPEN-241: every subnet this project has stood up so far is public-by-design with no
    NAT gateway. DISABLED (the old hardcoded value) left a task's ENI with no route to the
    internet at all, so it could never reach ECR to pull its own image -- confirmed live
    during OPEN-193's canary run, where every attempt failed with a
    ResourceInitializationError timing out trying to reach ECR."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    with patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")):
        cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=lambda cmd, env: FakeSubprocessResult(returncode=0),
        )

    network_cfg = ecs.run_task_calls[0]["networkConfiguration"]["awsvpcConfiguration"]
    assert network_cfg["assignPublicIp"] == "ENABLED"


def test_assign_public_ip_honors_explicit_fargate_config_override():
    """A future task definition that does run in a NAT-backed private subnet must still be
    able to opt back into DISABLED -- this isn't a removal of configurability, just a
    correct default for what's actually deployed today."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    with patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")):
        cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(assign_public_ip="DISABLED"),
            ecs_client=ecs, subprocess_runner=lambda cmd, env: FakeSubprocessResult(returncode=0),
        )

    network_cfg = ecs.run_task_calls[0]["networkConfiguration"]["awsvpcConfiguration"]
    assert network_cfg["assignPublicIp"] == "DISABLED"


def test_session_arg_reaches_both_collection_command_and_loader_command():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    captured = {}

    def fake_subprocess(cmd, env):
        captured["cmd"] = cmd
        return FakeSubprocessResult(returncode=0)

    with patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")):
        cst.run_cloud_scrape(
            "va", "session=2027", "/fake/root", _fargate_config(),
            ecs_client=ecs, subprocess_runner=fake_subprocess,
        )

    # OPEN-242: docker-entrypoint.sh already runs `python3 /app/cloud_collector.py "$@"`, so
    # the override must be just the entrypoint's own args, not another python3/script prefix.
    collect_cmd = ecs.run_task_calls[0]["overrides"]["containerOverrides"][0]["command"]
    assert collect_cmd == ["va", "session=2027"]
    assert captured["cmd"][-1] == "session=2027"


def test_multi_part_session_arg_is_split_into_separate_argv_tokens():
    """SYNC-54: USA's own config passes session_arg as ONE combined string ("119
    chamber=lower"), not separate session/chamber values. The Mac path tolerates this by
    accident (bash word-splits an unquoted variable when run-scrape.sh builds its own
    os-update invocation); nothing here has a shell to do that, so this function must split
    it itself -- found live when USA's cloud-path canary got "session=119 chamber=lower" as
    ONE argv token, which cloud_collector.py's parse_kv_args() then parsed as a single garbage
    key=value pair, and USBillScraper's sitemap filter matched nothing at all."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    captured = {}

    def fake_subprocess(cmd, env):
        captured["cmd"] = cmd
        return FakeSubprocessResult(returncode=0)

    with patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")):
        cst.run_cloud_scrape(
            "usa", "session=119 chamber=lower", "/fake/root", _fargate_config(),
            ecs_client=ecs, subprocess_runner=fake_subprocess,
        )

    collect_cmd = ecs.run_task_calls[0]["overrides"]["containerOverrides"][0]["command"]
    assert collect_cmd == ["usa", "session=119", "chamber=lower"]
    load_cmd = captured["cmd"]
    assert load_cmd[-2:] == ["session=119", "chamber=lower"]


def test_nonzero_exit_code_skips_the_load_entirely():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=90)],  # EXIT_DO_NOT_RETRY
    )
    subprocess_runner_called = []

    def fake_subprocess(cmd, env):
        subprocess_runner_called.append(cmd)
        return FakeSubprocessResult(returncode=0)

    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=fake_subprocess,
        )

    assert result["success"] is False
    assert result["error"] == "exit_code_90"
    assert subprocess_runner_called == []  # never attempted a load for a failed collection
    mock_alert.assert_called_once()


def test_task_stopped_with_no_matching_container_reports_exit_code_none():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(container_name="not-the-scraper")],
    )
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = cst.run_cloud_scrape("mi", None, "/fake/root", _fargate_config(), ecs_client=ecs)

    assert result["success"] is False
    assert result["error"] == "exit_code_none"


def test_max_wait_exceeded_gives_up_without_looping_forever():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_running_response()],
    )
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(max_wait_seconds=0), ecs_client=ecs,
        )

    assert result["success"] is False
    assert result["error"] == "exit_code_none"
    assert len(ecs.describe_calls) == 1  # gave up on the very first poll, no real sleep needed
    # pm-review, round 1: a timed-out run must not leave the ECS task running unbounded.
    assert ecs.stop_task_calls == [{"cluster": "ddp-scrapers-prototype", "task": "arn:task/1",
                                     "reason": "ddp-sync: max_wait_seconds exceeded"}]


def test_describe_tasks_exception_mid_poll_is_caught_and_reported():
    """pm-review, round 1: a throttling error or transient network blip from describe_tasks
    must come back as this function's normal failure dict, not escape uncaught out of
    asyncio.to_thread and crash the scheduler."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_error=RuntimeError("ThrottlingException: Rate exceeded"),
    )
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = cst.run_cloud_scrape("mi", None, "/fake/root", _fargate_config(), ecs_client=ecs)

    assert result["success"] is False
    assert "ThrottlingException" in result["error"]
    mock_alert.assert_called_once()


def test_ecs_client_construction_failure_is_caught_and_reported():
    """Same guarantee, for the other place an unexpected exception could originate:
    boto3.client("ecs") itself, when no ecs_client is injected."""
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("boto3.client", side_effect=RuntimeError("no region configured")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = cst.run_cloud_scrape("mi", None, "/fake/root", _fargate_config())

    assert result["success"] is False
    assert "no region configured" in result["error"]
    mock_alert.assert_called_once()


# ── load step ────────────────────────────────────────────────────────────────────────────────


def test_unresolvable_rds_credential_refuses_before_touching_ecs_at_all():
    """pm-review, round 1: the original version only discovered a missing RDS target after an
    hours-long collection had already run. Now it's the very first thing checked -- neither
    ECS nor the loader subprocess is ever touched. OPEN-260: "missing" now means Secrets
    Manager couldn't resolve a credential, not an unset env var."""
    ecs = FakeEcsClient()
    subprocess_calls = []

    def fake_subprocess(cmd, env):
        subprocess_calls.append(cmd)
        return FakeSubprocessResult(returncode=0)

    with (
        patch(
            "ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url",
            return_value=(None, "RDS_CREDENTIALS_SECRET_ARN not set -- refusing to guess which secret to read"),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=fake_subprocess,
        )

    assert result["success"] is False
    assert "cannot resolve an RDS target" in result["error"]
    assert ecs.run_task_calls == []
    assert subprocess_calls == []
    mock_alert.assert_called_once()


def test_run_load_directly_also_refuses_when_credential_unresolvable():
    """_run_load() keeps its own check too (not just run_cloud_scrape()'s earlier one), so a
    caller that invokes it directly -- including a future retry/resume path -- still gets the
    same guarantee."""
    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url",
        return_value=(None, "could not fetch RDS credential from Secrets Manager: boom"),
    ):
        ok, detail = cst._run_load("mi", None, "run-1", "/fake/root", _fargate_config()["cloud_path"]["fargate"], None)

    assert ok is False
    assert "cannot resolve an RDS target" in detail


def test_loader_nonzero_returncode_fails():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )

    def fake_subprocess(cmd, env):
        return FakeSubprocessResult(returncode=1, stderr=b"could not connect to server")

    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=fake_subprocess,
        )

    assert result["success"] is False
    assert "load_failed" in result["error"]
    assert "could not connect to server" in result["error"]
    mock_alert.assert_called_once()


def test_loader_never_reuses_the_ambient_database_url():
    """The whole reason the loader builds its own env from scratch: a pre-set DATABASE_URL in
    this process's own environment (the mac-side local Postgres URL, in production) must
    never leak into the loader's subprocess in place of the live-resolved RDS credential
    (OPEN-260: resolved from Secrets Manager at call time, not read from an env var either)."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    captured = {}

    def fake_subprocess(cmd, env):
        captured["env"] = env
        return FakeSubprocessResult(returncode=0)

    with (
        patch.dict(os.environ, {"DATABASE_URL": "postgresql://local/openstates"}),
        patch(
            "ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
        ),
    ):
        cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=fake_subprocess,
        )

    assert captured["env"]["DATABASE_URL"] == "postgresql://rds/openstates"


def test_loader_timeout_fails_instead_of_hanging_forever():
    """pm-review, round 1: the loader subprocess had no timeout at all -- a stuck database
    connection could block this orchestration indefinitely. subprocess.TimeoutExpired is just
    another exception to the existing broad catch in _run_load()."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )

    def hanging_subprocess(cmd, env):
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=7200)

    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=hanging_subprocess,
        )

    assert result["success"] is False
    assert "load_failed" in result["error"]
    mock_alert.assert_called_once()


def test_default_subprocess_runner_passes_the_configured_load_timeout():
    """The default runner (used when no subprocess_runner is injected) must actually apply
    cloud_path.fargate.load_timeout_seconds to subprocess.run, not just accept it in config."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = FakeSubprocessResult(returncode=0)
        runner = cst._default_subprocess_runner(load_timeout_s=1234)
        runner(["python3", "cloud_loader.py"], {})

    assert mock_run.call_args.kwargs["timeout"] == 1234


# ── malformed config shapes (independent review, round 2) ─────────────────────────────────


def test_non_dict_fargate_config_returns_clean_failure_instead_of_attributeerror():
    """The exact repro from independent review: cloud_path.fargate present but the wrong
    type (a plausible hand-authored YAML mistake) used to raise AttributeError instead of
    the documented failure dict -- which escaped uncaught all the way past _run_scrape()
    (no handler of its own) into openstates_secondary_scrapes()'s bare asyncio.gather(),
    cancelling every other jurisdiction's in-flight scrape in the same batch."""
    with patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert:
        result = cst.run_cloud_scrape(
            "fl", None, "/tmp", {"cloud_path": {"fargate": "oops-not-a-dict"}}
        )

    assert result["success"] is False
    assert result["failure_reason"] == "config_error"
    assert "must be a mapping" in result["error"]
    mock_alert.assert_called_once()


def test_non_dict_cloud_path_returns_clean_failure_instead_of_attributeerror():
    """Same class of bug, one level up: cloud_path itself the wrong type."""
    with patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert:
        result = cst.run_cloud_scrape("fl", None, "/tmp", {"cloud_path": "also-not-a-dict"})

    assert result["success"] is False
    assert result["failure_reason"] == "config_error"
    assert "must be a mapping" in result["error"]
    mock_alert.assert_called_once()


# ── OPEN-251: in-flight tracking survives a restart ────────────────────────────────────────


def test_task_arn_recorded_before_the_wait_begins_and_cleared_on_success():
    """The whole point: the record must exist while the (possibly hours-long) wait is still
    running, not only after -- a restart during the wait is exactly the case OPEN-251 exists
    for. Cleared again once the run reaches a terminal outcome, since a restart after that
    point has nothing left to reconcile."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/inflight"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    seen_while_waiting = {}

    def fake_wait(task_arn, fargate_cfg, ecs_client):
        seen_while_waiting["record"] = dict(cst.inflight_fargate_jobs.list_inflight())
        return 0, ""

    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.cloud_scrape_trigger._wait_for_task_stop", fake_wait),
        patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()),
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=lambda cmd, env: FakeSubprocessResult(returncode=0),
        )

    assert result["success"] is True
    recorded = seen_while_waiting["record"]
    assert len(recorded) == 1
    run_id, record = next(iter(recorded.items()))
    assert record["task_arn"] == "arn:task/inflight"
    assert record["jurisdiction"] == "mi"
    assert run_id == result["cloud_run_id"]
    # And cleared again once the run is done.
    assert cst.inflight_fargate_jobs.list_inflight() == {}


def test_inflight_record_cleared_even_when_the_load_step_fails():
    """A failed load is still a terminal outcome -- nothing left this side to reconcile."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/2"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
        patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()),
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=lambda cmd, env: FakeSubprocessResult(returncode=1, stderr=b"boom"),
        )

    assert result["success"] is False
    assert cst.inflight_fargate_jobs.list_inflight() == {}


def test_resume_inflight_fargate_job_finishes_a_persisted_task_arn_without_relaunching():
    """The resumed path must never call run_task again -- the ECS task from before the restart
    is still the one and only task for this run_id; resuming only ever waits and loads."""
    ecs = FakeEcsClient(describe_responses=[_stopped_response(exit_code=0)])
    record = {
        "jurisdiction": "mi",
        "session_arg": None,
        "task_arn": "arn:task/resumed",
        "fargate_cfg": _fargate_config()["cloud_path"]["fargate"],
        "openstates_root": "/fake/root",
    }
    captured = {}

    def fake_subprocess(cmd, env):
        captured["cmd"] = cmd
        return FakeSubprocessResult(returncode=0)

    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/openstates", "")),
        patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()),
    ):
        result = cst.resume_inflight_fargate_job(
            "mi-deadbeef0000", record, ecs_client=ecs, subprocess_runner=fake_subprocess
        )

    assert result["success"] is True
    assert result["cloud_run_id"] == "mi-deadbeef0000"
    assert ecs.run_task_calls == []
    assert ecs.describe_calls[0]["tasks"] == ["arn:task/resumed"]
    assert captured["cmd"] == ["python3", "/fake/root/cloud_loader.py", "mi", "mi-deadbeef0000"]


def test_resume_inflight_fargate_job_never_raises_on_a_malformed_record():
    """A record missing a required field must not escape uncaught into the caller -- this runs
    unobserved on a background thread (reconcile_inflight_fargate_jobs's asyncio.to_thread), so
    an uncaught exception here would silently abandon the job all over again. The record is
    still cleared (by run_id, passed in separately -- not read back out of the broken payload)
    and the failure is still alerted, so nothing about this is silent."""
    with (
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
        patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()),
    ):
        cst.inflight_fargate_jobs.record_started(
            "mi-broken", "mi", None, "arn:task/x", {}, "/fake/root"
        )
        result = cst.resume_inflight_fargate_job("mi-broken", {"jurisdiction": "mi"})

    assert result["success"] is False
    assert result["cloud_run_id"] == "mi-broken"
    mock_alert.assert_called_once()
    assert cst.inflight_fargate_jobs.list_inflight() == {}


@pytest.mark.asyncio
async def test_reconcile_inflight_fargate_jobs_resumes_every_persisted_record_concurrently():
    """Each persisted record gets its own background thread -- reconciliation itself must
    never block app startup waiting on one job before starting the next."""
    records = {
        "mi-aaa": {"jurisdiction": "mi", "task_arn": "arn:task/a"},
        "fl-bbb": {"jurisdiction": "fl", "task_arn": "arn:task/b"},
    }
    resumed_with = []

    def fake_resume(run_id, record, **kwargs):
        resumed_with.append((run_id, record))
        return {"success": True}

    with (
        patch.object(cst.inflight_fargate_jobs, "list_inflight", return_value=records),
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resume_inflight_fargate_job", fake_resume),
    ):
        tasks = cst.reconcile_inflight_fargate_jobs()
        assert len(tasks) == 2
        await asyncio.gather(*tasks)

    assert {run_id for run_id, _ in resumed_with} == {"mi-aaa", "fl-bbb"}


@pytest.mark.asyncio
async def test_reconciliation_tasks_are_retained_and_survive_without_external_references():
    """pm-review, round 1: a fire-and-forget asyncio.create_task with nothing else referencing
    it can be garbage-collected mid-run. reconcile_inflight_fargate_jobs() must keep its own
    strong reference so a caller that (like app.py's real lifespan) doesn't hold onto the
    returned list still sees every job through to completion."""
    records = {"mi-aaa": {"jurisdiction": "mi", "task_arn": "arn:task/a"}}
    started = asyncio.Event()
    finished = asyncio.Event()

    async def slow_resume(run_id, record, **kwargs):
        started.set()
        await asyncio.sleep(0.05)
        finished.set()
        return {"success": True}

    def fake_resume(run_id, record, **kwargs):
        # asyncio.to_thread runs this in a worker thread; drive the async fixture from there.
        asyncio.run(slow_resume(run_id, record, **kwargs))

    with (
        patch.object(cst.inflight_fargate_jobs, "list_inflight", return_value=records),
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resume_inflight_fargate_job", fake_resume),
    ):
        cst.reconcile_inflight_fargate_jobs()  # returned list deliberately discarded
        import gc

        gc.collect()
        await asyncio.sleep(0.2)

    assert finished.is_set()


# ── SYNC-59: cloud-owned scraper-completion hook ────────────────────────────────────────────
# The cloud-owned counterpart to test_scraper_completion_hook.py's mac-side coverage -- a
# completed OPEN-193 scrape+RDS-load reaches the Mac Studio's own ddp-sync over WireGuard
# instead of calling anything in-process, since this process has no CAMS/LegBot access.


def _cloud_hook_settings(**overrides) -> SyncSettings:
    defaults = dict(
        legbot_scrape_completion_trigger_enabled=True,
        legbot_scrape_completion_trigger_artifact_types=["bill_summary", "bill_changelog"],
        legbot_scrape_completion_trigger_limit=10000,
        legbot_scrape_completion_trigger_include_concept_statements=True,
        legbot_scrape_completion_trigger_resolution_max_bills=500,
        mac_ddp_sync_base_url="http://10.0.0.8:8001",
        mac_ddp_sync_api_key="mac-secret",
        rds_openstates_api_base="",
        rds_openstates_api_key="",
    )
    defaults.update(overrides)
    return SyncSettings(**defaults)


def _mock_httpx_client(json_body=None, status_code=200):
    """A MagicMock httpx.AsyncClient whose POST returns a canned JSON body."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = json_body if json_body is not None else {"success": True}
    client = MagicMock()
    client.post = AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm, client


class TestExplicitSessionCodeFromArg:
    def test_none_returns_none(self):
        assert cst._explicit_session_code_from_arg(None) is None

    def test_empty_string_returns_none(self):
        assert cst._explicit_session_code_from_arg("") is None

    def test_plain_session_equals_value(self):
        assert cst._explicit_session_code_from_arg("session=2026F") == "2026F"

    def test_multi_part_session_arg_takes_just_the_session_value(self):
        """USA's own config shape (SYNC-54): "session=119 chamber=lower" is one
        combined string -- only the session= token's own value is the session
        code, not the whole string."""
        assert cst._explicit_session_code_from_arg("session=119 chamber=lower") == "119"

    def test_no_session_token_at_all_returns_none(self):
        """The secondary/VA-UT-shaped case: session_arg carries no "session="
        token at all -- genuinely ambiguous, must not be misread as a session
        code itself."""
        assert cst._explicit_session_code_from_arg("chamber=lower") is None


@pytest.mark.asyncio
async def test_cloud_hook_disabled_flag_makes_no_call_at_all():
    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(legbot_scrape_completion_trigger_enabled=False),
    ), patch("ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient") as mock_client_cls:
        await cst._maybe_trigger_legbot_for_cloud_scrape(
            "mi", "session=2026", datetime.now(timezone.utc)
        )

    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_cloud_hook_no_mac_target_configured_makes_no_call():
    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(mac_ddp_sync_base_url=""),
    ), patch("ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient") as mock_client_cls:
        await cst._maybe_trigger_legbot_for_cloud_scrape(
            "mi", "session=2026", datetime.now(timezone.utc)
        )

    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_cloud_hook_explicit_session_arg_skips_ground_truth_resolution():
    """This process already knows exactly which session it just told
    cloud_loader.py to load -- no need to call resolve_touched_sessions at all."""
    cm, client = _mock_httpx_client()
    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(),
    ), patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(),
    ) as mock_resolve, patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient", return_value=cm
    ):
        await cst._maybe_trigger_legbot_for_cloud_scrape(
            "mi", "session=2026F", datetime.now(timezone.utc)
        )

    mock_resolve.assert_not_called()
    client.post.assert_awaited_once()
    call = client.post.await_args
    assert call.args[0] == "http://10.0.0.8:8001/ddp-sync/v1/trigger/scraper-session-legbot"
    assert call.kwargs["json"] == {"jurisdiction_iso2": "MI", "session_code": "2026F"}
    assert call.kwargs["headers"]["Authorization"] == "Bearer mac-secret"
    assert call.kwargs["headers"]["X-DDP-Environment"] == "prod"


@pytest.mark.asyncio
async def test_cloud_hook_ambiguous_session_arg_resolves_ground_truth_against_rds_base():
    """VA/UT-shaped: session_arg=None, so this process genuinely doesn't know which
    session(s) got touched -- must resolve against settings.rds_openstates_api_base,
    never the Mac's own local_openstates_api_base (which never sees RDS-loaded data)."""
    cm, client = _mock_httpx_client()
    since = datetime.now(timezone.utc)
    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(
            rds_openstates_api_base="http://rds-api-v3.internal:8002",
            rds_openstates_api_key="rds-key",
        ),
    ), patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(return_value=["2026S1", "2027"]),
    ) as mock_resolve, patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient", return_value=cm
    ):
        await cst._maybe_trigger_legbot_for_cloud_scrape("va", None, since)

    mock_resolve.assert_awaited_once_with(
        "VA", since=since, max_bills_scanned=500,
        api_base="http://rds-api-v3.internal:8002", api_key="rds-key",
    )
    assert client.post.await_count == 2
    dispatched_sessions = {
        call.kwargs["json"]["session_code"] for call in client.post.await_args_list
    }
    assert dispatched_sessions == {"2026S1", "2027"}


@pytest.mark.asyncio
async def test_cloud_hook_zero_resolved_sessions_makes_no_call():
    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(rds_openstates_api_base="http://rds-api-v3.internal:8002"),
    ), patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(return_value=[]),
    ), patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient"
    ) as mock_client_cls:
        await cst._maybe_trigger_legbot_for_cloud_scrape("va", None, datetime.now(timezone.utc))

    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_cloud_hook_resolution_exception_is_swallowed():
    """Must never affect the scrape job's own already-successful result."""
    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(rds_openstates_api_base="http://rds-api-v3.internal:8002"),
    ), patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ), patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient"
    ) as mock_client_cls:
        await cst._maybe_trigger_legbot_for_cloud_scrape(
            "va", None, datetime.now(timezone.utc)
        )  # must not raise

    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_cloud_hook_unconfigured_rds_base_short_circuits_before_resolution():
    """/pm-review: the known, deliberate partial-rollout gap -- VA/UT-shaped
    jurisdictions with an ambiguous session_arg do not trigger LegBot at all
    until rds_openstates_api_base is configured. Must never even attempt
    resolve_touched_sessions (there is nothing useful for it to query) and
    must not be silently indistinguishable from a real zero-sessions-found
    outcome in the logs."""
    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(rds_openstates_api_base=""),
    ), patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(),
    ) as mock_resolve, patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient"
    ) as mock_client_cls:
        await cst._maybe_trigger_legbot_for_cloud_scrape("va", None, datetime.now(timezone.utc))

    mock_resolve.assert_not_called()
    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_cloud_hook_logs_a_logical_failure_result_distinctly_from_success():
    """/pm-review: trigger_scraper_session_pipeline never raises -- a logical
    failure (trigger_disabled, already_running, etc.) comes back as a normal
    200 with "success": False, and must not be logged identically to a real
    success."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"success": False, "error": "already_running"}
    client = MagicMock()
    client.post = AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)

    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(),
    ), patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient", return_value=cm
    ), patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.logger"
    ) as mock_logger:
        await cst._maybe_trigger_legbot_for_cloud_scrape(
            "mi", "session=2026F", datetime.now(timezone.utc)
        )

    mock_logger.warning.assert_called_once()
    assert mock_logger.warning.call_args.args[0] == (
        "scraper_triggered_legbot_cloud_trigger_not_successful"
    )
    mock_logger.info.assert_not_called()


@pytest.mark.asyncio
async def test_cloud_hook_one_trigger_call_failure_does_not_stop_the_next():
    """Mirrors test_scraper_completion_hook.py's own
    test_one_session_trigger_exception_does_not_stop_the_next for the cloud path."""
    calls = []

    class _RaisingThenOkClient:
        def __init__(self):
            self.post = AsyncMock(side_effect=self._post)

        async def _post(self, url, headers=None, json=None):
            calls.append(json["session_code"])
            if json["session_code"] == "2026S1":
                raise RuntimeError("mac unreachable")
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {"success": True}
            return resp

    fake_client = _RaisingThenOkClient()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=fake_client)
    cm.__aexit__ = AsyncMock(return_value=False)

    with patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.get_settings",
        return_value=_cloud_hook_settings(rds_openstates_api_base="http://rds-api-v3.internal:8002"),
    ), patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(return_value=["2026S1", "2027"]),
    ), patch(
        "ddp_sync.pipelines.cloud_scrape_trigger.httpx.AsyncClient", return_value=cm
    ):
        await cst._maybe_trigger_legbot_for_cloud_scrape(
            "va", None, datetime.now(timezone.utc)
        )  # must not raise

    assert calls == ["2026S1", "2027"]
