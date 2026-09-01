"""
Tests for cloud_scrape_trigger.py (OPEN-193).

Uses a small in-memory fake ECS client rather than botocore.stub.Stubber -- same reasoning
test_cloud_collector.py already gives for S3: `run_task`/`describe_tasks` are the exact
methods this module calls, so a fake keeps these tests fast and focused on this module's own
orchestration logic (launch, poll-until-stopped, exit-code handling, load handoff) rather than
re-verifying botocore's request/response validation.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from ddp_sync.pipelines import cloud_scrape_trigger as cst


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
    """Records run_task calls; describe_tasks replays a scripted sequence of responses so a
    test can simulate "still running" polls before "stopped"."""

    def __init__(self, run_task_response=None, run_task_error=None, describe_responses=None):
        self._run_task_response = run_task_response
        self._run_task_error = run_task_error
        self._describe_responses = list(describe_responses or [])
        self.run_task_calls = []
        self.describe_calls = []

    def run_task(self, **kwargs):
        self.run_task_calls.append(kwargs)
        if self._run_task_error:
            raise self._run_task_error
        return self._run_task_response

    def describe_tasks(self, **kwargs):
        self.describe_calls.append(kwargs)
        return self._describe_responses.pop(0)


def _stopped_response(*, exit_code=0, container_name="scraper", reason=""):
    return {
        "tasks": [
            {
                "lastStatus": "STOPPED",
                "reason": reason,
                "containers": [{"name": container_name, "exitCode": exit_code}],
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
    with patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"):
        result = cst.run_cloud_scrape("mi", None, "/fake/root", _fargate_config(), ecs_client=ecs)

    assert result["success"] is False
    assert "run_task_failed" in result["error"]
    assert "no capacity" in result["error"]


def test_run_task_failures_list_fails_cleanly():
    ecs = FakeEcsClient(
        run_task_response={"failures": [{"reason": "RESOURCE:FARGATE"}], "tasks": []}
    )
    with patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"):
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
        patch.dict(os.environ, {"RDS_DATABASE_URL": "postgresql://rds/openstates"}, clear=False),
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


def test_session_arg_reaches_both_collection_command_and_loader_command():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    captured = {}

    def fake_subprocess(cmd, env):
        captured["cmd"] = cmd
        return FakeSubprocessResult(returncode=0)

    with patch.dict(os.environ, {"RDS_DATABASE_URL": "postgresql://rds/openstates"}):
        cst.run_cloud_scrape(
            "va", "session=2027", "/fake/root", _fargate_config(),
            ecs_client=ecs, subprocess_runner=fake_subprocess,
        )

    collect_cmd = ecs.run_task_calls[0]["overrides"]["containerOverrides"][0]["command"]
    assert collect_cmd == ["python3", "cloud_collector.py", "va", "session=2027"]
    assert captured["cmd"][-1] == "session=2027"


def test_nonzero_exit_code_skips_the_load_entirely():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=90)],  # EXIT_DO_NOT_RETRY
    )
    subprocess_runner_called = []

    def fake_subprocess(cmd, env):
        subprocess_runner_called.append(cmd)
        return FakeSubprocessResult(returncode=0)

    with patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert:
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
    with patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"):
        result = cst.run_cloud_scrape("mi", None, "/fake/root", _fargate_config(), ecs_client=ecs)

    assert result["success"] is False
    assert result["error"] == "exit_code_none"


def test_max_wait_exceeded_gives_up_without_looping_forever():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_running_response()],
    )
    with patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(max_wait_seconds=0), ecs_client=ecs,
        )

    assert result["success"] is False
    assert result["error"] == "exit_code_none"
    assert len(ecs.describe_calls) == 1  # gave up on the very first poll, no real sleep needed


# ── load step ────────────────────────────────────────────────────────────────────────────────


def test_missing_rds_database_url_fails_without_invoking_a_subprocess():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    subprocess_calls = []

    def fake_subprocess(cmd, env):
        subprocess_calls.append(cmd)
        return FakeSubprocessResult(returncode=0)

    env_without_rds_url = {k: v for k, v in os.environ.items() if k != "RDS_DATABASE_URL"}
    with (
        patch.dict(os.environ, env_without_rds_url, clear=True),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as mock_alert,
    ):
        result = cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=fake_subprocess,
        )

    assert result["success"] is False
    assert "RDS_DATABASE_URL" in result["error"]
    assert subprocess_calls == []
    mock_alert.assert_called_once()


def test_loader_nonzero_returncode_fails():
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )

    def fake_subprocess(cmd, env):
        return FakeSubprocessResult(returncode=1, stderr=b"could not connect to server")

    with (
        patch.dict(os.environ, {"RDS_DATABASE_URL": "postgresql://rds/openstates"}),
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
    """The whole reason RDS_DATABASE_URL is a separate variable: a pre-set DATABASE_URL in
    this process's own environment (the mac-side local Postgres URL, in production) must
    never leak into the loader's subprocess in place of the RDS one."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    captured = {}

    def fake_subprocess(cmd, env):
        captured["env"] = env
        return FakeSubprocessResult(returncode=0)

    with patch.dict(
        os.environ,
        {
            "DATABASE_URL": "postgresql://local/openstates",
            "RDS_DATABASE_URL": "postgresql://rds/openstates",
        },
    ):
        cst.run_cloud_scrape(
            "mi", None, "/fake/root", _fargate_config(), ecs_client=ecs,
            subprocess_runner=fake_subprocess,
        )

    assert captured["env"]["DATABASE_URL"] == "postgresql://rds/openstates"
