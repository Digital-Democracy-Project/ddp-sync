"""Tests for OPEN-192 (reopened): _run_archive_fargate, the real Fargate-launch path for
cloud_archiver.py, and the use_fargate dispatch in run_archive_jobs()/run_single_archive_job().

cloud_archiver.py itself was built and tested under OPEN-192/238, but nothing in ddp-sync
ever actually launched it -- the scheduler still called run-archive.sh locally, unconditionally.
These tests cover the new dispatch and the launch/wait mechanics this file adds, reusing the
same FakeEcsClient shape test_cloud_scrape_trigger.py already established for the scrape path's
own Fargate tests, so the two suites read the same way.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines import openstates_archive as oa


class FakeEcsClient:
    def __init__(self, run_task_response=None, run_task_error=None, describe_responses=None):
        self._run_task_response = run_task_response
        self._run_task_error = run_task_error
        self._describe_responses = list(describe_responses or [])
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
        return self._describe_responses.pop(0)

    def stop_task(self, **kwargs):
        self.stop_task_calls.append(kwargs)
        return {}


def _run_task_ok(task_arn="arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc"):
    return {"tasks": [{"taskArn": task_arn}], "failures": []}


def _stopped(exit_code=0, container_name="scraper", reason=""):
    return {
        "tasks": [
            {
                "lastStatus": "STOPPED",
                "stoppedReason": "",
                "containers": [{"name": container_name, "exitCode": exit_code, "reason": reason}],
            }
        ]
    }


_FARGATE_CFG = {
    "cluster": "ddp-scrapers",
    "task_definition": "ddp-scrapers",
    "subnets": ["subnet-a"],
    "security_groups": ["sg-a"],
    "container_name": "scraper",
    "max_wait_seconds": 5,
}


# ── dispatch ────────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_use_fargate_key_entirely_absent_still_calls_the_local_wrapper_path(monkeypatch):
    """pm-review: the safety property that matters isn't "use_fargate: false" being read
    correctly -- every existing/deployed config predates this key and won't have it at all.
    This config dict deliberately has no `use_fargate` key whatsoever (not even set to false),
    proving `.get("use_fargate", False)` defaults an absent key the same way a real, currently-
    deployed sync_schedule.yaml would -- adding this file's own import must not flip any live
    archive job onto the new path by accident."""
    with patch.object(oa, "_run_archive", new=AsyncMock(return_value={"success": True})) as mock_local, \
         patch.object(oa, "_run_archive_fargate", new=AsyncMock()) as mock_fargate:
        result = await oa.run_single_archive_job("fl", config={"openstates_root": "/x"})

    assert result == {"success": True}
    mock_local.assert_called_once()
    mock_fargate.assert_not_called()


@pytest.mark.asyncio
async def test_use_fargate_true_calls_the_fargate_path_not_the_local_wrapper():
    with patch.object(oa, "_run_archive", new=AsyncMock()) as mock_local, \
         patch.object(
             oa, "_run_archive_fargate", new=AsyncMock(return_value={"success": True})
         ) as mock_fargate:
        result = await oa.run_single_archive_job("fl", config={"use_fargate": True})

    assert result == {"success": True}
    mock_fargate.assert_called_once()
    mock_local.assert_not_called()


# ── config / precondition failures (never touch ECS) ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_missing_rds_database_url_refuses_without_touching_ecs(monkeypatch):
    monkeypatch.delenv("RDS_DATABASE_URL", raising=False)
    ecs = FakeEcsClient()

    result = await oa._run_archive_fargate("mi", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs)

    assert result["success"] is False
    assert "RDS_DATABASE_URL" in result["error"]
    assert ecs.run_task_calls == []


@pytest.mark.asyncio
async def test_missing_fargate_config_fails_without_touching_ecs(monkeypatch):
    monkeypatch.setenv("RDS_DATABASE_URL", "postgresql://rds/openstates")
    ecs = FakeEcsClient()

    result = await oa._run_archive_fargate("mi", config={}, ecs_client=ecs)

    assert result["success"] is False
    assert result["error"].startswith("config_error")
    assert ecs.run_task_calls == []


# ── launch + wait ───────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_successful_run_passes_runner_script_and_database_url_overrides(monkeypatch):
    monkeypatch.setenv("RDS_DATABASE_URL", "postgresql://rds/openstates")
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=0)])

    result = await oa._run_archive_fargate(
        "mi", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
    )

    assert result == {"success": True, "jurisdiction": "mi", "duration_seconds": result["duration_seconds"]}
    assert result["duration_seconds"] >= 0
    [call] = ecs.run_task_calls
    env = {e["name"]: e["value"] for e in call["overrides"]["containerOverrides"][0]["environment"]}
    assert env["RUNNER_SCRIPT"] == "cloud_archiver.py"
    assert env["DATABASE_URL"] == "postgresql://rds/openstates"
    assert call["overrides"]["containerOverrides"][0]["command"] == ["mi"]


@pytest.mark.asyncio
async def test_run_task_exception_fails_cleanly_and_alerts(monkeypatch):
    monkeypatch.setenv("RDS_DATABASE_URL", "postgresql://rds/openstates")
    ecs = FakeEcsClient(run_task_error=RuntimeError("no capacity"))

    with patch.object(oa, "_alert_archive_failure") as mock_alert:
        result = await oa._run_archive_fargate(
            "mi", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
        )

    assert result["success"] is False
    assert "no capacity" in result["error"]
    mock_alert.assert_called_once()


@pytest.mark.asyncio
async def test_run_task_response_with_failures_list_fails_cleanly_without_raising(monkeypatch):
    """pm-review: a real ECS failure mode distinct from an SDK exception -- run_task can return
    HTTP 200 with a non-empty `failures` list and an empty `tasks` list (capacity, placement,
    or config problems ECS itself rejects before ever starting the task). Confirms
    _launch_archive_fargate_task's failures-list check actually fires -- this was already
    implemented, but had no test proving it before this one."""
    monkeypatch.setenv("RDS_DATABASE_URL", "postgresql://rds/openstates")
    ecs = FakeEcsClient(
        run_task_response={"failures": [{"reason": "RESOURCE:FARGATE"}], "tasks": []}
    )

    with patch.object(oa, "_alert_archive_failure") as mock_alert:
        result = await oa._run_archive_fargate(
            "mi", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
        )

    assert result["success"] is False
    assert "RESOURCE:FARGATE" in result["error"]
    mock_alert.assert_called_once()
    assert ecs.describe_calls == []  # never got as far as waiting on a task that never started


@pytest.mark.asyncio
async def test_run_task_response_with_no_tasks_and_no_failures_fails_cleanly(monkeypatch):
    """The other half of the same real ECS shape: an empty tasks list with no failures entries
    either (no `reason` given at all) must still be treated as "never started", not as a
    successful launch with a None task_arn that then blows up waiting on it."""
    monkeypatch.setenv("RDS_DATABASE_URL", "postgresql://rds/openstates")
    ecs = FakeEcsClient(run_task_response={"failures": [], "tasks": []})

    with patch.object(oa, "_alert_archive_failure") as mock_alert:
        result = await oa._run_archive_fargate(
            "mi", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
        )

    assert result["success"] is False
    assert "no task returned" in result["error"]
    mock_alert.assert_called_once()
    assert ecs.describe_calls == []


@pytest.mark.asyncio
async def test_nonzero_exit_code_fails_cleanly_and_alerts(monkeypatch):
    monkeypatch.setenv("RDS_DATABASE_URL", "postgresql://rds/openstates")
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=1)])

    with patch.object(oa, "_alert_archive_failure") as mock_alert:
        result = await oa._run_archive_fargate(
            "mi", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
        )

    assert result["success"] is False
    assert result["error"] == "exit_code_1"
    mock_alert.assert_called_once()
