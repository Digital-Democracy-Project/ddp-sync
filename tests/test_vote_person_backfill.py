"""Tests for run_fargate_script_job, the generalized Fargate-launch path for registered
one-off data-patch scripts (SYNC-74 / OPEN-304 recurrence).

Reuses the same FakeEcsClient/FakeLogsClient shape test_openstates_backfill.py established for
the identical purpose. _fetch_task_output itself is reused directly from openstates_backfill.py
(not re-tested here) -- see that module's own test file for its coverage.
"""

from __future__ import annotations

import pytest
from ddp_sync.pipelines import openstates_backfill as ob
from ddp_sync.pipelines import vote_person_backfill as vpb


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


class FakeLogsClient:
    def __init__(self, pages=None, events=None, error=None):
        self._pages = list(pages) if pages is not None else ([events] if events is not None else [])
        self._error = error
        self._calls_made = 0
        self.get_log_events_calls = []

    def get_log_events(self, **kwargs):
        self.get_log_events_calls.append(kwargs)
        if self._error:
            raise self._error
        page_index = self._calls_made
        self._calls_made += 1
        if page_index < len(self._pages):
            events = self._pages[page_index]
            token = f"token-{page_index + 1}"
        else:
            events = []
            token = f"token-{len(self._pages)}"
        return {"events": events, "nextForwardToken": token}


def _run_task_ok(task_arn="arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc123"):
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


# ── JOBS registry ────────────────────────────────────────────────────────────────────────────


def test_jobs_registry_has_both_registered_scripts():
    assert vpb.JOBS["vote-person-backfill"] == "backfill-vote-person-resolution.py"
    assert vpb.JOBS["open304-lis-identifiers"] == "open304-add-lis-identifiers.py"


# ── _build_command ──────────────────────────────────────────────────────────────────────────


def test_build_command_dry_run():
    assert vpb._build_command("dry-run") == ["--dry-run"]


def test_build_command_commit():
    assert vpb._build_command("commit") == []


# ── validation (never touches ECS) ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unknown_job_refused_without_touching_ecs():
    ecs = FakeEcsClient()
    result = await vpb.run_fargate_script_job(job="not-a-real-job", ecs_client=ecs)
    assert result["success"] is False
    assert "unknown_job" in result["error"]
    assert ecs.run_task_calls == []
    assert "run_id" in result


@pytest.mark.asyncio
async def test_unknown_mode_refused_without_touching_ecs():
    ecs = FakeEcsClient()
    result = await vpb.run_fargate_script_job(
        job="vote-person-backfill", mode="delete-everything", ecs_client=ecs
    )
    assert result["success"] is False
    assert "unknown_mode" in result["error"]
    assert ecs.run_task_calls == []
    assert "run_id" in result


@pytest.mark.asyncio
async def test_caller_supplied_run_id_is_honored_even_on_a_validation_failure():
    ecs = FakeEcsClient()
    result = await vpb.run_fargate_script_job(
        job="vote-person-backfill",
        mode="delete-everything",
        ecs_client=ecs,
        run_id="vote-person-backfill-dry-run-deadbeef0000",
    )
    assert result["run_id"] == "vote-person-backfill-dry-run-deadbeef0000"


@pytest.mark.asyncio
async def test_unresolvable_rds_credential_refuses_without_touching_ecs(monkeypatch):
    monkeypatch.setattr(
        vpb,
        "resolve_rds_database_url",
        lambda: (None, "RDS_CREDENTIALS_SECRET_ARN not set -- refusing to guess which secret to read"),
    )
    ecs = FakeEcsClient()
    result = await vpb.run_fargate_script_job(
        job="vote-person-backfill", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
    )
    assert result["success"] is False
    assert "cannot resolve an RDS target" in result["error"]
    assert ecs.run_task_calls == []
    assert "run_id" in result


@pytest.mark.asyncio
async def test_missing_fargate_config_fails_without_touching_ecs(monkeypatch):
    monkeypatch.setattr(vpb, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    ecs = FakeEcsClient()
    result = await vpb.run_fargate_script_job(job="vote-person-backfill", config={}, ecs_client=ecs)
    assert result["success"] is False
    assert result["error"].startswith("config_error")
    assert ecs.run_task_calls == []
    assert "run_id" in result


# ── launch + wait + log fetch ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_successful_dry_run_passes_runner_script_command_and_database_url(monkeypatch):
    monkeypatch.setattr(vpb, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=0)])
    logs = FakeLogsClient(events=[{"message": "Dry run complete. Would resolve 19,803 records (3,244 still unresolvable)."}])

    result = await vpb.run_fargate_script_job(
        job="vote-person-backfill",
        mode="dry-run",
        config={"cloud_path": {"fargate": _FARGATE_CFG}},
        ecs_client=ecs,
        logs_client=logs,
    )

    assert result["success"] is True
    assert result["mode"] == "dry-run"
    assert result["job"] == "vote-person-backfill"
    assert "19,803" in result["output"]
    assert result["run_id"].startswith("vote-person-backfill-dry-run-")

    [call] = ecs.run_task_calls
    override = call["overrides"]["containerOverrides"][0]
    env = {e["name"]: e["value"] for e in override["environment"]}
    assert env["RUNNER_SCRIPT"] == "backfill-vote-person-resolution.py"
    assert env["DATABASE_URL"] == "postgresql://rds/openstates"
    assert override["command"] == ["--dry-run"]


@pytest.mark.asyncio
async def test_open304_job_passes_its_own_runner_script(monkeypatch):
    """The whole point of generalizing this pipeline (OPEN-304): a different job key launches a
    different RUNNER_SCRIPT, everything else (command building, RDS resolution, wait/log-fetch)
    identical."""
    monkeypatch.setattr(vpb, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=0)])
    logs = FakeLogsClient(events=[{"message": "Dry run complete. Would add 14, 0 already present, 0 person not found, 0 lis conflicts."}])

    result = await vpb.run_fargate_script_job(
        job="open304-lis-identifiers",
        mode="dry-run",
        config={"cloud_path": {"fargate": _FARGATE_CFG}},
        ecs_client=ecs,
        logs_client=logs,
    )

    assert result["success"] is True
    assert result["job"] == "open304-lis-identifiers"
    assert result["run_id"].startswith("open304-lis-identifiers-dry-run-")

    [call] = ecs.run_task_calls
    override = call["overrides"]["containerOverrides"][0]
    env = {e["name"]: e["value"] for e in override["environment"]}
    assert env["RUNNER_SCRIPT"] == "open304-add-lis-identifiers.py"


@pytest.mark.asyncio
async def test_commit_mode_builds_empty_command(monkeypatch):
    monkeypatch.setattr(vpb, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=0)])
    logs = FakeLogsClient(events=[{"message": "Done. Resolved 19,803 records (3,244 still unresolvable)."}])

    result = await vpb.run_fargate_script_job(
        job="vote-person-backfill",
        mode="commit",
        config={"cloud_path": {"fargate": _FARGATE_CFG}},
        ecs_client=ecs,
        logs_client=logs,
    )

    assert result["success"] is True
    [call] = ecs.run_task_calls
    override = call["overrides"]["containerOverrides"][0]
    assert override["command"] == []


@pytest.mark.asyncio
async def test_run_task_exception_fails_cleanly(monkeypatch):
    monkeypatch.setattr(vpb, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    ecs = FakeEcsClient(run_task_error=RuntimeError("no capacity"))

    result = await vpb.run_fargate_script_job(
        job="vote-person-backfill", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
    )

    assert result["success"] is False
    assert "no capacity" in result["error"]
    assert ecs.describe_calls == []


@pytest.mark.asyncio
async def test_nonzero_exit_code_reports_failure_with_whatever_output_exists(monkeypatch):
    monkeypatch.setattr(vpb, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=1)])
    logs = FakeLogsClient(events=[{"message": "Traceback (most recent call last): ..."}])

    result = await vpb.run_fargate_script_job(
        job="vote-person-backfill",
        config={"cloud_path": {"fargate": _FARGATE_CFG}},
        ecs_client=ecs,
        logs_client=logs,
    )

    assert result["success"] is False
    assert result["error"] == "exit_code_1"
    assert "Traceback" in result["output"]
