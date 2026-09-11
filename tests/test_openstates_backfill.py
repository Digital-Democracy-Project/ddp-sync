"""Tests for OPEN-268: run_backfill_job, the Fargate-launch path for ad-hoc os-text-extract
data-quality commands (reextract/refresh-extraction/recompute-diff-order).

Reuses the same FakeEcsClient shape test_openstates_archive_fargate.py established, plus a
small FakeLogsClient for the CloudWatch output-fetch this file adds on top of that pattern.
"""

from __future__ import annotations

import pytest

from ddp_sync.pipelines import openstates_backfill as ob


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
    """Simulates real CloudWatch pagination: each call consumes the next page of `pages` and
    returns a token that advances only while pages remain -- once exhausted, further calls
    with that same token return no new events and the same token again (matching CloudWatch's
    own "unchanged token means no more data" contract), never re-serving old events.
    """

    def __init__(self, pages=None, events=None, error=None):
        # `events` is a convenience alias for the common one-page case.
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


# ── _build_command ──────────────────────────────────────────────────────────────────────────


def test_build_command_dry_run_no_session():
    assert ob._build_command("recompute-diff-order", "fl", "dry-run", None) == [
        "recompute-diff-order", "fl", "--dry-run",
    ]


def test_build_command_commit_with_session():
    assert ob._build_command("refresh-extraction", "ut", "commit", "2025S2") == [
        "refresh-extraction", "ut", "--session", "2025S2", "--commit",
    ]


# ── validation (never touches ECS) ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unknown_subcommand_refused_without_touching_ecs():
    ecs = FakeEcsClient()
    result = await ob.run_backfill_job("fl", "archive", ecs_client=ecs)
    assert result["success"] is False
    assert "unknown_subcommand" in result["error"]
    assert ecs.run_task_calls == []


@pytest.mark.asyncio
async def test_unknown_mode_refused_without_touching_ecs():
    ecs = FakeEcsClient()
    result = await ob.run_backfill_job("fl", "recompute-diff-order", mode="delete-everything", ecs_client=ecs)
    assert result["success"] is False
    assert "unknown_mode" in result["error"]
    assert ecs.run_task_calls == []


@pytest.mark.asyncio
async def test_unresolvable_rds_credential_refuses_without_touching_ecs(monkeypatch):
    monkeypatch.setattr(
        ob,
        "resolve_rds_database_url",
        lambda: (None, "RDS_CREDENTIALS_SECRET_ARN not set -- refusing to guess which secret to read"),
    )
    ecs = FakeEcsClient()
    result = await ob.run_backfill_job(
        "fl", "recompute-diff-order", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
    )
    assert result["success"] is False
    assert "cannot resolve an RDS target" in result["error"]
    assert ecs.run_task_calls == []


@pytest.mark.asyncio
async def test_missing_fargate_config_fails_without_touching_ecs(monkeypatch):
    monkeypatch.setattr(ob, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    ecs = FakeEcsClient()
    result = await ob.run_backfill_job("fl", "recompute-diff-order", config={}, ecs_client=ecs)
    assert result["success"] is False
    assert result["error"].startswith("config_error")
    assert ecs.run_task_calls == []


# ── launch + wait + log fetch ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_successful_dry_run_passes_runner_script_command_and_database_url(monkeypatch):
    monkeypatch.setattr(ob, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=0)])
    logs = FakeLogsClient(events=[{"message": "fl: [DRY RUN] 7685 bills checked | unchanged=17325 corrected=2712 nulled=1"}])

    result = await ob.run_backfill_job(
        "fl", "recompute-diff-order", mode="dry-run",
        config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs, logs_client=logs,
    )

    assert result["success"] is True
    assert result["jurisdiction"] == "fl"
    assert result["subcommand"] == "recompute-diff-order"
    assert result["mode"] == "dry-run"
    assert "nulled=1" in result["output"]
    assert result["run_id"].startswith("fl-recompute-diff-order-dry-run-")

    [call] = ecs.run_task_calls
    override = call["overrides"]["containerOverrides"][0]
    env = {e["name"]: e["value"] for e in override["environment"]}
    assert env["RUNNER_SCRIPT"] == "cloud_text_extract.py"
    assert env["DATABASE_URL"] == "postgresql://rds/openstates"
    assert override["command"] == ["recompute-diff-order", "fl", "--dry-run"]
    # No MEMORY_BUCKET/MEMORY_PREFIX -- see _launch_backfill_fargate_task's own docstring.
    assert "MEMORY_BUCKET" not in env
    assert "MEMORY_PREFIX" not in env


@pytest.mark.asyncio
async def test_commit_mode_with_session_builds_correct_command(monkeypatch):
    monkeypatch.setattr(ob, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=0)])
    logs = FakeLogsClient(events=[{"message": "ut: committed"}])

    result = await ob.run_backfill_job(
        "ut", "refresh-extraction", mode="commit", session="2025S2",
        config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs, logs_client=logs,
    )

    assert result["success"] is True
    [call] = ecs.run_task_calls
    override = call["overrides"]["containerOverrides"][0]
    assert override["command"] == ["refresh-extraction", "ut", "--session", "2025S2", "--commit"]


@pytest.mark.asyncio
async def test_run_task_exception_fails_cleanly(monkeypatch):
    monkeypatch.setattr(ob, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    ecs = FakeEcsClient(run_task_error=RuntimeError("no capacity"))

    result = await ob.run_backfill_job(
        "fl", "recompute-diff-order", config={"cloud_path": {"fargate": _FARGATE_CFG}}, ecs_client=ecs
    )

    assert result["success"] is False
    assert "no capacity" in result["error"]
    assert ecs.describe_calls == []


@pytest.mark.asyncio
async def test_nonzero_exit_code_reports_failure_with_whatever_output_exists(monkeypatch):
    monkeypatch.setattr(ob, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    ecs = FakeEcsClient(run_task_response=_run_task_ok(), describe_responses=[_stopped(exit_code=1)])
    logs = FakeLogsClient(events=[{"message": "Traceback (most recent call last): ..."}])

    result = await ob.run_backfill_job(
        "fl", "recompute-diff-order", config={"cloud_path": {"fargate": _FARGATE_CFG}},
        ecs_client=ecs, logs_client=logs,
    )

    assert result["success"] is False
    assert result["error"] == "exit_code_1"
    assert "Traceback" in result["output"]


# ── _fetch_task_output ───────────────────────────────────────────────────────────────────────


def test_fetch_task_output_builds_expected_stream_name_and_joins_messages(monkeypatch):
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    logs = FakeLogsClient(events=[{"message": "line one"}, {"message": "line two"}])

    output = ob._fetch_task_output(
        "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc123", _FARGATE_CFG, logs
    )

    assert output == "line one\nline two"
    # One call gets the (only) page of events, a second confirms the stream has stabilized
    # (no new events on top of what's already collected) before returning -- not just one call.
    assert len(logs.get_log_events_calls) == 2
    call = logs.get_log_events_calls[0]
    assert call["logGroupName"] == "/aws/ecs/ddp-scrapers"
    assert call["logStreamName"] == "scraper/scraper/abc123"


def test_fetch_task_output_waits_for_a_later_page_instead_of_returning_the_first_batch(monkeypatch):
    """pm-review, round 1: the original version returned as soon as ANY events came back,
    which could mean returning only early log noise before the actual dry-run summary line
    (a later page) had even been ingested by CloudWatch yet. This proves that no longer
    happens -- a second page arriving after the first is still collected."""
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    logs = FakeLogsClient(
        pages=[
            [{"message": "some early log noise"}],
            [{"message": "fl: [DRY RUN] 7685 bills checked | unchanged=17325 corrected=2712 nulled=1"}],
        ]
    )

    output = ob._fetch_task_output(
        "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc123", _FARGATE_CFG, logs
    )

    assert output == (
        "some early log noise\n"
        "fl: [DRY RUN] 7685 bills checked | unchanged=17325 corrected=2712 nulled=1"
    )


def test_fetch_task_output_retries_then_gives_up_without_raising(monkeypatch):
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)  # don't actually wait in tests
    logs = FakeLogsClient(events=[])  # never has anything

    output = ob._fetch_task_output(
        "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc123", _FARGATE_CFG, logs
    )

    assert output == ""
    assert len(logs.get_log_events_calls) == ob._LOG_FETCH_ATTEMPTS


def test_fetch_task_output_survives_a_raising_logs_client(monkeypatch):
    monkeypatch.setattr(ob.time, "sleep", lambda _s: None)
    logs = FakeLogsClient(error=RuntimeError("ResourceNotFoundException"))

    output = ob._fetch_task_output(
        "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc123", _FARGATE_CFG, logs
    )

    assert output == ""
