"""SYNC-119: the nightly Docker cleanup job runs ddp-agents' docker-prune.sh on the Mac only.

The job is exercised against tiny real scripts (success, failure, a hang, a missing file), so the timeout
and the process-group kill are the real ones; only the Slack alert is patched.
"""

from __future__ import annotations

import os
import stat
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.config import _load_from_env
from ddp_sync.pipelines.docker_cleanup import run_docker_cleanup_job
from ddp_sync.scheduler import UpdateScheduler

_P = "ddp_sync.pipelines.docker_cleanup"


def _script(tmp_path: Path, body: str) -> str:
    path = tmp_path / "docker-prune.sh"
    path.write_text("#!/bin/bash\n" + body + "\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


@pytest.mark.asyncio
async def test_a_clean_run_succeeds_and_alerts_nothing(tmp_path):
    with patch(f"{_P}.post_alert") as alert:
        result = await run_docker_cleanup_job(_script(tmp_path, "echo pruned; exit 0"))

    assert result["success"] is True and result["error"] is None
    alert.assert_not_called()


@pytest.mark.asyncio
async def test_a_non_zero_exit_is_a_failure_and_alerts_through_post_alert(tmp_path):
    with patch(f"{_P}.post_alert") as alert:
        result = await run_docker_cleanup_job(_script(tmp_path, "echo 'docker unreachable' >&2; exit 1"))

    assert result["success"] is False
    assert result["error"] == "exit code 1: docker unreachable"
    alert.assert_called_once()
    assert alert.call_args.kwargs["source"] == "docker_cleanup"


@pytest.mark.asyncio
async def test_a_hung_script_and_its_children_are_killed_at_the_timeout_and_alert(tmp_path):
    # Like a wedged `docker` call: the script's own child hangs. The whole group must be gone when the job
    # returns (and so before the run lock is released), not just the script.
    pidfile = tmp_path / "child.pid"
    script = _script(tmp_path, f"sleep 30 & echo $! > {pidfile}; wait")
    with patch(f"{_P}.post_alert") as alert:
        result = await run_docker_cleanup_job(script, timeout_seconds=1)

    assert result["success"] is False
    assert "timed out after 1s" in result["error"]
    assert result["duration_seconds"] < 10
    alert.assert_called_once()
    child = int(pidfile.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(child, 0)


@pytest.mark.asyncio
async def test_a_missing_script_is_a_failure_not_an_exception(tmp_path):
    with patch(f"{_P}.post_alert") as alert:
        result = await run_docker_cleanup_job(str(tmp_path / "nope.sh"))

    assert result["success"] is False
    alert.assert_called_once()


def test_the_host_flag_is_off_unless_the_environment_says_true(monkeypatch):
    monkeypatch.delenv("DOCKER_CLEANUP_ENABLED", raising=False)
    assert _load_from_env()["docker_cleanup_enabled"] is False
    monkeypatch.setenv("DOCKER_CLEANUP_ENABLED", "true")
    assert _load_from_env()["docker_cleanup_enabled"] is True


def _scheduler(on_host: bool, in_yaml: bool) -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(f"""
            bill_sync:
              sync_time_utc: "04:00"
            docker_cleanup:
              enabled: {str(in_yaml).lower()}
              sync_time_utc: "08:00"
            """))
        path = Path(f.name)
    settings = MagicMock(sync_interval_minutes=30, docker_cleanup_enabled=on_host,
                         docker_prune_script="/x/docker-prune.sh")
    return UpdateScheduler(settings=settings, config_path=path)


@pytest.mark.parametrize(("on_host", "in_yaml", "registered"),
                         [(True, True, True), (False, True, False), (True, False, False)])
@pytest.mark.asyncio
async def test_the_job_is_scheduled_only_when_the_host_flag_and_the_yaml_are_both_on(on_host, in_yaml, registered):
    sched = _scheduler(on_host, in_yaml)
    sched.start()
    try:
        assert ("docker_cleanup" in {j.id for j in sched.scheduler.get_jobs()}) is registered
    finally:
        sched.stop()


def _post(on_host: bool):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    settings = MagicMock(docker_cleanup_enabled=on_host, docker_prune_script="/x/docker-prune.sh")
    scheduler = MagicMock(_sync_config={"docker_cleanup": {"timeout_seconds": 60}})
    with patch("ddp_sync.api.routes.triggers.get_settings", return_value=settings), \
            patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch(f"{_P}.run_docker_cleanup_job", new=AsyncMock()) as job:
        resp = TestClient(app).post("/trigger/docker-cleanup")
    return resp, job


def test_the_manual_trigger_runs_the_configured_script_once():
    resp, job = _post(True)
    assert resp.status_code == 202
    job.assert_awaited_once_with("/x/docker-prune.sh", timeout_seconds=60)


def test_the_manual_trigger_is_refused_on_a_host_without_the_flag():
    resp, job = _post(False)
    assert resp.status_code == 409
    job.assert_not_awaited()


def test_the_default_script_path_is_the_ddp_agents_one(monkeypatch):
    monkeypatch.delenv("DOCKER_PRUNE_SCRIPT", raising=False)
    assert _load_from_env()["docker_prune_script"].endswith("ddp-agents/deployment/scripts/docker-prune.sh")
    assert os.path.basename(_load_from_env()["docker_prune_script"]) == "docker-prune.sh"


@pytest.mark.asyncio
async def test_a_second_run_while_one_is_going_is_skipped_not_started(tmp_path):
    import asyncio

    script = _script(tmp_path, "sleep 2")
    with patch(f"{_P}.post_alert"):
        first = asyncio.create_task(run_docker_cleanup_job(script))
        await asyncio.sleep(0.3)
        second = await run_docker_cleanup_job(script)
        assert second["skipped"] == "already_running"
        assert (await first)["success"] is True
