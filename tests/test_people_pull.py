"""SYNC-102: the nightly people-pull job, its flag gating and its trigger/health wiring."""

from __future__ import annotations

import subprocess
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import run_people_pull_job
from ddp_sync.scheduler import UpdateScheduler
from tests.test_scheduler_per_task_flags import _settings

MOD = "ddp_sync.pipelines.openstates_scrape"
CONFIG = {"openstates_root": "/fake/root"}


def _run(returncode=0, stdout=b"Already up to date.\n", stderr=b"", timed_out=False):
    return patch(f"{MOD}._run_with_group_kill", return_value=(returncode, stdout, stderr, timed_out, False))


def _common():
    return (
        patch(f"{MOD}._people_head", return_value={"sha": "abc", "date": "2026-10-01T00:00:00+00:00"}),
        patch(f"{MOD}._people_branch", return_value="main"),
        patch(f"{MOD}._write_flow_status", new=AsyncMock()),
        patch(f"{MOD}._alert_scrape_failure"),
    )


@pytest.mark.asyncio
async def test_success_runs_exact_command_and_records_heads():
    head, branch, st, alert = _common()
    with _run() as run, head, branch, st as status, alert as mock_alert:
        result = await run_people_pull_job(CONFIG)
    cmd, env, timeout = run.call_args.args
    assert cmd == ["git", "-C", "/fake/root/people", "pull", "--ff-only"]
    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert timeout == 300
    assert result["success"] is True
    rec = status.await_args.args[1]
    assert status.await_args.args[0] == "openstates_people_pull"
    assert rec["status"] == "completed"
    assert rec["path"] == "/fake/root/people"
    assert rec["branch"] == "main"
    assert rec["summary"] == "Already up to date."
    assert rec["head_sha_before"] == rec["head_sha_after"] == "abc"
    assert rec["head_commit_date_after"] == "2026-10-01T00:00:00+00:00"
    mock_alert.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,expected",
    [
        (dict(returncode=128, stderr=b"fatal: Not possible to fast-forward"), "exit_code_128"),
        (dict(returncode=-9, timed_out=True), "timed out"),
    ],
)
async def test_nonzero_exit_and_timeout_write_failed_and_alert(kwargs, expected):
    head, branch, st, alert = _common()
    with _run(**kwargs), head, branch, st as status, alert as mock_alert:
        result = await run_people_pull_job(CONFIG)
    assert result["success"] is False
    assert status.await_args.args[1]["status"] == "failed"
    assert expected in status.await_args.args[1]["error"]
    mock_alert.assert_called_once()
    assert mock_alert.call_args.args[0] == "people pull"


@pytest.mark.asyncio
async def test_exception_writes_failed_and_alerts():
    head, branch, st, alert = _common()
    with patch(f"{MOD}._run_with_group_kill", side_effect=FileNotFoundError("git")), head, branch, st as status, alert as mock_alert:
        result = await run_people_pull_job(CONFIG)
    assert result["success"] is False
    assert status.await_args.args[1]["status"] == "failed"
    assert "FileNotFoundError" in status.await_args.args[1]["error"]
    mock_alert.assert_called_once()


@pytest.mark.asyncio
async def test_real_git_fast_forwards_and_refuses_diverged(tmp_path):
    """No mocks on git: a real ff pull succeeds, a diverged checkout fails and is left untouched."""
    def git(*a, cwd):
        subprocess.run(["git", *a], cwd=cwd, check=True, capture_output=True,
                       env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
                            "GIT_COMMITTER_EMAIL": "t@t", "PATH": "/usr/bin:/bin:/opt/homebrew/bin"})
    origin, root = tmp_path / "origin", tmp_path / "root"
    origin.mkdir(); root.mkdir()
    git("init", "-b", "main", cwd=origin)
    (origin / "a").write_text("1"); git("add", ".", cwd=origin); git("commit", "-m", "1", cwd=origin)
    git("clone", str(origin), str(root / "people"), cwd=tmp_path)
    (origin / "a").write_text("2"); git("commit", "-am", "2", cwd=origin)

    with patch(f"{MOD}._write_flow_status", new=AsyncMock()) as st, patch(f"{MOD}._alert_scrape_failure") as alert:
        ok = await run_people_pull_job({"openstates_root": str(root)})
        assert ok["success"] is True
        assert st.await_args.args[1]["head_sha_before"] != st.await_args.args[1]["head_sha_after"]
        alert.assert_not_called()

        (root / "people" / "a").write_text("local"); git("commit", "-am", "local", cwd=root / "people")
        (origin / "a").write_text("3"); git("commit", "-am", "3", cwd=origin)
        bad = await run_people_pull_job({"openstates_root": str(root)})
        assert bad["success"] is False
        alert.assert_called_once()
    assert (root / "people" / "a").read_text() == "local"


YAML = """
    bill_sync:
      sync_time_utc: "04:00"
    openstates_scrape:
      enabled: true
      people_pull:
        enabled: true
        sync_time_utc: "03:00"
"""


def _jobs(flag: bool, yaml_text: str = YAML):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(yaml_text))
    sched = UpdateScheduler(
        settings=_settings(openstates_people_pull_enabled=flag), config_path=Path(f.name)
    )
    sched.start()
    try:
        return {j.id: j for j in sched.scheduler.get_jobs()}
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_registered_daily_at_0300_utc_when_flag_on():
    job = _jobs(True)["openstates_people_pull"]
    assert job.name == "OpenStates: people pull"
    fields = {f.name: str(f) for f in job.trigger.fields}
    assert fields["hour"] == "3" and fields["minute"] == "0" and fields["day_of_week"] == "*"
    assert job.misfire_grace_time == 3600 and job.max_instances == 1


@pytest.mark.asyncio
async def test_not_registered_when_flag_off_and_other_jobs_untouched():
    jobs = _jobs(False)
    assert "openstates_people_pull" not in jobs
    assert "daily_bill_sync" in jobs


@pytest.mark.asyncio
async def test_shared_yaml_enabled_false_also_skips():
    jobs = _jobs(True, YAML.replace("people_pull:\n        enabled: true", "people_pull:\n        enabled: false"))
    assert "openstates_people_pull" not in jobs


def test_flag_defaults_off_but_env_true_turns_it_on(monkeypatch):
    from ddp_sync import config

    monkeypatch.delenv("OPENSTATES_PEOPLE_PULL_ENABLED", raising=False)
    assert config._load_from_env()["openstates_people_pull_enabled"] is False
    assert config._load_from_env()["openstates_patch_refresh_enabled"] is True
    monkeypatch.setenv("OPENSTATES_PEOPLE_PULL_ENABLED", "true")
    assert config._load_from_env()["openstates_people_pull_enabled"] is True


def test_shipped_yaml_has_people_pull_block():
    import yaml

    cfg = yaml.safe_load(Path("config/sync_schedule.yaml").read_text())
    assert cfg["openstates_scrape"]["people_pull"]["sync_time_utc"] == "03:00"
