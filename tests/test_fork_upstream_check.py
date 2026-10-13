"""The daily fork-vs-upstream gap check: measurement, alert de-duplication, failure handling, wiring.

Git is real here (local bare repos stand in for the GitHub forks and upstreams); only Slack and
Redis are stubbed.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import textwrap
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines import fork_upstream_check as mod
from ddp_sync.pipelines.fork_upstream_check import run_fork_upstream_check_job
from ddp_sync.scheduler import UpdateScheduler
from tests.test_scheduler_per_task_flags import _settings

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
    "PATH": "/usr/bin:/bin:/opt/homebrew/bin",
}


def _git(*args, cwd, date=None):
    env = dict(_GIT_ENV)
    if date:
        env["GIT_COMMITTER_DATE"] = env["GIT_AUTHOR_DATE"] = date.isoformat()
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout


class Repos:
    """An upstream, and a fork cloned from it that has since carried one fix of its own."""

    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.upstream = tmp / "upstream.git"
        self.fork = tmp / "fork.git"
        self.work = tmp / "work"
        self.root = tmp / "root"
        self.root.mkdir()
        _git("init", "--bare", "-b", "main", str(self.upstream), cwd=tmp)
        _git("clone", str(self.upstream), str(self.work), cwd=tmp)
        self.commit("initial", {"data/il/a.yml": "1"}, days_ago=30)
        _git("push", "origin", "HEAD:main", cwd=self.work)
        _git("clone", "--bare", str(self.upstream), str(self.fork), cwd=tmp)
        # The fork carries a fix of its own, so the histories genuinely differ.
        _git("clone", str(self.fork), str(tmp / "forkwork"), cwd=tmp)
        self.commit("our carried fix", {"data/il/carried.yml": "x"}, days_ago=20, cwd=tmp / "forkwork")
        _git("push", "origin", "HEAD:main", cwd=tmp / "forkwork")

    def commit(self, title, files, days_ago, cwd=None):
        cwd = cwd or self.work
        for path, content in files.items():
            (cwd / path).parent.mkdir(parents=True, exist_ok=True)
            (cwd / path).write_text(content)
        _git("add", "-A", cwd=cwd)
        _git("commit", "-m", title, cwd=cwd, date=datetime.now(UTC) - timedelta(days=days_ago))

    def upstream_commit(self, title, files, days_ago):
        self.commit(title, files, days_ago)
        _git("push", "origin", "HEAD:main", cwd=self.work)

    def config(self, **extra):
        return {
            "openstates_root": str(self.root),
            "fork_upstream_check": {
                "repos": [{"name": "people", "fork": str(self.fork), "upstream": str(self.upstream)}],
                **extra,
            },
        }

    @property
    def status(self):
        return json.loads((self.root / "logs" / "fork-upstream-status.json").read_text())["repos"]["people"]


@pytest.fixture(autouse=True)
def _no_env_root(monkeypatch):
    monkeypatch.delenv("OPENSTATES_ROOT", raising=False)


@pytest.fixture
def repos(tmp_path):
    return Repos(tmp_path)


def _run(cfg):
    with patch.object(mod, "post_alert") as alert, patch.object(mod, "_write_flow_status", new=AsyncMock()) as flow:
        result = asyncio_run(run_fork_upstream_check_job(cfg))
    return result, alert, flow


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


def test_current_fork_is_quiet_and_writes_status(repos):
    result, alert, flow = _run(repos.config())
    assert result["success"] is True
    assert repos.status["behind_count"] == 0
    assert repos.status["oldest_age_days"] is None
    alert.assert_not_called()
    assert flow.await_args.args[0] == "fork_upstream_check"
    assert flow.await_args.args[1]["status"] == "completed"


def test_young_gap_is_recorded_but_does_not_alert(repos):
    repos.upstream_commit("fresh upstream change", {"data/il/b.yml": "2"}, days_ago=1)
    result, alert, _ = _run(repos.config())
    assert result["success"] is True
    assert repos.status["behind_count"] == 1
    assert 0.5 < repos.status["oldest_age_days"] < 1.5
    assert repos.status["titles"] == ["fresh upstream change"]
    alert.assert_not_called()


def test_old_gap_alerts_once_with_titles_and_sensitive_paths(repos):
    repos.upstream_commit("old one", {"data/il/b.yml": "2"}, days_ago=5)
    repos.upstream_commit("touches automation", {".github/workflows/x.yml": "on: push"}, days_ago=2)
    repos.upstream_commit("adds migration", {"pkg/migrations/0001.py": "pass"}, days_ago=1)
    _, alert, _ = _run(repos.config())
    st = repos.status
    assert st["behind_count"] == 3
    assert 4.5 < st["oldest_age_days"] < 5.5
    assert st["titles"][0] == "adds migration"  # newest first
    assert ".github/workflows/x.yml" in st["sensitive_paths_changed"]
    assert "pkg/migrations/0001.py" in st["sensitive_paths_changed"]
    assert "data/il" in st["areas_changed"] and ".github/workflows" in st["areas_changed"]
    # Our own carried fix is not counted as something upstream changed.
    assert "data/il/carried.yml" not in st["sensitive_paths_changed"]
    alert.assert_called_once()
    text = alert.call_args.args[0]
    assert "people fork is 3 upstream commit(s) behind" in text and "adds migration" in text
    assert alert.call_args.kwargs["source"] == "fork_upstream_check"


def test_same_gap_does_not_alert_again_the_next_night(repos):
    repos.upstream_commit("old one", {"data/il/b.yml": "2"}, days_ago=5)
    _, first, _ = _run(repos.config())
    _, second, _ = _run(repos.config())
    first.assert_called_once()
    second.assert_not_called()
    assert repos.status["alerted_tier"] == 1


def test_realerts_after_repeat_days_and_when_a_later_tier_is_crossed(repos):
    repos.upstream_commit("old one", {"data/il/b.yml": "2"}, days_ago=5)
    _run(repos.config())
    path = repos.root / "logs" / "fork-upstream-status.json"
    data = json.loads(path.read_text())
    data["repos"]["people"]["last_alert_at"] = (datetime.now(UTC) - timedelta(days=8)).isoformat()
    path.write_text(json.dumps(data))
    _, alert, _ = _run(repos.config())
    alert.assert_called_once()  # a week of silence is long enough to remind

    # A gap that has aged into the next tier alerts at once even though the last alert was today.
    repos.upstream_commit("another", {"data/il/c.yml": "3"}, days_ago=0)
    data = json.loads(path.read_text())
    data["repos"]["people"]["alerted_tier"] = 0
    path.write_text(json.dumps(data))
    _, alert, _ = _run(repos.config())
    alert.assert_called_once()


def test_catching_up_resets_the_memory(repos):
    repos.upstream_commit("old one", {"data/il/b.yml": "2"}, days_ago=5)
    _run(repos.config())
    # The fork merges upstream: nothing is lacking any more.
    _git("clone", str(repos.fork), str(repos.tmp / "fw2"), cwd=repos.tmp)
    _git("pull", "--no-rebase", "--no-edit", str(repos.upstream), "main", cwd=repos.tmp / "fw2")
    _git("push", "origin", "HEAD:main", cwd=repos.tmp / "fw2")
    _, alert, _ = _run(repos.config())
    alert.assert_not_called()
    assert repos.status["behind_count"] == 0 and "alerted_tier" not in repos.status
    # A new old gap later alerts again, not suppressed by the earlier episode.
    repos.upstream_commit("later old one", {"data/il/d.yml": "4"}, days_ago=4)
    _, alert, _ = _run(repos.config())
    alert.assert_called_once()


def test_fetch_failure_alerts_night_one_and_three_not_two_and_keeps_other_repos(repos):
    cfg = repos.config()
    good = dict(cfg["fork_upstream_check"]["repos"][0], name="good")
    bad = {"name": "people", "fork": str(repos.tmp / "does-not-exist.git"), "upstream": str(repos.upstream)}
    cfg["fork_upstream_check"]["repos"] = [bad, good]
    outcomes = []
    for _ in range(3):
        result, alert, flow = _run(cfg)
        outcomes.append(alert.call_count)
        assert result["success"] is False
        assert flow.await_args.args[1]["status"] == "failed"
    assert outcomes == [1, 0, 1]
    status = json.loads((repos.root / "logs" / "fork-upstream-status.json").read_text())["repos"]
    assert status["people"]["status"] == "failed" and status["people"]["failure_streak"] == 3
    assert status["good"]["status"] == "ok"  # one bad repo did not hide the others


def test_failure_keeps_last_good_numbers_and_recovery_clears_the_streak(repos):
    repos.upstream_commit("old one", {"data/il/b.yml": "2"}, days_ago=5)
    _run(repos.config())
    broken = repos.config()
    broken["fork_upstream_check"]["repos"][0]["upstream"] = str(repos.tmp / "gone.git")
    _, alert, _ = _run(broken)
    assert repos.status["status"] == "failed" and repos.status["behind_count"] == 1
    assert "measured nothing" in alert.call_args.args[0]
    _run(repos.config())
    assert repos.status["status"] == "ok" and repos.status["failure_streak"] == 0


def test_never_touches_live_working_trees_and_never_uses_credentials(repos):
    live = repos.root / "people"
    live.mkdir()
    (live / "keep.txt").write_text("live scrape reads this")
    seen = []
    real = mod._run_with_group_kill

    def spy(cmd, env, timeout, cwd=None, **kw):
        seen.append((cmd, cwd))
        return real(cmd, env, timeout, cwd=cwd, **kw)

    with patch.object(mod, "_run_with_group_kill", side_effect=spy):
        _run(repos.config())
    assert (live / "keep.txt").read_text() == "live scrape reads this"
    assert sorted(p.name for p in live.iterdir()) == ["keep.txt"]
    assert seen and all(cmd[:3] == ["git", "-c", "credential.helper="] for cmd, _ in seen)
    assert all(str(live) not in (cwd or "") and str(live) not in " ".join(cmd) for cmd, cwd in seen)
    assert not any(c[0][3] in ("push", "merge", "commit") for c in seen)


def test_default_repo_list_covers_the_four_forks_with_public_urls():
    cfgs = {c["name"]: c for c in mod._default_repo_cfgs()}
    assert set(cfgs) == {"people", "openstates-core", "openstates-scrapers", "api-v3"}
    assert cfgs["openstates-scrapers"]["fork"] == "https://github.com/Digital-Democracy-Project/openstates-scrapers.git"
    assert cfgs["api-v3"]["upstream"] == "https://github.com/openstates/api-v3.git"
    assert all("@" not in c["fork"] + c["upstream"] for c in cfgs.values())  # no embedded credentials


# --- wiring -----------------------------------------------------------------------------------

YAML = """
    bill_sync:
      sync_time_utc: "04:00"
    openstates_scrape:
      enabled: true
      fork_upstream_check:
        enabled: true
        sync_time_utc: "01:30"
"""


def _jobs(flag: bool, yaml_text: str = YAML, **extra):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(yaml_text))
    sched = UpdateScheduler(settings=_settings(fork_upstream_check_enabled=flag, **extra), config_path=Path(f.name))
    sched.start()
    try:
        return {j.id: j for j in sched.scheduler.get_jobs()}
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_registered_daily_at_0130_utc_when_flag_on():
    job = _jobs(True)["fork_upstream_check"]
    assert job.name == "OpenStates: fork upstream check"
    fields = {f.name: str(f) for f in job.trigger.fields}
    assert fields["hour"] == "1" and fields["minute"] == "30" and fields["day_of_week"] == "*"
    assert job.max_instances == 1 and str(job.trigger.timezone) == "UTC"


@pytest.mark.asyncio
async def test_registered_even_when_this_host_runs_no_scrapes():
    # The Mac has OPENSTATES_SCRAPE_ENABLED=false (scraping runs in the cloud); the check must
    # still switch on there.
    jobs = _jobs(True, openstates_scrape_enabled=False)
    assert "fork_upstream_check" in jobs
    assert "openstates_fl_scrape" not in jobs


@pytest.mark.asyncio
async def test_not_registered_when_flag_off_or_yaml_disabled():
    assert "fork_upstream_check" not in _jobs(False)
    off = YAML.replace("fork_upstream_check:\n        enabled: true", "fork_upstream_check:\n        enabled: false")
    assert "fork_upstream_check" not in _jobs(True, off)
    assert "daily_bill_sync" in _jobs(False)


@pytest.mark.parametrize("value,expected", [(None, False), ("", False), ("false", False), ("true", True)])
def test_flag_defaults_off_and_only_the_word_true_enables_it(monkeypatch, value, expected):
    from ddp_sync import config

    if value is None:
        monkeypatch.delenv("FORK_UPSTREAM_CHECK_ENABLED", raising=False)
    else:
        monkeypatch.setenv("FORK_UPSTREAM_CHECK_ENABLED", value)
    assert config._load_from_env()["fork_upstream_check_enabled"] is expected


def test_shipped_yaml_runs_before_the_people_pull():
    import yaml

    cfg = yaml.safe_load(Path("config/sync_schedule.yaml").read_text())["openstates_scrape"]
    assert cfg["fork_upstream_check"]["sync_time_utc"] == "01:30"
    assert cfg["fork_upstream_check"]["alert_tiers_days"][0] == 3
    assert cfg["fork_upstream_check"]["sync_time_utc"] < cfg["people_pull"]["sync_time_utc"]


def test_manual_trigger_target_is_registered():
    from ddp_sync.api.routes.triggers import _OPENSTATES_JOB_TARGETS

    assert "fork-check" in _OPENSTATES_JOB_TARGETS
