"""SYNC-178: `ddp-sync status` (GET /ddp-sync/v1/status, `services.activity`, `status_report`, `cli`).

The acceptance cases: a session pipeline running while CAMS is idle shows with its progress and makes a restart unsafe;
with nothing running it says so and lists the next jobs; a held lease shows with its expiry; read only, no secret
printed, behind the API key. Plus: a run a restart stopped shows as such, and scheduled jobs register themselves."""

from __future__ import annotations

import asyncio
import fnmatch
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync import cli, status_report
from ddp_sync.api.routes.health import router as health_router
from ddp_sync.pipelines import session_pipeline_runner
from ddp_sync.services import activity


class FakeRedis:
    """The few async Redis calls activity and status_report make, in memory (strings, one hash, one list, TTLs)."""

    def __init__(self):
        self.strings: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}

    async def get(self, key):
        return self.strings.get(key)

    async def ttl(self, key):
        return self.ttls.get(key, -1)

    async def scan_iter(self, match="*", count=None):
        for key in list(self.strings):
            if fnmatch.fnmatchcase(key, match):
                yield key

    async def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value

    async def hdel(self, key, field):
        self.hashes.get(key, {}).pop(field, None)

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def delete(self, key):
        self.hashes.pop(key, None)
        self.strings.pop(key, None)

    async def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, end):
        self.lists[key] = self.lists.get(key, [])[start:end + 1]

    async def expire(self, key, seconds):
        return True

    async def lrange(self, key, start, end):
        return self.lists.get(key, [])[start:end + 1]


@pytest.fixture
def redis(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(activity, "_client", lambda: fake)
    monkeypatch.setattr(status_report, "_client", lambda: fake)
    monkeypatch.setattr(status_report, "_fargate_jobs", list)
    activity._reset_for_tests()
    yield fake
    activity._reset_for_tests()


@pytest.fixture
def no_scheduler(monkeypatch):
    monkeypatch.setattr(status_report, "scheduled_jobs", lambda: {"status": "running", "jobs": [
        {"id": "daily_bill_sync", "name": "bills", "next_run": "2026-11-18 04:00:00+00:00", "trigger": "cron"},
        {"id": "openstates_scrape_fl", "name": "FL", "next_run": "2026-11-18 06:00:00+00:00", "trigger": "cron"},
    ]})  # already in scheduled_jobs()' order (tested on its own below)


async def _settle():
    """Let the register's background Redis writes run."""
    for _ in range(5):
        await asyncio.sleep(0)


# ---- the acceptance cases ----

@pytest.mark.asyncio
async def test_a_running_session_pipeline_shows_with_its_progress_and_makes_a_restart_unsafe(redis, no_scheduler,
                                                                                          monkeypatch):
    """The 2026-11-18 gap: US 119 walked two bills at a time, most skipped, CAMS idle, nothing on any status screen."""
    session_pipeline_runner._legbot_line = None
    monkeypatch.setattr(session_pipeline_runner, "get_settings",
                        lambda: SimpleNamespace(session_pipeline_concurrency=1))
    candidates = [{"gov_id": f"HR {n}", "bill_openstates_id": f"ocd-bill/{n}"} for n in (1, 2, 3)]
    release_third = asyncio.Event()
    results = {"HR 1": {"artifacts_generated": ["bill_summary"]}, "HR 2": {}, "HR 3": {"error": "boom"}}

    async def fake_process_bill(candidate, **kwargs):
        if candidate["gov_id"] == "HR 3":
            await release_third.wait()
        return {"gov_id": candidate["gov_id"], "artifacts_generated": [], "artifacts_failed": [], "error": None,
                **results[candidate["gov_id"]]}

    with patch.object(session_pipeline_runner, "list_current_session_bill_candidates",
                      new=AsyncMock(return_value=candidates)), \
            patch.object(session_pipeline_runner, "_process_bill", new=fake_process_bill):
        run = asyncio.create_task(session_pipeline_runner.run_legbot_pipeline(
            "US", "119", ["bill_summary"], False, None, include_concept_statements=False, retry_failed=False,
            started_by="manual trigger"))
        for _ in range(50):
            await asyncio.sleep(0)
            if (activity.running() or [{}])[0].get("current") == ["HR 3"]:
                break

        data = await status_report.collect()
        [entry] = data["running"]
        assert (entry["kind"], entry["jurisdiction"], entry["session"], entry["started_by"]) == (
            "LegBot session pipeline", "US", "119", "manual trigger")
        assert (entry["done"], entry["total"], entry["generated"], entry["skipped"], entry["failed"]) == (2, 3, 1, 1, 0)
        assert entry["current"] == ["HR 3"]
        assert data["restart"]["safe"] is False
        assert "LegBot session pipeline (US 119)" in data["restart"]["would_interrupt"][0]
        text = status_report.format_text(data)
        assert "Safe to restart: NO" in text
        assert "bills 2/3: 1 generated, 1 skipped, 0 failed" in text
        assert "on now: HR 3" in text
        assert "started by manual trigger" in text
        assert entry["run_id"] in redis.hashes[activity.RUNNING_KEY]  # mirrored, so a restart would leave a trace

        release_third.set()
        await run
    await _settle()
    assert activity.running() == []
    assert redis.hashes[activity.RUNNING_KEY] == {}
    [finished] = await activity.recent()
    assert (finished["outcome"], finished["done"], finished["generated"], finished["skipped"], finished["failed"]) == (
        "done", 3, 1, 1, 1)


@pytest.mark.asyncio
async def test_with_nothing_running_it_says_so_and_lists_the_next_jobs_first(redis, no_scheduler):
    data = await status_report.collect()
    text = status_report.format_text(data)
    assert data["restart"]["safe"] is True
    assert text.startswith("Safe to restart: yes")  # "as of this moment"
    assert "Running now:\n  nothing" in text
    assert "Locks held:\n  none" in text
    scheduled = text.split("Scheduled next:\n", 1)[1]
    assert "11-18 04:00 UTC  daily_bill_sync" in scheduled
    assert scheduled.index("daily_bill_sync") < scheduled.index("openstates_scrape_fl")


def test_scheduled_jobs_come_next_run_first_with_paused_ones_last(monkeypatch):
    jobs = [SimpleNamespace(id="b", name="b", next_run_time=datetime(2026, 11, 19, tzinfo=UTC), trigger="cron"),
            SimpleNamespace(id="paused", name="p", next_run_time=None, trigger="cron"),
            SimpleNamespace(id="a", name="a", next_run_time=datetime(2026, 11, 18, tzinfo=UTC), trigger="cron")]
    fake = SimpleNamespace(scheduler=SimpleNamespace(running=True, get_jobs=lambda: jobs), _openstates_cadence={})
    monkeypatch.setattr("ddp_sync.scheduler.get_scheduler", lambda: fake)
    assert [j["id"] for j in status_report.scheduled_jobs()["jobs"]] == ["a", "b", "paused"]


@pytest.mark.asyncio
async def test_a_held_lease_shows_with_its_owner_and_expiry(redis, no_scheduler):
    redis.strings["ddp:pipeline_lock:fl"] = "token-abc"
    redis.ttls["ddp:pipeline_lock:fl"] = 1800
    redis.strings["ddp_sync:scraper_triggered_legbot:lock:US:119"] = "run-123"
    redis.ttls["ddp_sync:scraper_triggered_legbot:lock:US:119"] = 300
    redis.strings["ddp:unrelated:key"] = "x"
    data = await status_report.collect()
    keys = {lock["key"]: lock for lock in data["locks"]}
    assert set(keys) == {"ddp:pipeline_lock:fl", "ddp_sync:scraper_triggered_legbot:lock:US:119"}
    assert keys["ddp:pipeline_lock:fl"]["expires_in_s"] == 1800
    expires = datetime.fromisoformat(keys["ddp:pipeline_lock:fl"]["expires_at"])
    assert abs((expires - datetime.now(UTC)) - timedelta(seconds=1800)) < timedelta(seconds=5)
    text = status_report.format_text(data)
    assert "ddp:pipeline_lock:fl" in text and "owner token-abc" in text and "(in 30m)" in text
    assert "owner run-123" in text and "(in 5m)" in text


@pytest.mark.asyncio
async def test_no_secret_from_a_record_is_ever_shown(redis, no_scheduler):
    """A LegBot task record can hold a broker token (SYNC-61); the report copies named fields only."""
    redis.strings["ddp:legbot:task:cams-task-0001"] = json.dumps({
        "artifact_type": "bill_changelog", "jurisdiction": "US", "session_code": "119",
        "bill_openstates_id": "ocd-bill/1", "broker_api_token": "SECRET-TOKEN-VALUE",
        "broker_api_base": "https://broker.example"})
    redis.strings["ddp:sync:task:t1"] = json.dumps({"status": "running", "content_type": "bills",
                                                     "options": {"api_key": "SECRET-TOKEN-VALUE"}})
    data = await status_report.collect()
    assert data["legbot_tasks"] == [{"cams_task_id": "cams-task-0001", "artifact_type": "bill_changelog",
                                     "jurisdiction": "US", "session_code": "119", "bill_openstates_id": "ocd-bill/1"}]
    dumped = json.dumps(data) + status_report.format_text(data)
    assert "SECRET-TOKEN-VALUE" not in dumped
    assert "cams-tas" in status_report.format_text(data)  # the CAMS task id, as `cams status` prints its stub
    assert data["restart"]["safe"] is False  # the sync task would be interrupted
    assert data["restart"]["would_resume"] == ["1 LegBot task(s) in CAMS"]


def test_the_route_needs_the_api_key_and_answers_text_or_json(monkeypatch):
    app = FastAPI()
    app.include_router(health_router, prefix="/ddp-sync/v1")
    monkeypatch.setattr("ddp_sync.api.auth.get_settings", lambda: SimpleNamespace(api_key="right-key"))
    monkeypatch.setattr(status_report, "collect", AsyncMock(return_value={
        "running": [], "locks": [], "recent": [], "scheduled": {"status": "running", "jobs": []}}))
    client = TestClient(app)
    assert client.get("/ddp-sync/v1/status").status_code == 401
    assert client.get("/ddp-sync/v1/status", headers={"Authorization": "Bearer wrong"}).status_code == 401
    ok = client.get("/ddp-sync/v1/status?format=text", headers={"Authorization": "Bearer right-key"})
    assert ok.status_code == 200 and ok.headers["content-type"].startswith("text/plain")
    assert ok.text.startswith("Safe to restart: yes")
    as_json = client.get("/ddp-sync/v1/status", headers={"Authorization": "Bearer right-key"})
    assert as_json.json()["locks"] == []


# ---- restarts and the scheduler ----

@pytest.mark.asyncio
async def test_a_run_a_restart_stopped_shows_as_stopped_by_a_restart(redis, no_scheduler):
    redis.hashes[activity.RUNNING_KEY] = {"old-run": json.dumps({
        "run_id": "old-run", "kind": "LegBot session pipeline", "jurisdiction": "US", "session": "119",
        "started_by": "manual trigger", "started_at": "2026-11-18T03:22:00+00:00", "total": 900, "done": 140,
        "generated": 3, "skipped": 137, "failed": 0, "current": ["HR 2102"]})}
    assert await activity.sweep_restart_leftovers() == 1
    assert redis.hashes.get(activity.RUNNING_KEY) is None
    [stopped] = await activity.recent()
    assert (stopped["outcome"], stopped["done"], stopped["total"]) == ("stopped by a restart", 140, 900)
    text = status_report.format_text(await status_report.collect())
    assert "stopped by a restart" in text and "bills 140/900" in text


@pytest.mark.asyncio
async def test_scheduled_jobs_register_themselves_while_they_run(redis):
    from apscheduler.events import (
        EVENT_JOB_ERROR,
        EVENT_JOB_EXECUTED,
        EVENT_JOB_SUBMITTED,
    )

    activity.scheduler_listener(SimpleNamespace(code=EVENT_JOB_SUBMITTED, job_id="openstates_scrape_fl",
                                                scheduled_run_times=[datetime.now(UTC)]))
    assert [(r["kind"], r["detail"], r["started_by"]) for r in activity.running()] == [
        ("scheduled job", "openstates_scrape_fl", "schedule")]
    activity.scheduler_listener(SimpleNamespace(code=EVENT_JOB_EXECUTED, job_id="openstates_scrape_fl",
                                                scheduled_run_time=datetime.now(UTC)))
    activity.scheduler_listener(SimpleNamespace(code=EVENT_JOB_SUBMITTED, job_id="daily_bill_sync",
                                                scheduled_run_times=[datetime.now(UTC)]))
    activity.scheduler_listener(SimpleNamespace(code=EVENT_JOB_ERROR, job_id="daily_bill_sync",
                                                scheduled_run_time=datetime.now(UTC), exception=RuntimeError("x")))
    await _settle()
    assert activity.running() == []
    assert [(r["detail"], r["outcome"], r["error"]) for r in await activity.recent()] == [
        ("daily_bill_sync", "failed", "RuntimeError"), ("openstates_scrape_fl", "done", None)]


def test_the_scheduler_listens_for_job_starts_and_ends():
    from ddp_sync.scheduler import UpdateScheduler

    with patch.object(UpdateScheduler, "_load_sync_config", return_value={}):
        scheduler = UpdateScheduler(settings=SimpleNamespace())
    listeners = [callback for callback, _mask in scheduler.scheduler._listeners]
    assert activity.scheduler_listener in listeners


@pytest.mark.asyncio
async def test_a_pipeline_that_fails_before_its_first_bill_does_not_stay_running(redis, monkeypatch):
    session_pipeline_runner._legbot_line = None
    with patch.object(session_pipeline_runner, "list_current_session_bill_candidates",
                      new=AsyncMock(side_effect=RuntimeError("replica down"))), pytest.raises(RuntimeError):
        await session_pipeline_runner.run_legbot_pipeline(
            "US", "119", ["bill_summary"], False, None, include_concept_statements=False, retry_failed=False)
    await _settle()
    assert activity.running() == []
    [failed] = await activity.recent()
    assert (failed["outcome"], failed["error"]) == ("failed", "RuntimeError")


# ---- the command ----

def test_the_command_prints_the_text_and_never_the_key(monkeypatch, capsys):
    seen = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"Safe to restart: yes\n"

    def fake_urlopen(request, timeout):
        seen["url"], seen["auth"] = request.full_url, request.get_header("Authorization")
        return Response()

    monkeypatch.setattr(cli.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(cli, "_api_key", lambda: "the-key")
    assert cli.main(["status"]) == 0
    out = capsys.readouterr()
    assert out.out == "Safe to restart: yes\n"
    assert seen == {"url": "http://localhost:8001/ddp-sync/v1/status?format=text", "auth": "Bearer the-key"}
    assert "the-key" not in out.out + out.err


def test_the_command_says_plainly_when_the_service_is_down(monkeypatch, capsys):
    def refuse(request, timeout):
        raise cli.urllib.error.URLError("Connection refused")

    monkeypatch.setattr(cli.urllib.request, "urlopen", refuse)
    monkeypatch.setattr(cli, "_api_key", lambda: "the-key")
    assert cli.main(["status", "--url", "http://localhost:9"]) == 1
    assert "not answering at http://localhost:9" in capsys.readouterr().err


# ---- pm-review: never a reassuring "yes" on missing data; mirror writes in order ----

@pytest.mark.asyncio
async def test_an_unreadable_redis_makes_the_answer_unknown_not_yes(monkeypatch, no_scheduler):
    class Broken(FakeRedis):
        async def scan_iter(self, match="*", count=None):
            raise ConnectionError("redis down")
            yield  # pragma: no cover -- makes this an async generator

    broken = Broken()
    monkeypatch.setattr(status_report, "_client", lambda: broken)
    monkeypatch.setattr(activity, "_client", lambda: broken)
    monkeypatch.setattr(status_report, "_fargate_jobs", list)
    data = await status_report.collect()
    assert data["restart"]["safe"] is None
    assert status_report.format_text(data).startswith("Safe to restart: UNKNOWN (could not read redis")


@pytest.mark.asyncio
async def test_no_redis_at_all_is_unknown_too(monkeypatch, no_scheduler):
    monkeypatch.setattr(status_report, "_client", lambda: None)
    monkeypatch.setattr(activity, "_client", lambda: None)
    monkeypatch.setattr(status_report, "_fargate_jobs", list)
    assert (await status_report.collect())["restart"]["safe"] is None


@pytest.mark.asyncio
async def test_a_lock_held_while_nothing_listed_runs_makes_the_answer_unknown(redis, no_scheduler):
    redis.strings["ddp:pipeline_lock:fl"] = "token-abc"
    redis.ttls["ddp:pipeline_lock:fl"] = 600
    data = await status_report.collect()
    assert data["restart"]["safe"] is None
    assert "held but nothing listed is running: ddp:pipeline_lock:fl" in status_report.format_text(data)


@pytest.mark.asyncio
async def test_a_finish_never_reaches_redis_before_its_start(redis):
    """A slow first write (the start's HSET) must not let the finish's HDEL run first and leave a stale entry that the
    next start-up would report as stopped by a restart."""
    original_hset = redis.hset

    async def slow_hset(key, field, value):
        await asyncio.sleep(0.05)
        await original_hset(key, field, value)

    redis.hset = slow_hset
    activity.start("quick-run", "LegBot session pipeline", started_by="manual trigger")
    activity.finish("quick-run", "done")
    for _ in range(20):
        await asyncio.sleep(0.01)
    assert redis.hashes[activity.RUNNING_KEY] == {}
    assert [r["run_id"] for r in await activity.recent()] == ["quick-run"]


@pytest.mark.asyncio
async def test_a_run_that_starts_while_the_keyspace_is_read_is_not_missed(redis, no_scheduler):
    original_scan = redis.scan_iter

    async def scan_while_a_run_starts(match="*", count=None):
        activity.start("late-run", "LegBot session pipeline", started_by="manual trigger", jurisdiction="NC")
        async for key in original_scan(match, count):
            yield key

    redis.scan_iter = scan_while_a_run_starts
    data = await status_report.collect()
    assert [r["run_id"] for r in data["running"]] == ["late-run"]
    assert data["restart"]["safe"] is False
