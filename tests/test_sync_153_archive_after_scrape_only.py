"""SYNC-153: archive only after a state's scrape, under a per-state lock, with a bounded retry and an alert.

PLAN-push-button-onboarding.md, "2026-11-10: One pipeline per state", SYNC (b). The weekly archive days and the 30-minute
debounce are gone; one Redis lock per state (`ddp:pipeline_lock:<archive code>`) covers the whole pipeline run, scheduled or
manual; a failed archive is retried after 30 min and 2 h, then alerted. Redis is a fake that honours SET NX and the
compare-and-delete release; the archive, the scrape and the alert are faked.
"""

from __future__ import annotations

import asyncio
import inspect
import textwrap
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.pipelines import openstates_archive as archive
from ddp_sync.pipelines import openstates_scrape as scrape
from ddp_sync.scheduler import DEFAULT_CONFIG_PATH
from ddp_sync.services import jurisdiction_enrollment as je
from ddp_sync.services import redis_store

H = 3600


class FakeRedis:
    def __init__(self, fail_eval=False):
        self.data: dict[str, str] = {}
        self.ttl: dict[str, int] = {}
        self.fail_eval = fail_eval

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return None
        self.data[key], self.ttl[key] = value, ex
        return True

    async def eval(self, script, n, key, token, *args):
        assert "redis.call('get', KEYS[1]) == ARGV[1]" in script  # compare-and-delete / compare-and-expire, never blind
        if self.fail_eval:
            raise ConnectionError("redis went away")
        if self.data.get(key) != token:
            return 0
        if "'expire'" in script:
            self.ttl[key] = int(args[0])
        else:
            del self.data[key]
        return 1

    async def scan_iter(self, match):
        prefix = match.rstrip("*")
        for k in list(self.data):
            if k.startswith(prefix):
                yield k

    async def delete(self, *keys):
        for k in keys:
            self.data.pop(k, None)


@pytest.fixture(autouse=True)
def manifest(tmp_path, monkeypatch):
    je._cache.clear()
    je._logged.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({
        "pa": {"status": "scraping", "archive": {"enabled": True}},
        "us": {"status": "live", "archive": {"enabled": True}},
        "ma": {"status": "live", "archive": {"enabled": True}},
    }))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    return tmp_path


@pytest.fixture
def redis():
    fake = FakeRedis()
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=fake)):
        yield fake


@pytest.fixture
def no_sleep():
    with patch("ddp_sync.pipelines.openstates_archive.asyncio.sleep", new=AsyncMock()) as sleep:
        yield sleep


def _alert(**kw):
    return patch("ddp_sync.pipelines.openstates_archive.post_alert", **kw)


# --- the lock -------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_the_owner_releases_the_lock(redis):
    token = await redis_store.acquire_state_lock("pa", 60)
    assert token and await redis_store.acquire_state_lock("pa", 60) is None   # held: a second taker gets nothing
    await redis_store.release_state_lock("pa", "not-the-owner")
    assert redis.data["ddp:pipeline_lock:pa"] == token                          # a stranger's release does nothing
    await redis_store.release_state_lock("pa", token)
    assert "ddp:pipeline_lock:pa" not in redis.data


@pytest.mark.asyncio
async def test_the_us_scrape_and_the_us_archive_share_one_lock(redis):
    assert await redis_store.acquire_state_lock("usa", 60)
    assert await redis_store.acquire_state_lock("us", 60) is None


@pytest.mark.asyncio
async def test_startup_clears_every_lock_and_nothing_else(redis):
    await redis_store.acquire_state_lock("pa", 60)
    await redis_store.acquire_state_lock("us", 60)
    redis.data["ddp:flow:openstates_scrape_pa"] = "{}"
    assert await redis_store.clear_state_locks() == 2
    assert list(redis.data) == ["ddp:flow:openstates_scrape_pa"]


def test_ddp_sync_clears_the_locks_at_startup_before_the_scheduler_starts():
    from ddp_sync import app

    src = inspect.getsource(app.lifespan)
    assert "await clear_state_locks()" in src
    assert src.index("await clear_state_locks()") < src.index("scheduler.start()")


@pytest.mark.asyncio
async def test_a_lock_store_outage_fails_open():
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=None)):
        assert await redis_store.acquire_state_lock("pa", 60) == redis_store.NO_LOCK
        async with redis_store.state_lock("pa", 60) as held:
            assert held is True


def _config():
    with open(DEFAULT_CONFIG_PATH) as f:
        c = yaml.safe_load(f)
    return c["openstates_scrape"], c["openstates_archive"]


@pytest.mark.parametrize("state,hours", [
    # sessions x (scrape Fargate wait + RDS load 2 h) + 3 archive attempts x 12 h + 30 min + 2 h retry delays + 1 h
    ("usa", 2 * (12 + 2) + 3 * 12 + 2.5 + 1),   # two sessions: 67.5 h
    ("ma", 1 * (24 + 2) + 3 * 12 + 2.5 + 1),    # MA's 24 h scrape wait (OPEN-476): 65.5 h
    ("pa", 1 * (12 + 2) + 3 * 12 + 2.5 + 1),    # a one-session state: 53.5 h
])
def test_the_lock_ttl_covers_the_worst_case_run(state, hours):
    scrape_cfg, archive_cfg = _config()
    assert scrape.pipeline_lock_ttl_s(state, scrape_cfg, archive_cfg) == int(hours * H)


# --- the pipeline under the lock --------------------------------------------------------------------------------


def _pipeline_patches(statuses, archived=True):
    async def _status(key, payload):
        statuses.append((key, payload))

    return (
        patch.object(scrape, "_run_scrape", new=AsyncMock(return_value={"success": True, "jurisdiction": "pa"})),
        patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=AsyncMock(return_value=archived)),
        patch.object(scrape, "_check_sustained_blocking", new=AsyncMock()),
        patch.object(scrape, "_maybe_signal_broker_bill_events", new=AsyncMock()),
        patch.object(scrape, "_write_flow_status", new=_status),
    )


@pytest.mark.asyncio
async def test_a_second_run_gets_pipeline_busy_and_does_nothing(redis):
    """Acceptance 3: the scheduled run that finds the lock held raises (an error in the log) and writes no status."""
    await redis_store.acquire_state_lock("pa", 60)
    statuses = []
    p = _pipeline_patches(statuses)
    with p[0] as run, p[1], p[2], p[3], p[4], pytest.raises(scrape.PipelineBusy):
        await scrape.run_state_pipeline("pa", {}, {"enabled": True})
    run.assert_not_awaited()
    assert statuses == []


@pytest.mark.asyncio
async def test_the_lock_is_held_for_the_whole_run_then_released(redis):
    seen = []

    async def _archive(j, cfg):
        seen.append(dict(redis.data))
        return True

    statuses = []
    p = _pipeline_patches(statuses)
    with p[0], patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=_archive), p[2], p[3], p[4]:
        await scrape.run_state_pipeline("pa", {}, {"enabled": True})
    assert "ddp:pipeline_lock:pa" in seen[0]       # still held while the archive runs
    assert "ddp:pipeline_lock:pa" not in redis.data
    assert statuses[0][1]["status"] == "completed"


@pytest.mark.asyncio
async def test_a_manual_runs_token_is_used_not_taken_again(redis):
    token = await redis_store.acquire_state_lock("pa", 60)
    statuses = []
    p = _pipeline_patches(statuses)
    with p[0], p[1], p[2], p[3], p[4]:
        result = await scrape.run_state_pipeline("pa", {}, {"enabled": True}, lock_token=token)
    assert result["success"] is True and "ddp:pipeline_lock:pa" not in redis.data


@pytest.mark.asyncio
@pytest.mark.parametrize("archived", [True, False])
async def test_a_failed_release_never_replaces_the_outcome(archived):
    fake = FakeRedis(fail_eval=True)
    statuses = []
    p = _pipeline_patches(statuses, archived=archived)
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=fake)), \
            patch.object(redis_store.logger, "error") as log, p[0], p[1], p[2], p[3], p[4]:
        if archived:
            assert (await scrape.run_state_pipeline("pa", {}, {"enabled": True}))["success"] is True
        else:
            with pytest.raises(scrape.PipelineFailed):
                await scrape.run_state_pipeline("pa", {}, {"enabled": True})
    assert any(c.args[0] == "pipeline_lock_release_failed" for c in log.call_args_list)


# --- the archive: bounded retry, then an alert ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_three_failures_send_the_alert_and_return_false(no_sleep):
    with patch.object(archive, "run_single_archive_job", new=AsyncMock(return_value={"success": False, "error": "x"})) \
            as run, _alert() as alert:
        assert await archive.archive_with_retry("pa", {"enabled": True}) is False
    assert run.await_count == 1 + len(archive.ARCHIVE_RETRY_DELAYS_S) == 3
    assert [c.args[0] for c in no_sleep.await_args_list] == [1800, 7200]     # plan open question 3's proposal
    alert.assert_called_once()
    assert alert.call_args.kwargs["source"] == "openstates_archive.archive_failure" and "pa" in alert.call_args.args[0]


@pytest.mark.asyncio
async def test_an_exception_on_the_first_attempt_is_retried(no_sleep):
    run = AsyncMock(side_effect=[RuntimeError("fargate blew up"), {"success": True}])
    with patch.object(archive, "run_single_archive_job", new=run), _alert() as alert:
        assert await archive.archive_with_retry("pa", {"enabled": True}) is True
    assert run.await_count == 2 and no_sleep.await_args_list[0].args[0] == 1800
    alert.assert_not_called()


@pytest.mark.asyncio
async def test_a_failing_alert_still_returns_the_outcome(no_sleep):
    with patch.object(archive, "run_single_archive_job", new=AsyncMock(return_value={"success": False})), \
            _alert(side_effect=RuntimeError("slack down")):
        assert await archive.archive_with_retry("pa", {"enabled": True}) is False


@pytest.mark.asyncio
async def test_a_success_needs_no_retry(no_sleep):
    with patch.object(archive, "run_single_archive_job", new=AsyncMock(return_value={"success": True})) as run:
        assert await archive.archive_with_retry("pa", {"enabled": True}) is True
    assert run.await_count == 1 and no_sleep.await_count == 0


@pytest.mark.asyncio
async def test_three_archive_failures_fail_the_job_with_status_failed(no_sleep, redis):
    """Acceptance 4 end to end: the pipeline's job raises and its status says failed."""
    statuses = []

    async def _status(key, payload):
        statuses.append((key, payload))

    with patch.object(scrape, "_run_scrape", new=AsyncMock(return_value={"success": True})), \
            patch.object(scrape, "_check_sustained_blocking", new=AsyncMock()), \
            patch.object(scrape, "_maybe_signal_broker_bill_events", new=AsyncMock()), \
            patch.object(scrape, "_write_flow_status", new=_status), \
            patch.object(archive, "run_single_archive_job", new=AsyncMock(return_value={"success": False})), \
            _alert() as alert, pytest.raises(scrape.PipelineFailed):
        await scrape.run_state_pipeline("pa", {}, {"enabled": True})
    alert.assert_called_once()
    assert statuses[0][0] == "openstates_scrape_pa" and statuses[0][1]["status"] == "failed"


# --- the manual scrape trigger takes the lock before it answers ---------------------------------------------------


def _post_scrape(target):
    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "t"
    scheduler = SimpleNamespace(_sync_config={"openstates_scrape": {}, "openstates_archive": {"enabled": True}})
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch.object(scrape, "people_check_refusal", new=AsyncMock(return_value=None)), \
            patch("ddp_sync.api.routes.triggers._run_state_pipeline_in_background", new=AsyncMock()) as bg:
        return TestClient(app).post(f"/trigger/openstates-scrape/{target}"), bg


def test_a_202_means_the_run_owns_the_lock_and_a_concurrent_request_gets_409(redis):
    first, bg = _post_scrape("pa")
    assert first.status_code == 202
    assert bg.await_args.kwargs["lock_token"] == redis.data["ddp:pipeline_lock:pa"]
    second, bg2 = _post_scrape("pa")
    assert second.status_code == 409 and "in progress" in second.json()["detail"]
    bg2.assert_not_awaited()


def test_a_manual_us_archive_blocks_a_manual_usa_scrape(redis):
    asyncio.run(redis_store.acquire_state_lock("us", 60))
    resp, bg = _post_scrape("usa")
    assert resp.status_code == 409
    bg.assert_not_awaited()


def test_the_committed_yaml_says_the_archive_follows_the_scrape():
    text = DEFAULT_CONFIG_PATH.read_text()
    assert "schedule:" not in textwrap.dedent(text.split("openstates_archive:")[1].split("\nvotebot_eval:")[0])


# --- the hand-off from a manual trigger to its run (pm-review round 2) ---------------------------------------------


def test_a_manual_trigger_takes_the_lock_with_the_short_handoff_ttl(redis):
    """If the background run never starts while the process lives on, the lock frees itself in 15 min, not days."""
    resp, _ = _post_scrape("pa")
    assert resp.status_code == 202
    assert redis.ttl["ddp:pipeline_lock:pa"] == redis_store.HANDOFF_TTL_S == 900


@pytest.mark.asyncio
async def test_the_run_extends_a_handed_off_lock_to_the_full_ttl(redis):
    token = await redis_store.acquire_state_lock("pa", redis_store.HANDOFF_TTL_S)
    seen = []

    async def _archive(j, cfg):
        seen.append(redis.ttl["ddp:pipeline_lock:pa"])
        return True

    statuses = []
    p = _pipeline_patches(statuses)
    with p[0], patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=_archive), p[2], p[3], p[4]:
        await scrape.run_state_pipeline("pa", {}, {"enabled": True}, lock_token=token)
    assert seen == [scrape.pipeline_lock_ttl_s("pa", {}, {"enabled": True})]
    assert "ddp:pipeline_lock:pa" not in redis.data


@pytest.mark.asyncio
@pytest.mark.parametrize("now_held_by", [None, "another-run"])
async def test_a_lost_handoff_does_not_run_and_does_not_release_someone_elses_lock(redis, now_held_by):
    """The token expired (None) or another run took the state since: this run must not go ahead."""
    if now_held_by:
        redis.data["ddp:pipeline_lock:pa"] = now_held_by
    statuses = []
    p = _pipeline_patches(statuses)
    with p[0] as run, p[1], p[2], p[3], p[4], pytest.raises(scrape.PipelineBusy):
        await scrape.run_state_pipeline("pa", {}, {"enabled": True}, lock_token="stale-token")
    run.assert_not_awaited()
    assert redis.data.get("ddp:pipeline_lock:pa") == now_held_by


@pytest.mark.asyncio
async def test_a_lost_handoff_skips_a_manual_archive(redis):
    redis.data["ddp:pipeline_lock:pa"] = "another-run"
    with patch.object(archive, "run_single_archive_job", new=AsyncMock()) as run, \
            patch.object(archive, "_write_flow_status", new=AsyncMock()) as status:
        result = await archive.run_archive_only("pa", {"enabled": True}, lock_token="stale-token")
    assert result["skipped"] == "busy" and status.await_args.args[1]["status"] == "skipped"
    run.assert_not_awaited()
    assert redis.data["ddp:pipeline_lock:pa"] == "another-run"


@pytest.mark.asyncio
async def test_run_archive_only_with_no_token_takes_and_releases_the_lock(redis):
    """The optional-token path (pipeline_lock_ttl_s with no scrape config) works on its own."""
    held = []

    async def _one(j, cfg):
        held.append((dict(redis.data), dict(redis.ttl)))
        return {"success": True}

    with patch.object(archive, "run_single_archive_job", new=_one), \
            patch.object(archive, "_write_flow_status", new=AsyncMock()):
        result = await archive.run_archive_only("pa", {"enabled": True})
    assert result == {"success": True, "jurisdiction": "pa", "archived": True, "attempts": 1}
    assert "ddp:pipeline_lock:pa" in held[0][0]
    assert held[0][1]["ddp:pipeline_lock:pa"] == scrape.pipeline_lock_ttl_s("pa", None, {"enabled": True})
    assert "ddp:pipeline_lock:pa" not in redis.data


@pytest.mark.asyncio
@pytest.mark.parametrize("client", ["eval_raises", "not_connected"])
async def test_a_handoff_redis_cannot_confirm_fails_open_and_runs(client):
    """pm-review round 3: "cannot answer" (None) is not "lost" (False); the run goes ahead, as acquire does."""
    fake = FakeRedis(fail_eval=True) if client == "eval_raises" else None
    statuses = []
    p = _pipeline_patches(statuses)
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=fake)), \
            p[0] as run, p[1], p[2], p[3], p[4]:
        result = await scrape.run_state_pipeline("pa", {}, {"enabled": True}, lock_token="handed-off")
    assert result["success"] is True
    run.assert_awaited_once()
