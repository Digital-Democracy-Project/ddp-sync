"""SYNC-181: a whole-session re-import from the manual scrape trigger (Ramon, 2026-11-20: "modify that endpoint to let it
take all bills in a given jurisdiction/session").

`POST /trigger/openstates-scrape/<state>?sessions=...&full=true` runs the named sessions instead of the scheduled ones,
each collection full (the collector's `full=true`: no `start=` cutoff), and keeps the state's lock for the whole run so
the state's nightly is refused throughout. Redis, the scrape, the archive and ECS are faked as in the SYNC-153 and
cloud-path tests.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.pipelines import cloud_scrape_trigger as cst
from ddp_sync.pipelines import openstates_scrape as scrape
from ddp_sync.services import redis_store
from tests.test_cloud_scrape_trigger import (
    FakeEcsClient,
    FakeSubprocessResult,
    _fargate_config,
    _stopped_response,
)
from tests.test_sync_153_archive_after_scrape_only import (  # noqa: F401
    FakeRedis,
    _pipeline_patches,
    manifest,
)

US_REIMPORT = "118 chamber=lower,118 chamber=upper,119 chamber=lower,119 chamber=upper"


@pytest.fixture
def redis():
    fake = FakeRedis()
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=fake)):
        yield fake


def _post(target, cloud_owned=True, **params):
    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "t"
    scheduler = SimpleNamespace(_sync_config={"openstates_scrape": {}, "openstates_archive": {"enabled": True}})
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch.object(scrape, "people_check_refusal", new=AsyncMock(return_value=None)), \
            patch.object(scrape, "_cloud_path_owns", return_value=cloud_owned), \
            patch("ddp_sync.api.routes.triggers._run_state_pipeline_in_background", new=AsyncMock()) as bg:
        return TestClient(app).post(f"/trigger/openstates-scrape/{target}", params=params), bg


# --- the trigger ---------------------------------------------------------------------------------------------------


def test_full_and_sessions_reach_the_run_and_the_answer_says_so(redis):
    resp, bg = _post("usa", full="true", sessions=US_REIMPORT)
    assert resp.status_code == 202, resp.text
    assert resp.json() == {"status": "started", "target": "usa", "sessions": US_REIMPORT.split(","), "full": True}
    kwargs = bg.await_args.kwargs
    assert (kwargs["full"], kwargs["sessions"]) == (True, US_REIMPORT.split(","))
    assert kwargs["lock_token"] == redis.data["ddp:pipeline_lock:us"]


def test_without_them_the_run_and_the_answer_are_unchanged(redis):
    resp, bg = _post("usa")
    assert resp.status_code == 202
    assert resp.json() == {"status": "started", "target": "usa"}
    bg.assert_awaited_once_with("usa", {}, {"enabled": True}, people_checked=True,
                                lock_token=redis.data["ddp:pipeline_lock:us"])  # exactly as before SYNC-181


def test_sessions_alone_run_those_sessions_incrementally(redis):
    resp, bg = _post("usa", sessions="118 chamber=lower")
    assert resp.status_code == 202
    assert "full" not in bg.await_args.kwargs and bg.await_args.kwargs["sessions"] == ["118 chamber=lower"]


def test_full_is_a_400_off_the_cloud_path_and_takes_no_lock(redis):
    resp, bg = _post("usa", cloud_owned=False, full="true")
    assert resp.status_code == 400 and "cloud path only" in resp.json()["detail"]
    bg.assert_not_awaited()
    assert redis.data == {}


@pytest.mark.parametrize("bad", [
    "", "118,,119", "118 start=2020-01-01", "118 full=true", "118 session=119", "118;rm -rf", "118 chamber=lower;x",
    "118 Chamber=lower", "-118",
])
def test_a_bad_session_entry_is_a_400_and_takes_no_lock(redis, bad):
    resp, bg = _post("usa", sessions=bad)
    assert resp.status_code == 400, (bad, resp.text)
    bg.assert_not_awaited()
    assert redis.data == {}


def test_full_or_sessions_on_a_job_target_is_a_400(redis):
    for params in ({"full": "true"}, {"sessions": "118"}):
        resp, bg = _post("patches", **params)
        assert resp.status_code == 400, params
        bg.assert_not_awaited()


# --- the pipeline --------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_named_sessions_run_instead_of_the_scheduled_ones_and_full_reaches_the_collection(redis):
    seen = []

    async def _scrape(jurisdiction, session_arg, root, timeout_s=None, config=None):
        seen.append((session_arg, cst.FULL_COLLECTION.get()))
        return {"success": True}

    config = {"timing": {"usa": {"sessions": ["119 chamber=lower", "119 chamber=upper"]}}}
    p = _pipeline_patches([])
    with patch.object(scrape, "_run_scrape", new=_scrape), p[1], p[2], p[3], p[4]:
        await scrape.run_state_pipeline("usa", config, {"enabled": True}, sessions=["118 chamber=lower"], full=True)
        await scrape.run_state_pipeline("usa", config, {"enabled": True})
    assert seen == [("session=118 chamber=lower", True),
                    ("session=119 chamber=lower", False), ("session=119 chamber=upper", False)]
    assert cst.FULL_COLLECTION.get() is False  # reset after the run


def test_the_lock_ttl_counts_the_named_sessions():
    config = {"timing": {"usa": {"sessions": ["119 chamber=lower", "119 chamber=upper"]}}}
    two = scrape.pipeline_lock_ttl_s("usa", config, {})
    four = scrape.pipeline_lock_ttl_s("usa", config, {}, US_REIMPORT.split(","))
    one_session = scrape.pipeline_lock_ttl_s("usa", config, {}, ["118"]) - scrape.pipeline_lock_ttl_s("usa", config, {}, [])
    assert two == scrape.pipeline_lock_ttl_s("usa", config, {}, ["a", "b"])
    assert four > two and four - two == 2 * (two - scrape.pipeline_lock_ttl_s("usa", config, {}, ["a"]))
    assert one_session == 0  # an empty list still counts as one run, as the scheduled path does


@pytest.mark.asyncio
@pytest.mark.parametrize("full,renewed", [(True, True), (False, False)])
async def test_a_full_run_keeps_its_lock_and_a_nightly_is_refused_throughout(redis, full, renewed):
    """The lock is renewed to the run's TTL while a full re-import runs, so it never runs out under the run (the
    computed TTL can be shorter than a full re-import: the scrape's wait is an alert, not a stop). An ordinary run keeps
    today's behaviour: no renewal."""
    key = "ddp:pipeline_lock:us"
    token = await redis_store.acquire_state_lock("usa", redis_store.HANDOFF_TTL_S)
    observed = {}

    async def _scrape(jurisdiction, session_arg, root, timeout_s=None, config=None):
        redis.ttl[key] = 5  # the lock about to run out mid-run
        await asyncio.sleep(0.05)
        observed["ttl"] = redis.ttl[key]
        observed["nightly"] = await redis_store.acquire_state_lock("usa", 60)  # the 03:00 job firing now
        return {"success": True}

    p = _pipeline_patches([])
    with patch.object(scrape, "STATE_LOCK_RENEW_S", 0.01), patch.object(scrape, "_run_scrape", new=_scrape), \
            p[1], p[2], p[3], p[4]:
        await scrape.run_state_pipeline("usa", {}, {"enabled": True}, sessions=["118"], full=full, lock_token=token)
    ttl = scrape.pipeline_lock_ttl_s("usa", {}, {"enabled": True}, ["118"])
    assert (observed["ttl"] == ttl) is renewed
    assert observed["nightly"] is None  # refused while the run holds the lock
    assert key not in redis.data  # released at the end; the renewal stopped with the run


@pytest.mark.asyncio
async def test_a_lost_lock_stops_the_renewal_and_is_logged(redis):
    token = await redis_store.acquire_state_lock("usa", 60)
    redis.data["ddp:pipeline_lock:us"] = "someone-else"
    with patch.object(scrape, "STATE_LOCK_RENEW_S", 0.001), patch.object(scrape.logger, "error") as err:
        await asyncio.wait_for(scrape._keep_state_lock("usa", token, 100), timeout=1)
    err.assert_called_once()
    assert redis.ttl.get("ddp:pipeline_lock:us") != 100


# --- the collection command --------------------------------------------------------------------------------------


@pytest.mark.parametrize("full,expected", [
    (True, ["usa", "session=118", "chamber=lower", "full=true"]),
    (False, ["usa", "session=118", "chamber=lower"]),
])
def test_full_is_added_to_the_collection_command_only_never_to_the_load(full, expected):
    ecs = FakeEcsClient(run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
                        describe_responses=[_stopped_response(exit_code=0)])
    captured = {}

    def fake_subprocess(cmd, env):
        captured["cmd"] = cmd
        return FakeSubprocessResult(returncode=0)

    token = cst.FULL_COLLECTION.set(full)
    try:
        with patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url",
                   return_value=("postgresql://rds/openstates", "")):
            cst.run_cloud_scrape("usa", "session=118 chamber=lower", "/fake/root", _fargate_config(),
                                 ecs_client=ecs, subprocess_runner=fake_subprocess)
    finally:
        cst.FULL_COLLECTION.reset(token)
    assert ecs.run_task_calls[0]["overrides"]["containerOverrides"][0]["command"] == expected
    assert "full=true" not in captured["cmd"]  # the load's key and lock stay the session's own
