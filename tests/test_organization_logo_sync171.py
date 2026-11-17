"""SYNC-171: the logo catch-up runs through the service (202 + run_id, one run at a time), never records a verdict the judge
did not give, stops with one alert on a run of judge failures, and waits out Wikidata's 429s."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.pipelines import organization_logo as ol
from ddp_sync.services.legbot_client import LegBotDispatchError
from tests.test_organization_logo import OWN, _network, _state, fake_dns, png  # noqa: F401 -- fake_dns is a fixture
from tests.test_organization_logo_catch_up import FakeRedis

_L = "ddp_sync.pipelines.organization_logo"
_ALERT = "ddp_sync.slack_alerts.post_alert"
PAGE = '<img class="logo" src="/a-logo.png"><img class="logo" src="/b-logo.png">'
ROUTES = {"https://www.sierraclub.org/a-logo.png": (200, "image/png", png()),
          "https://www.sierraclub.org/b-logo.png": (200, "image/png", png(color=(200, 20, 20, 255))),
          "https://www.sierraclub.org/": (200, "text/html", PAGE)}


@pytest.fixture(autouse=True)
def fake_redis():
    from ddp_sync.services.redis_store import get_redis_store

    client = FakeRedis()
    with patch.object(get_redis_store(), "_client", client):
        yield client


async def _find(state, judge):
    write = AsyncMock(return_value={})
    with patch(f"{_L}.get_organization_logo_state", new=AsyncMock(return_value=state)), \
            patch(f"{_L}.write_organization_logo", new=write), patch(f"{_L}.dispatch_judge_logo", new=judge):
        async with _network(ROUTES) as client:
            result = await ol.find_and_store_organization_logo(organization_id=7, org_name="Sierra Club", client=client)
    return result, write


# ---- never a verdict the judge did not give ----

@pytest.mark.asyncio
async def test_an_organization_whose_candidates_all_failed_to_be_judged_gets_no_write():
    judge = AsyncMock(side_effect=LegBotDispatchError("All connection attempts failed"))
    result, write = await _find(_state(), judge)
    assert result["outcome"] == "judge_failed"
    assert result["judge_error"] == "All connection attempts failed"
    write.assert_not_awaited()  # not "none"


@pytest.mark.asyncio
async def test_a_chapter_whose_own_candidates_were_not_judged_does_not_fall_back_to_its_parents_logo():
    judge = AsyncMock(side_effect=LegBotDispatchError("All connection attempts failed"))
    result, write = await _find(_state(parent={"id": 1, "name": "Sierra Club National", "has_logo": True}), judge)
    assert result["outcome"] == "judge_failed"
    write.assert_not_awaited()  # not "parent"


@pytest.mark.asyncio
async def test_one_failed_judgement_does_not_stop_a_later_accepted_logo_being_stored():
    judge = AsyncMock(side_effect=[LegBotDispatchError("ended with status=failed"), {"answer": OWN, "backend": "mlx"}])
    result, write = await _find(_state(), judge)
    assert result["outcome"] == "website"
    assert write.await_args.kwargs["action"] == "set"


# ---- the catch-up: organization ids, and a run of judge failures ----

def _outcome(organization_id, outcome="none", **extra):
    return {"organization_id": organization_id, "outcome": outcome, "source": None, "rejected": 0, "skipped": {}, **extra}


@pytest.mark.asyncio
async def test_organization_ids_with_force_re_check_exactly_those_and_not_the_brokers_list():
    find = AsyncMock(side_effect=lambda **kw: _outcome(kw["organization_id"]))
    queue = AsyncMock()
    with patch("ddp_sync.services.broker_client.list_organizations_needing_logo", new=queue), \
            patch(f"{_L}.find_and_store_organization_logo", new=find):
        report = await ol.backfill_organization_logos(organization_ids=[5, 9, 5, 3], force=True, pause_seconds=0)
    queue.assert_not_awaited()
    assert [c.kwargs["organization_id"] for c in find.await_args_list] == [5, 9, 3]  # each once, in order
    assert {c.kwargs["force"] for c in find.await_args_list} == {True}
    assert (report["looked"], report["due"]) == (3, 3)


@pytest.mark.asyncio
async def test_a_run_of_judge_failures_stops_the_catch_up_with_exactly_one_alert():
    ids = list(range(1, 30))
    find = AsyncMock(side_effect=[_outcome(1, "website")] + [
        _outcome(i, "judge_failed", judge_error=f"down {i}") for i in ids[1:]])
    with patch(f"{_L}.find_and_store_organization_logo", new=find), patch(_ALERT) as alert:
        report = await ol.backfill_organization_logos(organization_ids=ids, pause_seconds=0)
    assert report["stopped"] == "judge_failing"
    assert find.await_count == 1 + ol.JUDGE_FAILURE_STOP  # stopped at the tenth in a row, the rest untouched
    assert report["judge_error"] == "down 2"  # the first of the run
    alert.assert_called_once()
    assert f"{ol.JUDGE_FAILURE_STOP} organizations in a row" in alert.call_args.args[0]
    assert "down 2" in alert.call_args.args[0]


@pytest.mark.asyncio
async def test_a_judged_organization_breaks_the_run_of_failures():
    pattern = ["judge_failed"] * (ol.JUDGE_FAILURE_STOP - 1) + ["none"] + ["judge_failed"] * (ol.JUDGE_FAILURE_STOP - 1)
    find = AsyncMock(side_effect=[_outcome(i, o, judge_error="x") for i, o in enumerate(pattern)])
    with patch(f"{_L}.find_and_store_organization_logo", new=find), patch(_ALERT) as alert:
        report = await ol.backfill_organization_logos(organization_ids=list(range(len(pattern))), pause_seconds=0)
    assert report["stopped"] is None
    alert.assert_not_called()


@pytest.mark.asyncio
async def test_the_shared_entry_adds_no_second_alert_when_the_backfill_stopped_for_judge_failures():
    report = {"looked": 10, "due": 10, "waited_seconds": 0, "stopped": "judge_failing", "judge_error": "down",
              "outcomes": {"judge_failed": 10}, "sources": {}, "rejected": 0, "skipped": {}}
    with patch(f"{_L}.backfill_organization_logos", new=AsyncMock(return_value=report)), patch(_ALERT) as alert:
        await ol.run_organization_logo_catch_up_job(limit=None, trigger="manual")
    alert.assert_not_called()


# ---- Wikidata 429 ----

@pytest.mark.asyncio
async def test_a_429_from_wikidata_waits_as_asked_and_retries():
    answers = iter([httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200, json={"search": [{"id": "Q1"}]})])
    sleep = AsyncMock()
    with patch(f"{_L}.asyncio.sleep", new=sleep):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: next(answers))) as client:
            body = await ol._json(client, ol.WIKIDATA_API, {"action": "wbsearchentities"})
    assert body == {"search": [{"id": "Q1"}]}  # not counted commons_unreachable
    sleep.assert_awaited_once_with(7.0)


@pytest.mark.asyncio
async def test_wikimedia_429s_are_waited_out_then_counted_unreachable_only_after_the_retries():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "120"})

    sleep = AsyncMock()
    with patch(f"{_L}.asyncio.sleep", new=sleep):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(ol.LogoFetchError):
                await ol._json(client, ol.WIKIDATA_API, {"action": "wbsearchentities"})
    assert len(calls) == ol.WIKIMEDIA_RETRIES + 1
    assert [c.args[0] for c in sleep.await_args_list] == [ol.WIKIMEDIA_MAX_WAIT_SECONDS] * ol.WIKIMEDIA_RETRIES  # capped


# ---- the trigger ----

def _post(query=""):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    with patch(f"{_L}.run_organization_logo_catch_up_job", new=AsyncMock(return_value={})) as job:
        resp = TestClient(app).post(f"/trigger/organization-logo-backfill{query}")
    return resp, job


def test_the_trigger_answers_202_with_a_run_id_and_runs_the_catch_up_with_the_given_arguments(fake_redis):
    resp, job = _post("?limit=20&older_than_days=90&dry_run=true&organization_ids=4,8,15&force=true")
    assert resp.status_code == 202
    run_id = resp.json()["run_id"]
    assert run_id.startswith("organization-logo-backfill-")
    job.assert_awaited_once_with(limit=20, older_than_days=90, dry_run=True, organization_ids=[4, 8, 15], force=True,
                                 run_id=run_id, lease_held=True, trigger="manual")
    assert fake_redis.data[ol.CATCH_UP_LOCK_KEY] == run_id.encode()  # the lease is this run's (the faked job did not release it)


def test_a_second_start_while_one_runs_answers_409_with_the_running_id(fake_redis):
    fake_redis.data[ol.CATCH_UP_LOCK_KEY] = b"organization-logo-catch-up-abc"
    resp, job = _post()
    assert resp.status_code == 409
    assert resp.json()["detail"]["current_run_id"] == "organization-logo-catch-up-abc"
    job.assert_not_awaited()


def test_no_redis_means_no_start():
    from ddp_sync.services.redis_store import get_redis_store

    with patch.object(get_redis_store(), "_client", None):
        resp, job = _post()
    assert resp.status_code == 503
    job.assert_not_awaited()


@pytest.mark.parametrize("query", ["?organization_ids=4,x", "?organization_ids=,", "?limit=0", "?older_than_days=0"])
def test_bad_arguments_are_refused_before_anything_starts(query, fake_redis):
    resp, job = _post(query)
    assert resp.status_code == 422
    job.assert_not_awaited()
    assert ol.CATCH_UP_LOCK_KEY not in fake_redis.data


@pytest.mark.asyncio
async def test_the_weekly_job_does_nothing_while_a_manual_run_holds_the_lease(fake_redis):
    fake_redis.data[ol.CATCH_UP_LOCK_KEY] = b"organization-logo-backfill-manual"
    backfill = AsyncMock()
    with patch(f"{_L}.backfill_organization_logos", new=backfill):
        result = await ol.run_organization_logo_catch_up_job(limit=1500)
    assert result == {"skipped": "already_running", "current_run_id": "organization-logo-backfill-manual"}
    backfill.assert_not_awaited()
    assert fake_redis.data[ol.CATCH_UP_LOCK_KEY] == b"organization-logo-backfill-manual"  # not released by the skipper


@pytest.mark.asyncio
async def test_a_run_releases_only_its_own_lease(fake_redis):
    async def backfill(**kw):
        fake_redis.data[ol.CATCH_UP_LOCK_KEY] = b"someone-else"  # our lease lapsed and another run took it
        return {"looked": 0, "due": 0, "waited_seconds": 0, "stopped": None, "outcomes": {}, "sources": {}}

    with patch(f"{_L}.backfill_organization_logos", side_effect=backfill):
        await ol.run_organization_logo_catch_up_job(limit=5)
    assert fake_redis.data[ol.CATCH_UP_LOCK_KEY] == b"someone-else"


@pytest.mark.asyncio
async def test_the_release_cannot_delete_a_lease_that_changed_hands_between_a_check_and_a_delete(fake_redis):
    """A check-then-delete release would read "mine", then delete the newer run's lease taken in between. Any read during
    the release hands the lease to a newer run; it must survive (the release is one atomic owner-checked step)."""
    real_get = fake_redis.get
    handed_over = []

    async def get_then_lose(key):
        value = await real_get(key)
        fake_redis.data[key] = b"newer-run"  # the lease lapses and another run takes it right after the read
        handed_over.append(True)
        return value

    async def backfill(**kw):
        fake_redis.get = get_then_lose
        return {"looked": 0, "due": 0, "waited_seconds": 0, "stopped": None, "outcomes": {}, "sources": {}}

    with patch(f"{_L}.backfill_organization_logos", side_effect=backfill):
        await ol.run_organization_logo_catch_up_job(limit=5)
    if handed_over:
        assert fake_redis.data.get(ol.CATCH_UP_LOCK_KEY) == b"newer-run"
    else:
        assert ol.CATCH_UP_LOCK_KEY not in fake_redis.data  # this run's own lease, released


@pytest.mark.asyncio
async def test_the_lease_is_renewed_only_while_this_run_holds_it(fake_redis):
    fake_redis.data[ol.CATCH_UP_LOCK_KEY] = b"run-a"
    calls = []
    real_eval = fake_redis.eval

    async def eval_spy(script, numkeys, key, owner, *args):
        calls.append(owner)
        return await real_eval(script, numkeys, key, owner, *args)

    fake_redis.eval = eval_spy
    sleeps = iter([None, None, asyncio.CancelledError()])

    async def sleep(_):
        value = next(sleeps)
        if isinstance(value, BaseException):
            raise value

    with patch(f"{_L}.asyncio.sleep", new=sleep), patch(f"{_L}.logger") as log:
        fake_redis.data[ol.CATCH_UP_LOCK_KEY] = b"run-b"  # another run holds it now
        with pytest.raises(asyncio.CancelledError):
            await ol._renew_catch_up_lease("run-a")
    assert calls == ["run-a", "run-a"]
    assert fake_redis.data[ol.CATCH_UP_LOCK_KEY] == b"run-b"  # never extended or touched
    assert [c.args[0] for c in log.error.call_args_list] == ["organization_logo_catch_up_lease_lost"] * 2
