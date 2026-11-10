"""SYNC-147: the first scrape of a state that is not live yet refuses to start when production holds no legislators for it.

Pilot #2's first scrapes loaded with 0 legislators (runbook rule 1 was text nobody checked), so every vote and sponsor was
stored with a name and no person. A tiny manifest in a temp dir stands in for the ddp-open-states checkout; the people
count is patched. Live, paused and probing states are never checked.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.pipelines import openstates_scrape as scrape
from ddp_sync.services import jurisdiction_enrollment as je
from ddp_sync.services import local_openstates_client as client

COUNT = "ddp_sync.services.local_openstates_client.count_current_people"
CLOUD = "ddp_sync.pipelines.openstates_scrape.run_cloud_scrape"


def _entry(status):
    return {"name": status, "status": status, "tier": "secondary", "enrollment": {"cloud_path": True}}


async def _pipeline(code, cfg, **kwargs):
    """SYNC-152: the manual trigger runs the state pipeline (run_single_scrape_job is gone). Archive and bookkeeping are
    faked; a failed run's PipelineFailed is turned back into a result so the people-check assertions read the same."""
    with patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=AsyncMock(return_value=None)), \
            patch.object(scrape, "_check_sustained_blocking", new=AsyncMock()), \
            patch.object(scrape, "_write_flow_status", new=AsyncMock()):
        try:
            return await scrape.run_state_pipeline(code, cfg, **kwargs)
        except scrape.PipelineFailed as e:
            return {"success": False, "results": e.results}


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    je._cache.clear()
    je._logged.clear()
    monkeypatch.delenv("OPENSTATES_ROOT", raising=False)


@pytest.fixture
def cfg(tmp_path):
    manifest = {
        "nc": _entry("live"), "al": _entry("paused"), "zz": _entry("probing"),
        "pa": _entry("scraping"), "ne": _entry("validating"), "la": _entry("archived"), "us": _entry("live"),
    }
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(manifest))
    return {
        "openstates_root": str(tmp_path),
        "cloud_path": {"enabled": True, "jurisdictions": ["nc", "pa", "ne", "la", "usa"]},
    }


# --- the check ----------------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["nc", "al", "zz", "unknown", "usa"])
async def test_live_paused_probing_and_unknown_states_are_never_checked(cfg, code):
    with patch(COUNT, new=AsyncMock(return_value=0)) as count:
        assert await scrape.people_check_refusal(code, cfg) is None
    count.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unreadable_manifest_checks_nothing(tmp_path):
    with patch(COUNT, new=AsyncMock(return_value=0)) as count:
        assert await scrape.people_check_refusal("pa", {"openstates_root": str(tmp_path / "nowhere")}) is None
    count.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["pa", "ne", "la"])
async def test_a_not_yet_live_state_with_no_legislators_is_refused_in_plain_words(cfg, code):
    with patch(COUNT, new=AsyncMock(return_value=0)):
        msg = await scrape.people_check_refusal(code, cfg)
    assert msg and "no legislators in production" in msg and f"os-people to-database {code}" in msg


@pytest.mark.asyncio
async def test_an_unreadable_count_also_refuses(cfg):
    with patch(COUNT, new=AsyncMock(return_value=None)):
        msg = await scrape.people_check_refusal("pa", cfg)
    assert msg and "cannot read the legislator count" in msg


@pytest.mark.asyncio
async def test_a_state_with_legislators_starts_exactly_as_before(cfg):
    with patch(COUNT, new=AsyncMock(return_value=253)):
        assert await scrape.people_check_refusal("pa", cfg) is None


# --- through the one funnel -----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_funnel_refuses_before_any_launch_when_there_are_no_legislators(cfg):
    with patch(COUNT, new=AsyncMock(return_value=0)), patch(CLOUD) as cloud, capture_logs() as logs:
        result = await scrape._run_scrape_impl("pa", None, cfg["openstates_root"], config=cfg)
    cloud.assert_not_called()
    assert result["success"] is False and result["refused"] == "no_legislators_in_production"
    assert any(l["event"].endswith("refused, no legislators in production") for l in logs)


@pytest.mark.asyncio
async def test_the_funnel_launches_normally_when_legislators_exist(cfg):
    with patch(COUNT, new=AsyncMock(return_value=253)), patch(CLOUD, return_value={"success": True}) as cloud:
        result = await scrape._run_scrape_impl("pa", None, cfg["openstates_root"], config=cfg)
    cloud.assert_called_once()
    assert result == {"success": True}


@pytest.mark.asyncio
async def test_a_live_state_launches_even_if_the_count_would_be_zero(cfg):
    with patch(COUNT, new=AsyncMock(return_value=0)) as count, patch(CLOUD, return_value={"success": True}) as cloud:
        await scrape._run_scrape_impl("nc", None, cfg["openstates_root"], config=cfg)
    count.assert_not_awaited()
    cloud.assert_called_once()


@pytest.mark.asyncio
async def test_the_manual_override_starts_it_anyway_logs_it_and_does_not_stick(cfg):
    with patch(COUNT, new=AsyncMock(return_value=0)) as count, patch(CLOUD, return_value={"success": True}) as cloud, \
            capture_logs() as logs:
        await _pipeline("pa", cfg, skip_people_check=True)
    count.assert_not_awaited()
    cloud.assert_called_once()
    assert any(l["event"] == "openstates_scrape: people check skipped on purpose" for l in logs)
    assert scrape._SKIP_PEOPLE_CHECK.get() is False                     # the override ended with the call
    with patch(COUNT, new=AsyncMock(return_value=0)), patch(CLOUD) as cloud2:
        result = await _pipeline("pa", cfg)          # the next, plain call is checked again
    cloud2.assert_not_called()
    assert result["success"] is False


# --- the manual trigger answers the person who pressed it -------------------------------------------------------


def _post(target, cfg, count, query=""):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    scheduler = SimpleNamespace(_sync_config={"openstates_scrape": cfg})
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch("ddp_sync.api.routes.triggers._run_state_pipeline_in_background", new=AsyncMock()) as single, \
            patch(COUNT, new=AsyncMock(return_value=count)), \
            patch("ddp_sync.services.redis_store.acquire_state_lock", new=AsyncMock(return_value="tok")):
        resp = TestClient(app).post(f"/trigger/openstates-scrape/{target}{query}")
    return resp, single


def test_the_trigger_answers_409_with_the_reason_and_does_not_start_the_job(cfg):
    resp, single = _post("pa", cfg, 0)
    assert resp.status_code == 409
    assert "no legislators in production" in resp.json()["detail"] and "skip_people_check=true" in resp.json()["detail"]
    single.assert_not_awaited()


def test_the_trigger_starts_a_state_that_has_legislators_exactly_as_before(cfg):
    resp, single = _post("pa", cfg, 253)
    assert resp.status_code == 202 and resp.json() == {"status": "started", "target": "pa"}
    single.assert_awaited_once_with("pa", cfg, {}, people_checked=True, lock_token="tok")   # one read, not two


def test_the_trigger_override_starts_it_and_says_so(cfg):
    resp, single = _post("pa", cfg, 0, "?skip_people_check=true")
    assert resp.status_code == 202 and resp.json()["people_check"] == "skipped on purpose"
    single.assert_awaited_once_with("pa", cfg, {}, skip_people_check=True, lock_token="tok")


def test_a_live_state_is_triggered_with_no_check(cfg):
    resp, single = _post("nc", cfg, 0)
    assert resp.status_code == 202
    single.assert_awaited_once_with("nc", cfg, {}, people_checked=True, lock_token="tok")


# --- the count itself: its own short timeout, at most one retry ---------------------------------------------------


class _FakeResp:
    def __init__(self, status=200, body=None):
        self.status_code, self._body = status, body

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


def _fake_http(script, seen):
    """A stand-in for httpx.AsyncClient: `script` is a list of responses or exceptions, one per get()."""

    class _C:
        def __init__(self, *a, **kw):
            seen["timeouts"].append(kw.get("timeout"))

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, params=None, headers=None):
            seen["gets"].append((url, params, headers))
            item = script[min(len(seen["gets"]) - 1, len(script) - 1)]
            if isinstance(item, Exception):
                raise item
            return item

    return _C


@pytest.mark.asyncio
@pytest.mark.parametrize("body,expected", [
    ({"pagination": {"total_items": 0}}, 0), ({"pagination": {"total_items": 253}}, 253), ({"pagination": {}}, None),
    ({"pagination": {"total_items": "253"}}, None), ({"pagination": {"total_items": True}}, None),
    ({"pagination": {"total_items": -1}}, None), (["not", "a", "dict"], None),
])
async def test_count_current_people_reads_total_items_and_says_none_when_it_cannot(body, expected):
    seen = {"gets": [], "timeouts": []}
    with patch.object(client.httpx, "AsyncClient", _fake_http([_FakeResp(200, body)], seen)), \
            patch.object(client.asyncio, "sleep", new=AsyncMock()):
        assert await client.count_current_people("pa", api_base="http://x", api_key="k") == expected
    assert seen["gets"][0][1] == {"jurisdiction": "PA", "per_page": "1"} and seen["gets"][0][2] == {"x-api-key": "k"}


@pytest.mark.asyncio
async def test_count_current_people_with_no_base_url_is_unreadable_and_makes_no_call():
    seen = {"gets": [], "timeouts": []}
    with patch.object(client.httpx, "AsyncClient", _fake_http([], seen)):
        assert await client.count_current_people("pa", api_base="") is None
    assert seen["gets"] == []


@pytest.mark.asyncio
async def test_the_count_has_a_short_timeout_of_its_own_and_at_most_one_retry():
    seen = {"gets": [], "timeouts": []}
    boom = RuntimeError("down")
    with patch.object(client.httpx, "AsyncClient", _fake_http([boom, boom, boom], seen)), \
            patch.object(client.asyncio, "sleep", new=AsyncMock()) as nap:
        assert await client.count_current_people("pa", api_base="http://x") is None
    assert len(seen["gets"]) == 2 and nap.await_count == 1            # one retry, not three attempts
    assert all(t is not None and t <= 10 for t in seen["timeouts"])  # not the shared client's 120 s


@pytest.mark.asyncio
async def test_a_failed_first_read_is_retried_once_and_the_second_answer_is_used():
    seen = {"gets": [], "timeouts": []}
    script = [RuntimeError("blip"), _FakeResp(200, {"pagination": {"total_items": 7}})]
    with patch.object(client.httpx, "AsyncClient", _fake_http(script, seen)), patch.object(client.asyncio, "sleep", new=AsyncMock()):
        assert await client.count_current_people("pa", api_base="http://x") == 7
    assert len(seen["gets"]) == 2


@pytest.mark.asyncio
async def test_an_error_status_and_a_raising_client_are_unreadable_not_a_crash():
    seen = {"gets": [], "timeouts": []}
    with patch.object(client.httpx, "AsyncClient", _fake_http([_FakeResp(500, None)], seen)), \
            patch.object(client.asyncio, "sleep", new=AsyncMock()):
        assert await client.count_current_people("pa", api_base="http://x") is None


@pytest.mark.asyncio
async def test_a_raising_client_still_gives_the_manual_trigger_its_409_and_the_funnel_its_structured_refusal(cfg):
    seen = {"gets": [], "timeouts": []}
    with patch.object(client.httpx, "AsyncClient", _fake_http([RuntimeError("boom")], seen)), \
            patch.object(client.asyncio, "sleep", new=AsyncMock()), patch(CLOUD) as cloud:
        result = await scrape._run_scrape_impl("pa", None, cfg["openstates_root"], config=cfg)
        cloud.assert_not_called()
        assert result["success"] is False and "cannot read the legislator count" in result["error"]
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[api_key_auth] = lambda: "test-token"
        scheduler = SimpleNamespace(_sync_config={"openstates_scrape": cfg})
        with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler):
            resp = TestClient(app).post("/trigger/openstates-scrape/pa")
    assert resp.status_code == 409 and "cannot read the legislator count" in resp.json()["detail"]


# --- which database it reads: production's, not a stand-in -----------------------------------------------------------


def _settings(rds_base="", local_base="http://localhost:8002"):
    return SimpleNamespace(rds_openstates_api_base=rds_base, rds_openstates_api_key="rds-key",
                           local_openstates_api_base=local_base, local_openstates_api_key="local-key")


@pytest.mark.asyncio
async def test_it_reads_the_rds_facing_api_v3_when_one_is_configured_and_never_the_local_one(cfg):
    with patch("ddp_sync.config.get_settings", return_value=_settings("http://rds:8002")), \
            patch(COUNT, new=AsyncMock(return_value=253)) as count, capture_logs() as logs:
        assert await scrape.people_check_refusal("pa", cfg) is None
    assert count.await_args.kwargs == {"api_base": "http://rds:8002", "api_key": "rds-key"}
    assert all("localhost" not in str(c) for c in count.await_args_list)          # fails if the local base is read here
    assert [l["source"] for l in logs if l["event"] == "openstates_scrape: people count read"] == ["rds"]


@pytest.mark.asyncio
async def test_it_falls_back_to_the_local_api_v3_only_when_the_rds_base_is_empty(cfg):
    with patch("ddp_sync.config.get_settings", return_value=_settings("")), \
            patch(COUNT, new=AsyncMock(return_value=253)) as count, capture_logs() as logs:
        assert await scrape.people_check_refusal("pa", cfg) is None
    assert count.await_args.kwargs == {"api_base": "http://localhost:8002", "api_key": "local-key"}
    assert [l["source"] for l in logs if l["event"] == "openstates_scrape: people count read"] == ["local"]


@pytest.mark.asyncio
async def test_a_zero_from_the_rds_base_refuses_even_though_the_local_copy_has_people(cfg):
    """The pilot #2 failure: people on the Mac's copy, none in production."""
    async def by_base(code, *, api_base, api_key=""):
        return 0 if "rds" in api_base else 257

    with patch("ddp_sync.config.get_settings", return_value=_settings("http://rds:8002")), patch(COUNT, new=by_base):
        msg = await scrape.people_check_refusal("pa", cfg)
    assert msg and "no legislators in production" in msg


# --- one read, not two ----------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_job_the_trigger_already_checked_does_not_read_the_count_again(cfg):
    with patch(COUNT, new=AsyncMock(return_value=0)) as count, patch(CLOUD, return_value={"success": True}) as cloud:
        await _pipeline("pa", cfg, people_checked=True)
    count.assert_not_awaited()
    cloud.assert_called_once()
    assert scrape._PEOPLE_ALREADY_CHECKED.get() is False          # not sticky


@pytest.mark.asyncio
async def test_a_scheduled_run_with_no_trigger_still_reads_the_count_in_the_funnel(cfg):
    with patch(COUNT, new=AsyncMock(return_value=0)) as count, patch(CLOUD) as cloud:
        result = await scrape._run_scrape_impl("pa", None, cfg["openstates_root"], config=cfg)
    count.assert_awaited_once()
    cloud.assert_not_called()
    assert result["success"] is False


@pytest.mark.asyncio
async def test_the_markers_are_cleared_even_when_the_job_raises_so_a_later_scheduled_run_still_checks(cfg):
    with patch(CLOUD, side_effect=RuntimeError("launch failed")), patch(COUNT, new=AsyncMock(return_value=253)):
        result = await _pipeline("pa", cfg, skip_people_check=True, people_checked=True)
    assert result["success"] is False and "launch failed" in result["results"][0]["error"]
    assert scrape._PEOPLE_ALREADY_CHECKED.get() is False and scrape._SKIP_PEOPLE_CHECK.get() is False
    with patch(COUNT, new=AsyncMock(return_value=0)) as count, patch(CLOUD) as cloud:
        result = await scrape._run_scrape_impl("pa", None, cfg["openstates_root"], config=cfg)
    count.assert_awaited_once()
    cloud.assert_not_called()
    assert result["success"] is False
