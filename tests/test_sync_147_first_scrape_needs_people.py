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
        "secondary": {"jurisdictions": ["nc", "pa", "ne", "la"]},
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
        await scrape.run_single_scrape_job("pa", cfg, skip_people_check=True)
    count.assert_not_awaited()
    cloud.assert_called_once()
    assert any(l["event"] == "openstates_scrape: people check skipped on purpose" for l in logs)
    assert scrape._SKIP_PEOPLE_CHECK.get() is False                     # the override ended with the call
    with patch(COUNT, new=AsyncMock(return_value=0)), patch(CLOUD) as cloud2:
        result = await scrape.run_single_scrape_job("pa", cfg)          # the next, plain call is checked again
    cloud2.assert_not_called()
    assert result["success"] is False


# --- the manual trigger answers the person who pressed it -------------------------------------------------------


def _post(target, cfg, count, query=""):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    scheduler = SimpleNamespace(_sync_config={"openstates_scrape": cfg})
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch("ddp_sync.pipelines.openstates_scrape.run_single_scrape_job", new=AsyncMock()) as single, \
            patch(COUNT, new=AsyncMock(return_value=count)):
        resp = TestClient(app).post(f"/trigger/openstates-scrape/{target}{query}")
    return resp, single


def test_the_trigger_answers_409_with_the_reason_and_does_not_start_the_job(cfg):
    resp, single = _post("pa", cfg, 0)
    assert resp.status_code == 409
    assert "no legislators in production" in resp.json()["detail"] and "skip_people_check=true" in resp.json()["detail"]
    single.assert_not_awaited()


def test_the_trigger_starts_a_state_that_has_legislators_exactly_as_before(cfg):
    resp, single = _post("pa", cfg, 253)
    assert resp.status_code == 200 and resp.json() == {"status": "started", "target": "pa"}
    single.assert_awaited_once_with("pa", cfg)


def test_the_trigger_override_starts_it_and_says_so(cfg):
    resp, single = _post("pa", cfg, 0, "?skip_people_check=true")
    assert resp.status_code == 200 and resp.json()["people_check"] == "skipped on purpose"
    single.assert_awaited_once_with("pa", cfg, skip_people_check=True)


def test_a_live_state_is_triggered_with_no_check(cfg):
    resp, single = _post("nc", cfg, 0)
    assert resp.status_code == 200
    single.assert_awaited_once_with("nc", cfg)


# --- the count itself -------------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("answer,expected", [
    ({"pagination": {"total_items": 0}}, 0), ({"pagination": {"total_items": 253}}, 253), (None, None),
    ({"pagination": {}}, None), ({"pagination": {"total_items": "253"}}, None), ({"pagination": {"total_items": True}}, None),
    ({"pagination": {"total_items": -1}}, None),
])
async def test_count_current_people_reads_total_items_and_says_none_when_it_cannot(answer, expected):
    with patch.object(client, "_get_json_with_retry", new=AsyncMock(return_value=answer)) as get:
        assert await client.count_current_people("pa", api_base="http://x", api_key="k") == expected
    assert get.await_args.args[1] == {"jurisdiction": "PA", "per_page": "1"}


@pytest.mark.asyncio
async def test_count_current_people_with_no_base_url_is_unreadable():
    assert await client.count_current_people("pa", api_base="") is None


@pytest.mark.asyncio
async def test_a_client_that_raises_is_an_unreadable_count_not_a_crash():
    with patch.object(client, "_get_json_with_retry", new=AsyncMock(side_effect=RuntimeError("boom"))):
        assert await client.count_current_people("pa", api_base="http://x") is None


@pytest.mark.asyncio
async def test_a_raising_client_still_gives_the_manual_trigger_its_409_and_the_funnel_its_structured_refusal(cfg):
    with patch.object(client, "_get_json_with_retry", new=AsyncMock(side_effect=RuntimeError("boom"))), patch(CLOUD) as cloud:
        result = await scrape._run_scrape_impl("pa", None, cfg["openstates_root"], config=cfg)
    cloud.assert_not_called()
    assert result["success"] is False and "cannot read the legislator count" in result["error"]
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    scheduler = SimpleNamespace(_sync_config={"openstates_scrape": cfg})
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch.object(client, "_get_json_with_retry", new=AsyncMock(side_effect=RuntimeError("boom"))):
        resp = TestClient(app).post("/trigger/openstates-scrape/pa")
    assert resp.status_code == 409 and "cannot read the legislator count" in resp.json()["detail"]
