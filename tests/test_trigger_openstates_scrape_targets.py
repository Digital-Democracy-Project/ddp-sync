"""SYNC-57: the manual scrape trigger's single-state targets come from the schedule config
(openstates_scrape.secondary.jurisdictions), not a hardcoded copy that can miss a state.

Mirrors test_trigger_grantbot_scrape_funders.py's TestClient shape.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router


def _post(target, openstates_scrape_config):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    scheduler = SimpleNamespace(_sync_config={"openstates_scrape": openstates_scrape_config})
    with (
        patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler),
        patch("ddp_sync.pipelines.openstates_scrape.run_single_scrape_job", new=AsyncMock()) as single,
        patch("ddp_sync.pipelines.openstates_scrape.run_fl_scrapes_job", new=AsyncMock()) as fl,
    ):
        resp = TestClient(app).post(f"/trigger/openstates-scrape/{target}")
    return resp, single, fl


def test_a_state_only_in_the_schedule_config_is_a_single_target():
    config = {"secondary": {"jurisdictions": ["va", "zz"]}}
    resp, single, _ = _post("zz", config)
    assert resp.status_code == 200
    assert resp.json() == {"status": "started", "target": "zz"}
    single.assert_awaited_once_with("zz", config)


def test_a_state_missing_from_the_config_is_404_and_the_list_comes_from_the_config():
    resp, single, _ = _post("va", {"secondary": {"jurisdictions": ["nc"]}})
    assert resp.status_code == 404
    detail = resp.json()["detail"]
    assert "Unknown target 'va'" in detail
    assert "nc" in detail.split("Available: ")[1].split(", ")
    single.assert_not_awaited()


def test_batch_targets_still_run_their_batch_job():
    resp, single, fl = _post("fl", {"secondary": {"jurisdictions": ["va"]}})
    assert resp.status_code == 200
    fl.assert_awaited_once()
    single.assert_not_awaited()


def test_nc_is_a_single_target_in_the_checked_in_schedule():
    config_path = Path(__file__).resolve().parents[1] / "config" / "sync_schedule.yaml"
    openstates_scrape = yaml.safe_load(config_path.read_text())["openstates_scrape"]
    resp, single, _ = _post("nc", openstates_scrape)
    assert resp.status_code == 200
    single.assert_awaited_once_with("nc", openstates_scrape)
