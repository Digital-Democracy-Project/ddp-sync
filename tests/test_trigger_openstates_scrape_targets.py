"""The manual scrape trigger's single-state targets.

SYNC-57 took them from the schedule config instead of a hardcoded copy; SYNC-152 takes them from the manifest's ONE
rule (status not paused or probing), the same one the scheduler registers jobs from, because the secondary-list reader
answered 404 for Pennsylvania (a nightly state) on 2026-11-10. A state target runs the same pipeline as its scheduled
job. A tiny manifest in a temp dir stands in for the ddp-open-states checkout.

Mirrors test_trigger_grantbot_scrape_funders.py's TestClient shape.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.services import jurisdiction_enrollment as je


def _entry(status="live"):
    return {"status": status, "tier": "secondary", "archive": {"enabled": True, "scheduled": True},
            "enrollment": {"cloud_path": True}}


MANIFEST = {"va": _entry(), "nc": _entry(), "fl": _entry(), "wa": _entry(), "us": _entry(),
            "pa": _entry("scraping"), "al": _entry("paused"), "zz": _entry("probing")}


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    je._cache.clear()
    je._logged.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(MANIFEST))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    monkeypatch.delenv(je.MODE_ENV, raising=False)
    return str(tmp_path)


def _post(target, openstates_scrape_config, archive_config=None):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    scheduler = SimpleNamespace(_sync_config={"openstates_scrape": openstates_scrape_config,
                                              "openstates_archive": archive_config or {}})
    with (
        patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler),
        patch("ddp_sync.api.routes.triggers._run_state_pipeline_in_background", new=AsyncMock()) as pipeline,
        patch("ddp_sync.pipelines.openstates_scrape.people_check_refusal", new=AsyncMock(return_value=None)),
        patch("ddp_sync.pipelines.openstates_scrape.run_patch_refresh_job", new=AsyncMock()) as patches,
    ):
        resp = TestClient(app).post(f"/trigger/openstates-scrape/{target}")
    return resp, pipeline, patches


@pytest.mark.parametrize("target", ["pa", "fl", "wa", "usa", "va", "nc"])
def test_every_scraped_state_is_a_target_and_runs_the_state_pipeline(target):
    """Acceptance 2: pa (and any scraped state) answers 202, not 404; fl/wa/usa are states now, not batch targets."""
    archive = {"enabled": True}
    resp, pipeline, _ = _post(target, {}, archive)
    assert resp.status_code == 202
    assert resp.json() == {"status": "started", "target": target}
    pipeline.assert_awaited_once_with(target, {}, archive, people_checked=True)


@pytest.mark.parametrize("target", ["al", "zz", "secondary", "us", "mi"])
def test_a_paused_probing_unknown_or_retired_target_is_404(target):
    resp, pipeline, _ = _post(target, {"secondary": {"jurisdictions": ["al", "mi"]}})  # a leftover yaml list is ignored
    assert resp.status_code == 404
    available = resp.json()["detail"].split("Available: ")[1].split(", ")
    assert {"pa", "fl", "usa", "va"} <= set(available) and "secondary" not in available and "al" not in available
    pipeline.assert_not_awaited()


def test_job_targets_still_run_their_job():
    resp, pipeline, patches = _post("patches", {})
    assert resp.status_code == 202
    patches.assert_awaited_once()
    pipeline.assert_not_awaited()


def test_pa_is_a_target_with_the_checked_in_schedule():
    config_path = Path(__file__).resolve().parents[1] / "config" / "sync_schedule.yaml"
    openstates_scrape = yaml.safe_load(config_path.read_text())["openstates_scrape"]
    resp, pipeline, _ = _post("pa", openstates_scrape)
    assert resp.status_code == 202
    pipeline.assert_awaited_once_with("pa", openstates_scrape, {}, people_checked=True)


@pytest.mark.asyncio
async def test_the_background_runner_logs_a_failed_run_instead_of_raising_into_the_server():
    from ddp_sync.api.routes import triggers
    from ddp_sync.pipelines.openstates_scrape import PipelineFailed

    failed = PipelineFailed("pa", [{"success": False, "jurisdiction": "pa"}], None)
    with patch("ddp_sync.pipelines.openstates_scrape.run_state_pipeline", new=AsyncMock(side_effect=failed)) as run, \
            patch.object(triggers.logger, "error") as log:
        await triggers._run_state_pipeline_in_background("pa", {}, {}, people_checked=True)
    run.assert_awaited_once_with("pa", {}, {}, people_checked=True)
    log.assert_called_once()
