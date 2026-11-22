"""SYNC-186: a state whose manifest `enrollment.legbot_replica` is false is never handed to LegBot.

On 2026-11-22 North Dakota and Kentucky (both false) started real LegBot session runs after their first archives; only the
per-bill check, nested under replica_freshness_check_enabled, stopped every bill. Three gates now read the same answer
(`session_pipeline_runner._legbot_allowed_codes`, SYNC-142), each tested here against a stand-in manifest:

1. the archive hook (`openstates_archive._maybe_trigger_legbot_for_archive`) hands nothing off, on the Mac and on EC2;
2. `scraper_triggered_legbot.trigger_scraper_session_pipeline` refuses before taking the lock, whoever calls it;
3. the per-bill check in `_process_bill_inner` runs whether or not replica_freshness_check_enabled is on.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from structlog.testing import capture_logs

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.openstates_archive import _maybe_trigger_legbot_for_archive
from ddp_sync.pipelines.scraper_triggered_legbot import trigger_scraper_session_pipeline
from ddp_sync.pipelines.session_pipeline_runner import (
    run_legbot_pipeline,
    run_single_bill_full,
)
from tests import sync157_manifest
from tests.test_scraper_triggered_legbot import FakeRedisClient
from tests.test_session_pipeline_runner import (
    _CANDIDATE,
    _patch_coverage,
    _patch_lister,
)

_ND_CANDIDATE = {**_CANDIDATE, "gov_id": "HB 1001", "session_code": "69"}


@pytest.fixture(autouse=True)
def manifest_with_north_dakota_off(tmp_path, monkeypatch):
    """The stand-in manifest plus North Dakota with `legbot_replica: false`, as in ddp-open-states on 2026-11-22."""
    manifest = copy.deepcopy(sync157_manifest.MANIFEST)
    manifest["nd"] = copy.deepcopy(manifest["al"])
    manifest["nd"]["name"] = "North Dakota"
    assert manifest["nd"]["enrollment"]["legbot_replica"] is False and manifest["fl"]["enrollment"]["legbot_replica"]
    return sync157_manifest.use(tmp_path, monkeypatch, manifest)


def _archive_settings(**overrides) -> SyncSettings:
    defaults = {
        "legbot_scrape_completion_trigger_enabled": True,
        "legbot_scrape_completion_trigger_artifact_types": ["bill_summary"],
        "legbot_scrape_completion_trigger_limit": None,
        "legbot_scrape_completion_trigger_include_concept_statements": False,
        "legbot_scrape_completion_trigger_resolution_max_bills": 500,
        "cams_api_token": "fake-mac-cams-token",
    }
    defaults.update(overrides)
    return SyncSettings(**defaults)


def _ec2_settings() -> SyncSettings:
    return _archive_settings(
        cams_api_token="",
        rds_openstates_api_base="http://10.0.0.9:8002",
        rds_openstates_api_key="fake-rds-api-key",
        mac_ddp_sync_base_url="http://10.0.0.8:8001",
        mac_ddp_sync_api_key="fake-mac-ddp-sync-key",
    )


async def _run_archive_hook(code: str, settings: SyncSettings):
    with (
        patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=settings),
        patch("ddp_sync.services.local_openstates_client.resolve_touched_sessions",
              new=AsyncMock(return_value=["69"])) as resolve,
        patch("ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
              new=AsyncMock(return_value={"success": True})) as in_process,
        patch("ddp_sync.pipelines.openstates_archive._trigger_legbot_session_via_mac_wireguard",
              new=AsyncMock()) as over_wireguard,
        capture_logs() as logs,
    ):
        await _maybe_trigger_legbot_for_archive(code, datetime.now(UTC))
    return resolve, in_process, over_wireguard, logs


# ── gate 1: the archive hook ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("settings", [_archive_settings(), _ec2_settings()], ids=["mac", "ec2"])
async def test_the_archive_hook_hands_off_nothing_for_a_state_with_legbot_off_and_says_so_once(settings):
    resolve, in_process, over_wireguard, logs = await _run_archive_hook("nd", settings)
    resolve.assert_not_awaited()
    in_process.assert_not_awaited()
    over_wireguard.assert_not_awaited()
    skipped = [e for e in logs if e["event"] == "archiver_triggered_legbot_skipped_legbot_replica_off"]
    assert len(skipped) == 1 and skipped[0]["jurisdiction"] == "ND"


@pytest.mark.asyncio
@pytest.mark.parametrize("settings", [_archive_settings(), _ec2_settings()], ids=["mac", "ec2"])
async def test_the_archive_hook_still_hands_off_a_state_with_legbot_on(settings):
    resolve, in_process, over_wireguard, logs = await _run_archive_hook("fl", settings)
    resolve.assert_awaited_once()
    assert (in_process.await_count + over_wireguard.await_count) == 1
    assert not [e for e in logs if e["event"] == "archiver_triggered_legbot_skipped_legbot_replica_off"]


@pytest.mark.asyncio
async def test_the_archive_hook_hands_off_nothing_when_the_manifest_cannot_be_read(tmp_path, monkeypatch):
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path / "nowhere"))
    resolve, in_process, over_wireguard, _ = await _run_archive_hook("fl", _archive_settings())
    resolve.assert_not_awaited()
    in_process.assert_not_awaited()
    over_wireguard.assert_not_awaited()


# ── gate 2: the entry point, whoever calls it ────────────────────────────────────────────


@pytest.fixture
def fake_redis_store(monkeypatch):
    rs = MagicMock()
    rs._client = FakeRedisClient()
    monkeypatch.setattr("ddp_sync.services.redis_store.get_redis_store", lambda: rs)
    return rs


@pytest.mark.asyncio
@pytest.mark.parametrize("automated", [True, False], ids=["automated", "manual"])
@pytest.mark.parametrize("code", ["ND", "nd"])
async def test_the_entry_point_refuses_a_state_with_legbot_off_before_taking_the_lock(
    fake_redis_store, monkeypatch, automated, code
):
    monkeypatch.setattr("ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
                        lambda: _archive_settings(legbot_scrape_completion_trigger_lock_ttl_seconds=600))
    with patch("ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline", new=AsyncMock()) as run:
        result = await trigger_scraper_session_pipeline(
            code, "69", ["bill_summary"], False, None,
            include_concept_statements=False, require_trigger_enabled=automated,
        )
    assert result == {"success": False, "error": "legbot_replica_off"}
    run.assert_not_awaited()
    assert fake_redis_store._client.store == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["FL", "fl"])  # a manual caller may type lowercase
async def test_the_entry_point_still_runs_a_state_with_legbot_on(fake_redis_store, monkeypatch, code):
    monkeypatch.setattr("ddp_sync.pipelines.scraper_triggered_legbot.get_settings",
                        lambda: _archive_settings(legbot_scrape_completion_trigger_lock_ttl_seconds=600))
    with patch("ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
               new=AsyncMock(return_value={"results": []})) as run:
        result = await trigger_scraper_session_pipeline(
            code, "2026F", ["bill_summary"], False, None, include_concept_statements=False,
        )
    assert result["success"] is True
    run.assert_awaited_once()


def test_the_manual_route_answers_409_for_a_state_with_legbot_off(fake_redis_store):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    payload = {"jurisdiction_iso2": "nd", "session_code": "69", "artifact_types": ["bill_summary"],
               "include_org_research": False, "include_concept_statements": False, "retry_failed": False,
               "limit": 10}
    with patch("ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline", new=AsyncMock()) as run:
        response = TestClient(app).post("/trigger/bill-artifact-generation", json=payload)
    assert response.status_code == 409
    assert response.json()["detail"] == {"error": "legbot_replica_off", "jurisdiction_iso2": "ND"}
    run.assert_not_awaited()


# ── gate 3: per bill, whatever replica_freshness_check_enabled says ──────────────────────


def _runner_settings(freshness_on: bool):
    return patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_settings",
        return_value=SimpleNamespace(
            replica_freshness_check_enabled=freshness_on,
            replica_freshness_content_check_enabled=False,
            session_pipeline_concurrency=1,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("freshness_on", [True, False], ids=["freshness-on", "freshness-off"])
async def test_a_session_run_skips_every_bill_of_a_state_with_legbot_off(freshness_on):
    with _patch_lister([_ND_CANDIDATE]), _runner_settings(freshness_on), _patch_coverage(None) as coverage, patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact", new=AsyncMock()
    ) as artifact:
        result = await run_legbot_pipeline("nd", "69", ["bill_summary"], True, limit=10,
                                           include_concept_statements=False, retry_failed=False)
    coverage.assert_not_awaited()
    artifact.assert_not_awaited()
    assert result["results"][0]["error"] == "jurisdiction_not_on_rds_replica_allowlist: ND"


@pytest.mark.asyncio
async def test_a_single_bill_run_skips_a_state_with_legbot_off_with_the_freshness_check_off():
    with _runner_settings(False), _patch_coverage(None) as coverage, patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact", new=AsyncMock()
    ) as artifact:
        result = await run_single_bill_full(
            bill_openstates_id=_ND_CANDIDATE["bill_openstates_id"], jurisdiction_iso2="nd", session_code="69",
            gov_id="HB 1001", bill_source="archived", artifact_types=["bill_summary"], include_org_research=False,
            include_concept_statements=False, retry_failed=False,
        )
    coverage.assert_not_awaited()
    artifact.assert_not_awaited()
    assert result["error"] == "jurisdiction_not_on_rds_replica_allowlist: ND"


@pytest.mark.asyncio
async def test_a_state_with_legbot_on_reaches_the_coverage_read_with_the_freshness_check_off():
    with _patch_lister([_CANDIDATE]), _runner_settings(False), _patch_coverage(None) as coverage, patch(
        "ddp_sync.pipelines.session_pipeline_runner.get_current_version_identity", new=AsyncMock(return_value=None)
    ):
        await run_legbot_pipeline("fl", "2026F", ["bill_summary"], False, limit=10,
                                  include_concept_statements=False, retry_failed=False, dry_run=True)
    coverage.assert_awaited_once()
