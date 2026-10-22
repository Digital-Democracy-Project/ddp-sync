"""OPEN-328: the dual-read of ddp-open-states' jurisdictions.yaml.

A tiny manifest in a temp dir stands in for the ddp-open-states checkout. The contract under test: shadow mode
(the default) never changes an answer, authoritative mode returns the manifest's, a manifest that cannot be
read degrades to the yaml's answer and never raises, and every disagreement is logged with the manifest's mtime.
"""

from __future__ import annotations

import os
import time

import pytest
import yaml
from structlog.testing import capture_logs

from ddp_sync.pipelines import openstates_archive, openstates_scrape
from ddp_sync.services import jurisdiction_enrollment as je


def _entry(tier="secondary", status="live", archive=True, embedding=False, search_refresh=False, cloud_path=False):
    return {
        "tier": tier,
        "status": status,
        "archive": {"enabled": archive},
        "enrollment": {"embedding": embedding, "search_refresh": search_refresh, "cloud_path": cloud_path},
    }


# opt in: point OPENSTATES_ROOT at a ddp-open-states checkout on main (read at import, before the fixture clears it)
_REAL_CHECKOUT = os.environ.get("OPENSTATES_ROOT", "")

MANIFEST = {
    "fl": _entry("primary", embedding=True, search_refresh=True, cloud_path=True),
    "us": _entry("primary", embedding=True, search_refresh=True, cloud_path=True),
    "nc": _entry("secondary", search_refresh=True, cloud_path=True),
    "al": _entry("secondary", status="paused"),
}


@pytest.fixture(autouse=True)
def _fresh_state(monkeypatch):
    je._cache.clear()
    je._logged.clear()
    monkeypatch.delenv(je.MODE_ENV, raising=False)
    monkeypatch.delenv("OPENSTATES_ROOT", raising=False)


@pytest.fixture
def root(tmp_path):
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(MANIFEST))
    return str(tmp_path)


# --- what the manifest says -----------------------------------------------------------------------------

def test_each_feature_is_read_from_the_manifest(root):
    assert je.manifest_codes("archive", root) == ["al", "fl", "nc", "us"]
    assert je.manifest_codes("embedding", root) == ["fl", "us"]
    assert je.manifest_codes("search_refresh", root) == ["fl", "nc", "us"]


def test_the_federal_jurisdiction_is_usa_on_the_scrape_side_and_us_on_the_archive_side(root):
    assert "usa" in je.manifest_codes("cloud_path", root) and "us" not in je.manifest_codes("cloud_path", root)
    assert je.manifest_codes("scrape_primary", root) == ["fl", "usa"]
    assert "us" in je.manifest_codes("archive", root)


def test_a_paused_jurisdiction_is_on_no_scrape_list(root):
    assert "al" not in je.manifest_codes("scrape_secondary", root)
    assert je.manifest_codes("scrape_secondary", root) == ["nc"]


# --- never raises; degrades to the yaml ------------------------------------------------------------------

def test_a_missing_manifest_gives_none_and_logs_once(tmp_path):
    with capture_logs() as logs:
        assert je.manifest_codes("archive", str(tmp_path)) is None
        assert je.manifest_codes("archive", str(tmp_path)) is None
    assert [e["event"] for e in logs].count("jurisdiction_manifest_unavailable") == 1


def test_an_invalid_manifest_gives_none(tmp_path):
    (tmp_path / "jurisdictions.yaml").write_text("not: [valid")
    with capture_logs() as logs:
        assert je.manifest_codes("archive", str(tmp_path)) is None
    assert any(e["event"] == "jurisdiction_manifest_unusable" for e in logs)


def test_a_manifest_missing_a_field_gives_none_for_that_feature(tmp_path):
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({"fl": {"tier": "primary", "status": "live"}}))
    assert je.manifest_codes("archive", str(tmp_path)) is None


def test_enrolled_with_an_unreadable_manifest_returns_the_yaml_list_unchanged(tmp_path):
    assert je.enrolled("archive", ["FL", " nc "], str(tmp_path)) == ["fl", "nc"]


def test_the_manifest_is_reread_when_the_file_changes(root):
    assert je.manifest_codes("embedding", root) == ["fl", "us"]
    changed = dict(MANIFEST, nc=_entry(embedding=True))
    path = os.path.join(root, "jurisdictions.yaml")
    with open(path, "w") as f:
        f.write(yaml.safe_dump(changed))
    os.utime(path, (time.time() + 5, time.time() + 5))
    assert je.manifest_codes("embedding", root) == ["fl", "nc", "us"]


# --- enrolled(): shadow vs authoritative -----------------------------------------------------------------

def test_shadow_mode_returns_the_yaml_list_even_when_the_manifest_differs_and_logs_it(root):
    with capture_logs() as logs:
        assert je.enrolled("embedding", ["fl"], root) == ["fl"]
    [event] = [e for e in logs if e["event"] == "jurisdiction_manifest_disagrees"]
    assert event["feature"] == "embedding" and event["only_in_manifest"] == ["us"] and event["only_in_yaml"] == []
    assert event["mode"] == "shadow" and event["manifest_mtime"]


def test_a_repeated_disagreement_is_logged_once(root):
    with capture_logs() as logs:
        je.enrolled("embedding", ["fl"], root)
        je.enrolled("embedding", ["fl"], root)
    assert [e["event"] for e in logs].count("jurisdiction_manifest_disagrees") == 1


def test_agreement_logs_nothing_and_returns_the_yaml_list(root):
    with capture_logs() as logs:
        assert je.enrolled("embedding", ["us", "fl"], root) == ["us", "fl"]
    assert not [e for e in logs if e["event"] == "jurisdiction_manifest_disagrees"]


def test_authoritative_mode_returns_the_manifest_list(root, monkeypatch):
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    assert je.enrolled("embedding", ["fl"], root) == ["fl", "us"]


def test_authoritative_mode_still_returns_the_yaml_list_when_the_manifest_is_unreadable(tmp_path, monkeypatch):
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    assert je.enrolled("embedding", ["fl"], str(tmp_path)) == ["fl"]


def test_a_yaml_with_no_list_falls_back_to_the_manifest_in_either_mode(root, monkeypatch):
    assert je.enrolled("embedding", None, root) == ["fl", "us"]
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    assert je.enrolled("embedding", None, root) == ["fl", "us"]


def test_no_yaml_list_and_no_manifest_returns_none_so_the_caller_uses_its_own_default(tmp_path):
    assert je.enrolled("archive", None, str(tmp_path)) is None


def test_an_unrecognised_mode_is_shadow(root, monkeypatch):
    monkeypatch.setenv(je.MODE_ENV, "yes please")
    assert je.enrolled("embedding", ["fl"], root) == ["fl"]


# --- the whole-config comparison (startup, and the OPEN-330 daily check) ---------------------------------

AGREEING_CONFIG = {
    "openstates_archive": {
        "jurisdictions": ["fl", "us", "nc", "al"],
        "knowledge_base_embedding": {"jurisdictions": ["fl", "us"]},
        "bill_search_refresh": {"jurisdictions": ["us", "fl", "nc"]},
    },
    "openstates_scrape": {
        "primary": {"fl": {"enabled": True}, "usa": {"enabled": True}, "wa": {"enabled": False}},
        "secondary": {"jurisdictions": ["nc"]},
        "cloud_path": {"jurisdictions": ["fl", "usa", "nc"]},
    },
}


def test_a_config_that_agrees_reports_nothing(root):
    assert je.config_disagreements(AGREEING_CONFIG, root) == []
    with capture_logs() as logs:
        assert je.log_config_disagreements(AGREEING_CONFIG, root) == 0
    assert [e["event"] for e in logs] == ["jurisdiction_manifest_agrees"]


@pytest.mark.parametrize(
    "mutate,feature,only_yaml,only_manifest",
    [
        (lambda c: c["openstates_archive"]["jurisdictions"].remove("nc"), "archive", [], ["nc"]),
        (lambda c: c["openstates_archive"]["knowledge_base_embedding"]["jurisdictions"].append("nc"), "embedding", ["nc"], []),
        (lambda c: c["openstates_archive"]["bill_search_refresh"]["jurisdictions"].remove("nc"), "search_refresh", [], ["nc"]),
        (lambda c: c["openstates_scrape"]["cloud_path"]["jurisdictions"].remove("nc"), "cloud_path", [], ["nc"]),
        (lambda c: c["openstates_scrape"]["primary"].pop("usa"), "scrape_primary", [], ["usa"]),
        (lambda c: c["openstates_scrape"]["secondary"]["jurisdictions"].append("al"), "scrape_secondary", ["al"], []),
    ],
)
def test_each_list_that_disagrees_is_named(root, mutate, feature, only_yaml, only_manifest):
    import copy

    config = copy.deepcopy(AGREEING_CONFIG)
    mutate(config)
    assert je.config_disagreements(config, root) == [
        {"feature": feature, "only_in_yaml": only_yaml, "only_in_manifest": only_manifest}
    ]
    with capture_logs() as logs:
        assert je.log_config_disagreements(config, root) == 1
    assert [e["event"] for e in logs] == ["jurisdiction_manifest_disagrees"]


def test_log_config_disagreements_logs_every_call_unlike_enrolled(root):
    config = {"openstates_archive": {"jurisdictions": ["fl"]}}
    with capture_logs() as logs:
        je.log_config_disagreements(config, root)
        je.log_config_disagreements(config, root)
    assert [e["event"] for e in logs].count("jurisdiction_manifest_disagrees") == 2


def test_a_missing_manifest_is_not_counted_as_drift(tmp_path):
    assert je.log_config_disagreements(AGREEING_CONFIG, str(tmp_path)) == 0


def test_log_config_disagreements_never_raises(root, monkeypatch):
    monkeypatch.setattr(je, "config_disagreements", lambda *a, **k: 1 / 0)
    assert je.log_config_disagreements(AGREEING_CONFIG, root) == 0


# --- the wired consumers: shadow keeps their answer, authoritative changes it ------------------------------

def _archive_cfg(root, embedding):
    return {"openstates_root": root, "knowledge_base_embedding": {"enabled": True, "jurisdictions": embedding}}


def test_the_embedding_hook_gate_is_unchanged_in_shadow_mode(root):
    assert openstates_archive._knowledge_base_embedding_eligible("nc", _archive_cfg(root, ["fl", "us", "nc"])) is True
    assert openstates_archive._knowledge_base_embedding_eligible("nc", _archive_cfg(root, ["fl", "us"])) is False


def test_the_embedding_hook_gate_follows_the_manifest_in_authoritative_mode(root, monkeypatch):
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    assert openstates_archive._knowledge_base_embedding_eligible("nc", _archive_cfg(root, ["fl", "us", "nc"])) is False
    assert openstates_archive._knowledge_base_embedding_eligible("us", _archive_cfg(root, ["fl"])) is True


def test_a_disabled_embedding_block_stays_off_whatever_the_manifest_says(root, monkeypatch):
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    cfg = _archive_cfg(root, ["fl", "us"])
    cfg["knowledge_base_embedding"]["enabled"] = False
    assert openstates_archive._knowledge_base_embedding_eligible("fl", cfg) is False


def test_the_search_refresh_hook_gate(root, monkeypatch):
    cfg = {"openstates_root": root, "bill_search_refresh": {"enabled": True, "jurisdictions": ["fl"]}}
    assert openstates_archive._bill_search_refresh_eligible("nc", cfg) is False  # shadow: the yaml's answer
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    assert openstates_archive._bill_search_refresh_eligible("nc", cfg) is True  # the manifest says nc is on it


def test_the_env_root_override_is_honoured_for_the_manifest_location(root, monkeypatch):
    monkeypatch.setenv("OPENSTATES_ROOT", root)
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    cfg = {"openstates_root": "/nonexistent/other/host/path", "knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"]}}
    assert openstates_archive._knowledge_base_embedding_eligible("us", cfg) is True


def test_run_secondary_scrapes_fallback_uses_the_manifest_not_a_stale_constant(root):
    """The in-code fallback used to be va mi ma ut az (no nc). With no list in the yaml the manifest answers."""
    assert je.enrolled("scrape_secondary", None, root) == ["nc"]


def test_the_last_resort_archive_default_now_includes_nc():
    assert "nc" in openstates_archive.DEFAULT_ARCHIVE_JURISDICTIONS


def test_the_manual_single_state_targets_follow_the_schedule(root):
    from ddp_sync.api.routes.triggers import _single_jurisdiction_targets

    assert _single_jurisdiction_targets({"openstates_root": root, "secondary": {"jurisdictions": ["nc", "mi", "usa"]}}) == {"nc", "mi"}


def test_the_shipped_sync_schedule_agrees_with_the_real_manifest_when_a_checkout_is_present():
    root = _REAL_CHECKOUT
    if not root or not os.path.exists(os.path.join(root, "jurisdictions.yaml")):
        pytest.skip("set OPENSTATES_ROOT to a ddp-open-states checkout to run this")
    config = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "..", "config", "sync_schedule.yaml")))
    assert je.config_disagreements(config, root) == []


# --- an explicitly empty list is an answer, not "absent" (pm-review round 1) -------------------------------

def test_an_empty_yaml_list_stays_empty_in_shadow_mode(root):
    assert je.enrolled("archive", [], root) == []


def test_an_empty_manifest_result_stays_empty_in_authoritative_mode(tmp_path, monkeypatch):
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({"fl": _entry(archive=False)}))
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    assert je.enrolled("archive", ["fl"], str(tmp_path)) == []


def test_the_manual_archive_trigger_does_not_fall_back_to_the_default_when_the_list_is_empty(root):
    from unittest.mock import AsyncMock, MagicMock, patch

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "t"
    scheduler = MagicMock()
    scheduler._sync_config = {"openstates_archive": {"openstates_root": root, "jurisdictions": []}}
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock()
    ) as run:
        resp = TestClient(app).post("/trigger/openstates-archive/fl")
    assert resp.status_code == 404  # nothing enrolled; the default list (which contains fl) must not be used
    run.assert_not_awaited()


def test_run_archive_jobs_with_an_empty_list_archives_nothing_rather_than_the_default(root):
    import asyncio
    from unittest.mock import AsyncMock, patch

    with patch.object(openstates_archive, "_write_flow_status", new=AsyncMock()), patch.object(
        openstates_archive, "_run_archive_with_hook", new=AsyncMock(return_value={"success": True})
    ) as run:
        asyncio.run(openstates_archive.run_archive_jobs({"openstates_root": root, "jurisdictions": []}))
    run.assert_not_awaited()


def test_run_secondary_scrapes_with_an_empty_list_scrapes_nothing_rather_than_the_default(root):
    import asyncio
    from unittest.mock import AsyncMock, patch

    with patch.object(openstates_scrape, "_write_flow_status", new=AsyncMock()), patch.object(
        openstates_scrape, "_run_scrape", new=AsyncMock()
    ) as run:
        asyncio.run(openstates_scrape.run_secondary_scrapes_job({"openstates_root": root, "secondary": {"jurisdictions": []}}))
    run.assert_not_awaited()
