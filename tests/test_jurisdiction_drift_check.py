"""OPEN-330: the daily host check that jurisdiction lists agree with the manifest.

A tiny manifest in a temp dir stands in for the ddp-open-states checkout. Alerts go through `post_alert` (patched);
Redis flow status is faked. The contract: drift and "cannot verify" are both alerts, an unset env list is not drift,
a repeated identical finding does not re-alert until repeat_days, and the job never raises.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from ddp_sync.pipelines import jurisdiction_drift_check as jdc
from ddp_sync.services import jurisdiction_enrollment as je


def _entry(tier="secondary", status="live", archive=True, embedding=False, search_refresh=False, cloud_path=False,
           replica_routing=True, legbot_replica=True):
    return {
        "tier": tier, "status": status, "archive": {"enabled": archive},
        "enrollment": {"embedding": embedding, "search_refresh": search_refresh, "cloud_path": cloud_path,
                       "replica_routing": replica_routing, "legbot_replica": legbot_replica},
    }


MANIFEST = {
    "fl": _entry("primary", embedding=True, search_refresh=True, cloud_path=True),
    "us": _entry("primary", embedding=True, search_refresh=True, cloud_path=True),
    "nc": _entry("secondary", search_refresh=True, cloud_path=True),
    "al": _entry("secondary", status="paused", replica_routing=False, legbot_replica=False),
}

AGREEING_CONFIG = {
    "openstates_archive": {
        "jurisdictions": ["fl", "us", "nc", "al"],
        "knowledge_base_embedding": {"jurisdictions": ["fl", "us"]},
        "bill_search_refresh": {"jurisdictions": ["us", "fl", "nc"]},
    },
    "openstates_scrape": {
        "primary": {"fl": {"enabled": True}, "usa": {"enabled": True}},
        "secondary": {"jurisdictions": ["nc"]},
        "cloud_path": {"jurisdictions": ["fl", "usa", "nc"]},
    },
}


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    je._cache.clear()
    je._logged.clear()
    monkeypatch.delenv("OPENSTATES_ROOT", raising=False)
    monkeypatch.delenv(je.MODE_ENV, raising=False)


@pytest.fixture
def root(tmp_path):
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(MANIFEST))
    return str(tmp_path)


def _settings(routing=(), legbot=()):
    return SimpleNamespace(ddp_openstates_jurisdictions=list(routing), legbot_rds_replica_jurisdiction_allowlist=frozenset(legbot))


# --- collect_findings -----------------------------------------------------------------------------------

def test_everything_agreeing_has_no_findings(root):
    f = jdc.collect_findings(AGREEING_CONFIG, _settings(["US", "FL", "NC"], ["fl", "us", "nc"]), root)
    assert f == {"drift": [], "unverifiable": [], "skipped": []}


def test_an_env_list_the_host_does_not_set_is_not_drift_but_is_named_as_skipped(root):
    f = jdc.collect_findings(AGREEING_CONFIG, _settings(), root)
    assert f["drift"] == [] and f["unverifiable"] == []
    assert f["skipped"] == ["DDP_OPENSTATES_JURISDICTIONS", "LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST"]


def test_skipped_env_lists_do_not_change_the_alert_signature_or_cause_an_alert(root):
    result, post, _ = _run(root, AGREEING_CONFIG, _settings())
    assert not post.called and result["findings"]["skipped"]
    a = jdc._signature({"drift": [], "unverifiable": [], "skipped": []})
    b = jdc._signature({"drift": [], "unverifiable": [], "skipped": ["X"]})
    assert a == b


def test_an_env_list_that_differs_is_drift_with_both_sides(root):
    f = jdc.collect_findings(AGREEING_CONFIG, _settings(["US", "FL"], ["FL", "US", "NC", "AL"]), root)
    by = {d["source"]: d for d in f["drift"]}
    assert by["env DDP_OPENSTATES_JURISDICTIONS"]["only_in_manifest"] == ["nc"]
    assert by["env LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST"]["only_in_yaml"] == ["al"]


def test_a_live_yaml_list_that_differs_is_drift(root):
    import copy

    config = copy.deepcopy(AGREEING_CONFIG)
    config["openstates_archive"]["jurisdictions"].remove("nc")
    f = jdc.collect_findings(config, _settings(), root)
    assert [d["feature"] for d in f["drift"]] == ["archive"] and f["drift"][0]["source"] == "sync_schedule.yaml"


def test_a_missing_manifest_is_unverifiable_not_silent(tmp_path):
    f = jdc.collect_findings(AGREEING_CONFIG, _settings(["US"], ["us"]), str(tmp_path))
    assert f["drift"] == [] and "archive" in f["unverifiable"] and "replica_routing" in f["unverifiable"]


def test_a_manifest_older_than_the_new_fields_is_unverifiable_for_those_only(tmp_path):
    old = {c: {k: v for k, v in e.items() if k != "enrollment"} | {"enrollment": {
        k: v for k, v in e["enrollment"].items() if k not in ("replica_routing", "legbot_replica")}} for c, e in MANIFEST.items()}
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(old))
    f = jdc.collect_findings(AGREEING_CONFIG, _settings(["US", "FL", "NC"], ["fl"]), str(tmp_path))
    assert f["unverifiable"] == ["legbot_replica", "replica_routing"] and f["drift"] == []


# --- the job: alerting, de-duplication, never raises ----------------------------------------------------

def _run(root, config, settings, previous=None, alert=True):
    scheduler = MagicMock()
    scheduler._sync_config = dict(config)
    scheduler._sync_config["openstates_scrape"] = dict(config["openstates_scrape"], openstates_root=root)
    store = MagicMock()
    store.get_flow_status = AsyncMock(return_value=previous)
    written = {}

    async def _write(key, status):
        written[key] = status

    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), patch(
        "ddp_sync.config.get_settings", return_value=settings
    ), patch("ddp_sync.services.redis_store.get_redis_store", return_value=store), patch.object(
        jdc, "_write_flow_status", new=_write
    ), patch.object(jdc, "post_alert", return_value=alert) as post:
        result = asyncio.run(jdc.run_jurisdiction_drift_check({"jurisdiction_drift_check": {"repeat_days": 7}}))
    return result, post, written


def test_no_findings_means_no_alert_and_an_ok_status(root):
    result, post, written = _run(root, AGREEING_CONFIG, _settings())
    assert result["status"] == "ok" and not post.called
    assert written["jurisdiction_drift_check"]["status"] == "ok"


def test_drift_alerts_once_with_the_lists_named(root):
    result, post, written = _run(root, AGREEING_CONFIG, _settings(["US"], []))
    assert result["status"] == "drift"
    post.assert_called_once()
    text = post.call_args.args[0]
    assert "DDP_OPENSTATES_JURISDICTIONS" in text and "fl" in text and "nc" in text
    assert post.call_args.kwargs["source"] == "jurisdiction_drift_check"
    assert written["jurisdiction_drift_check"]["last_alert_at"]


def test_the_same_finding_does_not_alert_again_before_repeat_days(root):
    first, _, _ = _run(root, AGREEING_CONFIG, _settings(["US"]))
    result, post, _ = _run(root, AGREEING_CONFIG, _settings(["US"]), previous=first)
    assert not post.called and result["status"] == "drift"


def test_a_changed_finding_alerts_again_immediately(root):
    first, _, _ = _run(root, AGREEING_CONFIG, _settings(["US"]))
    _, post, _ = _run(root, AGREEING_CONFIG, _settings(["US", "FL"]), previous=first)
    assert post.called


def test_the_same_finding_alerts_again_after_repeat_days(root):
    first, _, _ = _run(root, AGREEING_CONFIG, _settings(["US"]))
    first["last_alert_at"] = "2020-01-01T00:00:00+00:00"
    _, post, _ = _run(root, AGREEING_CONFIG, _settings(["US"]), previous=first)
    assert post.called


def test_a_failed_alert_post_is_retried_next_run_not_recorded_as_sent(root):
    result, post, written = _run(root, AGREEING_CONFIG, _settings(["US"]), alert=False)
    assert post.called and written["jurisdiction_drift_check"]["last_alert_at"] is None
    _, post2, _ = _run(root, AGREEING_CONFIG, _settings(["US"]), previous=written["jurisdiction_drift_check"])
    assert post2.called


def test_an_unreadable_manifest_alerts_as_unverifiable(tmp_path):
    result, post, _ = _run(str(tmp_path), AGREEING_CONFIG, _settings())
    assert result["status"] == "drift" and post.called and "cannot verify" in post.call_args.args[0]


def test_recovery_clears_the_signature(root):
    first, _, _ = _run(root, AGREEING_CONFIG, _settings(["US"]))
    result, post, _ = _run(root, AGREEING_CONFIG, _settings(), previous=first)
    assert result["status"] == "ok" and result["signature"] == "" and not post.called


def test_the_job_never_raises_and_a_broken_check_alerts(root):
    with patch("ddp_sync.scheduler.get_scheduler", side_effect=RuntimeError("boom")), patch.object(jdc, "post_alert") as post:
        result = asyncio.run(jdc.run_jurisdiction_drift_check({}))
    assert result["status"] == "error"
    post.assert_called_once()
    assert "could not run" in post.call_args.args[0] and "boom" in post.call_args.args[0]


def test_a_failing_failure_alert_still_does_not_escape(root):
    with patch("ddp_sync.scheduler.get_scheduler", side_effect=RuntimeError("boom")), patch.object(
        jdc, "post_alert", side_effect=OSError("slack down")
    ):
        assert asyncio.run(jdc.run_jurisdiction_drift_check({}))["status"] == "error"


# --- wiring ---------------------------------------------------------------------------------------------

YAML = """
    bill_sync:
      sync_time_utc: "04:00"
    openstates_scrape:
      enabled: true
      jurisdiction_drift_check:
        enabled: true
        sync_time_utc: "06:20"
"""


def _jobs(flag: bool, yaml_text: str = YAML, **extra):
    import tempfile
    import textwrap
    from pathlib import Path

    from ddp_sync.scheduler import UpdateScheduler
    from tests.test_scheduler_per_task_flags import _settings

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(yaml_text))
    sched = UpdateScheduler(settings=_settings(jurisdiction_drift_check_enabled=flag, **extra), config_path=Path(f.name))
    sched.start()
    try:
        return {j.id: j for j in sched.scheduler.get_jobs()}
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_registered_daily_at_0620_utc_when_flag_on():
    job = _jobs(True)["jurisdiction_drift_check"]
    fields = {f.name: str(f) for f in job.trigger.fields}
    assert fields["hour"] == "6" and fields["minute"] == "20" and fields["day_of_week"] == "*"
    assert job.max_instances == 1 and str(job.trigger.timezone) == "UTC"


@pytest.mark.asyncio
async def test_registered_even_when_this_host_runs_no_scrapes():
    jobs = _jobs(True, openstates_scrape_enabled=False)
    assert "jurisdiction_drift_check" in jobs and "openstates_fl_scrape" not in jobs


@pytest.mark.asyncio
async def test_not_registered_when_flag_off_or_yaml_disabled():
    assert "jurisdiction_drift_check" not in _jobs(False)
    off = YAML.replace("jurisdiction_drift_check:\n        enabled: true", "jurisdiction_drift_check:\n        enabled: false")
    assert "jurisdiction_drift_check" not in _jobs(True, off)


@pytest.mark.asyncio
async def test_registered_job_actually_runs_with_the_scrape_config(monkeypatch):
    # The wrapper once referenced a variable that only existed in the registration method (fork check, 2026-10-13):
    # it registered fine and died at its first cron. Call the registered wrapper for real.
    seen = {}

    async def fake_run(config=None):
        seen["config"] = config
        return {"status": "ok"}

    monkeypatch.setattr(jdc, "run_jurisdiction_drift_check", fake_run)
    job = _jobs(True)["jurisdiction_drift_check"]
    assert await job.func() == {"status": "ok"}
    assert seen["config"]["jurisdiction_drift_check"]["sync_time_utc"] == "06:20"


@pytest.mark.parametrize("value,expected", [(None, False), ("", False), ("false", False), ("true", True)])
def test_flag_defaults_off_and_only_the_word_true_enables_it(monkeypatch, value, expected):
    from ddp_sync import config

    if value is None:
        monkeypatch.delenv("JURISDICTION_DRIFT_CHECK_ENABLED", raising=False)
    else:
        monkeypatch.setenv("JURISDICTION_DRIFT_CHECK_ENABLED", value)
    assert config._load_from_env()["jurisdiction_drift_check_enabled"] is expected


def test_the_shipped_yaml_block_exists():
    import os

    config = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "..", "config", "sync_schedule.yaml")))
    block = config["openstates_scrape"]["jurisdiction_drift_check"]
    assert block["enabled"] is True and block["sync_time_utc"] == "06:20"


def test_the_manual_trigger_knows_drift_check():
    from ddp_sync.api.routes.triggers import _OPENSTATES_JOB_TARGETS

    assert "drift-check" in _OPENSTATES_JOB_TARGETS
