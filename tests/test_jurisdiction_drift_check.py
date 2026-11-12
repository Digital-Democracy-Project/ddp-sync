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
        "tier": tier, "status": status, "archive": {"enabled": archive, "scheduled": archive},
        "enrollment": {"embedding": embedding, "search_refresh": search_refresh, "cloud_path": cloud_path,
                       "replica_routing": replica_routing, "legbot_replica": legbot_replica,
                       "broker_bill_event_signal": False},
        # SYNC-157: the per-state behaviour ddp-sync reads from here
        "scrape": {"sweep_import": False, "full_walk": False, "retry": False, "cadence_escalation": True,
                   "fargate_max_wait_s": None},
        "waf": {"profile": "none", "scrapebot_fallback": False},
    }


MANIFEST = {
    "fl": _entry("primary", embedding=True, search_refresh=True, cloud_path=True),
    "us": _entry("primary", embedding=True, search_refresh=True, cloud_path=True),
    "nc": _entry("secondary", search_refresh=True, cloud_path=True),
    "al": _entry("secondary", status="paused", replica_routing=False, legbot_replica=False),
}

# SYNC-157: the config holds no per-state list; only `openstates_scrape.timing` can disagree with the manifest.
AGREEING_CONFIG = {
    "openstates_archive": {"knowledge_base_embedding": {"enabled": True}, "bill_search_refresh": {"enabled": True}},
    "openstates_scrape": {"timing": {"fl": {"floor": "weekly"}, "usa": {"floor": "nightly"}}, "cloud_path": {"enabled": True}},
}
# timing for a paused state (al) is drift; a second extra name (zz, unknown) is a different finding
DRIFT_CONFIG = {**AGREEING_CONFIG, "openstates_scrape": {**AGREEING_CONFIG["openstates_scrape"],
                "timing": {**AGREEING_CONFIG["openstates_scrape"]["timing"], "al": {"floor": "weekly"}}}}
DRIFT_CONFIG_2 = {**AGREEING_CONFIG, "openstates_scrape": {**AGREEING_CONFIG["openstates_scrape"],
                  "timing": {**AGREEING_CONFIG["openstates_scrape"]["timing"], "al": {}, "zz": {}}}}


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


def _settings(*_ignored):
    """SYNC-157: no env list is compared any more; a host's settings are not read by the check."""
    return SimpleNamespace()


# --- collect_findings -----------------------------------------------------------------------------------

def test_everything_agreeing_has_no_findings(root):
    f = jdc.collect_findings(AGREEING_CONFIG, _settings(), root)
    assert f == {"drift": [], "unverifiable": [], "skipped": []}


def test_no_env_list_is_compared_any_more(root, monkeypatch):
    """SYNC-157: DDP_OPENSTATES_JURISDICTIONS and the LegBot env list are deleted; a stale one on a host is not read."""
    monkeypatch.setenv("DDP_OPENSTATES_JURISDICTIONS", "US")
    f = jdc.collect_findings(AGREEING_CONFIG, SimpleNamespace(ddp_openstates_jurisdictions=["US"]), root)
    assert f == {"drift": [], "unverifiable": [], "skipped": []}


def test_timing_for_a_state_the_manifest_does_not_scrape_is_drift(root):
    """SYNC-152: the yaml has no list of scraped states, only timing; timing for a paused state (al) is drift."""
    import copy

    config = copy.deepcopy(AGREEING_CONFIG)
    config["openstates_scrape"]["timing"]["al"] = {"floor": "weekly"}
    f = jdc.collect_findings(config, _settings(), root)
    assert f["drift"] == [{"feature": "scrape_timing", "only_in_yaml": ["al"], "only_in_manifest": [],
                           "source": "sync_schedule.yaml"}]


def test_a_scraped_state_without_timing_is_not_drift(root):
    import copy

    config = copy.deepcopy(AGREEING_CONFIG)
    config["openstates_scrape"]["timing"] = {}
    assert jdc.collect_findings(config, _settings(), root)["drift"] == []


def test_a_missing_manifest_is_unverifiable_not_silent(tmp_path):
    f = jdc.collect_findings(AGREEING_CONFIG, _settings(), str(tmp_path))
    assert f["drift"] == [] and "archive" in f["unverifiable"] and "sweep_import" in f["unverifiable"]
    assert "scrape" in f["unverifiable"]  # SYNC-152: a host that cannot read who is scraped must alert


def test_a_manifest_older_than_sync_157_is_unverifiable_for_its_fields_only(tmp_path):
    """A host whose ddp-open-states checkout predates SYNC-157 must alert: every reader would treat its states as not
    enrolled (MI without its ScrapeBot fallback, MA without its retry)."""
    old = {}
    for c, e in MANIFEST.items():
        old[c] = {k: v for k, v in e.items() if k not in ("scrape", "waf")}
        old[c]["enrollment"] = {k: v for k, v in e["enrollment"].items() if k != "broker_bill_event_signal"}
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(old))
    f = jdc.collect_findings(AGREEING_CONFIG, _settings(), str(tmp_path))
    assert f["unverifiable"] == sorted(["broker_bill_event_signal", "cadence_escalation", "full_walk", "scrape_retry",
                                        "scrapebot_fallback", "sweep_import"])
    assert f["drift"] == []


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
    result, post, written = _run(root, DRIFT_CONFIG, _settings())
    assert result["status"] == "drift"
    post.assert_called_once()
    text = post.call_args.args[0]
    assert "scrape_timing" in text and "al" in text
    assert post.call_args.kwargs["source"] == "jurisdiction_drift_check"
    assert written["jurisdiction_drift_check"]["last_alert_at"]


def test_the_same_finding_does_not_alert_again_before_repeat_days(root):
    first, _, _ = _run(root, DRIFT_CONFIG, _settings())
    result, post, _ = _run(root, DRIFT_CONFIG, _settings(), previous=first)
    assert not post.called and result["status"] == "drift"


def test_a_changed_finding_alerts_again_immediately(root):
    first, _, _ = _run(root, DRIFT_CONFIG, _settings())
    _, post, _ = _run(root, DRIFT_CONFIG_2, _settings(), previous=first)
    assert post.called


def test_the_same_finding_alerts_again_after_repeat_days(root):
    first, _, _ = _run(root, DRIFT_CONFIG, _settings())
    first["last_alert_at"] = "2020-01-01T00:00:00+00:00"
    _, post, _ = _run(root, DRIFT_CONFIG, _settings(), previous=first)
    assert post.called


def test_a_failed_alert_post_is_retried_next_run_not_recorded_as_sent(root):
    result, post, written = _run(root, DRIFT_CONFIG, _settings(), alert=False)
    assert post.called and written["jurisdiction_drift_check"]["last_alert_at"] is None
    _, post2, _ = _run(root, DRIFT_CONFIG, _settings(), previous=written["jurisdiction_drift_check"])
    assert post2.called


def test_an_unreadable_manifest_alerts_as_unverifiable(tmp_path):
    result, post, _ = _run(str(tmp_path), AGREEING_CONFIG, _settings())
    assert result["status"] == "drift" and post.called and "cannot verify" in post.call_args.args[0]


def test_recovery_clears_the_signature(root):
    first, _, _ = _run(root, DRIFT_CONFIG, _settings())
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
