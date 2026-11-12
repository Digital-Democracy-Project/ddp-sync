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
        "archive": {"enabled": archive, "scheduled": archive},
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
    assert je.manifest_codes("scrape", root) == ["fl", "nc", "usa"]
    assert "us" in je.manifest_codes("archive", root)


def test_a_paused_jurisdiction_is_on_no_scrape_list(root):
    assert "al" not in je.manifest_codes("scrape", root)


@pytest.mark.parametrize("tier", ["primary", "secondary"])
def test_a_probing_jurisdiction_is_on_no_scrape_list(tmp_path, tier):
    """OPEN-478/479/480: a state added at Stage 0 is on no schedule until Stage 2, whatever its tier says."""
    manifest = {**MANIFEST, "pa": _entry(tier, status="probing", archive=False)}
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(manifest))
    assert "pa" not in je.manifest_codes("scrape", str(tmp_path))


@pytest.mark.parametrize("tier", ["primary", "secondary"])
@pytest.mark.parametrize("status", ["scraping", "validating", "archived", "live"])
def test_a_jurisdiction_past_probing_is_scraped_whatever_its_tier(tmp_path, tier, status):
    """SYNC-152: one rule, status only. The tier is not read (OPEN-521 will remove it from the manifest)."""
    manifest = {**MANIFEST, "pa": _entry(tier, status=status, archive=False)}
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(manifest))
    assert "pa" in je.manifest_codes("scrape", str(tmp_path))


def test_the_scrape_rule_works_without_a_tier_field(tmp_path):
    """SYNC-152 must work with today's manifest (tier present) and after OPEN-521 (tier gone)."""
    manifest = {c: {k: v for k, v in e.items() if k != "tier"} for c, e in MANIFEST.items()}
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(manifest))
    assert je.manifest_codes("scrape", str(tmp_path)) == ["fl", "nc", "usa"]


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

# SYNC-157: the config holds no per-state list any more; only `openstates_scrape.timing` can disagree.
AGREEING_CONFIG = {
    "openstates_archive": {"knowledge_base_embedding": {"enabled": True}, "bill_search_refresh": {"enabled": True}},
    "openstates_scrape": {"timing": {"fl": {"floor": "weekly"}, "usa": {"floor": "nightly"}}, "cloud_path": {"enabled": True}},
}


def test_a_config_that_agrees_reports_nothing(root):
    assert je.config_disagreements(AGREEING_CONFIG, root) == []
    with capture_logs() as logs:
        assert je.log_config_disagreements(AGREEING_CONFIG, root) == 0
    assert [e["event"] for e in logs] == ["jurisdiction_manifest_agrees"]


@pytest.mark.parametrize(
    "mutate,feature,only_yaml,only_manifest",
    [
        # SYNC-152: timing for a state the manifest does not scrape (al is paused) is drift; a scraped state with no
        # timing entry (nc) is not.
        (lambda c: c["openstates_scrape"]["timing"].update(al={"floor": "weekly"}), "scrape_timing", ["al"], []),
        (lambda c: c["openstates_scrape"]["timing"].pop("usa"), None, None, None),
    ],
)
def test_each_list_that_disagrees_is_named(root, mutate, feature, only_yaml, only_manifest):
    import copy

    config = copy.deepcopy(AGREEING_CONFIG)
    mutate(config)
    if feature is None:
        assert je.config_disagreements(config, root) == []
        return
    assert je.config_disagreements(config, root) == [
        {"feature": feature, "only_in_yaml": only_yaml, "only_in_manifest": only_manifest}
    ]
    with capture_logs() as logs:
        assert je.log_config_disagreements(config, root) == 1
    assert [e["event"] for e in logs] == ["jurisdiction_manifest_disagrees"]


def test_log_config_disagreements_logs_every_call_unlike_enrolled(root):
    config = {"openstates_scrape": {"timing": {"al": {"floor": "weekly"}}}}
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


def test_the_embedding_hook_gate_is_the_manifest_in_shadow_mode_too(root):
    """SYNC-157: no yaml list to be shadowed; a leftover list is ignored in either mode."""
    assert openstates_archive._knowledge_base_embedding_eligible("nc", _archive_cfg(root, ["fl", "us", "nc"])) is False
    assert openstates_archive._knowledge_base_embedding_eligible("us", _archive_cfg(root, ["fl"])) is True


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
    assert openstates_archive._bill_search_refresh_eligible("nc", cfg) is True  # SYNC-157: the manifest in shadow mode too
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    assert openstates_archive._bill_search_refresh_eligible("nc", cfg) is True  # the manifest says nc is on it


def test_the_env_root_override_is_honoured_for_the_manifest_location(root, monkeypatch):
    monkeypatch.setenv("OPENSTATES_ROOT", root)
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    cfg = {"openstates_root": "/nonexistent/other/host/path", "knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"]}}
    assert openstates_archive._knowledge_base_embedding_eligible("us", cfg) is True


def test_the_scraped_states_come_from_the_manifest_alone(root, monkeypatch):
    """SYNC-152: there is no yaml list and no in-code fallback; shadow and authoritative give the same answer."""
    cfg = {"openstates_root": root}
    assert openstates_scrape.enrolled_scrape_states(cfg) == ["fl", "nc", "usa"]
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    assert openstates_scrape.enrolled_scrape_states(cfg) == ["fl", "nc", "usa"]


def test_an_unreadable_manifest_gives_no_scraped_states_rather_than_a_guess(tmp_path):
    assert openstates_scrape.enrolled_scrape_states({"openstates_root": str(tmp_path)}) is None


def test_the_manual_single_state_targets_are_every_scraped_state(root):
    from ddp_sync.api.routes.triggers import _single_jurisdiction_targets

    # SYNC-152: a leftover yaml list is ignored; the manifest's scraped states, usa included, are the targets
    assert _single_jurisdiction_targets({"openstates_root": root, "secondary": {"jurisdictions": ["mi"]}}) == {"fl", "nc", "usa"}


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


# --- SYNC-153: who is archived is the manifest's archive.enabled, one list -------------------------------------

def test_the_archive_feature_reads_enabled_not_scheduled(tmp_path):
    """A state mid-pilot (enabled, not scheduled) is archived after its scrape; `scheduled` is not read at all."""
    manifest = {
        "fl": _entry("primary"),
        "nc": dict(_entry("secondary", status="validating"), archive={"enabled": True, "scheduled": False}),
        "al": dict(_entry("secondary", status="paused"), archive={"enabled": False, "scheduled": True}),
    }
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(manifest))
    assert je.manifest_codes("archive", str(tmp_path)) == ["fl", "nc"]


def test_the_archive_feature_works_without_a_scheduled_field(tmp_path):
    """OPEN-521 will drop `archive.scheduled` from the manifest."""
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({"fl": dict(_entry("primary"), archive={"enabled": True})}))
    assert je.manifest_codes("archive", str(tmp_path)) == ["fl"]


def test_a_leftover_yaml_archive_list_is_not_compared_any_more(root):
    assert je.config_disagreements({"openstates_archive": {"jurisdictions": ["zz"]}}, root) == []


# --- SYNC-146 / SYNC-153: the archive that runs right after a scrape follows archive.enabled ---------------

def _new_state_manifest(tmp_path, *, enabled=True):
    """Pennsylvania's real shape at Stage 4: archive enabled, weekly archive not yet scheduled (OPEN-418)."""
    manifest = dict(MANIFEST)
    manifest["pa"] = _entry("primary", status="scraping", cloud_path=True)
    manifest["pa"]["archive"] = {"enabled": enabled, "scheduled": False}
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(manifest))
    return str(tmp_path)


async def _hook(monkeypatch, root, state, yaml_list=("fl", "us")):
    from unittest.mock import AsyncMock, patch

    monkeypatch.setenv("OPENSTATES_ROOT", root)
    cfg = {"enabled": True, "jurisdictions": list(yaml_list)}
    with patch.object(openstates_archive, "_load_archive_config_for_scrape_trigger", return_value=cfg), \
            patch.object(openstates_archive, "run_single_archive_job", new=AsyncMock(return_value={"success": True})) as run:
        await openstates_archive.archive_with_retry(state)
    return run


@pytest.mark.asyncio
async def test_authoritative_new_state_scrape_triggers_its_archive_with_no_yaml_entry(tmp_path, monkeypatch):
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    run = await _hook(monkeypatch, _new_state_manifest(tmp_path), "pa")
    run.assert_awaited_once()
    assert run.await_args.args[0] == "pa"


@pytest.mark.asyncio
async def test_shadow_mode_also_follows_the_shared_list(tmp_path, monkeypatch):
    """SYNC-153: there is no yaml list left to keep, so both modes archive what archive.enabled says."""
    run = await _hook(monkeypatch, _new_state_manifest(tmp_path), "pa")
    run.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_unreadable_list_archives_nothing(tmp_path, monkeypatch):
    """SYNC-153: no yaml fallback; the drift check reports `archive` as unverifiable instead."""
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    empty = str(tmp_path)  # no jurisdictions.yaml here
    assert (await _hook(monkeypatch, empty, "pa")).await_count == 0
    assert (await _hook(monkeypatch, empty, "fl")).await_count == 0


@pytest.mark.asyncio
async def test_authoritative_archive_disabled_in_the_shared_list_is_skipped_even_if_the_yaml_lists_it(tmp_path, monkeypatch):
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    run = await _hook(monkeypatch, _new_state_manifest(tmp_path, enabled=False), "pa", yaml_list=("fl", "pa"))
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_authoritative_live_state_still_triggers_and_usa_maps_to_us(tmp_path, monkeypatch):
    monkeypatch.setenv(je.MODE_ENV, "authoritative")
    root = _new_state_manifest(tmp_path)
    assert (await _hook(monkeypatch, root, "fl")).await_count == 1
    run = await _hook(monkeypatch, root, "usa")
    assert run.await_args.args[0] == "us"



# --- SYNC-157: a config that still carries a per-state list is refused ---------------------------------------------


@pytest.mark.parametrize("path", sorted(je.REMOVED_CONFIG_KEYS))
def test_every_removed_list_is_refused_and_named(path):
    config: dict = {}
    node = config
    for key in path[:-1]:
        node = node.setdefault(key, {})
    node[path[-1]] = ["fl"]
    with pytest.raises(je.RemovedConfigKey, match="SYNC-157") as raised:
        je.check_removed_keys(config)
    assert ".".join(path) in str(raised.value)


def test_the_shipped_schedule_has_none_of_them():
    import pathlib

    import yaml as _yaml

    path = pathlib.Path(__file__).resolve().parent.parent / "config" / "sync_schedule.yaml"
    je.check_removed_keys(_yaml.safe_load(path.read_text()))  # does not raise


def test_the_scheduler_refuses_to_start_on_such_a_config(tmp_path):
    from unittest.mock import MagicMock

    from ddp_sync.scheduler import UpdateScheduler

    path = tmp_path / "sync_schedule.yaml"
    path.write_text("openstates_scrape:\n  sweep_import:\n    enabled: true\n    jurisdictions: [va]\n")
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    with pytest.raises(je.RemovedConfigKey, match="openstates_scrape.sweep_import.jurisdictions"):
        UpdateScheduler(settings=settings, config_path=path)


def test_manifest_value_reads_one_field_and_spells_usa_as_us(root):
    import pathlib

    import yaml as _yaml

    path = pathlib.Path(root) / "jurisdictions.yaml"
    data = _yaml.safe_load(path.read_text())
    data["us"].setdefault("scrape", {})["fargate_max_wait_s"] = 7
    path.write_text(_yaml.safe_dump(data))
    je._cache.clear()
    assert je.manifest_value("usa", ("scrape", "fargate_max_wait_s"), root) == 7
    assert je.manifest_value("fl", ("scrape", "fargate_max_wait_s"), root) is None  # field missing: None, logged
    assert je.manifest_value("zz", ("scrape", "fargate_max_wait_s"), root) is None
