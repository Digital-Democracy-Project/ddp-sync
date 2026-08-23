"""Tests for OPEN-86's per-jurisdiction sweep-import gate.

`run-scrape.sh` has read `SWEEP_IMPORT_ENABLED` since 2026-07-30 and nothing anywhere set it,
so import-as-you-go has been merged but dark that whole time. These tests pin down the gate that
turns it on, and — more importantly — that it stays OFF for every jurisdiction not explicitly
listed. That second half is the risk: this flag changes when data lands in Postgres for a live
jurisdiction, so an accidentally-broad gate would alter FL/WA/USA behaviour silently.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import _run_scrape, _sweep_import_eligible

CANARY_CFG = {"sweep_import": {"enabled": True, "jurisdictions": ["va", "ut"]}}


# --- the gate itself -------------------------------------------------------------------

@pytest.mark.parametrize("jurisdiction", ["va", "ut"])
def test_listed_jurisdictions_are_eligible(jurisdiction):
    assert _sweep_import_eligible(jurisdiction, CANARY_CFG) is True


@pytest.mark.parametrize("jurisdiction", ["fl", "wa", "usa", "mi", "ma", "az", "al"])
def test_unlisted_jurisdictions_are_not_eligible(jurisdiction):
    """The canary must not leak to anything else — including the three primaries."""
    assert _sweep_import_eligible(jurisdiction, CANARY_CFG) is False


def test_disabled_flag_overrides_the_list():
    """The documented rollback: flip enabled: false and the list stops mattering."""
    cfg = {"sweep_import": {"enabled": False, "jurisdictions": ["va", "ut"]}}
    assert _sweep_import_eligible("va", cfg) is False


@pytest.mark.parametrize(
    "cfg",
    [
        None,
        {},
        {"sweep_import": {}},
        {"sweep_import": {"enabled": True}},  # enabled but no list
    ],
)
def test_absent_or_incomplete_config_is_off(cfg):
    """Absent config must be off, not on. This is the pre-change behaviour for every caller
    that passes no config at all."""
    assert _sweep_import_eligible("va", cfg) is False


# --- does it actually reach run-scrape.sh's environment? ------------------------------

async def _captured_env_for(jurisdiction, config):
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(0, b"", b"", False),
        ) as helper,
        patch(
            "ddp_sync.pipelines.openstates_scrape._maybe_preseed_scrapebot_cookies",
            new=AsyncMock(),
        ),
    ):
        await _run_scrape(jurisdiction, None, "/fake/root", timeout_s=10, config=config)
    # _run_with_group_kill(cmd, env, timeout, ...) — env is positional arg 1 regardless of
    # whether OPEN-127's optional cwd param has landed yet.
    return helper.call_args.args[1]


@pytest.mark.asyncio
async def test_env_var_is_set_for_a_canary_jurisdiction():
    env = await _captured_env_for("va", CANARY_CFG)
    assert env["SWEEP_IMPORT_ENABLED"] == "1"
    assert env["SKIP_PATCHES"] == "1"  # unchanged


@pytest.mark.asyncio
async def test_env_var_is_absent_entirely_for_a_non_canary_jurisdiction():
    """Absent, not "0" — run-scrape.sh defaults it to 0, so not setting it is the honest
    representation of "this jurisdiction was never opted in"."""
    env = await _captured_env_for("fl", CANARY_CFG)
    assert "SWEEP_IMPORT_ENABLED" not in env
    assert env["SKIP_PATCHES"] == "1"


@pytest.mark.asyncio
async def test_env_var_is_absent_when_no_config_is_passed():
    env = await _captured_env_for("va", None)
    assert "SWEEP_IMPORT_ENABLED" not in env


# --- the shipped config ---------------------------------------------------------------

def test_shipped_yaml_enables_exactly_the_two_canary_jurisdictions():
    """Guards the actual rollout state, so widening it is a deliberate, reviewed edit."""
    from pathlib import Path

    import yaml

    cfg_path = Path(__file__).resolve().parents[1] / "config" / "sync_schedule.yaml"
    cfg = yaml.safe_load(cfg_path.read_text())
    sweep = cfg["openstates_scrape"]["sweep_import"]

    assert sweep["enabled"] is True
    assert sweep["jurisdictions"] == ["va", "ut"]
