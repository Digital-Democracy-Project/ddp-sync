"""Tests for OPEN-87's per-jurisdiction bounded-retry gate.

These scrapes run weekly, so a run killed by a transient fault waits a week for its next attempt
-- MA lost a run that way to a network timeout on 2026-08-01. The gate here decides which
jurisdictions get run-scrape-retrying.sh instead of run-scrape.sh.

The half that actually carries risk is the OFF half, and it is what most of these tests pin
down. Two things must stay true no matter how the rollout is widened later:

  * MI must never be retried. OPEN-53 established that a blind retry against a WAF worsens a
    block rather than recovering from it, so retrying MI is not a wasted attempt, it is an
    actively harmful one. SYNC-157: WHO is retried is the manifest's `scrape.retry`, and the
    ddp-open-states validator refuses `retry: true` for a state with a WAF profile, so MI cannot
    be enrolled by widening anything (that rule is tested there; here MI's manifest says false).
  * A jurisdiction that has not opted in must be invoked exactly as it is today -- same script,
    same argv, no retry env vars. The opt-out is the absence of a wrapper process, not a
    wrapper told to behave itself.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import _retry_eligible, _run_scrape
from ddp_sync.services import jurisdiction_enrollment as je
from tests import sync157_manifest

# SYNC-157: the yaml keeps the switch and the numbers; WHO is the manifest (ma and, for these tests, us).
ROLLOUT_CFG = {
    "scrape_retry": {
        "enabled": True,
        "max_attempts": 3,
        "backoff_secs": [900, 1800],
    }
}


@pytest.fixture(autouse=True)
def manifest(tmp_path, monkeypatch):
    je._cache.clear()
    je._logged.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", sync157_manifest.write(tmp_path, **{"us.scrape.retry": True}))


# --- the gate itself -------------------------------------------------------------------

@pytest.mark.parametrize("jurisdiction", ["ma", "usa"])
def test_listed_jurisdictions_are_eligible(jurisdiction):
    assert _retry_eligible(jurisdiction, ROLLOUT_CFG) is True


@pytest.mark.parametrize("jurisdiction", ["mi", "va", "ut", "fl", "wa", "az", "al"])
def test_unlisted_jurisdictions_are_not_eligible(jurisdiction):
    """The rollout must not leak to anything else."""
    assert _retry_eligible(jurisdiction, ROLLOUT_CFG) is False


def test_a_yaml_list_is_ignored_the_manifest_decides():
    """SYNC-157: a leftover yaml list does not enroll anyone (the scheduler refuses such a config at load anyway)."""
    cfg = {"scrape_retry": {"enabled": True, "jurisdictions": ["mi", "va"]}}
    assert _retry_eligible("mi", cfg) is False
    assert _retry_eligible("va", cfg) is False
    assert _retry_eligible("ma", cfg) is True


def test_an_unreadable_manifest_retries_nobody(tmp_path, monkeypatch):
    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path / "nowhere"))
    assert _retry_eligible("ma", ROLLOUT_CFG) is False


def test_disabled_flag_overrides_the_list():
    """The documented rollback: flip enabled: false and the list stops mattering."""
    cfg = {"scrape_retry": {"enabled": False, "jurisdictions": ["ma", "usa"]}}
    assert _retry_eligible("ma", cfg) is False


@pytest.mark.parametrize(
    "cfg",
    [
        None,
        {},
        {"scrape_retry": {}},
        {"scrape_retry": {"enabled": False}},
    ],
)
def test_absent_or_incomplete_config_is_off(cfg):
    """Absent config must be off, not on -- this is the pre-change behaviour for every caller
    that passes no config at all."""
    assert _retry_eligible("ma", cfg) is False


# --- does it actually change what gets invoked? --------------------------------------

async def _captured_call_for(jurisdiction, config, session_arg=None, wrapper_present=True):
    """wrapper_present models whether ddp-open-states has deployed run-scrape-retrying.sh yet.
    _run_scrape() falls back to run-scrape.sh when it hasn't, so the default here is True and
    the False case is exercised by its own test below."""
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(0, b"", b"", False, False),
        ) as helper,
        patch(
            "ddp_sync.pipelines.openstates_scrape._maybe_preseed_scrapebot_cookies",
            new=AsyncMock(),
        ),
        patch(
            "ddp_sync.pipelines.openstates_scrape.os.path.exists",
            return_value=wrapper_present,
        ),
    ):
        await _run_scrape(jurisdiction, session_arg, "/fake/root", timeout_s=10, config=config)
    # _run_with_group_kill(cmd, env, timeout, ...) -- cmd and env are positional args 0 and 1
    # regardless of whether OPEN-127's optional cwd param has landed yet.
    return helper.call_args.args[0], helper.call_args.args[1]


@pytest.mark.asyncio
async def test_eligible_jurisdiction_gets_the_wrapper_script():
    cmd, env = await _captured_call_for("ma", ROLLOUT_CFG)
    assert cmd == ["/bin/bash", "/fake/root/run-scrape-retrying.sh", "ma"]
    assert env["SCRAPE_RETRY_MAX_ATTEMPTS"] == "3"
    assert env["SCRAPE_RETRY_BACKOFF_SECS"] == "900,1800"
    assert env["SKIP_PATCHES"] == "1"  # unchanged


@pytest.mark.asyncio
async def test_ineligible_jurisdiction_invocation_is_unchanged():
    """Not "the wrapper with retries turned off" -- the wrapper is not involved at all, so a
    jurisdiction outside the rollout runs exactly the same command it runs today."""
    cmd, env = await _captured_call_for("fl", ROLLOUT_CFG, session_arg="session=2026")
    assert cmd == ["/bin/bash", "/fake/root/run-scrape.sh", "fl", "session=2026"]
    assert "SCRAPE_RETRY_MAX_ATTEMPTS" not in env
    assert "SCRAPE_RETRY_BACKOFF_SECS" not in env
    assert env["SKIP_PATCHES"] == "1"


@pytest.mark.asyncio
async def test_mi_never_gets_the_wrapper():
    """The one that matters most. MI is excluded, so it must reach plain run-scrape.sh."""
    cmd, env = await _captured_call_for("mi", ROLLOUT_CFG)
    assert cmd == ["/bin/bash", "/fake/root/run-scrape.sh", "mi"]
    assert "SCRAPE_RETRY_MAX_ATTEMPTS" not in env


@pytest.mark.asyncio
async def test_no_config_means_no_wrapper():
    cmd, env = await _captured_call_for("ma", None)
    assert cmd == ["/bin/bash", "/fake/root/run-scrape.sh", "ma"]
    assert "SCRAPE_RETRY_MAX_ATTEMPTS" not in env


@pytest.mark.asyncio
async def test_session_arg_is_passed_through_to_the_wrapper():
    """USA scrapes carry a session argument; the wrapper takes the same argv as run-scrape.sh."""
    cmd, _ = await _captured_call_for("usa", ROLLOUT_CFG, session_arg="session=119 chamber=lower")
    assert cmd == [
        "/bin/bash",
        "/fake/root/run-scrape-retrying.sh",
        "usa",
        "session=119 chamber=lower",
    ]


@pytest.mark.asyncio
async def test_excluded_jurisdictions_env_var_is_not_passed():
    """_retry_eligible() is the single source of the opt-out decision. The wrapper also supports
    SCRAPE_RETRY_EXCLUDED_JURISDICTIONS, but that is for a human invoking it by hand -- passing
    it from here too would create a second place the policy lives."""
    _, env = await _captured_call_for("ma", ROLLOUT_CFG)
    assert "SCRAPE_RETRY_EXCLUDED_JURISDICTIONS" not in env


@pytest.mark.asyncio
async def test_backoff_and_attempts_come_from_config_not_hardcoded():
    """The backoff values are an open question on OPEN-87, so they must be changeable by editing
    YAML alone. One mechanism covers both candidate shapes: a repeated value is a fixed backoff,
    ascending values are a growing one."""
    cfg = {
        "scrape_retry": {
            "enabled": True,
            "max_attempts": 5,
            "backoff_secs": [1800, 3600, 7200, 7200],
        }
    }
    _, env = await _captured_call_for("ma", cfg)
    assert env["SCRAPE_RETRY_MAX_ATTEMPTS"] == "5"
    assert env["SCRAPE_RETRY_BACKOFF_SECS"] == "1800,3600,7200,7200"


@pytest.mark.asyncio
async def test_falls_back_to_run_scrape_when_the_wrapper_is_not_deployed_yet():
    """Two repos, two deploys, so this config can go live before the wrapper script does.

    The fallback matters more than it looks. Without it, /bin/bash on a missing script exits
    127 -- a *positive* return code -- and _run_scrape()'s `returncode != 0` branch deliberately
    does not alert on positive codes, because it assumes run-scrape.sh already alerted from
    inside its own process. Here run-scrape.sh never ran, so nobody would alert at all and an
    opted-in jurisdiction would fail silently every cycle until someone noticed missing data.
    """
    cmd, env = await _captured_call_for("ma", ROLLOUT_CFG, wrapper_present=False)
    assert cmd == ["/bin/bash", "/fake/root/run-scrape.sh", "ma"]
    # And it must not claim retry is active by leaving the env vars behind.
    assert "SCRAPE_RETRY_MAX_ATTEMPTS" not in env
    assert "SCRAPE_RETRY_BACKOFF_SECS" not in env
    assert env["SKIP_PATCHES"] == "1"


@pytest.mark.asyncio
async def test_fallback_does_not_disturb_an_ineligible_jurisdiction():
    """The existence check must only ever run for an eligible jurisdiction; an ineligible one
    reaches run-scrape.sh either way and must not depend on the wrapper being present."""
    for present in (True, False):
        cmd, env = await _captured_call_for("fl", ROLLOUT_CFG, wrapper_present=present)
        assert cmd == ["/bin/bash", "/fake/root/run-scrape.sh", "fl"]
        assert "SCRAPE_RETRY_MAX_ATTEMPTS" not in env


# --- the shipped config ---------------------------------------------------------------

def _shipped_retry_cfg():
    from pathlib import Path

    import yaml

    cfg_path = Path(__file__).resolve().parents[1] / "config" / "sync_schedule.yaml"
    cfg = yaml.safe_load(cfg_path.read_text())
    return cfg["openstates_scrape"]["scrape_retry"]


def test_shipped_yaml_enables_the_switch_and_lists_nobody():
    """SYNC-157: the shipped yaml keeps the switch and the numbers; WHO (ma) is the manifest's scrape.retry."""
    retry = _shipped_retry_cfg()
    assert retry["enabled"] is True
    assert "jurisdictions" not in retry and "jurisdictions_excluded" not in retry
    assert retry["max_attempts"] == 3


def test_the_real_manifest_values_retry_only_ma():
    """The values copied into the manifest: MA only (USA held back: its 4h ceiling cannot fit 3 attempts)."""
    assert sorted(c for c, e in sync157_manifest.MANIFEST.items() if e["scrape"]["retry"]) == ["ma"]
    assert sync157_manifest.MANIFEST["mi"]["scrape"]["retry"] is False


def test_shipped_config_is_internally_consistent():
    """backoff_secs supplies the waits *between* attempts, so N attempts need at most N-1 values.
    More values than that means someone mis-set one of the two and the extras are dead."""
    retry = _shipped_retry_cfg()
    assert len(retry["backoff_secs"]) <= retry["max_attempts"] - 1
    assert all(isinstance(s, int) and s > 0 for s in retry["backoff_secs"])


def test_mi_is_not_retryable_under_the_shipped_config():
    """End-to-end over the real YAML rather than a fixture: whatever the file says, MI must come
    out ineligible."""
    from pathlib import Path

    import yaml

    cfg_path = Path(__file__).resolve().parents[1] / "config" / "sync_schedule.yaml"
    cfg = yaml.safe_load(cfg_path.read_text())["openstates_scrape"]
    assert _retry_eligible("mi", cfg) is False
    assert _retry_eligible("ma", cfg) is True
