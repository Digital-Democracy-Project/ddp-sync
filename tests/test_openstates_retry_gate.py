"""Tests for OPEN-87's per-jurisdiction bounded-retry gate.

These scrapes run weekly, so a run killed by a transient fault waits a week for its next attempt
-- MA lost a run that way to a network timeout on 2026-08-01. The gate here decides which
jurisdictions get run-scrape-retrying.sh instead of run-scrape.sh.

The half that actually carries risk is the OFF half, and it is what most of these tests pin
down. Two things must stay true no matter how the rollout is widened later:

  * MI must never be retried. OPEN-53 established that a blind retry against a WAF worsens a
    block rather than recovering from it, so retrying MI is not a wasted attempt, it is an
    actively harmful one. `jurisdictions_excluded` has to beat `jurisdictions`, including when
    someone lists MI in both.
  * A jurisdiction that has not opted in must be invoked exactly as it is today -- same script,
    same argv, no retry env vars. The opt-out is the absence of a wrapper process, not a
    wrapper told to behave itself.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import _retry_eligible, _run_scrape

ROLLOUT_CFG = {
    "retry": {
        "enabled": True,
        "jurisdictions": ["ma", "usa"],
        "jurisdictions_excluded": ["mi"],
        "max_attempts": 3,
        "backoff_secs": [900, 1800],
    }
}


# --- the gate itself -------------------------------------------------------------------

@pytest.mark.parametrize("jurisdiction", ["ma", "usa"])
def test_listed_jurisdictions_are_eligible(jurisdiction):
    assert _retry_eligible(jurisdiction, ROLLOUT_CFG) is True


@pytest.mark.parametrize("jurisdiction", ["mi", "va", "ut", "fl", "wa", "az", "al"])
def test_unlisted_jurisdictions_are_not_eligible(jurisdiction):
    """The rollout must not leak to anything else."""
    assert _retry_eligible(jurisdiction, ROLLOUT_CFG) is False


def test_exclusion_beats_the_allowlist():
    """The case this exists for: the rollout finishes, someone widens `jurisdictions` to
    everything, and MI must still be excluded. A retry against a WAF makes the block worse
    (OPEN-53), so this is the one entry that must survive a careless widening."""
    cfg = {
        "retry": {
            "enabled": True,
            "jurisdictions": ["ma", "usa", "mi", "va", "ut", "fl", "wa", "az"],
            "jurisdictions_excluded": ["mi"],
        }
    }
    assert _retry_eligible("mi", cfg) is False
    assert _retry_eligible("ma", cfg) is True


def test_disabled_flag_overrides_the_list():
    """The documented rollback: flip enabled: false and the list stops mattering."""
    cfg = {"retry": {"enabled": False, "jurisdictions": ["ma", "usa"]}}
    assert _retry_eligible("ma", cfg) is False


@pytest.mark.parametrize(
    "cfg",
    [
        None,
        {},
        {"retry": {}},
        {"retry": {"enabled": True}},  # enabled but no list
    ],
)
def test_absent_or_incomplete_config_is_off(cfg):
    """Absent config must be off, not on -- this is the pre-change behaviour for every caller
    that passes no config at all."""
    assert _retry_eligible("ma", cfg) is False


# --- does it actually change what gets invoked? --------------------------------------

async def _captured_call_for(jurisdiction, config, session_arg=None):
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
        "retry": {
            "enabled": True,
            "jurisdictions": ["ma"],
            "max_attempts": 5,
            "backoff_secs": [1800, 3600, 7200, 7200],
        }
    }
    _, env = await _captured_call_for("ma", cfg)
    assert env["SCRAPE_RETRY_MAX_ATTEMPTS"] == "5"
    assert env["SCRAPE_RETRY_BACKOFF_SECS"] == "1800,3600,7200,7200"


# --- the shipped config ---------------------------------------------------------------

def _shipped_retry_cfg():
    from pathlib import Path

    import yaml

    cfg_path = Path(__file__).resolve().parents[1] / "config" / "sync_schedule.yaml"
    cfg = yaml.safe_load(cfg_path.read_text())
    return cfg["openstates_scrape"]["retry"]


def test_shipped_yaml_enables_exactly_the_staged_rollout():
    """Guards the actual rollout state, so widening it is a deliberate, reviewed edit."""
    retry = _shipped_retry_cfg()
    assert retry["enabled"] is True
    assert retry["jurisdictions"] == ["ma", "usa"]
    assert retry["max_attempts"] == 3


def test_shipped_yaml_excludes_mi():
    """Separate test on purpose. If someone widens the rollout above, this one must still pass,
    and its failure should read as "you have re-enabled retries for MI" and nothing else."""
    assert "mi" in _shipped_retry_cfg()["jurisdictions_excluded"]


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
