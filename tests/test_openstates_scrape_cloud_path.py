"""Tests for OPEN-208: exactly one path owns a jurisdiction.

_cloud_path_owns()/_memory_backend_enabled() are the ddp-sync side of the "one path owns
a jurisdiction" gate (PLAN-scraper-execution-migration.md §3) -- config lives under
openstates_scrape.cloud_path in sync_schedule.yaml, mirroring the shape
scrapebot_fallback/sweep_import/scrape_retry already use (OPEN-124/OPEN-140).

The acceptance question is not "does the config parse". It is:

  * a jurisdiction listed under cloud_path.jurisdictions must never have run-scrape.sh
    (or run-scrape-retrying.sh) invoked for it at all -- checked through the ONE funnel
    every launch path already goes through (_run_scrape()), so this covers scheduled
    jobs, the manual-trigger endpoint, and retry by construction.
  * a jurisdiction absent from cloud_path (or with the feature disabled) sees zero
    behavior change -- the same "opt-in, not opt-out" discipline every sibling
    eligibility function in this file already follows.
  * S3-backed memory (OPEN-181) is a FLOOR, not tied 1:1 to current cloud ownership: a
    jurisdiction rolled BACK from cloud to mac still needs it, or its first run back
    silently full-walks instead of hydrating from the store.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    _cloud_path_owns,
    _memory_backend_enabled,
    _run_scrape,
    run_single_scrape_job,
)


def _config(*, enabled=True, jurisdictions=("mi",), memory_backend_jurisdictions=()):
    return {
        "cloud_path": {
            "enabled": enabled,
            "jurisdictions": list(jurisdictions),
            "memory_backend_jurisdictions": list(memory_backend_jurisdictions),
        }
    }


# ── _cloud_path_owns ────────────────────────────────────────────────────────────────────────


def test_owns_true_when_enabled_and_listed():
    assert _cloud_path_owns("mi", _config()) is True


def test_owns_false_when_disabled():
    assert _cloud_path_owns("mi", _config(enabled=False)) is False


def test_owns_false_when_jurisdiction_not_listed():
    assert _cloud_path_owns("va", _config()) is False


def test_owns_false_when_config_missing_entirely():
    assert _cloud_path_owns("mi", None) is False
    assert _cloud_path_owns("mi", {}) is False


# ── _memory_backend_enabled -- the floor ────────────────────────────────────────────────────


def test_memory_backend_enabled_for_a_currently_cloud_owned_jurisdiction():
    assert _memory_backend_enabled("mi", _config(jurisdictions=("mi",))) is True


def test_memory_backend_enabled_for_a_rolled_back_jurisdiction():
    """THE rollback case: mi is no longer in `jurisdictions` (cloud no longer owns it,
    so the mac path runs it again), but it IS in `memory_backend_jurisdictions` -- the
    floor an operator never shrinks. Memory must stay on so this run hydrates from S3
    instead of silently full-walking."""
    config = _config(jurisdictions=(), memory_backend_jurisdictions=("mi",))
    assert _cloud_path_owns("mi", config) is False  # mac runs it again
    assert _memory_backend_enabled("mi", config) is True  # but still S3-backed


def test_memory_backend_disabled_for_a_jurisdiction_never_split():
    config = _config(jurisdictions=("mi",), memory_backend_jurisdictions=())
    assert _memory_backend_enabled("va", config) is False


def test_memory_backend_disabled_when_config_missing_entirely():
    assert _memory_backend_enabled("mi", None) is False
    assert _memory_backend_enabled("mi", {}) is False


# ── _run_scrape: the actual refusal, through the one real funnel ───────────────────────────


@pytest.mark.asyncio
async def test_run_scrape_routes_a_cloud_owned_jurisdiction_to_the_cloud_trigger():
    """OPEN-193: cloud ownership used to be a bare skip -- now it's what actually triggers
    the Fargate collection + RDS load. The local mac-side subprocess must still never run
    (OPEN-208's "exactly one path" invariant), but "nothing happens" is no longer correct."""
    config = _config(jurisdictions=("mi",))
    cloud_result = {
        "success": True,
        "jurisdiction": "mi",
        "duration_seconds": 42.0,
        "cloud_run_id": "mi-abc123",
    }
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        ) as mock_run,
        patch(
            "ddp_sync.pipelines.openstates_scrape.run_cloud_scrape",
            return_value=cloud_result,
        ) as mock_cloud,
    ):
        result = await _run_scrape("mi", None, "/fake/root", config=config)

    mock_run.assert_not_called()
    mock_cloud.assert_called_once_with("mi", None, "/fake/root", config)
    assert result == cloud_result


@pytest.mark.asyncio
async def test_run_scrape_triggers_cloud_before_scrapebot_preseed_for_a_cloud_owned_jurisdiction():
    """The docstring's ordering claim, pinned down: the ownership check runs BEFORE
    ScrapeBot cookie pre-seeding, so a cloud-owned jurisdiction never triggers a real
    cookie mint against its WAF (MI's, in practice) for a scrape the mac isn't going
    to run -- that stays true now that the cloud branch does real work instead of a
    no-op skip. Regression coverage for pm-review's "assert the ordering, not just the
    non-invocation" finding on the original PR."""
    config = _config(jurisdictions=("mi",))
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._maybe_preseed_scrapebot_cookies",
        ) as mock_preseed,
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        ) as mock_run,
        patch(
            "ddp_sync.pipelines.openstates_scrape.run_cloud_scrape",
            return_value={"success": True, "jurisdiction": "mi", "duration_seconds": 1.0},
        ),
    ):
        result = await _run_scrape("mi", None, "/fake/root", config=config)

    mock_preseed.assert_not_called()
    mock_run.assert_not_called()
    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_scrape_runs_normally_for_a_jurisdiction_not_cloud_owned():
    config = _config(jurisdictions=("mi",))  # va is not in the list
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        return_value=(0, b"", b"", False, False),
    ) as mock_run:
        result = await _run_scrape("va", None, "/fake/root", config=config)

    mock_run.assert_called_once()
    assert result["success"] is True
    assert "skipped" not in result


@pytest.mark.asyncio
async def test_run_scrape_is_a_noop_gate_when_config_omitted():
    """Every existing _run_scrape caller that doesn't pass config must see zero
    behavior change -- same discipline as every other eligibility function here."""
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        return_value=(0, b"", b"", False, False),
    ) as mock_run:
        result = await _run_scrape("mi", None, "/fake/root")

    mock_run.assert_called_once()
    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_single_scrape_job_also_routes_to_the_cloud_trigger():
    """The manual-trigger endpoint (POST /trigger/openstates-scrape/<state>) funnels
    through _run_scrape() too -- proving OPEN-208's "a manual invocation for a
    jurisdiction owned by the other path never runs the local subprocess" criterion for
    free, not via a separate check duplicated at the trigger route."""
    config = _config(jurisdictions=("mi",))
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
        ) as mock_run,
        patch(
            "ddp_sync.pipelines.openstates_scrape.run_cloud_scrape",
            return_value={"success": True, "jurisdiction": "mi", "duration_seconds": 1.0},
        ) as mock_cloud,
    ):
        result = await run_single_scrape_job("mi", config)

    mock_run.assert_not_called()
    mock_cloud.assert_called_once()
    assert result["success"] is True


# ── _run_scrape: the memory-backend env vars actually reach the subprocess ─────────────────


@pytest.mark.asyncio
async def test_run_scrape_sets_memory_backend_env_when_enabled():
    config = _config(jurisdictions=(), memory_backend_jurisdictions=("mi",))
    captured = {}

    def fake_run(cmd, env, timeout, *args, **kwargs):
        captured["env"] = env
        return 0, b"", b"", False, False

    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill", side_effect=fake_run
    ):
        result = await _run_scrape("mi", None, "/fake/root", config=config)

    assert result["success"] is True
    assert captured["env"]["SCRAPER_MEMORY_BACKEND"] == "s3"
    assert captured["env"]["SCRAPER_MEMORY_PREFIX"] == "prod"


@pytest.mark.asyncio
async def test_run_scrape_does_not_set_memory_backend_env_for_an_unsplit_jurisdiction():
    captured = {}

    def fake_run(cmd, env, timeout, *args, **kwargs):
        captured["env"] = env
        return 0, b"", b"", False, False

    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_with_group_kill", side_effect=fake_run
    ):
        await _run_scrape("va", None, "/fake/root")

    assert "SCRAPER_MEMORY_BACKEND" not in captured["env"]
    assert "SCRAPER_MEMORY_PREFIX" not in captured["env"]
