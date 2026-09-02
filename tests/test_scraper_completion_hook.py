"""Unit tests for SYNC-50's real scraper-completion hook.

SYNC-48 built `trigger_scraper_session_pipeline()` but deliberately never wired it to a
real scrape-completion event, because a completed scrape tells you a *jurisdiction*, not
a *session* -- and a jurisdiction can have more than one session simultaneously active
(VA, UT). `_maybe_trigger_legbot_for_scrape()` closes that gap by resolving which
session(s) actually had a bill touched by this scrape run (via
`resolve_touched_sessions()`'s own `updated_since` read), then triggering the pipeline
once per resolved session.

Covers:
  * the flag gate -- disabled means zero calls of any kind, not even session resolution
  * one resolved session triggers exactly once, with `include_org_research=False`
    (Gate 1 item 4's decision) and the configured artifact_types/limit/concept-statements
  * multiple resolved sessions (the VA/UT-shaped case SYNC-50's own AC names) trigger once
    per session
  * zero resolved sessions triggers nothing
  * a session-resolution exception is swallowed, never propagates to the scrape job
  * a per-session trigger exception doesn't stop the remaining sessions
  * the `_run_scrape` / `_run_scrape_impl` wrapper split: success invokes the hook,
    failure does not
  * a cloud-owned jurisdiction's success is skipped explicitly (pm-review round 1: its
    scraped data lands in RDS, not the local Postgres `resolve_touched_sessions()`
    reads -- resolution would otherwise silently find nothing there forever)
  * a hook-body exception (including one from `get_settings()` or a lazy import, not
    just the two call sites `_maybe_trigger_legbot_for_scrape` already wraps in its own
    try/except) can never propagate out of `_run_scrape` and replace a successful
    scrape's own result (pm-review round 1: the original wrapper's try/except coverage
    had a real gap here)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.openstates_scrape import (
    _maybe_trigger_legbot_for_scrape,
    _run_scrape,
)


def _enabled_settings(**overrides) -> SyncSettings:
    defaults = dict(
        session_pipeline_scraper_trigger_enabled=True,
        session_pipeline_scraper_trigger_artifact_types=["bill_summary", "bill_changelog"],
        session_pipeline_scraper_trigger_limit=10000,
        session_pipeline_scraper_trigger_include_concept_statements=True,
        session_pipeline_scraper_trigger_resolution_max_bills=500,
    )
    defaults.update(overrides)
    return SyncSettings(**defaults)


# ── _maybe_trigger_legbot_for_scrape: the flag gate ─────────────────────────────────────


@pytest.mark.asyncio
async def test_disabled_flag_skips_session_resolution_entirely(monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape.get_settings",
        lambda: _enabled_settings(session_pipeline_scraper_trigger_enabled=False),
    )
    with patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(),
    ) as mock_resolve:
        await _maybe_trigger_legbot_for_scrape("va", datetime.now(timezone.utc))

    mock_resolve.assert_not_awaited()


# ── _maybe_trigger_legbot_for_scrape: real resolution → dispatch ───────────────────────


@pytest.mark.asyncio
async def test_one_resolved_session_triggers_once_with_org_research_disabled(monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape.get_settings",
        lambda: _enabled_settings(),
    )
    with (
        patch(
            "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
            new=AsyncMock(return_value=["2026"]),
        ),
        patch(
            "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
            new=AsyncMock(return_value={"success": True}),
        ) as mock_trigger,
    ):
        await _maybe_trigger_legbot_for_scrape("va", datetime.now(timezone.utc))

    mock_trigger.assert_awaited_once_with(
        "VA",
        "2026",
        ["bill_summary", "bill_changelog"],
        False,
        10000,
        include_concept_statements=True,
    )


@pytest.mark.asyncio
async def test_multiple_resolved_sessions_each_trigger_once(monkeypatch):
    """The VA/UT-shaped case SYNC-50's own AC names: two simultaneously active
    sessions in one jurisdiction must both get triggered, not just one."""
    from datetime import datetime, timezone

    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape.get_settings",
        lambda: _enabled_settings(),
    )
    with (
        patch(
            "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
            new=AsyncMock(return_value=["2026", "2026S1"]),
        ),
        patch(
            "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
            new=AsyncMock(return_value={"success": True}),
        ) as mock_trigger,
    ):
        await _maybe_trigger_legbot_for_scrape("va", datetime.now(timezone.utc))

    assert mock_trigger.await_count == 2
    triggered_sessions = {call.args[1] for call in mock_trigger.await_args_list}
    assert triggered_sessions == {"2026", "2026S1"}


@pytest.mark.asyncio
async def test_zero_resolved_sessions_triggers_nothing(monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape.get_settings",
        lambda: _enabled_settings(),
    )
    with (
        patch(
            "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
            new=AsyncMock(),
        ) as mock_trigger,
    ):
        await _maybe_trigger_legbot_for_scrape("mi", datetime.now(timezone.utc))

    mock_trigger.assert_not_awaited()


# ── _maybe_trigger_legbot_for_scrape: failures never propagate ─────────────────────────


@pytest.mark.asyncio
async def test_session_resolution_exception_is_swallowed(monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape.get_settings",
        lambda: _enabled_settings(),
    )
    with (
        patch(
            "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ),
        patch(
            "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
            new=AsyncMock(),
        ) as mock_trigger,
    ):
        await _maybe_trigger_legbot_for_scrape("mi", datetime.now(timezone.utc))  # must not raise

    mock_trigger.assert_not_awaited()


@pytest.mark.asyncio
async def test_one_session_trigger_exception_does_not_stop_the_next(monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape.get_settings",
        lambda: _enabled_settings(),
    )
    with (
        patch(
            "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
            new=AsyncMock(return_value=["2026", "2026S1"]),
        ),
        patch(
            "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
            new=AsyncMock(side_effect=[RuntimeError("boom"), {"success": True}]),
        ) as mock_trigger,
    ):
        await _maybe_trigger_legbot_for_scrape("va", datetime.now(timezone.utc))  # must not raise

    assert mock_trigger.await_count == 2


# ── _run_scrape / _run_scrape_impl wrapper split ────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_scrape_invokes_hook_after_a_successful_scrape(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        AsyncMock(return_value={"success": True, "jurisdiction": "mi", "duration_seconds": 1.0}),
    )
    with patch(
        "ddp_sync.pipelines.openstates_scrape._maybe_trigger_legbot_for_scrape",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_scrape("mi", None, "/fake/root")

    assert result["success"] is True
    mock_hook.assert_awaited_once()
    assert mock_hook.await_args.args[0] == "mi"


@pytest.mark.asyncio
async def test_run_scrape_skips_hook_after_a_failed_scrape(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        AsyncMock(return_value={"success": False, "jurisdiction": "mi", "error": "boom"}),
    )
    with patch(
        "ddp_sync.pipelines.openstates_scrape._maybe_trigger_legbot_for_scrape",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_scrape("mi", None, "/fake/root")

    assert result["success"] is False
    mock_hook.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_scrape_skips_hook_for_a_cloud_owned_jurisdiction_even_on_success(monkeypatch):
    """pm-review round 1: OPEN-193's cloud-owned branch loads into RDS, a separate
    database from the local Postgres resolve_touched_sessions() reads -- resolving
    against local api-v3 for a cloud-owned jurisdiction would silently find nothing,
    forever, not because nothing changed but because it's the wrong database. The
    wrapper must skip the hook entirely for a cloud-owned jurisdiction rather than
    let that play out as a permanent silent no-op."""
    config = {"cloud_path": {"enabled": True, "jurisdictions": ["mi"]}}
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        AsyncMock(
            return_value={
                "success": True,
                "jurisdiction": "mi",
                "duration_seconds": 42.0,
                "cloud_run_id": "mi-abc123",
            }
        ),
    )
    with patch(
        "ddp_sync.pipelines.openstates_scrape._maybe_trigger_legbot_for_scrape",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_scrape("mi", None, "/fake/root", config=config)

    assert result["cloud_run_id"] == "mi-abc123"
    mock_hook.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_scrape_still_invokes_hook_for_a_non_cloud_owned_jurisdiction(monkeypatch):
    """Same config object, but the jurisdiction isn't in the cloud_path list -- the
    hook must still fire normally. Guards against the cloud-owned check accidentally
    gating on "config was passed" instead of real per-jurisdiction ownership."""
    config = {"cloud_path": {"enabled": True, "jurisdictions": ["mi"]}}
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        AsyncMock(return_value={"success": True, "jurisdiction": "va", "duration_seconds": 1.0}),
    )
    with patch(
        "ddp_sync.pipelines.openstates_scrape._maybe_trigger_legbot_for_scrape",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_scrape("va", None, "/fake/root", config=config)

    assert result["success"] is True
    mock_hook.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_scrape_isolates_a_hook_exception_from_the_scrape_result(monkeypatch):
    """pm-review round 1: the original wrapper only relied on
    _maybe_trigger_legbot_for_scrape's own internal try/excepts, which don't cover
    its get_settings() call or its lazy imports -- an exception there would have
    escaped _run_scrape entirely and turned an already-successful scrape into a
    raised exception. The wrapper's own try/except must catch anything, not just
    the cases the hook function already anticipated."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        AsyncMock(return_value={"success": True, "jurisdiction": "mi", "duration_seconds": 1.0}),
    )
    with patch(
        "ddp_sync.pipelines.openstates_scrape._maybe_trigger_legbot_for_scrape",
        new=AsyncMock(side_effect=RuntimeError("settings blew up")),
    ):
        result = await _run_scrape("mi", None, "/fake/root")  # must not raise

    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_scrape_isolates_a_cloud_ownership_check_exception_too(monkeypatch):
    """pm-review round 2: round 1's fix wrapped the hook call but left
    _cloud_path_owns() itself outside the try/except -- exercised here directly,
    since the wrapper's contract is "nothing after a successful scrape may change
    that scrape's result," not "nothing we currently believe can raise"."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        AsyncMock(return_value={"success": True, "jurisdiction": "mi", "duration_seconds": 1.0}),
    )
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._cloud_path_owns",
            side_effect=RuntimeError("config blew up"),
        ),
        patch(
            "ddp_sync.pipelines.openstates_scrape._maybe_trigger_legbot_for_scrape",
            new=AsyncMock(),
        ) as mock_hook,
    ):
        result = await _run_scrape("mi", None, "/fake/root")  # must not raise

    assert result["success"] is True
    mock_hook.assert_not_awaited()
