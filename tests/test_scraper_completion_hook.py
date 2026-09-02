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
    failure does not, and both of `_run_scrape_impl`'s own success paths (local
    subprocess vs. OPEN-193's cloud-owned branch) reach it uniformly through the one
    wrapper -- no per-path duplication.
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
async def test_run_scrape_invokes_hook_for_a_cloud_owned_success_too(monkeypatch):
    """The wrapper sits above `_run_scrape_impl` entirely, so OPEN-193's cloud-owned
    branch (inside `_run_scrape_impl`) reaches the hook the same way the local
    run-scrape.sh branch does -- no separate wiring needed at that branch point."""
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
        result = await _run_scrape("mi", None, "/fake/root")

    assert result["cloud_run_id"] == "mi-abc123"
    mock_hook.assert_awaited_once()
