"""Unit tests for SYNC-65's archive-completion LegBot hook.

SYNC-50/SYNC-59 built two scrape-completion hooks (removed by this same ticket,
see git history for `_maybe_trigger_legbot_for_scrape` /
`_maybe_trigger_legbot_for_cloud_scrape`). Both fired immediately after a scrape
succeeded, but LegBot's `_resolve_bill_source()` (`bill_artifact_generation.py`)
only ever reads already-archived, already-extracted text (`get_archived_bill_text`,
OPEN-13) -- it has no live-fetch fallback. Archiving runs on its own separate,
per-jurisdiction weekly schedule with no connection to the scrape schedule, so a
scrape-triggered dispatch routinely lost that race and produced a permanent
`status="failed", failure_reason="no_archived_bill_text"` (SYNC-42's
`retry_failed=False` default means nothing ever retries it).

`_maybe_trigger_legbot_for_archive()` removes the race by triggering off archive
completion instead -- the actual producer of the data LegBot depends on. Its shape
directly mirrors the deleted `_maybe_trigger_legbot_for_scrape`: resolve which
session(s) actually had a bill touched (via `resolve_touched_sessions()`'s own
`updated_since` read), then trigger the SYNC-48 pipeline once per resolved session.

Covers:
  * the flag gate -- disabled means zero calls of any kind, not even session
    resolution
  * one resolved session triggers exactly once, with `include_org_research=False`
    (the same Gate 1 item 4 decision the scrape-side hook already made) and the
    configured artifact_types/limit/concept-statements
  * multiple resolved sessions each trigger once
  * zero resolved sessions triggers nothing
  * a session-resolution exception is swallowed, never propagates to the archive
    job
  * a per-session trigger exception doesn't stop the remaining sessions
  * the `_run_archive_with_hook` wrapper: success (both the local run-archive.sh
    branch and the Fargate cloud_archiver.py branch) invokes the hook, failure
    does not
  * a hook-body exception can never propagate out of `_run_archive_with_hook` and
    replace an already-successful archive job's own result -- same pm-review
    round 1 finding `_run_scrape` already had to fix, applied here from the start
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.openstates_archive import (
    _maybe_trigger_legbot_for_archive,
    _run_archive_with_hook,
)


def _enabled_settings(**overrides) -> SyncSettings:
    defaults = dict(
        legbot_scrape_completion_trigger_enabled=True,
        legbot_scrape_completion_trigger_artifact_types=["bill_summary", "bill_changelog"],
        legbot_scrape_completion_trigger_limit=10000,
        legbot_scrape_completion_trigger_include_concept_statements=True,
        legbot_scrape_completion_trigger_resolution_max_bills=500,
    )
    defaults.update(overrides)
    return SyncSettings(**defaults)


# ── _maybe_trigger_legbot_for_archive: the flag gate ────────────────────────────────────


@pytest.mark.asyncio
async def test_disabled_flag_skips_session_resolution_entirely(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
        lambda: _enabled_settings(legbot_scrape_completion_trigger_enabled=False),
    )
    with patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(),
    ) as mock_resolve:
        await _maybe_trigger_legbot_for_archive("va", datetime.now(timezone.utc))

    mock_resolve.assert_not_awaited()


# ── _maybe_trigger_legbot_for_archive: real resolution → dispatch ──────────────────────


@pytest.mark.asyncio
async def test_one_resolved_session_triggers_once_with_org_research_disabled(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
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
        await _maybe_trigger_legbot_for_archive("va", datetime.now(timezone.utc))

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
    """The VA/UT-shaped case: two simultaneously active sessions in one
    jurisdiction must both get triggered, not just one."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
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
        await _maybe_trigger_legbot_for_archive("va", datetime.now(timezone.utc))

    assert mock_trigger.await_count == 2
    triggered_sessions = {call.args[1] for call in mock_trigger.await_args_list}
    assert triggered_sessions == {"2026", "2026S1"}


@pytest.mark.asyncio
async def test_zero_resolved_sessions_triggers_nothing(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
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
        await _maybe_trigger_legbot_for_archive("mi", datetime.now(timezone.utc))

    mock_trigger.assert_not_awaited()


# ── _maybe_trigger_legbot_for_archive: failures never propagate ────────────────────────


@pytest.mark.asyncio
async def test_session_resolution_exception_is_swallowed(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
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
        await _maybe_trigger_legbot_for_archive("mi", datetime.now(timezone.utc))  # must not raise

    mock_trigger.assert_not_awaited()


@pytest.mark.asyncio
async def test_one_session_trigger_exception_does_not_stop_the_next(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
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
        await _maybe_trigger_legbot_for_archive("va", datetime.now(timezone.utc))  # must not raise

    assert mock_trigger.await_count == 2


# ── _run_archive_with_hook: local + Fargate branches, both wired to the hook ───────────


@pytest.mark.asyncio
async def test_local_archive_success_invokes_the_hook(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive._run_archive",
        AsyncMock(return_value={"success": True, "jurisdiction": "mi", "duration_seconds": 1.0}),
    )
    with patch(
        "ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_archive_with_hook("mi", "/fake/root", config={})

    assert result["success"] is True
    mock_hook.assert_awaited_once()
    assert mock_hook.await_args.args[0] == "mi"


@pytest.mark.asyncio
async def test_fargate_archive_success_invokes_the_hook_too(monkeypatch):
    """use_fargate routes to _run_archive_fargate instead of _run_archive -- the
    hook must fire for that branch identically, not just the local one."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive._run_archive_fargate",
        AsyncMock(return_value={"success": True, "jurisdiction": "us", "duration_seconds": 1.0}),
    )
    with patch(
        "ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_archive_with_hook("us", config={"use_fargate": True})

    assert result["success"] is True
    mock_hook.assert_awaited_once()
    assert mock_hook.await_args.args[0] == "us"


@pytest.mark.asyncio
async def test_failed_archive_skips_the_hook(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive._run_archive",
        AsyncMock(return_value={"success": False, "jurisdiction": "mi", "error": "boom"}),
    )
    with patch(
        "ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_archive_with_hook("mi", "/fake/root", config={})

    assert result["success"] is False
    mock_hook.assert_not_awaited()


@pytest.mark.asyncio
async def test_hook_exception_never_replaces_a_successful_archive_result(monkeypatch):
    """Same pm-review round 1 finding _run_scrape's own wrapper had to fix: the
    whole post-success block must be wrapped, not just relying on the hook's own
    internal try/excepts (which don't cover get_settings() or a lazy import)."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive._run_archive",
        AsyncMock(return_value={"success": True, "jurisdiction": "mi", "duration_seconds": 1.0}),
    )
    with patch(
        "ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive",
        new=AsyncMock(side_effect=RuntimeError("settings blew up")),
    ):
        result = await _run_archive_with_hook("mi", "/fake/root", config={})  # must not raise

    assert result["success"] is True
