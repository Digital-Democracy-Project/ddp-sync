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
    """Defaults to the Mac-capable path (cams_api_token set) -- most of this file's
    existing coverage predates the Mac/EC2 split and assumes in-process dispatch.
    See _enabled_ec2_settings() for the non-Mac-capable (WireGuard-hop) path."""
    defaults = dict(
        legbot_scrape_completion_trigger_enabled=True,
        legbot_scrape_completion_trigger_artifact_types=["bill_summary", "bill_changelog"],
        legbot_scrape_completion_trigger_limit=10000,
        legbot_scrape_completion_trigger_include_concept_statements=True,
        legbot_scrape_completion_trigger_resolution_max_bills=500,
        cams_api_token="fake-mac-cams-token",
    )
    defaults.update(overrides)
    return SyncSettings(**defaults)


def _enabled_ec2_settings(**overrides) -> SyncSettings:
    """SYNC-65 (real conflict found by the prod agent, 2026-09-13): the EC2-broker
    instance's own shape -- no cams_api_token, but rds_openstates_api_base and the
    Mac WireGuard target both configured."""
    defaults = dict(
        cams_api_token="",
        rds_openstates_api_base="http://10.0.0.9:8002",
        rds_openstates_api_key="fake-rds-api-key",
        mac_ddp_sync_base_url="http://10.0.0.8:8001",
        mac_ddp_sync_api_key="fake-mac-ddp-sync-key",
    )
    defaults.update(overrides)
    return _enabled_settings(**defaults)


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
async def test_resolution_uses_document_updated_since_not_updated_since(monkeypatch):
    """The actual bug pm-review caught in this ticket's first version:
    archive_bill_versions() (openstates-core) never touches Bill.updated_at,
    only the BillVersionDocument row's own updated_at -- so reusing the
    scrape hook's default `since_param="updated_since"` here would have
    silently resolved zero sessions on every real archive run. Regression
    guard for that specific mistake, not just "resolve_touched_sessions gets
    called" (which every other test in this file already covers)."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
        lambda: _enabled_settings(),
    )
    with patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(return_value=[]),
    ) as mock_resolve:
        await _maybe_trigger_legbot_for_archive("va", datetime.now(timezone.utc))

    assert mock_resolve.await_args.kwargs["since_param"] == "document_updated_since"


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


@pytest.mark.asyncio
async def test_resolution_failure_logs_error_not_info_and_triggers_nothing(monkeypatch):
    """SYNC-66: resolve_touched_sessions returning None means resolution itself
    failed (e.g. api-v3 stayed unreachable across every retry) -- this must be
    logged as a real failure (ERROR), not folded into the same INFO-level
    "nothing touched" path a genuine empty result gets. Root cause of the
    production incident this ticket was filed from: both cases used to look
    identical in the logs."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
        lambda: _enabled_settings(),
    )
    with (
        patch(
            "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
            new=AsyncMock(),
        ) as mock_trigger,
        patch("ddp_sync.pipelines.openstates_archive.logger") as mock_logger,
    ):
        await _maybe_trigger_legbot_for_archive("us", datetime.now(timezone.utc))

    mock_trigger.assert_not_awaited()
    mock_logger.error.assert_called_once()
    assert mock_logger.error.call_args.args[0] == (
        "archiver_triggered_legbot_session_resolution_failed"
    )
    mock_logger.info.assert_not_called()


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


# ── _maybe_trigger_legbot_for_archive: EC2 (not Mac-capable) routes over WireGuard ──────
#
# SYNC-65 (real conflict found by the prod agent, 2026-09-13): this ticket's first version
# assumed archiving only ever runs on the Mac's own ddp-sync instance. OPENSTATES_ARCHIVE_
# ENABLED is now also true on the EC2-broker instance (OPEN-192), which has no local CAMS
# server at all -- confirmed directly (CAMS_BASE_URL unset there). These tests cover the
# fix: when cams_api_token isn't configured, resolve sessions via rds_openstates_api_base
# instead of the Mac's local api-v3, and dispatch each session over WireGuard instead of
# calling trigger_scraper_session_pipeline in-process.


@pytest.mark.asyncio
async def test_ec2_resolves_sessions_via_rds_api_base_not_local(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
        lambda: _enabled_ec2_settings(),
    )
    with (
        patch(
            "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
            new=AsyncMock(return_value=["2026"]),
        ) as mock_resolve,
        patch(
            "ddp_sync.pipelines.openstates_archive._trigger_legbot_session_via_mac_wireguard",
            new=AsyncMock(),
        ),
    ):
        await _maybe_trigger_legbot_for_archive("us", datetime.now(timezone.utc))

    assert mock_resolve.await_args.kwargs["api_base"] == "http://10.0.0.9:8002"
    assert mock_resolve.await_args.kwargs["api_key"] == "fake-rds-api-key"
    assert mock_resolve.await_args.kwargs["since_param"] == "document_updated_since"


@pytest.mark.asyncio
async def test_ec2_dispatches_over_wireguard_not_in_process(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
        lambda: _enabled_ec2_settings(),
    )
    with (
        patch(
            "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
            new=AsyncMock(return_value=["2026", "2026S1"]),
        ),
        patch(
            "ddp_sync.pipelines.openstates_archive._trigger_legbot_session_via_mac_wireguard",
            new=AsyncMock(),
        ) as mock_wireguard,
        patch(
            "ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
            new=AsyncMock(),
        ) as mock_in_process,
    ):
        await _maybe_trigger_legbot_for_archive("us", datetime.now(timezone.utc))

    assert mock_wireguard.await_count == 2
    triggered = {call.args[1] for call in mock_wireguard.await_args_list}
    assert triggered == {"2026", "2026S1"}
    mock_in_process.assert_not_awaited()


@pytest.mark.asyncio
async def test_ec2_skips_entirely_when_rds_api_base_unconfigured(monkeypatch):
    monkeypatch.setattr(
        "ddp_sync.pipelines.openstates_archive.get_settings",
        lambda: _enabled_ec2_settings(rds_openstates_api_base=""),
    )
    with patch(
        "ddp_sync.services.local_openstates_client.resolve_touched_sessions",
        new=AsyncMock(),
    ) as mock_resolve:
        await _maybe_trigger_legbot_for_archive("us", datetime.now(timezone.utc))

    mock_resolve.assert_not_awaited()


@pytest.mark.asyncio
async def test_wireguard_helper_posts_to_the_mac_and_logs_success(monkeypatch):
    mock_resp = AsyncMock()
    mock_resp.raise_for_status = lambda: None
    mock_resp.json = lambda: {"success": True, "run_id": "abc123"}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    from ddp_sync.pipelines.openstates_archive import _trigger_legbot_session_via_mac_wireguard

    with patch("httpx.AsyncClient", return_value=mock_client):
        await _trigger_legbot_session_via_mac_wireguard(
            "US", "2026", _enabled_ec2_settings()
        )

    call = mock_client.post.await_args
    assert call.args[0] == "http://10.0.0.8:8001/ddp-sync/v1/trigger/bill-artifact-generation"
    assert call.kwargs["headers"]["Authorization"] == "Bearer fake-mac-ddp-sync-key"
    assert call.kwargs["headers"]["X-DDP-Automated-Trigger"] == "true"
    assert call.kwargs["json"] == {
        "jurisdiction_iso2": "US",
        "session_code": "2026",
        "artifact_types": ["bill_summary", "bill_changelog"],
        "include_org_research": False,
        "include_concept_statements": True,
        "limit": 10000,
        "retry_failed": False,
        "dry_run": False,
    }


@pytest.mark.asyncio
async def test_wireguard_helper_no_mac_target_configured_makes_no_call():
    with patch("httpx.AsyncClient") as mock_client_cls:
        from ddp_sync.pipelines.openstates_archive import (
            _trigger_legbot_session_via_mac_wireguard,
        )

        await _trigger_legbot_session_via_mac_wireguard(
            "US", "2026", _enabled_ec2_settings(mac_ddp_sync_base_url="")
        )

    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_wireguard_helper_no_mac_api_key_configured_makes_no_call():
    """pm-review: a base URL with no key is a distinct, real misconfiguration --
    catch it before ever sending a request guaranteed to come back 401."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        from ddp_sync.pipelines.openstates_archive import (
            _trigger_legbot_session_via_mac_wireguard,
        )

        await _trigger_legbot_session_via_mac_wireguard(
            "US", "2026", _enabled_ec2_settings(mac_ddp_sync_api_key="")
        )

    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_wireguard_helper_non_dict_json_response_never_raises(monkeypatch):
    """pm-review round 1 found this real bug: the first version's `result.get(
    "success")` sat outside the try block, so a 2xx response whose body wasn't a
    JSON object (null, a list, a bare string) would raise AttributeError past this
    function's own documented never-raise contract."""
    mock_resp = AsyncMock()
    mock_resp.raise_for_status = lambda: None
    mock_resp.json = lambda: ["not", "a", "dict"]
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    from ddp_sync.pipelines.openstates_archive import _trigger_legbot_session_via_mac_wireguard

    with patch("httpx.AsyncClient", return_value=mock_client):
        await _trigger_legbot_session_via_mac_wireguard(
            "US", "2026", _enabled_ec2_settings()
        )  # must not raise


@pytest.mark.asyncio
async def test_wireguard_helper_request_failure_is_swallowed():
    with patch("httpx.AsyncClient", side_effect=RuntimeError("network blew up")):
        from ddp_sync.pipelines.openstates_archive import (
            _trigger_legbot_session_via_mac_wireguard,
        )

        await _trigger_legbot_session_via_mac_wireguard(
            "US", "2026", _enabled_ec2_settings()
        )  # must not raise


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
