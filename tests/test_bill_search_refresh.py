"""SYNC-87: the post-archive `ddp_bill_search` refresh (PLAN-enterprise-search.md 4.5.5).

api-v3 is mocked at the httpx layer; nothing here can reach a real instance. Covers the ticket's list:
the call (URL, params, x-api-key), `busy` retried 3 times 20 s apart, `bill_search_refresh_incomplete`
whenever the run is not drained, the yaml gate (OPEN-124 rule), and that the hook always targets the
RDS-backed api-v3 and is independent of the other post-archive hooks.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import yaml
from structlog.testing import capture_logs

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines import bill_search_refresh as bsr
from ddp_sync.pipelines.openstates_archive import (
    _bill_search_refresh_eligible,
    _maybe_refresh_bill_search,
    _run_archive_with_hook,
)

pytestmark = pytest.mark.asyncio


def _resp(body, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body
    return r


def _ok(refreshed=0, more=False, busy=False, with_text=0, orphans=0):
    return _resp({"refreshed": refreshed, "with_text": with_text, "orphans_removed": orphans,
                  "more": more, "busy": busy})


def _client(*responses):
    """Patch httpx.AsyncClient so each `post` returns/raises the next item; returns (patcher, post)."""
    post = AsyncMock(side_effect=list(responses))
    client = MagicMock()
    client.post = post
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.pipelines.bill_search_refresh.httpx.AsyncClient", return_value=cm), post


def _events(logs):
    return [e["event"] for e in logs]


async def _run(*responses, jurisdiction="fl"):
    patcher, post = _client(*responses)
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    with patcher, patch("ddp_sync.pipelines.bill_search_refresh.asyncio.sleep", new=fake_sleep), \
         capture_logs() as logs:
        totals = await bsr.refresh_bill_search("FL", api_base="http://rds:8002", api_key="k")
    return totals, post, sleeps, logs


# --- the call -----------------------------------------------------------------------------------


async def test_posts_to_the_refresh_route_with_the_api_key_and_lowercase_jurisdiction():
    totals, post, _, logs = await _run(_ok(refreshed=3, with_text=2))
    call = post.await_args
    assert call.args[0] == "http://rds:8002/ddp/search/refresh"
    assert call.kwargs["params"] == {"jurisdiction": "fl", "limit": "200"}
    assert call.kwargs["headers"] == {"x-api-key": "k"}
    assert totals["drained"] is True
    assert "bill_search_refresh_run" in _events(logs)
    assert "bill_search_refresh_incomplete" not in _events(logs)


async def test_no_api_key_sends_no_header():
    patcher, post = _client(_ok())
    with patcher:
        await bsr.refresh_bill_search("fl", api_base="http://rds")
    assert post.await_args.kwargs["headers"] == {}


async def test_loops_while_more_and_sums_the_totals():
    totals, post, _, _ = await _run(
        _ok(refreshed=200, with_text=190, more=True),
        _ok(refreshed=200, with_text=180, orphans=1, more=True),
        _ok(refreshed=20, with_text=20),
    )
    assert post.await_count == 3
    assert (totals["calls"], totals["refreshed"], totals["with_text"], totals["orphans_removed"]) == (3, 420, 390, 1)
    assert totals["drained"] is True


async def test_nothing_stale_is_one_cheap_call_and_drained():
    totals, post, _, _ = await _run(_ok())
    assert post.await_count == 1 and totals["drained"] is True and totals["refreshed"] == 0


# --- busy ---------------------------------------------------------------------------------------


async def test_busy_is_retried_20_seconds_apart_then_succeeds():
    totals, post, sleeps, _ = await _run(_ok(busy=True), _ok(busy=True), _ok(refreshed=5))
    assert post.await_count == 3
    assert sleeps == [20.0, 20.0]
    assert totals["drained"] is True and totals["refreshed"] == 5


async def test_busy_three_times_is_incomplete_and_stops():
    totals, post, sleeps, logs = await _run(_ok(busy=True), _ok(busy=True), _ok(busy=True))
    assert post.await_count == 3 and totals["calls"] == 3
    assert sleeps == [20.0, 20.0]  # no sleep after the last attempt
    assert totals["drained"] is False
    incomplete = [e for e in logs if e["event"] == "bill_search_refresh_incomplete"]
    assert len(incomplete) == 1 and "busy" in incomplete[0]["reason"]
    assert incomplete[0]["jurisdiction"] == "FL"


async def test_busy_in_a_later_call_keeps_the_earlier_progress():
    totals, _, _, logs = await _run(_ok(refreshed=200, more=True), _ok(busy=True), _ok(busy=True), _ok(busy=True))
    assert totals["refreshed"] == 200 and totals["drained"] is False
    assert "bill_search_refresh_incomplete" in _events(logs)


# --- failures never raise and are reported as incomplete -------------------------------------------


@pytest.mark.parametrize("response, fragment", [
    (_resp({}, status=500), "500"),
    (_resp({}, status=403), "403"),
])
async def test_http_errors_are_incomplete_not_raised(response, fragment):
    totals, post, _, logs = await _run(response)
    assert post.await_count == 1  # an error is not retried: the next archive run repairs it
    assert totals["drained"] is False
    reason = next(e for e in logs if e["event"] == "bill_search_refresh_incomplete")["reason"]
    assert fragment in reason


async def test_connection_error_is_incomplete_not_raised():
    totals, _, _, logs = await _run(httpx.ConnectError("refused"))
    assert totals["drained"] is False
    assert "unreachable" in next(e for e in logs if e["event"] == "bill_search_refresh_incomplete")["reason"]


async def test_non_json_and_non_object_bodies_are_incomplete():
    bad = MagicMock()
    bad.status_code = 200
    bad.json.side_effect = ValueError("no json")
    totals, _, _, _ = await _run(bad)
    assert totals["drained"] is False
    totals, _, _, _ = await _run(_resp(["not", "an", "object"]))
    assert totals["drained"] is False


async def test_more_with_no_progress_stops_instead_of_looping_forever():
    totals, post, _, logs = await _run(_ok(refreshed=0, more=True))
    assert post.await_count == 1
    assert totals["drained"] is False
    assert "no progress" in next(e for e in logs if e["event"] == "bill_search_refresh_incomplete")["reason"]


async def test_non_numeric_counters_are_incomplete_not_raised():
    totals, _, _, logs = await _run(_resp({"refreshed": "lots", "more": False, "busy": False}))
    assert totals["drained"] is False
    assert "non-numeric" in next(e for e in logs if e["event"] == "bill_search_refresh_incomplete")["reason"]


async def test_busy_attempts_count_toward_the_ceiling():
    """busy, busy, ok(more) repeated: 3 POSTs per round; the ceiling is strict, so 5 means exactly 5."""
    rounds = [_ok(busy=True), _ok(busy=True), _ok(refreshed=1, more=True)] * 3
    with patch.object(bsr, "MAX_CALLS_PER_RUN", 5):
        totals, post, _, logs = await _run(*rounds)
    assert post.await_count == 5 and totals["calls"] == 5
    assert totals["drained"] is False
    assert "stopped after 5 calls" in next(e for e in logs if e["event"] == "bill_search_refresh_incomplete")["reason"]


async def test_call_ceiling_stops_a_runaway_loop():
    with patch.object(bsr, "MAX_CALLS_PER_RUN", 3):
        totals, post, _, logs = await _run(*[_ok(refreshed=1, more=True) for _ in range(5)])
    assert post.await_count == 3 and totals["drained"] is False
    assert "stopped after 3 calls" in next(e for e in logs if e["event"] == "bill_search_refresh_incomplete")["reason"]


# --- hook gate and wiring -------------------------------------------------------------------------


async def test_eligibility_is_yaml_policy_default_off():
    assert not _bill_search_refresh_eligible("fl", None)
    assert not _bill_search_refresh_eligible("fl", {"bill_search_refresh": {"enabled": False, "jurisdictions": ["fl"]}})
    cfg = {"bill_search_refresh": {"enabled": True, "jurisdictions": ["FL", "us"]}}
    assert _bill_search_refresh_eligible("fl", cfg) and _bill_search_refresh_eligible("US", cfg)
    assert not _bill_search_refresh_eligible("va", cfg)


_CFG = {"bill_search_refresh": {"enabled": True, "jurisdictions": ["fl"]}}


async def test_hook_always_uses_the_rds_api_even_where_a_local_api_exists():
    settings = SyncSettings(
        local_openstates_api_base="http://local", local_openstates_api_key="lk",
        rds_openstates_api_base="http://rds", rds_openstates_api_key="rk", cams_api_token="t",
    )
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.bill_search_refresh.refresh_bill_search", new=AsyncMock()) as run:
        await _maybe_refresh_bill_search("fl", _CFG)
    run.assert_awaited_once()
    assert run.await_args.args == ("fl",)
    assert (run.await_args.kwargs["api_base"], run.await_args.kwargs["api_key"]) == ("http://rds", "rk")


@pytest.mark.parametrize("base, key", [("", "rk"), ("http://rds", "")])
async def test_hook_warns_and_skips_when_the_rds_base_or_key_is_not_configured(base, key):
    settings = SyncSettings(local_openstates_api_base="http://local",
                            rds_openstates_api_base=base, rds_openstates_api_key=key)
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.bill_search_refresh.refresh_bill_search", new=AsyncMock()) as run, \
         capture_logs() as logs:
        await _maybe_refresh_bill_search("fl", _CFG)
    run.assert_not_awaited()
    assert "bill_search_refresh_read_path_not_configured" in _events(logs)


async def test_hook_does_nothing_when_not_enrolled():
    settings = SyncSettings(rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.bill_search_refresh.refresh_bill_search", new=AsyncMock()) as run:
        await _maybe_refresh_bill_search("fl", None)
        await _maybe_refresh_bill_search("va", _CFG)
    run.assert_not_awaited()


async def test_archive_wrapper_runs_the_refresh_independently_and_never_changes_the_result():
    ok = {"success": True, "jurisdiction": "fl"}
    with patch("ddp_sync.pipelines.openstates_archive._acquire_archive_debounce", new=AsyncMock(return_value=True)), \
         patch("ddp_sync.pipelines.openstates_archive._run_archive", new=AsyncMock(return_value=ok)), \
         patch("ddp_sync.pipelines.openstates_archive._get_root", return_value="/x"), \
         patch("ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive",
               new=AsyncMock(side_effect=RuntimeError("legbot boom"))), \
         patch("ddp_sync.pipelines.openstates_archive._maybe_refresh_bill_search",
               new=AsyncMock(side_effect=RuntimeError("refresh boom"))) as refresh, \
         patch("ddp_sync.pipelines.openstates_archive._maybe_embed_knowledge_base", new=AsyncMock()) as kb_hook:
        result = await _run_archive_with_hook("fl", config=_CFG)
    assert result == ok
    refresh.assert_awaited_once()
    assert refresh.await_args.args == ("fl", _CFG)
    kb_hook.assert_awaited_once()  # a failing refresh does not stop the embedding hook


async def test_failed_archive_does_not_refresh():
    bad = {"success": False, "jurisdiction": "fl"}
    with patch("ddp_sync.pipelines.openstates_archive._acquire_archive_debounce", new=AsyncMock(return_value=True)), \
         patch("ddp_sync.pipelines.openstates_archive._run_archive", new=AsyncMock(return_value=bad)), \
         patch("ddp_sync.pipelines.openstates_archive._get_root", return_value="/x"), \
         patch("ddp_sync.pipelines.openstates_archive._maybe_refresh_bill_search", new=AsyncMock()) as refresh:
        await _run_archive_with_hook("fl", config=_CFG)
    refresh.assert_not_awaited()


async def test_checked_in_yaml_block_is_on_and_enrolls_the_eight_search_jurisdictions():
    cfg = yaml.safe_load((Path(__file__).parent.parent / "config" / "sync_schedule.yaml").read_text())
    block = cfg["openstates_archive"]["bill_search_refresh"]
    # On since 2026-10-05; inert on a host without RDS_OPENSTATES_API_BASE and its key (test_..._read_path_not_configured).
    assert block["enabled"] is True
    assert sorted(block["jurisdictions"]) == sorted(["us", "fl", "mi", "az", "va", "wa", "ut", "nc"])
