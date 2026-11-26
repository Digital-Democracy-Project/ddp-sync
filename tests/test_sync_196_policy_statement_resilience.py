"""SYNC-196: the policy_statement step survives and reports an outage.

On 2026-11-25 two outages made policy_statement runs do nothing while ending "completed": the dev broker's web container was
down for about 25 minutes, and TypeSafe's Jev answered 400 "Unknown model: jev-latest" for about 12 minutes. These tests pin
what now happens: the Jev client retries a blip, a run stops starting bills after 3 failures in a row from one dependency and
reports itself partial or failed with counts, one alert goes out per stopped run, and a clean run is untouched.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from ddp_sync import status_report
from ddp_sync.pipelines import scraper_triggered_legbot as stl
from ddp_sync.pipelines import session_pipeline_runner as spr
from ddp_sync.pipelines.session_pipeline_runner import run_legbot_pipeline
from ddp_sync.services import jev_client
from ddp_sync.services.broker_client import BrokerClientError
from ddp_sync.services.jev_client import JevError, JevScorer
from ddp_sync.services.legbot_client import LegBotDispatchError

# --------------------------------------------------------------------------------------------------------------------
# The Jev client
# --------------------------------------------------------------------------------------------------------------------

_REAL_ASYNC_CLIENT = httpx.AsyncClient
_OK = {"answers": {"place": {"probabilities": {"o0": 0.9, "none": 0.1}}}, "usage": {"input_tokens": 100}}
_UNKNOWN_MODEL = {"detail": {"error_type": "api_usage_error", "message": "Unknown model: jev-latest"}}


def _jev(monkeypatch, replies):
    """A scorer whose HTTP answers come from `replies` in order (a Response or an exception); returns (scorer, calls, sleeps)."""
    calls, sleeps = [], []

    def handler(request):
        calls.append(request)
        reply = replies[len(calls) - 1]
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(jev_client.httpx, "AsyncClient",
                        lambda *a, **k: _REAL_ASYNC_CLIENT(*a, **{**k, "transport": httpx.MockTransport(handler)}))

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(jev_client.asyncio, "sleep", sleep)
    return JevScorer(api_key="k", model_id="jev-latest"), calls, sleeps


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [
    httpx.Response(503, text="busy"),
    httpx.ConnectError("refused"),
    httpx.Response(400, json=_UNKNOWN_MODEL),
], ids=["a 503", "a connection error", "the Unknown model 400"])
async def test_a_transient_failure_is_retried_and_the_next_answer_is_used(monkeypatch, failure):
    jev, calls, sleeps = _jev(monkeypatch, [failure, httpx.Response(200, json=_OK)])
    async with jev:
        assert await jev.score("text", ["a"]) == [0.9]
    assert len(calls) == 2 and sleeps == [2.0] and jev.input_tokens == 100


@pytest.mark.asyncio
@pytest.mark.parametrize("failure, message", [
    (httpx.Response(503, text="busy"), "Jev answered 503"),
    (httpx.ConnectError("refused"), "Jev unreachable"),
    (httpx.Response(400, json=_UNKNOWN_MODEL), "Unknown model"),
], ids=["a 503", "a connection error", "the Unknown model 400"])
async def test_three_retries_then_a_jev_error(monkeypatch, failure, message):
    jev, calls, sleeps = _jev(monkeypatch, [failure] * 4)
    async with jev:
        with pytest.raises(JevError, match=message):
            await jev.score("text", ["a"])
    assert len(calls) == 4 and sleeps == [2.0, 8.0, 30.0]


@pytest.mark.asyncio
@pytest.mark.parametrize("reply", [
    httpx.Response(401, json={"detail": "bad key"}),
    httpx.Response(400, json={"detail": {"message": "Too many options"}}),
], ids=["a 401", "any other 400"])
async def test_other_client_errors_are_not_retried(monkeypatch, reply):
    jev, calls, sleeps = _jev(monkeypatch, [reply, httpx.Response(200, json=_OK)])
    async with jev:
        with pytest.raises(JevError):
            await jev.score("text", ["a"])
    assert len(calls) == 1 and sleeps == []


@pytest.mark.asyncio
async def test_a_429_still_waits_as_told(monkeypatch):
    jev, _calls, sleeps = _jev(monkeypatch, [httpx.Response(429, headers={"retry-after": "5"}), httpx.Response(200, json=_OK)])
    async with jev:
        assert await jev.score("text", ["a"]) == [0.9]
    assert sleeps == [5.0]


# --------------------------------------------------------------------------------------------------------------------
# The run: stop early, status, alert
# --------------------------------------------------------------------------------------------------------------------

_OK_STEP = {"statements": 2, "matched": 1, "created": 1, "excluded": 0, "skipped_reason": None}


def _cands(n):
    return [{"gov_id": f"HB {i}", "bill_openstates_id": f"ocd-bill/{i}", "live_url_fallback": "u"} for i in range(1, n + 1)]


@pytest.fixture
def run_policy(monkeypatch, legbot_stand_in_manifest):
    """run(replies, n=bills, **kw): bills go one at a time; `replies` are the policy step's answers in order (a dict, or an
    exception to raise). Returns (summary, step mock, alert mock)."""
    spr._legbot_line = None
    step = AsyncMock()
    alerts = []
    monkeypatch.setattr(spr, "dispatch_and_store_policy_statements", step)
    monkeypatch.setattr(spr, "post_alert", lambda text, **kw: alerts.append((text, kw)) or True)
    monkeypatch.setattr(spr, "get_archived_bill_text", AsyncMock(return_value=None))
    monkeypatch.setattr(spr, "_polled_bill_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(spr, "get_bill_artifacts", AsyncMock(return_value=None))
    monkeypatch.setattr(spr, "list_current_session_bill_candidates", AsyncMock(side_effect=AssertionError("no scan")))
    state = {"stop_after": 3, "concurrency": 1}
    monkeypatch.setattr(spr, "get_settings", lambda: SimpleNamespace(
        session_pipeline_concurrency=state["concurrency"], replica_freshness_check_enabled=False,
        policy_statement_stop_after_failures=state["stop_after"]))

    async def run(replies, n=None, stop_after=3, concurrency=1, **kw):
        """`replies` is a list of answers in order, or an async function (then `n` bills is required)."""
        state["stop_after"], state["concurrency"] = stop_after, concurrency
        spr._legbot_line = None  # the line is process-wide: rebuilt so it takes this run's concurrency
        step.side_effect = replies if callable(replies) else list(replies)
        summary = await run_legbot_pipeline("fl", "2026F", [], False, 10, include_concept_statements=False, retry_failed=False,
                                            bill_candidates=_cands(n or len(replies)), include_policy_statement=True, **kw)
        return summary

    return run, step, alerts


JEV, BROKER, LEGBOT = JevError("Jev unreachable"), BrokerClientError("broker down"), LegBotDispatchError("writer failed")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure, source", [(JEV, "jev"), (BROKER, "broker"), (LEGBOT, "legbot"), (ValueError("bug"), "other")])
async def test_three_failures_in_a_row_from_one_dependency_stop_the_run(run_policy, failure, source):
    run, step, alerts = run_policy
    summary = await run([failure] * 3 + [_OK_STEP] * 3, n=6)
    assert step.await_count == 3  # bills 4 to 6 were never started
    assert (summary["status"], summary["bills_failed"], summary["bills_not_started"], summary["bills_processed"]) == ("failed", 3, 3, 3)
    assert summary["stopped_early"] == f"{source} failed 3 bills in a row"
    assert source in summary["first_failure"]
    assert [r.get("not_started") for r in summary["results"]] == [None, None, None, True, True, True]
    assert len(alerts) == 1


@pytest.mark.asyncio
async def test_with_two_bills_in_flight_the_ones_already_running_finish_and_the_queued_ones_never_start(run_policy):
    """Concurrency 2, six bills. Bills 1 and 2 fail, bills 3 and 4 start together; bill 3 fails (the third in a row: the run
    trips) while bill 4 is still running. Bill 4 finishes normally; bills 5 and 6 were queued and must never start."""
    import asyncio

    run, _step, alerts = run_policy
    started = []

    async def the_step(*args, **kwargs):
        n = len(started) + 1
        started.append(n)
        await asyncio.sleep(0)
        if n == 4:  # still running when bill 3 trips the run
            for _ in range(5):
                await asyncio.sleep(0)
            return _OK_STEP
        raise JEV

    summary = await run(the_step, n=6, concurrency=2)
    assert started == [1, 2, 3, 4]  # bills 5 and 6 never reached the step
    assert [r.get("not_started") for r in summary["results"]] == [None, None, None, None, True, True]
    assert (summary["bills_failed"], summary["bills_not_started"], summary["bills_processed"]) == (3, 2, 4)
    assert summary["status"] == "partial"  # bill 4 got through, so the stopped run is partial, not failed
    assert summary["stopped_early"] == "jev failed 3 bills in a row"
    assert len(alerts) == 1


@pytest.mark.asyncio
async def test_the_broker_failing_the_coverage_check_counts_as_a_broker_failure(run_policy, monkeypatch):
    run, step, alerts = run_policy
    monkeypatch.setattr(spr, "get_bill_artifacts", AsyncMock(side_effect=BrokerClientError("unreachable")))
    summary = await run([_OK_STEP] * 5, n=5)
    step.assert_not_awaited()  # the policy step is never reached when the coverage read fails
    assert summary["stopped_early"] == "broker failed 3 bills in a row" and summary["bills_not_started"] == 2
    assert summary["status"] == "failed" and len(alerts) == 1


@pytest.mark.asyncio
async def test_bills_that_finished_before_the_stop_keep_the_run_partial_not_failed(run_policy):
    run, step, _alerts = run_policy
    summary = await run([_OK_STEP, JEV, JEV, JEV, _OK_STEP], n=5)
    assert summary["status"] == "partial" and summary["bills_failed"] == 3 and summary["bills_not_started"] == 1
    assert step.await_count == 4


@pytest.mark.asyncio
async def test_failures_that_are_not_in_a_row_or_not_from_one_dependency_do_not_stop_the_run(run_policy):
    run, step, alerts = run_policy
    summary = await run([JEV, JEV, _OK_STEP, JEV, JEV, BROKER, JEV, BROKER], n=8)
    assert step.await_count == 8 and summary["bills_not_started"] == 0 and summary["stopped_early"] is None
    assert summary["status"] == "partial" and summary["bills_failed"] == 7  # reported, but nothing was cut short
    assert alerts == []


@pytest.mark.asyncio
async def test_a_stop_after_of_zero_turns_the_stop_off(run_policy):
    run, step, alerts = run_policy
    summary = await run([JEV] * 5, n=5, stop_after=0)
    assert step.await_count == 5 and summary["stopped_early"] is None and summary["status"] == "partial"
    assert alerts == []


@pytest.mark.asyncio
async def test_a_clean_run_is_completed_with_zero_failures_and_sends_no_alert(run_policy):
    run, _step, alerts = run_policy
    summary = await run([_OK_STEP] * 3)
    assert (summary["status"], summary["bills_failed"], summary["bills_not_started"], summary["stopped_early"]) == (
        "completed", 0, 0, None)
    assert alerts == []


@pytest.mark.asyncio
async def test_the_alert_goes_out_once_with_a_state_and_session_dedupe_key(run_policy):
    run, _step, alerts = run_policy
    await run([JEV] * 3 + [_OK_STEP] * 4, n=7)
    [(text, kw)] = alerts
    assert kw["source"] == "policy_statement.run_stopped"
    assert kw["dedupe_key"] == "policy_statement_run_stopped:FL:2026F"
    assert "FL 2026F" in text and "jev failed 3 bills in a row" in text and "re-run" in text.lower()


@pytest.mark.asyncio
async def test_a_failed_alert_never_fails_the_run(run_policy, monkeypatch):
    run, _step, _alerts = run_policy

    def boom(*a, **k):
        raise RuntimeError("slack down")

    monkeypatch.setattr(spr, "post_alert", boom)
    summary = await run([JEV] * 3 + [_OK_STEP], n=4)
    assert summary["status"] == "failed" and summary["bills_not_started"] == 1


@pytest.mark.asyncio
async def test_a_run_without_the_policy_step_keeps_its_summary_unchanged(run_policy, monkeypatch):
    """The post-archive hook never sends the flag: its summary has no `status` key and nothing is counted or stopped."""
    _, step, alerts = run_policy
    monkeypatch.setattr(spr, "generate_and_store_bill_artifact", AsyncMock(return_value={"id": 1, "status": "complete"}))
    monkeypatch.setattr(spr, "get_current_version_identity", AsyncMock(return_value={
        "version_date": "2026-06-01", "version_note": "c1", "bill_title": "t", "chamber_classification": "lower",
        "jurisdiction_classification": "state"}))
    summary = await run_legbot_pipeline("fl", "2026F", ["bill_summary"], False, 10, include_concept_statements=False,
                                        retry_failed=False, bill_candidates=_cands(2))
    assert "status" not in summary and "bills_failed" not in summary
    step.assert_not_awaited() and alerts == []


def test_the_breaker_trips_once_even_if_bills_already_in_flight_keep_failing():
    """With more than one bill in flight, bills that were already running finish after the run has tripped and are recorded
    too: that must not trip it (and so alert) a second time, nor change the reason."""
    breaker = spr._RunBreaker(3)
    failing = {"dependency_failure": "jev", "policy_statement_skipped_reason": "dispatch_failed: x"}
    tripped = [breaker.record(dict(failing)) for _ in range(5)]
    assert tripped == [False, False, True, False, False]
    assert breaker.reason == "jev failed 3 bills in a row" and breaker.failed == 5


def test_a_bill_that_was_never_started_is_not_a_failure_and_does_not_break_a_streak():
    breaker = spr._RunBreaker(3)
    jev = {"dependency_failure": "jev"}
    assert [breaker.record(dict(jev)), breaker.record(dict(jev))] == [False, False]
    assert breaker.record({"not_started": True, "dependency_failure": None}) is False
    assert breaker.record(dict(jev)) is True  # the streak was still 2, so this is the third
    assert breaker.failed == 3


def test_bill_outcome_counts_a_failed_policy_step_as_failed_and_written_statements_as_generated():
    assert spr._bill_outcome({"policy_statement_failed": True}) == "failed"
    assert spr._bill_outcome({"policy_statement_statements": 3}) == "generated"
    assert spr._bill_outcome({}) == "skipped"


# --------------------------------------------------------------------------------------------------------------------
# The run record, the poller and `ddp-sync status`
# --------------------------------------------------------------------------------------------------------------------


class _FakeStore:
    def __init__(self):
        self.saved = {}

    async def set_legbot_session_run(self, run_id, record):
        self.saved[run_id] = dict(record)


async def _record_for(monkeypatch, result):
    from ddp_sync.services import redis_store

    store = _FakeStore()
    monkeypatch.setattr(redis_store, "get_redis_store", lambda: store)
    monkeypatch.setattr(stl, "trigger_scraper_session_pipeline", AsyncMock(return_value=result))
    await stl.run_session_pipeline_in_background("run1", "FL", "2026F")
    return store.saved["run1"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status, expected_error", [
    ("completed", None),
    ("partial", "jev failed 3 bills in a row"),
    ("failed", "jev failed 3 bills in a row"),
])
async def test_the_run_record_carries_the_runs_own_status(monkeypatch, status, expected_error):
    record = await _record_for(monkeypatch, {"success": True, "status": status, "stopped_early": expected_error,
                                             "first_failure": "jev: x", "bills_failed": 3, "bills_not_started": 2, "results": []})
    assert record["status"] == status and record["error"] == expected_error
    # the count, the not-started count and the first reason are all in the record the status route returns
    assert (record["result"]["bills_failed"], record["result"]["bills_not_started"], record["result"]["first_failure"]) == (3, 2, "jev: x")


@pytest.mark.asyncio
async def test_a_run_with_no_status_key_is_completed_or_failed_as_before(monkeypatch):
    assert (await _record_for(monkeypatch, {"success": True, "results": []}))["status"] == "completed"
    failed = await _record_for(monkeypatch, {"success": False, "error": "already_running"})
    assert (failed["status"], failed["error"]) == ("failed", "already_running")


def test_a_finished_partial_record_is_not_reported_lost_later():
    record = {"status": "partial", "updated_at": "2026-01-01T00:00:00+00:00"}
    assert stl.session_run_as_read(record)["status"] == "partial"
    assert "partial" in stl.SESSION_RUN_FINAL


@pytest.mark.asyncio
async def test_the_archive_hooks_poller_stops_waiting_on_a_partial_run(monkeypatch):
    from ddp_sync.pipelines import openstates_archive as oa

    class Client:
        async def get(self, url, headers=None, timeout=None):  # the real poller passes timeout=30.0
            return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                                   json=lambda: {"run_id": "r", "status": "partial", "updated_at": "now"})

    monkeypatch.setattr(oa.asyncio, "sleep", AsyncMock())  # the real poll waits a minute first
    record = await oa._wait_for_mac_run(Client(), "http://mac/x", {}, "r")
    assert record["status"] == "partial"


def test_ddp_sync_status_shows_failed_bills_and_a_partial_outcome():
    entry = {"run_id": "r", "kind": "LegBot session pipeline", "outcome": "partial", "jurisdiction": "FL", "session": "2026F",
             "started_at": "2026-11-25T23:00:00+00:00", "finished_at": "2026-11-25T23:05:00+00:00", "done": 6, "total": 6,
             "failed": 3, "error": "jev failed 3 bills in a row", "started_by": "trigger", "current": []}
    text = status_report.format_text({"running": [], "locks": [], "recent": [entry],
                                      "scheduled": {"status": "running", "jobs": []}})
    line = next(row for row in text.splitlines() if "LegBot session pipeline" in row and "partial" in row)
    assert "bills 6/6, 3 failed" in line and "jev failed 3 bills in a row" in line


# ── the setting fails safe ─────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("raw", "expected"), [
    (None, 3), ("", 3), ("   ", 3), ("abc", 3), ("2.5", 3), ("-2", 3), ("-1", 3),  # a typo or a negative never turns it off
    ("0", 0),                                                                      # only an explicit 0 does
    ("1", 1), ("3", 3), (" 5 ", 5),
])
def test_the_stop_after_failures_setting_falls_back_to_3_and_only_an_explicit_0_turns_it_off(monkeypatch, raw, expected):
    from ddp_sync import config

    if raw is None:
        monkeypatch.delenv("POLICY_STATEMENT_STOP_AFTER_FAILURES", raising=False)
    else:
        monkeypatch.setenv("POLICY_STATEMENT_STOP_AFTER_FAILURES", raw)
    assert config._load_from_env()["policy_statement_stop_after_failures"] == expected  # and loading never raises
