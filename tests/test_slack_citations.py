"""SYNC-127: the nightly Slack #legislation citation pull.

Slack, api-v3, Redis and the verification run are all patched: these cover the parsing, what is sent,
what is recorded as handled, and the gates (report-only, one host, a cap, an unreadable record).
"""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.config import _load_from_env
from ddp_sync.pipelines.slack_citations import (
    _window,
    parse_thread,
    run_slack_citations_job,
)
from ddp_sync.scheduler import UpdateScheduler
from ddp_sync.services.local_openstates_client import find_bill

_P = "ddp_sync.pipelines.slack_citations"

ROOT = {
    "ts": "1760000000.000100",
    "text": "Ramon just submitted `Florida` bill `HB 7`: <https://openstates.org/fl/bills/2026/HB7/|Open States>",
}


def _reply(ts, org="Sierra Club", verb="supports", url="https://a.invalid/x"):
    return {
        "ts": ts,
        "text": f"I will update the records to show that `{org}` {verb} this legislation.  "
                f"Here is my citation: <{url}>",
    }


# ── parsing ────────────────────────────────────────────────────────────────────────────────────


def test_a_citation_reply_takes_the_bill_from_the_threads_open_states_link():
    bill, citations = parse_thread([ROOT, _reply("1760000100.0", org="Smith &amp; Co", verb="opposes")])

    assert bill == {"jurisdiction": "FL", "session": "2026", "number": "HB7"}
    assert citations == [{"ts": "1760000100.0", "org_name": "Smith & Co", "position": "oppose",
                          "citation_url": "https://a.invalid/x"}]


def test_a_thread_without_an_open_states_link_has_no_bill_even_if_the_root_names_one():
    root = {"ts": "1.0", "text": "just submitted `Florida` bill `HB 7`"}
    bill, citations = parse_thread([root, _reply("2.0")])

    assert bill is None
    assert len(citations) == 1


def test_the_same_citation_twice_in_a_thread_is_kept_once_and_other_messages_are_ignored():
    _, citations = parse_thread([ROOT, _reply("2.0"), {"ts": "3.0", "text": "thanks!"}, _reply("4.0")])

    assert [c["ts"] for c in citations] == ["2.0"]


def test_the_window_never_reaches_back_before_the_start_date():
    now = 1_800_000_000.0
    since, roots_since = _window({"lookback_days": 3, "start_date": "2030-01-10", "thread_lookback_days": 14}, now)
    assert since == pytest.approx(1_894_233_600.0)  # 2030-01-10 UTC, later than now - 3 days
    assert roots_since == pytest.approx(since - 14 * 86400)


# ── the job ────────────────────────────────────────────────────────────────────────────────────

_BILL = {"bill_openstates_id": "uuid-1", "jurisdiction": "FL", "session_code": "2026", "gov_id": "HB 7"}


def _store(done=frozenset(), readable=True):
    store = MagicMock()
    store.get_processed_slack_citations = AsyncMock(return_value=set(done) if readable else None)
    store.mark_slack_citations_processed = AsyncMock(return_value=True)
    store.set_verify_run = AsyncMock()
    return store


def _echo(outcomes):
    """A verification run that answers each item in order, as verify_org_citations does."""
    fields = ("bill_openstates_id", "org_name", "position", "citation_url")

    async def run(items, **_kwargs):
        results = [{**{f: item[f] for f in fields}, "outcome": outcome} for item, outcome in zip(items, outcomes)]
        return {"counts": {}, "results": results}

    return run


async def _job(threads, *, mode="report_only", store=None, bill=(_BILL, True), outcomes=(), verify=None,
               cap=200, token="xoxb-test"):
    store = store or _store()
    config = {"mode": mode, "max_citations_per_run": cap, "lookback_days": 100000, "thread_lookback_days": 0}
    verify = verify or AsyncMock(side_effect=_echo(outcomes))
    with patch(f"{_P}.fetch_threads", new=AsyncMock(return_value=threads)), \
            patch(f"{_P}.find_bill", new=AsyncMock(return_value=bill)) as lookup, \
            patch(f"{_P}.get_redis_store", return_value=store), \
            patch(f"{_P}.run_verify_org_citations_in_background", new=verify), \
            patch(f"{_P}.post_alert") as alert, \
            patch.dict("os.environ", {"SLACK_BOT_TOKEN": token}):
        summary = await run_slack_citations_job(config, trigger="test")
    return summary, store, verify, lookup, alert


@pytest.mark.asyncio
async def test_report_only_lists_what_it_would_send_and_writes_and_records_nothing():
    summary, store, verify, lookup, _ = await _job([[ROOT, _reply("1760000100.0")]])

    assert summary["status"] == "reported"
    assert summary["ready"] == 1
    lookup.assert_awaited_once_with("FL", "2026", "HB7")
    verify.assert_not_awaited()
    store.mark_slack_citations_processed.assert_not_awaited()


@pytest.mark.asyncio
async def test_write_mode_sends_the_items_and_records_only_settled_messages():
    threads = [
        [ROOT, _reply("1760000100.0", org="A"), _reply("1760000200.0", org="B")],
        [{"ts": "1760000300.0", "text": "no link here"}, _reply("1760000400.0", org="C")],
    ]
    summary, store, verify, _, _ = await _job(threads, mode="write", outcomes=["written", "retryable"])

    items = verify.await_args.args[0]
    assert [i["org_name"] for i in items] == ["A", "B"]
    assert items[0] == {**_BILL, "org_name": "A", "position": "support", "citation_url": "https://a.invalid/x"}
    assert verify.await_args.kwargs["source"] == "slack-legislation"
    # A was written; B was rate limited (tried again next run); C's thread has no link (settled, reported).
    assert sorted(store.mark_slack_citations_processed.await_args.args[0]) == ["1760000100.0", "1760000400.0"]
    assert summary["unmatched"] == {"no_openstates_link": 1}
    assert summary["status"] == "finished"


@pytest.mark.asyncio
async def test_a_message_already_handled_is_not_sent_again():
    summary, _, verify, _, _ = await _job(
        [[ROOT, _reply("1760000100.0")]], mode="write", store=_store(done={"1760000100.0"}),
    )

    assert summary["already_handled"] == 1
    assert verify.await_args.args[0] == []


@pytest.mark.asyncio
async def test_a_bill_lookup_that_could_not_answer_is_left_for_the_next_run():
    summary, store, _, _, _ = await _job(
        [[ROOT, _reply("1760000100.0")]], mode="write", bill=(None, False),
    )

    assert summary["unmatched"] == {"bill_lookup_failed": 1}
    store.mark_slack_citations_processed.assert_awaited_once_with([])


@pytest.mark.asyncio
async def test_the_cap_limits_what_one_run_sends():
    # Slack lists newest first; the cap must take the oldest, so the newest wait for the next run.
    replies = [_reply(f"17600001{i:02d}.0", org=f"Org {i}") for i in reversed(range(5))]
    summary, _, verify, _, _ = await _job([[ROOT, *replies]], mode="write", cap=2, outcomes=["written"] * 2)

    assert [i["org_name"] for i in verify.await_args.args[0]] == ["Org 0", "Org 1"]
    assert summary["left_for_next_run"] == 3


@pytest.mark.asyncio
async def test_results_that_do_not_line_up_with_the_items_mark_nothing_as_handled():
    async def short_run(items, **_kwargs):
        return {"counts": {"written": 1}, "results": [{"outcome": "written"}]}  # one result, two items

    summary, store, _, _, _ = await _job(
        [[ROOT, _reply("1760000100.0", org="A"), _reply("1760000200.0", org="B")]], mode="write",
        verify=AsyncMock(side_effect=short_run),
    )

    store.mark_slack_citations_processed.assert_awaited_once_with([])
    assert summary["status"] == "failed"


@pytest.mark.asyncio
async def test_an_unreadable_handled_record_stops_the_run_before_anything_is_sent():
    summary, _, verify, _, alert = await _job([[ROOT, _reply("1760000100.0")]], mode="write",
                                              store=_store(readable=False))

    assert summary["status"] == "failed"
    verify.assert_not_awaited()
    alert.assert_called_once()


@pytest.mark.asyncio
async def test_no_slack_token_fails_the_run_and_alerts():
    summary, _, verify, _, alert = await _job([[ROOT, _reply("1760000100.0")]], token="")

    assert summary["status"] == "failed"
    assert "SLACK_BOT_TOKEN" in summary["error"]
    verify.assert_not_awaited()
    alert.assert_called_once()


# ── the bill lookup ────────────────────────────────────────────────────────────────────────────


def _client(status, body=None):
    resp = MagicMock(status_code=status)
    resp.json.return_value = body
    client = MagicMock()
    client.get = AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.local_openstates_client.httpx.AsyncClient", return_value=cm), client


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (200, {"id": "ocd-bill/abc", "identifier": "HB 7"},
         ({"bill_openstates_id": "abc", "jurisdiction": "FL", "session_code": "2026", "gov_id": "HB 7"}, True)),
        (404, None, (None, True)),
        (500, None, (None, False)),
    ],
)
@pytest.mark.asyncio
async def test_find_bill_tells_not_found_apart_from_could_not_answer(status, body, expected):
    settings = MagicMock(local_openstates_api_base="http://api.invalid", local_openstates_api_key="k")
    client_patch, client = _client(status, body)
    with client_patch, patch("ddp_sync.services.local_openstates_client.get_settings", return_value=settings):
        assert await find_bill("FL", "2026", "HB7") == expected
    assert client.get.await_args.args[0] == "http://api.invalid/bills/fl/2026/HB7"
    assert client.get.await_args.kwargs["headers"] == {"x-api-key": "k"}


# ── one host: the per-host flag, the scheduler and the trigger ────────────────────────────────


def test_the_host_flag_is_off_unless_the_environment_says_true(monkeypatch):
    monkeypatch.delenv("SLACK_CITATIONS_ENABLED", raising=False)
    assert _load_from_env()["slack_citations_enabled"] is False
    monkeypatch.setenv("SLACK_CITATIONS_ENABLED", "true")
    assert _load_from_env()["slack_citations_enabled"] is True


def _scheduler(enabled_on_host: bool, yaml_enabled: bool) -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(f"""
            bill_sync:
              sync_time_utc: "04:00"
            slack_citations:
              enabled: {str(yaml_enabled).lower()}
              sync_time_utc: "09:00"
            """))
        path = Path(f.name)
    settings = MagicMock(sync_interval_minutes=30, slack_citations_enabled=enabled_on_host)
    return UpdateScheduler(settings=settings, config_path=path)


@pytest.mark.parametrize(("on_host", "in_yaml", "registered"),
                         [(True, True, True), (False, True, False), (True, False, False)])
@pytest.mark.asyncio
async def test_the_job_is_registered_only_when_the_host_and_the_yaml_both_say_so(on_host, in_yaml, registered):
    sched = _scheduler(on_host, in_yaml)
    sched.start()
    try:
        assert ("slack_citations" in {j.id for j in sched.scheduler.get_jobs()}) is registered
    finally:
        sched.stop()


def _post(enabled_on_host: bool):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    scheduler = MagicMock(_sync_config={"slack_citations": {"mode": "report_only"}})
    with patch("ddp_sync.api.routes.triggers.get_settings",
               return_value=MagicMock(slack_citations_enabled=enabled_on_host)), \
            patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch(f"{_P}.run_slack_citations_job", new=AsyncMock()) as job:
        resp = TestClient(app).post("/trigger/slack-citations")
    return resp, job


def test_the_manual_trigger_runs_one_pass_with_the_configured_mode():
    resp, job = _post(True)
    assert resp.status_code == 202
    assert resp.json() == {"status": "started", "mode": "report_only"}
    job.assert_awaited_once_with({"mode": "report_only"}, trigger="manual")


def test_the_manual_trigger_is_refused_on_a_host_that_does_not_run_the_pull():
    resp, job = _post(False)
    assert resp.status_code == 409
    job.assert_not_awaited()


# ── reading Slack ──────────────────────────────────────────────────────────────────────────────


def _slack_responses(*payloads):
    responses = []
    for status, body in payloads:
        resp = MagicMock(status_code=status, headers={"Retry-After": "0"})
        resp.json.return_value = body
        responses.append(resp)
    client = MagicMock()
    client.get = AsyncMock(side_effect=responses)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch(f"{_P}.httpx.AsyncClient", return_value=cm), client


@pytest.mark.asyncio
async def test_threads_are_read_with_their_replies_across_pages_and_a_rate_limit():
    from ddp_sync.pipelines.slack_citations import fetch_threads

    client_patch, client = _slack_responses(
        (429, None),  # history, rate limited once
        (200, {"ok": True, "messages": [{"ts": "1.0", "reply_count": 2, "latest_reply": "900.0"}, {"ts": "2.0"}],
               "response_metadata": {"next_cursor": "c1"}}),
        (200, {"ok": True, "messages": [{"ts": "3.0", "reply_count": 1, "latest_reply": "950.0"},
                                        {"ts": "4.0", "reply_count": 3, "latest_reply": "10.0"}]}),
        (200, {"ok": True, "messages": [ROOT, _reply("900.0")]}),           # replies of 1.0
        (200, {"ok": True, "messages": [{"ts": "3.0", "text": "root"}]}),  # replies of 3.0
    )
    with client_patch, patch(f"{_P}.asyncio.sleep", new=AsyncMock()):
        threads = await fetch_threads("xoxb-test", "C1", roots_since=0.0, active_since=500.0)

    # 2.0 has no replies; 4.0 is an old thread with no reply since 500, so its replies are not read.
    # 1.0 was started long before 500 but has a new reply: a late citation is still found.
    assert [len(t) for t in threads] == [2, 1]
    methods = [call.args[0].rsplit("/", 1)[1] for call in client.get.await_args_list]
    assert methods == ["conversations.history"] * 3 + ["conversations.replies"] * 2
    assert client.get.await_args_list[2].kwargs["params"]["cursor"] == "c1"


@pytest.mark.asyncio
async def test_a_slack_error_stops_the_read():
    from ddp_sync.pipelines.slack_citations import SlackReadError, fetch_threads

    client_patch, _ = _slack_responses((200, {"ok": False, "error": "missing_scope"}))
    with client_patch, pytest.raises(SlackReadError, match="missing_scope"):
        await fetch_threads("xoxb-test", "C1", roots_since=0.0)


@pytest.mark.asyncio
async def test_an_alert_is_posted_only_when_a_left_over_citation_nears_the_end_of_the_window():
    import time as _time

    now = _time.time()
    old = [_reply(f"{now - 26 * 86400 + i:.1f}", org=f"Old {i}") for i in range(3)]   # 26 days old
    fresh = [_reply(f"{now - 86400 + i:.1f}", org=f"New {i}") for i in range(3)]       # 1 day old
    store = _store()
    config = {"mode": "report_only", "max_citations_per_run": 2, "lookback_days": 30, "thread_lookback_days": 0}

    async def run(replies):
        with patch(f"{_P}.fetch_threads", new=AsyncMock(return_value=[[ROOT, *replies]])), \
                patch(f"{_P}.find_bill", new=AsyncMock(return_value=(_BILL, True))), \
                patch(f"{_P}.get_redis_store", return_value=store), \
                patch(f"{_P}.post_alert") as alert, \
                patch.dict("os.environ", {"SLACK_BOT_TOKEN": "xoxb-test"}):
            await run_slack_citations_job(config, trigger="test")
        return alert

    assert (await run(old)).call_count == 1   # the third old one is 26 days old, 30 - 7 = 23
    assert (await run(fresh)).call_count == 0  # left over too, but only a day old
