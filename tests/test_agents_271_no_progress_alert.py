"""AGENTS-271: a LegBot session run that is "running" but has finished no artifact and no bill for 10 minutes sends one alert.

2026-11-26: two CAMS tasks stuck at "running" held both bill slots for 29 min 43 s; nothing was written and nothing alerted.
The clock is faked (`activity.clock`); sleeps are real but tiny.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines import session_pipeline_runner as spr
from ddp_sync.services import activity

RUN = "run-1"


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def watch(monkeypatch):
    """start(): a run with `bills` in flight. Returns (clock, alerts, tick) where tick() lets the watchdog check once."""
    activity._reset_for_tests()
    clock = Clock()
    monkeypatch.setattr(activity, "clock", clock)
    alerts = []
    monkeypatch.setattr(spr, "post_alert", lambda text, **kw: alerts.append((text, kw)) or True)
    activity.start(RUN, "LegBot session pipeline", started_by="trigger", jurisdiction="wy", session="2026")

    async def tick(n=1):
        task = asyncio.create_task(spr._watch_no_progress(RUN, "wy", "2026", interval=0))
        for _ in range(n * 6):
            await asyncio.sleep(0)
        task.cancel()

    return clock, alerts, tick


@pytest.mark.asyncio
async def test_two_bills_in_flight_and_nothing_written_for_10_minutes_alerts_once(watch):
    clock, alerts, tick = watch
    activity.bill_started(RUN, "SF 87")
    activity.bill_started(RUN, "HB 114")
    clock.t += 599
    await tick()
    assert alerts == []  # not yet 10 minutes
    clock.t += 2
    await tick(5)  # many checks in the same quiet stretch
    [(text, kw)] = alerts
    assert kw == {"source": "legbot.no_progress", "dedupe_key": f"legbot_no_progress:{RUN}"}
    assert RUN in text and "WY 2026" in text and "10 min" in text and "SF 87" in text and "HB 114" in text


@pytest.mark.asyncio
async def test_a_run_that_keeps_finishing_artifacts_never_alerts(watch):
    clock, alerts, tick = watch
    activity.bill_started(RUN, "SF 87")
    for _ in range(5):  # 5 x 9 min = 45 min, an artifact every 9 min
        clock.t += 540
        activity.progress(RUN)
        await tick()
    assert alerts == []


@pytest.mark.asyncio
async def test_a_finished_bill_is_progress_too(watch):
    clock, alerts, tick = watch
    activity.bill_started(RUN, "SF 87")
    activity.bill_started(RUN, "HB 114")
    clock.t += 590
    activity.bill_finished(RUN, "SF 87", "skipped")
    clock.t += 590
    await tick()
    assert alerts == []


@pytest.mark.asyncio
async def test_a_run_with_no_bill_in_flight_is_waiting_its_turn_not_stuck(watch):
    clock, alerts, tick = watch
    clock.t += 3600  # queued behind another run for an hour
    await tick()
    assert alerts == []
    activity.bill_started(RUN, "SF 87")  # its turn comes: the clock starts now, not an hour ago
    await tick()
    assert alerts == []


@pytest.mark.asyncio
async def test_a_second_quiet_stretch_after_progress_alerts_again(watch):
    clock, alerts, tick = watch
    activity.bill_started(RUN, "SF 87")
    clock.t += 700
    await tick()
    activity.progress(RUN)
    clock.t += 700
    await tick()
    assert len(alerts) == 2


@pytest.mark.asyncio
async def test_a_failing_alert_does_not_stop_the_watchdog(watch, monkeypatch):
    clock, _alerts, tick = watch

    def boom(*a, **k):
        raise RuntimeError("slack down")

    monkeypatch.setattr(spr, "post_alert", boom)
    activity.bill_started(RUN, "SF 87")
    clock.t += 700
    await tick()  # must not raise


@pytest.mark.asyncio
async def test_a_failed_send_is_tried_again_at_the_next_check_and_then_sent_once(watch, monkeypatch):
    clock, _alerts, tick = watch
    sent, attempts = [], []

    def flaky(text, **kw):
        attempts.append(text)
        if len(attempts) == 1:
            raise RuntimeError("slack down")
        sent.append(text)
        return True

    monkeypatch.setattr(spr, "post_alert", flaky)
    activity.bill_started(RUN, "SF 87")
    clock.t += 700
    await tick(5)
    assert len(attempts) == 2 and len(sent) == 1  # failed once, retried, then no repeats


@pytest.mark.asyncio
async def test_a_finished_run_is_not_watched(watch):
    clock, alerts, tick = watch
    activity.bill_started(RUN, "SF 87")
    activity.finish(RUN, "done")
    clock.t += 3600
    await tick()
    assert alerts == []


@pytest.mark.asyncio
async def test_through_the_real_pipeline_a_stuck_artifact_alerts_and_is_not_cancelled(monkeypatch, legbot_stand_in_manifest):
    """Two bills, concurrency 2, each dispatch hangs until released. The watchdog (interval patched to 0) sees the quiet run
    once the fake clock moves 11 minutes; the run is not cancelled and ends normally after the release."""
    activity._reset_for_tests()
    clock = Clock()
    monkeypatch.setattr(activity, "clock", clock)
    spr._legbot_line = None
    alerts = []
    monkeypatch.setattr(spr, "post_alert", lambda text, **kw: alerts.append((text, kw)) or True)
    real_watch = spr._watch_no_progress
    monkeypatch.setattr(spr, "_watch_no_progress", lambda *a, **k: real_watch(*a, interval=0))
    release = asyncio.Event()

    async def stuck_step(*a, **k):
        await release.wait()
        return {"statements": 0, "matched": 0, "created": 0, "excluded": 0, "skipped_reason": None}

    monkeypatch.setattr(spr, "dispatch_and_store_policy_statements", stuck_step)
    monkeypatch.setattr(spr, "get_archived_bill_text", AsyncMock(return_value=None))
    monkeypatch.setattr(spr, "_polled_bill_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(spr, "get_bill_artifacts", AsyncMock(return_value=None))
    monkeypatch.setattr(spr, "list_current_session_bill_candidates", AsyncMock(side_effect=AssertionError("no scan")))
    monkeypatch.setattr(spr, "get_settings", lambda: SimpleNamespace(
        session_pipeline_concurrency=2, replica_freshness_check_enabled=False, policy_statement_stop_after_failures=3))
    cands = [{"gov_id": f"HB {i}", "bill_openstates_id": f"ocd-bill/{i}", "live_url_fallback": "u"} for i in (1, 2)]
    run = asyncio.create_task(spr.run_legbot_pipeline("fl", "2026", [], False, 10, include_concept_statements=False,
                                                      retry_failed=False, bill_candidates=cands, include_policy_statement=True))
    for _ in range(50):
        await asyncio.sleep(0)
    assert alerts == [] and not run.done()
    clock.t += 660
    for _ in range(50):
        await asyncio.sleep(0)
    assert len(alerts) == 1 and "HB 1" in alerts[0][0] and not run.done()
    release.set()
    summary = await run
    assert summary["bills_processed"] == 2 and len(alerts) == 1
    await asyncio.sleep(0)
    assert not [t for t in asyncio.all_tasks() if "_watch_no_progress" in repr(t) and not t.done()]  # no watchdog left behind


@pytest.mark.asyncio
async def test_a_finished_artifact_dispatch_resets_the_quiet_clock(monkeypatch):
    """One bill, two artifact types; the first finishes after the fake clock moved 9 minutes, the second after 9 more.
    Each finish is progress, so 18 minutes in total never reaches 10 quiet minutes."""
    from tests import test_session_pipeline_runner as t
    activity._reset_for_tests()
    clock = Clock()
    monkeypatch.setattr(activity, "clock", clock)
    seen = []

    async def artifact(**kw):
        clock.t += 540
        seen.append(activity.snapshot(spr_run_id[0])["last_progress_ts"])
        return {"id": 1, "status": "complete"}

    spr_run_id = []
    real_start = activity.start
    monkeypatch.setattr(activity, "start", lambda run_id, *a, **k: (spr_run_id.append(run_id), real_start(run_id, *a, **k))[1])
    with t._patch_lister([t._CANDIDATE]), t._patch_coverage(None), t._patch_version(), patch(
        "ddp_sync.pipelines.session_pipeline_runner.generate_and_store_bill_artifact", new=artifact,
    ):
        await spr.run_legbot_pipeline("fl", "2026F", ["bill_summary", "bill_topics"], False, limit=1,
                                      include_concept_statements=False, retry_failed=False)
    # the second dispatch started 540 s after the first one finished, so the stamp the second one saw is the first's finish
    assert seen[1] - seen[0] == 540
