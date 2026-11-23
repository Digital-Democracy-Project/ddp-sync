"""SYNC-187: cloud scrapes run on their own pool, a run that waits for a slot says so, and alerts never wait behind them.

On 2026-11-23 the EC2 ddp-sync started 8 cloud scrapes and the next 8 of a burst sat in Python's default thread pool
(8 threads on 4 CPUs) with no log line (notes/ops-handoff c35a38c). These tests go through the real funnel,
`_run_scrape`, with `run_cloud_scrape` replaced by a stand-in that blocks until the test lets it finish.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
from structlog.testing import capture_logs

from ddp_sync.pipelines import cloud_scrape_trigger as cst
from ddp_sync.pipelines.openstates_scrape import _run_scrape
from tests import sync157_manifest

STATES = ("az", "fl", "ma", "ut")  # live states: the SYNC-147 people check passes them


@pytest.fixture
def slots(monkeypatch, tmp_path):
    """A cap of 3 on a fresh pool, and the four states on the cloud path."""
    sync157_manifest.use(tmp_path, monkeypatch, **sync157_manifest.only("enrollment", "cloud_path", list(STATES)))
    pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="cloud-scrape-test")
    monkeypatch.setattr(cst, "CLOUD_SCRAPE_SLOTS", 3)
    monkeypatch.setattr(cst, "_cloud_scrape_pool", pool)
    monkeypatch.setattr(cst, "_cloud_scrape_runs", [])
    yield 3
    pool.shutdown(wait=False, cancel_futures=True)


class BlockingScrape:
    """Stands in for run_cloud_scrape: records who started, holds the thread until released."""

    def __init__(self):
        self.started: list[str] = []
        self.release = threading.Event()
        self._lock = threading.Lock()

    def __call__(self, jurisdiction, session_arg, openstates_root, config):
        with self._lock:
            self.started.append(jurisdiction)
        assert self.release.wait(10), "test never released the scrape"
        return {"success": True, "jurisdiction": jurisdiction, "duration_seconds": 0.0}


async def _until(check, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        assert asyncio.get_running_loop().time() < deadline, "timed out waiting"
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_n_plus_one_scrapes_with_a_cap_of_n_one_waits_and_logs_it(slots):
    scrape = BlockingScrape()
    config = {"cloud_path": {"enabled": True}}
    with capture_logs() as logs, patch("ddp_sync.pipelines.openstates_scrape.run_cloud_scrape", scrape):
        runs = [asyncio.create_task(_run_scrape(st, None, "/fake/root", config=config)) for st in STATES]
        await _until(lambda: any(e["event"] == "cloud_scrape_waiting_for_slot" for e in logs))
        await _until(lambda: len(scrape.started) == slots)
        await asyncio.sleep(0.2)
        # Exactly the cap is running; the fourth (whichever reached the pool last) is queued, not started.
        assert len(scrape.started) == slots
        waiting = [e for e in logs if e["event"] == "cloud_scrape_waiting_for_slot"]
        assert len(waiting) == 1
        late = waiting[0]["jurisdiction"]
        assert late in STATES and late not in scrape.started
        assert waiting[0]["queue_position"] == 1
        assert waiting[0]["slots"] == 3
        assert waiting[0]["log_level"] == "warning"  # seen at production's log level, not debug
        assert sorted(waiting[0]["running"]) == sorted(scrape.started)

        scrape.release.set()
        results = await asyncio.wait_for(asyncio.gather(*runs), 5)

    assert [r["success"] for r in results] == [True] * 4
    assert scrape.started[-1] == late
    acquired = [e for e in logs if e["event"] == "cloud_scrape_slot_acquired"]
    assert [e["jurisdiction"] for e in acquired] == [late]
    assert acquired[0]["log_level"] == "info" and acquired[0]["waited_seconds"] >= 0.2
    assert cst._cloud_scrape_runs == []


@pytest.mark.asyncio
async def test_queue_position_counts_every_run_already_waiting(slots):
    scrape = BlockingScrape()
    with capture_logs() as logs:
        runs = [asyncio.create_task(cst.run_in_cloud_scrape_slot(st, scrape, st, None, "/r", None))
                for st in STATES + ("wa",)]
        await _until(lambda: len(scrape.started) == slots)
        scrape.release.set()
        await asyncio.wait_for(asyncio.gather(*runs), 5)
    waiting = {e["jurisdiction"]: e["queue_position"] for e in logs if e["event"] == "cloud_scrape_waiting_for_slot"}
    assert waiting == {"ut": 1, "wa": 2}


@pytest.mark.asyncio
async def test_no_wait_line_while_slots_are_free(slots):
    scrape = BlockingScrape()
    scrape.release.set()
    with capture_logs() as logs:
        await cst.run_in_cloud_scrape_slot("hi", scrape, "hi", None, "/r", None)
    assert not [e for e in logs if e["event"].startswith("cloud_scrape_waiting") or e["event"] == "cloud_scrape_slot_acquired"]


@pytest.mark.asyncio
async def test_an_alert_during_a_full_scrape_pool_is_sent_at_once(slots):
    """Production's default pool is 8 threads on 4 CPUs; here it is as small as the scrape cap, so if scrapes used
    it (the bug) the alert's `to_thread` would wait for a scrape to finish."""
    loop = asyncio.get_running_loop()
    default_pool = ThreadPoolExecutor(max_workers=slots)
    loop.set_default_executor(default_pool)
    scrape = BlockingScrape()
    config = {"cloud_path": {"enabled": True}}
    sent = []
    try:
        with patch("ddp_sync.pipelines.openstates_scrape.run_cloud_scrape", scrape):
            runs = [asyncio.create_task(_run_scrape(st, None, "/fake/root", config=config)) for st in STATES]
            await _until(lambda: len(scrape.started) == slots)
            await asyncio.wait_for(asyncio.to_thread(sent.append, "alert"), 2)
            assert sent == ["alert"]
            assert len(scrape.started) == slots  # the scrapes were all still holding their slots
            scrape.release.set()
            await asyncio.wait_for(asyncio.gather(*runs), 5)
    finally:
        scrape.release.set()
        default_pool.shutdown(wait=False, cancel_futures=True)


@pytest.mark.asyncio
async def test_context_variables_reach_the_scrape_thread(slots):
    """FULL_COLLECTION (SYNC-181) is set by the caller and read inside run_cloud_scrape's thread, as to_thread did."""
    seen = []
    token = cst.FULL_COLLECTION.set(True)
    try:
        await cst.run_in_cloud_scrape_slot("fl", lambda: seen.append(cst.FULL_COLLECTION.get()))
    finally:
        cst.FULL_COLLECTION.reset(token)
    assert seen == [True]


@pytest.mark.asyncio
async def test_a_resumed_job_takes_a_cloud_scrape_slot(slots):
    """reconcile_inflight_fargate_jobs (after a restart) runs on the same pool, so it counts against the cap."""
    names = []

    def fake_resume(run_id, record):
        names.append(threading.current_thread().name)
        return {"success": True}

    with (
        patch.object(cst.inflight_fargate_jobs, "list_inflight", return_value={"ok-1": {"jurisdiction": "ok"}}),
        patch.object(cst, "resume_inflight_fargate_job", fake_resume),
    ):
        tasks = cst.reconcile_inflight_fargate_jobs()
        await asyncio.wait_for(asyncio.gather(*tasks), 5)
    assert len(names) == 1 and names[0].startswith("cloud-scrape-test")
