"""SYNC-179: after a restart, the previous process's background LegBot session runs stop saying "running" and their
per-session locks are released (2026-11-18: `ddp-sync status` still showed US 119 and NC 2025 running, "Safe to restart: NO",
and North Carolina's resend was refused "already running" until the old lease ran out)."""

from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync import app as app_module
from ddp_sync import status_report
from ddp_sync.pipelines import scraper_triggered_legbot as stl
from ddp_sync.services import activity
from ddp_sync.services import redis_store as rs_module
from ddp_sync.services.redis_store import (
    PIPELINE_LOCK_PREFIX,
    RedisStore,
    reconnect_until_available,
)
from tests.test_sync_160_redis_reconnect import (
    _connect_after,
    _enabled_settings,
    _FakeRedisWithScan,
)

# SYNC-186: the LegBot gate reads the stand-in manifest (tests/conftest.py), not this host's checkout.
pytestmark = pytest.mark.usefixtures("legbot_stand_in_manifest")


class _Fake(_FakeRedisWithScan):
    """SYNC-160's fake, taking the `count` hint the start-up cleanup passes to SCAN."""

    async def scan_iter(self, match=None, count=None):
        async for key in super().scan_iter(match=match):
            yield key

    async def ttl(self, key):
        return 600 if key in self.store else -2


def _record(run_id: str, status: str, iso2: str, session: str) -> bytes:
    """Last heartbeat a moment ago: the restart came within SESSION_RUN_LOST_AFTER_SECONDS of it, as on 2026-11-18, so
    the old rule (`session_run_as_read`) still reads it as running."""
    now = datetime.now(UTC).isoformat()
    return json.dumps({"run_id": run_id, "status": status, "jurisdiction_iso2": iso2, "session_code": session,
                       "accepted_at": "2026-11-18T03:22:00+00:00", "updated_at": now,
                       "finished_at": None, "error": None, "result": None}).encode()


@pytest.fixture
def client(monkeypatch):
    store = RedisStore()
    fake = _Fake()
    store._client = fake
    monkeypatch.setattr(rs_module, "_redis_store", store)
    p = RedisStore.LEGBOT_SESSION_RUN_PREFIX
    fake.store[f"{p}legbot-session-us"] = _record("legbot-session-us", "running", "US", "119")
    fake.store[f"{p}legbot-session-nc"] = _record("legbot-session-nc", "queued", "NC", "2025")
    fake.store[f"{p}legbot-session-done"] = _record("legbot-session-done", "completed", "NC", "2025")
    fake.store[stl._lock_key("US", "119")] = b"old-run-us"
    fake.store[stl._lock_key("NC", "2025")] = b"old-run-nc"
    fake.store[f"{PIPELINE_LOCK_PREFIX}fl"] = b"not-ours"  # SYNC-153's own lock: cleared by clear_state_locks, not here
    return fake


def _saved(fake, run_id):
    return json.loads(fake.store[f"{RedisStore.LEGBOT_SESSION_RUN_PREFIX}{run_id}"])


@pytest.mark.asyncio
async def test_the_previous_processes_runs_are_marked_stopped_and_their_locks_released(client):
    counts = await stl.stop_previous_process_session_runs()
    assert counts == {"runs": 2, "locks": 2}
    for run_id in ("legbot-session-us", "legbot-session-nc"):
        record = _saved(client, run_id)
        assert record["status"] == "lost" and record["error"] == stl.RESTART_STOPPED_ERROR
        assert record["finished_at"] and record["updated_at"] == record["finished_at"]
    assert _saved(client, "legbot-session-done")["status"] == "completed"  # a finished run is left alone
    assert stl._lock_key("US", "119") not in client.store and stl._lock_key("NC", "2025") not in client.store
    assert f"{PIPELINE_LOCK_PREFIX}fl" in client.store


@pytest.mark.asyncio
async def test_status_then_shows_nothing_running_and_the_runs_as_stopped_by_a_restart(client, monkeypatch):
    """Acceptance 1."""
    monkeypatch.setattr(status_report, "_client", lambda: client)
    monkeypatch.setattr(activity, "_client", lambda: None)
    monkeypatch.setattr(status_report, "_fargate_jobs", list)
    monkeypatch.setattr(status_report, "scheduled_jobs", lambda: {"status": "running", "jobs": []})
    monkeypatch.setattr(activity, "recent", AsyncMock(return_value=[]))
    client.store.pop(f"{PIPELINE_LOCK_PREFIX}fl")  # cleared at the same start-up by clear_state_locks
    before = await status_report.collect()
    assert before["restart"]["safe"] is False  # what Ramon saw

    await stl.stop_previous_process_session_runs()
    data = await status_report.collect()
    text = status_report.format_text(data)
    assert data["restart"]["safe"] is True
    assert "Running now:\n  nothing" in text and "Locks held:\n  none" in text
    finished = text.split("Recently finished:\n", 1)[1]
    assert "legbot-session-us (US 119)" in finished and "legbot-session-nc (NC 2025)" in finished
    assert finished.count("(stopped by a restart: ddp-sync restarted during the run)") == 2


@pytest.mark.asyncio
async def test_a_resend_for_the_same_state_is_accepted_at_once(client, monkeypatch):
    """Acceptance 2: refused "already running" before the clean-up (the 2026-11-18 wait), accepted right after it."""
    monkeypatch.setattr("ddp_sync.pipelines.scraper_triggered_legbot.get_settings", _enabled_settings)

    async def resend():
        return await stl.trigger_scraper_session_pipeline(
            "NC", "2025", ["bill_summary"], False, None, include_concept_statements=False, retry_failed=False)

    with patch("ddp_sync.pipelines.session_pipeline_runner.run_legbot_pipeline",
               new=AsyncMock(return_value={"bills_considered": 1, "bills_processed": 1})):
        refused = await resend()
        await stl.stop_previous_process_session_runs()
        accepted = await resend()
    assert refused["error"] == "already_running" and refused["current_run_id"] == "old-run-nc"
    assert accepted["success"] is True


@pytest.mark.asyncio
async def test_no_redis_and_a_redis_error_never_break_startup(monkeypatch):
    store = RedisStore()
    monkeypatch.setattr(rs_module, "_redis_store", store)
    assert await stl.stop_previous_process_session_runs() == {"runs": 0, "locks": 0}

    class Broken(_Fake):
        async def scan_iter(self, match=None, count=None):
            raise ConnectionError("redis went away")
            yield  # pragma: no cover

    store._client = Broken()
    assert await stl.stop_previous_process_session_runs() == {"runs": 0, "locks": 0}


@pytest.mark.asyncio
async def test_redis_arriving_late_gets_the_same_clean_up(monkeypatch):
    """SYNC-160's path: Redis was not up at start-up; when it connects, the old runs are cleaned up as start-up would have."""
    store = RedisStore()
    monkeypatch.setattr(rs_module, "_redis_store", store)
    fake = _Fake()
    fake.store[f"{RedisStore.LEGBOT_SESSION_RUN_PREFIX}legbot-session-us"] = _record("legbot-session-us", "running",
                                                                                      "US", "119")
    fake.store[stl._lock_key("US", "119")] = b"old-run-us"
    _connect_after(store, failures=1, client=fake)
    await reconnect_until_available(interval=0, alert_after=10_000)
    assert _saved(fake, "legbot-session-us")["status"] == "lost"
    assert stl._lock_key("US", "119") not in fake.store


def test_startup_runs_the_clean_up_before_the_scheduler_starts():
    source = inspect.getsource(app_module.lifespan)
    assert "await stop_previous_process_session_runs()" in source
    assert source.index("await stop_previous_process_session_runs()") < source.index("scheduler.start()")


@pytest.mark.asyncio
async def test_only_queued_and_running_records_are_touched(client):
    p = RedisStore.LEGBOT_SESSION_RUN_PREFIX
    client.store[f"{p}odd"] = _record("odd", "paused-by-someone", "US", "119")
    no_status = json.loads(_record("bare", "running", "US", "119"))
    del no_status["status"]
    client.store[f"{p}bare"] = json.dumps(no_status).encode()
    await stl.stop_previous_process_session_runs()
    assert _saved(client, "odd")["status"] == "paused-by-someone"
    assert "status" not in _saved(client, "bare")


@pytest.mark.asyncio
async def test_a_redis_error_on_the_records_still_releases_the_locks(client):
    """The locks are what refuse a resend: they go first, and a failure in the record step does not undo or skip it."""
    async def broken_get(key):
        raise ConnectionError("redis hiccup")

    client.get = broken_get
    counts = await stl.stop_previous_process_session_runs()
    assert counts["locks"] == 2
    assert stl._lock_key("NC", "2025") not in client.store


@pytest.mark.asyncio
async def test_a_record_without_its_run_id_is_saved_under_its_own_key(client):
    p = RedisStore.LEGBOT_SESSION_RUN_PREFIX
    record = json.loads(_record("x", "running", "US", "119"))
    del record["run_id"]
    client.store[f"{p}legbot-session-noid"] = json.dumps(record).encode()

    async def byte_keys(match=None, count=None):
        async for key in _Fake.scan_iter(client, match=match, count=count):
            yield key.encode()

    original_get = client.get

    async def get_any(key):  # real Redis takes bytes or text keys alike
        return await original_get(key.decode() if isinstance(key, bytes) else key)

    client.scan_iter = byte_keys
    client.get = get_any
    await stl.stop_previous_process_session_runs()
    assert _saved(client, "legbot-session-noid")["status"] == "lost"
    assert not any(k.startswith(f"{p}b'") for k in client.store)
