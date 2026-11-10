"""POST /trigger/openstates-archive/{target} (SYNC-153).

Any state whose manifest entry has `archive.enabled` (PA, NE and LA answered 404 before, because the endpoint read the
recurring `archive.scheduled` list). The state's pipeline lock is taken BEFORE the answer: 202 means this run owns it,
409 while another run (a scrape pipeline, or another archive; `us` and `usa` are one lock) holds it. The `all` target is
gone. A tiny manifest in a temp dir stands in for ddp-open-states; Redis is a fake that honours SET NX.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.services import jurisdiction_enrollment as je


def _entry(status="live", archive=True):
    return {"status": status, "archive": {"enabled": archive, "scheduled": status == "live"}}


MANIFEST = {"fl": _entry(), "us": _entry(), "pa": _entry("scraping"), "ne": _entry("scraping"),
            "la": _entry("scraping"), "al": _entry("paused"), "zz": _entry("probing", archive=False)}


class FakeRedis:
    def __init__(self):
        self.data: dict[str, str] = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return None
        self.data[key] = value
        return True

    async def eval(self, script, n, key, token):
        if self.data.get(key) == token:
            del self.data[key]
            return 1
        return 0


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    je._cache.clear()
    je._logged.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(MANIFEST))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    return tmp_path


@pytest.fixture
def redis():
    fake = FakeRedis()
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=fake)):
        yield fake


def _post(target, archive_config=None, run=None):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "t"
    scheduler = SimpleNamespace(_sync_config={"openstates_archive": archive_config or {"enabled": True},
                                              "openstates_scrape": {}})
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch("ddp_sync.api.routes.triggers._run_archive_only_in_background", new=run or AsyncMock()) as bg:
        return TestClient(app).post(f"/trigger/openstates-archive/{target}"), bg


@pytest.mark.parametrize("target", ["pa", "ne", "la", "fl", "us", "al"])
def test_every_archive_enabled_state_answers_202_and_owns_the_lock(target, redis):
    """Acceptance 5 (PA, NE, LA answered 404). Alabama is archive.enabled in today's manifest, so it can still be archived
    by hand even though nothing archives it on a schedule any more."""
    resp, bg = _post(target)
    assert resp.status_code == 202 and resp.json() == {"status": "started", "target": target}
    code, _cfg, token = bg.await_args.args
    assert code == target and redis.data[f"ddp:pipeline_lock:{target}"] == token


def test_usa_is_accepted_as_an_alias_for_us(redis):
    resp, bg = _post("usa")
    assert resp.status_code == 202 and bg.await_args.args[0] == "us"


@pytest.mark.parametrize("target", ["zz", "mi", "all"])
def test_a_state_not_archive_enabled_unknown_or_the_old_all_target_is_404(target, redis):
    resp, bg = _post(target)
    assert resp.status_code == 404
    bg.assert_not_awaited()
    assert not redis.data


def test_a_second_request_while_the_first_holds_the_lock_is_409(redis):
    first, _ = _post("pa")
    second, bg = _post("pa")
    assert first.status_code == 202 and second.status_code == 409
    bg.assert_not_awaited()


def test_a_manual_us_archive_while_a_usa_scrape_runs_is_409(redis):
    """One lock name per state: the US scrape (`usa`) and the US archive (`us`) contend for the same key."""
    import asyncio

    from ddp_sync.services.redis_store import acquire_state_lock

    assert asyncio.run(acquire_state_lock("usa", 60))
    resp, bg = _post("us")
    assert resp.status_code == 409
    bg.assert_not_awaited()


def test_the_real_background_run_writes_its_own_status_record_and_releases_the_lock(redis):
    """An archive-only run writes `openstates_archive_<state>` (with the attempt count), not the scrape record."""
    from ddp_sync.api.routes import triggers

    statuses = []

    async def _status(key, payload):
        statuses.append((key, payload))

    with patch("ddp_sync.pipelines.openstates_archive.run_single_archive_job",
               new=AsyncMock(return_value={"success": True})), \
            patch("ddp_sync.pipelines.openstates_archive._write_flow_status", new=_status):
        resp, _ = _post("pa", run=triggers._run_archive_only_in_background)
    assert resp.status_code == 202
    assert statuses == [("openstates_archive_pa", statuses[0][1])]
    assert statuses[0][1]["status"] == "completed" and statuses[0][1]["attempts"] == 1
    assert "ddp:pipeline_lock:pa" not in redis.data  # released by its owner at the end
