"""SYNC-152: one pipeline per state (PLAN-push-button-onboarding.md, "2026-11-10: One pipeline per state").

WHO is scraped is one manifest rule (status not paused or probing); WHEN is `openstates_scrape.timing`. Every scraped state
gets one job, `openstates_scrape_<state>`, running `run_state_pipeline`: every session in turn, the run history, the
broker signal, then ONE archive. The job raises when a session or the archive failed, and the status record is written
once, in `finally`. The manual trigger runs the same function.

The real UpdateScheduler and APScheduler are used; a temp manifest stands in for ddp-open-states (except in the guard
test at the bottom, which reads the real one); the scrape, the archive and Redis are faked.
"""

from __future__ import annotations

import os
import re
import tempfile
import textwrap
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.pipelines import openstates_scrape as scrape
from ddp_sync.scheduler import DEFAULT_CONFIG_PATH, UpdateScheduler
from ddp_sync.services import jurisdiction_enrollment as je

# The real ddp-open-states checkout for the guard test: OPENSTATES_ROOT when set (read at import: other tests clear it),
# else this Mac's default checkout path when it exists, else the guard test is skipped.
_REAL_CHECKOUT = os.environ.get("OPENSTATES_ROOT") or je.default_root()

_OLD_IDS = (
    "openstates_secondary_scrapes", "openstates_secondary_scrape_va", "openstates_primary_scrape_pa",
    "openstates_fl_scrape", "openstates_wa_scrape", "openstates_usa_scrape",
)


def _entry(status="live"):
    return {"status": status, "tier": "secondary", "archive": {"enabled": True, "scheduled": True},
            "enrollment": {"cloud_path": True}}


MANIFEST = {"us": _entry(), "fl": _entry(), "va": _entry(), "pa": _entry("scraping"), "al": _entry("paused"),
            "zz": _entry("probing")}
SCRAPED = {"usa", "fl", "va", "pa"}

_YAML = """
    openstates_scrape:
      enabled: true
      patch_refresh: {enabled: false}
      people_refresh: {enabled: false}
      defaults: {floor: weekly, day: sunday, start_utc: "02:00"}
      timing:
        fl: {floor: weekly, day: sunday, start_utc: "02:00"}
        usa: {floor: nightly, start_utc: "03:00", sessions: ["119 chamber=lower", "119 chamber=upper"]}
        pa: {floor: nightly, start_utc: "04:00"}
"""


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    je._cache.clear()
    je._logged.clear()
    monkeypatch.delenv(je.MODE_ENV, raising=False)
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(MANIFEST))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    return tmp_path


def _scheduler(path: Path | None = None) -> UpdateScheduler:
    if path is None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(textwrap.dedent(_YAML))
            path = Path(f.name)
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    return UpdateScheduler(settings=settings, config_path=path)


def _scrape_ids(sched) -> set[str]:
    return {j.id for j in sched.scheduler.get_jobs() if j.id.startswith("openstates_scrape_")}


# --- registration ---------------------------------------------------------------------------------------------


def test_exactly_one_job_per_scraped_state_and_none_of_the_old_ids():
    """Acceptance 1."""
    sched = _scheduler()
    for old in _OLD_IDS:  # a live scheduler still carrying the old layout
        sched.scheduler.add_job(lambda: None, "interval", hours=1, id=old)
    sched._register_openstates_scrape_jobs()

    ids = {j.id for j in sched.scheduler.get_jobs()}
    assert _scrape_ids(sched) == {f"openstates_scrape_{j}" for j in SCRAPED}
    assert not ids & set(_OLD_IDS)


def test_timing_and_defaults_set_the_trigger():
    sched = _scheduler()
    sched._register_openstates_scrape_jobs()

    def fields(j):
        return {f.name: str(f) for f in sched.scheduler.get_job(f"openstates_scrape_{j}").trigger.fields}

    assert (fields("pa")["hour"], fields("pa")["day_of_week"]) == ("4", "*")
    assert (fields("usa")["hour"], fields("usa")["day_of_week"]) == ("3", "*")
    assert (fields("fl")["hour"], fields("fl")["day_of_week"]) == ("2", "sun")
    assert (fields("va")["hour"], fields("va")["minute"], fields("va")["day_of_week"]) == ("2", "0", "sun")  # defaults


def test_a_state_that_is_paused_loses_its_job_on_the_next_registration(root):
    sched = _scheduler()
    sched._register_openstates_scrape_jobs()
    assert "openstates_scrape_va" in _scrape_ids(sched)

    (root / "jurisdictions.yaml").write_text(yaml.safe_dump({**MANIFEST, "va": _entry("paused")}))
    os.utime(root / "jurisdictions.yaml", (1, 1))  # a different mtime, so the cached manifest is re-read
    sched._register_openstates_scrape_jobs({})

    assert "openstates_scrape_va" not in _scrape_ids(sched)


def test_an_unreadable_manifest_keeps_the_jobs_already_registered(root, monkeypatch):
    sched = _scheduler()
    sched._register_openstates_scrape_jobs()
    before = _scrape_ids(sched)

    monkeypatch.setenv("OPENSTATES_ROOT", str(root / "nowhere"))
    sched._register_openstates_scrape_jobs({})

    assert _scrape_ids(sched) == before


def test_state_timing_merges_defaults_and_rejects_a_bad_floor():
    cfg = {"defaults": {"start_utc": "05:15"}, "timing": {"xx": {"floor": "hourly"}}}
    assert scrape.state_timing(cfg, "va") == {"floor": "weekly", "day": "sunday", "hour": 5, "minute": 15, "sessions": []}
    assert scrape.state_timing(cfg, "xx")["floor"] == "weekly"


# --- the pipeline -----------------------------------------------------------------------------------------------


class _Run:
    """Fakes for one pipeline run: the scrape (per session), the archive, history and status; records the order."""

    def __init__(self, scrape_results=None, archived=True, history_error=None, status_error=None, signal_error=None):
        self.order: list[str] = []
        self.statuses: list[tuple[str, dict]] = []
        self._results = list(scrape_results or [])
        self._archived = archived
        self._history_error, self._status_error, self._signal_error = history_error, status_error, signal_error

    async def run_scrape(self, jurisdiction, session_arg, *_a, **_kw):
        self.order.append(f"scrape:{session_arg}")
        r = self._results.pop(0) if self._results else {"success": True}
        if isinstance(r, Exception):
            raise r
        return {"jurisdiction": jurisdiction, **r}

    async def archive(self, jurisdiction, archive_config):
        self.order.append(f"archive:{jurisdiction}")
        return self._archived

    async def history(self, flow, jurisdictions, results, config):
        self.order.append(f"history:{flow}:{len(results)}")
        if self._history_error:
            raise self._history_error

    async def signal(self, jurisdiction, config):
        self.order.append("signal")
        if self._signal_error:
            raise self._signal_error

    async def status(self, key, payload):
        self.statuses.append((key, payload))
        if self._status_error:
            raise self._status_error

    def patches(self):
        return (
            patch.object(scrape, "_run_scrape", new=self.run_scrape),
            patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=self.archive),
            patch.object(scrape, "_check_sustained_blocking", new=self.history),
            patch.object(scrape, "_maybe_signal_broker_bill_events", new=self.signal),
            patch.object(scrape, "_write_flow_status", new=self.status),
        )


async def _go(run: _Run, jurisdiction="usa", config=None):
    config = yaml.safe_load(textwrap.dedent(_YAML))["openstates_scrape"] if config is None else config
    p = run.patches()
    with p[0], p[1], p[2], p[3], p[4]:
        return await scrape.run_state_pipeline(jurisdiction, config, {"enabled": True})


@pytest.mark.asyncio
async def test_the_us_is_scraped_in_both_chambers_then_archived_once():
    """Acceptance (SYNC-153 #2 builds on this): one archive, after both sessions; the US used to be archived twice."""
    run = _Run()
    result = await _go(run)
    assert run.order == [
        "scrape:session=119 chamber=lower", "scrape:session=119 chamber=upper",
        "history:openstates_scrape:2", "signal", "archive:usa",
    ]
    assert result["success"] is True and result["archived"] is True
    assert run.statuses[-1][0] == "openstates_scrape_usa" and run.statuses[-1][1]["status"] == "completed"
    assert len(run.statuses) == 1  # written once


@pytest.mark.asyncio
async def test_one_us_session_failing_still_archives_but_the_job_raises_and_the_status_is_failed():
    run = _Run([{"success": True}, {"success": False, "error": "exit_code_1"}])
    with pytest.raises(scrape.PipelineFailed) as exc:
        await _go(run)
    assert "archive:usa" in run.order
    assert "1 of 2" in str(exc.value)
    assert run.statuses == [("openstates_scrape_usa", run.statuses[0][1])] and run.statuses[0][1]["status"] == "failed"


@pytest.mark.asyncio
async def test_every_session_failing_means_no_archive_and_the_job_raises():
    run = _Run([{"success": False}, {"success": False}])
    with pytest.raises(scrape.PipelineFailed):
        await _go(run)
    assert not [o for o in run.order if o.startswith("archive")] and "signal" not in run.order
    assert run.statuses[0][1]["status"] == "failed"


@pytest.mark.asyncio
async def test_a_session_that_raises_does_not_stop_the_next_one():
    run = _Run([RuntimeError("fargate launch blew up"), {"success": True}])
    with pytest.raises(scrape.PipelineFailed):
        await _go(run)
    assert run.order[:2] == ["scrape:session=119 chamber=lower", "scrape:session=119 chamber=upper"]
    assert "archive:usa" in run.order
    first = run.statuses[0][1]["results"][0]
    assert first["success"] is False and "fargate launch blew up" in first["error"]


@pytest.mark.asyncio
async def test_a_failed_archive_fails_the_job():
    run = _Run(archived=False)
    with pytest.raises(scrape.PipelineFailed) as exc:
        await _go(run, "pa")
    assert "archive failed" in str(exc.value)
    assert run.statuses[0][1]["status"] == "failed" and run.statuses[0][1]["archived"] is False


@pytest.mark.asyncio
async def test_a_state_not_archived_after_its_scrape_completes():
    run = _Run(archived=None)
    result = await _go(run, "va")
    assert result["archived"] is None and run.statuses[0][1]["status"] == "completed"
    assert run.order == ["scrape:None", "history:openstates_scrape:1", "signal", "archive:va"]


@pytest.mark.asyncio
@pytest.mark.parametrize("which", ["history", "signal"])
async def test_a_bookkeeping_failure_never_stops_the_archive(which):
    run = _Run(**{f"{which}_error": ConnectionError("redis down")})
    result = await _go(run, "pa")
    assert "archive:pa" in run.order and result["success"] is True


@pytest.mark.asyncio
async def test_a_failed_status_write_never_replaces_the_real_outcome():
    ok = _Run(status_error=ConnectionError("redis down"))
    assert (await _go(ok, "pa"))["success"] is True
    bad = _Run([{"success": False}], status_error=ConnectionError("redis down"))
    with pytest.raises(scrape.PipelineFailed):
        await _go(bad, "pa")


@pytest.mark.asyncio
async def test_history_is_written_for_every_state_under_the_new_key():
    """Acceptance 4 (the writing half): FL, WA, USA and PA now record history, not only the old secondary batch."""
    store = MagicMock()
    store.append_run_history = AsyncMock()
    store.get_run_history = AsyncMock(return_value=[])
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store), \
            patch.object(scrape, "read_filing_counts", return_value={"bills_new": 3, "bills_updated": 1, "mode": "full"}):
        for j in ("fl", "wa", "usa", "pa"):
            await scrape._record_scrape_history(j, [{"success": True, "jurisdiction": j}], {})
    written = [(c.args[0], c.args[1], c.args[2]["bills_new"]) for c in store.append_run_history.await_args_list]
    assert written == [("openstates_scrape", j, 3) for j in ("fl", "wa", "usa", "pa")]


# --- the status for a scheduled run and a manual run --------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_scheduled_run_writes_the_status_record():
    sched = _scheduler()
    sched._register_openstates_scrape_jobs()
    run = _Run()
    p = run.patches()
    with p[0], p[1], p[2], p[3], p[4]:
        await sched.scheduler.get_job("openstates_scrape_pa").func()
    assert [k for k, _ in run.statuses] == ["openstates_scrape_pa"]


def test_a_manual_run_writes_the_same_status_record_and_answers_202():
    """Acceptance 2 end to end: POST /trigger/openstates-scrape/pa answers 202 (it answered 404 on 2026-11-10) and runs
    the real pipeline (TestClient runs the background task before returning)."""
    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "t"
    config = yaml.safe_load(textwrap.dedent(_YAML))["openstates_scrape"]
    scheduler = SimpleNamespace(_sync_config={"openstates_scrape": config, "openstates_archive": {"enabled": True}})
    run = _Run([{"success": False}])  # a failed run: logged by the background runner, never a 500
    p = run.patches()
    with p[0], p[1], p[2], p[3], p[4], patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler), \
            patch.object(scrape, "people_check_refusal", new=AsyncMock(return_value=None)):
        resp = TestClient(app).post("/trigger/openstates-scrape/pa")
    assert resp.status_code == 202
    assert [(k, s["status"]) for k, s in run.statuses] == [("openstates_scrape_pa", "failed")]


# --- the guard SYNC-57 and SYNC-146 lacked: the real manifest and the real sync_schedule.yaml -----------------------


def _guard_problems(sched: UpdateScheduler) -> list[str]:
    """Every way the scrape lists can disagree, read from the real config through the real code paths."""
    from ddp_sync.api.routes.triggers import _single_jurisdiction_targets

    config = sched._sync_config["openstates_scrape"]
    scraped = set(scrape.enrolled_scrape_states(config) or [])
    problems = []
    if not scraped:
        problems.append("the manifest gave no scraped states")
    jobs = {i.removeprefix("openstates_scrape_") for i in _scrape_ids(sched)}
    if jobs != scraped:
        problems.append(f"jobs != scraped: missing {sorted(scraped - jobs)}, extra {sorted(jobs - scraped)}")
    targets = _single_jurisdiction_targets(config)
    if targets != scraped:
        problems.append(f"manual targets != scraped: missing {sorted(scraped - targets)}, extra {sorted(targets - scraped)}")
    extra_timing = set(config.get("timing") or {}) - scraped
    if extra_timing:
        problems.append(f"timing for states not scraped: {sorted(extra_timing)}")
    if re.search(r"\b(primary|secondary)\b", yaml.safe_dump(config)):
        problems.append("openstates_scrape still says primary/secondary")
    return problems


def test_every_scraped_state_has_a_job_and_a_manual_trigger_and_nothing_else_does(monkeypatch):
    if not os.path.exists(os.path.join(_REAL_CHECKOUT, "jurisdictions.yaml")):
        pytest.skip("set OPENSTATES_ROOT to a ddp-open-states checkout to run this")
    monkeypatch.setenv("OPENSTATES_ROOT", _REAL_CHECKOUT)
    je._cache.clear()
    sched = _scheduler(DEFAULT_CONFIG_PATH)
    sched._register_openstates_scrape_jobs()
    assert _guard_problems(sched) == []


def test_the_guard_names_a_state_that_has_no_job(monkeypatch):
    """The guard itself fails on a deliberately missing state (acceptance 3)."""
    sched = _scheduler()
    sched._register_openstates_scrape_jobs()
    sched.scheduler.remove_job("openstates_scrape_pa")
    assert _guard_problems(sched) == ["jobs != scraped: missing ['pa'], extra []"]


def test_a_malformed_timing_entry_leaves_the_schedule_whole():
    """pm-review: every state's timing is read before any job is removed or replaced."""
    sched = _scheduler()
    sched.scheduler.add_job(lambda: None, "interval", hours=1, id="openstates_secondary_scrapes")
    before = {j.id: str(j.trigger) for j in sched.scheduler.get_jobs()}
    sched._sync_config["openstates_scrape"]["timing"]["va"] = {"start_utc": "two o'clock"}
    with pytest.raises(ValueError):
        sched._register_openstates_scrape_jobs({})
    assert {j.id: str(j.trigger) for j in sched.scheduler.get_jobs()} == before


def test_an_unreadable_manifest_leaves_even_old_layout_jobs_alone(root, monkeypatch):
    """pm-review: the manifest is read before the retired ids are removed."""
    sched = _scheduler()
    sched.scheduler.add_job(lambda: None, "interval", hours=1, id="openstates_secondary_scrapes")
    monkeypatch.setenv("OPENSTATES_ROOT", str(root / "nowhere"))
    sched._register_openstates_scrape_jobs()
    assert "openstates_secondary_scrapes" in {j.id for j in sched.scheduler.get_jobs()}
