"""OPEN-140: making a cadence decision actually change the schedule.

The decision half already shipped and is pure — services/scrape_cadence.py, tested in
test_scrape_cadence.py. These cover the actuation half: resolving floors, applying a change
without a restart, and the failure directions that would be silent.

SYNC-152 (one pipeline per state) changed the structure these tests pin:

  1. Every scraped state has ONE job, `openstates_scrape_<state>`, whatever its tempo. There is
     no weekly batch any more, so escalating a state is a trigger edit on that job (it loses its
     weekday), not a move between jobs. WHO is scraped is the manifest (a temp one here); the floor
     comes from `openstates_scrape.timing.<state>` or `defaults`.
  2. The review covers EVERY scraped state (FL, WA, USA, PA included) and reads the history every
     pipeline now writes, `openstates_scrape:<state>`, falling back to the retired
     `openstates_secondary_scrapes:<state>` while the new one is empty.
  3. start() still registers from the floors and never reads Redis. Overrides arrive later, from
     the review job. That is what makes a Redis outage structurally safe.

These drive the real UpdateScheduler against a real APScheduler instance and a real temp
config file. Only Redis is faked, because it is the one thing that is genuinely external.
"""

from __future__ import annotations

import tempfile
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from ddp_sync.scheduler import UpdateScheduler
from ddp_sync.services import jurisdiction_enrollment as je

_YAML = """
    bill_sync:
      sync_time_utc: "04:00"

    openstates_scrape:
      enabled: true
      openstates_root: "{root}"

      dynamic_cadence:
        enabled: {cadence_enabled}
        review_time_utc: "00:30"
        escalate_window: 2
        quiet_window: 4
        jurisdictions_excluded: ["mi"]

      patch_refresh:
        enabled: false

      defaults:
        floor: weekly
        day: sunday
        start_utc: "02:00"
      timing:
        fl: {{floor: weekly, day: sunday, start_utc: "02:00"}}
        wa: {{floor: nightly, start_utc: "02:30"}}

      people_refresh:
        enabled: false
"""

# The scraped states, from the (temp) manifest: FL and WA have timing entries, the rest take `defaults`.
_STATES = ["fl", "wa", "va", "mi", "ma", "ut", "az"]
_ROOT = ""  # the temp manifest's folder, set per test by the fixture below


@pytest.fixture(autouse=True)
def _manifest_root(tmp_path, monkeypatch):
    global _ROOT
    je._cache.clear()
    je._logged.clear()
    monkeypatch.delenv("OPENSTATES_ROOT", raising=False)
    manifest = {j: {"status": "live", "tier": "secondary"} for j in _STATES}
    manifest["al"] = {"status": "paused", "tier": "secondary"}
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(manifest))
    _ROOT = str(tmp_path)
    yield


def _scheduler(cadence_enabled: str = "false") -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(_YAML.format(cadence_enabled=cadence_enabled, root=_ROOT)))
        path = Path(f.name)
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    return UpdateScheduler(settings=settings, config_path=path)


def _job_ids(sched) -> set[str]:
    return {j.id for j in sched.scheduler.get_jobs()}


def _trigger(sched, job_id: str) -> str:
    return str(next(j.trigger for j in sched.scheduler.get_jobs() if j.id == job_id))


class TestRegistrationFromFloors:
    def test_no_cadences_registers_one_job_per_scraped_state_on_its_floor(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs()

        ids = _job_ids(sched)
        assert {i for i in ids if i.startswith("openstates_scrape_")} == {f"openstates_scrape_{j}" for j in _STATES}
        assert "openstates_scrape_al" not in ids  # paused
        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_fl")
        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_va")
        assert "day_of_week" not in _trigger(sched, "openstates_scrape_wa")

    def test_the_effective_cadence_is_recorded_for_every_jurisdiction(self):
        """The floor rule takes the live cadence out of git, so it has to be
        readable somewhere. /schedule reports this map."""
        sched = _scheduler()
        sched._register_openstates_scrape_jobs()

        assert sched._openstates_cadence == {
            "fl": "weekly", "wa": "nightly", "va": "weekly", "mi": "weekly",
            "ma": "weekly", "ut": "weekly", "az": "weekly",
        }


class TestEscalation:
    def test_an_escalated_state_keeps_its_job_and_loses_its_weekday(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"va": "nightly"})

        assert "day_of_week" not in _trigger(sched, "openstates_scrape_va"), "an escalated state runs nightly"
        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_ma"), "the others are untouched"
        assert sched._openstates_cadence["va"] == "nightly"

    def test_escalating_florida_drops_its_weekly_day(self):
        """The case OPEN-140 was filed for. FL's floor is weekly while out of session."""
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"fl": "nightly"})

        assert "day_of_week" not in _trigger(sched, "openstates_scrape_fl")
        assert sched._openstates_cadence["fl"] == "nightly"


class TestExcludedAtTheActuator:
    """Defence in depth, folded in from pm-review.

    cadence_review() already refuses to escalate MI, but that only covers decisions
    this process made. A value hand-written into Redis, or a future caller passing a
    map straight to the registration method, would otherwise reach APScheduler
    unchecked. OPEN-53: more traffic against a WAF worsens a block.
    """

    def test_an_excluded_jurisdiction_is_never_given_a_nightly_trigger(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"mi": "nightly"})

        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_mi")
        assert sched._openstates_cadence["mi"] == "weekly"

    def test_the_caller_s_map_is_not_mutated(self):
        sched = _scheduler()
        requested = {"mi": "nightly"}
        sched._register_openstates_scrape_jobs(requested)

        assert requested == {"mi": "nightly"}


class TestExactlyOneCronPerJob:
    def test_reverting_an_escalation_restores_the_weekday_on_the_same_job(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"va": "nightly"})
        sched._register_openstates_scrape_jobs({})

        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_va")

    def test_re_registering_never_duplicates_a_job(self):
        sched = _scheduler()
        for _ in range(3):
            sched._register_openstates_scrape_jobs({"va": "nightly"})

        ids = [j.id for j in sched.scheduler.get_jobs()]
        assert len(ids) == len(set(ids)), f"duplicate job ids: {ids}"


class TestReviewJobRegistration:
    def test_disabled_registers_no_review_job(self):
        sched = _scheduler(cadence_enabled="false")
        sched._register_openstates_scrape_jobs()
        assert "openstates_cadence_review" not in _job_ids(sched)

    def test_enabled_registers_the_review_job(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        assert "openstates_cadence_review" in _job_ids(sched)

    def test_the_review_runs_before_the_days_first_scrape(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        trig = _trigger(sched, "openstates_cadence_review")
        assert "hour='0'" in trig and "minute='30'" in trig

    def test_startup_schedules_a_catch_up_review_soon(self):
        before = datetime.now(timezone.utc)
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        job = next(
            j for j in sched.scheduler.get_jobs() if j.id == "openstates_cadence_review"
        )
        assert job.next_run_time is not None
        assert job.next_run_time < before + timedelta(hours=1), (
            "the catch-up must not wait for the daily cron"
        )

    def test_a_review_does_not_schedule_another_catch_up(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()          # startup
        sched._register_openstates_scrape_jobs({})        # as a review would

        job = next(
            j for j in sched.scheduler.get_jobs() if j.id == "openstates_cadence_review"
        )
        assert getattr(job, "next_run_time", None) is None


def _redis(overrides=None, history=None, set_ok=True, legacy=None):
    """`history` is the new `openstates_scrape:<state>` key; `legacy` the retired `openstates_secondary_scrapes` one."""
    store = MagicMock()
    store.get_scrape_cadence = AsyncMock(side_effect=lambda j: (overrides or {}).get(j))

    async def _history(flow, j):
        source = history if flow == "openstates_scrape" else legacy if flow == "openstates_secondary_scrapes" else {}
        return list((source or {}).get(j, []))

    store.get_run_history = AsyncMock(side_effect=_history)
    store.set_scrape_cadence = AsyncMock(return_value=set_ok)
    return store


def _productive():
    return [{"bills_new": 12, "success": True}, {"bills_new": 4, "success": True}]


class TestReviewActuation:
    @pytest.mark.asyncio
    async def test_filing_activity_moves_a_state_to_nightly_without_a_restart(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_va")

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"va": _productive()}),
        ):
            result = await sched._run_cadence_review()

        assert result["success"]
        assert "va" in result["changed"]
        # The same live scheduler object, the same job id, now nightly. No restart.
        assert "day_of_week" not in _trigger(sched, "openstates_scrape_va")

    @pytest.mark.asyncio
    async def test_florida_weekly_becomes_nightly_after_two_productive_runs_in_its_new_history(self):
        """The plan's named case: FL's review used to read a key nothing wrote, so it could never step up."""
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_fl")

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"fl": _productive()}),
        ):
            result = await sched._run_cadence_review()

        assert "fl" in result["changed"]
        assert "day_of_week" not in _trigger(sched, "openstates_scrape_fl")

    @pytest.mark.asyncio
    async def test_every_scraped_state_is_reviewed_from_its_own_history(self):
        """Acceptance 4: the review reads history for every scraped state (FL and WA here), under the new key."""
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        store = _redis()

        with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store):
            result = await sched._run_cadence_review()

        read = {(c.args[0], c.args[1]) for c in store.get_run_history.await_args_list}
        assert {("openstates_scrape", j) for j in _STATES} <= read
        assert {v["jurisdiction"] for v in result["verdicts"]} == set(_STATES)
        assert result["cadences"]["wa"] == "nightly"      # a nightly floor is reviewed, never changed
        assert "al" not in result["cadences"]              # paused: not scraped, not reviewed

    @pytest.mark.asyncio
    async def test_the_old_secondary_history_is_read_while_the_new_key_is_empty(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={}, legacy={"va": _productive()}),
        ):
            result = await sched._run_cadence_review()

        assert "va" in result["changed"]

    @pytest.mark.asyncio
    async def test_the_new_history_wins_over_the_old_one_once_it_exists(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"va": [{"bills_new": 0, "success": True}]}, legacy={"va": _productive()}),
        ):
            result = await sched._run_cadence_review()

        assert result["changed"] == []

    @pytest.mark.asyncio
    async def test_the_escalation_is_persisted(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        store = _redis(history={"va": _productive()})

        with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store):
            await sched._run_cadence_review()

        store.set_scrape_cadence.assert_awaited_with("va", "nightly")

    @pytest.mark.asyncio
    async def test_an_escalation_that_cannot_be_persisted_is_not_scheduled(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"va": _productive()}, set_ok=False),
        ):
            await sched._run_cadence_review()

        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_va")

    @pytest.mark.asyncio
    async def test_michigan_is_never_escalated(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"mi": _productive()}),
        ):
            result = await sched._run_cadence_review()

        assert "mi" not in result["changed"]
        assert result["cadences"]["mi"] == "weekly"
        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_mi")

    @pytest.mark.asyncio
    async def test_a_stored_override_is_applied_on_review(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(overrides={"va": "nightly"}),
        ):
            result = await sched._run_cadence_review()

        assert result["cadences"]["va"] == "nightly"

    @pytest.mark.asyncio
    async def test_a_review_that_changes_nothing_leaves_the_schedule_alone(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        before = {j.id: str(j.trigger) for j in sched.scheduler.get_jobs()}

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"va": [{"bills_new": 0, "success": True}]}),
        ):
            result = await sched._run_cadence_review()

        assert result["changed"] == []
        assert {j.id: str(j.trigger) for j in sched.scheduler.get_jobs()} == before

    @pytest.mark.asyncio
    async def test_the_review_is_a_no_op_when_disabled(self):
        sched = _scheduler(cadence_enabled="false")
        sched._register_openstates_scrape_jobs()
        result = await sched._run_cadence_review()
        assert result == {"success": True, "skipped": "disabled"}

    @pytest.mark.asyncio
    async def test_an_unreadable_manifest_leaves_the_schedule_alone(self, tmp_path):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        before = _job_ids(sched)
        sched._sync_config["openstates_scrape"]["openstates_root"] = str(tmp_path / "nowhere")

        with patch("ddp_sync.services.redis_store.get_redis_store", return_value=_redis()) as get_store:
            result = await sched._run_cadence_review()

        assert result["success"] is False
        get_store.return_value.set_scrape_cadence.assert_not_awaited()
        assert _job_ids(sched) == before


class TestFailingSafe:
    @pytest.mark.asyncio
    async def test_redis_being_down_leaves_the_committed_schedule_in_force(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        before = _job_ids(sched)

        store = MagicMock()
        store.get_scrape_cadence = AsyncMock(side_effect=ConnectionError("redis down"))
        store.get_run_history = AsyncMock(return_value=[])
        with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store):
            result = await sched._run_cadence_review()

        assert result["success"] is False
        assert _job_ids(sched) == before

    def test_startup_registration_never_consults_redis(self):
        with patch("ddp_sync.services.redis_store.get_redis_store") as get_store:
            sched = _scheduler(cadence_enabled="true")
            sched._register_openstates_scrape_jobs()

        get_store.assert_not_called()
        assert "openstates_scrape_va" in _job_ids(sched)

    @pytest.mark.asyncio
    async def test_a_jurisdiction_is_never_scheduled_below_its_floor(self):
        """A 'weekly' override on a nightly-floored state must not demote it."""
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(overrides={"wa": "weekly"}),
        ):
            result = await sched._run_cadence_review()

        assert result["cadences"]["wa"] == "nightly"
        assert "day_of_week" not in _trigger(sched, "openstates_scrape_wa")

    @pytest.mark.asyncio
    async def test_a_read_failure_happens_before_any_write(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        store = _redis()
        calls = {"n": 0}

        async def _flaky_history(_flow, jurisdiction):
            calls["n"] += 1
            if calls["n"] > 3:
                raise ConnectionError("redis went away mid-review")
            return _productive() if jurisdiction in ("va", "ma", "fl") else []

        store.get_run_history = AsyncMock(side_effect=_flaky_history)

        with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store):
            result = await sched._run_cadence_review()

        assert result["success"] is False
        store.set_scrape_cadence.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failure_applying_the_schedule_is_caught(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"va": _productive()}),
        ), patch.object(
            sched, "_register_openstates_scrape_jobs",
            side_effect=RuntimeError("apscheduler blew up"),
        ):
            result = await sched._run_cadence_review()  # must not raise

        assert result["success"] is False
        assert result["cadences"]["va"] == "nightly"

    @pytest.mark.asyncio
    async def test_junk_in_redis_falls_back_to_the_floor(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(overrides={"va": "hourly-ish"}),
        ):
            result = await sched._run_cadence_review()

        assert result["cadences"]["va"] == "weekly"
        assert "day_of_week='sun'" in _trigger(sched, "openstates_scrape_va")
