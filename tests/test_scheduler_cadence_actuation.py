"""OPEN-140: making a cadence decision actually change the schedule.

The decision half already shipped and is pure — services/scrape_cadence.py, tested in
test_scrape_cadence.py. Nothing called it. These cover the actuation half: resolving
floors, splitting the secondary batch, applying a change without a restart, and the
failure directions that would be silent.

Two structural facts drive most of this and are worth stating before the tests:

  1. The secondary states do NOT have one job each. They share a single weekly job that
     fans out over a list. So escalating one is a *move* — out of the batch and into a
     job of its own — not a trigger edit. Getting that wrong scrapes a jurisdiction
     twice on the batch's day, and each run wipes the other's _data dir.

  2. start() registers from the YAML floors and never reads Redis. Overrides arrive
     later, from the review job. That is what makes a Redis outage structurally safe
     rather than merely handled — boot cannot depend on something that is down.

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

from ddp_sync.scheduler import UpdateScheduler

_YAML = """
    bill_sync:
      sync_time_utc: "04:00"

    openstates_scrape:
      enabled: true
      openstates_root: "/tmp/openstates"

      dynamic_cadence:
        enabled: {cadence_enabled}
        review_time_utc: "00:30"
        escalate_window: 2
        quiet_window: 4
        jurisdictions_excluded: ["mi"]

      patch_refresh:
        enabled: false

      primary:
        fl:
          enabled: true
          sync_time_utc: "02:00"
          sync_day: sunday
        wa:
          enabled: false
        usa:
          enabled: false

      secondary:
        enabled: true
        sync_day: sunday
        sync_time_utc: "02:00"
        jurisdictions: [va, mi, ma, ut, az]

      people_refresh:
        enabled: false
"""


def _scheduler(cadence_enabled: str = "false") -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(_YAML.format(cadence_enabled=cadence_enabled)))
        path = Path(f.name)
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    return UpdateScheduler(settings=settings, config_path=path)


def _job_ids(sched) -> set[str]:
    return {j.id for j in sched.scheduler.get_jobs()}


def _trigger(sched, job_id: str) -> str:
    return str(next(j.trigger for j in sched.scheduler.get_jobs() if j.id == job_id))


async def _scraped_by(job) -> list[str]:
    """Run a registered scrape job for real and report which jurisdictions it scraped.

    Patches the one call that would touch the disk and the network. Everything above
    it -- the wrapper the scheduler registered, run_secondary_scrapes_job, the list
    narrowing -- is the production code path.
    """
    calls: list[str] = []

    async def _fake_run_scrape(jurisdiction, *_a, **_kw):
        calls.append(jurisdiction)
        return {"success": True, "jurisdiction": jurisdiction}

    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape", new=_fake_run_scrape
    ), patch(
        "ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()
    ), patch(
        "ddp_sync.pipelines.openstates_scrape._check_sustained_blocking", new=AsyncMock()
    ):
        await job.func()
    return calls


class TestRegistrationFromFloors:
    """No overrides — the schedule must be byte-for-byte what it is today."""

    def test_no_cadences_registers_exactly_the_current_layout(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs()

        ids = _job_ids(sched)
        assert "openstates_secondary_scrapes" in ids
        assert "openstates_fl_scrape" in ids
        # No per-jurisdiction secondary jobs exist unless something escalated.
        assert not [i for i in ids if i.startswith("openstates_secondary_scrape_")]
        assert "day_of_week='sun'" in _trigger(sched, "openstates_fl_scrape")

    def test_the_effective_cadence_is_recorded_for_every_jurisdiction(self):
        """The floor rule takes the live cadence out of git, so it has to be
        readable somewhere. /schedule reports this map."""
        sched = _scheduler()
        sched._register_openstates_scrape_jobs()

        assert sched._openstates_cadence == {
            "fl": "weekly", "va": "weekly", "mi": "weekly",
            "ma": "weekly", "ut": "weekly", "az": "weekly",
        }


class TestEscalation:
    def test_an_escalated_secondary_jurisdiction_leaves_the_batch(self):
        """The core structural move. Leaving it in both would scrape it twice on
        the batch's day, and each run wipes the other's _data dir."""
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"va": "nightly"})

        ids = _job_ids(sched)
        assert "openstates_secondary_scrape_va" in ids
        assert "openstates_secondary_scrapes" in ids

        va = _trigger(sched, "openstates_secondary_scrape_va")
        assert "day_of_week" not in va, "an escalated jurisdiction runs nightly"
        assert "day_of_week='sun'" in _trigger(sched, "openstates_secondary_scrapes")

    @pytest.mark.asyncio
    async def test_the_batch_no_longer_scrapes_the_escalated_jurisdiction(self):
        """Not just a different job id — the batch must actually stop scraping it.

        The batch reads its list from config by default, and config still names every
        secondary jurisdiction including the escalated ones, so the narrowed list has
        to be passed explicitly.

        Patches _run_scrape rather than run_secondary_scrapes_job, so the real job
        function runs and this asserts which jurisdictions were actually scraped
        rather than which arguments were passed to a stub.
        """
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"va": "nightly"})

        batch = next(
            j for j in sched.scheduler.get_jobs() if j.id == "openstates_secondary_scrapes"
        )
        scraped = await _scraped_by(batch)

        assert scraped == ["mi", "ma", "ut", "az"]
        assert "va" not in scraped

    @pytest.mark.asyncio
    async def test_the_escalated_job_scrapes_only_its_own_jurisdiction(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"va": "nightly"})

        job = next(
            j for j in sched.scheduler.get_jobs()
            if j.id == "openstates_secondary_scrape_va"
        )
        assert await _scraped_by(job) == ["va"]

    @pytest.mark.asyncio
    async def test_the_split_out_job_writes_its_own_flow_status(self):
        """The flow STATUS is one document per flow, so two jobs sharing a key would
        overwrite each other. The run HISTORY deliberately still uses the shared
        secondary flow name — that is what the review reads, and splitting it on a
        cadence change would erase the evidence that caused the change."""
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"va": "nightly"})
        job = next(
            j for j in sched.scheduler.get_jobs()
            if j.id == "openstates_secondary_scrape_va"
        )

        with patch(
            "ddp_sync.pipelines.openstates_scrape._run_scrape",
            new=AsyncMock(return_value={"success": True, "jurisdiction": "va"}),
        ), patch(
            "ddp_sync.pipelines.openstates_scrape._write_flow_status",
            new=AsyncMock(),
        ) as status, patch(
            "ddp_sync.pipelines.openstates_scrape._check_sustained_blocking",
            new=AsyncMock(),
        ) as history:
            await job.func()

        assert status.await_args.args[0] == "openstates_secondary_scrape_va"
        assert history.await_args.args[0] == "openstates_secondary_scrapes", (
            "history must stay under the shared flow name across a cadence change"
        )

    def test_each_escalated_jurisdiction_gets_its_own_job(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"va": "nightly", "az": "nightly"})

        ids = _job_ids(sched)
        assert "openstates_secondary_scrape_va" in ids
        assert "openstates_secondary_scrape_az" in ids

    def test_escalating_florida_drops_its_weekly_day(self):
        """The case this ticket was filed for. FL's sync_day carries a 'remove once
        the 2027 session opens' comment that depends on a human remembering."""
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"fl": "nightly"})

        assert "day_of_week" not in _trigger(sched, "openstates_fl_scrape")
        assert sched._openstates_cadence["fl"] == "nightly"

    def test_the_batch_is_removed_when_everything_escalates(self):
        """An empty batch job would fire weekly and scrape nothing.

        MI is left out because it can never be escalated (see TestExcludedAtTheActuator),
        so "everything" here means every jurisdiction that is eligible.
        """
        sched = _scheduler()
        sched._sync_config["openstates_scrape"]["secondary"]["jurisdictions"] = [
            "va", "ma", "ut", "az",
        ]
        sched._register_openstates_scrape_jobs(
            {j: "nightly" for j in ("va", "ma", "ut", "az")}
        )

        assert "openstates_secondary_scrapes" not in _job_ids(sched)
        assert len([i for i in _job_ids(sched) if i.startswith("openstates_secondary_scrape_")]) == 4


class TestExcludedAtTheActuator:
    """Defence in depth, folded in from pm-review.

    cadence_review() already refuses to escalate MI, but that only covers decisions
    this process made. A value hand-written into Redis, or a future caller passing a
    map straight to the registration method, would otherwise reach APScheduler
    unchecked. OPEN-53: more traffic against a WAF worsens a block.
    """

    def test_an_excluded_jurisdiction_is_never_given_a_nightly_job(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"mi": "nightly"})

        assert "openstates_secondary_scrape_mi" not in _job_ids(sched)
        assert sched._openstates_cadence["mi"] == "weekly"

    @pytest.mark.asyncio
    async def test_it_stays_in_the_weekly_batch(self):
        """Refusing the escalation must not drop it from the schedule altogether."""
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"mi": "nightly"})

        batch = next(
            j for j in sched.scheduler.get_jobs() if j.id == "openstates_secondary_scrapes"
        )
        assert "mi" in await _scraped_by(batch)

    def test_the_caller_s_map_is_not_mutated(self):
        """The caller keeps its own record — including the review, whose returned
        cadences are what an operator reads to see what was decided."""
        sched = _scheduler()
        requested = {"mi": "nightly"}
        sched._register_openstates_scrape_jobs(requested)

        assert requested == {"mi": "nightly"}


class TestExactlyOneCronPerJob:
    """The stale-job-id hazard. scheduler.py already solved this once for
    legislator_bio_sync; the same trap is here, because a jurisdiction moving
    between the batch and its own job changes which job id it lives under."""

    def test_reverting_an_escalation_removes_the_nightly_job(self):
        sched = _scheduler()
        sched._register_openstates_scrape_jobs({"va": "nightly"})
        assert "openstates_secondary_scrape_va" in _job_ids(sched)

        sched._register_openstates_scrape_jobs({})

        assert "openstates_secondary_scrape_va" not in _job_ids(sched), (
            "a stale nightly cron alongside the batch would scrape va twice"
        )
        assert "openstates_secondary_scrapes" in _job_ids(sched)

    def test_re_registering_never_duplicates_a_job(self):
        sched = _scheduler()
        for _ in range(3):
            sched._register_openstates_scrape_jobs({"va": "nightly"})

        ids = [j.id for j in sched.scheduler.get_jobs()]
        assert len(ids) == len(set(ids)), f"duplicate job ids: {ids}"

    def test_the_batch_comes_back_after_a_full_escalate_and_revert(self):
        sched = _scheduler()
        sched._sync_config["openstates_scrape"]["secondary"]["jurisdictions"] = [
            "va", "ma", "ut", "az",
        ]
        sched._register_openstates_scrape_jobs(
            {j: "nightly" for j in ("va", "ma", "ut", "az")}
        )
        assert "openstates_secondary_scrapes" not in _job_ids(sched)

        sched._register_openstates_scrape_jobs({})

        assert "openstates_secondary_scrapes" in _job_ids(sched)
        assert not [i for i in _job_ids(sched) if i.startswith("openstates_secondary_scrape_")]


class TestReviewJobRegistration:
    def test_disabled_registers_no_review_job(self):
        """Ships dark: the decision needs weeks of bills_new history first."""
        sched = _scheduler(cadence_enabled="false")
        sched._register_openstates_scrape_jobs()
        assert "openstates_cadence_review" not in _job_ids(sched)

    def test_enabled_registers_the_review_job(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        assert "openstates_cadence_review" in _job_ids(sched)

    def test_the_review_runs_before_the_days_first_scrape(self):
        """00:30 vs patch refresh 01:00 and FL/secondary 02:00 — so a cadence
        decided this morning takes effect this morning, not tomorrow."""
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        trig = _trigger(sched, "openstates_cadence_review")
        assert "hour='0'" in trig and "minute='30'" in trig

    def test_startup_schedules_a_catch_up_review_soon(self):
        """pm-review's restart finding.

        Startup registers from the floors and never reads Redis, so a restart drops any
        stored escalation back to its floor. On a 00:30 cron alone, a 03:00 restart
        would leave a jurisdiction under-scraped for nearly 24 hours — and ddp-sync is
        deployed by launchctl kickstart, so restarts are routine.
        """
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
        """The review re-registers the scrape jobs, which re-registers the review job.
        Carrying the catch-up through would make every review schedule another one."""
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()          # startup
        sched._register_openstates_scrape_jobs({})        # as a review would

        job = next(
            j for j in sched.scheduler.get_jobs() if j.id == "openstates_cadence_review"
        )
        # No explicit next_run_time override, so it follows the plain daily cron.
        # (Before start() APScheduler leaves the attribute unset unless one was passed,
        # which is exactly the difference this is checking.)
        assert getattr(job, "next_run_time", None) is None


def _redis(overrides=None, history=None, set_ok=True):
    store = MagicMock()
    store.get_scrape_cadence = AsyncMock(side_effect=lambda j: (overrides or {}).get(j))
    store.get_run_history = AsyncMock(side_effect=lambda _f, j: (history or {}).get(j, []))
    store.set_scrape_cadence = AsyncMock(return_value=set_ok)
    return store


def _productive():
    return [{"bills_new": 12, "success": True}, {"bills_new": 4, "success": True}]


class TestReviewActuation:
    """The ticket's one genuine gap: scheduler.py loads its YAML once in __init__ and
    builds CronTriggers at start(), so a cadence change needed a restart."""

    @pytest.mark.asyncio
    async def test_filing_activity_moves_a_jurisdiction_to_nightly_without_a_restart(self):
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        assert "openstates_secondary_scrape_va" not in _job_ids(sched)

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"va": _productive()}),
        ):
            result = await sched._run_cadence_review()

        assert result["success"]
        assert "va" in result["changed"]
        # The same live scheduler object now has the nightly job. No restart.
        assert "openstates_secondary_scrape_va" in _job_ids(sched)
        assert "day_of_week" not in _trigger(sched, "openstates_secondary_scrape_va")

    @pytest.mark.asyncio
    async def test_the_escalation_is_persisted(self):
        """Without the write, the next restart silently reverts to the floor."""
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()
        store = _redis(history={"va": _productive()})

        with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store):
            await sched._run_cadence_review()

        store.set_scrape_cadence.assert_awaited_with("va", "nightly")

    @pytest.mark.asyncio
    async def test_an_escalation_that_cannot_be_persisted_is_not_scheduled(self):
        """A nightly job with no override behind it reverts at the next restart —
        a schedule that disagrees with its own stored state is worse than not
        escalating, because nothing would ever say so."""
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"va": _productive()}, set_ok=False),
        ):
            await sched._run_cadence_review()

        assert "openstates_secondary_scrape_va" not in _job_ids(sched)

    @pytest.mark.asyncio
    async def test_michigan_is_never_escalated(self):
        """OPEN-53: more traffic against a WAF worsens a block. MI is already
        excluded from scrape retries for the same reason."""
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"mi": _productive()}),
        ):
            result = await sched._run_cadence_review()

        assert "mi" not in result["changed"]
        assert result["cadences"]["mi"] == "weekly"
        assert "openstates_secondary_scrape_mi" not in _job_ids(sched)

    @pytest.mark.asyncio
    async def test_a_stored_override_is_applied_on_review(self):
        """Startup does not read Redis, so an override recorded before a restart is
        re-applied by the first review rather than being lost."""
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
        before = _job_ids(sched)

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(history={"va": [{"bills_new": 0, "success": True}]}),
        ):
            result = await sched._run_cadence_review()

        assert result["changed"] == []
        assert _job_ids(sched) == before

    @pytest.mark.asyncio
    async def test_the_review_is_a_no_op_when_disabled(self):
        sched = _scheduler(cadence_enabled="false")
        sched._register_openstates_scrape_jobs()
        result = await sched._run_cadence_review()
        assert result == {"success": True, "skipped": "disabled"}


class TestFailingSafe:
    @pytest.mark.asyncio
    async def test_redis_being_down_leaves_the_committed_schedule_in_force(self):
        """The direction that matters. Losing Redis must degrade to sync_schedule.yaml,
        never to an unscheduled jurisdiction."""
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
        assert "openstates_secondary_scrapes" in _job_ids(sched)
        assert "openstates_fl_scrape" in _job_ids(sched)

    def test_startup_registration_never_consults_redis(self):
        """Structural, not merely handled: boot cannot depend on something that is
        down. Overrides arrive from the first review instead."""
        with patch("ddp_sync.services.redis_store.get_redis_store") as get_store:
            sched = _scheduler(cadence_enabled="true")
            sched._register_openstates_scrape_jobs()

        get_store.assert_not_called()
        assert "openstates_secondary_scrapes" in _job_ids(sched)

    @pytest.mark.asyncio
    async def test_a_jurisdiction_is_never_scheduled_below_its_floor(self):
        """A 'weekly' override on a nightly-floored jurisdiction must not demote it.
        The floor rule is one-directional by design: automation may raise, never lower.
        """
        sched = _scheduler(cadence_enabled="true")
        # Remove FL's sync_day so its floor is nightly.
        sched._sync_config["openstates_scrape"]["primary"]["fl"].pop("sync_day")
        sched._register_openstates_scrape_jobs()

        with patch(
            "ddp_sync.services.redis_store.get_redis_store",
            return_value=_redis(overrides={"fl": "weekly"}),
        ):
            result = await sched._run_cadence_review()

        assert result["cadences"]["fl"] == "nightly"
        assert "day_of_week" not in _trigger(sched, "openstates_fl_scrape")

    @pytest.mark.asyncio
    async def test_a_read_failure_happens_before_any_write(self):
        """pm-review's ordering finding.

        With read-decide-write interleaved per jurisdiction, a read that throws on the
        fourth jurisdiction lands after the first three escalations are already stored
        — Redis saying nightly while the live schedule stays weekly. All reads now
        happen first, so a read failure aborts before any write exists to disagree with.
        """
        sched = _scheduler(cadence_enabled="true")
        sched._register_openstates_scrape_jobs()

        store = _redis(history={"va": _productive(), "ma": _productive()})
        calls = {"n": 0}

        async def _flaky_history(_flow, jurisdiction):
            calls["n"] += 1
            if calls["n"] > 2:
                raise ConnectionError("redis went away mid-review")
            return _productive() if jurisdiction in ("va", "ma") else []

        store.get_run_history = AsyncMock(side_effect=_flaky_history)

        with patch("ddp_sync.services.redis_store.get_redis_store", return_value=store):
            result = await sched._run_cadence_review()

        assert result["success"] is False
        store.set_scrape_cadence.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failure_applying_the_schedule_is_caught(self):
        """The docstring promises the review never raises. That has to cover the
        re-registration too, not only the Redis calls."""
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
        # The stored cadence is the correct one and the live schedule is the floor --
        # the safe direction, and it self-heals at the next review or restart.
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
        assert "openstates_secondary_scrape_va" not in _job_ids(sched)
