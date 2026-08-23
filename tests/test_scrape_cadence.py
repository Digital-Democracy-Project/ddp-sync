"""Tests for OPEN-140: cadence that follows observed filing activity.

Two operator decisions are load-bearing and each has assertions that fail loudly if reversed:

  * the YAML value is a FLOOR -- automation may raise a jurisdiction's cadence, never lower it,
    and losing the Redis override degrades to the configured value rather than to nothing;
  * escalation is automatic, demotion is only ever advice.

The Arizona case is the regression guard that matters. It imported zero new bills across ten
consecutive runs because it was wedged, so any rule that demotes on a quiet stretch would have
demoted it *because* it was broken.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.services.scrape_cadence import (
    NIGHTLY,
    WEEKLY,
    cadence_from_yaml,
    cadence_review,
    demotion_looks_reasonable,
    resolve_cadence,
    should_escalate,
)


def h(*specs):
    """History from (bills_new, success) pairs. bills_new None = run not measured."""
    out = []
    for s in specs:
        n, ok = s if isinstance(s, tuple) else (s, True)
        r = {"success": ok}
        if not ok:
            r["failure_reason"] = "waf_block"
        if n is not None:
            r["bills_new"] = n
        out.append(r)
    return out


# -- the floor rule ---------------------------------------------------------------------------


def test_yaml_sync_day_means_weekly_and_its_absence_means_nightly():
    assert cadence_from_yaml({"sync_day": "sunday"}) == WEEKLY
    assert cadence_from_yaml({}) == NIGHTLY
    assert cadence_from_yaml({"sync_day": None}) == NIGHTLY


def test_an_override_may_raise_the_cadence():
    assert resolve_cadence(WEEKLY, NIGHTLY) == NIGHTLY


def test_an_override_may_never_lower_it_below_the_floor():
    """The whole floor rule. A nightly jurisdiction in YAML cannot be dropped to weekly by
    anything in Redis -- that would be unattended demotion by the back door."""
    assert resolve_cadence(NIGHTLY, WEEKLY) == NIGHTLY


@pytest.mark.parametrize("bad", [None, "", "hourly", "WEEKLY", "monthly", "0"])
def test_a_missing_or_junk_override_falls_back_to_the_configured_floor(bad):
    """Losing Redis, or finding rubbish in it, must degrade to the committed configuration --
    never to an unscheduled jurisdiction."""
    assert resolve_cadence(WEEKLY, bad) == WEEKLY
    assert resolve_cadence(NIGHTLY, bad) == NIGHTLY


def test_an_unrecognised_floor_degrades_to_weekly_not_to_nothing():
    assert resolve_cadence("fortnightly", None) == WEEKLY


# -- escalation -------------------------------------------------------------------------------


def test_recent_filing_activity_escalates():
    assert should_escalate(h(0, 0, 12)) is True


def test_a_quiet_window_does_not_escalate():
    assert should_escalate(h(5, 0, 0)) is False


def test_one_productive_run_in_the_window_is_enough():
    """Deliberately generous: escalating wrongly costs a few scrapes, failing to escalate costs
    freshness on a jurisdiction that is actively legislating."""
    assert should_escalate(h(0, 3), window=2) is True


def test_unmeasured_runs_are_skipped_not_read_as_zero():
    """OPEN-139 distinguishes `ok:0` from `unparsed`. A run we could not measure must not be
    counted as a quiet one."""
    assert should_escalate(h(7, None, None), window=2) is True


def test_empty_history_does_not_escalate():
    assert should_escalate([]) is False


# -- demotion: advice only, and heavily guarded ------------------------------------------------


def test_a_genuinely_quiet_jurisdiction_is_a_reasonable_demotion_candidate():
    assert demotion_looks_reasonable(h(40, 30, 0, 0, 0, 0), quiet_window=4) is True


def test_arizona_is_never_a_demotion_candidate():
    """The regression guard. AZ imported zero new bills across ten consecutive runs because its
    incremental cutoff was stuck -- it was broken, not quiet. Demoting it would have checked the
    broken thing less often and slowed both detection and recovery."""
    az = h(*([0] * 10))
    assert demotion_looks_reasonable(az, quiet_window=4) is False


def test_a_jurisdiction_with_failing_runs_is_never_demoted():
    """Quiet AND failing is broken, not quiet. Even with prior filing history."""
    hist = h(40, 30, (0, True), (0, False), (0, True), (0, True))
    assert demotion_looks_reasonable(hist, quiet_window=4) is False


def test_a_jurisdiction_that_has_never_filed_is_never_demoted():
    """Indistinguishable from broken-since-inception, and demoting it would make finding out
    harder rather than easier."""
    assert demotion_looks_reasonable(h(0, 0, 0, 0, 0, 0), quiet_window=4) is False


def test_demotion_needs_a_full_quiet_window():
    assert demotion_looks_reasonable(h(40, 0, 0), quiet_window=4) is False


def test_recent_filing_blocks_the_demotion_suggestion():
    assert demotion_looks_reasonable(h(40, 0, 0, 9, 0), quiet_window=4) is False


# -- the review verdict -----------------------------------------------------------------------


def test_a_weekly_jurisdiction_that_starts_filing_is_escalated():
    v = cadence_review("fl", floor=WEEKLY, current_override=None, history=h(0, 14))
    assert v["action"] == "escalate"
    assert v["previous"] == WEEKLY and v["effective"] == NIGHTLY


def test_an_already_nightly_jurisdiction_needs_no_action():
    v = cadence_review("wa", floor=NIGHTLY, current_override=None, history=h(0, 14))
    assert v["action"] == "none"
    assert v["effective"] == NIGHTLY


def test_a_quiet_weekly_jurisdiction_is_left_alone_and_not_demoted_further():
    v = cadence_review("ut", floor=WEEKLY, current_override=None, history=h(40, 0, 0, 0, 0))
    assert v["action"] == "none"
    assert v["effective"] == WEEKLY
    # Already at its floor, so there is nothing to advise demoting.
    assert v["demotion_advice"] is False


def test_a_quiet_escalated_jurisdiction_produces_advice_but_no_action():
    """The asymmetry, end to end: the verdict says demotion would be defensible and still
    changes nothing. Acting on it is a human edit to sync_day."""
    v = cadence_review(
        "fl", floor=WEEKLY, current_override=NIGHTLY, history=h(40, 30, 0, 0, 0, 0)
    )
    assert v["demotion_advice"] is True
    assert v["action"] == "none"
    assert v["effective"] == NIGHTLY, "advice must not change the effective cadence"


def test_arizona_escalated_and_wedged_produces_no_demotion_advice():
    v = cadence_review("az", floor=WEEKLY, current_override=NIGHTLY, history=h(*([0] * 10)))
    assert v["demotion_advice"] is False
    assert v["effective"] == NIGHTLY


# -- storage ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cadence_round_trips_through_redis():
    from ddp_sync.services.redis_store import RedisStore

    s = RedisStore.__new__(RedisStore)
    s._client = AsyncMock()
    s._client.get.return_value = b"nightly"
    assert await s.get_scrape_cadence("fl") == NIGHTLY
    assert await s.set_scrape_cadence("fl", NIGHTLY) is True
    s._client.set.assert_awaited_once()
    assert s._client.set.call_args.args[0] == "ddp:scrape_cadence:fl"


@pytest.mark.asyncio
async def test_a_redis_failure_reads_as_no_override_not_as_weekly():
    from ddp_sync.services.redis_store import RedisStore

    s = RedisStore.__new__(RedisStore)
    s._client = AsyncMock()
    s._client.get.side_effect = RuntimeError("connection refused")
    assert await s.get_scrape_cadence("fl") is None
    # ...which the caller floors at the configured cadence.
    assert resolve_cadence(NIGHTLY, await s.get_scrape_cadence("fl")) == NIGHTLY


@pytest.mark.asyncio
async def test_a_failed_write_is_reported_so_the_caller_can_stop_re_deciding():
    from ddp_sync.services.redis_store import RedisStore

    s = RedisStore.__new__(RedisStore)
    s._client = AsyncMock()
    s._client.set.side_effect = RuntimeError("read only replica")
    assert await s.set_scrape_cadence("fl", NIGHTLY) is False


@pytest.mark.asyncio
async def test_no_redis_client_at_all_is_not_an_error():
    from ddp_sync.services.redis_store import RedisStore

    s = RedisStore.__new__(RedisStore)
    s._client = None
    assert await s.get_scrape_cadence("fl") is None
    assert await s.set_scrape_cadence("fl", NIGHTLY) is False


# -- pm-review round 1: the exclusion was declarative, and writes were unvalidated -------------


def test_an_excluded_jurisdiction_is_never_escalated():
    """MI must never be escalated: OPEN-53 established that more traffic against a WAF worsens a
    block. The config key existed but nothing read it, which pm-review correctly called a
    declaration rather than a rule. Enforced at the decision point so no caller can forget."""
    hist = h(0, 25)  # qualifying activity — would escalate any other jurisdiction
    v = cadence_review("mi", floor=WEEKLY, current_override=None, history=hist, excluded=["mi"])
    assert v["excluded"] is True
    assert v["action"] == "none"
    assert v["effective"] == WEEKLY, "an excluded jurisdiction must stay at its floor"

    # ...and the same history DOES escalate a jurisdiction that is not excluded, so the test is
    # proving the exclusion rather than a quiet history.
    other = cadence_review("va", floor=WEEKLY, current_override=None, history=hist, excluded=["mi"])
    assert other["action"] == "escalate"


def test_exclusion_does_not_strip_an_override_already_in_force():
    """Excluding a jurisdiction stops it being escalated; it does not demote one that already is.
    Demotion is never automatic, and that includes via the exclusion list."""
    v = cadence_review(
        "mi", floor=WEEKLY, current_override=NIGHTLY, history=h(0, 30), excluded=["mi"]
    )
    assert v["effective"] == NIGHTLY
    assert v["action"] == "none"


def test_exclusion_accepts_a_set_or_a_list():
    hist = h(0, 25)
    for ex in (["mi"], {"mi"}, ("mi",)):
        assert cadence_review("mi", WEEKLY, None, hist, excluded=ex)["excluded"] is True


def test_no_exclusions_configured_is_not_an_error():
    assert cadence_review("va", WEEKLY, None, h(0, 25), excluded=None)["action"] == "escalate"


@pytest.mark.asyncio
async def test_an_unrecognised_cadence_is_refused_at_the_write():
    """resolve_cadence would floor junk anyway, so this is not a safety fix -- it is so that a
    successful write means something and a typo surfaces where it happened."""
    from ddp_sync.services.redis_store import RedisStore

    s = RedisStore.__new__(RedisStore)
    s._client = AsyncMock()
    for bad in ("hourly", "WEEKLY", "", "daily"):
        assert await s.set_scrape_cadence("fl", bad) is False
    s._client.set.assert_not_awaited()
