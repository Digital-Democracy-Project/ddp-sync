"""Tests for OPEN-162's periodic forced full walk.

The mechanism is one file delete: `run-scrape.sh` full-walks whenever
`logs/last-run/<key>.ts` is absent, so forcing a walk means removing that marker
before the scrape starts. Nothing in production ever removed one, which is why MI
had exactly one full walk ever.

Two things are worth pinning hardest, and neither is "does it delete the file":

  * **It must stay off for every jurisdiction not listed.** Promoting an
    unlisted jurisdiction to a full walk is not a cosmetic bug — for MI it is
    ~3,924 requests at a 10/min WAF cap against the fleet's most hostile site.
  * **It must not fire every run once it is due**, or a monthly walk becomes a
    permanent one. That is what the `.fullwalk` stamp prevents.
  * **But the stamp must mean "a walk completed", not "a walk was requested."**
    run-scrape.sh takes the per-state scrape lock itself, after this module has
    already cleared the marker, and exits immediately if another run holds it.
    Stamping at decision time would record a backstop that never ran and leave MI
    uncovered for another interval, silently — the exact shape of failure this
    ticket exists to remove. pm-review round 1 found this; it was right.
"""

from __future__ import annotations

import os
import time

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    SCRAPE_TIMEOUT_S,
    _full_walk_eligible,
    _maybe_force_full_walk,
    _record_full_walk,
)

MI_CFG = {"full_walk": {"enabled": True, "jurisdictions": ["mi"], "interval_days": 30}}


# --- the gate --------------------------------------------------------------------------

def test_listed_jurisdiction_is_eligible():
    assert _full_walk_eligible("mi", MI_CFG) == (True, 30)


@pytest.mark.parametrize("jurisdiction", ["va", "ut", "fl", "wa", "usa", "ma", "az"])
def test_unlisted_jurisdictions_are_not_eligible(jurisdiction):
    """A full walk is expensive everywhere and dangerous on a rate-capped site.
    It must never leak to a jurisdiction nobody opted in."""
    assert _full_walk_eligible(jurisdiction, MI_CFG)[0] is False


def test_disabled_flag_overrides_the_list():
    """The documented rollback."""
    cfg = {"full_walk": {"enabled": False, "jurisdictions": ["mi"], "interval_days": 30}}
    assert _full_walk_eligible("mi", cfg)[0] is False


@pytest.mark.parametrize(
    "cfg",
    [None, {}, {"full_walk": {}}, {"full_walk": {"enabled": True}}],
)
def test_absent_or_incomplete_config_is_off(cfg):
    """Absent config must be off. Every caller that passes no config at all — which
    is most of them — has to be completely unaffected by this feature existing."""
    assert _full_walk_eligible("mi", cfg)[0] is False


# --- the mechanism ---------------------------------------------------------------------

def _root_with_marker(tmp_path, key="mi", ts=True, stamp_age_days=None):
    last_run = tmp_path / "logs" / "last-run"
    last_run.mkdir(parents=True)
    if ts:
        (last_run / f"{key}.ts").write_text("2026-08-24T03:51:48")
    if stamp_age_days is not None:
        stamp = last_run / f"{key}.fullwalk"
        stamp.write_text("2026-07-01T00:00:00")
        old = time.time() - stamp_age_days * 86400
        os.utime(stamp, (old, old))
    return str(tmp_path)


def test_a_first_ever_run_forces_a_walk_and_removes_the_marker(tmp_path):
    """No stamp means this jurisdiction has never had a forced walk — which is
    exactly MI's real state, and the reason this ticket exists."""
    root = _root_with_marker(tmp_path)
    ts = tmp_path / "logs" / "last-run" / "mi.ts"
    assert ts.exists()

    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root)
    assert not ts.exists(), "the marker must be gone — its absence IS the full walk"


def test_an_overdue_jurisdiction_forces_a_walk(tmp_path):
    root = _root_with_marker(tmp_path, stamp_age_days=31)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root)
    assert not (tmp_path / "logs" / "last-run" / "mi.ts").exists()


def test_a_recent_walk_does_not_force_another(tmp_path):
    """The whole point of the interval. MI's weekly incremental runs must stay
    incremental for the other ~29 days."""
    root = _root_with_marker(tmp_path, stamp_age_days=3)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is False
    assert (tmp_path / "logs" / "last-run" / "mi.ts").exists(), "marker must survive"


def test_a_completed_walk_stops_it_firing_again(tmp_path):
    """Once the run that did the walk succeeds, the stamp lands and the
    jurisdiction stops being due — otherwise a monthly walk becomes a permanent
    one: ~3,924 WAF-capped requests on every scrape, forever."""
    root = _root_with_marker(tmp_path)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root)

    _record_full_walk("mi", root)          # what the success path does

    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is False


# --- pm-review round 1: the stamp must mean "completed", not "attempted" ----------------

def test_forcing_a_walk_does_not_by_itself_stamp_it_done(tmp_path):
    """The highest-severity finding of round 1, and it was right.

    run-scrape.sh takes the per-state scrape lock *itself*, after this module has
    already decided — and if another run for the same jurisdiction holds it, the
    script exits EXIT_DO_NOT_RETRY immediately rather than waiting. Verified in
    run-scrape.sh directly, not assumed.

    So clearing the marker does not mean a walk happened. If the stamp were written
    at decision time, that lock clash would record a completed backstop for a run
    that scraped nothing, and MI would go another full interval uncovered —
    silently, which is the exact failure this ticket exists to remove."""
    root = _root_with_marker(tmp_path)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root)

    stamp = tmp_path / "logs" / "last-run" / "mi.fullwalk"
    assert not stamp.exists(), "no stamp until a run actually completes"

    # Still due, because nothing has completed yet.
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root)


def test_recording_a_walk_is_what_makes_it_not_due(tmp_path):
    root = _root_with_marker(tmp_path)
    _record_full_walk("mi", root)
    assert (tmp_path / "logs" / "last-run" / "mi.fullwalk").exists()
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is False


def test_a_stamp_failure_never_fails_the_run(tmp_path):
    """The walk already succeeded by then. A missing stamp costs a repeated walk,
    which is wasteful; raising here would fail a scrape that actually worked."""
    _record_full_walk("mi", str(tmp_path / "nope" / "still-nope"))  # must not raise


# --- pm-review round 1: config that would cause a walk on every run --------------------

@pytest.mark.parametrize("bad", [0, -1, "", "thirty", None, [30]])
def test_a_bad_interval_fails_closed(bad):
    """A 0, a -1 or a stray string would make every run overdue — MI full-walking
    ~3,924 WAF-capped requests on every scrape from a one-character typo. And a
    raising int() would take down the scrape path itself, since that call sits
    outside the filesystem error handler."""
    cfg = {"full_walk": {"enabled": True, "jurisdictions": ["mi"], "interval_days": bad}}
    assert _full_walk_eligible("mi", cfg)[0] is False


def test_a_bad_interval_does_not_delete_the_marker(tmp_path):
    """Fail-closed asserted on the filesystem, not just the return value."""
    cfg = {"full_walk": {"enabled": True, "jurisdictions": ["mi"], "interval_days": 0}}
    root = _root_with_marker(tmp_path)
    assert _maybe_force_full_walk("mi", "mi", cfg, root) is False
    assert (tmp_path / "logs" / "last-run" / "mi.ts").exists()


def test_an_unlisted_jurisdiction_never_loses_its_marker(tmp_path):
    """The safety property, asserted on the filesystem rather than on the gate's
    return value."""
    root = _root_with_marker(tmp_path, key="fl")
    ts = tmp_path / "logs" / "last-run" / "fl.ts"

    assert _maybe_force_full_walk("fl", "fl", MI_CFG, root) is False
    assert ts.exists()


def test_a_session_label_targets_the_right_marker(tmp_path):
    """scrape_key_for() turns `usa session=119` into `usa_session_119`. If this
    cleared the bare `usa.ts` instead, it would force a walk of a run nobody
    configured and leave the real one untouched."""
    cfg = {"full_walk": {"enabled": True, "jurisdictions": ["usa"], "interval_days": 30}}
    last_run = tmp_path / "logs" / "last-run"
    last_run.mkdir(parents=True)
    (last_run / "usa_session_119.ts").write_text("x")
    (last_run / "usa.ts").write_text("x")

    assert _maybe_force_full_walk("usa", "usa session=119", cfg, str(tmp_path))
    assert not (last_run / "usa_session_119.ts").exists()
    assert (last_run / "usa.ts").exists(), "the bare marker is a different run"


def test_a_missing_marker_is_success_not_failure(tmp_path):
    """An absent marker already means "full walk", so there is nothing to fix and
    nothing to complain about."""
    root = _root_with_marker(tmp_path, ts=False)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root)


def test_a_filesystem_error_leaves_the_scrape_alone(tmp_path):
    """Best-effort by design: failing to force a full walk must never cost the
    ordinary incremental run that was going to happen anyway."""
    root = str(tmp_path / "does-not-exist")
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is False


# --- the ceiling this walk needs -------------------------------------------------------

def test_mi_has_a_ceiling_that_fits_a_full_walk():
    """A 6.5h walk under the 6h default would be killed from outside the process by
    subprocess.run(timeout=...), so run-scrape.sh's cleanup, marker writes and
    alerting never run — and openstates-core has already wiped the data directory.
    The entire walk's work would be discarded silently. MA hit exactly this trap
    before it got its own entry."""
    assert SCRAPE_TIMEOUT_S["mi"] > SCRAPE_TIMEOUT_S["default"]
    assert SCRAPE_TIMEOUT_S["mi"] >= int(6.5 * 3600 * 1.4), "needs real headroom over ~6.5h"


# ── OPEN-162 round 2: a failed forced walk must give the watermark back ────────
#
# The three behaviours that combine into the trap, each defensible alone:
#   1. _maybe_force_full_walk DELETES logs/last-run/<key>.ts -- that is the
#      mechanism; an absent marker is what makes run-scrape.sh full-walk.
#   2. run-scrape.sh writes .ts only at the end of a SUCCESSFUL run.
#   3. _record_full_walk stamps .fullwalk only on success (round 1's fix).
#
# So a forced walk that dies partway leaves no .ts and no stamp. The next
# scheduled run sees an absent marker, full-walks again, fails again -- and the
# jurisdiction never returns to incremental collection. Each cycle spends hours
# of WAF-capped requests to import a fraction of the corpus. The first live MI
# walk died at hour four, which is what surfaced this.


def test_forced_walk_captures_the_marker_it_deletes(tmp_path):
    """The payload carries the old contents, so a failure can restore them."""
    from ddp_sync.pipelines import openstates_scrape as m

    last_run = tmp_path / "logs" / "last-run"
    last_run.mkdir(parents=True)
    (last_run / "mi.ts").write_text("2026-08-24T03:51:48Z")

    forced = m._maybe_force_full_walk("mi", "mi", MI_CFG, str(tmp_path))

    assert forced, "should have forced a walk with no .fullwalk stamp present"
    assert not (last_run / "mi.ts").exists(), "the delete IS the mechanism"
    assert forced.previous_marker == "2026-08-24T03:51:48Z"


def test_restore_puts_back_the_exact_previous_value(tmp_path):
    """Never a fresh timestamp.

    Writing "now" would advance the watermark past bills nothing examined -- the
    same trap OPEN-163's sweep watermark had to avoid -- turning a failed
    backstop into silent data loss, which is worse than the loop being fixed.
    """
    from ddp_sync.pipelines import openstates_scrape as m

    last_run = tmp_path / "logs" / "last-run"
    last_run.mkdir(parents=True)
    ts = last_run / "mi.ts"
    ts.write_text("2026-08-24T03:51:48Z")

    forced = m._maybe_force_full_walk("mi", "mi", MI_CFG, str(tmp_path))
    m._restore_full_walk_marker(forced)

    assert ts.read_text() == "2026-08-24T03:51:48Z"


def test_restore_does_not_clobber_a_marker_the_run_itself_wrote(tmp_path):
    """A partial run that got far enough to record its own cutoff owns that value.

    Overwriting it with an older one would re-scrape work that did land.
    """
    from ddp_sync.pipelines import openstates_scrape as m

    last_run = tmp_path / "logs" / "last-run"
    last_run.mkdir(parents=True)
    ts = last_run / "mi.ts"
    ts.write_text("2026-08-24T03:51:48Z")

    forced = m._maybe_force_full_walk("mi", "mi", MI_CFG, str(tmp_path))
    ts.write_text("2026-08-26T09:00:00Z")  # the run wrote its own
    m._restore_full_walk_marker(forced)

    assert ts.read_text() == "2026-08-26T09:00:00Z", "the newer value must win"


def test_restore_is_a_no_op_when_there_was_no_marker(tmp_path):
    """A jurisdiction that never had a watermark must not gain a fabricated one."""
    from ddp_sync.pipelines import openstates_scrape as m

    last_run = tmp_path / "logs" / "last-run"
    last_run.mkdir(parents=True)

    forced = m._maybe_force_full_walk("mi", "mi", MI_CFG, str(tmp_path))
    m._restore_full_walk_marker(forced)

    assert not (last_run / "mi.ts").exists()


def test_the_loop_this_prevents(tmp_path):
    """The regression test proper: two consecutive failed walks.

    Without the restore, the second run finds no marker and full-walks again --
    forever. With it, the watermark is back and the next run is incremental.
    """
    from ddp_sync.pipelines import openstates_scrape as m

    last_run = tmp_path / "logs" / "last-run"
    last_run.mkdir(parents=True)
    ts = last_run / "mi.ts"
    ts.write_text("2026-08-24T03:51:48Z")

    # first forced walk -- fails, so restore runs
    first = m._maybe_force_full_walk("mi", "mi", MI_CFG, str(tmp_path))
    assert first
    m._restore_full_walk_marker(first)

    # the marker is back, so the jurisdiction is collecting incrementally again
    assert ts.exists(), "stranded in full-walk mode -- this is the bug"
    assert ts.read_text() == "2026-08-24T03:51:48Z"
