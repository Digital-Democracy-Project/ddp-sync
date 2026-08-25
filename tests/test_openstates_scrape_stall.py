"""Tests for OPEN-155: end a scrape that stopped making progress, not one that is merely slow.

Wall-clock ceilings get both cases wrong. They kill a healthy run that is slow — MA's full walk
measured 5.7h and 8.21h on consecutive attempts, a 44% spread caused entirely by
malegislature.gov's own response time — and they tolerate a wedged run right up until the
ceiling expires, which for FL is sixteen hours of nothing.

The signal that should end a run is the absence of new bill files in the jurisdiction's own data
directory: the actual work product, requiring no cooperation from the scraper.

These drive the real `_run_with_group_kill` against short-lived shell processes and a real
temporary directory. Nothing is mocked except the clock budget — the watchdog thread, the
process-group kill and the directory polling are all the production code paths.

TIMING DISCIPLINE IN THIS FILE (OPEN-157) — the rule, then the snapshot.

Every test here races a `sleep` against a stall window, so the ratio between the two decides
whether the test is deterministic or a coin flip. THE RULE, which is what to actually apply:

    ratio = stall_seconds / sleep
      <= 0.5   the window fires first, deterministically. Assert the outcome.
      >= 2.0   the process exits first, deterministically. Assert the outcome.
      between  it is a coin flip. Either separate the two events, or assert an INVARIANT that
               holds whichever side wins. Never tune the numbers until it passes on this machine.

Snapshot of an audit run 2026-08-25, which will drift as tests are added and is not the rule:
every test then in the file sat at 0.03–0.17 or 5–16x except `..._is_never_both` at 0.95, which
is deliberately racy and asserts an invariant, and
`test_setting_the_stall_window_to_zero_disables_detection`, which starts no watchdog at all
(`stall_seconds=0`) and so has no race to lose regardless of timing.

A previous version of the boundary test asserted the outcome of the 0.95 race and failed roughly
four runs in five, which is what prompted writing this down.
"""
import os
import subprocess
import tempfile
import threading
import time
import uuid

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    SCRAPE_STALL_SECONDS,
    _run_with_group_kill,
)
import ddp_sync.pipelines.openstates_scrape as os_scrape


@pytest.fixture
def fast_poll(monkeypatch):
    """Poll every 0.1s so a stall is observable in a test rather than in 30-second steps."""
    monkeypatch.setattr(os_scrape, "_STALL_POLL_SECONDS", 0.1)


@pytest.fixture
def datadir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def test_a_process_making_no_progress_is_killed(fast_poll, datadir):
    # Sleeps far longer than the stall window and never writes a file: the definition of wedged.
    rc, _out, _err, timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", "sleep 30"], dict(os.environ), timeout=25,
        progress_dir=datadir, stall_seconds=1,
    )
    assert stalled is True
    assert timed_out is False, "a stall must be reported as a stall, not as a ceiling hit"


def test_a_process_still_producing_files_is_left_alone(fast_poll, datadir):
    # Writes a file every 0.2s for ~2s against a 1s stall window. Never idle long enough to
    # trip, so a correct watchdog must not kill it — this is the false-positive direction, and
    # getting it wrong would kill healthy slow scrapes, which is the bug being fixed.
    script = (
        f'for i in $(seq 1 10); do touch "{datadir}/bill_$i.json"; sleep 0.2; done'
    )
    rc, _out, _err, timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", script], dict(os.environ), timeout=25,
        progress_dir=datadir, stall_seconds=1,
    )
    assert stalled is False
    assert timed_out is False
    assert rc == 0
    assert len(os.listdir(datadir)) == 10


def test_a_short_run_that_never_writes_anything_is_not_killed(fast_poll, datadir):
    """A genuine no-op run produces no files at all and must still be allowed to finish.

    VA does exactly this in ~90s. The stall window only has to be longer than such a run, which
    at 45 minutes it comfortably is — but the mechanism must not depend on that margin being
    lucky, so assert it directly.
    """
    rc, _out, _err, timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", "sleep 0.3"], dict(os.environ), timeout=25,
        progress_dir=datadir, stall_seconds=5,
    )
    assert stalled is False
    assert rc == 0


def test_a_missing_progress_directory_does_not_kill_a_healthy_run(fast_poll, datadir):
    """openstates-core wipes and recreates the data directory at scrape start.

    So the directory is legitimately absent for a moment at the very beginning. Treating absent
    as "no progress" would be fine, but treating it as an *error* — or counting it as a change
    every poll — would either kill good runs or never kill bad ones.
    """
    missing = os.path.join(datadir, "does-not-exist")
    rc, _out, _err, timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", "sleep 0.3"], dict(os.environ), timeout=25,
        progress_dir=missing, stall_seconds=5,
    )
    assert stalled is False
    assert rc == 0


def test_no_watchdog_runs_when_progress_dir_is_omitted(datadir):
    """The two non-scrape callers (patch refresh, people refresh) pass nothing extra.

    They must behave exactly as before — no thread, no polling, no possibility of a stall kill.
    """
    before = threading.active_count()
    rc, _out, _err, timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", "sleep 0.2"], dict(os.environ), timeout=25,
    )
    assert stalled is False
    assert rc == 0
    assert threading.active_count() <= before, "no watchdog thread should have been left running"


def test_the_watchdog_thread_does_not_outlive_the_call(fast_poll, datadir):
    # A daemon thread that leaks per scrape would accumulate one per run for the life of the
    # process. The `finally` block joins it; this proves the join actually happens.
    before = threading.active_count()
    _run_with_group_kill(
        ["/bin/bash", "-c", "sleep 0.3"], dict(os.environ), timeout=25,
        progress_dir=datadir, stall_seconds=5,
    )
    time.sleep(0.3)
    assert threading.active_count() <= before


def test_the_whole_process_group_dies_on_a_stall(fast_poll, datadir):
    """A stall kill must take the grandchildren too.

    This is the same reason _run_with_group_kill exists at all: run-scrape.sh spawns os-update,
    and killing only the direct child leaves the scraper running — still hitting the site, and
    now unsupervised. Verified by having the grandchild write a file after the kill should have
    happened; if it survives, the file appears.
    """
    marker = os.path.join(datadir, "grandchild-survived")
    # The inner bash is a grandchild of the process we start.
    script = f'bash -c \'sleep 3; touch "{marker}"\' & wait'
    _rc, _out, _err, _timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", script], dict(os.environ), timeout=25,
        progress_dir=os.path.join(datadir, "nope"), stall_seconds=0.5,
    )
    assert stalled is True
    time.sleep(3.5)
    assert not os.path.exists(marker), "grandchild survived the stall kill"


def test_progress_files_match_the_openstates_core_layout(fast_poll, datadir):
    """The count is only a valid progress signal because of how openstates-core writes files.

    save_object() in openstates-core's scrape/base.py writes one flat file per object, named
    f"{obj._type}_{obj._id}.json", where _id is a fresh str(uuid.uuid1()) assigned per object.
    Two consequences, and the detector depends on both: a save never overwrites an existing
    file, and output never nests into subdirectories. So during a healthy run the top-level
    entry count strictly increases.

    Verified against the live checkout on 2026-08-25: _data/ma/ held 11,592 flat
    bill_<uuid>.json files and no subdirectories.

    This writes files in that exact shape rather than generic touch targets, so that if the
    upstream layout ever changes to rewriting-in-place or nesting, the assumption fails here
    rather than silently in production as a scrape that is killed while working.
    """
    ids = [str(uuid.uuid1()) for _ in range(6)]
    writes = " ".join(
        f'printf \'{{}}\' > "{datadir}/bill_{i}.json"; sleep 0.2;' for i in ids
    )
    rc, _out, _err, timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", writes], dict(os.environ), timeout=25,
        progress_dir=datadir, stall_seconds=1,
    )
    assert stalled is False, "distinct per-object filenames must read as continuous progress"
    assert rc == 0
    written = os.listdir(datadir)
    assert len(written) == len(set(ids)) == 6, "each save must add an entry, never replace one"
    assert all(os.path.isfile(os.path.join(datadir, f)) for f in written), "layout is flat"


def test_a_run_that_finishes_at_the_stall_boundary_is_never_both(fast_poll, datadir):
    """A no-op run writes nothing at all, so it can reach the stall window and exit cleanly.

    The watchdog must not label that a failure. A successful scrape reported as stalled is the
    same class of bug this whole ticket family is about — the run says one thing and the record
    says another — only pointing the other way.

    Deliberately timed so the process exit and the stall deadline land together, because that
    interleaving is what `process.poll()` in the watchdog exists to handle. Which of the two
    wins is a genuine race and **not** something to assert: an earlier version of this test
    pinned `rc == 0 and stalled is False` and failed roughly four runs in five, because it was
    asserting the outcome of a coin flip rather than the guard.

    What must hold either way is the invariant: a run may be killed as stalled, or it may exit
    successfully, but it may never be reported as both. That is exactly the contradiction the
    liveness check prevents, and it holds whichever way the race falls.

    Scope, so this is not over-read (pm-review's point): a pass here does NOT prove the
    `process.poll()` branch executed on that particular run, because the other side of the race
    may have won. It proves the invariant survives the boundary. Deterministic coverage of "a
    process that exits well before the window is never called stalled" is
    `test_a_short_run_that_never_writes_anything_is_not_killed`, which separates the two events
    by 16x and asserts the outcome directly.
    """
    rc, _out, _err, timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", "sleep 1"], dict(os.environ), timeout=25,
        progress_dir=datadir, stall_seconds=0.95,
    )
    assert timed_out is False, "nothing here should reach the 25s ceiling"

    # Exactly two outcomes are legitimate. Enumerating them rather than only forbidding the
    # contradictory one closes a vacuous pass pm-review spotted: `rc != 0 and not stalled` would
    # mean `sleep 1` failed for some unrelated reason -- a missing shell, a broken fixture -- and
    # the old assertion would have sailed straight past it.
    if stalled:
        # A stall kill is SIGKILL to the process group, so the direct child's status is -9.
        # Asserting the signal rather than just "nonzero" is what makes this branch prove a
        # group kill happened rather than merely that something went wrong.
        assert rc == -9, f"a stall kill must show SIGKILL, got rc={rc}"
    else:
        assert rc == 0, (
            f"not stalled, so the process must have exited cleanly on its own, got rc={rc} — "
            "a nonzero code here means the subprocess failed for an unrelated reason and this "
            "test would otherwise pass without exercising anything"
        )


def test_setting_the_stall_window_to_zero_disables_detection(fast_poll, datadir):
    """0 is the documented escape hatch: falls back to the SCRAPE_TIMEOUT_S ceilings alone.

    That is the pre-OPEN-155 behaviour, reachable by env var without a deploy, if stall
    detection ever starts killing healthy runs.
    """
    before = threading.active_count()
    rc, _out, _err, timed_out, stalled = _run_with_group_kill(
        ["/bin/bash", "-c", "sleep 0.5"], dict(os.environ), timeout=25,
        progress_dir=datadir, stall_seconds=0,
    )
    assert stalled is False
    assert rc == 0
    assert threading.active_count() <= before, "0 must start no watchdog at all"


def test_the_stall_window_is_configurable_and_sane():
    # Overridable for an operator who needs to tune it without a deploy, but the shipped default
    # must be long enough that no legitimate gap between two bills reaches it. MI is the slowest
    # jurisdiction at a hard 10 requests/minute, where a bill costs well under a minute.
    assert SCRAPE_STALL_SECONDS == 45 * 60
    assert SCRAPE_STALL_SECONDS > 10 * 60, "too tight — MI's rate limit alone would trip it"
