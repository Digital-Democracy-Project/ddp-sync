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
"""
import os
import subprocess
import tempfile
import threading
import time

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


def test_the_stall_window_is_configurable_and_sane():
    # Overridable for an operator who needs to tune it without a deploy, but the shipped default
    # must be long enough that no legitimate gap between two bills reaches it. MI is the slowest
    # jurisdiction at a hard 10 requests/minute, where a bill costs well under a minute.
    assert SCRAPE_STALL_SECONDS == 45 * 60
    assert SCRAPE_STALL_SECONDS > 10 * 60, "too tight — MI's rate limit alone would trip it"
