"""Tests for OPEN-162's periodic forced full walk.

The mechanism is one file delete: `run-scrape.sh` full-walks whenever
`logs/last-run/<key>.ts` is absent, so forcing a walk means removing that marker
before the scrape starts. Nothing in production ever removed one, which is why MI
had exactly one full walk ever.

Two things are worth pinning hardest, and neither is "does it delete the file":

  * **It must stay off for every jurisdiction not listed.** Promoting an
    unlisted jurisdiction to a full walk is not a cosmetic bug — for MI it is
    ~3,924 requests at a 10/min WAF cap against the fleet's most hostile site.
  * **It must not fire every run once it is due.** The stamp has to be written
    when the walk is triggered, or a jurisdiction past its interval would
    full-walk on *every* subsequent scrape rather than once a month.
"""

from __future__ import annotations

import os
import time

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    SCRAPE_TIMEOUT_S,
    _full_walk_eligible,
    _maybe_force_full_walk,
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

    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is True
    assert not ts.exists(), "the marker must be gone — its absence IS the full walk"


def test_an_overdue_jurisdiction_forces_a_walk(tmp_path):
    root = _root_with_marker(tmp_path, stamp_age_days=31)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is True
    assert not (tmp_path / "logs" / "last-run" / "mi.ts").exists()


def test_a_recent_walk_does_not_force_another(tmp_path):
    """The whole point of the interval. MI's weekly incremental runs must stay
    incremental for the other ~29 days."""
    root = _root_with_marker(tmp_path, stamp_age_days=3)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is False
    assert (tmp_path / "logs" / "last-run" / "mi.ts").exists(), "marker must survive"


def test_triggering_writes_the_stamp_so_it_does_not_fire_every_run(tmp_path):
    """The regression that would turn a monthly walk into a permanent one: if the
    stamp is not written when the walk is triggered, the jurisdiction stays overdue
    and every subsequent scrape is promoted to a full walk — ~3,924 WAF-capped
    requests, weekly, forever."""
    root = _root_with_marker(tmp_path)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is True

    # Second call, immediately after: now recent, so no.
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is False


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

    assert _maybe_force_full_walk("usa", "usa session=119", cfg, str(tmp_path)) is True
    assert not (last_run / "usa_session_119.ts").exists()
    assert (last_run / "usa.ts").exists(), "the bare marker is a different run"


def test_a_missing_marker_is_success_not_failure(tmp_path):
    """An absent marker already means "full walk", so there is nothing to fix and
    nothing to complain about."""
    root = _root_with_marker(tmp_path, ts=False)
    assert _maybe_force_full_walk("mi", "mi", MI_CFG, root) is True


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
