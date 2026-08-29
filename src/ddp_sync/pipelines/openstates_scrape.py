"""Pipeline that runs OpenStates jurisdiction scrapes on a schedule.

Managed by ddp-sync's APScheduler — replaces the ad-hoc run-all-scrapes.sh
launchd job that ran everything sequentially and caused Sunday jobs to be
skipped when Saturday's FL scrape overran into Sunday morning.

Each primary jurisdiction (FL, WA, USA) is an independent APScheduler job so
a long-running FL scrape does not delay WA or USA. Secondary states (VA, MI,
MA, UT, AZ) are fanned out concurrently inside a single Sunday job since they
use independent _data/{state}/ directories and don't conflict.

run-scrape.sh is invoked with SKIP_PATCHES=1; a dedicated patch_refresh job
at 01:00 UTC handles apply-local-patches.sh before the 02:00 UTC scrapes.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Any, NamedTuple

import requests
import structlog

from ddp_sync.services import scrapebot_client

logger = structlog.get_logger()

DEFAULT_OPENSTATES_ROOT = "/Users/agentsmith/Developer/repos/ddp-open-states"

# Per-jurisdiction scrape timeouts. FL 2026 regularly takes 12+ hours.
#
# OPEN-128: MA needs an explicit entry. It used to filter its bill list on the sponsors'
# ResponseDate, which never moves once a bill is filed, so a bill whose only change was an
# action was skipped every run and its actions went stale. That filter is gone
# (openstates-scrapers, ma/bills.py), so every MA run is now a full walk of ~11,500 bills.
#
# A real full walk measured 341 minutes -- 5.7h -- against the 6h `default` this previously fell
# under. A 19-minute margin is not a margin: the walk is bound by malegislature.gov's own ~2s
# per-page response time, which varies, so a slightly slow night would cross it. That matters
# more than a normal timeout because of HOW the kill lands. _run_scrape uses
# subprocess.run(timeout=...), which kills run-scrape.sh from outside its own process, so the
# script's cleanup, marker writes and on_failure() alerting never run -- and openstates-core has
# already wiped the jurisdiction's data directory at scrape start. A timeout kill therefore
# discards the entire run's work, which is exactly the failure OPEN-86's import-as-you-go sweep
# exists to bound. The comment on _alert_scrape_failure() below records that MA already came
# within an hour of this once, at ~5h, before the walk got longer.
#
# OPEN-155 changes what these values are FOR. They are no longer the primary guard against a
# run that has gone wrong -- SCRAPE_STALL_SECONDS is, and it catches a wedged run in 45 minutes
# instead of after hours of nothing. These are now a far backstop for the one case a stall
# detector cannot see: a run making steady, genuine progress that will simply never finish.
#
# That reframing resolves a real problem with MA's 12h. It was sized against a 5.7h walk, then
# the very next walk took 8.21h -- 1.46x headroom, under the 1.5x this module's own test
# asserts. Chasing that with a bigger number was always going to be re-litigated by the third
# measurement, because the spread comes from malegislature.gov's own response time and not from
# anything we control. MA now matches FL at 16h: ~2x the worst observed, and safe to be generous
# with precisely because a wedged run no longer waits for it.
#
# If MA ever genuinely approaches 16h of *productive* work, the answer is still not a bigger
# number: ma/bills.py's scrape() already takes a `scrape_chunk_number` (1-12) built to split
# this walk into shorter runs.
SCRAPE_TIMEOUT_S: dict[str, int] = {
    "fl": 16 * 3600,
    "ma": 16 * 3600,
    # OPEN-162: MI needs its own ceiling because it now gets a periodic FULL walk, and a full
    # walk is a completely different shape from its weekly incremental. ~3,924 bills against a
    # hard 10 requests/minute WAF cap is ~6.5h; the 6h default would have killed it just short
    # of finishing -- and a timeout kill is not an ordinary failure here. subprocess.run(timeout)
    # kills run-scrape.sh from outside its own process, so its cleanup, marker writes and
    # on_failure() alerting never run, and openstates-core has already wiped the data directory
    # at scrape start. The whole walk's work would be discarded, silently, which is exactly the
    # trap MA hit before it got its own entry.
    "mi": 10 * 3600,
    "wa": 8 * 3600,
    "usa": 4 * 3600,
    "default": 6 * 3600,
}


def _get_root(config: dict | None) -> str:
    return (config or {}).get("openstates_root", DEFAULT_OPENSTATES_ROOT)


def _alert_scrape_failure(label: str, error: str, duration_seconds: float) -> None:
    """Best-effort Slack + CAMS alert for a scrape that ddp-sync itself gave up on.

    run-scrape.sh has its own Slack/CAMS alerting (run-scrape.sh's on_failure()), but that
    only fires from *inside* the script's own process — a ddp-sync subprocess.run(timeout=...)
    kill (subprocess.TimeoutExpired) or any other exception here happens outside that process
    entirely, so run-scrape.sh never gets a chance to alert on it. Before this, a timeout-kill
    was 100% silent: logged at ERROR and written to a Redis flow-status key that health.py
    doesn't even surface. Found live 2026-08-01 while scoping a scrape auto-retry feature —
    MA's own ~5h failure was close enough to its 6h default timeout that a retry wrapper could
    plausibly hit this exact silent path. Never raises — same convention as every other
    alerting call site in this codebase (push_health_alert, run-scrape.sh's on_failure).
    """
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if token:
        channel = os.getenv("HEALTH_ALERT_SLACK_CHANNEL", "#automation-errors")
        text = (
            f":red_circle: *OpenStates scrape failed: {label}* — {error} "
            f"(after {duration_seconds:.0f}s) — check ddp-sync logs / scraper.log"
        )
        try:
            resp = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {token}"},
                json={"channel": channel, "text": text},
                timeout=15,
            )
            if not (resp.ok and resp.json().get("ok")):
                logger.error("openstates_scrape: Slack alert failed", response=resp.text[:200])
        except Exception as e:  # noqa: BLE001
            logger.error("openstates_scrape: Slack alert error", error=str(e))
    else:
        logger.warning("openstates_scrape: SLACK_BOT_TOKEN not set — cannot alert on scrape failure")

    cams_token = os.getenv("CAMS_API_TOKEN", "")
    if cams_token:
        cams_url = os.getenv("CAMS_BASE_URL", "http://localhost:8000")
        payload = {
            "v": 1,
            "service": "ddp-sync",
            "error_type": "ScrapeTimeoutOrSubprocessError",
            "message": f"scrape failed for {label}: {error} (after {duration_seconds:.0f}s)",
            "metadata": {"jurisdiction": label},
        }
        try:
            resp = requests.post(
                f"{cams_url}/api/v1/failures",
                headers={"Authorization": f"Bearer {cams_token}", "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10,
            )
            if not resp.ok:
                logger.error("openstates_scrape: CAMS report failed", status=resp.status_code)
        except Exception as e:  # noqa: BLE001
            logger.error("openstates_scrape: CAMS report error", error=str(e))


def _alert_sustained_block(jurisdiction: str, blocked_count: int, window: int) -> None:
    """Distinctly-worded Slack alert for a *sustained* blocking pattern (OPEN-22 AC2) --
    separate from _alert_scrape_failure's per-run failure alert, so a human notices the
    pattern (e.g. "MI has been blocked 3 of the last 4 weekly runs") without having to
    reconstruct it manually from past per-run alerts/logs. Same channel/token convention as
    _alert_scrape_failure -- no new secret/webhook for what's still #automation-errors.
    Never raises, same convention as every other alerting call site in this module.
    """
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        logger.warning(
            "openstates_scrape: SLACK_BOT_TOKEN not set — cannot alert on sustained block",
            jurisdiction=jurisdiction,
        )
        return
    channel = os.getenv("HEALTH_ALERT_SLACK_CHANNEL", "#automation-errors")
    text = (
        f":rotating_light: *{jurisdiction} has been blocked {blocked_count} of the last "
        f"{window} weekly runs* — likely a sustained reputation-blocking window, not a "
        f"one-off failure. See OPEN-22 / README.md."
    )
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": text},
            timeout=15,
        )
        if not (resp.ok and resp.json().get("ok")):
            logger.error(
                "openstates_scrape: sustained-block Slack alert failed", response=resp.text[:200]
            )
    except Exception as e:  # noqa: BLE001
        logger.error("openstates_scrape: sustained-block Slack alert error", error=str(e))


def _alert_quiet_jurisdiction(jurisdiction: str, quiet_window: int) -> None:
    """Slack alert for a jurisdiction that used to file bills and has stopped (OPEN-139).

    Worded to say what was measured rather than what it means, because the same signal has more
    than one cause: a stuck incremental cutoff (Arizona, 14 days), a broken change signal, a
    genuine recess, or an end of session. The alert's job is to get a human to look, not to
    diagnose. Same channel/token convention as the other two alert paths in this module, and
    never raises.
    """
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        logger.warning(
            "openstates_scrape: SLACK_BOT_TOKEN not set — cannot alert on quiet jurisdiction",
            jurisdiction=jurisdiction,
        )
        return
    channel = os.getenv("HEALTH_ALERT_SLACK_CHANNEL", "#automation-errors")
    text = (
        f":mag: *{jurisdiction} has imported no new bills in its last {quiet_window} runs* — "
        f"it was filing before that, so collection has stopped rather than the legislature "
        f"being quiet all along. Could be a stuck incremental cutoff (see AZ/OPEN-139), a "
        f"broken change signal, or a real recess. Worth a look either way."
    )
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": text},
            timeout=15,
        )
        if not (resp.ok and resp.json().get("ok")):
            logger.error(
                "openstates_scrape: quiet-jurisdiction Slack alert failed",
                response=resp.text[:200],
            )
    except Exception as e:  # noqa: BLE001
        logger.error("openstates_scrape: quiet-jurisdiction Slack alert error", error=str(e))


# Substring markers matched against a failed run's stderr tail to classify *why* it failed
# (OPEN-22 AC0b), so a sustained WAF-block pattern can be told apart from an unrelated network
# blip or code bug. Matched against this same GitHub org's own first-party ScrapeError message
# text (scrapers/mi/_waf_circuit_breaker.py's MAX_CONSECUTIVE_WAF_BLOCKS abort message, OPEN-18/
# OPEN-22 AC7) -- a stable, versioned string this codebase controls, not free-form third-party
# prose, so this is a narrow literal match rather than the kind of generic text-parsing
# reuse-before-reinvent.md warns against.
WAF_BLOCK_MARKERS = ("consecutive waf blocks detected", "waf block detected")


def classify_failure_reason(error: str, stderr_tail: str) -> str:
    """Classify a failed run's reason for OPEN-22's sustained-pattern escalation.

    Returns one of "waf_block", "timeout", "network_error", "nonzero_exit_other". Best-effort:
    good enough to distinguish the one thing OPEN-22's escalation looks for (a WAF-block-
    classified failure) from everything else, not an exhaustive failure taxonomy.
    """
    if error == "timeout":
        return "timeout"
    haystack = stderr_tail.lower()
    if any(marker in haystack for marker in WAF_BLOCK_MARKERS):
        return "waf_block"
    if error.startswith("exit_code_"):
        return "nonzero_exit_other"
    return "network_error"


def should_escalate(history: list[dict], window: int, threshold: int) -> bool:
    """Pure function (OPEN-22 AC1-3): has this jurisdiction been WAF-blocked in most/all of
    its last `window` recorded runs?

    Stateless by design -- recomputed from the rolling history every time, no separate streak
    counter to keep in sync. AC5's "a recovered run resets the streak cleanly" falls out for
    free: a success anywhere in the window changes the ratio automatically, with no second
    piece of state that could be forgotten. AC3's "a single bad run must not escalate" holds
    because `threshold` (>1) can't be reached by one failure alone.
    """
    recent = history[-window:]
    blocked = sum(1 for r in recent if r.get("failure_reason") == "waf_block")
    return blocked >= threshold


def scrape_key_for(label: str) -> str:
    """Reproduce run-scrape.sh's own SCRAPE_KEY, which names its marker files.

        SCRAPE_KEY=$(echo "${STATE}${SESSION_ARG:+ $SESSION_ARG}" | tr ' =' '__')

    `label` is this module's existing per-run label -- `f"{jurisdiction} {session_arg}"` when
    there is a session, else the bare jurisdiction -- which is character-for-character the string
    run-scrape.sh builds its key from, because it is assembled from the same two arguments this
    module passes to that script. So `fl session=2026` becomes `fl_session_2026`, and
    `usa session=119 chamber=lower` becomes `usa_session_119_chamber_lower`.

    This is a derivation duplicated across two languages, which is a real coupling and worth
    naming as one: if run-scrape.sh ever changes how it builds that key, this goes quietly
    wrong -- it would read a file that does not exist and report no filing figures rather than
    failing loudly. Pinned by tests against the actual filenames in
    ddp-open-states/logs/last-run so a change on either side breaks a test instead of a metric.
    The alternative -- having run-scrape.sh write to Redis itself -- would put a Redis client in
    a bash script, which is worse.
    """
    return label.replace(" ", "_").replace("=", "_")


def read_filing_counts(openstates_root: str, scrape_key: str) -> dict[str, Any] | None:
    """Read one run's filing figures from run-scrape.sh's `.imported` marker (OPEN-139).

    File format, one line -- see import-summary.sh in ddp-open-states for the full contract:

        ok:37:83:168:incremental      real figures for the last completed run
        unparsed::::incremental       the import ran, its report could not be read

    Returns None when there is nothing trustworthy to report: no file (the jurisdiction has not
    completed a run since that change shipped), an unreadable file, or a status of `unparsed`.
    None means "no measurement", which callers must treat differently from a measured zero --
    conflating them is exactly how a broken scraper comes to look like a quiet week.
    """
    path = os.path.join(openstates_root, "logs", "last-run", f"{scrape_key}.imported")
    try:
        with open(path) as f:
            line = f.read().strip()
    except OSError:
        return None
    parts = line.split(":")
    if len(parts) != 5 or parts[0] != "ok":
        return None
    try:
        return {
            "bills_new": int(parts[1]),
            "bills_updated": int(parts[2]),
            "bills_noop": int(parts[3]),
            "mode": parts[4],
        }
    except ValueError:
        return None


def should_alert_quiet(history: list[dict], window: int) -> bool:
    """Pure function (OPEN-139): has a jurisdiction that used to file bills stopped filing?

    Same stateless shape as should_escalate above -- recomputed from the rolling history every
    time, no separate streak counter to drift out of sync.

    Fires only when both halves are true:

      1. The last `window` MEASURED runs all imported zero new bills, and there are at least
         `window` of them. Runs with no measurement (`bills_new` absent, because the marker was
         missing or the import report was unparseable) are skipped rather than counted as zero.
      2. Somewhere further back in the retained history, this jurisdiction did import new bills.
         Without this, every out-of-session jurisdiction would alert every week all recess --
         and a jurisdiction we have never successfully collected from would alert forever
         instead of being investigated once.

    Deliberately NOT keyed on session state. is_in_session() has five confirmed false-negative
    paths (OPEN-138), including four states reported out-of-session for all 365 days of 2026, so
    gating on it would silently exempt whole jurisdictions from this check. "It used to file and
    has stopped" needs no calendar.

    WHAT THIS DOES AND DOES NOT DETECT -- state it plainly, because condition 2 is a real limit
    and not just an anti-noise nicety:

      detects      a jurisdiction observed filing within its retained history, now at zero for
                   `window` consecutive measured runs. This is the Arizona case.
      misses       a jurisdiction whose entire retained history is zeroes -- e.g. out of session
                   for longer than the history goes back -- that then breaks at the moment
                   filing should have resumed. It stays silent until one positive run is
                   observed. Accepted trade: the alternative alerts every recess, every week,
                   for every quiet jurisdiction, which trains people to ignore it.
      misses       under-collection. A jurisdiction filing 5 bills a week when it should be
                   filing 85 looks healthy here (that is OPEN-134, Michigan).

    Repeat behaviour: while the condition holds this returns True on every run, so a sustained
    quiet period alerts once per run rather than once per episode. That matches should_escalate
    above, which also re-fires while its window stays bad, and for a weekly job it is roughly one
    message a week. Left deliberately consistent with the existing check rather than inventing a
    second, different dedupe policy here.
    """
    measured = [r for r in history if r.get("bills_new") is not None]
    if len(measured) < window:
        return False
    recent = measured[-window:]
    if any(r["bills_new"] > 0 for r in recent):
        return False
    return any(r["bills_new"] > 0 for r in measured[:-window])


def _scrapebot_eligible(jurisdiction: str, config: dict | None) -> bool:
    """Is this jurisdiction opted into ScrapeBot cookie-mint fallback (PLAN-scrapebot.md §3.7)?

    Config-gated per jurisdiction under secondary.scrapebot_fallback in
    sync_schedule.yaml -- absent/disabled by default, so adding ScrapeBot never
    changes behavior for a jurisdiction that hasn't explicitly opted in.
    """
    fallback_cfg = (config or {}).get("secondary", {}).get("scrapebot_fallback", {})
    if not fallback_cfg.get("enabled", False):
        return False
    return jurisdiction in fallback_cfg.get("jurisdictions", [])


def _sweep_import_eligible(jurisdiction: str, config: dict | None) -> bool:
    """Is this jurisdiction opted into run-scrape.sh's import-as-you-go sweep (OPEN-86)?

    Without this, a scrape killed partway through loses everything it had already
    collected: run-scrape.sh imports into Postgres once, at the very end, and the
    next `--scrape` wipes $STATE_DATADIR before starting. That has cost real data
    (FL 213 + MA 473 bills, 2026-07-30) and cost it again in SYNC-35, where a
    ddp-sync restart orphaned AZ's run after it had successfully scraped all 895
    bills -- all of which were then wiped. The sweep imports throughout the run
    instead, so a death mid-scrape keeps whatever earlier sweeps already landed.

    The mechanism has been merged but dark since 2026-07-30: run-scrape.sh reads
    SWEEP_IMPORT_ENABLED and nothing anywhere set it. This is the gate that turns
    it on, deliberately per-jurisdiction so the VA/UT canary can run before FL/MA/
    USA -- PLAN-incremental-scraping.md's own rollout condition.

    Config lives under openstates_scrape.sweep_import in sync_schedule.yaml,
    mirroring secondary.scrapebot_fallback's shape above. Absent/disabled by
    default, so a jurisdiction that hasn't opted in is completely unaffected.
    Deliberately NOT a $STATE test in shell (OPEN-124): ddp-sync already resolves
    per-jurisdiction opt-ins from this YAML, and it is what invokes run-scrape.sh,
    so it is the right place to decide.
    """
    sweep_cfg = (config or {}).get("sweep_import", {})
    if not sweep_cfg.get("enabled", False):
        return False
    return jurisdiction in sweep_cfg.get("jurisdictions", [])


def _retry_eligible(jurisdiction: str, config: dict | None) -> bool:
    """Is this jurisdiction opted into bounded whole-run retry (OPEN-87)?

    These scrapes run weekly, so a run killed by a transient fault does not get
    another attempt for a week. That is not hypothetical: MA lost a run to a
    network timeout on 2026-08-01 that would have succeeded on a re-run. When a
    jurisdiction is eligible, _run_scrape() invokes run-scrape-retrying.sh
    instead of run-scrape.sh, which re-runs the scrape a bounded number of times
    and lets run-scrape.sh fire exactly one alert at the end rather than one per
    attempt.

    Two conditions, not one: opted in via scrape_retry.jurisdictions AND not
    named in scrape_retry.jurisdictions_excluded. The exclusion is not redundant with the
    allowlist -- it is what still holds when the staged rollout finishes and
    someone widens the allowlist to everything. MI must never be retried, because
    OPEN-53 established that a blind retry against a WAF worsens a block rather
    than recovering from it: every attempt is more traffic from a client the WAF
    already distrusts. MI is the jurisdiction that actually gets WAF-blocked --
    it is why secondary.scrapebot_fallback lists exactly ["mi"].

    Config lives under openstates_scrape.scrape_retry in sync_schedule.yaml
    -- named scrape_retry because that file already has an unrelated top-level
    `retry:` block for OpenStates API call retries. Mirroring
    secondary.scrapebot_fallback's shape above. Absent/disabled by default, so a
    jurisdiction that hasn't opted in is invoked exactly as it is today -- same
    script, same arguments, no wrapper process at all.

    Deliberately NOT a $STATE test in shell (OPEN-124): ddp-sync already resolves
    per-jurisdiction opt-ins from this YAML, and it is what invokes the scrape,
    so it is the right place to decide. This function is the single source of
    that decision -- the wrapper's own SCRAPE_RETRY_EXCLUDED_JURISDICTIONS is for
    a human invoking it by hand, and is deliberately not passed from here.
    """
    retry_cfg = (config or {}).get("scrape_retry", {})
    if not retry_cfg.get("enabled", False):
        return False
    if jurisdiction in retry_cfg.get("jurisdictions_excluded", []):
        return False
    return jurisdiction in retry_cfg.get("jurisdictions", [])


def _cloud_path_owns(jurisdiction: str, config: dict | None) -> bool:
    """Does the AWS Fargate path own this jurisdiction right now (OPEN-208)?

    Every phase of the scraper-execution migration rolls out per jurisdiction and rolls
    back per jurisdiction, and `run-scrape.sh`'s all-in-one collect-and-load keeps working
    the whole time (PLAN-scraper-execution-migration.md §3). That is deliberate -- but it
    does not by itself stop the SAME jurisdiction running on both paths at once, which
    doubles request rate against a source site (OPEN-19/21/22/23/52/53/54/106/132's
    resilience work exists precisely because that is expensive) and reproduces the
    duplicate-delivery hazard OPEN-203's import lock exists to survive.

    This is the "exactly one path owns a jurisdiction" gate, and it is a config check, not
    a new mechanism -- `sync_schedule.yaml` already decides per-jurisdiction eligibility in
    one place (OPEN-124/OPEN-140: _scrapebot_eligible/_sweep_import_eligible/_retry_eligible
    above), and this is one more thing it decides. Config lives under
    openstates_scrape.cloud_path, mirroring their shape. Absent/disabled by default, so
    every jurisdiction is mac-owned exactly as today until one is explicitly listed here --
    which as of this writing is none; OPEN-190 is what actually moves one.

    Checked inside _run_scrape() itself -- the one funnel every launch path already goes
    through (scheduled jobs, the manual-trigger endpoint via run_single_scrape_job(), and
    retry via run-scrape-retrying.sh, since that wrapper is chosen INSIDE this same
    function) -- rather than at each call site, so "refuses rather than running" is true
    for all of them by construction, not by remembering to add the check everywhere.

    Ownership transfers at the NEXT scheduled run, not at the moment this config changes:
    an in-flight run keeps going, uninterrupted, and OPEN-187's shared lock is what makes
    that safe even in the window where this file says one thing and a run started under
    the old answer is still finishing.
    """
    cloud_cfg = (config or {}).get("cloud_path", {})
    if not cloud_cfg.get("enabled", False):
        return False
    return jurisdiction in cloud_cfg.get("jurisdictions", [])


def _memory_backend_enabled(jurisdiction: str, config: dict | None) -> bool:
    """Should THIS mac-side run externalise its memory to S3 (OPEN-181) for this
    jurisdiction (OPEN-208's rollback requirement)?

    Deliberately a FLOOR, not tied 1:1 to _cloud_path_owns() -- a jurisdiction rolled back
    from cloud to mac ownership has no local watermark at all (the mac never ran it while
    it was cloud-owned), so its first run back needs to HYDRATE from the S3 store rather
    than silently fall back to "no local file found" and full-walk. Turning memory off the
    moment a jurisdiction leaves cloud_path.jurisdictions would be exactly the "rollback
    with the backend disabled, caught rather than silently re-collecting" failure this
    function exists to prevent.

    So there are two lists, not one, mirroring dynamic_cadence's own floor-not-default
    rule elsewhere in this file: `jurisdictions` is who cloud owns RIGHT NOW (rolls back by
    removing a name); `memory_backend_jurisdictions` is who has EVER been split (an
    operator adds a name here at the same time as adding it to `jurisdictions`, and never
    removes it on rollback -- only `jurisdictions` shrinks). A jurisdiction in either list
    gets the S3 backend.
    """
    cloud_cfg = (config or {}).get("cloud_path", {})
    if jurisdiction in cloud_cfg.get("jurisdictions", []):
        return True
    return jurisdiction in cloud_cfg.get("memory_backend_jurisdictions", [])


def _full_walk_eligible(jurisdiction: str, config: dict | None) -> tuple[bool, int]:
    """Is this jurisdiction opted into a periodic forced full walk (OPEN-162)?

    Returns (eligible, interval_days). Config lives under
    openstates_scrape.full_walk in sync_schedule.yaml, mirroring sweep_import's
    shape above -- per OPEN-124 this is orchestration (who runs a full walk and
    when), so it belongs in ddp-sync's YAML rather than as a $STATE test in bash.

    Exists because MI's incremental filter has a measured blind spot and no
    backstop. OPEN-158 found that a repeated last-action string hides real
    intervening actions on ~1.2% of bills -- adopted substitutes, committee
    reports, floor referrals, in one case 25 actions behind one repeated string.
    That rate is fine *if* something eventually re-reads every bill. Nothing did:
    run-scrape.sh full-walks only when the .ts marker is absent, and nothing in
    production ever removed one, so MI had exactly one full walk ever (when the
    marker was first absent) and would never have had another.
    """
    cfg = (config or {}).get("full_walk", {})
    if not cfg.get("enabled", False):
        return False, 0
    if jurisdiction not in cfg.get("jurisdictions", []):
        return False, 0

    # Fail closed on a bad interval rather than trusting the YAML (pm-review round 1).
    # This is worth four lines because of what the blast radius actually is: a `0`, a `-1`
    # or a stray string would make every eligible run overdue, so MI would full-walk on
    # *every* scrape -- ~3,924 requests at a hard 10/min cap against the fleet's most
    # WAF-sensitive site, weekly, from a one-character config typo. int() raising would
    # also take down the scrape path itself, which is outside the OSError handler below.
    try:
        interval_days = int(cfg.get("interval_days", 30))
    except (TypeError, ValueError):
        interval_days = 0
    if interval_days <= 0:
        logger.warning(
            "openstates_scrape: full_walk.interval_days is not a positive number, "
            "not forcing a full walk",
            jurisdiction=jurisdiction,
            interval_days=cfg.get("interval_days"),
        )
        return False, 0
    return True, interval_days


class _ForcedWalk(NamedTuple):
    """A forced full walk in flight, and what it displaced.

    Returned instead of a bare True so the failure path can put the marker back.
    Truthy either way, so `if forced_full_walk:` still reads naturally.
    """

    ts_path: str
    previous_marker: str | None


def _restore_full_walk_marker(forced: _ForcedWalk) -> None:
    """Put back the watermark a forced walk cleared, when that walk did not finish.

    Restores the EXACT previous contents, never a fresh timestamp. Writing "now"
    would advance the watermark past bills nothing examined -- the same trap
    OPEN-163's sweep watermark had to avoid -- turning a failed backstop into
    silent data loss, which is strictly worse than the repeated-full-walk loop
    this is fixing.

    Skipped when the marker could not be read before deletion, or when the run
    already wrote a new one (a partial run that got far enough to record its own
    cutoff owns that value; overwriting it with an older one would re-scrape work
    that did land).
    """
    if forced.previous_marker is None:
        return
    if os.path.exists(forced.ts_path):
        return
    try:
        with open(forced.ts_path, "w") as fh:
            fh.write(forced.previous_marker)
        logger.info(
            "openstates_scrape: forced full walk did not complete, watermark restored "
            "so the next run is incremental again (OPEN-162)",
            marker=forced.ts_path,
            restored_to=forced.previous_marker.strip()[:32],
        )
    except OSError as e:
        # Worth a warning rather than silence: the consequence is that the next
        # scheduled run full-walks again, which is exactly the loop this exists
        # to prevent.
        logger.warning(
            "openstates_scrape: could not restore the full-walk watermark; the next "
            "run will full-walk again",
            marker=forced.ts_path,
            error=str(e),
        )


def _maybe_force_full_walk(
    jurisdiction: str,
    label: str,
    config: dict | None,
    openstates_root: str,
) -> bool:
    """Clear the .ts marker when this jurisdiction's periodic full walk is due.

    run-scrape.sh already full-walks when `logs/last-run/<key>.ts` is absent, so
    forcing one needs no new scraper logic at all -- only a decision about when,
    and a file delete. That is the whole mechanism.

    **Done inline, immediately before the scrape this same call is about to
    start, rather than from a separate scheduled job.** That ordering is the
    design, not an implementation detail:

      * A separate timer could delete the marker while a scrape was in flight.
        run-scrape.sh reads it once at startup, so the delete would not affect
        the running scrape -- it would land on the *next* one, at a time nobody
        chose, and would also destroy the cutoff the in-flight run is about to
        write. Deciding here means the clear and the run that consumes it are
        the same event.
      * It cannot leave a "marker cleared but no scrape ran" state, which would
        silently promote whatever ran next -- possibly a different jurisdiction's
        operator-triggered run -- into an unplanned full walk.
      * Cadence becomes "the last full walk was >= N days ago" rather than a
        calendar date, so a missed or failed run does not skip the cycle: the
        next scrape simply still finds it due. That matches this fleet's standing
        preference for acting on observed state over a schedule.

    Due-ness is tracked in a sibling `<key>.fullwalk` stamp rather than inferred
    from `.ts`'s mtime, because `.ts` is rewritten by *every* run and so cannot
    distinguish "scraped recently" from "fully walked recently".

    Best-effort by design: any filesystem problem here logs and returns False,
    leaving an ordinary incremental run. A failure to force a full walk should
    never cost the regular scrape that was going to happen anyway.
    """
    eligible, interval_days = _full_walk_eligible(jurisdiction, config)
    if not eligible:
        return False

    key = scrape_key_for(label)
    last_run_dir = os.path.join(openstates_root, "logs", "last-run")
    ts_path = os.path.join(last_run_dir, f"{key}.ts")
    stamp_path = os.path.join(last_run_dir, f"{key}.fullwalk")

    # A root that does not exist is a misconfiguration, not a jurisdiction due for
    # a walk. Checked rather than left to makedirs() below, which would happily
    # build logs/last-run/ under a bogus path and report success -- littering a
    # stray tree and claiming a full walk was forced for a scrape that is about to
    # fail for an unrelated reason.
    if not os.path.isdir(openstates_root):
        logger.warning(
            "openstates_scrape: openstates_root does not exist, not forcing a full walk",
            jurisdiction=jurisdiction,
            openstates_root=openstates_root,
        )
        return False

    try:
        now = time.time()
        due_after = interval_days * 86400
        if os.path.exists(stamp_path):
            age = now - os.path.getmtime(stamp_path)
            if age < due_after:
                return False
            last_walk = f"{age / 86400:.1f} days ago"
        else:
            # No stamp: either this jurisdiction just opted in, or it has never
            # had a forced walk. Either way the next run is the right time.
            last_walk = "never"

        # Capture the marker's contents BEFORE deleting, so a failed walk can put
        # it back (see _restore_full_walk_marker). Without this, the three
        # behaviours here combine into a trap: the delete is the mechanism, but
        # run-scrape.sh writes .ts only on success and _record_full_walk stamps
        # only on success -- so a walk that dies partway leaves neither, the next
        # scheduled run sees an absent marker, full-walks again, fails again, and
        # MI never returns to incremental collection. Each cycle spends hours of
        # WAF-capped requests to import a fraction of the corpus. Found by review
        # after the first live walk died at hour four.
        previous_marker = None
        if os.path.exists(ts_path):
            try:
                with open(ts_path) as fh:
                    previous_marker = fh.read()
            except OSError:
                # Readable-or-not, the delete still has to happen for the walk to
                # be a walk. A marker we could not read is one we must not invent
                # a replacement for, so leave previous_marker None and let the
                # restore path skip it rather than writing a guessed timestamp.
                previous_marker = None
            os.remove(ts_path)

        logger.info(
            "openstates_scrape: forcing a full walk, periodic backstop is due (OPEN-162)",
            jurisdiction=jurisdiction,
            interval_days=interval_days,
            last_full_walk=last_walk,
            cleared_marker=ts_path,
            marker_saved=previous_marker is not None,
        )
        # Truthy, so existing `if forced_full_walk:` call sites keep working.
        return _ForcedWalk(ts_path=ts_path, previous_marker=previous_marker)
    except OSError as e:
        logger.warning(
            "openstates_scrape: could not force a full walk, proceeding incrementally",
            jurisdiction=jurisdiction,
            error=str(e),
        )
        return False


def _record_full_walk(label: str, openstates_root: str) -> None:
    """Stamp a forced full walk as DONE, once the run that did it has succeeded.

    Deliberately not written at the moment the marker is cleared (pm-review round
    1's highest-severity finding, and it was right). Clearing the marker does not
    mean a walk happened. The gap is real and reachable: run-scrape.sh takes the
    per-state scrape lock itself, *after* this module has already decided, and if
    another run for the same jurisdiction holds it the script exits
    EXIT_DO_NOT_RETRY immediately rather than waiting. Stamping up front would
    then record a completed backstop for a run that never scraped anything, and
    MI would go another interval with no full walk -- silently, which is the
    failure mode this whole ticket exists to remove.

    Writing it only on success also answers "attempted or completed?" the safer
    way. A failed, stalled or timed-out walk leaves no stamp, so the next run
    still finds one due. The cost of that choice is at worst a repeated full
    walk; the cost of the other choice is a missing one nobody notices.
    """
    key = scrape_key_for(label)
    last_run_dir = os.path.join(openstates_root, "logs", "last-run")
    try:
        os.makedirs(last_run_dir, exist_ok=True)
        with open(os.path.join(last_run_dir, f"{key}.fullwalk"), "w") as fh:
            fh.write(datetime.now(timezone.utc).isoformat())
        logger.info(
            "openstates_scrape: full walk completed, backstop recorded (OPEN-162)",
            jurisdiction=label,
        )
    except OSError as e:
        # The walk itself succeeded, so this must not fail the run. Worst case the
        # stamp is missing and the next run walks again -- wasteful, not wrong.
        logger.warning(
            "openstates_scrape: full walk succeeded but its stamp could not be written",
            jurisdiction=label,
            error=str(e),
        )


async def _maybe_preseed_scrapebot_cookies(
    jurisdiction: str,
    config: dict | None,
    openstates_root: str,
) -> None:
    """Proactively mint fresh WAF-passing cookies via ScrapeBot before scraping a
    jurisdiction opted into scrapebot_fallback (PLAN-scrapebot.md §3.7, revised
    2026-08-05), rather than reactively after a failure.

    Reactive seeding (mint only after a run classified its own failure as
    waf_block) never actually fired against a real production run: the detailed
    WafBlockDetected error only ever reaches scraper.log (redirected there inside
    run-scrape.sh's own scrape_attempt() tee pipeline), never run-scrape.sh's
    external stdout/stderr -- the only thing classify_failure_reason() can see.
    So a real MI WAF block always classified as nonzero_exit_other, never
    waf_block, and the reactive fallback silently never triggered. Proactive
    minting sidesteps that gap entirely: always start with fresh cookies, never
    depend on detecting the failure after the fact.

    Best-effort: a mint failure here must never block or fail the actual scrape
    that follows -- it just proceeds with whatever's already cached, or
    CookieProvider's own self-warm.
    """
    if not _scrapebot_eligible(jurisdiction, config):
        return
    try:
        mint_result = await scrapebot_client.dispatch_mint_cookies(jurisdiction)
        cache_path = scrapebot_client.cache_path_for(jurisdiction, openstates_root)
        scrapebot_client.write_cookie_cache(
            cache_path,
            cookies=mint_result["cookies"],
            user_agent=mint_result["user_agent"],
        )
        logger.info(
            "openstates_scrape: ScrapeBot pre-seeded fresh cookies before scrape",
            jurisdiction=jurisdiction,
            cache_path=cache_path,
        )
    except scrapebot_client.ScrapeBotDispatchError as e:
        logger.warning(
            "openstates_scrape: ScrapeBot pre-seed mint failed, proceeding with "
            "existing/self-warmed cookies",
            jurisdiction=jurisdiction,
            error=str(e),
        )


# OPEN-155: how long a scrape may go without producing any new bill file before it is treated
# as stalled. This is the signal that should end a run -- "is it still doing anything" -- rather
# than the wall-clock ceilings in SCRAPE_TIMEOUT_S, which get both cases wrong: they kill a
# healthy run that is merely slow (MA's full walk measured 5.7h and 8.2h on consecutive
# attempts, a 44% spread caused entirely by the site's own response time) and tolerate a wedged
# one right up until the ceiling expires.
#
# 45 minutes is deliberately far above any legitimate gap between two bills. The slowest
# jurisdiction is MI at a hard 10 requests/minute, where a bill costs a handful of requests --
# under a minute. MA sustains ~1,390 bills/hour. A 45-minute silence is not slowness.
#
# It has to clear the *startup* gap too, not just the between-bills gap: a run produces no files
# at all while it fetches and parses its search page, and for a no-op run it produces none ever.
# That is safe here because a no-op run also exits quickly (VA: 90s), so it is gone long before
# the timer matters.
#
# Setting SCRAPE_STALL_SECONDS=0 disables stall detection entirely and falls back to the
# SCRAPE_TIMEOUT_S ceilings alone -- i.e. exactly the behaviour before this change. That is the
# escape hatch if this ever starts killing healthy runs: it needs no deploy, only a restart.
SCRAPE_STALL_SECONDS = int(os.getenv("SCRAPE_STALL_SECONDS", str(45 * 60)))

# How often the watchdog looks. Cheap (one listdir) and not latency-critical -- the cost of a
# coarse poll is only that a stalled run is killed up to this long after the fact.
_STALL_POLL_SECONDS = 30


def _run_with_group_kill(
    cmd: list[str],
    env: dict,
    timeout: int,
    cwd: str | None = None,
    progress_dir: str | None = None,
    stall_seconds: int | None = None,
) -> tuple[int, bytes, bytes, bool, bool]:
    """Run cmd to completion or timeout, killing its whole process group on timeout.

    subprocess.run(timeout=...) only kills the direct child on TimeoutExpired — verified
    empirically 2026-08-01 that a grandchild process survives a plain subprocess.run
    timeout-kill even with start_new_session=True (that flag only makes the child its own
    process-group leader; nothing then targets that group). For run-scrape.sh specifically,
    the surviving grandchildren are exactly the processes actually doing work — os-update's
    scrape/import, the backgrounded sweep-import loop — which would otherwise keep running
    (and keep holding the import lock, keep writing into $STATE_DATADIR) after ddp-sync has
    already decided the run failed and moved on. Managing the Popen object directly here so a
    timeout can os.killpg() the whole group instead of just the one process we started.
    """
    process = subprocess.Popen(
        cmd,
        env=env,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    # OPEN-155: watch for a stall in a side thread rather than polling communicate() in a loop.
    # communicate() drains both pipes concurrently; replacing it with repeated wait() calls
    # would leave stdout/stderr unread and deadlock the child once a pipe buffer filled. So the
    # blocking call stays exactly as it was and the watchdog only observes and kills.
    stalled = False
    stop_watch = threading.Event()
    watchdog: threading.Thread | None = None

    if progress_dir and stall_seconds:
        def _watch() -> None:
            nonlocal stalled

            def _count() -> int:
                # The work product itself, not the log: a scraper can log continuously while
                # producing nothing. Counting entries is enough -- we only need "did this
                # change", never the actual number. A missing directory reads as -1 and simply
                # counts as "unchanged", which is right: openstates-core wipes and recreates it
                # at scrape start, so it is legitimately absent for a moment.
                #
                # A count is a sound progress signal only because of how openstates-core writes:
                # save_object() (scrape/base.py) writes one flat file per object named
                # f"{obj._type}_{obj._id}.json" where _id is a fresh uuid1() per object. So a
                # save never rewrites an existing file and never nests -- during a healthy run
                # this number only goes up. If that layout ever changes, this detector has to
                # change with it; test_progress_files_match_the_openstates_core_layout pins it.
                try:
                    return len(os.listdir(progress_dir))
                except OSError:
                    return -1

            last_count = _count()
            last_change = time.monotonic()
            while not stop_watch.wait(_STALL_POLL_SECONDS):
                now_count = _count()
                if now_count != last_count:
                    last_count, last_change = now_count, time.monotonic()
                    continue
                if time.monotonic() - last_change >= stall_seconds:
                    # The process may have finished on its own between the last poll and now --
                    # a no-op run writes nothing at all, so it can reach the stall window and
                    # exit cleanly in the same breath. Killing a corpse is harmless, but
                    # *labelling* that run stalled would report a successful scrape as a
                    # failure, so check liveness before claiming anything.
                    if process.poll() is not None:
                        return
                    stalled = True
                    logger.error(
                        "openstates_scrape: no new bills for the stall window — killing",
                        progress_dir=progress_dir,
                        stall_seconds=stall_seconds,
                        files_seen=now_count,
                    )
                    try:
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    except (ProcessLookupError, OSError):
                        pass  # already gone; communicate() below will reap it
                    return

        watchdog = threading.Thread(target=_watch, daemon=True)
        watchdog.start()

    try:
        stdout, stderr = process.communicate(timeout=timeout)
        # A stall kill lands here, not in the TimeoutExpired branch: the process group is dead,
        # so communicate() returns normally. `stalled` is what distinguishes the two.
        return process.returncode, stdout, stderr, False, stalled
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass  # process (and its group) already gone
        stdout, stderr = process.communicate()  # reap; collect whatever was already buffered
        return process.returncode, stdout, stderr, True, stalled
    finally:
        stop_watch.set()
        if watchdog is not None:
            watchdog.join(timeout=_STALL_POLL_SECONDS + 5)


async def _run_scrape(
    jurisdiction: str,
    session_arg: str | None,
    openstates_root: str,
    timeout_s: int | None = None,
    config: dict | None = None,
) -> dict[str, Any]:
    """Run run-scrape.sh for one jurisdiction off the event loop.

    Uses asyncio.to_thread so concurrent job coroutines (e.g. WA and USA
    both at 02:00 UTC) don't block each other in the event loop.
    SKIP_PATCHES=1 is set so apply-local-patches.sh is not re-run for every
    jurisdiction — the patch_refresh job owns that step.

    Pre-seeds ScrapeBot cookies first (PLAN-scrapebot.md §3.7) for any
    jurisdiction opted into scrapebot_fallback -- see
    _maybe_preseed_scrapebot_cookies()'s own docstring for why this is proactive
    now rather than reactive-after-failure. A no-op for every jurisdiction not
    opted in (config defaults to None, in which case it's always a no-op).
    """
    # OPEN-208: checked first, before anything else -- including ScrapeBot pre-seeding and
    # full-walk forcing below, both of which would be wasted work (and, for ScrapeBot, a
    # real mint against MI's WAF) for a jurisdiction this mac is not supposed to touch at
    # all. Every caller of _run_scrape() funnels through here, so this one check covers
    # scheduled jobs, the manual-trigger endpoint, and retry alike.
    if _cloud_path_owns(jurisdiction, config):
        logger.info(
            "openstates_scrape: skipping -- this jurisdiction is cloud-owned (OPEN-208)",
            jurisdiction=jurisdiction,
        )
        return {
            "success": True,
            "skipped": True,
            "reason": "cloud_path_owns",
            "jurisdiction": jurisdiction,
            "duration_seconds": 0.0,
        }

    await _maybe_preseed_scrapebot_cookies(jurisdiction, config, openstates_root)

    # OPEN-162: force a periodic full walk if one is due, by clearing the marker
    # run-scrape.sh reads. Deliberately here -- inside the one funnel every scrape
    # goes through, immediately before the subprocess that consumes it -- rather
    # than in a separate job; see _maybe_force_full_walk() for why that ordering
    # is what makes it safe against an in-flight run.
    # Same label shape scrape_key_for() and the reporting below both use, so the
    # marker this clears is the one run-scrape.sh will look for.
    forced_full_walk = _maybe_force_full_walk(
        jurisdiction,
        f"{jurisdiction} {session_arg}" if session_arg else jurisdiction,
        config,
        openstates_root,
    )
    # Set only on the success path below. Read by the finally: that restores the
    # watermark -- deliberately one place rather than a restore call at each of
    # the four failure exits, so a fifth exit added later cannot miss it.
    full_walk_completed = False

    # OPEN-87: an opted-in jurisdiction goes through the bounded-retry wrapper instead. The
    # wrapper takes the same arguments and calls run-scrape.sh itself, so nothing else about
    # this invocation changes. Choosing the script (rather than passing run-scrape.sh a "please
    # retry" flag) is what makes the opt-out absolute: a jurisdiction that is not eligible does
    # not get a wrapper process at all, so its invocation is byte-identical to today's.
    retry_enabled = _retry_eligible(jurisdiction, config)
    script = os.path.join(openstates_root, "run-scrape-retrying.sh") if retry_enabled else ""
    if retry_enabled and not os.path.exists(script):
        # These are two separate repos with two separate deploys, so this config can legitimately
        # be live before the wrapper script is. Falling back matters more than it looks: without
        # it, /bin/bash on a missing script exits 127, which is a *positive* return code -- and
        # the returncode != 0 branch below deliberately does not alert on those, on the grounds
        # that run-scrape.sh already alerted from inside the process. Here run-scrape.sh never
        # ran, so nobody would alert and an opted-in jurisdiction would fail silently every
        # cycle until someone noticed the missing data.
        logger.warning(
            "openstates_scrape: retry enabled but run-scrape-retrying.sh is missing — "
            "falling back to run-scrape.sh (deploy ddp-open-states first)",
            jurisdiction=jurisdiction,
            expected_path=script,
        )
        retry_enabled = False
    if not retry_enabled:
        script = os.path.join(openstates_root, "run-scrape.sh")
    cmd = ["/bin/bash", script, jurisdiction]
    if session_arg:
        cmd.append(session_arg)

    env = {**os.environ, "SKIP_PATCHES": "1"}
    if _memory_backend_enabled(jurisdiction, config):
        # scraper-memory.sh reads these; see _memory_backend_enabled() for why this is a
        # floor (once split, always memory-backed) rather than tied 1:1 to current cloud
        # ownership. "prod" matches cloud_collector.py's own MEMORY_PREFIX convention --
        # both sides have to agree on the namespace or they hydrate from empty air.
        env["SCRAPER_MEMORY_BACKEND"] = "s3"
        env["SCRAPER_MEMORY_PREFIX"] = "prod"
        logger.info(
            "openstates_scrape: S3-backed memory enabled for this run (OPEN-208)",
            jurisdiction=jurisdiction,
        )
    if _sweep_import_eligible(jurisdiction, config):
        # run-scrape.sh reads this; see _sweep_import_eligible() for why it exists and why the
        # opt-in lives in YAML rather than as a $STATE test in the script.
        env["SWEEP_IMPORT_ENABLED"] = "1"
        logger.info(
            "openstates_scrape: import-as-you-go sweep enabled for this run",
            jurisdiction=jurisdiction,
        )
    if retry_enabled:
        # run-scrape-retrying.sh reads these; see _retry_eligible() for why the opt-in lives in
        # YAML rather than as a $STATE test in the script. backoff_secs is a YAML list and the
        # wrapper wants a comma string, since it is bash 3.2 and has no arrays to receive.
        retry_cfg = (config or {}).get("scrape_retry", {})
        env["SCRAPE_RETRY_MAX_ATTEMPTS"] = str(retry_cfg.get("max_attempts", 3))
        env["SCRAPE_RETRY_BACKOFF_SECS"] = ",".join(
            str(s) for s in retry_cfg.get("backoff_secs", [900, 1800])
        )
        logger.info(
            "openstates_scrape: bounded retry enabled for this run",
            jurisdiction=jurisdiction,
            max_attempts=env["SCRAPE_RETRY_MAX_ATTEMPTS"],
            backoff_secs=env["SCRAPE_RETRY_BACKOFF_SECS"],
        )
    # OPEN-87, unresolved and deliberately not worked around here: this timeout applies to the
    # whole invocation, which for a retry-enabled jurisdiction means the wrapper AND all of its
    # attempts AND the backoff between them. A 3-attempt wrapper can therefore be killed partway
    # through attempt 2 no matter what budget it keeps for itself, and these values were sized
    # for a single attempt. The plan's original RETRY_TOTAL_BUDGET_SECS was the mechanism for
    # keeping the wrapper inside this ceiling, and the simplified scope dropped it -- so the
    # interaction is now unguarded by design rather than by oversight. Raising these values,
    # having the wrapper read the ceiling, or scheduling per-attempt instead of per-invocation
    # are all real options; picking one is an operator decision on OPEN-87, not something to
    # settle by quietly inventing a budget here.
    timeout = timeout_s or SCRAPE_TIMEOUT_S.get(jurisdiction, SCRAPE_TIMEOUT_S["default"])
    label = f"{jurisdiction} {session_arg}" if session_arg else jurisdiction

    logger.info(
        "openstates_scrape: starting",
        jurisdiction=jurisdiction,
        session=session_arg,
        timeout_s=timeout,
    )

    start = time.monotonic()
    try:
        # OPEN-155: the wall-clock `timeout` is now a far backstop, not the primary guard.
        # What ends a wedged run is the absence of new bill files in the jurisdiction's own data
        # directory -- the actual work product. openstates-core wipes and repopulates this
        # directory, so "count changed" is a direct read of whether the scrape is still doing
        # anything, and it needs no cooperation from the scraper.
        progress_dir = os.path.join(openstates_root, "openstates-scrapers", "_data", jurisdiction)
        returncode, _stdout, stderr, timed_out, stalled = await asyncio.to_thread(
            _run_with_group_kill, cmd, env, timeout, None, progress_dir, SCRAPE_STALL_SECONDS
        )
        duration = round(time.monotonic() - start, 1)

        # OPEN-155: checked before `timed_out` because it is the more specific diagnosis. A
        # stall says "this run wedged" -- actionable, and pointing at the scraper or the site. A
        # ceiling hit says only "this took longer than a number someone chose", which after this
        # change should be vanishingly rare and means the run was making steady progress the
        # whole time. Conflating them would throw away exactly the distinction this ticket
        # exists to draw.
        #
        # Alerts for the same reason a timeout does: the process group was killed from outside,
        # so run-scrape.sh's own ERR trap never ran and nothing else will say this happened.
        if stalled:
            logger.error(
                "openstates_scrape: stalled — no new bills for the stall window",
                jurisdiction=jurisdiction,
                session=session_arg,
                stall_seconds=SCRAPE_STALL_SECONDS,
                duration_seconds=duration,
            )
            _alert_scrape_failure(
                label,
                f"stalled — no new bill files for {SCRAPE_STALL_SECONDS}s "
                f"(killed after {duration:.0f}s; the wall-clock ceiling was {timeout}s)",
                duration,
            )
            return {
                "success": False,
                "error": "stalled",
                # Reuses the timeout classification deliberately: OPEN-22's sustained-pattern
                # escalation counts categories, and a stall and a ceiling hit are the same thing
                # to it -- "this jurisdiction keeps not finishing". Adding a category would make
                # a repeatedly-stalling jurisdiction look like two smaller unrelated problems.
                "failure_reason": classify_failure_reason("timeout", ""),
                "jurisdiction": label,
                "duration_seconds": duration,
            }

        if timed_out:
            logger.error(
                "openstates_scrape: timeout",
                jurisdiction=jurisdiction,
                session=session_arg,
                timeout_s=timeout,
                duration_seconds=duration,
            )
            # Whole process group killed above, before run-scrape.sh's own ERR trap ever got a
            # chance to run — it never alerted on this one. We're the only ones who know it
            # happened, so we're the only ones who can alert.
            _alert_scrape_failure(label, f"timed out after {timeout}s", duration)
            return {
                "success": False,
                "error": "timeout",
                "failure_reason": classify_failure_reason("timeout", ""),
                "jurisdiction": label,
                "duration_seconds": duration,
            }

        if returncode != 0:
            stderr_tail = (stderr or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_scrape: failed",
                jurisdiction=jurisdiction,
                session=session_arg,
                returncode=returncode,
                stderr_tail=stderr_tail,
                duration_seconds=duration,
            )
            if returncode < 0:
                # Negative returncode = killed by a signal that didn't originate from our own
                # timeout handling above (OOM killer, `kill` from an operator, another
                # supervisor). run-scrape.sh's `trap ... ERR` only fires on an ordinary command
                # failure inside the script, not on the script's own process receiving a
                # terminating signal — so unlike a plain nonzero exit, this one was never
                # self-alerted, and we're the only ones who saw it.
                _alert_scrape_failure(label, f"killed by signal {-returncode}", duration)
            # else (positive returncode): run-scrape.sh's own on_failure() already fired its
            # Slack/CAMS alert from inside the process before exiting nonzero — alerting again
            # here would double-page for the exact same failure.
            error = f"exit_code_{returncode}"
            return {
                "success": False,
                "error": error,
                "failure_reason": classify_failure_reason(error, stderr_tail),
                "jurisdiction": label,
                "duration_seconds": duration,
            }

        logger.info(
            "openstates_scrape: done",
            jurisdiction=jurisdiction,
            session=session_arg,
            duration_seconds=duration,
        )
        # OPEN-162: only now is the backstop genuinely done. Every failure path
        # above returns before this, so a walk that was forced but did not
        # complete leaves no stamp and stays due.
        if forced_full_walk:
            _record_full_walk(label, openstates_root)
        full_walk_completed = True
        return {"success": True, "jurisdiction": label, "duration_seconds": duration}

    except Exception as e:
        duration = round(time.monotonic() - start, 1)
        logger.error(
            "openstates_scrape: subprocess error",
            jurisdiction=jurisdiction,
            session=session_arg,
            error=str(e),
            duration_seconds=duration,
        )
        # Something failed before/while invoking the subprocess itself (e.g. the script or
        # openstates_root path doesn't exist) — run-scrape.sh never started, so it never had a
        # chance to alert either.
        _alert_scrape_failure(label, str(e), duration)
        error = str(e)
        return {
            "success": False,
            "error": error,
            "failure_reason": classify_failure_reason(error, ""),
            "jurisdiction": label,
            "duration_seconds": duration,
        }
    finally:
        # OPEN-162: a forced walk that did not finish must give the watermark back,
        # or MI is stranded in full-walk mode -- absent marker means full walk, and
        # nothing else writes .ts on a failure. In `finally` rather than at each
        # failure return so there is exactly one place this can be got wrong.
        if forced_full_walk and not full_walk_completed:
            _restore_full_walk_marker(forced_full_walk)


async def _write_flow_status(flow_key: str, status: dict) -> None:
    """Best-effort Redis flow_status write. Never raises."""
    try:
        from ddp_sync.services.redis_store import get_redis_store
        redis_store = get_redis_store()
        await redis_store.set_flow_status(flow_key, status)
    except Exception as e:
        logger.warning("openstates_scrape: redis write failed", flow=flow_key, error=str(e))


# Defaults if sync_schedule.yaml's secondary.escalation block is absent -- deliberately
# "most, not all" (3 of 4): a single bad run (AC3) must never escalate, but escalating only on
# a full 4-for-4 streak would delay detection by an extra week versus catching it at 3.
DEFAULT_ESCALATION_WINDOW = 4
DEFAULT_ESCALATION_THRESHOLD = 3

# OPEN-139: how many consecutive measured runs with zero new bills before saying a jurisdiction
# has gone quiet. 3 rather than 2 because these are weekly jobs and a real legislature can
# genuinely file nothing for a fortnight; 3 weeks of silence from a jurisdiction that was filing
# is worth a look. Unlike the escalation pair above there is no separate threshold -- this fires
# only on an unbroken run of zeroes, because a single non-zero week proves collection still works
# and there is nothing to report.
DEFAULT_QUIET_WINDOW = 3


async def _check_sustained_blocking(
    flow_key: str,
    jurisdictions: list[str],
    results: list[dict[str, Any]],
    config: dict | None,
) -> None:
    """Record this run's outcome into each jurisdiction's rolling history, then run two
    independent checks over that history.

    The first is OPEN-22's: escalate if a sustained WAF-blocking pattern shows up (AC0-AC5).
    The second is OPEN-139's: alert if a jurisdiction that used to file bills has stopped.

    They share the append because there is one record per run, and splitting it would mean two
    writers racing to extend the same Redis list. Best-effort throughout: a Redis hiccup here
    must never fail the scrape job itself, same convention as _write_flow_status.
    """
    escalation_cfg = (config or {}).get("secondary", {}).get("escalation", {})
    window = escalation_cfg.get("window_size", DEFAULT_ESCALATION_WINDOW)
    threshold = escalation_cfg.get("threshold", DEFAULT_ESCALATION_THRESHOLD)

    filing_cfg = (config or {}).get("filing_activity", {})
    filing_enabled = filing_cfg.get("enabled", False)
    quiet_window = filing_cfg.get("quiet_window", DEFAULT_QUIET_WINDOW)
    openstates_root = _get_root(config)

    try:
        from ddp_sync.services.redis_store import get_redis_store
        redis_store = get_redis_store()
    except Exception as e:
        logger.warning("openstates_scrape: redis unavailable for history tracking", error=str(e))
        return

    now = datetime.now(timezone.utc).isoformat()
    for jurisdiction, result in zip(jurisdictions, results):
        record = {
            "timestamp": now,
            "success": result["success"],
            "failure_reason": result.get("failure_reason"),
        }
        # OPEN-139: fold this run's filing figures into the same record. Only for a run that
        # succeeded -- a failed run's marker is either absent or left over from an earlier run,
        # and either way it is not a measurement of this one. Absent keys mean "not measured",
        # which should_alert_quiet skips rather than counting as a zero.
        if result["success"]:
            key = scrape_key_for(result.get("jurisdiction", jurisdiction))
            counts = read_filing_counts(openstates_root, key)
            if counts is not None:
                record["bills_new"] = counts["bills_new"]
                record["bills_updated"] = counts["bills_updated"]
                record["scrape_mode"] = counts["mode"]
            else:
                # A run that SUCCEEDED and still has no usable marker is the one case worth
                # saying out loud. Before the producing half of OPEN-139 ships this is expected
                # and harmless; after it, it means the two sides disagree -- most likely because
                # scrape_key_for() and run-scrape.sh's SCRAPE_KEY have drifted apart, which
                # otherwise degrades silently into "this jurisdiction reports no filing figures,
                # forever". The path is logged so an operator can settle it with one `ls`.
                # Warning, not an error, and never raises: instrumentation must not fail a scrape.
                logger.warning(
                    "openstates_scrape: no usable filing figures for a successful run — "
                    "expected marker missing, unreadable, or unparsed",
                    jurisdiction=jurisdiction,
                    scrape_key=key,
                    expected_path=os.path.join(
                        openstates_root, "logs", "last-run", f"{key}.imported"
                    ),
                )
        try:
            await redis_store.append_run_history(
                flow_key, jurisdiction, record, max_len=max(window, 20)
            )
            history = await redis_store.get_run_history(flow_key, jurisdiction)
        except Exception as e:
            logger.warning(
                "openstates_scrape: run-history tracking failed",
                jurisdiction=jurisdiction,
                error=str(e),
            )
            continue

        if should_escalate(history, window, threshold):
            recent = history[-window:]
            blocked = sum(1 for r in recent if r.get("failure_reason") == "waf_block")
            logger.error(
                "openstates_scrape: sustained blocking pattern detected",
                jurisdiction=jurisdiction,
                blocked=blocked,
                window=window,
                threshold=threshold,
            )
            _alert_sustained_block(jurisdiction, blocked, window)

        # OPEN-139. Gated off by default: this needs a few runs of history before it can say
        # anything, and until then it would only ever be wrong in one direction or the other.
        if filing_enabled and should_alert_quiet(history, quiet_window):
            logger.error(
                "openstates_scrape: jurisdiction has stopped filing new bills",
                jurisdiction=jurisdiction,
                quiet_window=quiet_window,
            )
            _alert_quiet_jurisdiction(jurisdiction, quiet_window)


# ---------------------------------------------------------------------------
# Public job functions — called by the scheduler (via closures) and by
# the trigger endpoint directly. Each accepts the openstates_scrape config
# block so settings are driven from sync_schedule.yaml.
# ---------------------------------------------------------------------------

async def run_patch_refresh_job(config: dict | None = None) -> dict[str, Any]:
    """Apply local patches to openstates-core and openstates-scrapers.

    Runs apply-local-patches.sh once daily at 01:00 UTC before any scrapes
    start. The scrape jobs set SKIP_PATCHES=1 so they don't repeat this step.
    """
    openstates_root = _get_root(config)
    script = os.path.join(openstates_root, "apply-local-patches.sh")
    start_time = datetime.now(timezone.utc)

    logger.info("openstates_patch_refresh: starting", openstates_root=openstates_root)
    t = time.monotonic()

    try:
        # _run_with_group_kill rather than subprocess.run: apply-local-patches.sh shells out to
        # git, and subprocess.run(timeout=...) kills only the direct child — leaving those git
        # operations running against a half-rebuilt scraper worktree that a concurrent scrape
        # may already be reading through run-scrape.sh's READER_MARKER lock. Same reasoning as
        # _run_scrape's own use of this helper.
        returncode, _stdout, stderr_bytes, timed_out, _stalled = await asyncio.to_thread(
            _run_with_group_kill,
            ["/bin/bash", script],
            dict(os.environ),
            300,
            openstates_root,
        )
        duration = round(time.monotonic() - t, 1)

        if timed_out:
            logger.error("openstates_patch_refresh: timeout", duration_seconds=duration)
            await _write_flow_status("openstates_patch_refresh", {
                "flow": "openstates_patch_refresh",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "status": "failed",
                "error": "timeout",
                "duration_seconds": duration,
            })
            # We killed the script's whole process group, so its own ERR trap never ran and it
            # never alerted. We're the only ones who know this happened.
            _alert_scrape_failure("patch refresh", "timed out after 300s", duration)
            return {"success": False, "error": "timeout", "duration_seconds": duration}

        if returncode != 0:
            stderr = (stderr_bytes or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_patch_refresh: failed",
                returncode=returncode,
                stderr_tail=stderr,
                duration_seconds=duration,
            )
            await _write_flow_status("openstates_patch_refresh", {
                "flow": "openstates_patch_refresh",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "status": "failed",
                "error": f"exit_code_{returncode}",
                "duration_seconds": duration,
            })
            _alert_scrape_failure("patch refresh", f"exited {returncode}: {stderr[-200:]}", duration)
            return {"success": False, "error": f"exit_code_{returncode}", "duration_seconds": duration}

        logger.info("openstates_patch_refresh: done", duration_seconds=duration)
        await _write_flow_status("openstates_patch_refresh", {
            "flow": "openstates_patch_refresh",
            "started_at": start_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "duration_seconds": duration,
        })
        return {"success": True, "duration_seconds": duration}

    # No `except subprocess.TimeoutExpired` here any more: _run_with_group_kill swallows it and
    # returns timed_out=True, handled inside the try above.
    except Exception as e:
        duration = round(time.monotonic() - t, 1)
        logger.error("openstates_patch_refresh: error", error=str(e), duration_seconds=duration)
        # Deliberately NOT alerting here. OPEN-127 is scoped to subprocess failures (timeout,
        # nonzero exit), and _run_scrape has no generic-exception branch at all, so there's no
        # established behaviour to match. An exception here is a scheduler/config/coding fault
        # rather than a scrape failure, and routing those into #automation-errors is its own
        # decision. Left as-is per /pm-review round 1.
        return {"success": False, "error": str(e), "duration_seconds": duration}


async def run_fl_scrapes_job(config: dict | None = None) -> dict[str, Any]:
    """Run all FL sessions sequentially (they share _data/fl/).

    Sessions run in order: 2026 → 2026D → 2026E → 2026F. A failed session
    is logged but does not abort the remaining sessions.
    """
    openstates_root = _get_root(config)
    sessions = (
        (config or {}).get("primary", {}).get("fl", {})
        .get("sessions", ["2026", "2026D", "2026E", "2026F"])
    )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_fl_scrapes: starting", sessions=sessions)

    results = []
    for session in sessions:
        result = await _run_scrape(
            "fl", f"session={session}", openstates_root, SCRAPE_TIMEOUT_S["fl"], config
        )
        results.append(result)

    duration = round(time.monotonic() - t, 1)
    failed = [r for r in results if not r["success"]]

    log_fn = logger.error if failed else logger.info
    log_fn(
        "openstates_fl_scrapes: completed",
        sessions=sessions,
        total=len(results),
        failed=len(failed),
        duration_seconds=duration,
    )

    await _write_flow_status("openstates_fl_scrapes", {
        "flow": "openstates_fl_scrapes",
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if not failed else "completed_with_errors",
        "sessions": sessions,
        "total": len(results),
        "failed": len(failed),
        "results": results,
        "duration_seconds": duration,
    })
    return {
        "success": not failed,
        "sessions": sessions,
        "results": results,
        "failed": len(failed),
        "duration_seconds": duration,
    }


async def run_wa_scrape_job(config: dict | None = None) -> dict[str, Any]:
    """Run the WA scrape. Runs in parallel with FL and USA on the event loop."""
    openstates_root = _get_root(config)
    start_time = datetime.now(timezone.utc)

    result = await _run_scrape("wa", None, openstates_root, SCRAPE_TIMEOUT_S["wa"], config)

    await _write_flow_status("openstates_wa_scrape", {
        "flow": "openstates_wa_scrape",
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if result["success"] else "failed",
        **result,
    })
    return result


async def run_usa_scrapes_job(config: dict | None = None) -> dict[str, Any]:
    """Run USA lower then upper sequentially (they share _data/usa/)."""
    openstates_root = _get_root(config)
    sessions = (
        (config or {}).get("primary", {}).get("usa", {})
        .get("sessions", ["119 chamber=lower", "119 chamber=upper"])
    )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_usa_scrapes: starting", sessions=sessions)

    results = []
    for session in sessions:
        result = await _run_scrape(
            "usa", f"session={session}", openstates_root, SCRAPE_TIMEOUT_S["usa"], config
        )
        results.append(result)

    duration = round(time.monotonic() - t, 1)
    failed = [r for r in results if not r["success"]]

    log_fn = logger.error if failed else logger.info
    log_fn(
        "openstates_usa_scrapes: completed",
        total=len(results),
        failed=len(failed),
        duration_seconds=duration,
    )

    await _write_flow_status("openstates_usa_scrapes", {
        "flow": "openstates_usa_scrapes",
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if not failed else "completed_with_errors",
        "sessions": sessions,
        "total": len(results),
        "failed": len(failed),
        "results": results,
        "duration_seconds": duration,
    })
    return {
        "success": not failed,
        "sessions": sessions,
        "results": results,
        "failed": len(failed),
        "duration_seconds": duration,
    }


async def run_secondary_scrapes_job(
    config: dict | None = None,
    jurisdictions: list[str] | None = None,
    flow_status_key: str | None = None,
) -> dict[str, Any]:
    """Run secondary states (VA, MI, MA, UT, AZ) concurrently.

    Each jurisdiction uses a distinct _data/{state}/ directory so they don't
    conflict. asyncio.gather fans them out into separate threads simultaneously,
    cutting total wall-clock from ~sum(durations) to ~max(durations).

    OPEN-140 added the two optional arguments, and both default to exactly
    today's behaviour. `jurisdictions` narrows the run to a subset, because a
    jurisdiction escalated to nightly leaves this weekly batch and gets its own
    job -- otherwise it would be scraped twice on the batch's day.

    `flow_status_key` renames only the *flow status* Redis key. The rolling
    per-jurisdiction run HISTORY deliberately keeps the "openstates_secondary_scrapes"
    flow name in every case: that history is keyed per jurisdiction, it is what
    the cadence review reads to decide, and splitting it on a cadence change
    would erase the very evidence that caused the change. The flow status is a
    single per-flow document, though, so two jobs sharing one key would
    overwrite each other -- hence the separate name for the split-out jobs.
    """
    openstates_root = _get_root(config)
    if jurisdictions is None:
        jurisdictions = (
            (config or {}).get("secondary", {})
            .get("jurisdictions", ["va", "mi", "ma", "ut", "az"])
        )
    start_time = datetime.now(timezone.utc)
    t = time.monotonic()

    logger.info("openstates_secondary_scrapes: starting", jurisdictions=jurisdictions)

    # session_arg=None is deliberate, not a gap -- do not "fix" this to pass an explicit
    # session per jurisdiction without re-reading OPEN-24 first. openstates-core's do_scrape()
    # scrapes every currently-active session when none is given; VA and UT have had two
    # sessions simultaneously active at once (confirmed live 2026-08-02: VA had 2026S1 + 2027,
    # UT had 2026 + 2025S2). Passing a single hardcoded/resolved session per jurisdiction
    # (mirroring fl/usa's own sessions: config, which OPEN-24 originally proposed) would
    # silently drop whichever second active session doesn't get picked, for exactly those two.
    results: list[dict[str, Any]] = await asyncio.gather(
        *[_run_scrape(j, None, openstates_root, config=config) for j in jurisdictions]
    )

    duration = round(time.monotonic() - t, 1)
    failed = [r for r in results if not r["success"]]

    log_fn = logger.error if failed else logger.info
    log_fn(
        "openstates_secondary_scrapes: completed",
        jurisdictions=jurisdictions,
        total=len(results),
        failed=len(failed),
        duration_seconds=duration,
    )

    await _check_sustained_blocking(
        "openstates_secondary_scrapes", jurisdictions, results, config
    )

    status_key = flow_status_key or "openstates_secondary_scrapes"
    await _write_flow_status(status_key, {
        "flow": status_key,
        "started_at": start_time.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if not failed else "completed_with_errors",
        "jurisdictions": jurisdictions,
        "total": len(results),
        "failed": len(failed),
        "results": list(results),
        "duration_seconds": duration,
    })
    return {
        "success": not failed,
        "jurisdictions": jurisdictions,
        "results": list(results),
        "failed": len(failed),
        "duration_seconds": duration,
    }


async def run_single_scrape_job(
    jurisdiction: str,
    config: dict | None = None,
) -> dict[str, Any]:
    """Run a single arbitrary jurisdiction. Used by the manual trigger endpoint.

    ScrapeBot pre-seeding (PLAN-scrapebot.md §3.7) happens inside _run_scrape()
    itself now, keyed off the same config passed through here -- a jurisdiction
    triggered standalone gets the identical treatment as one triggered as part
    of the full secondary batch, with no separate call needed at this level.
    """
    openstates_root = _get_root(config)
    return await _run_scrape(jurisdiction, None, openstates_root, config=config)


async def run_people_refresh_job(config: dict | None = None) -> dict[str, Any]:
    """Pull the people repo and run os-people to-database for all states."""
    openstates_root = _get_root(config)
    script = os.path.join(openstates_root, "run-people-refresh.sh")
    start_time = datetime.now(timezone.utc)

    logger.info("openstates_people_refresh: starting")
    t = time.monotonic()

    try:
        # This call site already passed start_new_session=True, which _run_with_group_kill's own
        # docstring explains is not sufficient on its own — it makes the child a process-group
        # leader but nothing then targets that group, so a timeout still orphaned the real work
        # (git pull, os-people to-database across every state). Routed through the helper so the
        # group actually gets killed.
        returncode, _stdout, stderr_bytes, timed_out, _stalled = await asyncio.to_thread(
            _run_with_group_kill,
            ["/bin/bash", script],
            dict(os.environ),
            3600,
        )
        duration = round(time.monotonic() - t, 1)

        if timed_out:
            logger.error("openstates_people_refresh: timeout", duration_seconds=duration)
            await _write_flow_status("openstates_people_refresh", {
                "flow": "openstates_people_refresh",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "status": "failed",
                "error": "timeout",
                "duration_seconds": duration,
            })
            _alert_scrape_failure("people refresh", "timed out after 3600s", duration)
            return {"success": False, "error": "timeout", "duration_seconds": duration}

        if returncode != 0:
            stderr_tail = (stderr_bytes or b"").decode(errors="replace")[-500:]
            logger.error(
                "openstates_people_refresh: failed",
                returncode=returncode,
                stderr_tail=stderr_tail,
                duration_seconds=duration,
            )
            await _write_flow_status("openstates_people_refresh", {
                "flow": "openstates_people_refresh",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "status": "failed",
                "error": f"exit_code_{returncode}",
                "duration_seconds": duration,
            })
            _alert_scrape_failure("people refresh", f"exited {returncode}: {stderr_tail[-200:]}", duration)
            return {"success": False, "error": f"exit_code_{returncode}", "duration_seconds": duration}

        logger.info("openstates_people_refresh: done", duration_seconds=duration)
        await _write_flow_status("openstates_people_refresh", {
            "flow": "openstates_people_refresh",
            "started_at": start_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "duration_seconds": duration,
        })
        return {"success": True, "duration_seconds": duration}

    # No `except subprocess.TimeoutExpired` here any more: _run_with_group_kill swallows it and
    # returns timed_out=True, handled inside the try above.
    except Exception as e:
        duration = round(time.monotonic() - t, 1)
        logger.error("openstates_people_refresh: error", error=str(e), duration_seconds=duration)
        # Deliberately NOT alerting here — see the matching note in run_patch_refresh_job.
        return {"success": False, "error": str(e), "duration_seconds": duration}
