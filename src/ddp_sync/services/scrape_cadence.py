"""Effective scrape cadence per jurisdiction (OPEN-140).

Today a jurisdiction's cadence is whatever `sync_day` says in config/sync_schedule.yaml:
present means weekly, absent means nightly. Which is fine, except it is maintained by hand.
Florida currently carries this, in a comment:

    sync_day: sunday
    # Weekly while FL out of session -- new draft bills aren't introduced until ~November...
    # Remove sync_day (revert to daily) once the 2027 session opens.

That is this module's logic, implemented as a note and a human's memory. If nobody remembers in
November, FL scrapes weekly through its entire session.

WHAT THIS DOES
    Resolves an *effective* cadence from two sources: the YAML value, and a runtime override in
    Redis that automation may raise. Escalation (weekly -> nightly on observed filing activity)
    is automatic. Demotion is deliberately NOT automatic -- see below.

THE FLOOR RULE (operator decision, 2026-08-23)
    Redis is the runtime source of truth; the YAML value is a FLOOR, not a default to be
    overwritten. So automation may make a jurisdiction run MORE often than YAML says and never
    less. `sync_day: sunday` in YAML means "weekly at minimum": Redis can lift Florida to
    nightly, and cannot drop anything below its configured cadence.

    Nothing here writes back to sync_schedule.yaml. A process editing its own config was
    considered and rejected.

    Losing Redis therefore degrades to the committed configuration rather than to silence --
    resolve_cadence() falls back to the floor on a missing, unreadable or unrecognised override.

WHY DEMOTION IS ONLY EVER A RECOMMENDATION HERE
    "No new bills" is exactly what a broken scraper looks like. Arizona produced zero new bills
    across ten consecutive imports because it was wedged, not because Arizona had nothing to
    file -- so a rule that drops a jurisdiction to weekly after a quiet stretch would have
    demoted Arizona *because* it was broken, checking the broken thing less often and slowing
    both detection and recovery.

    OPEN-22 reached the same conclusion for a different signal and wrote it into
    sync_schedule.yaml: its WAF escalation "deliberately does NOT skip or delay a scheduled run
    based on this history... skipping a scheduled attempt has unproven value and would mean less
    data on whether the block has actually cleared."

    So the asymmetry is deliberate and it is the point: escalation costs a few extra scrapes if
    wrong, demotion costs data. This module will *say* when a jurisdiction looks over-scraped and
    will not act on it.
"""

from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger()

NIGHTLY = "nightly"
WEEKLY = "weekly"
_ORDER = {WEEKLY: 0, NIGHTLY: 1}  # higher = more frequent


def cadence_from_yaml(jurisdiction_cfg: dict) -> str:
    """The configured floor. `sync_day` present means weekly, absent means nightly."""
    return WEEKLY if (jurisdiction_cfg or {}).get("sync_day") else NIGHTLY


def resolve_cadence(floor: str, override: str | None) -> str:
    """Effective cadence: the override, but never less frequent than the floor.

    An unrecognised or missing override resolves to the floor. That is the load-bearing part --
    losing Redis, or finding junk in it, must degrade to the committed configuration and not to
    "no schedule".
    """
    if floor not in _ORDER:
        logger.warning("scrape_cadence: unrecognised floor, defaulting to weekly", floor=floor)
        floor = WEEKLY
    if override not in _ORDER:
        if override is not None:
            logger.warning(
                "scrape_cadence: unrecognised override, using the configured floor",
                override=override,
                floor=floor,
            )
        return floor
    return override if _ORDER[override] > _ORDER[floor] else floor


def should_escalate(history: list[dict], window: int = 2) -> bool:
    """Has this jurisdiction filed new bills recently enough to deserve nightly scraping?

    True when any of the last `window` MEASURED runs imported new bills. Runs with no
    measurement (`bills_new` absent, because the marker was missing or the import report was
    unparseable -- see OPEN-139) are skipped rather than counted as zero.

    Deliberately generous: one productive run in the window is enough. Escalating wrongly costs
    a few extra scrapes; failing to escalate costs freshness on a jurisdiction that is actively
    legislating.
    """
    measured = [r for r in history if r.get("bills_new") is not None]
    if not measured:
        return False
    return any(r["bills_new"] > 0 for r in measured[-window:])


def demotion_looks_reasonable(history: list[dict], quiet_window: int = 4) -> bool:
    """Would demoting this jurisdiction to weekly be defensible? Advisory only -- never acted on.

    Requires all three, because the first two are what separate "genuinely quiet" from "broken":

      1. the last `quiet_window` MEASURED runs all imported zero new bills;
      2. every one of those runs SUCCEEDED and recorded no failure reason -- a jurisdiction whose
         runs are failing is not quiet, it is broken, and must not be checked less often;
      3. somewhere earlier in the retained history it did import new bills -- so a jurisdiction
         we have never successfully collected from is never demoted, that state being
         indistinguishable from broken-since-inception.

    Even when all three hold this only produces a log line and an alert. See the module docstring.
    """
    measured = [r for r in history if r.get("bills_new") is not None]
    if len(measured) < quiet_window:
        return False
    recent = measured[-quiet_window:]
    if any(r["bills_new"] > 0 for r in recent):
        return False
    if not all(r.get("success") and not r.get("failure_reason") for r in recent):
        return False
    return any(r["bills_new"] > 0 for r in measured[:-quiet_window])


def cadence_review(
    jurisdiction: str,
    floor: str,
    current_override: str | None,
    history: list[dict],
    escalate_window: int = 2,
    quiet_window: int = 4,
    excluded: set[str] | list[str] | None = None,
) -> dict[str, Any]:
    """Decide what should happen to one jurisdiction's cadence. Pure; performs no I/O.

    Both windows count **measured reviews, not calendar days.** `escalate_window=2` means "in the
    last two reviews that produced a figure", which for a weekly jurisdiction is about two weeks
    and for a nightly one about two nights. Runs we could not measure do not consume a slot, so a
    stretch of unparseable import reports stretches the window in wall-clock terms rather than
    silently ageing a productive run out of it. Deliberate, and worth knowing before tuning these.

    `excluded` is enforced here rather than left to the caller. Michigan must never be escalated:
    OPEN-53 established that more traffic against a WAF worsens a block, which is why MI is
    already excluded from scrape retries. Enforcing it at the decision point means no future
    caller can forget to check, and pm-review's objection was exactly that a config key nothing
    reads is only a declaration.

    Returns a verdict the caller acts on:
        effective          the cadence that should be in force now
        action             "escalate" | "none"
        excluded           True when this jurisdiction is barred from escalation
        demotion_advice    True when demotion would be defensible (advisory only)
    """
    is_excluded = jurisdiction in set(excluded or ())
    previous = resolve_cadence(floor, current_override)
    effective = previous
    action = "none"

    if is_excluded:
        logger.info(
            "scrape_cadence: jurisdiction excluded from escalation, leaving cadence alone",
            jurisdiction=jurisdiction,
            effective=effective,
        )
    elif effective == WEEKLY and should_escalate(history, escalate_window):
        effective = NIGHTLY
        action = "escalate"
        logger.info(
            "scrape_cadence: escalating to nightly on observed filing activity",
            jurisdiction=jurisdiction,
            floor=floor,
            window=escalate_window,
        )

    advice = effective == NIGHTLY and demotion_looks_reasonable(history, quiet_window)
    if advice:
        # Logged rather than acted on, and this log line is the whole delivery mechanism for the
        # advice today -- see the module docstring for why demotion is never automatic.
        logger.info(
            "scrape_cadence: quiet for a sustained window and healthy — demotion to its "
            "configured floor would be defensible. NOT acting; edit sync_day to do it.",
            jurisdiction=jurisdiction,
            quiet_window=quiet_window,
            effective=effective,
            floor=floor,
        )

    return {
        "jurisdiction": jurisdiction,
        "floor": floor,
        "previous": previous,
        "effective": effective,
        "action": action,
        "excluded": is_excluded,
        "demotion_advice": advice,
    }
