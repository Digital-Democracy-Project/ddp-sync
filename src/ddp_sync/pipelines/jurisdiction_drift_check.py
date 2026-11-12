"""Daily check that this host's jurisdiction lists still agree with the manifest (OPEN-318 slice 5, OPEN-330).

`jurisdictions.yaml` in the ddp-open-states checkout says which jurisdictions are enrolled in which feature
(OPEN-328 added the dual-read). Nothing noticed when a list on a host drifted from it between PRs: the CI drift
check only runs on ddp-open-states PRs, and the two env-var lists are not in git at all. This job compares, on
the host it runs on:

  - the LIVE sync_schedule.yaml (the dict this process loaded, which on a host can be hand-edited), via
    `jurisdiction_enrollment.config_disagreements`: since SYNC-157 it holds no per-state list (the manifest is read
    directly), so only `openstates_scrape.timing` can disagree;
  - whether the manifest can answer at all. A monitor that cannot see is an alert condition, not a log line
    (the retired staleness watchdog watched nothing for six weeks): a missing, unreadable or out-of-date
    checkout (the EC2 bind mount is never auto-pulled) is reported as "unverifiable".

It only measures and tells (Slack `#automation-errors` through `post_alert`). The alert is repeated only when the
set of findings changes or after `repeat_days` (default 7), so a drift that stays does not message every night.
Default OFF per host (`JURISDICTION_DRIFT_CHECK_ENABLED`), like the fork check.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import structlog

from ddp_sync.pipelines.openstates_scrape import _get_root, _write_flow_status
from ddp_sync.services import jurisdiction_enrollment as je
from ddp_sync.slack_alerts import post_alert

logger = structlog.get_logger()

FLOW_KEY = "jurisdiction_drift_check"
DEFAULT_REPEAT_DAYS = 7

# SYNC-157: no env list is compared any more. DDP_OPENSTATES_JURISDICTIONS and LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST
# are deleted (config.REMOVED_ENV warns when a host still sets one); every reader takes the manifest.
# SYNC-142: `legbot_replica` is checked here because the LegBot gate now reads it from the manifest; a manifest that cannot
# answer it makes the gate fall back to the env list, which must be visible (unverifiable), not silent.
# SYNC-152: `scrape` (one rule, status not paused or probing) replaces scrape_primary/scrape_secondary: a manifest that cannot
# answer it leaves this host scheduling no new scrape job, so it must alert as unverifiable.
# SYNC-157: the per-state behaviour fields too. A manifest older than SYNC-157 answers None for them, and every reader then
# treats the state as not enrolled (MI loses its ScrapeBot fallback, MA its retry and its longer Fargate wait), so it must
# alert as unverifiable rather than run quietly with the safe-but-wrong answer.
_CHECKED_FEATURES = ("archive", "embedding", "search_refresh", "cloud_path", "scrape", "legbot_replica", "sweep_import",
                     "full_walk", "scrape_retry", "cadence_escalation", "scrapebot_fallback", "broker_bill_event_signal")


def collect_findings(sync_config: dict, settings: Any, root: str) -> dict[str, Any]:
    """Everything wrong right now, as plain data: {"drift": [...], "unverifiable": [...], "skipped": [...]}.
    `skipped` is always empty since SYNC-157 (no env list is compared); the key stays for the status record's shape.
    """
    drift = [dict(d, source="sync_schedule.yaml") for d in je.config_disagreements(sync_config, root)]
    unverifiable = [f for f in _CHECKED_FEATURES if je.manifest_codes(f, root) is None]
    return {"drift": drift, "unverifiable": sorted(set(unverifiable)), "skipped": []}


def _signature(findings: dict[str, Any]) -> str:
    # `skipped` describes this host's setup, not a problem: it must not change the signature (re-alert)
    problems = {k: findings[k] for k in ("drift", "unverifiable")}
    return hashlib.sha256(json.dumps(problems, sort_keys=True).encode()).hexdigest()[:16]


def _alert_text(findings: dict[str, Any], root: str) -> str:
    lines = [":warning: *Jurisdiction lists disagree with jurisdictions.yaml* (host check, OPEN-330)"]
    for d in findings["drift"]:
        lines.append(f"• `{d['feature']}` ({d['source']}): on this host but not in the manifest: "
                     f"{', '.join(d['only_in_yaml']) or '-'}; in the manifest but not on this host: "
                     f"{', '.join(d['only_in_manifest']) or '-'}")
    if findings["unverifiable"]:
        lines.append(f"• cannot verify {', '.join(findings['unverifiable'])}: the manifest under `{root}` is missing, "
                     f"unreadable, or older than the fields this check needs (pull the ddp-open-states checkout)")
    lines.append("Fix the list on this host, or the manifest if it is what is wrong. Manifest file times are in the ddp-sync log.")
    return "\n".join(lines)


async def run_jurisdiction_drift_check(config: dict | None = None) -> dict[str, Any]:
    """Compare, record the outcome in the flow status, and alert when due. Never raises."""
    now = datetime.now(timezone.utc)
    try:
        from ddp_sync.config import get_settings
        from ddp_sync.scheduler import get_scheduler

        scheduler = get_scheduler()
        sync_config = scheduler._sync_config if scheduler else {}
        root = _get_root((sync_config or {}).get("openstates_scrape") or config)
        findings = collect_findings(sync_config or {}, get_settings(), root)
        # Log the same lines a weekly review counts (OPEN-328's evidence), then the findings.
        je.log_config_disagreements(sync_config or {}, root)
        sig = _signature(findings)
        problem = bool(findings["drift"] or findings["unverifiable"])

        previous: dict = {}
        try:
            from ddp_sync.services.redis_store import get_redis_store

            previous = await get_redis_store().get_flow_status(FLOW_KEY) or {}
        except Exception as exc:  # noqa: BLE001 - alert de-duplication must not stop the check itself
            logger.warning("jurisdiction_drift_check: could not read the previous status", error=str(exc))

        repeat_days = int(((config or {}).get("jurisdiction_drift_check") or {}).get("repeat_days", DEFAULT_REPEAT_DAYS))
        last_alert = previous.get("last_alert_at")
        due = problem and (
            previous.get("signature") != sig
            or not last_alert
            or (now - datetime.fromisoformat(last_alert)).total_seconds() >= repeat_days * 86400
        )
        alerted = False
        if due:
            alerted = await asyncio.to_thread(post_alert, _alert_text(findings, root), source="jurisdiction_drift_check")
        result = {
            "flow": FLOW_KEY, "status": "drift" if problem else "ok", "checked_at": now.isoformat(),
            "signature": sig if problem else "", "findings": findings,
            "last_alert_at": now.isoformat() if alerted else (last_alert if problem else None),
        }
        await _write_flow_status(FLOW_KEY, result)
        (logger.warning if problem else logger.info)("jurisdiction_drift_check: done", status=result["status"],
                                                      drift=len(findings["drift"]), unverifiable=findings["unverifiable"],
                                                      env_lists_not_set_on_this_host=findings["skipped"], alerted=alerted)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("jurisdiction_drift_check: failed", error=str(exc))
        # A monitor that cannot run is an alert condition too. Not de-duplicated: a check that breaks should nag.
        try:
            await asyncio.to_thread(post_alert, f":warning: jurisdiction drift check could not run on this host: {exc}", source="jurisdiction_drift_check")
        except Exception:  # noqa: BLE001 - never let the alert path escape into the scheduler
            logger.error("jurisdiction_drift_check: failure alert also failed")
        return {"flow": FLOW_KEY, "status": "error", "error": str(exc)}
