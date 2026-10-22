"""Daily check that this host's jurisdiction lists still agree with the manifest (OPEN-318 slice 5, OPEN-330).

`jurisdictions.yaml` in the ddp-open-states checkout says which jurisdictions are enrolled in which feature
(OPEN-328 added the dual-read). Nothing noticed when a list on a host drifted from it between PRs: the CI drift
check only runs on ddp-open-states PRs, and the two env-var lists are not in git at all. This job compares, on
the host it runs on:

  - every jurisdiction list in the LIVE sync_schedule.yaml (the dict this process loaded, which on a host can be
    hand-edited), via `jurisdiction_enrollment.config_disagreements`;
  - this host's own `DDP_OPENSTATES_JURISDICTIONS` and `LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST`, but only when
    the host sets them (a host that routes nothing to the replica is not "drift");
  - whether the manifest can answer at all. A monitor that cannot see is an alert condition, not a log line
    (the retired staleness watchdog watched nothing for six weeks): a missing, unreadable or out-of-date
    checkout (the EC2 bind mount is never auto-pulled) is reported as "unverifiable".

It only measures and tells (Slack `#automation-errors` through `post_alert`). The alert is repeated only when the
set of findings changes or after `repeat_days` (default 7), so a drift that stays does not message every night.
Default OFF per host (`JURISDICTION_DRIFT_CHECK_ENABLED`), like the fork check.
"""

from __future__ import annotations

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

# env var name -> (manifest feature, the value in this host's settings)
_ENV_LISTS = {
    "DDP_OPENSTATES_JURISDICTIONS": ("replica_routing", "ddp_openstates_jurisdictions"),
    "LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST": ("legbot_replica", "legbot_rds_replica_jurisdiction_allowlist"),
}
_CHECKED_FEATURES = ("archive", "embedding", "search_refresh", "cloud_path", "scrape_primary", "scrape_secondary")


def collect_findings(sync_config: dict, settings: Any, root: str) -> dict[str, Any]:
    """Everything wrong right now, as plain data: {"drift": [...], "unverifiable": [...], "skipped": [...]}.
    `skipped` names the env lists this host does not set (unset and an empty value are the same thing to the
    settings layer, so a list accidentally cleared reads as "not set"). It is reported in the status and the log
    so the gap is visible, but it is not a finding and does not alert."""
    drift = [dict(d, source="sync_schedule.yaml") for d in je.config_disagreements(sync_config, root)]
    unverifiable = [f for f in _CHECKED_FEATURES if je.manifest_codes(f, root) is None]
    skipped = []
    for name, (feature, attr) in _ENV_LISTS.items():
        have = {str(c).strip().lower() for c in (getattr(settings, attr, None) or []) if str(c).strip()}
        if not have:
            skipped.append(name)  # this host does not set it: nothing to compare
            continue
        want = je.manifest_codes(feature, root)
        if want is None:
            unverifiable.append(feature)
            continue
        if have != set(want):
            drift.append({"feature": feature, "source": f"env {name}", "only_in_yaml": sorted(have - set(want)),
                          "only_in_manifest": sorted(set(want) - have)})
    return {"drift": drift, "unverifiable": sorted(set(unverifiable)), "skipped": skipped}


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
            alerted = post_alert(_alert_text(findings, root), source="jurisdiction_drift_check")
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
            post_alert(f":warning: jurisdiction drift check could not run on this host: {exc}", source="jurisdiction_drift_check")
        except Exception:  # noqa: BLE001 - never let the alert path escape into the scheduler
            logger.error("jurisdiction_drift_check: failure alert also failed")
        return {"flow": FLOW_KEY, "status": "error", "error": str(exc)}
