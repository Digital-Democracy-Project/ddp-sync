"""Pipeline that runs votebot's evaluate_production.py on a schedule.

See plans/PLAN-eval-and-cache-hit-logging.md §3 for the full design.

Wire-up summary:
- ddp-sync's APScheduler triggers ``run_votebot_eval`` weekly (Sunday 12:00 UTC).
- The function subprocess-invokes votebot's eval script in votebot's own venv
  to keep the two repos decoupled (no cross-import).
- Headline metrics are parsed from the saved JSON's pinned ``headline`` block
  (Phase 2 contract, plan §2.8).
- Regression detection runs against (a) fixed thresholds (citation_rate_floor,
  pass_rate_floor) and (b) deltas from the previous run (votebot:eval:last_run).
- Run-summary alert is posted to Zapier via ``push_eval_alert``, mirroring
  ``pipelines/legislator_bio.py::push_bio_sync_alert``. Zapier filters route
  on ``on_failure``, ``on_regression``, and ``on_bill_history_leak`` flags.
- Concurrency lock (``votebot:eval:running``) is acquired/released INSIDE this
  function so both the scheduled job and the manual-trigger endpoint share
  identical lock semantics. Lock TTL = subprocess timeout + 300s safety margin.
- Path validation is done at scheduler.start() time; this function defends
  against the path going stale mid-day.
- Never raises — all exceptions are caught, logged, and surfaced via the
  return dict + Redis flow_status.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import structlog

from ddp_sync.config import Settings, get_settings

logger = structlog.get_logger()


# -----------------------------------------------------------------------------
# Pinned constants (plan §3.6)
# -----------------------------------------------------------------------------
# These are referenced by external log-monitoring rules (and Zapier filters).
# Tests assert literal string equality so a typo surfaces in CI before it
# silently breaks production alerting.

METRIC_RUN_COMPLETED = "votebot_eval.scheduled_run_completed"
METRIC_REGRESSION = "votebot_eval.regression_detected"
METRIC_RUN_FAILED = "votebot_eval.run_failed"
METRIC_ALERT_SENT = "votebot_eval.alert_sent"
METRIC_ALERT_SKIPPED = "votebot_eval.alert_skipped"
METRIC_ALERT_FAILED = "votebot_eval.alert_failed"
METRIC_UNKNOWN_YAML_KEY = "votebot_eval.unknown_yaml_key"

FLOW_NAME = "votebot_eval"
LOCK_KEY = "votebot:eval:running"
LAST_RUN_KEY = "votebot:eval:last_run"

# Thresholds default below current production baseline so the first scheduled
# run isn't a false positive. Plan §3.2 step 5 + §3.3 YAML config block.
DEFAULT_CITATION_RATE_FLOOR = 0.20
DEFAULT_PASS_RATE_FLOOR = 0.40
DEFAULT_DELTA_DROP_PP = 10
DEFAULT_MAX_DAYS = 30
DEFAULT_VOTEBOT_PATH = "/home/ubuntu/votebot"
DEFAULT_REPORT_RETENTION_DAYS = 180

# Subprocess + lock timing.
LOCK_SAFETY_MARGIN_S = 300  # plan §3.4 + PM v4 review (was 120s in v3)


# -----------------------------------------------------------------------------
# Path resolution + validation (plan §3.9)
# -----------------------------------------------------------------------------

def resolve_votebot_path(yaml_config: dict | None = None) -> str:
    """Resolve the votebot path. Order: env var > YAML > default."""
    env = os.environ.get("VOTEBOT_PATH")
    if env:
        return env
    if yaml_config:
        configured = yaml_config.get("votebot_path")
        if configured:
            return configured
    return DEFAULT_VOTEBOT_PATH


def validate_votebot_path(path: str) -> tuple[bool, str | None]:
    """Validate that the votebot path has the venv + script we need.

    Returns (is_valid, error_message). Used both at scheduler.start() time
    (to skip job registration) and at manual-trigger time (to live-validate
    in case the path was moved/removed since startup).
    """
    p = Path(path).expanduser()
    if not p.is_dir():
        return False, f"votebot path does not exist or is not a directory: {p}"
    venv_python = p / ".venv" / "bin" / "python"
    if not venv_python.is_file():
        return False, f"venv python not found: {venv_python}"
    if not os.access(venv_python, os.X_OK):
        return False, f"venv python not executable: {venv_python}"
    script = p / "scripts" / "evaluate_production.py"
    if not script.is_file():
        return False, f"eval script not found: {script}"
    return True, None


# -----------------------------------------------------------------------------
# YAML config validation (plan §3.8)
# -----------------------------------------------------------------------------

# The set of keys we recognize under votebot_eval. Anything not in this set
# triggers an info-level "unknown YAML key" warning (typo guard).
_KNOWN_KEYS = {
    "enabled", "frequency", "sync_day", "sync_time_utc", "days",
    "max_days", "votebot_path", "thresholds", "notifications",
}
_KNOWN_THRESHOLD_KEYS = {"citation_rate_floor", "pass_rate_floor", "delta_drop_pp"}
_KNOWN_NOTIFICATION_KEYS = {"enabled", "alert_on_success"}
_VALID_DAYS_OF_WEEK = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}


def validate_yaml_config(config: dict | None) -> tuple[dict | None, list[str]]:
    """Validate the votebot_eval YAML block.

    Returns (validated_config, errors). On any required-key failure, errors
    is non-empty and the caller MUST skip job registration. Optional keys
    fall to defaults (logged once at info level by the caller). Unknown keys
    produce warnings but don't abort registration.
    """
    errors: list[str] = []
    if config is None:
        # Block missing entirely — that's not an error per se; the caller
        # decides whether to register the job at all (frequency=disabled
        # or enabled=false would also skip).
        return None, errors

    # Type check the top-level dict.
    if not isinstance(config, dict):
        return None, [f"votebot_eval block must be a mapping, got {type(config).__name__}"]

    # Required keys.
    if "enabled" not in config or not isinstance(config["enabled"], bool):
        errors.append("votebot_eval.enabled must be present and a bool")
    if "frequency" not in config or config["frequency"] not in {"weekly", "daily"}:
        errors.append("votebot_eval.frequency must be 'weekly' or 'daily'")
    if config.get("frequency") == "weekly":
        sd = config.get("sync_day")
        if not sd or sd.lower() not in _VALID_DAYS_OF_WEEK:
            errors.append("votebot_eval.sync_day must be a weekday name (e.g. 'sunday')")
    sync_time = config.get("sync_time_utc")
    if not sync_time or not isinstance(sync_time, str) or ":" not in sync_time:
        errors.append("votebot_eval.sync_time_utc must be 'HH:MM'")
    days = config.get("days")
    if not isinstance(days, int) or days < 1:
        errors.append("votebot_eval.days must be a positive int")
    max_days = config.get("max_days", DEFAULT_MAX_DAYS)
    if not isinstance(max_days, int) or max_days < 1 or max_days > 90:
        errors.append("votebot_eval.max_days must be int in [1, 90]")
    elif isinstance(days, int) and days > max_days:
        errors.append(f"votebot_eval.days ({days}) exceeds max_days ({max_days})")

    # Optional thresholds block — validate types/ranges if present.
    th = config.get("thresholds")
    if th is not None:
        if not isinstance(th, dict):
            errors.append("votebot_eval.thresholds must be a mapping")
        else:
            for k in ("citation_rate_floor", "pass_rate_floor"):
                v = th.get(k)
                if v is not None and (not isinstance(v, (int, float)) or not 0 <= v <= 1):
                    errors.append(f"votebot_eval.thresholds.{k} must be a float in [0.0, 1.0]")
            v = th.get("delta_drop_pp")
            if v is not None and (not isinstance(v, (int, float)) or not 0 <= v <= 100):
                errors.append(f"votebot_eval.thresholds.delta_drop_pp must be in [0, 100]")
            for k in th.keys():
                if k not in _KNOWN_THRESHOLD_KEYS:
                    logger.warning(
                        "Unknown YAML key under votebot_eval.thresholds",
                        metric=METRIC_UNKNOWN_YAML_KEY,
                        key=f"thresholds.{k}",
                    )

    # Optional notifications block.
    notif = config.get("notifications")
    if notif is not None:
        if not isinstance(notif, dict):
            errors.append("votebot_eval.notifications must be a mapping")
        else:
            for k in ("enabled", "alert_on_success"):
                v = notif.get(k)
                if v is not None and not isinstance(v, bool):
                    errors.append(f"votebot_eval.notifications.{k} must be a bool")
            for k in notif.keys():
                if k not in _KNOWN_NOTIFICATION_KEYS:
                    logger.warning(
                        "Unknown YAML key under votebot_eval.notifications",
                        metric=METRIC_UNKNOWN_YAML_KEY,
                        key=f"notifications.{k}",
                    )

    # Top-level unknown keys.
    for k in config.keys():
        if k not in _KNOWN_KEYS:
            logger.warning(
                "Unknown YAML key under votebot_eval",
                metric=METRIC_UNKNOWN_YAML_KEY,
                key=k,
            )

    return (config if not errors else None), errors


def get_threshold(config: dict | None, key: str, default: float) -> float:
    """Read a threshold value from the YAML config, falling back to default."""
    if not config:
        return default
    th = config.get("thresholds") or {}
    v = th.get(key)
    return float(v) if v is not None else default


# -----------------------------------------------------------------------------
# Zapier alerting (plan §3.7)
# -----------------------------------------------------------------------------

def push_eval_alert(
    webhook_url: str,
    headline: dict,
    regression_details: list[dict],
    *,
    report_path: str,
    trigger: str,
    error: str | None = None,
) -> bool:
    """POST a votebot-eval run summary to the configured Zapier webhook.

    Mirrors ``pipelines/legislator_bio.py::push_bio_sync_alert`` — sync HTTP
    via ``requests`` (Zapier is fire-and-forget), 30s timeout, never raises.
    Returns True on 2xx, False otherwise.

    Skipped silently with ``METRIC_ALERT_SKIPPED`` when webhook URL is empty
    so a missing-webhook config doesn't fail the cron.

    The three flag bools (``on_failure``, ``on_regression``,
    ``on_bill_history_leak``) map directly to Zapier filter rules; routing
    config (which Slack channel, who to ping) lives in the Zap, not here.
    """
    if not webhook_url:
        logger.info(
            "No Zapier webhook URL configured for votebot-eval alert",
            metric=METRIC_ALERT_SKIPPED,
            reason="no_webhook_url",
        )
        return False

    on_failure = error is not None
    on_regression = bool(regression_details)
    on_bill_history_leak = (headline.get("bill_history_leak_count") or 0) > 0

    failure_warning = f"⚠️ Eval run failed: {error}" if on_failure else ""
    leak_warning = (
        f"🚨 bill_history_leak_count = {headline.get('bill_history_leak_count')}"
        if on_bill_history_leak
        else ""
    )
    regression_warning = ""
    if regression_details:
        parts = []
        for r in regression_details:
            parts.append(
                f"{r.get('type', 'regression')}: {r.get('metric')}="
                f"{r.get('value')} (threshold {r.get('threshold')})"
            )
        regression_warning = "⚠️ Regressions: " + "; ".join(parts)

    payload = {
        "alert_type": "votebot_eval_complete",
        "summary": (
            f"n={headline.get('n_query_processed', 0)} "
            f"citation_rate={headline.get('citation_rate', 0):.1%} "
            f"pass_rate={(headline.get('pass_rate') or 0):.1%} "
            f"cache_hit_rate={headline.get('cache_hit_rate', 0):.1%} "
            f"bill_history_leak_count={headline.get('bill_history_leak_count', 0)}"
        ),
        "window_days": headline.get("window_days"),
        "window_start": headline.get("window_start"),
        "window_end": headline.get("window_end"),
        "n_query_processed": headline.get("n_query_processed", 0),
        "citation_rate": headline.get("citation_rate", 0),
        "pass_rate": headline.get("pass_rate") or 0,
        "avg_confidence": headline.get("avg_confidence", 0),
        "cache_hit_rate": headline.get("cache_hit_rate", 0),
        "fallback_rate": headline.get("fallback_rate", 0),
        "bill_history_leak_count": headline.get("bill_history_leak_count", 0),
        "p50_latency_ms_rag_only": headline.get("p50_latency_ms_rag_only", 0),
        "p95_latency_ms_rag_only": headline.get("p95_latency_ms_rag_only", 0),
        # Routing flags (Zapier filters key off these).
        "on_failure": on_failure,
        "on_regression": on_regression,
        "on_bill_history_leak": on_bill_history_leak,
        # Pre-formatted warning strings (concat unconditionally in Slack template).
        "failure_warning": failure_warning,
        "regression_warning": regression_warning,
        "leak_warning": leak_warning,
        "report_path": report_path,
        "synced_at": datetime.now(timezone.utc).isoformat(),
        "trigger": trigger,
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=30)
        if 200 <= response.status_code < 300:
            logger.info(
                "votebot-eval Zapier alert sent",
                metric=METRIC_ALERT_SENT,
                citation_rate=headline.get("citation_rate"),
                pass_rate=headline.get("pass_rate"),
                bill_history_leak_count=headline.get("bill_history_leak_count"),
            )
            return True
        logger.error(
            "Zapier webhook returned non-2xx",
            status_code=response.status_code,
            response=response.text[:200],
            metric=METRIC_ALERT_FAILED,
        )
    except Exception as e:  # noqa: BLE001 — never raise from this helper
        logger.error(
            "Zapier webhook error",
            error=str(e),
            metric=METRIC_ALERT_FAILED,
        )
    return False


# -----------------------------------------------------------------------------
# Regression detection (plan §3.2 step 5)
# -----------------------------------------------------------------------------

def detect_regressions(headline: dict, last_run: dict | None, config: dict | None) -> list[dict]:
    """Return a list of regression dicts; empty if everything is healthy.

    Hard alerts (always-on):
      - bill_history_leak_count > 0
      - citation_rate < threshold (configurable, default 0.20)
      - pass_rate < threshold (configurable, default 0.40)
    Soft alerts (delta from last run):
      - citation_rate dropped > delta_drop_pp percentage points
      - pass_rate dropped > delta_drop_pp percentage points
    """
    regressions: list[dict] = []
    citation_floor = get_threshold(config, "citation_rate_floor", DEFAULT_CITATION_RATE_FLOOR)
    pass_floor = get_threshold(config, "pass_rate_floor", DEFAULT_PASS_RATE_FLOOR)
    delta_pp = get_threshold(config, "delta_drop_pp", DEFAULT_DELTA_DROP_PP)

    leak = headline.get("bill_history_leak_count") or 0
    if leak > 0:
        regressions.append({
            "type": "leak_canary",
            "metric": "bill_history_leak_count",
            "value": leak,
            "threshold": 0,
        })

    cr = headline.get("citation_rate")
    if cr is not None and cr < citation_floor:
        regressions.append({
            "type": "fixed_floor",
            "metric": "citation_rate",
            "value": cr,
            "threshold": citation_floor,
        })

    pr = headline.get("pass_rate")
    if pr is not None and pr < pass_floor:
        regressions.append({
            "type": "fixed_floor",
            "metric": "pass_rate",
            "value": pr,
            "threshold": pass_floor,
        })

    # Delta checks against last run.
    if last_run:
        last_h = last_run.get("headline", {}) if "headline" in last_run else last_run
        for metric in ("citation_rate", "pass_rate"):
            prev = last_h.get(metric)
            curr = headline.get(metric)
            if prev is not None and curr is not None:
                drop_pp = (prev - curr) * 100
                if drop_pp > delta_pp:
                    regressions.append({
                        "type": "delta_drop",
                        "metric": metric,
                        "value": curr,
                        "threshold": prev - (delta_pp / 100),
                        "previous": prev,
                        "drop_pp": round(drop_pp, 2),
                    })

    return regressions


# -----------------------------------------------------------------------------
# Main pipeline (plan §3.2)
# -----------------------------------------------------------------------------

def _compute_timeout(days: int) -> int:
    """Subprocess timeout scales with --days. Plan §3.2 step 2.

    PM v5 build review on Phase 3 noted historical 30-day runs have
    occasionally taken 45-55 min on c6g.large. Coefficient widened from
    60s/day to 90s/day so a 30-day window gets 2760s (46 min) before
    timeout, covering the documented worst case with margin.
    """
    return max(600, 120 + days * 90)


async def run_votebot_eval(
    days: int = 7,
    *,
    settings: Settings | None = None,
    yaml_config: dict | None = None,
    trigger: str = "scheduled",
) -> dict[str, Any]:
    """Run the votebot eval and post a Zapier alert.

    Plan §3.2 — single entry point shared by the scheduled job and the
    manual-trigger endpoint. Lock is acquired here (not at the endpoint
    boundary) so both paths share identical concurrency semantics.

    Returns a dict with at minimum ``success: bool``. On lock contention
    returns ``{"success": False, "error": "already_running", "current_run_id": ...}``.
    Never raises.
    """
    settings = settings or get_settings()
    start_time = datetime.now(timezone.utc)
    run_id = uuid.uuid4().hex
    timeout_s = _compute_timeout(days)
    lock_ttl = timeout_s + LOCK_SAFETY_MARGIN_S

    # Path validation (live, in case the path moved since startup).
    votebot_path = resolve_votebot_path(yaml_config)
    is_valid, err = validate_votebot_path(votebot_path)
    if not is_valid:
        logger.error(
            "votebot_eval: path validation failed at run time",
            metric=METRIC_RUN_FAILED,
            error=err,
            votebot_path=votebot_path,
        )
        return {"success": False, "error": "votebot_path_invalid", "detail": err}

    # Acquire the lock (plan §3.4 — symmetric across scheduled + manual paths).
    from ddp_sync.services.redis_store import get_redis_store
    redis_store = get_redis_store()
    if not redis_store._client:
        logger.error("votebot_eval: Redis unavailable", metric=METRIC_RUN_FAILED)
        return {"success": False, "error": "redis_unavailable"}

    acquired = await redis_store._client.set(LOCK_KEY, run_id, nx=True, ex=lock_ttl)
    if not acquired:
        existing = await redis_store._client.get(LOCK_KEY)
        existing_id = existing.decode() if isinstance(existing, bytes) else existing
        logger.warning(
            "votebot_eval: lock already held",
            current_run_id=existing_id,
        )
        return {
            "success": False,
            "error": "already_running",
            "current_run_id": existing_id,
        }

    try:
        return await _run_eval_holding_lock(
            settings=settings,
            yaml_config=yaml_config,
            days=days,
            timeout_s=timeout_s,
            votebot_path=votebot_path,
            run_id=run_id,
            start_time=start_time,
            trigger=trigger,
            redis_store=redis_store,
        )
    finally:
        # Defensive: only delete the lock if we still own it (fence against
        # TTL-then-takeover where another process picked up the lock).
        try:
            current = await redis_store._client.get(LOCK_KEY)
            current_id = current.decode() if isinstance(current, bytes) else current
            if current_id == run_id:
                await redis_store._client.delete(LOCK_KEY)
        except Exception as e:  # noqa: BLE001
            logger.warning("votebot_eval: lock release failed", error=str(e))


async def _run_eval_holding_lock(
    *,
    settings: Settings,
    yaml_config: dict | None,
    days: int,
    timeout_s: int,
    votebot_path: str,
    run_id: str,
    start_time: datetime,
    trigger: str,
    redis_store,
) -> dict[str, Any]:
    """Inner function — runs while the Redis lock is held."""
    # Build the report path so we can pass --output to the script.
    eval_reports_dir = Path(votebot_path).expanduser() / "eval_reports"
    eval_reports_dir.mkdir(parents=True, exist_ok=True)
    end_date = start_time.strftime("%Y-%m-%d")
    hms = start_time.strftime("%H%M%S")
    report_filename = f"eval_report_{end_date}_last{days}d_{hms}.json"
    report_path = eval_reports_dir / report_filename

    cmd = [
        str(Path(votebot_path).expanduser() / ".venv" / "bin" / "python"),
        "scripts/evaluate_production.py",
        "--days", str(days),
        "--output", str(report_path),
    ]

    logger.info(
        "votebot_eval: subprocess starting",
        run_id=run_id,
        days=days,
        timeout_s=timeout_s,
        report_path=str(report_path),
    )

    proc_start = time.monotonic()
    try:
        # Run the (potentially multi-minute) subprocess off the event loop
        # via asyncio.to_thread so it doesn't starve the FastAPI worker /
        # APScheduler heartbeat. PM v5 build review concern #1 — mirrors
        # legislator_bio.py's asyncio.to_thread pattern for push_bio_sync_alert.
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            cwd=str(Path(votebot_path).expanduser()),
            capture_output=True,
            timeout=timeout_s,
            start_new_session=True,  # plan §3.2 — kill grandchildren on timeout
            check=False,
        )
        duration_s = time.monotonic() - proc_start
    except subprocess.TimeoutExpired as e:
        duration_s = time.monotonic() - proc_start
        logger.error(
            "votebot_eval: subprocess timed out",
            metric=METRIC_RUN_FAILED,
            timeout_s=timeout_s,
            duration_s=round(duration_s, 1),
            run_id=run_id,
        )
        await _write_failure_status(
            redis_store, start_time, "subprocess_timeout", trigger
        )
        return {"success": False, "error": "subprocess_timeout"}
    except Exception as e:  # noqa: BLE001
        logger.error(
            "votebot_eval: subprocess errored",
            metric=METRIC_RUN_FAILED,
            error=str(e),
            run_id=run_id,
        )
        await _write_failure_status(redis_store, start_time, str(e), trigger)
        return {"success": False, "error": "subprocess_error", "detail": str(e)}

    if result.returncode != 0:
        stderr_tail = (result.stderr or b"").decode(errors="replace")[-500:]
        logger.error(
            "votebot_eval: subprocess returned non-zero",
            metric=METRIC_RUN_FAILED,
            returncode=result.returncode,
            stderr_tail=stderr_tail,
            run_id=run_id,
        )
        await _write_failure_status(
            redis_store, start_time, f"exit_code_{result.returncode}", trigger
        )
        # Still try to send a failure alert.
        try:
            await _send_failure_alert(settings, yaml_config, str(report_path), trigger,
                                      f"exit code {result.returncode}: {stderr_tail}")
        except Exception:
            pass
        return {"success": False, "error": "subprocess_nonzero", "returncode": result.returncode}

    # Parse the saved JSON for the headline block. PM v5 build review LOW
    # concern: subprocess can return 0 yet write nothing (OOM mid-write,
    # disk-full, etc.). Guard with explicit existence + non-zero size
    # check so we surface "subprocess succeeded but no report" distinctly
    # from "report exists but malformed".
    try:
        if not report_path.exists():
            raise FileNotFoundError(f"report not written: {report_path}")
        if report_path.stat().st_size == 0:
            raise ValueError(f"report file is empty: {report_path}")
        with open(report_path) as f:
            report_json = json.load(f)
        headline = report_json.get("headline")
        if not headline:
            raise ValueError("saved JSON missing 'headline' block")
    except Exception as e:  # noqa: BLE001
        logger.error(
            "votebot_eval: failed to parse report JSON",
            metric=METRIC_RUN_FAILED,
            error=str(e),
            run_id=run_id,
        )
        await _write_failure_status(redis_store, start_time, f"parse_error: {e}", trigger)
        return {"success": False, "error": "parse_error", "detail": str(e)}

    # Detect regressions.
    last_run_blob = await redis_store._client.get(LAST_RUN_KEY)
    last_run = json.loads(last_run_blob) if last_run_blob else None
    regressions = detect_regressions(headline, last_run, yaml_config)

    if regressions:
        logger.error(  # error level so external log monitors can alert
            "votebot_eval: regressions detected",
            metric=METRIC_REGRESSION,
            regression_details=regressions,
            run_id=run_id,
        )

    # Write flow status to Redis.
    completed_at = datetime.now(timezone.utc)
    flow_status = {
        "flow": FLOW_NAME,
        "started_at": start_time.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": round((completed_at - start_time).total_seconds(), 1),
        "status": "completed",
        "trigger": trigger,
        "headline": headline,
        "regressions_detected": bool(regressions),
        "regression_details": regressions,
        "report_path": str(report_path),
        "error": None,
    }
    await redis_store.set_flow_status(FLOW_NAME, flow_status)

    # Update last_run for next-run delta detection.
    last_run_blob = {**headline, "completed_at": completed_at.isoformat()}
    try:
        await redis_store._client.set(LAST_RUN_KEY, json.dumps(last_run_blob))
    except Exception as e:  # noqa: BLE001
        logger.warning("votebot_eval: failed to update last_run", error=str(e))

    # Post Zapier alert (plan §3.7).
    notif_enabled = True
    alert_on_success = True
    if yaml_config:
        notif = yaml_config.get("notifications") or {}
        notif_enabled = notif.get("enabled", True)
        alert_on_success = notif.get("alert_on_success", True)

    should_alert = notif_enabled and (alert_on_success or regressions)
    if should_alert:
        # Run the sync HTTP call off the event loop. push_eval_alert is
        # fire-and-forget (returns bool, never raises) so we don't block
        # the orchestrator on Zapier latency. Mirrors legislator_bio's
        # use of asyncio.to_thread around push_bio_sync_alert.
        try:
            await asyncio.to_thread(
                push_eval_alert,
                getattr(settings, "zapier_webhook_url", "") or "",
                headline,
                regressions,
                report_path=str(report_path),
                trigger=trigger,
            )
        except Exception as e:  # noqa: BLE001
            # push_eval_alert never raises, but defend against
            # asyncio.to_thread surprises so a webhook hiccup doesn't
            # mask a successful eval run.
            logger.error(
                "votebot_eval: Zapier alert task crashed",
                error=str(e),
                metric=METRIC_ALERT_FAILED,
            )

    # Run-completed metric event (info level — distinct from regression_detected).
    logger.info(
        "votebot_eval: run completed",
        metric=METRIC_RUN_COMPLETED,
        run_id=run_id,
        duration_s=round((completed_at - start_time).total_seconds(), 1),
        n_query_processed=headline.get("n_query_processed"),
        citation_rate=headline.get("citation_rate"),
        pass_rate=headline.get("pass_rate"),
        regressions_detected=bool(regressions),
    )

    # Retention prune (best-effort).
    try:
        _prune_old_reports(eval_reports_dir, DEFAULT_REPORT_RETENTION_DAYS)
    except Exception as e:  # noqa: BLE001
        logger.warning("votebot_eval: prune failed", error=str(e))

    return {
        "success": True,
        "run_id": run_id,
        "headline": headline,
        "regressions": regressions,
        "report_path": str(report_path),
        "duration_seconds": round((completed_at - start_time).total_seconds(), 1),
    }


async def _write_failure_status(redis_store, start_time, error: str, trigger: str) -> None:
    """Write a flow_status entry for a failed run (best-effort)."""
    try:
        await redis_store.set_flow_status(FLOW_NAME, {
            "flow": FLOW_NAME,
            "started_at": start_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
            "trigger": trigger,
            "error": error,
        })
    except Exception:  # noqa: BLE001
        pass


async def _send_failure_alert(
    settings: Settings,
    yaml_config: dict | None,
    report_path: str,
    trigger: str,
    error: str,
) -> None:
    """Send a Zapier alert for a failed run with on_failure=True."""
    try:
        await asyncio.to_thread(
            push_eval_alert,
            getattr(settings, "zapier_webhook_url", "") or "",
            headline={
                "n_query_processed": 0,
                "citation_rate": 0,
                "pass_rate": 0,
                "cache_hit_rate": 0,
                "bill_history_leak_count": 0,
            },
            regression_details=[],
            report_path=report_path,
            trigger=trigger,
            error=error,
        )
    except Exception as e:  # noqa: BLE001
        logger.error(
            "votebot_eval: failure-alert task crashed",
            error=str(e),
            metric=METRIC_ALERT_FAILED,
        )


def _prune_old_reports(reports_dir: Path, max_age_days: int) -> int:
    """Delete eval reports older than max_age_days. Returns count deleted."""
    if not reports_dir.is_dir():
        return 0
    cutoff = time.time() - max_age_days * 86400
    deleted = 0
    for f in reports_dir.glob("eval_report_*.json"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                deleted += 1
        except Exception:  # noqa: BLE001
            pass
    return deleted
