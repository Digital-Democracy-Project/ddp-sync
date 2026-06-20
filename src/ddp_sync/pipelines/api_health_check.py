"""API health check pipeline.

Runs HTTP assertions against configured DDP API endpoints and posts a
Zapier alert on failures. Called by the APScheduler nightly job and by
the CLI wrapper at scripts/check_api_health.py.

Check config fields:
    name (str)        — unique id used in alert messages
    description (str) — human-readable label
    path (str)        — URL path relative to base_url
    method (str)      — "GET" (default) or "POST"
    params (dict)     — query params (GET) or JSON body (POST)
    voatz_auth (bool) — authenticate with Voatz first; merges
                        {organizationId, WS, Csrf-Token} into the POST body
    voatz_org (dict)  — org config to use for auth (set by _build_checks)
    assertions (list) — ordered list of assertion names (first failure wins)
    min_count (int)   — minimum items for non_empty_result (default 1)

Available assertions:
    status_200        — HTTP status must be 200
    body_not_empty    — body must not be "" or whitespace-only
    valid_json        — body must parse as JSON
    non_empty_result  — parsed value must contain >= min_count items;
                        walks up to two levels of wrapper keys
                        (items / data / events / results / bills)
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

DDP_API_BASE_URL = os.getenv("DDP_API_BASE_URL", "https://api.digitaldemocracyproject.org")
REQUEST_TIMEOUT = 30

FALLBACK_CHECKS: list[dict] = [
    # Add non-Voatz checks here and they'll always run regardless of org config.
    # Example:
    # {
    #     "name": "ddp_api_health",
    #     "description": "DDP API /health returns 200",
    #     "path": "/health",
    #     "method": "GET",
    #     "params": {},
    #     "assertions": ["status_200", "body_not_empty", "valid_json"],
    # },
]


# ---------------------------------------------------------------------------
# Check definitions
# ---------------------------------------------------------------------------

def build_checks() -> list[dict]:
    """Generate one /get_events check per configured Voatz org."""
    try:
        from ddp_sync.pipelines.voatz_brevo import _get_org_configs
        orgs = _get_org_configs()
    except Exception as e:
        logger.warning("Could not load org configs (%s); using fallback checks", e)
        return FALLBACK_CHECKS

    if not orgs:
        logger.warning("No org configs found; using fallback checks")
        return FALLBACK_CHECKS

    checks = []
    for org in orgs:
        org_name = org.get("name") or f"org_{org.get('voatz_org_id', 'unknown')}"
        slug = org_name.lower().replace(" ", "_")
        checks.append({
            "name": f"{slug}_get_events",
            "description": f"/get_events for {org_name} returns a non-empty JSON result",
            "path": "/get_events",
            "method": "POST",
            "voatz_auth": True,
            "voatz_org": org,
            "params": {},
            "assertions": ["status_200", "body_not_empty", "valid_json", "non_empty_result"],
            "min_count": 1,
        })

    return checks + FALLBACK_CHECKS


# ---------------------------------------------------------------------------
# Voatz authentication
# ---------------------------------------------------------------------------

def _get_voatz_tokens_for_check(org: dict) -> tuple[dict, str] | tuple[None, str]:
    """Authenticate with the given Voatz org config.

    Returns (auth_fields_dict, org_name) on success, (None, error_msg) on failure.
    """
    try:
        from ddp_sync.pipelines.voatz_brevo import get_voatz_tokens
    except ImportError as e:
        return None, f"could not import voatz_brevo: {e}"

    org_name = org.get("name", "unknown")
    tokens = get_voatz_tokens(org["voatz_email"], org["voatz_password"], org["voatz_org_id"])
    if not tokens:
        return None, f"Voatz authentication failed for org '{org_name}'"

    ws_token, csrf_token = tokens
    return {
        "organizationId": org["voatz_org_id"],
        "WS": ws_token,
        "Csrf-Token": csrf_token,
    }, org_name


# ---------------------------------------------------------------------------
# Core check logic
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    description: str
    passed: bool
    error: str | None = None
    status_code: int | None = None
    body_preview: str = ""
    duration_ms: int = 0


_LIST_KEYS = ("items", "data", "events", "results", "bills")


def _find_result_list(obj: object, depth: int = 0) -> tuple[list | None, str]:
    """Recursively find the first list under a known wrapper key (up to 2 levels)."""
    if isinstance(obj, list):
        return obj, ""
    if isinstance(obj, dict) and depth < 2:
        for key in _LIST_KEYS:
            if key in obj:
                found, path = _find_result_list(obj[key], depth + 1)
                if found is not None:
                    return found, f".{key}{path}"
    return None, ""


def _check_non_empty(parsed: object, min_count: int) -> str | None:
    if isinstance(parsed, list):
        if len(parsed) < min_count:
            return f"result is a list with {len(parsed)} items (expected >= {min_count})"
        return None
    if isinstance(parsed, dict):
        result_list, path = _find_result_list(parsed)
        if result_list is not None:
            if len(result_list) < min_count:
                return f"result{path} has {len(result_list)} items (expected >= {min_count})"
            return None
        if not parsed:
            return "response is an empty object {}"
        return None
    return f"unexpected response type: {type(parsed).__name__}"


def _run_assertions(
    assertions: list[str],
    resp: requests.Response,
    min_count: int = 1,
) -> str | None:
    parsed = None
    parse_error = None
    try:
        parsed = resp.json()
    except Exception as e:
        parse_error = str(e)

    for assertion in assertions:
        if assertion == "status_200":
            if resp.status_code != 200:
                return f"HTTP {resp.status_code}"
        elif assertion == "body_not_empty":
            stripped = resp.text.strip()
            if not stripped or stripped in ('""', "''"):
                return "response body is empty string"
        elif assertion == "valid_json":
            if parse_error is not None:
                preview = resp.text[:100].replace("\n", " ")
                return f"body is not valid JSON ({parse_error}); body={preview!r}"
            if parsed is None:
                return "response parsed to null/None"
        elif assertion == "non_empty_result":
            if parsed is None:
                return "cannot check non_empty_result: response is not valid JSON"
            err = _check_non_empty(parsed, min_count)
            if err:
                return err

    return None


def run_check(check: dict, base_url: str, headers: dict) -> CheckResult:
    """Execute one health check. Never raises."""
    url = base_url.rstrip("/") + check["path"]
    method = check.get("method", "GET").upper()
    assertions = check.get("assertions", [])
    min_count = check.get("min_count", 1)
    body = dict(check.get("params") or {})

    if check.get("voatz_auth"):
        org = check.get("voatz_org") or {}
        auth_fields, msg = _get_voatz_tokens_for_check(org)
        if auth_fields is None:
            return CheckResult(
                name=check["name"],
                description=check.get("description", check["path"]),
                passed=False,
                error=f"Voatz auth failed: {msg}",
            )
        body.update(auth_fields)

    t0 = time.monotonic()
    try:
        if method == "POST":
            resp = requests.post(url, headers=headers, json=body, timeout=REQUEST_TIMEOUT)
        else:
            resp = requests.get(url, headers=headers, params=body, timeout=REQUEST_TIMEOUT)

        duration_ms = int((time.monotonic() - t0) * 1000)
        body_preview = resp.text[:200].replace("\n", " ")
        error = _run_assertions(assertions, resp, min_count)

        return CheckResult(
            name=check["name"],
            description=check.get("description", check["path"]),
            passed=error is None,
            error=error,
            status_code=resp.status_code,
            body_preview=body_preview,
            duration_ms=duration_ms,
        )

    except requests.Timeout:
        return CheckResult(
            name=check["name"],
            description=check.get("description", check["path"]),
            passed=False,
            error=f"timed out after {REQUEST_TIMEOUT}s",
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            name=check["name"],
            description=check.get("description", check["path"]),
            passed=False,
            error=f"request error: {e}",
            duration_ms=int((time.monotonic() - t0) * 1000),
        )


# ---------------------------------------------------------------------------
# Zapier alerting
# ---------------------------------------------------------------------------

def push_health_alert(webhook_url: str, results: list[CheckResult]) -> bool:
    """POST health check results to Zapier. Returns True on 2xx. Never raises."""
    if not webhook_url:
        logger.warning("No Zapier webhook URL configured — skipping alert")
        return False

    failures = [r for r in results if not r.passed]
    failure_lines = [f"{r.name}: {r.error}" for r in failures]

    failures_text = "\n".join(
        f"• *{r.name}*: {r.error}"
        + (f"\n  `{r.body_preview[:120]}`" if r.body_preview else "")
        for r in failures
    )

    slack_message = (
        f":red_circle: *DDP API Health Check Failed* — {len(failures)} of {len(results)} checks failed\n\n"
        + failures_text
        + f"\n\n_Checked at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_"
    )

    payload = {
        "alert_type": "api_health_check_failed",
        "on_failure": True,
        "failure_count": len(failures),
        "total_checks": len(results),
        "summary": f"{len(failures)}/{len(results)} checks failed",
        "failure_warning": "⚠️ DDP API health check failures: " + "; ".join(failure_lines),
        "failures_text": failures_text,
        "slack_message": slack_message,
        "failures": [
            {
                "name": r.name,
                "description": r.description,
                "error": r.error,
                "status_code": r.status_code,
                "body_preview": r.body_preview,
                "duration_ms": r.duration_ms,
            }
            for r in failures
        ],
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=30)
        if 200 <= resp.status_code < 300:
            return True
        logger.error(
            "Zapier webhook returned non-2xx",
            extra={"status_code": resp.status_code, "body": resp.text[:200]},
        )
    except Exception as e:  # noqa: BLE001
        logger.error("Zapier webhook error: %s", e)
    return False


# ---------------------------------------------------------------------------
# Scheduler entry point
# ---------------------------------------------------------------------------

def run_api_health_check_job() -> dict:
    """Run all API health checks and post a Zapier alert on any failures.

    Designed to be called directly by APScheduler (sync). Never raises.
    Returns a summary dict.
    """
    from ddp_sync.config import get_settings

    settings = get_settings()
    base_url = DDP_API_BASE_URL
    ddp_api_key = settings.ddp_api_key or os.getenv("DDP_API_KEY", "")
    headers: dict = {}
    if ddp_api_key:
        headers["Authorization"] = f"Bearer {ddp_api_key}"
    else:
        logger.warning("DDP_API_KEY not configured — health check requests will be unauthenticated")

    checks = build_checks()
    logger.info("API health check starting", extra={"check_count": len(checks), "base_url": base_url})

    results = [run_check(check, base_url, headers) for check in checks]

    failures = [r for r in results if not r.passed]
    passed = len(results) - len(failures)

    for r in results:
        log = logger.info if r.passed else logger.error
        log(
            "API health check result",
            extra={
                "check": r.name,
                "passed": r.passed,
                "duration_ms": r.duration_ms,
                "error": r.error,
            },
        )

    if failures:
        webhook_url = settings.zapier_webhook_url or ""
        push_health_alert(webhook_url, results)
        logger.error(
            "API health check completed with failures",
            extra={"passed": passed, "failed": len(failures), "total": len(results)},
        )
    else:
        logger.info(
            "API health check completed — all passed",
            extra={"passed": passed, "total": len(results)},
        )

    return {
        "success": len(failures) == 0,
        "passed": passed,
        "failed": len(failures),
        "total": len(results),
        "failures": [{"name": r.name, "error": r.error} for r in failures],
    }
