"""Periodic health check for api.digitaldemocracyproject.org endpoints.

Runs HTTP assertions against configured endpoints and posts a Zapier
alert if anything fails. Designed to run from cron or the ddp-sync scheduler.

Usage:
    .venv/bin/python scripts/check_api_health.py
    .venv/bin/python scripts/check_api_health.py --dry-run    # skip Zapier

Cron example (every 15 minutes):
    */15 * * * * cd /home/ubuntu/ddp-sync && .venv/bin/python scripts/check_api_health.py >> /var/log/ddp-api-health.log 2>&1

Environment (from .env or Secrets Manager):
    DDP_API_BASE_URL   — default: https://api.digitaldemocracyproject.org
    DDP_API_KEY        — Bearer token (leave blank for Voatz-authenticated endpoints)
    ZAPIER_WEBHOOK_URL — Zapier webhook for failure alerts

Check config fields:
    name (str)          — unique identifier used in alert messages
    description (str)   — human-readable description of what is being checked
    path (str)          — URL path relative to DDP_API_BASE_URL
    method (str)        — "GET" (default) or "POST"
    params (dict)       — query string params (GET) or JSON body (POST)
    voatz_auth (bool)   — if True, authenticate with Voatz first and merge
                          {organizationId, WS, Csrf-Token} into the POST body;
                          uses the first org from settings.organizations
    assertions (list)   — see below
    min_count (int)     — minimum items for non_empty_result (default 1)

Available assertions (run in order; first failure wins):
    status_200          — HTTP status must be 200
    body_not_empty      — body must not be "" or whitespace-only
    valid_json          — body must parse as JSON
    non_empty_result    — parsed value must be a non-empty list or dict;
                          for dicts, checks items/data/events/results/bills keys
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

DDP_API_BASE_URL = os.getenv("DDP_API_BASE_URL", "https://api.digitaldemocracyproject.org")
DDP_API_KEY = os.getenv("DDP_API_KEY", "")
REQUEST_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Check definitions
#
# CHECKS is built dynamically at startup: one /get_events check per
# configured Voatz org (each org is scoped to a jurisdiction). If org
# configs can't be loaded (e.g. Secrets Manager unavailable), falls back
# to FALLBACK_CHECKS so the script still runs with a best-effort check.
#
# To add non-Voatz checks (e.g. a public /health endpoint), append them
# directly to FALLBACK_CHECKS — they'll always be included.
# ---------------------------------------------------------------------------

FALLBACK_CHECKS: list[dict] = [
    # Uncomment to add non-Voatz checks that always run:
    # {
    #     "name": "ddp_api_health",
    #     "description": "DDP API /health returns 200",
    #     "path": "/health",
    #     "method": "GET",
    #     "params": {},
    #     "assertions": ["status_200", "body_not_empty", "valid_json"],
    # },
]


def _build_checks() -> list[dict]:
    """Generate one /get_events check per configured Voatz org."""
    try:
        from ddp_sync.pipelines.voatz_brevo import _get_org_configs
        orgs = _get_org_configs()
    except Exception as e:
        print(f"Warning: could not load org configs ({e}), using fallback checks", file=sys.stderr)
        return FALLBACK_CHECKS

    if not orgs:
        print("Warning: no org configs found, using fallback checks", file=sys.stderr)
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

    Returns (auth_fields_dict, org_name) on success, or (None, error_message).
    auth_fields_dict contains the keys DDP-API expects: organizationId, WS, Csrf-Token.
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
    """Recursively find the first non-empty list under a known wrapper key.

    Handles flat lists, one-level dicts ({"results": [...]}), and two-level
    dicts ({"events": {"results": [...]}}) so assertions work regardless of
    how deeply the API wraps its payload.

    Returns (list_or_None, dotted_path_string).
    """
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
    """Return an error string if parsed contains fewer than min_count items."""
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
        # Dict with no recognised list key — just ensure it isn't empty.
        if not parsed:
            return "response is an empty object {}"
        return None

    return f"unexpected response type: {type(parsed).__name__}"


def _run_assertions(
    assertions: list[str],
    resp: requests.Response,
    min_count: int = 1,
) -> str | None:
    """Run assertions in order. Returns the first error message or None."""
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

    # Voatz pre-authentication: merge auth tokens into the POST body.
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
        print("No Zapier webhook URL configured — skipping alert", file=sys.stderr)
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
        print(f"Zapier webhook returned {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
    except Exception as e:  # noqa: BLE001
        print(f"Zapier webhook error: {e}", file=sys.stderr)
    return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="DDP API health check")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print results but skip Zapier alert",
    )
    args = parser.parse_args()

    headers: dict = {}
    if DDP_API_KEY:
        headers["Authorization"] = f"Bearer {DDP_API_KEY}"

    checks = _build_checks()

    ts = datetime.now(timezone.utc).isoformat()
    print(f"[{ts}] Running {len(checks)} check(s) against {DDP_API_BASE_URL}")

    results = [run_check(check, DDP_API_BASE_URL, headers) for check in checks]

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        line = f"  [{status}] {r.name}  ({r.duration_ms}ms)"
        if r.error:
            line += f"\n         error: {r.error}"
        if not r.passed and r.body_preview:
            line += f"\n         body:  {r.body_preview}"
        print(line)

    failures = [r for r in results if not r.passed]

    if not failures:
        print(f"\nAll {len(results)} check(s) passed.")
        return 0

    print(f"\n{len(failures)} check(s) FAILED.")

    if args.dry_run:
        print("[dry-run] Skipping Zapier alert.")
    else:
        from ddp_sync.config import get_settings
        webhook_url = get_settings().zapier_webhook_url or ""
        sent = push_health_alert(webhook_url, results)
        if sent:
            print("Zapier alert sent.")
        else:
            print("Zapier alert NOT sent (see stderr).")

    return 1


if __name__ == "__main__":
    sys.exit(main())
