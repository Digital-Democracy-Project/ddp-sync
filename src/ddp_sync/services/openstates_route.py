"""The ONE way ddp-sync reaches OpenStates data: our own api-v3 (SYNC-157).

We scrape every state we serve, so ddp-sync never calls the public v3.openstates.org API. Before SYNC-157 any state
missing from the per-host `DDP_OPENSTATES_JURISDICTIONS` list fell through to the public API (a default chosen as
"safer than refusing to boot" when the public service was our source); on 2026-11-12 that list lacked pa, ne and la
on the EC2 host. That list, the public base and its key are deleted.

`api_v3(settings, jurisdiction)` answers `(base, key)` of THIS host's api-v3:

- a host with no CAMS access and an RDS-backed api-v3 configured (EC2): that one (`rds_openstates_api_base`);
- otherwise (the Mac, dev, tests): the local api-v3 (`local_openstates_api_base`; the Mac's reads the RDS replica).
  This is SYNC-93's split, which the ingestion source already used; the other callers used only the local base, so on
  EC2 they would have reached nothing (localhost there is the ddp-sync container). An empty base raises.

With a jurisdiction, the state must be in the jurisdiction manifest (ddp-open-states `jurisdictions.yaml`): a state we
do not serve raises `NoOpenStatesRoute` and alerts once, instead of fetching anyone else's data. When the manifest
cannot be read the call still goes to our own api-v3 (it can only hold our data), with one log line. Without a
jurisdiction (an opaque-id lookup, a crawl) the call goes to our api-v3, which holds only the states we serve.

Do not add a public base, a key for it, or a per-host routing list back; `tests/test_sync_157_no_public_openstates.py`
fails if any code builds a URL on the public host.
"""

from __future__ import annotations

import threading

import structlog

from ddp_sync.services.jurisdiction_enrollment import default_root, load_manifest

logger = structlog.get_logger()

PUBLIC_HOST = "v3.openstates.org"  # named only so the guard test and this module can refuse it


class NoOpenStatesRoute(RuntimeError):
    """ddp-sync cannot serve this request from our own api-v3 (SYNC-157); it never falls back to the public API."""


_alerted: set = set()
_lock = threading.Lock()


def _alert_once(key: str, text: str) -> None:
    with _lock:
        if key in _alerted:
            return
        _alerted.add(key)
    logger.error("openstates_route_refused", reason=text)
    try:
        from ddp_sync.slack_alerts import post_alert

        post_alert(f":warning: ddp-sync (SYNC-157): {text}", source="openstates_route", dedupe_key=f"openstates_route:{key}")
    except Exception as exc:  # noqa: BLE001 - an alert failure must not hide the refusal itself
        logger.warning("openstates_route_alert_failed", error=str(exc))


def _host_api_v3(settings) -> tuple[str, str]:
    # SYNC-93's split, unchanged: a host with no CAMS access (`cams_api_token` unset, the EC2 signal) and an RDS-backed
    # api-v3 configured reads that one; otherwise (the Mac, dev, tests) the local one.
    if not settings.cams_api_token and settings.rds_openstates_api_base:
        return settings.rds_openstates_api_base, settings.rds_openstates_api_key
    return settings.local_openstates_api_base, settings.local_openstates_api_key


def _manifest_code(jurisdiction: str) -> str:
    code = str(jurisdiction).strip().lower()
    return "us" if code == "usa" else code


def api_v3(settings, jurisdiction: str | None = None) -> tuple[str, str]:
    """`(base, key)` of this host's own api-v3 for `jurisdiction` (or for a call with no state). Raises
    `NoOpenStatesRoute` (and alerts once) for a state the manifest does not serve, or when this host has no api-v3
    configured. Never returns the public API."""
    base, key = _host_api_v3(settings)
    if not base or PUBLIC_HOST in base.lower():  # any casing of the public host is refused (pm-review)
        problem = ("this host has no api-v3 of its own configured (RDS_OPENSTATES_API_BASE on EC2, "
                   "LOCAL_OPENSTATES_API_BASE on the Mac)" if not base else f"the api-v3 base points at {PUBLIC_HOST}")
        _alert_once(f"no-base:{base}", f"{problem}; refusing the OpenStates call instead of using the public API")
        raise NoOpenStatesRoute(problem)
    if jurisdiction is None:
        return base, key
    code = _manifest_code(jurisdiction)
    manifest = load_manifest(default_root())
    if manifest is None:
        logger.warning("openstates_route_manifest_unreadable_using_own_api", jurisdiction=code, api_base=base)
        return base, key
    if code not in manifest:
        text = f"`{code}` is not in the jurisdiction manifest, so ddp-sync does not serve it and will not fetch it from the public API"
        _alert_once(f"unknown:{code}", text)
        raise NoOpenStatesRoute(text)
    return base, key
