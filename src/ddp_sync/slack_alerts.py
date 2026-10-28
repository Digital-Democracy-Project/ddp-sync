"""The one way ddp-sync posts an alert to Slack.

Every alert goes to the shared alerts channel (`HEALTH_ALERT_SLACK_CHANNEL`, default
#automation-errors) through the one Slack app's `SLACK_BOT_TOKEN`. Slack labels a message with the
app's own name (Agent Smith) unless the post says otherwise, so the identity is set here, once,
rather than at each call site where a new alert could forget it.

Route first (SYNC-132, PLAN-cams-operator-alerts): when `OPERATOR_ALERT_URL` and a token are set, the alert
is sent to CAMS's `POST /api/v1/operator-alerts`, which queues it, retries, de-duplicates and records a refused
alert where `cams status` shows it. Anything other than a 2xx answer (a rejection, a 401, a 5xx, a timeout, no
route) falls back to the direct post below, so an alert is never lost because CAMS or the tunnel is down. Via
the route the channel comes from CAMS's own map (`config/operator_alert_channels.yaml`);
`HEALTH_ALERT_SLACK_CHANNEL` applies only to the direct post.

Never raises: an alert that fails to send is logged, not allowed to break the pipeline that
raised it. `test_only_the_alert_helper_posts_to_slack` (tests/test_slack_identity_alerts.py) fails if
any other module posts to Slack directly, so a new alert can't bypass this.
"""

from __future__ import annotations

import os

import requests
import structlog

from ddp_sync.slack_identity import codebot_identity

logger = structlog.get_logger()

DEFAULT_ALERT_CHANNEL = "#automation-errors"
_SLACK_POST_URL = "https://slack.com/api/chat.postMessage"
_ROUTE_TIMEOUT_S = 5


def post_alert(text: str, *, source: str, channel: str | None = None, dedupe_key: str | None = None) -> bool:
    """Send `text` as an operator alert. Returns True if CAMS queued it or Slack accepted it.

    Tries the CAMS route first when this host is set up for it, then posts to Slack directly as CodeBot.
    `source` names the caller in the log line when sending fails or isn't configured, so a
    swallowed failure can still be traced back to the alert that wanted to send it; it also becomes the
    route's `source` (`ddp-sync/<source>`), so it must match `[a-z0-9._-]{1,40}` (a test checks every caller).
    `dedupe_key` (optional) makes CAMS post a repeat of the same alert once a day instead of every time.
    """
    if _post_to_route(text, source=source, channel=channel, dedupe_key=dedupe_key):
        return True
    return _post_direct(text, source=source, channel=channel)


def _route_settings() -> tuple[str, str] | None:
    """(url, token) when this host should try the CAMS route, else None (the direct post is used).

    Read from the environment on purpose, not `get_settings()`: `cams_api_token` there is the code's
    "this host is the Mac" signal (`openstates_archive._mac_capable()`, replica routing in `ingestion/sources/
    openstates.py`), so EC2 must never get it. EC2 gets this one alert-only variable instead
    (`OPERATOR_ALERT_TOKEN`, rendered from the secret's `operator_alert_token`); the Mac already has
    `CAMS_API_TOKEN` and falls back to it. Both carry the same CAMS token value.
    """
    if os.getenv("OPERATOR_ALERT_VIA_CAMS", "true").strip().lower() == "false":
        return None
    url = os.getenv("OPERATOR_ALERT_URL", "").strip()
    token = os.getenv("OPERATOR_ALERT_TOKEN", "").strip() or os.getenv("CAMS_API_TOKEN", "").strip()
    return (url, token) if url and token else None


def _post_to_route(text: str, *, source: str, channel: str | None, dedupe_key: str | None) -> bool:
    """POST the alert to CAMS. True only for a 2xx answer (`queued`, `duplicate` and `dropped_rate_limited`
    are all 202: the alert was sent or deliberately summarised, so the caller must not post it again).

    The token is the main CAMS token, so it goes only to `OPERATOR_ALERT_URL` (no redirects are followed) and
    is scrubbed from anything logged."""
    route = _route_settings()
    if route is None:
        return False
    url, token = route
    body: dict = {"v": 1, "source": f"ddp-sync/{source}", "text": text}
    if channel:
        body["channel"] = channel
    if dedupe_key:
        body["dedupe_key"] = dedupe_key
    try:
        resp = requests.post(
            url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=_ROUTE_TIMEOUT_S,
            allow_redirects=False,
        )
        if 200 <= resp.status_code < 300:
            logger.info("operator_alert_sent_via_cams", source=source, http_status=resp.status_code)
            return True
        logger.error(
            "operator_alert_rejected", source=source, status=resp.status_code, body=resp.text[:200].replace(token, "[token]"),
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("operator_alert_cams_unreachable", source=source, error=str(e).replace(token, "[token]"))
    return False


def _post_direct(text: str, *, source: str, channel: str | None) -> bool:
    """Post to Slack as CodeBot with `SLACK_BOT_TOKEN`: the path every alert took before the route, and the
    fallback now."""
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        logger.warning("slack_alert_not_sent_no_token", source=source, text=text[:200])
        return False
    channel = channel or os.getenv("HEALTH_ALERT_SLACK_CHANNEL", DEFAULT_ALERT_CHANNEL)
    try:
        resp = requests.post(
            _SLACK_POST_URL,
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": text, **codebot_identity()},
            timeout=15,
        )
        if resp.ok and resp.json().get("ok"):
            return True
        logger.error("slack_alert_failed", source=source, response=resp.text[:200])
    except Exception as e:  # noqa: BLE001
        logger.error("slack_alert_error", source=source, error=str(e))
    return False
