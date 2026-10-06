"""The one way ddp-sync posts an alert to Slack.

Every alert goes to the shared alerts channel (`HEALTH_ALERT_SLACK_CHANNEL`, default
#automation-errors) through the one Slack app's `SLACK_BOT_TOKEN`. Slack labels a message with the
app's own name (Agent Smith) unless the post says otherwise, so the identity is set here, once,
rather than at each call site where a new alert could forget it.

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


def post_alert(text: str, *, source: str, channel: str | None = None) -> bool:
    """Post `text` to the alerts channel as CodeBot. Returns True if Slack accepted it.

    `source` names the caller in the log line when sending fails or isn't configured, so a
    swallowed failure can still be traced back to the alert that wanted to send it.
    """
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
