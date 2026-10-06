"""Sender identity for the alerts ddp-sync posts to #automation-errors (SYNC-99).

Every alert here goes through the one Slack app (Agent Smith's `SLACK_BOT_TOKEN`). With only `channel` and
`text` in the payload Slack shows the app's own name, but the failure stream is meant to read as CodeBot, as
ddp-agents' failure-listener triage posts already do. ddp-agents sets that with
`cams.slack_identity.identity_kwargs("codebot")`; it is a separate service and cannot be imported here, so this
mirrors it: the same `CODEBOT_SLACK_USERNAME` / `CODEBOT_SLACK_ICON_EMOJI` variables and the same defaults.

A new alert posts through `ddp_sync.slack_alerts.post_alert`, which applies this identity; do not call
`chat.postMessage` directly (a test fails if any module other than `slack_alerts` does) or write another copy.
Needs the `chat:write.customize` scope on the Slack app: without it Slack ignores `username` and `icon_emoji`
and the post still succeeds under the default name, so this can never make an alert fail.
"""

from __future__ import annotations

import os

DEFAULT_USERNAME = "CodeBot"
DEFAULT_ICON_EMOJI = ":robot_face:"


def codebot_identity() -> dict[str, str]:
    """`username` and `icon_emoji` for a `chat.postMessage` payload. An unset or empty variable means the
    default (an empty `icon_emoji` would otherwise be sent as if it were a choice)."""
    return {
        "username": os.environ.get("CODEBOT_SLACK_USERNAME") or DEFAULT_USERNAME,
        "icon_emoji": os.environ.get("CODEBOT_SLACK_ICON_EMOJI") or DEFAULT_ICON_EMOJI,
    }
