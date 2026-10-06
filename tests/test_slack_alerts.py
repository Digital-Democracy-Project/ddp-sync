"""`slack_alerts.post_alert`, the single path every ddp-sync alert takes to Slack (SYNC-99)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ddp_sync.slack_alerts import post_alert


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.delenv("HEALTH_ALERT_SLACK_CHANNEL", raising=False)
    monkeypatch.delenv("CODEBOT_SLACK_USERNAME", raising=False)
    monkeypatch.delenv("CODEBOT_SLACK_ICON_EMOJI", raising=False)


def _resp(ok=True, body=None, text=""):
    return MagicMock(ok=ok, text=text, json=lambda: body if body is not None else {"ok": True})


def test_posts_to_the_alerts_channel_as_codebot_and_reports_success():
    with patch("requests.post", return_value=_resp()) as post:
        assert post_alert("hello", source="t") is True
    assert post.call_args.args[0] == "https://slack.com/api/chat.postMessage"
    assert post.call_args.kwargs["headers"] == {"Authorization": "Bearer xoxb-test"}
    assert post.call_args.kwargs["json"] == {
        "channel": "#automation-errors", "text": "hello", "username": "CodeBot", "icon_emoji": ":robot_face:",
    }
    assert post.call_args.kwargs["timeout"] == 15


def test_channel_comes_from_the_env_and_an_explicit_channel_wins(monkeypatch):
    monkeypatch.setenv("HEALTH_ALERT_SLACK_CHANNEL", "#ops")
    with patch("requests.post", return_value=_resp()) as post:
        post_alert("a", source="t")
        post_alert("b", source="t", channel="#other")
    assert [c.kwargs["json"]["channel"] for c in post.call_args_list] == ["#ops", "#other"]


def test_no_token_posts_nothing_and_reports_failure(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN")
    with patch("requests.post") as post:
        assert post_alert("x", source="t") is False
    post.assert_not_called()


@pytest.mark.parametrize(
    "response",
    [_resp(ok=False, text="boom"), _resp(body={"ok": False, "error": "not_in_channel"})],
    ids=["http_error", "slack_rejected"],
)
def test_a_rejected_post_reports_failure_without_raising(response):
    with patch("requests.post", return_value=response):
        assert post_alert("x", source="t") is False


def test_a_network_error_is_swallowed():
    with patch("requests.post", side_effect=ConnectionError("down")):
        assert post_alert("x", source="t") is False
