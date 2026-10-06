"""SYNC-99: ddp-sync's #automation-errors alerts post as CodeBot, not as the Slack app (Agent Smith).

Every alert path posts `chat.postMessage` with `username` and `icon_emoji`; the variables override the
defaults; the text, channel and CAMS report are unchanged."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from ddp_sync.pipelines import api_health_check as health
from ddp_sync.pipelines import openstates_archive as archive
from ddp_sync.pipelines import openstates_scrape as scrape
from ddp_sync.slack_identity import (
    DEFAULT_ICON_EMOJI,
    DEFAULT_USERNAME,
    codebot_identity,
)


def _failed_check():
    return [health.CheckResult(name="bills", description="d", passed=False, error="HTTP 500")]


# (module whose `requests.post` is used, a call that triggers the alert, a snippet its text must keep)
ALERT_PATHS = {
    "scrape_failure": (scrape, lambda: scrape._alert_scrape_failure("ut", "exit 1", 12.0), "OpenStates scrape failed: ut"),
    "sustained_block": (scrape, lambda: scrape._alert_sustained_block("mi", 3, 4), "mi has been blocked 3 of the last 4"),
    "quiet_jurisdiction": (scrape, lambda: scrape._alert_quiet_jurisdiction("az", 5), "az has imported no new bills"),
    "archive_failure": (archive, lambda: archive._alert_archive_failure("ma", "gave up", 99.0), "OpenStates archive failed: ma"),
    "knowledge_base": (archive, lambda: archive._post_slack_alert("kb is behind"), ":warning: kb is behind"),
    "health_check": (health, lambda: health.push_health_alert("", _failed_check()), "DDP API Health Check Failed"),
}


@pytest.fixture(autouse=True)
def _slack_env(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.delenv("HEALTH_ALERT_SLACK_CHANNEL", raising=False)
    monkeypatch.delenv("CAMS_API_TOKEN", raising=False)
    monkeypatch.delenv("CODEBOT_SLACK_USERNAME", raising=False)
    monkeypatch.delenv("CODEBOT_SLACK_ICON_EMOJI", raising=False)


def _post(module, trigger):
    with patch.object(module.requests, "post") as post:
        post.return_value = MagicMock(ok=True, text="", json=lambda: {"ok": True})
        trigger()
    assert post.call_count == 1
    return post.call_args


def test_the_identity_defaults_match_the_agents_registry():
    assert codebot_identity() == {"username": "CodeBot", "icon_emoji": ":robot_face:"}
    assert (DEFAULT_USERNAME, DEFAULT_ICON_EMOJI) == ("CodeBot", ":robot_face:")


def test_the_identity_variables_override_and_an_empty_value_means_the_default(monkeypatch):
    monkeypatch.setenv("CODEBOT_SLACK_USERNAME", "Failure Bot")
    monkeypatch.setenv("CODEBOT_SLACK_ICON_EMOJI", ":codebot:")
    assert codebot_identity() == {"username": "Failure Bot", "icon_emoji": ":codebot:"}
    monkeypatch.setenv("CODEBOT_SLACK_USERNAME", "")
    monkeypatch.setenv("CODEBOT_SLACK_ICON_EMOJI", "")
    assert codebot_identity() == {"username": "CodeBot", "icon_emoji": ":robot_face:"}


@pytest.mark.parametrize("path", sorted(ALERT_PATHS))
def test_every_alert_path_posts_as_codebot_with_its_text_and_channel_unchanged(path):
    module, trigger, snippet = ALERT_PATHS[path]
    call = _post(module, trigger)
    body = call.kwargs["json"]
    assert body["username"] == "CodeBot" and body["icon_emoji"] == ":robot_face:"
    assert body["channel"] == "#automation-errors" and snippet in body["text"]
    assert set(body) == {"channel", "text", "username", "icon_emoji"}  # nothing else rides along
    assert call.kwargs["headers"] == {"Authorization": "Bearer xoxb-test"}


@pytest.mark.parametrize("path", sorted(ALERT_PATHS))
def test_every_alert_path_follows_the_identity_variables(path, monkeypatch):
    monkeypatch.setenv("CODEBOT_SLACK_USERNAME", "Failure Bot")
    monkeypatch.setenv("CODEBOT_SLACK_ICON_EMOJI", ":codebot:")
    module, trigger, _ = ALERT_PATHS[path]
    body = _post(module, trigger).kwargs["json"]
    assert (body["username"], body["icon_emoji"]) == ("Failure Bot", ":codebot:")


def test_the_cams_report_is_untouched_by_the_identity(monkeypatch):
    """Only the Slack payload gains the persona: the CAMS failure report keeps its own shape."""
    monkeypatch.setenv("CAMS_API_TOKEN", "cams-token")
    with patch.object(scrape.requests, "post") as post:
        post.return_value = MagicMock(ok=True, text="", json=lambda: {"ok": True})
        scrape._alert_scrape_failure("ut", "exit 1", 12.0)
    slack, cams = post.call_args_list
    assert "username" in slack.kwargs["json"]
    payload = json.loads(cams.kwargs["data"])
    assert payload["service"] == "ddp-sync" and "username" not in payload and "icon_emoji" not in payload


def test_only_the_alert_helper_posts_to_slack():
    """Every alert goes through `slack_alerts.post_alert`, which owns the token, channel, timeout and
    CodeBot identity. A second module posting to Slack directly could skip the identity and post as Agent
    Smith again (SYNC-99), so the Slack URL may appear in no other module under src (as SYNC-42's scan does
    for provenance)."""
    from pathlib import Path

    src = Path(__file__).parent.parent / "src"
    allowed = Path("ddp_sync") / "slack_alerts.py"
    offenders = [
        str(path.relative_to(src))
        for path in src.rglob("*.py")
        if "slack.com/api/chat.postMessage" in path.read_text() and path.relative_to(src) != allowed
    ]
    assert not offenders, f"post through ddp_sync.slack_alerts.post_alert instead: {offenders}"
    assert "**codebot_identity()" in (src / allowed).read_text()
