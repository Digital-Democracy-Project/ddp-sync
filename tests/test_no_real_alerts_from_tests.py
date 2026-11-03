"""SYNC-148: a test run must never be able to send a real operator alert.

2026-11-03, 3:25 PM Eastern: two real alerts ("scrape failed for va / mi: cloud_path.fargate is missing required config")
reached #automation-errors from a test run, 43 seconds after a review session started the suite from a worktree inside
the production ddp-sync folder. `config._load_from_env()` calls `load_dotenv()`, which walks up from the code's folder and
found the production checkout's `.env` (OPERATOR_ALERT_URL and CAMS_API_TOKEN, both set), restoring the alert route after
`conftest.py` had deleted it from the environment. These tests pin the guards in `conftest.py`.

Every address and token below is fake (`.invalid` is a reserved name that cannot resolve); nothing here can reach a real route.
"""

import os
from unittest.mock import patch

import dotenv
import dotenv.main
import pytest

from ddp_sync import config, slack_alerts

FAKE_ENV = (
    "OPERATOR_ALERT_URL=http://alerts.invalid/api/v1/operator-alerts\n"
    "CAMS_API_TOKEN=not-a-real-token\n"
    "OPERATOR_ALERT_TOKEN=not-a-real-token\n"
    "SLACK_BOT_TOKEN=xoxb-not-a-real-token\n"
)
ALERT_VARS = ("OPERATOR_ALERT_URL", "OPERATOR_ALERT_TOKEN", "OPERATOR_ALERT_VIA_CAMS", "CAMS_API_TOKEN", "SLACK_BOT_TOKEN")


def test_a_test_starts_with_no_alert_route_even_if_the_developers_shell_exports_one():
    """The autouse fixture clears every variable the alert route or the direct Slack post can read. (Run the suite with
    these exported to see it matter: the guard exists for exactly that environment.)"""
    for name in ALERT_VARS:
        assert not os.environ.get(name), f"{name} leaked into a test"
    assert slack_alerts._route_settings() is None


def test_a_dotenv_in_a_checkout_cannot_restore_the_alert_route(tmp_path, monkeypatch, request):
    """The 3:25 PM leak. Point the real file lookup at a `.env` that carries a route and tokens (what a worktree inside
    the production folder finds) and load settings the way the app does: the route must still be off afterwards."""
    env_file = tmp_path / ".env"
    env_file.write_text(FAKE_ENV)
    monkeypatch.setattr(dotenv.main, "find_dotenv", lambda *a, **k: str(env_file))
    # load_dotenv writes straight into os.environ, so whatever it does must not outlive this test.
    request.addfinalizer(lambda: [os.environ.pop(name, None) for name in ALERT_VARS])

    config._load_from_env()

    assert slack_alerts._route_settings() is None
    assert not os.environ.get("SLACK_BOT_TOKEN")
    with patch("ddp_sync.slack_alerts.requests.post") as post:
        assert slack_alerts.post_alert("a test alert", source="sync148_test") is False
    post.assert_not_called()


def test_a_real_http_post_from_the_alert_code_fails_the_test_loudly(monkeypatch):
    """Belt and braces for any other way a route could come back: if a test sets a route up and does not replace the
    HTTP call, the post is refused with a test failure (not swallowed: slack_alerts catches every Exception)."""
    monkeypatch.setenv("OPERATOR_ALERT_URL", "http://alerts.invalid/api/v1/operator-alerts")
    monkeypatch.setenv("OPERATOR_ALERT_TOKEN", "not-a-real-token")
    with pytest.raises(pytest.fail.Exception, match="real HTTP post"):
        slack_alerts._post_to_route("a test alert", source="sync148_test", channel=None, dedupe_key=None)
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-not-a-real-token")
    with pytest.raises(pytest.fail.Exception, match="real HTTP post"):
        slack_alerts._post_direct("a test alert", source="sync148_test", channel=None)


def test_a_test_that_replaces_the_http_call_still_works_either_way(monkeypatch):
    """The route's own tests patch `ddp_sync.slack_alerts.requests.post`; older tests patch `requests.post` itself. Both
    keep working: the guard only refuses a post nothing replaced."""
    monkeypatch.setenv("OPERATOR_ALERT_URL", "http://alerts.invalid/api/v1/operator-alerts")
    monkeypatch.setenv("OPERATOR_ALERT_TOKEN", "not-a-real-token")
    with patch("ddp_sync.slack_alerts.requests.post") as post:
        post.return_value.status_code = 202
        assert slack_alerts._post_to_route("x", source="t", channel=None, dedupe_key=None) is True
    with patch("requests.post") as post:
        post.return_value.status_code = 202
        assert slack_alerts._post_to_route("x", source="t", channel=None, dedupe_key=None) is True


class TestTheGuardsComeBackAfterATestUndoesThem:
    """The fixture re-asserts the guards for every test, so a test that reassigns them (or reloads `slack_alerts`) does not
    leave the following test unguarded. The two tests run in definition order (no random-order plugin is installed)."""

    def test_first_a_test_that_removes_both_guards(self):
        import requests

        dotenv.load_dotenv = dotenv.main.load_dotenv  # the real loader
        slack_alerts.requests = requests  # the real HTTP library

    def test_then_the_next_test_starts_guarded_again(self):
        assert dotenv.load_dotenv is not dotenv.main.load_dotenv
        with pytest.raises(pytest.fail.Exception, match="real HTTP post"):
            slack_alerts.requests.post("http://alerts.invalid/x")
