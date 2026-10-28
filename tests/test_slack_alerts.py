"""`slack_alerts.post_alert`, the single path every ddp-sync alert takes to Slack (SYNC-99), and its CAMS
route-first step (SYNC-132, PLAN-cams-operator-alerts)."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from structlog.testing import capture_logs

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


# --- SYNC-132: the CAMS route first, the direct post as the fallback ---------------------------------------------

ROUTE_URL = "http://10.0.0.8:8000/api/v1/operator-alerts"
CAMS_TOKEN = "cams-secret-token-value"
SLACK_URL = "https://slack.com/api/chat.postMessage"


@pytest.fixture
def route(monkeypatch):
    monkeypatch.setenv("OPERATOR_ALERT_URL", ROUTE_URL)
    monkeypatch.setenv("OPERATOR_ALERT_TOKEN", CAMS_TOKEN)


def _route_resp(status=202, body=None, text=""):
    return MagicMock(status_code=status, ok=status < 400, text=text or str(body or {}), json=lambda: body or {})


def _by_url(post, url):
    return [c for c in post.call_args_list if c.args[0] == url]


def test_the_route_takes_the_alert_and_slack_is_not_called(route):
    with patch("requests.post", return_value=_route_resp(202, {"status": "queued", "id": "a1"})) as post:
        assert post_alert("hello", source="t") is True
    assert post.call_count == 1
    call = post.call_args
    assert call.args[0] == ROUTE_URL
    assert call.kwargs["headers"] == {"Authorization": f"Bearer {CAMS_TOKEN}"}
    assert call.kwargs["json"] == {"v": 1, "source": "ddp-sync/t", "text": "hello"}
    assert call.kwargs["timeout"] == 5
    assert call.kwargs["allow_redirects"] is False


def test_a_cams_sent_alert_is_logged_and_a_fallback_is_not(route):
    """Route delivery must be distinguishable from the direct fallback in the logs."""
    with capture_logs() as logs, patch("requests.post", return_value=_route_resp(202, {"status": "queued"})):
        post_alert("x", source="t")
    assert [log["event"] for log in logs] == ["operator_alert_sent_via_cams"]
    with capture_logs() as logs, patch("requests.post", side_effect=[_route_resp(503), _resp()]):
        post_alert("x", source="t")
    assert "operator_alert_sent_via_cams" not in [log["event"] for log in logs]


def test_channel_and_dedupe_key_ride_along_only_when_given(route):
    with patch("requests.post", return_value=_route_resp(202, {"status": "queued"})) as post:
        post_alert("a", source="t", channel="#other", dedupe_key="fork:people")
    assert post.call_args.kwargs["json"] == {
        "v": 1, "source": "ddp-sync/t", "text": "a", "channel": "#other", "dedupe_key": "fork:people",
    }


@pytest.mark.parametrize("status", ["duplicate", "dropped_rate_limited"])
def test_a_suppressed_alert_is_a_success_and_is_not_posted_again(route, status):
    """Both are 202: an identical alert was already sent, or a summary notice was (plan 4.13)."""
    with patch("requests.post", return_value=_route_resp(202, {"status": status})) as post:
        assert post_alert("x", source="t") is True
    assert _by_url(post, SLACK_URL) == []


@pytest.mark.parametrize("status", [400, 401, 422, 500, 503, 302], ids=lambda s: f"http_{s}")
def test_any_non_2xx_answer_falls_back_to_one_direct_post_as_codebot(route, status):
    """A rejection, a bad token, a server error, a redirect: the alert still arrives, and the rejection is logged."""
    answers = [_route_resp(status, text="nope"), _resp()]
    with capture_logs() as logs, patch("requests.post", side_effect=answers) as post:
        assert post_alert("x", source="t") is True
    assert [c.args[0] for c in post.call_args_list] == [ROUTE_URL, SLACK_URL]
    slack = post.call_args_list[1].kwargs
    assert slack["json"]["username"] == "CodeBot" and slack["json"]["icon_emoji"] == ":robot_face:"
    rejected = [log for log in logs if log["event"] == "operator_alert_rejected"]
    assert rejected and rejected[0]["status"] == status and rejected[0]["source"] == "t"


@pytest.mark.parametrize("error", [ConnectionError("refused"), TimeoutError("slow")], ids=["refused", "timeout"])
def test_a_route_that_cannot_be_reached_falls_back_to_the_direct_post(route, error):
    with capture_logs() as logs, patch("requests.post", side_effect=[error, _resp()]) as post:
        assert post_alert("x", source="t") is True
    assert [c.args[0] for c in post.call_args_list] == [ROUTE_URL, SLACK_URL]
    assert any(log["event"] == "operator_alert_cams_unreachable" for log in logs)


def test_the_route_and_the_direct_post_both_failing_reports_failure_without_raising(route):
    with patch("requests.post", side_effect=[ConnectionError("down"), ConnectionError("down")]):
        assert post_alert("x", source="t") is False


@pytest.mark.parametrize(
    "env",
    [
        {},                                                                  # no URL: this host posts directly
        {"OPERATOR_ALERT_URL": ROUTE_URL},                                   # a URL but no token
        {"OPERATOR_ALERT_URL": ROUTE_URL, "OPERATOR_ALERT_TOKEN": CAMS_TOKEN, "OPERATOR_ALERT_VIA_CAMS": "false"},
        {"OPERATOR_ALERT_TOKEN": CAMS_TOKEN},                                # a token but no URL
    ],
    ids=["no_url", "no_token", "switched_off", "no_url_with_token"],
)
def test_a_host_not_set_up_for_the_route_posts_directly_and_never_calls_cams(monkeypatch, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    with patch("requests.post", return_value=_resp()) as post:
        assert post_alert("x", source="t") is True
    assert [c.args[0] for c in post.call_args_list] == [SLACK_URL]


def test_the_alert_only_token_wins_and_the_macs_cams_token_is_the_fallback(monkeypatch):
    monkeypatch.setenv("OPERATOR_ALERT_URL", ROUTE_URL)
    monkeypatch.setenv("CAMS_API_TOKEN", "mac-cams-token")
    with patch("requests.post", return_value=_route_resp(202)) as post:
        post_alert("x", source="t")
        monkeypatch.setenv("OPERATOR_ALERT_TOKEN", "alert-token")
        post_alert("y", source="t")
    assert [c.kwargs["headers"]["Authorization"] for c in post.call_args_list] == [
        "Bearer mac-cams-token", "Bearer alert-token",
    ]


def test_the_cams_token_goes_only_to_the_route_url(route):
    """It is the main CAMS token: the Slack fallback must carry the Slack token and nothing else."""
    with patch("requests.post", side_effect=[_route_resp(503), _resp()]) as post:
        post_alert("x", source="t")
    for call in post.call_args_list:
        sent = repr(call.kwargs)
        if call.args[0] == ROUTE_URL:
            assert CAMS_TOKEN in sent
        else:
            assert CAMS_TOKEN not in sent and call.args[0] == SLACK_URL


def test_the_cams_token_never_appears_in_a_log_line(route):
    """An exception or a response body that echoes the token must not carry it into the logs."""
    echoed = f"401 for Bearer {CAMS_TOKEN}"
    with capture_logs() as logs, patch("requests.post", side_effect=[ConnectionError(echoed), _resp()]):
        post_alert("x", source="t")
    with capture_logs() as logs2, patch("requests.post", side_effect=[_route_resp(401, text=echoed), _resp()]):
        post_alert("x", source="t")
    assert logs and logs2
    assert CAMS_TOKEN not in repr(logs) and CAMS_TOKEN not in repr(logs2)


def test_every_alert_source_is_a_valid_route_source():
    """The route rejects a `source` outside `<service>/<job>` = `[a-z0-9._-]{1,40}/[a-z0-9._-]{1,40}` with a 422,
    which would fall back to the direct post and read as a sender bug (ddp-agents `cams.api.routes`). Every
    `post_alert` caller passes a literal source, so a bad one is caught here, not in production."""
    pattern = re.compile(r"^[a-z0-9._-]{1,40}/[a-z0-9._-]{1,40}$")
    src = Path(__file__).parent.parent / "src"
    seen = []
    for path in src.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Call) and getattr(node.func, "id", getattr(node.func, "attr", "")) == "post_alert":
                kw = {k.arg: k.value for k in node.keywords}
                assert isinstance(kw.get("source"), ast.Constant), f"{path.name}:{node.lineno} needs a literal source"
                assert pattern.match(f"ddp-sync/{kw['source'].value}"), f"{path.name}:{node.lineno} {kw['source'].value!r}"
                seen.append(kw["source"].value)
    assert len(seen) >= 8, seen
