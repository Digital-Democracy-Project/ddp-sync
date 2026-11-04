"""Shared pytest fixtures.

Sets ``asyncio_mode = "auto"`` so we don't need ``@pytest.mark.asyncio`` on
every async test. Also configures structlog into a quiet processor chain so
log noise doesn't clutter test output.
"""

import os

import dotenv
import pytest
import requests

from ddp_sync import slack_alerts

_REAL_REQUESTS_POST = requests.post


class _NoUnreplacedAlertPost:
    """Stands in for `requests` inside `ddp_sync.slack_alerts` for the length of a test (SYNC-148). It behaves as `requests`
    except that `post` fails the test when nothing replaced it: a test that reaches an alert route without faking the HTTP
    call is a test that could page the team. `slack_alerts` catches every `Exception`, so this raises pytest's `Failed`
    (not an `Exception`) to be seen. A test that patches `ddp_sync.slack_alerts.requests.post` or `requests.post` is
    untouched: its replacement is what runs."""

    def __getattr__(self, name):
        return getattr(requests, name)

    def post(self, *args, **kwargs):
        if requests.post is _REAL_REQUESTS_POST:
            pytest.fail(
                "SYNC-148: a test tried to make a real HTTP post through ddp_sync.slack_alerts (an operator alert or a "
                "Slack message). Patch `ddp_sync.slack_alerts.requests.post` in the test.",
                pytrace=False,
            )
        return requests.post(*args, **kwargs)


_ALERT_VARIABLES = (
    "OPERATOR_ALERT_URL", "OPERATOR_ALERT_TOKEN", "OPERATOR_ALERT_VIA_CAMS", "CAMS_API_TOKEN", "SLACK_BOT_TOKEN",
)

# SYNC-148: installed when this file is imported, not only per test, so it also covers test collection (module-level code
# in a test file) and anything still running after a test's fixtures are torn down. pytest imports conftest.py before any
# test module. The alert variables the developer's shell exports are dropped for the whole session; a test that needs one
# sets it with `monkeypatch.setenv`.
for _name in _ALERT_VARIABLES:
    os.environ.pop(_name, None)
# `config._load_from_env()` calls `load_dotenv()`, which walks up from the code's folder: a worktree inside the production
# ddp-sync folder finds that checkout's real `.env` and would put the route and tokens back (the 2026-11-03 3:25 PM false
# "scrape failed for va / mi" alerts). No test needs a `.env`.
def _no_dotenv(*args, **kwargs):
    """Stands in for `dotenv.load_dotenv` for the whole test session (SYNC-148): loads nothing."""
    return False


_NO_DOTENV = _no_dotenv
_NO_UNREPLACED_POST = _NoUnreplacedAlertPost()
dotenv.load_dotenv = _NO_DOTENV
# Whatever route comes back some other way still cannot post: see _NoUnreplacedAlertPost.
slack_alerts.requests = _NO_UNREPLACED_POST


@pytest.fixture(autouse=True)
def _no_operator_alert_route(monkeypatch):
    """SYNC-132: a developer's environment must not send a test's alert to a real CAMS (or change how many
    posts a test sees). The route's own tests set these back."""
    # SYNC-148: CAMS_API_TOKEN too: it is the route's token when OPERATOR_ALERT_TOKEN is empty (the Mac has no other).
    for name in ("OPERATOR_ALERT_URL", "OPERATOR_ALERT_TOKEN", "OPERATOR_ALERT_VIA_CAMS", "CAMS_API_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    # SYNC-138: nor may the direct fallback post a test's alert to real Slack with a developer's token.
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    # SYNC-148: the `.env` loader and the HTTP tripwire are installed once, at import, above; re-asserted here so a test that
    # reloads `slack_alerts` or assigns to either attribute cannot leave the next test unguarded (monkeypatch puts the guard
    # back at teardown, whatever the test did to it).
    monkeypatch.setattr(dotenv, "load_dotenv", _NO_DOTENV)
    monkeypatch.setattr(slack_alerts, "requests", _NO_UNREPLACED_POST)


def pytest_collection_modifyitems(config, items):
    """Auto-mark all coroutine tests with asyncio."""
    for item in items:
        if "asyncio" not in item.keywords and item.get_closest_marker("asyncio") is None:
            # Only mark async test functions
            try:
                import inspect
                func = getattr(item, "function", None)
                if func is not None and inspect.iscoroutinefunction(func):
                    item.add_marker(pytest.mark.asyncio)
            except Exception:
                pass
