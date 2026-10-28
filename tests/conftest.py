"""Shared pytest fixtures.

Sets ``asyncio_mode = "auto"`` so we don't need ``@pytest.mark.asyncio`` on
every async test. Also configures structlog into a quiet processor chain so
log noise doesn't clutter test output.
"""

import pytest


@pytest.fixture(autouse=True)
def _no_operator_alert_route(monkeypatch):
    """SYNC-132: a developer's environment must not send a test's alert to a real CAMS (or change how many
    posts a test sees). The route's own tests set these back."""
    for name in ("OPERATOR_ALERT_URL", "OPERATOR_ALERT_TOKEN", "OPERATOR_ALERT_VIA_CAMS"):
        monkeypatch.delenv(name, raising=False)


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
