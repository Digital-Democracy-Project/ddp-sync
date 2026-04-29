"""Shared pytest fixtures.

Sets ``asyncio_mode = "auto"`` so we don't need ``@pytest.mark.asyncio`` on
every async test. Also configures structlog into a quiet processor chain so
log noise doesn't clutter test output.
"""

import pytest


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
