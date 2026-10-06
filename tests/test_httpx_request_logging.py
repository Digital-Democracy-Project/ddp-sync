"""SYNC-68: httpx's own request log line must not reach the logs.

It prints the full request url at INFO, so a credential in the query string (`apikey=`) was written to every
log sink on every api-v3 call (found live 2026-09-16 and 2026-10-06)."""

from __future__ import annotations

import logging

import httpx
import pytest

from ddp_sync import app as app_module

SECRET = "super-secret-key-123"


async def _call_with_key_in_url() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    async with httpx.AsyncClient(transport=transport) as client:
        await client.get("http://10.0.0.11:8002/bills", params={"jurisdiction": "UT", "apikey": SECRET})


@pytest.fixture
def _reset_http_loggers():
    saved = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    for n in saved:
        logging.getLogger(n).setLevel(logging.NOTSET)
    yield
    for n, level in saved.items():
        logging.getLogger(n).setLevel(level)


@pytest.mark.asyncio
async def test_control_httpx_logs_the_url_with_the_key_at_info(caplog, _reset_http_loggers):
    """Without the fix the key is in the log: this is the leak, so the next test proves something."""
    with caplog.at_level(logging.INFO):
        await _call_with_key_in_url()
    assert any(SECRET in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_configured_logging_keeps_the_url_and_the_key_out_of_the_logs(caplog, _reset_http_loggers):
    app_module._configure_logging("INFO")
    with caplog.at_level(logging.INFO):  # root at INFO, as in production
        await _call_with_key_in_url()
    assert not any(SECRET in r.getMessage() for r in caplog.records)
    assert not [r for r in caplog.records if r.name.startswith(("httpx", "httpcore"))]


def test_the_lifespan_uses_the_configured_logging():
    """The service's startup must go through `_configure_logging`, not its own copy of basicConfig."""
    import inspect

    source = inspect.getsource(app_module.lifespan)
    assert "_configure_logging(settings.log_level)" in source and "basicConfig" not in source


def test_an_unknown_level_name_still_falls_back_to_info(_reset_http_loggers):
    app_module._configure_logging("not-a-level")  # must not raise, as the inline version did not
