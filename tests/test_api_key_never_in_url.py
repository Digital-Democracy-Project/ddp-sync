"""SYNC-68: the api-v3 key is a header on every request, never part of the URL.

httpx logs the full request URL at INFO, so `?apikey=<key>` wrote the real key into every log sink on every call
(found 2026-09-16, again 2026-10-06). These tests run the real httpx logging with the logger mitigation
(`app._configure_logging`) deliberately NOT applied, so they prove the key is out of the URL itself, which is
the property that does not depend on how logging is configured."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from ddp_sync.services import local_openstates_client as client_module

KEY = "key-that-must-never-be-logged"
BASE = "http://10.0.0.11:8002"
SINCE = datetime(2026, 1, 1, tzinfo=UTC)


class _Settings:
    local_openstates_api_base = BASE
    local_openstates_api_key = KEY


# Each call is one of the functions that used to put `apikey` in the query string.
CALLS = {
    "get_archived_bill_text": lambda: client_module.get_archived_bill_text("ocd-bill/1"),
    "get_bill_version_document_text": lambda: client_module.get_bill_version_document_text(
        "ocd-bill/1", version_note="Introduced", version_date="2026-01-01", source_url="https://x/1"),
    "get_current_version_identity": lambda: client_module.get_current_version_identity("ocd-bill/1"),
    "get_archived_changelog_inputs": lambda: client_module.get_archived_changelog_inputs("ocd-bill/1"),
    "get_archived_version_transitions": lambda: client_module.get_archived_version_transitions("ocd-bill/1"),
    "list_current_session_bill_candidates": lambda: client_module.list_current_session_bill_candidates(
        "ut", session_code="2026", limit=5),
    "resolve_touched_sessions": lambda: client_module.resolve_touched_sessions(
        "ut", since=SINCE, max_bills_scanned=5),
    "resolve_touched_sessions_with_override": lambda: client_module.resolve_touched_sessions(
        "ut", since=SINCE, max_bills_scanned=5, api_base=BASE, api_key=KEY),
}


@pytest.fixture
def _http_logging_as_it_was_before_the_mitigation():
    saved = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    for n in saved:
        logging.getLogger(n).setLevel(logging.NOTSET)
    yield
    for n, level in saved.items():
        logging.getLogger(n).setLevel(level)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", sorted(CALLS))
async def test_the_key_is_a_header_and_never_in_the_url_or_the_logs(name, caplog, _http_logging_as_it_was_before_the_mitigation):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"results": [], "pagination": {"max_page": 1}})

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(handler)
    with patch.object(client_module, "get_settings", return_value=_Settings()), \
         patch.object(client_module.httpx, "AsyncClient",
                      side_effect=lambda **kw: real_client(transport=transport, **kw)), \
         caplog.at_level(logging.INFO):
        await CALLS[name]()

    assert seen, f"{name} made no request"
    for request in seen:
        assert request.headers["x-api-key"] == KEY
        assert "apikey" not in str(request.url) and KEY not in str(request.url)
    assert not [r for r in caplog.records if KEY in r.getMessage()], "the key reached a log record"
    assert any(r.name == "httpx" for r in caplog.records), "httpx's own request line should be there for this test to mean anything"


def test_nothing_in_src_puts_apikey_in_a_query_string():
    """A new call site that copies the old `params["apikey"] = ...` pattern fails here, naming file and line."""
    src = Path(__file__).parent.parent / "src"
    pattern = re.compile(r"""["']apikey["']""")
    hits = [
        f"{path.relative_to(src)}:{number}: {line.strip()}"
        for path in src.rglob("*.py")
        for number, line in enumerate(path.read_text().splitlines(), 1)
        if pattern.search(line)
    ]
    assert not hits, hits
