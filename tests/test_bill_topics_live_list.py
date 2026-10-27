"""SYNC-134: bill_topics answers are checked against the broker's live topic list, not a copied file.

The list comes from ddp-broker-py's `GET /api/bill-topics/taxonomy/` (BROKER-314), kept for a few minutes per broker.
A topic staff add in the broker admin is accepted on the next run; with no list at all, bill_topics is not
generated (no LegBot call, no row); the list's version is recorded on each row written.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.pipelines import bill_artifact_generation as gen
from ddp_sync.services.broker_client import BrokerClientError, get_bill_topics_taxonomy

KWARGS = {
    "bill_openstates_id": "8d71a94e-0000-0000-0000-000000000001",
    "jurisdiction": "FL",
    "session_code": "2026",
    "version_date": "2026-01-05",
    "version_note": "Introduced",
}
LIST_V1 = {"version": "aaaaaaaaaaaa", "max_topics": 4, "topics": ["Drugs", "Education", "Guns"]}
LIST_V2 = {"version": "bbbbbbbbbbbb", "max_topics": 4, "topics": ["Drugs", "Education", "Guns", "Zoning"]}


@dataclass
class _Settings:
    ddp_broker_api_base: str = "http://broker.test"
    ddp_broker_api_token: str = "tok"


@pytest.fixture(autouse=True)
def isolated():
    with patch.dict(gen._bill_topics_cache, clear=True), \
            patch.object(gen, "get_settings", return_value=_Settings()), \
            patch.object(gen, "get_archived_bill_text", new=AsyncMock(return_value="BILL TEXT")):
        yield


def _answer(*topics, primary=None):
    return {"answer": {"primary_topic": primary or topics[0], "topics": list(topics),
                       "insufficient_information": False}, "backend": "mlx"}


async def _run(fetch, dispatch_answer, artifact_type="bill_topics"):
    with patch.object(gen, "get_bill_topics_taxonomy", new=fetch), \
            patch.object(gen, "dispatch_bill_question", new=AsyncMock(return_value=dispatch_answer)) as dispatch, \
            patch.object(gen, "write_bill_artifact", new=AsyncMock(return_value={"id": 1})) as write:
        result = await gen.generate_and_store_bill_artifact(**KWARGS, artifact_type=artifact_type)
    return result, dispatch, write


# ── generation ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_topic_added_on_the_broker_is_accepted_on_the_next_run(monkeypatch):
    fetch = AsyncMock(side_effect=[LIST_V1, LIST_V2])
    monkeypatch.setattr(gen, "_BILL_TOPICS_CACHE_SECONDS", 0)  # the next run is past the cache window

    _, _, write = await _run(fetch, _answer("Zoning", "Drugs"))
    assert write.await_args.kwargs["content"] == "**Primary:** Drugs\n\n- Drugs"

    _, _, write = await _run(fetch, _answer("Zoning", "Drugs"))
    assert write.await_args.kwargs["content"] == "**Primary:** Zoning\n\n- Zoning\n- Drugs"
    assert write.await_args.kwargs["generation_config"] == {"bill_topics_list_version": "bbbbbbbbbbbb"}


@pytest.mark.asyncio
async def test_unreachable_broker_and_no_list_means_no_legbot_call_and_no_row():
    fetch = AsyncMock(side_effect=BrokerClientError("ddp-broker-py unreachable: boom"))
    with patch.object(gen, "get_bill_topics_taxonomy", new=fetch), \
            patch.object(gen, "dispatch_bill_question", new=AsyncMock()) as dispatch, \
            patch.object(gen, "write_bill_artifact", new=AsyncMock()) as write, \
            pytest.raises(BrokerClientError, match="no bill-topics list"):
        await gen.generate_and_store_bill_artifact(**KWARGS, artifact_type="bill_topics")
    dispatch.assert_not_awaited()
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_refresh_keeps_using_the_list_that_broker_gave(monkeypatch):
    fetch = AsyncMock(side_effect=[LIST_V1, BrokerClientError("timeout")])
    monkeypatch.setattr(gen, "_BILL_TOPICS_CACHE_SECONDS", 0)
    await _run(fetch, _answer("Drugs"))
    result, _, write = await _run(fetch, _answer("Guns"))
    assert result["status"] == "complete"
    assert write.await_args.kwargs["generation_config"] == {"bill_topics_list_version": "aaaaaaaaaaaa"}


@pytest.mark.asyncio
async def test_the_list_is_read_once_within_the_cache_window():
    fetch = AsyncMock(return_value=LIST_V1)
    await _run(fetch, _answer("Drugs"))
    await _run(fetch, _answer("Guns"))
    assert fetch.await_count == 1


@pytest.mark.asyncio
async def test_each_broker_has_its_own_list():
    fetch = AsyncMock(side_effect=[LIST_V1, LIST_V2])
    for base in ("http://prod.test", "http://dev.test"):
        with patch.object(gen, "get_bill_topics_taxonomy", new=fetch), \
                patch.object(gen, "dispatch_bill_question", new=AsyncMock(return_value=_answer("Zoning", "Drugs"))), \
                patch.object(gen, "write_bill_artifact", new=AsyncMock(return_value={"id": 1})) as write:
            await gen.generate_and_store_bill_artifact(**KWARGS, artifact_type="bill_topics",
                                                       broker_api_base=base, broker_api_token="t")
    assert fetch.await_args.kwargs == {"broker_api_base": "http://dev.test", "broker_api_token": "t"}
    assert write.await_args.kwargs["generation_config"] == {"bill_topics_list_version": "bbbbbbbbbbbb"}


@pytest.mark.asyncio
async def test_the_cap_comes_from_the_list():
    fetch = AsyncMock(return_value={**LIST_V1, "max_topics": 2})
    _, _, write = await _run(fetch, _answer("Drugs", "Education", "Guns"))
    assert write.await_args.kwargs["content"] == "**Primary:** Drugs\n\n- Drugs\n- Education"


@pytest.mark.asyncio
async def test_no_valid_topics_row_records_the_list_version():
    _, _, write = await _run(AsyncMock(return_value=LIST_V1), _answer("Zoning"))
    assert write.await_args.kwargs["failure_reason"] == "no_valid_topics"
    assert write.await_args.kwargs["generation_config"] == {"bill_topics_list_version": "aaaaaaaaaaaa"}


@pytest.mark.asyncio
async def test_other_artifact_types_do_not_read_the_list():
    fetch = AsyncMock()
    _, _, write = await _run(fetch, {"answer": {"text": "Summary."}, "backend": "mlx"}, artifact_type="bill_summary")
    fetch.assert_not_awaited()
    assert write.await_args.kwargs["generation_config"] is None


# ── the fetch ─────────────────────────────────────────────────────────────────────


def _client_returning(response):
    client = MagicMock()
    client.get = AsyncMock(return_value=response)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.broker_client.httpx.AsyncClient", return_value=cm), client


@pytest.mark.asyncio
async def test_fetch_reads_the_endpoint_with_the_service_token():
    patcher, client = _client_returning(httpx.Response(200, json=LIST_V1))
    with patcher, patch("ddp_sync.services.broker_client.get_settings", return_value=_Settings()):
        assert await get_bill_topics_taxonomy() == LIST_V1
    assert client.get.await_args.args[0] == "http://broker.test/api/bill-topics/taxonomy/"
    assert client.get.await_args.kwargs["headers"] == {"Authorization": "Bearer tok"}


@pytest.mark.parametrize("response", [
    httpx.Response(404, text="Not Found"),  # a broker without BROKER-314
    httpx.Response(401, json={"detail": "Invalid service token."}),
    httpx.Response(200, text="not json"),
    httpx.Response(200, json={"version": "x", "max_topics": 4, "topics": []}),
    httpx.Response(200, json={"version": "x", "max_topics": 0, "topics": ["Drugs"]}),
    httpx.Response(200, json={"version": "x", "max_topics": True, "topics": ["Drugs"]}),
    httpx.Response(200, json={"version": "", "max_topics": 4, "topics": ["Drugs"]}),
    httpx.Response(200, json={"version": "x", "max_topics": 4, "topics": ["Drugs", 7]}),
])
@pytest.mark.asyncio
async def test_fetch_refuses_anything_but_a_usable_list(response):
    patcher, _ = _client_returning(response)
    with patcher, patch("ddp_sync.services.broker_client.get_settings", return_value=_Settings()), \
            pytest.raises(BrokerClientError):
        await get_bill_topics_taxonomy()


@pytest.mark.asyncio
async def test_fetch_unreachable_broker_raises():
    client = MagicMock()
    client.get = AsyncMock(side_effect=httpx.ConnectError("refused"))
    cm = MagicMock(__aenter__=AsyncMock(return_value=client), __aexit__=AsyncMock(return_value=False))
    with patch("ddp_sync.services.broker_client.httpx.AsyncClient", return_value=cm), \
            patch("ddp_sync.services.broker_client.get_settings", return_value=_Settings()), \
            pytest.raises(BrokerClientError, match="unreachable"):
        await get_bill_topics_taxonomy()
