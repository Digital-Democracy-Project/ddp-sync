"""Tests for services/local_openstates_client.py's bill-enumeration helper
(ddp-infra PLAN-bill-concept-polling.md §0.4) and its bill_changelog archived-
inputs lookup (ddp-infra's diff-endpoint fix, 2026-07-30) -- get_archived_bill_text
itself is already exercised indirectly via test_bill_artifact_generation.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.services.local_openstates_client import (
    get_archived_bill_text,
    get_archived_changelog_inputs,
    list_current_session_bill_candidates,
)


@dataclass
class _FakeSettings:
    local_openstates_api_base: str = "http://localhost:8002"
    local_openstates_api_key: str = "test-key"


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.local_openstates_client.httpx.AsyncClient", return_value=cm)


def _patch_current_session(session_code):
    return patch(
        "ddp_sync.ingestion.sources.openstates.OpenStatesSource.get_current_session_identifier",
        new=AsyncMock(return_value=session_code),
    )


@pytest.mark.asyncio
async def test_returns_empty_list_when_limit_is_zero():
    result = await list_current_session_bill_candidates("fl", limit=0)
    assert result == []


@pytest.mark.asyncio
async def test_returns_empty_list_when_no_current_session_resolved():
    with _patch_current_session(None):
        result = await list_current_session_bill_candidates("fl", limit=10)
    assert result == []


@pytest.mark.asyncio
async def test_happy_path_returns_gov_id_session_and_live_url():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "results": [
            {
                "id": "ocd-bill/11111111-0000-0000-0000-000000000001",
                "identifier": "SJR 1",
                "sources": [{"url": "https://flsenate.gov/bill/1.pdf"}],
            },
            {
                "id": "ocd-bill/22222222-0000-0000-0000-000000000002",
                "identifier": "HB 219",
                "sources": [{"url": "https://flsenate.gov/bill/2.pdf"}],
            },
        ]
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026"):
        result = await list_current_session_bill_candidates("fl", limit=10)

    assert result == [
        {
            "gov_id": "SJR 1",
            "bill_openstates_id": "11111111-0000-0000-0000-000000000001",
            "session_code": "2026",
            "live_url_fallback": "https://flsenate.gov/bill/1.pdf",
        },
        {
            "gov_id": "HB 219",
            "bill_openstates_id": "22222222-0000-0000-0000-000000000002",
            "session_code": "2026",
            "live_url_fallback": "https://flsenate.gov/bill/2.pdf",
        },
    ]
    call = mock_client.get.await_args
    assert call.args[0] == "http://localhost:8002/bills"
    assert call.kwargs["params"]["jurisdiction"] == "fl"
    assert call.kwargs["params"]["session"] == "2026"


@pytest.mark.asyncio
async def test_gov_id_is_the_short_identifier_not_the_uuid_regression():
    """Regression test for the 2026-07-30 live-testing bug: gov_id must
    always fit ConceptStatementSet.gov_id's max_length=20 -- a bare
    OpenStates UUID (36 characters) never does, and every real dispatch
    through the old code failed this exact way, every time."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "results": [{
            "id": "ocd-bill/a3afb726-fac4-41e7-b428-0cae1f4ddada",
            "identifier": "SJR 2F",
            "sources": [{"url": "https://flsenate.gov/bill/x.pdf"}],
        }]
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026F"):
        result = await list_current_session_bill_candidates("fl", limit=10)

    assert len(result[0]["gov_id"]) <= 20
    assert result[0]["gov_id"] == "SJR 2F"
    assert result[0]["bill_openstates_id"] == "a3afb726-fac4-41e7-b428-0cae1f4ddada"


@pytest.mark.asyncio
async def test_truncates_to_limit():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "results": [
            {"id": f"ocd-bill/{n}", "identifier": f"HB {n}", "sources": [{"url": f"https://x/{n}"}]}
            for n in range(5)
        ]
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026"):
        result = await list_current_session_bill_candidates("fl", limit=2)

    assert len(result) == 2


@pytest.mark.asyncio
async def test_missing_sources_yields_empty_live_url_fallback_not_a_crash():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "results": [{
            "id": "ocd-bill/33333333-0000-0000-0000-000000000003",
            "identifier": "SB 42",
            "sources": [],
        }]
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026"):
        result = await list_current_session_bill_candidates("fl", limit=10)

    assert result == [{
        "gov_id": "SB 42",
        "bill_openstates_id": "33333333-0000-0000-0000-000000000003",
        "session_code": "2026",
        "live_url_fallback": "",
    }]


@pytest.mark.asyncio
async def test_bill_missing_identifier_is_skipped_not_crashed():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "results": [
            {"id": "ocd-bill/44444444-0000-0000-0000-000000000004", "sources": []},
            {
                "id": "ocd-bill/55555555-0000-0000-0000-000000000005",
                "identifier": "HB 55",
                "sources": [],
            },
        ]
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026"):
        result = await list_current_session_bill_candidates("fl", limit=10)

    assert len(result) == 1
    assert result[0]["gov_id"] == "HB 55"


@pytest.mark.asyncio
async def test_returns_empty_list_on_unreachable_local_api():
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026"):
        result = await list_current_session_bill_candidates("fl", limit=10)

    assert result == []


@pytest.mark.asyncio
async def test_returns_empty_list_on_error_status():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 500
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026"):
        result = await list_current_session_bill_candidates("fl", limit=10)

    assert result == []


@pytest.mark.asyncio
async def test_returns_empty_list_when_no_local_api_base_configured():
    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(local_openstates_api_base=""),
    ), _patch_current_session("2026"):
        result = await list_current_session_bill_candidates("fl", limit=10)

    assert result == []


# ---------------------------------------------------------------------------
# get_archived_bill_text
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_archived_bill_text_picks_latest_not_first_with_raw_text():
    """Regression guard for the bill_changelog diff-endpoint fix (2026-07-30): api-v3 now
    attaches raw_text to both the latest version AND the one immediately before it, so this
    must explicitly pick latest by (date, note) rather than returning the first non-empty
    raw_text encountered -- which, depending on api-v3's list order, could be the *previous*
    version's text instead."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    # Previous version listed first, on purpose -- proves this isn't relying on list order.
    response.json.return_value = {
        "versions": [
            {
                "note": "Introduced",
                "date": "2026-01-01",
                "links": [{"url": "https://x/introduced.pdf", "raw_text": "Old text."}],
            },
            {
                "note": "Engrossed",
                "date": "2026-02-01",
                "links": [{"url": "https://x/engrossed.pdf", "raw_text": "New text."}],
            },
        ]
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_archived_bill_text("some-uuid")

    assert result == "New text."


# ---------------------------------------------------------------------------
# get_archived_changelog_inputs
# ---------------------------------------------------------------------------

def _versions_response(*, introduced_raw_text=None, engrossed_raw_text=None, engrossed_diff=None):
    """A two-version api-v3 bill-detail response, deliberately listed out of (date, note)
    order -- get_archived_changelog_inputs must sort by (date, note) itself, not trust
    the API's own list order."""
    engrossed = {
        "note": "Engrossed",
        "date": "2026-02-01",
        "links": [{"url": "https://x/engrossed.pdf", "media_type": "application/pdf"}],
    }
    if engrossed_raw_text is not None:
        engrossed["links"][0]["raw_text"] = engrossed_raw_text
    if engrossed_diff is not None:
        engrossed["diff_from_previous_version"] = engrossed_diff

    introduced = {
        "note": "Introduced",
        "date": "2026-01-01",
        "links": [{"url": "https://x/introduced.pdf", "media_type": "application/pdf"}],
    }
    if introduced_raw_text is not None:
        introduced["links"][0]["raw_text"] = introduced_raw_text

    return {"versions": [engrossed, introduced]}


@pytest.mark.asyncio
async def test_changelog_inputs_happy_path():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = _versions_response(
        introduced_raw_text="Archived introduced text.",
        engrossed_raw_text="Archived engrossed text.",
        engrossed_diff="--- Introduced\n+++ Engrossed\n@@ -1 +1 @@\n-old\n+new\n",
    )
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_archived_changelog_inputs("a3afb726-fac4-41e7-b428-0cae1f4ddada")

    assert result == {
        "old_bill_source": "Archived introduced text.",
        "diff_source": "--- Introduced\n+++ Engrossed\n@@ -1 +1 @@\n-old\n+new\n",
    }
    call = mock_client.get.await_args
    assert call.args[0] == (
        "http://localhost:8002/bills/ocd-bill/a3afb726-fac4-41e7-b428-0cae1f4ddada"
    )


@pytest.mark.asyncio
async def test_changelog_inputs_none_when_fewer_than_two_versions():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "versions": [{"note": "Introduced", "date": "2026-01-01", "links": []}]
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_archived_changelog_inputs("some-uuid")

    assert result is None


@pytest.mark.asyncio
async def test_changelog_inputs_none_when_latest_has_no_diff():
    """Latest version exists and even has raw_text, but no diff_from_previous_version --
    e.g. archive_bill_versions() hasn't computed it yet, or this bill predates that
    2026-07-20 pipeline change."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = _versions_response(
        introduced_raw_text="Archived introduced text.",
        engrossed_raw_text="Archived engrossed text.",
        engrossed_diff=None,
    )
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_archived_changelog_inputs("some-uuid")

    assert result is None


@pytest.mark.asyncio
async def test_changelog_inputs_none_when_previous_has_no_raw_text():
    """Latest has a diff, but the previous version's own raw_text isn't archived --
    old_bill_source would be missing, so this must fall back rather than dispatch with
    an empty/missing prior text."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = _versions_response(
        introduced_raw_text=None,
        engrossed_raw_text="Archived engrossed text.",
        engrossed_diff="--- Introduced\n+++ Engrossed\n@@ -1 +1 @@\n-old\n+new\n",
    )
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_archived_changelog_inputs("some-uuid")

    assert result is None


@pytest.mark.asyncio
async def test_changelog_inputs_none_on_unreachable_local_api():
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_archived_changelog_inputs("some-uuid")

    assert result is None


@pytest.mark.asyncio
async def test_changelog_inputs_none_on_error_status():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 404
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_archived_changelog_inputs("some-uuid")

    assert result is None


@pytest.mark.asyncio
async def test_changelog_inputs_none_on_non_json_response():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.side_effect = ValueError("not json")
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_archived_changelog_inputs("some-uuid")

    assert result is None


@pytest.mark.asyncio
async def test_changelog_inputs_none_when_no_local_api_base_configured():
    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(local_openstates_api_base=""),
    ):
        result = await get_archived_changelog_inputs("some-uuid")

    assert result is None
