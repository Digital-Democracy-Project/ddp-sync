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
    get_current_version_identity,
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
    # Real bug fix, 2026-08-01: api-v3's jurisdiction filter is
    # case-sensitive for 2-letter codes (routes through the `us` package's
    # lookup(abbr=...)) -- lowercase "fl" silently matched nothing, live.
    assert call.kwargs["params"]["jurisdiction"] == "FL"
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
async def test_explicit_session_code_skips_current_session_resolution():
    """Step 1's own real need (session-targeted, not just 'current') --
    supplying session_code bypasses get_current_session_identifier entirely,
    and that value is what's sent as the session query param and stamped
    on every returned candidate."""
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
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.ingestion.sources.openstates.OpenStatesSource.get_current_session_identifier",
        new=AsyncMock(side_effect=AssertionError("should not be called when session_code is supplied")),
    ):
        result = await list_current_session_bill_candidates("fl", session_code="2026F", limit=10)

    assert result[0]["session_code"] == "2026F"
    assert mock_client.get.await_args.kwargs["params"]["session"] == "2026F"


@pytest.mark.asyncio
async def test_pagination_collects_across_pages_up_to_limit():
    """Real bug fix, 2026-08-01: per_page=limit used to be sent as a single
    request's page size -- silently under-covering any session with more
    bills than `limit`. This asserts a real multi-page loop happens,
    capped at the API's own max per_page (20)."""
    page_1 = MagicMock()
    page_1.status_code = 200
    page_1.json.return_value = {
        "results": [
            {"id": f"ocd-bill/{n}", "identifier": f"HB {n}", "sources": []}
            for n in range(20)
        ],
        "pagination": {"per_page": 20, "page": 1, "max_page": 2, "total_items": 25},
    }
    page_2 = MagicMock()
    page_2.status_code = 200
    page_2.json.return_value = {
        "results": [
            {"id": f"ocd-bill/{n}", "identifier": f"HB {n}", "sources": []}
            for n in range(20, 25)
        ],
        "pagination": {"per_page": 20, "page": 2, "max_page": 2, "total_items": 25},
    }
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=[page_1, page_2])

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026"):
        result = await list_current_session_bill_candidates("fl", limit=25)

    assert len(result) == 25
    assert mock_client.get.await_count == 2
    first_call_params = mock_client.get.await_args_list[0].kwargs["params"]
    second_call_params = mock_client.get.await_args_list[1].kwargs["params"]
    assert first_call_params["per_page"] == "20"
    assert first_call_params["page"] == "1"
    assert second_call_params["page"] == "2"


@pytest.mark.asyncio
async def test_per_page_never_exceeds_api_max_even_when_limit_is_larger():
    """api-v3 itself rejects per_page > 20 (confirmed live) -- this must
    never be sent even when the caller's limit is much larger."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"results": [], "pagination": {"max_page": 1}}
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client), _patch_current_session("2026"):
        await list_current_session_bill_candidates("fl", limit=500)

    assert mock_client.get.await_args.kwargs["params"]["per_page"] == "20"


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
    """Regression guard for the bill_changelog diff-endpoint fix (2026-07-30): api-v3
    attaches raw_text to both the latest version AND the one immediately before it, so
    this must pick the *last* entry (api-v3's own guaranteed latest, SYNC-16/OPEN-92)
    rather than the first non-empty raw_text encountered -- which would be the
    *previous* version's text here, since it's listed first."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    # Previous version listed first, latest last -- the real api-v3 ordering contract.
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
    """A two-version api-v3 bill-detail response, in the order api-v3 itself now
    guarantees (SYNC-16/OPEN-92): correctly stage-ordered, latest last --
    get_archived_changelog_inputs trusts versions[-1]/versions[-2] directly rather
    than re-deriving order itself."""
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

    return {"versions": [introduced, engrossed]}


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
        "old_version_date": "2026-01-01",
        "old_version_note": "Introduced",
        "latest_version_date": "2026-02-01",
        "latest_version_note": "Engrossed",
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


@pytest.mark.asyncio
async def test_current_version_identity_picks_latest_and_title():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "title": "Save our Homes from Excessive Property Taxes",
        # api-v3 now guarantees this ordering itself (latest last, SYNC-16/OPEN-92) --
        # get_current_version_identity trusts versions[-1] directly.
        "versions": [
            {"note": "Introduced", "date": "2026-01-01", "links": []},
            {"note": "Engrossed", "date": "2026-02-01", "links": []},
        ],
        # SYNC-21: read for chamber_classification/jurisdiction_classification below.
        "from_organization": {"classification": "lower", "id": "org-1", "name": "House"},
        "jurisdiction": {"classification": "state", "name": "Florida"},
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_current_version_identity("some-uuid")

    assert result == {
        "version_date": "2026-02-01",
        "version_note": "Engrossed",
        "bill_title": "Save our Homes from Excessive Property Taxes",
        "chamber_classification": "lower",
        "jurisdiction_classification": "state",
    }


@pytest.mark.asyncio
async def test_current_version_identity_classification_keys_default_empty_when_absent():
    """SYNC-21: chamber_classification/jurisdiction_classification are
    additive keys -- a response that doesn't have from_organization/
    jurisdiction at all (shouldn't happen in practice, but this function
    never raises on a missing field) must still return the two keys as
    empty strings, not omit them or raise a KeyError/AttributeError."""
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "title": "A bill",
        "versions": [{"note": "Introduced", "date": "2026-01-01", "links": []}],
    }
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_current_version_identity("some-uuid")

    assert result["chamber_classification"] == ""
    assert result["jurisdiction_classification"] == ""


@pytest.mark.asyncio
async def test_current_version_identity_none_when_no_versions():
    mock_client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"title": "A bill", "versions": []}
    mock_client.get = AsyncMock(return_value=response)

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_current_version_identity("some-uuid")

    assert result is None


@pytest.mark.asyncio
async def test_current_version_identity_none_on_unreachable_api():
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=httpx.RequestError("boom"))

    with patch(
        "ddp_sync.services.local_openstates_client.get_settings",
        return_value=_FakeSettings(),
    ), _patch_async_client(mock_client):
        result = await get_current_version_identity("some-uuid")

    assert result is None
