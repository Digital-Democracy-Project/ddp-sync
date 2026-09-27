"""Tests for SYNC-77: LegislatorSyncService must normalize the CMS's bare-UUID
`openstates_id` to OpenStates' `ocd-person/{uuid}` format before using it in any
OpenStates-facing request or comparison. Without this, every sponsor lookup and
vote-attribution match silently fails (OpenStates returns 200 with zero results,
not an error), so `legislator_sync` was producing zero real bills/votes for every
legislator, every week, for ~8 months.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines.legislator_sync import LegislatorSyncService, _ocd_person_id

_BARE_ID = "c495bde9-29b3-5e71-ab0e-dedff0de9a84"
_PREFIXED_ID = f"ocd-person/{_BARE_ID}"


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.pipelines.legislator_sync.httpx.AsyncClient", return_value=cm)


def _make_service(**settings_overrides) -> LegislatorSyncService:
    settings = SyncSettings(
        openai_api_key="test-openai-key",
        openstates_api_key="public-key",
        openstates_api_base="https://v3.openstates.org",
        rds_openstates_api_base="http://localhost:8002",
        rds_openstates_api_key="local-key",
        **settings_overrides,
    )
    return LegislatorSyncService(settings)


def _mock_response(data: dict) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = data
    response.raise_for_status.return_value = None
    return response


# --- _ocd_person_id -----------------------------------------------------------


def test_ocd_person_id_adds_prefix_to_bare_uuid():
    assert _ocd_person_id(_BARE_ID) == _PREFIXED_ID


def test_ocd_person_id_is_idempotent_on_already_prefixed_id():
    assert _ocd_person_id(_PREFIXED_ID) == _PREFIXED_ID


def test_ocd_person_id_passes_through_empty_string():
    assert _ocd_person_id("") == ""


# --- _get_sponsor_name ----------------------------------------------------------


@pytest.mark.asyncio
async def test_get_sponsor_name_sends_prefixed_id_given_bare_uuid():
    service = _make_service()
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {"results": [{"family_name": "Kelly"}]}
    )

    with _patch_async_client(mock_client):
        name = await service._get_sponsor_name(_BARE_ID)

    assert name == "Kelly"
    _, called_kwargs = mock_client.get.call_args
    assert ("id", _PREFIXED_ID) in called_kwargs["params"]


# --- fetch_sponsored_bills: sponsorship-match comparison -----------------------


@pytest.mark.asyncio
async def test_fetch_sponsored_bills_matches_sponsorship_given_bare_uuid():
    service = _make_service(ddp_openstates_jurisdictions=[])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {
            "results": [
                {
                    "identifier": "HB 1",
                    "sponsorships": [{"person": {"id": _PREFIXED_ID}}],
                }
            ],
            "pagination": {"max_page": 1},
        }
    )

    with _patch_async_client(mock_client):
        bills = await service.fetch_sponsored_bills(
            _BARE_ID, "us", sponsor_name="Kelly"
        )

    assert len(bills) == 1
    assert bills[0]["identifier"] == "HB 1"


# --- fetch_legislator_votes / _extract_legislator_votes_from_bill -------------


@pytest.mark.asyncio
async def test_fetch_legislator_votes_matches_voter_given_bare_uuid():
    service = _make_service(ddp_openstates_jurisdictions=[])
    mock_client = AsyncMock()

    bill_list_response = _mock_response(
        {"results": [{"id": "bill-1"}], "pagination": {"max_page": 1}}
    )
    bill_detail_response = _mock_response(
        {
            "identifier": "HB 1",
            "title": "A Bill",
            "votes": [
                {
                    "motion_text": "Passage",
                    "start_date": "2026-01-01",
                    "result": "pass",
                    "organization": {"classification": "lower"},
                    "votes": [
                        {"option": "yes", "voter": {"id": _PREFIXED_ID}},
                    ],
                }
            ],
        }
    )
    mock_client.get.side_effect = [bill_list_response, bill_detail_response]

    with _patch_async_client(mock_client):
        votes = await service.fetch_legislator_votes(_BARE_ID, "us")

    assert len(votes) == 1
    assert votes[0].vote_option == "yes"


def test_extract_legislator_votes_from_bill_matches_given_prefixed_id():
    """`_extract_legislator_votes_from_bill` does no normalization itself --
    callers (`fetch_legislator_votes`) are responsible for normalizing
    first. This asserts the comparison itself works given the already-
    prefixed form; `test_fetch_legislator_votes_matches_voter_given_bare_uuid`
    above covers the bare-UUID case end-to-end through the real caller."""
    service = _make_service()
    bill = {
        "identifier": "HB 1",
        "title": "A Bill",
        "votes": [
            {
                "motion_text": "Passage",
                "start_date": "2026-01-01",
                "result": "pass",
                "organization": {"classification": "lower"},
                "votes": [{"option": "yes", "voter": {"id": _PREFIXED_ID}}],
            }
        ],
    }

    votes = service._extract_legislator_votes_from_bill(bill, _PREFIXED_ID)

    assert len(votes) == 1


# --- sync_legislator: persisted IDs stay bare, only the OpenStates request/match
# boundary gets the ocd-person/ prefix -----------------------------------------


@pytest.mark.asyncio
async def test_sync_legislator_persists_bare_id_while_matching_via_prefixed_id():
    """End-to-end: given a legislator dict with a bare CMS UUID,
    sync_legislator() must (1) successfully match sponsorships from
    OpenStates (which always returns prefixed person.id) and (2) still use
    the bare UUID for the persisted document_id/legislator_id metadata --
    changing that would alter existing Pinecone document IDs' shape and
    orphan previously-ingested documents, which is explicitly out of scope
    for this fix."""
    service = _make_service(ddp_openstates_jurisdictions=[])
    mock_client = AsyncMock()
    mock_client.get.return_value = _mock_response(
        {
            "results": [
                {
                    "identifier": "HB 1",
                    "sponsorships": [{"person": {"id": _PREFIXED_ID}}],
                }
            ],
            "pagination": {"max_page": 1},
        }
    )

    with _patch_async_client(mock_client), patch(
        "ddp_sync.pipelines.legislator_sync.LegislatorSyncService._get_sponsor_name",
        AsyncMock(return_value="Kelly"),
    ), patch.object(
        service.pipeline, "ingest_document", AsyncMock(
            return_value=MagicMock(chunks_created=1)
        ),
    ) as mock_ingest:
        result = await service.sync_legislator(
            {"openstates_id": _BARE_ID, "name": "Test Rep", "jurisdiction": "us"}
        )

    assert result.success
    assert result.bills_found == 1
    assert result.legislator_id == _BARE_ID  # bare, not prefixed

    _, ingest_kwargs = mock_ingest.call_args
    metadata = ingest_kwargs["metadata"]
    assert metadata.document_id == f"legislator-bills-{_BARE_ID}"
    assert metadata.legislator_id == _BARE_ID
