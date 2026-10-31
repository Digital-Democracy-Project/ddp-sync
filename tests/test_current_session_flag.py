"""SYNC-139: "which session is current" is read from api-v3's is_current flag, never worked out from dates.

The session lists below are real identifiers and dates, trimmed to the rows that matter. The flags are
what api-v3 (OPEN-467, live on EC2 since 2026-10-31 20:43 UTC) published for each jurisdiction. The old
ddp-sync rule is kept in OLD_RULE_PICKS only to show, in a test, where it was wrong.
"""

from __future__ import annotations

import datetime as real_datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import structlog

from ddp_sync.config import SyncSettings
from ddp_sync.ingestion.sources import openstates as openstates_module
from ddp_sync.ingestion.sources.openstates import OpenStatesSource
from ddp_sync.pipelines.bill_sync import BillSyncService
from ddp_sync.services.local_openstates_client import (
    list_current_session_bill_candidates,
)

pytestmark = pytest.mark.asyncio

# (identifier, classification, start, end, is_current, is_previous)
S = {
    "us": [
        ("118", "primary", "2023-01-03", "2025-01-03", False, True),
        ("119", "primary", "2025-01-03", "2027-01-03", True, False),
        ("120", "primary", "2027-01-03", "2029-01-03", False, False),
    ],
    "al": [
        ("2025rs", "primary", "2025-02-04", "2025-05-15", False, True),
        ("2026rs", "primary", "2026-01-13", "2026-04-02", True, False),
    ],
    "az": [
        ("57th-1st-regular", "primary", "2025-01-13", "2025-04-26", False, True),
        ("57th-2nd-regular", "primary", "2026-01-12", "2026-04-25", True, False),
    ],
    "fl": [
        ("2025", "primary", "2025-03-04", "2025-05-02", False, True),
        ("2025A", "special", "2025-01-27", "2025-12-31", False, False),
        ("2026", "primary", "2026-01-13", "2026-03-13", True, False),
        ("2027", "primary", "2027-03-02", "2027-04-30", False, False),
    ],
    "ma": [
        ("193rd", "primary", "2023-01-04", "2024-07-31", False, True),
        ("194th", "primary", "2025-01-01", "2026-07-31", True, False),
    ],
    "mi": [
        ("2023-2024", "primary", "2023-01-11", "2024-12-22", False, True),
        ("2025-2026", "primary", "2025-01-08", "2026-12-31", True, False),
    ],
    "nc": [
        ("2023", "primary", "2023-01-25", "2024-06-27", False, True),
        ("2025", "primary", "2025-01-15", "2025-12-31", True, False),
    ],
    "ut": [
        ("2025", "primary", "2025-01-21", "2025-03-07", False, True),
        ("2026", "primary", "2026-01-20", "2026-03-06", True, False),
        ("2027", "primary", "2027-01-19", "2027-03-05", False, False),
    ],
    "va": [  # blank classification, as in the real data
        ("2025", "", "2025-01-08", "2025-04-12", False, True),
        ("2026", "", "2026-01-14", "2026-03-14", True, False),
        ("2027", "", "2027-01-13", "2027-03-13", False, False),
    ],
    "wa": [
        ("2023-2024", "", "2023-01-09", "2024-03-07", False, True),
        ("2025-2026", "", "2025-01-13", "2026-03-06", True, False),
    ],
}
EXPECTED_CURRENT = {
    "us": "119", "al": "2026rs", "az": "57th-2nd-regular", "fl": "2026", "ma": "194th",
    "mi": "2025-2026", "nc": "2025", "ut": "2026", "va": "2026", "wa": "2025-2026",
}
# What the deleted date rule picked on 2026-10-31 (newest "primary" by start date when none is active):
# wrong for FL, UT (2027 has not started). VA/WA have blank classification so it picked nothing sensible.
OLD_RULE_PICKS = {"fl": "2027", "ut": "2027"}


def _body(code, sessions=None, with_flags=True):
    rows = S[code] if sessions is None else sessions
    out = []
    for ident, cls, start, end, cur, prev in rows:
        row = {"identifier": ident, "name": ident, "classification": cls, "start_date": start, "end_date": end}
        if with_flags:
            row["is_current"], row["is_previous"] = cur, prev
        out.append(row)
    return {"id": f"ocd-jurisdiction/{code}", "name": code, "classification": "state", "url": "",
            "legislative_sessions": out, "organizations": []}


def _response(data):
    r = MagicMock()
    r.status_code = 200
    r.json.return_value = data
    r.raise_for_status.return_value = None
    return r


def _client_returning(data):
    client = AsyncMock()
    client.get.return_value = _response(data)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.ingestion.sources.openstates.httpx.AsyncClient", return_value=cm)


def _settings():
    return SyncSettings(openstates_api_key="k", openstates_api_base="https://v3.openstates.org",
                        local_openstates_api_base="http://localhost:8002", local_openstates_api_key="l")


def _source():
    return OpenStatesSource(_settings())


@pytest.mark.parametrize("code", sorted(EXPECTED_CURRENT))
async def test_current_session_is_the_one_api_v3_flags(code):
    with _client_returning(_body(code)):
        assert await _source().get_current_session_identifiers(code) == [EXPECTED_CURRENT[code]]


@pytest.mark.parametrize("code", sorted(OLD_RULE_PICKS))
async def test_a_session_that_has_not_started_is_never_current(code):
    # the old rule named the 2027 session (not started) as current for these two
    with _client_returning(_body(code)):
        got = await _source().get_current_session_identifiers(code)
    assert OLD_RULE_PICKS[code] not in got and got == [EXPECTED_CURRENT[code]]


async def test_previous_flag_is_read_too():
    with _client_returning(_body("va")):
        info = await _source().fetch_jurisdiction("va")
    assert [s.identifier for s in info.sessions if s.is_previous] == ["2025"]


@pytest.mark.parametrize("year", [2020, 2035])
async def test_no_date_is_compared(year):
    class FakeDatetime(real_datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(year, 6, 1)

    with patch.object(openstates_module, "datetime", FakeDatetime), _client_returning(_body("fl")):
        assert await _source().get_current_session_identifiers("fl") == ["2026"]


async def test_two_current_sessions_are_both_returned():
    rows = [
        ("2026", "", "2026-01-14", "2026-03-14", True, False),
        ("2026S1", "", "2026-04-22", "2026-12-31", True, False),
        ("2025", "", "2025-01-08", "2025-04-12", False, True),
    ]
    with _client_returning(_body("va", rows)):
        assert await _source().get_current_session_identifiers("va") == ["2026", "2026S1"]


async def test_flag_not_published_means_unknown_not_a_guess():
    with _client_returning(_body("fl", with_flags=False)), structlog.testing.capture_logs() as logs:
        assert await _source().get_current_session_identifiers("fl") == []
    assert any(e["log_level"] == "error" and "is_current" in e["event"] for e in logs)


async def test_a_partly_flagged_answer_is_unknown_not_authoritative():
    body = _body("va")
    del body["legislative_sessions"][1]["is_current"]  # one session missing the flag
    with _client_returning(body), structlog.testing.capture_logs() as logs:
        assert await _source().get_current_session_identifiers("va") == []
    assert any(e["log_level"] == "error" for e in logs)


async def test_api_v3_unreachable_means_unknown():
    client = AsyncMock()
    client.get.side_effect = RuntimeError("down")
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    with patch("ddp_sync.ingestion.sources.openstates.httpx.AsyncClient", return_value=cm):
        assert await _source().get_current_session_identifiers("fl") == []


# --- bill_sync.is_current_session_async -------------------------------------------------------


async def _is_current(code, year, session_code, rows=None, with_flags=True):
    service = BillSyncService.__new__(BillSyncService)
    service._jurisdiction_cache = {}
    service._openstates_source = _source()
    with _client_returning(_body(code, rows, with_flags)):
        return await service.is_current_session_async(year, session_code, code.upper())


async def test_bill_in_the_flagged_session_is_current_and_others_are_not():
    assert await _is_current("fl", "2026", "2026") is True
    assert await _is_current("fl", "2025", "2025") is False  # previous session
    assert await _is_current("fl", "2027", "2027") is False  # not started
    assert await _is_current("az", "2026", "57th-2nd-regular") is True
    assert await _is_current("az", "2025", "57th-1st-regular") is False
    assert await _is_current("wa", "2025-2026", "2025-2026") is True
    assert await _is_current("us", "2025", "119") is True


async def test_bill_in_either_of_two_current_sessions_is_current():
    rows = [
        ("2026", "", "2026-01-14", "2026-03-14", True, False),
        ("2026S1", "", "2026-04-22", "2026-12-31", True, False),
    ]
    assert await _is_current("va", None, "2026S1", rows) is True  # the second current session, matched by code only
    assert await _is_current("va", "2026", "2026", rows) is True


async def test_year_heuristic_is_gone_when_flag_missing_or_api_down():
    # the old year-based fallback said True for ("2026", "2026"); now unknown means False
    assert await _is_current("fl", "2026", "2026", with_flags=False) is False
    assert not hasattr(BillSyncService, "is_current_session")


# --- list_current_session_bill_candidates with two current sessions -----------------------------


class _Settings:
    local_openstates_api_base = "http://local"
    local_openstates_api_key = "k"


def _bills_page(prefix, n):
    return {
        "results": [{"id": f"ocd-bill/{prefix}-{i}", "identifier": f"B {i}", "sources": []} for i in range(n)],
        "pagination": {"max_page": 1},
    }


async def test_candidates_come_from_every_current_session_and_share_the_limit():
    seen = []

    async def get(url, params=None, headers=None):
        seen.append(params["session"])
        return _response(_bills_page(params["session"], 3))

    client = MagicMock()
    client.get = get
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    patches = (
        patch("ddp_sync.services.local_openstates_client.get_settings", return_value=_Settings()),
        patch("ddp_sync.services.local_openstates_client.httpx.AsyncClient", return_value=cm),
        patch(
            "ddp_sync.ingestion.sources.openstates.OpenStatesSource.get_current_session_identifiers",
            new=AsyncMock(return_value=["2026", "2026S1"]),
        ),
    )
    with patches[0], patches[1], patches[2]:
        everything = await list_current_session_bill_candidates("va", limit=None)
        seen.clear()
        capped = await list_current_session_bill_candidates("va", limit=4)
    assert {c["session_code"] for c in everything} == {"2026", "2026S1"} and len(everything) == 6
    assert len(capped) == 4 and seen == ["2026", "2026S1"]
