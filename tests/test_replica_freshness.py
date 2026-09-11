"""Tests for services/replica_freshness.py -- OPEN-275's AC5 freshness check (ddp-infra
PLAN-rds-local-postgres-replication.md §4/§7.7).

Two required negative tests (plan §4's last AC, expanded per that plan's own pm-review round 3):
(a) test_not_fresh_when_row_not_yet_replicated_locally -- a bill whose RDS-side version-document
row has no matching row at all on the local replica (row never arrived).
(b) test_not_fresh_when_local_content_is_stale -- a bill whose local row exists at the exact same
natural key RDS reports, but with different (stale) content -- the case a presence-only check
would miss, per the plan's own v0.4 correction. (a) alone never exercises the content-hash check.

Neither test touches a real database -- asyncpg.connect and get_bill_version_document_text are
both mocked, matching this repo's existing convention (see test_local_openstates_client.py,
test_rds_credentials.py) of never hitting real network/DB from a unit test.
"""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.services.replica_freshness import (
    FreshnessResult,
    check_bill_version_freshness,
)

_BILL_ID = "11111111-0000-0000-0000-000000000001"


def _mock_rds_connection(row: dict | None):
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=row)
    conn.close = AsyncMock()
    return conn


@pytest.mark.asyncio
async def test_not_fresh_when_rds_credential_unavailable():
    with patch(
        "ddp_sync.services.replica_freshness.resolve_rds_database_url",
        return_value=(None, "RDS_CREDENTIALS_SECRET_ARN not set"),
    ):
        result = await check_bill_version_freshness(_BILL_ID)

    assert result == FreshnessResult(False, "rds_credential_unavailable: RDS_CREDENTIALS_SECRET_ARN not set")


@pytest.mark.asyncio
async def test_not_fresh_when_rds_connection_fails():
    with (
        patch(
            "ddp_sync.services.replica_freshness.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
        ),
        patch(
            "ddp_sync.services.replica_freshness.asyncpg.connect",
            new=AsyncMock(side_effect=OSError("connection refused")),
        ),
    ):
        result = await check_bill_version_freshness(_BILL_ID)

    assert result.is_fresh is False
    assert "rds_connection_failed" in result.reason


@pytest.mark.asyncio
async def test_not_fresh_when_no_version_document_row_on_rds():
    conn = _mock_rds_connection(row=None)
    with (
        patch(
            "ddp_sync.services.replica_freshness.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
        ),
        patch("ddp_sync.services.replica_freshness.asyncpg.connect", new=AsyncMock(return_value=conn)),
    ):
        result = await check_bill_version_freshness(_BILL_ID)

    assert result == FreshnessResult(False, "no_version_document_row_on_rds")
    conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_not_fresh_when_row_not_yet_replicated_locally():
    """Negative test (a): RDS has this bill's latest version-document row, but the local replica
    has no matching row at all yet (has not caught up). Dispatch must block/defer, not proceed."""
    rds_row = {
        "version_note": "Engrossed",
        "version_date": "2026-01-15",
        "source_url": "https://example.gov/bill/1.pdf",
        "raw_text_hash": "abc123",
    }
    conn = _mock_rds_connection(row=rds_row)
    with (
        patch(
            "ddp_sync.services.replica_freshness.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
        ),
        patch("ddp_sync.services.replica_freshness.asyncpg.connect", new=AsyncMock(return_value=conn)),
        patch(
            "ddp_sync.services.replica_freshness.get_bill_version_document_text",
            new=AsyncMock(return_value=None),
        ) as mock_local_lookup,
    ):
        result = await check_bill_version_freshness(_BILL_ID)

    assert result == FreshnessResult(False, "row_not_yet_replicated_locally")
    mock_local_lookup.assert_awaited_once_with(
        _BILL_ID,
        version_note="Engrossed",
        version_date="2026-01-15",
        source_url="https://example.gov/bill/1.pdf",
    )


@pytest.mark.asyncio
async def test_not_fresh_when_local_content_is_stale():
    """Negative test (b): the local replica has a row at the EXACT same natural key RDS reports
    (present, non-null raw_text), but its content differs -- matching how os-text-extract's own
    reextract/refresh-extraction tooling updates raw_text on an existing row in place without
    changing the natural key. Presence alone would wrongly pass this; the content-hash comparison
    must not."""
    rds_row = {
        "version_note": "Engrossed",
        "version_date": "2026-01-15",
        "source_url": "https://example.gov/bill/1.pdf",
        "raw_text_hash": "d41d8cd98f00b204e9800998ecf8427e",  # md5("")
    }
    conn = _mock_rds_connection(row=rds_row)
    with (
        patch(
            "ddp_sync.services.replica_freshness.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
        ),
        patch("ddp_sync.services.replica_freshness.asyncpg.connect", new=AsyncMock(return_value=conn)),
        patch(
            "ddp_sync.services.replica_freshness.get_bill_version_document_text",
            new=AsyncMock(return_value="this is stale text, not empty"),
        ),
    ):
        result = await check_bill_version_freshness(_BILL_ID)

    assert result == FreshnessResult(False, "content_hash_mismatch_stale_local_text")


@pytest.mark.asyncio
async def test_fresh_when_content_matches():
    local_text = "the real, current bill text"
    rds_row = {
        "version_note": "Engrossed",
        "version_date": "2026-01-15",
        "source_url": "https://example.gov/bill/1.pdf",
        "raw_text_hash": hashlib.md5(local_text.encode("utf-8")).hexdigest(),
    }
    conn = _mock_rds_connection(row=rds_row)
    with (
        patch(
            "ddp_sync.services.replica_freshness.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
        ),
        patch("ddp_sync.services.replica_freshness.asyncpg.connect", new=AsyncMock(return_value=conn)),
        patch(
            "ddp_sync.services.replica_freshness.get_bill_version_document_text",
            new=AsyncMock(return_value=local_text),
        ),
    ):
        result = await check_bill_version_freshness(_BILL_ID)

    assert result == FreshnessResult(True, "content_matches")


@pytest.mark.asyncio
async def test_bill_id_is_prefixed_with_ocd_bill_for_the_rds_query():
    """ddp_bill_version_document.bill_id on RDS is the full 'ocd-bill/<uuid>' form (confirmed
    against the real local Postgres schema), not the bare UUID ddp-sync's own convention uses
    everywhere else -- this must be prefixed before querying, not passed through as-is."""
    conn = _mock_rds_connection(row=None)
    with (
        patch(
            "ddp_sync.services.replica_freshness.resolve_rds_database_url",
            return_value=("postgresql://rds/openstates", ""),
        ),
        patch("ddp_sync.services.replica_freshness.asyncpg.connect", new=AsyncMock(return_value=conn)),
    ):
        await check_bill_version_freshness(_BILL_ID)

    args, _kwargs = conn.fetchrow.call_args
    assert args[1] == f"ocd-bill/{_BILL_ID}"
