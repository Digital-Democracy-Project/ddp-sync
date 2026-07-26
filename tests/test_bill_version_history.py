"""Tests for bill version history, changelog generation, and surplus chunk deletion.

Covers PLAN-bill-version-history.md Phase 1:
- Surplus chunk deletion (upsert-then-delete with chunk_count guard)
- bill-text-history ingestion on first ingest and on version update
- bill-changelog generation and graceful fallback on failures
- chunk_count written to Redis version cache
- Batch result counters
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.pipelines.bill_version import BillVersionSyncService, VersionSyncBatchResult

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service() -> BillVersionSyncService:
    settings = MagicMock()
    settings.openai_api_key = "test-key"
    return BillVersionSyncService(settings=settings)


def _make_doc_source(content: str = "Bill text content") -> MagicMock:
    doc = MagicMock()
    doc.content = content
    doc.metadata = MagicMock()
    doc.metadata.document_id = "bill-pdf-test123"
    return doc


def _make_ingest_result(chunks: int = 3) -> MagicMock:
    r = MagicMock()
    r.chunks_created = chunks
    r.chunks_upserted = chunks
    return r


# ---------------------------------------------------------------------------
# _delete_surplus_chunks
# ---------------------------------------------------------------------------

async def test_surplus_chunks_deleted_when_new_version_is_shorter():
    """old_count=5, new_count=3 → chunks 3 and 4 are deleted by exact ID."""
    svc = _make_service()
    mock_vs = AsyncMock()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=mock_vs):
        deleted = await svc._delete_surplus_chunks(
            document_id="bill-pdf-abc123",
            old_chunk_count=5,
            new_chunk_count=3,
        )
    assert deleted == 2
    mock_vs.delete.assert_awaited_once_with(
        ids=["bill-pdf-abc123-chunk-3", "bill-pdf-abc123-chunk-4"]
    )


async def test_no_deletion_when_new_version_is_longer():
    """old_count=3, new_count=5 → no delete call."""
    svc = _make_service()
    mock_vs = AsyncMock()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=mock_vs):
        deleted = await svc._delete_surplus_chunks(
            document_id="bill-pdf-abc123",
            old_chunk_count=3,
            new_chunk_count=5,
        )
    assert deleted == 0
    mock_vs.delete.assert_not_called()


async def test_no_deletion_on_first_ingest():
    """old_chunk_count=None (no cached entry) → no delete call."""
    svc = _make_service()
    mock_vs = AsyncMock()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=mock_vs):
        deleted = await svc._delete_surplus_chunks(
            document_id="bill-pdf-abc123",
            old_chunk_count=None,
            new_chunk_count=3,
        )
    assert deleted == 0
    mock_vs.delete.assert_not_called()


async def test_surplus_deletion_skipped_when_old_count_implausibly_large():
    """old_count > new_count * 4 → guard fires, no delete, warning logged."""
    svc = _make_service()
    mock_vs = AsyncMock()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=mock_vs):
        deleted = await svc._delete_surplus_chunks(
            document_id="bill-pdf-abc123",
            old_chunk_count=100,  # 100 > 3 * 4 = 12
            new_chunk_count=3,
        )
    assert deleted == 0
    mock_vs.delete.assert_not_called()


async def test_no_deletion_when_counts_equal():
    """old_count == new_count → no-op."""
    svc = _make_service()
    mock_vs = AsyncMock()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=mock_vs):
        deleted = await svc._delete_surplus_chunks(
            document_id="bill-pdf-abc123",
            old_chunk_count=4,
            new_chunk_count=4,
        )
    assert deleted == 0
    mock_vs.delete.assert_not_called()


# ---------------------------------------------------------------------------
# _ingest_bill_history
# ---------------------------------------------------------------------------

async def test_history_ingested_with_versioned_document_id():
    """History document uses bill-text-history-{webflow_id}-{version_date} as ID."""
    svc = _make_service()
    mock_pipeline = AsyncMock()
    mock_pipeline.ingest_document = AsyncMock(return_value=_make_ingest_result(3))

    with patch("ddp_sync.ingestion.pipeline.IngestionPipeline", return_value=mock_pipeline):
        chunks = await svc._ingest_bill_history(
            webflow_id="webflow123",
            bill_title="Test Bill",
            bill_slug="test-bill-2026",
            text_url="https://example.gov/bill.pdf",
            media_type="application/pdf",
            version_date="2026-05-20",
            version_note="Placed on Calendar Senate",
            jurisdiction="US",
            content="Full bill text here.",
        )

    assert chunks == 3
    call_kwargs = mock_pipeline.ingest_document.call_args.kwargs
    assert call_kwargs["metadata"].document_id == "bill-text-history-webflow123-2026-05-20"
    assert call_kwargs["metadata"].document_type == "bill-text-history"
    assert call_kwargs["metadata"].extra["version_date"] == "2026-05-20"
    assert call_kwargs["metadata"].extra["version_note"] == "Placed on Calendar Senate"
    assert call_kwargs["skip_duplicates"] is False


async def test_history_returns_zero_on_empty_content():
    """No ingest attempt when content is empty."""
    svc = _make_service()
    mock_pipeline = AsyncMock()

    with patch("ddp_sync.ingestion.pipeline.IngestionPipeline", return_value=mock_pipeline):
        chunks = await svc._ingest_bill_history(
            webflow_id="webflow123",
            bill_title="Test Bill",
            bill_slug="test-bill",
            text_url="https://example.gov/bill.pdf",
            media_type="application/pdf",
            version_date="2026-05-20",
            version_note="Introduced",
            jurisdiction="US",
            content="",
        )

    assert chunks == 0
    mock_pipeline.ingest_document.assert_not_called()


# ---------------------------------------------------------------------------
# _generate_and_ingest_changelog
# ---------------------------------------------------------------------------

async def test_changelog_generated_and_ingested():
    """Happy path: old URL downloadable, OpenAI returns content, changelog ingested."""
    svc = _make_service()

    old_doc = _make_doc_source("Old bill text " * 50)
    mock_pipeline = AsyncMock()
    mock_pipeline.ingest_document = AsyncMock(return_value=_make_ingest_result(1))

    mock_openai_response = MagicMock()
    mock_openai_response.choices[0].message.content = "## What Changed\n**From:** Introduced\n**To:** Engrossed\n\n### Summary\nSections were added."

    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_openai_response)

    with (
        patch("ddp_sync.ingestion.sources.webflow.WebflowSource") as MockWS,
        patch("ddp_sync.ingestion.pipeline.IngestionPipeline", return_value=mock_pipeline),
        patch("openai.AsyncOpenAI", return_value=mock_client),
    ):
        MockWS.return_value._process_bill_pdf = AsyncMock(return_value=old_doc)

        chunks, skipped, reason = await svc._generate_and_ingest_changelog(
            webflow_id="webflow123",
            bill_title="Test Bill",
            bill_slug="test-bill",
            jurisdiction="US",
            old_version={
                "text_url": "https://example.gov/old.pdf",
                "media_type": "application/pdf",
                "version_date": "2026-03-01",
                "version_note": "Introduced",
            },
            new_version_date="2026-05-20",
            new_version_note="Engrossed",
            new_content="New bill text " * 50,
        )

    assert chunks == 1
    assert skipped is False
    assert reason == ""
    call_kwargs = mock_pipeline.ingest_document.call_args.kwargs
    assert call_kwargs["metadata"].document_id == "bill-changelog-webflow123-2026-05-20"
    assert call_kwargs["metadata"].document_type == "bill-changelog"
    assert call_kwargs["metadata"].extra["version_from_note"] == "Introduced"
    assert call_kwargs["metadata"].extra["version_to_note"] == "Engrossed"


async def test_changelog_skipped_when_no_old_url():
    """old_version has no text_url → skip immediately."""
    svc = _make_service()
    chunks, skipped, reason = await svc._generate_and_ingest_changelog(
        webflow_id="webflow123",
        bill_title="Test Bill",
        bill_slug="test-bill",
        jurisdiction="US",
        old_version={"text_url": "", "media_type": "application/pdf", "version_date": "", "version_note": ""},
        new_version_date="2026-05-20",
        new_version_note="Engrossed",
        new_content="New content",
    )
    assert chunks == 0
    assert skipped is True
    assert reason == "no_old_url"


async def test_changelog_skipped_on_stale_url():
    """Old URL returns HTTP error → skipped, ingest not blocked."""
    import httpx
    svc = _make_service()

    with patch("ddp_sync.ingestion.sources.webflow.WebflowSource") as MockWS:
        MockWS.return_value._process_bill_pdf = AsyncMock(
            side_effect=httpx.HTTPStatusError("404", request=MagicMock(), response=MagicMock())
        )
        chunks, skipped, reason = await svc._generate_and_ingest_changelog(
            webflow_id="webflow123",
            bill_title="Test Bill",
            bill_slug="test-bill",
            jurisdiction="US",
            old_version={
                "text_url": "https://example.gov/old.pdf",
                "media_type": "application/pdf",
                "version_date": "2026-03-01",
                "version_note": "Introduced",
            },
            new_version_date="2026-05-20",
            new_version_note="Engrossed",
            new_content="New content " * 50,
        )

    assert chunks == 0
    assert skipped is True
    assert "old_url_fetch_failed" in reason


async def test_changelog_skipped_on_openai_error():
    """OpenAI raises → skipped gracefully, no exception propagated."""
    svc = _make_service()
    old_doc = _make_doc_source("Old bill text " * 50)

    with (
        patch("ddp_sync.ingestion.sources.webflow.WebflowSource") as MockWS,
        patch("openai.AsyncOpenAI") as MockOAI,
    ):
        MockWS.return_value._process_bill_pdf = AsyncMock(return_value=old_doc)
        mock_client = AsyncMock()
        mock_client.chat.completions.create = AsyncMock(side_effect=Exception("API unavailable"))
        MockOAI.return_value = mock_client

        chunks, skipped, reason = await svc._generate_and_ingest_changelog(
            webflow_id="webflow123",
            bill_title="Test Bill",
            bill_slug="test-bill",
            jurisdiction="US",
            old_version={
                "text_url": "https://example.gov/old.pdf",
                "media_type": "application/pdf",
                "version_date": "2026-03-01",
                "version_note": "Introduced",
            },
            new_version_date="2026-05-20",
            new_version_note="Engrossed",
            new_content="New bill text " * 50,
        )

    assert chunks == 0
    assert skipped is True
    assert "openai_error" in reason


async def test_changelog_skipped_when_old_content_too_short():
    """Old text under 500 chars → skipped (not worth diffing)."""
    svc = _make_service()
    short_doc = _make_doc_source("Too short.")

    with patch("ddp_sync.ingestion.sources.webflow.WebflowSource") as MockWS:
        MockWS.return_value._process_bill_pdf = AsyncMock(return_value=short_doc)

        chunks, skipped, reason = await svc._generate_and_ingest_changelog(
            webflow_id="webflow123",
            bill_title="Test Bill",
            bill_slug="test-bill",
            jurisdiction="US",
            old_version={
                "text_url": "https://example.gov/old.pdf",
                "media_type": "application/pdf",
                "version_date": "2026-03-01",
                "version_note": "Introduced",
            },
            new_version_date="2026-05-20",
            new_version_note="Engrossed",
            new_content="New bill text " * 50,
        )

    assert chunks == 0
    assert skipped is True
    assert reason == "old_content_too_short"


# ---------------------------------------------------------------------------
# Versioned document ID uniqueness
# ---------------------------------------------------------------------------

async def test_versioned_document_ids_are_unique():
    """Two version transitions produce non-colliding document IDs."""
    svc = _make_service()
    mock_pipeline = AsyncMock()
    mock_pipeline.ingest_document = AsyncMock(return_value=_make_ingest_result(2))

    ids_seen = []

    async def capture_ingest(content, metadata, skip_duplicates):
        ids_seen.append(metadata.document_id)
        return _make_ingest_result(2)

    mock_pipeline.ingest_document = capture_ingest

    with patch("ddp_sync.ingestion.pipeline.IngestionPipeline", return_value=mock_pipeline):
        await svc._ingest_bill_history(
            webflow_id="webflow123", bill_title="T", bill_slug="s",
            text_url="u", media_type="application/pdf",
            version_date="2026-03-01", version_note="Introduced",
            jurisdiction="US", content="content A",
        )
        await svc._ingest_bill_history(
            webflow_id="webflow123", bill_title="T", bill_slug="s",
            text_url="u", media_type="application/pdf",
            version_date="2026-05-20", version_note="Engrossed",
            jurisdiction="US", content="content B",
        )

    assert len(ids_seen) == 2
    assert ids_seen[0] != ids_seen[1]
    assert "2026-03-01" in ids_seen[0]
    assert "2026-05-20" in ids_seen[1]


# ---------------------------------------------------------------------------
# BillVersion chunk_count persistence (Phase 4 -- replaces the old Redis
# version cache; see PLAN-bill-document-provenance.md's 2026-07-26 decision
# to drop Redis from this job entirely)
# ---------------------------------------------------------------------------

async def test_chunk_count_written_to_bill_version():
    """chunk_count is included in the BillVersion write after ingest."""
    svc = _make_service()

    mock_doc = _make_doc_source("Bill text " * 100)
    mock_ingest = _make_ingest_result(4)
    mock_pipeline = AsyncMock()
    mock_pipeline.ingest_document = AsyncMock(return_value=mock_ingest)
    mock_redis = AsyncMock()
    mock_redis.set_bill_version = AsyncMock()
    mock_redis.publish = AsyncMock(return_value=0)
    mock_write_bill_version = AsyncMock(return_value={"id": 1, "created": True})

    bill_data = {
        "id": "ocd-bill/test-uuid-1234",
        "versions": [{"date": "2026-05-20", "note": "Introduced", "links": [{"url": "https://example.gov/bill.pdf", "media_type": "application/pdf"}]}]
    }

    with (
        patch("ddp_sync.services.redis_store.get_redis_store", return_value=mock_redis),
        patch("ddp_sync.services.broker_client.get_latest_bill_version", new=AsyncMock(return_value=None)),
        patch("ddp_sync.services.broker_client.write_bill_version", new=mock_write_bill_version),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._ingest_bill_text", new=AsyncMock(return_value=(4, "Bill text " * 100))),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._delete_surplus_chunks", new=AsyncMock(return_value=0)),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._ingest_bill_history", new=AsyncMock(return_value=4)),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._generate_and_ingest_changelog", new=AsyncMock(return_value=(0, True, "no_old_url"))),
    ):
        await svc.check_and_reingest_version(
            webflow_id="webflow123",
            bill_title="Test Bill",
            jurisdiction_code="US",
            bill_data=bill_data,
            bill_slug="test-bill",
            fields={},
        )

    write_call = mock_write_bill_version.call_args
    assert write_call.kwargs["chunk_count"] == 4
    assert write_call.kwargs["bill_openstates_id"] == "test-uuid-1234"
    assert write_call.kwargs["pinecone_ingested"] is True


async def test_no_bill_version_write_when_bill_data_has_no_id():
    """A malformed OpenStates response (no 'id') can't be recorded in
    BillVersion -- logged and skipped, not a crash, and doesn't block the
    Pinecone ingestion that already happened."""
    svc = _make_service()
    mock_write_bill_version = AsyncMock()
    mock_redis = AsyncMock()
    mock_redis.set_bill_version = AsyncMock()
    mock_redis.publish = AsyncMock(return_value=0)

    bill_data = {
        "versions": [{"date": "2026-05-20", "note": "Introduced", "links": [{"url": "https://example.gov/bill.pdf", "media_type": "application/pdf"}]}]
    }

    with (
        patch("ddp_sync.services.redis_store.get_redis_store", return_value=mock_redis),
        patch("ddp_sync.services.broker_client.write_bill_version", new=mock_write_bill_version),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._ingest_bill_text", new=AsyncMock(return_value=(4, "Bill text " * 100))),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._delete_surplus_chunks", new=AsyncMock(return_value=0)),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._ingest_bill_history", new=AsyncMock(return_value=4)),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._generate_and_ingest_changelog", new=AsyncMock(return_value=(0, True, "no_old_url"))),
    ):
        result = await svc.check_and_reingest_version(
            webflow_id="webflow123",
            bill_title="Test Bill",
            jurisdiction_code="US",
            bill_data=bill_data,
            bill_slug="test-bill",
            fields={},
        )

    mock_write_bill_version.assert_not_called()
    assert result["chunks_created"] == 4


async def test_broker_read_failure_skips_this_bill_without_crashing():
    """A ddp-broker-py outage during the read must not be treated as 'never
    seen before' (which would re-ingest/re-bill every bill on every run
    during an outage) -- it should skip this bill this run instead."""
    from ddp_sync.services.broker_client import BrokerClientError

    svc = _make_service()
    mock_ingest_text = AsyncMock()
    mock_redis = AsyncMock()
    mock_redis.set_bill_version = AsyncMock()

    bill_data = {
        "id": "ocd-bill/test-uuid-5678",
        "versions": [{"date": "2026-05-20", "note": "Introduced", "links": [{"url": "https://example.gov/bill.pdf", "media_type": "application/pdf"}]}]
    }

    with (
        patch("ddp_sync.services.redis_store.get_redis_store", return_value=mock_redis),
        patch(
            "ddp_sync.services.broker_client.get_latest_bill_version",
            new=AsyncMock(side_effect=BrokerClientError("ddp-broker-py unreachable")),
        ),
        patch("ddp_sync.pipelines.bill_version.BillVersionSyncService._ingest_bill_text", new=mock_ingest_text),
    ):
        result = await svc.check_and_reingest_version(
            webflow_id="webflow123",
            bill_title="Test Bill",
            jurisdiction_code="US",
            bill_data=bill_data,
            bill_slug="test-bill",
            fields={},
        )

    assert result["is_newer"] is False
    mock_ingest_text.assert_not_called()


# ---------------------------------------------------------------------------
# Batch result counters
# ---------------------------------------------------------------------------

async def test_batch_result_tracks_all_new_counts():
    """history_chunks, changelog_chunks, changelogs_skipped, surplus_deleted
    are accumulated correctly in VersionSyncBatchResult."""
    result = VersionSyncBatchResult()
    result.history_chunks_created += 3
    result.changelog_chunks_created += 1
    result.changelogs_skipped += 1
    result.surplus_chunks_deleted += 2

    assert result.history_chunks_created == 3
    assert result.changelog_chunks_created == 1
    assert result.changelogs_skipped == 1
    assert result.surplus_chunks_deleted == 2
