"""Tests for mi_cookie_publish.py (OPEN-188, SYNC-53).

Mirrors test_openstates_archive_scrapebot_preseed.py's mocking style. The acceptance
question is not "does it call ScrapeBot" -- it is:

  * a missing SCRAPER_MEMORY_PREFIX refuses outright rather than publishing under an
    unnamespaced key (the same OPEN-159/172 discipline every other publisher in this
    project follows).
  * a mint failure or a publish failure is best-effort: logged, returned as a
    structured failure, never raised -- a bad tick must not crash the scheduler.
  * the S3 key this job publishes to matches scraper_memory_cache_key's own formula
    exactly, since cloud_collector.py's S3Memory.cache_key() and scraper-memory.sh's
    scraper_memory_cache_key() both have to agree on it independently.
  * SYNC-53: the publish actually lands in the real scraper-memory bucket
    (ddp-openstates-scraper-memory by default), not just that "some S3 call" happened --
    the whole bug this ticket fixes was a publish that "succeeded" while silently
    targeting the wrong bucket.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import botocore.exceptions
import pytest

from ddp_sync.pipelines.mi_cookie_publish import (
    DEFAULT_SCRAPER_MEMORY_S3_BUCKET,
    _publish_key,
    run_mi_cookie_publish_job,
)
from ddp_sync.services.scrapebot_client import ScrapeBotDispatchError

_MINT_RESULT = {"cookies": [{"name": "x", "value": "y", "expires": 0}], "user_agent": "ua"}


def _fake_s3_client(upload_file_mock=None):
    client = MagicMock()
    client.upload_file = upload_file_mock or MagicMock()
    return client


def test_publish_key_matches_scraper_memory_cache_key_shape():
    assert _publish_key("prod") == "prod/mi/_cache/mi_waf_cookies.json"


@pytest.mark.asyncio
async def test_refuses_when_scraper_memory_prefix_is_not_set(monkeypatch):
    monkeypatch.delenv("SCRAPER_MEMORY_PREFIX", raising=False)
    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        result = await run_mi_cookie_publish_job()

    mock_dispatch.assert_not_awaited()
    assert result == {"success": False, "reason": "missing_scraper_memory_prefix"}


@pytest.mark.asyncio
async def test_publishes_to_the_real_scraper_memory_bucket_by_default(monkeypatch):
    """SYNC-53's own regression case: the whole bug was a publish that reported success
    while silently landing in the deprecated ddp-openstates-backups bucket instead of
    ddp-openstates-scraper-memory (the one cloud_collector.py's S3Memory actually reads
    MI's cookie from). Asserts the real default bucket name, not just that upload_file
    was called with *some* arguments."""
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    monkeypatch.delenv("SCRAPER_MEMORY_S3_BUCKET", raising=False)
    mock_client = _fake_s3_client()

    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value=_MINT_RESULT,
    ) as mock_dispatch, patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.write_cookie_cache"
    ) as mock_write, patch(
        "ddp_sync.pipelines.mi_cookie_publish.boto3.client", return_value=mock_client
    ) as mock_boto_client:
        result = await run_mi_cookie_publish_job()

    mock_dispatch.assert_awaited_once_with("mi")
    mock_write.assert_called_once()
    mock_boto_client.assert_called_once_with("s3")
    mock_client.upload_file.assert_called_once()
    call_args = mock_client.upload_file.call_args.args
    assert call_args[1] == DEFAULT_SCRAPER_MEMORY_S3_BUCKET == "ddp-openstates-scraper-memory"
    assert call_args[2] == "prod/mi/_cache/mi_waf_cookies.json"
    assert result == {"success": True, "key": "prod/mi/_cache/mi_waf_cookies.json"}


@pytest.mark.asyncio
async def test_honors_an_explicit_scraper_memory_s3_bucket_override(monkeypatch):
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    monkeypatch.setenv("SCRAPER_MEMORY_S3_BUCKET", "some-other-bucket")
    mock_client = _fake_s3_client()

    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value=_MINT_RESULT,
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.write_cookie_cache"
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.boto3.client", return_value=mock_client
    ):
        result = await run_mi_cookie_publish_job()

    assert mock_client.upload_file.call_args.args[1] == "some-other-bucket"
    assert result["success"] is True


@pytest.mark.asyncio
async def test_never_raises_when_mint_fails(monkeypatch):
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        side_effect=ScrapeBotDispatchError("mint failed"),
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.boto3.client"
    ) as mock_boto_client:
        result = await run_mi_cookie_publish_job()

    mock_boto_client.assert_not_called()
    assert result["success"] is False
    assert result["reason"] == "mint_failed"


@pytest.mark.asyncio
async def test_never_raises_when_publish_fails_with_a_client_error(monkeypatch):
    """The AccessDenied/NoSuchBucket-shaped failure -- boto3's real exception for an S3 API
    error, the equivalent of the old wrapper's nonzero exit."""
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    mock_client = _fake_s3_client(
        upload_file_mock=MagicMock(
            side_effect=botocore.exceptions.ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "PutObject"
            )
        )
    )
    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value=_MINT_RESULT,
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.write_cookie_cache"
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.boto3.client", return_value=mock_client
    ):
        result = await run_mi_cookie_publish_job()

    assert result["success"] is False
    assert result["reason"] == "publish_failed"


@pytest.mark.asyncio
async def test_never_raises_when_boto3_client_construction_itself_fails(monkeypatch):
    """No credentials configured at all (NoCredentialsError, a BotoCoreError subclass, not
    a ClientError) must still come back structured, not crash the scheduler."""
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value=_MINT_RESULT,
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.write_cookie_cache"
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.boto3.client",
        side_effect=botocore.exceptions.NoCredentialsError(),
    ):
        result = await run_mi_cookie_publish_job()

    assert result["success"] is False
    assert result["reason"] == "unexpected_error"


@pytest.mark.asyncio
async def test_never_raises_when_write_cookie_cache_raises(monkeypatch):
    """pm-review: the docstring's "never raises" claim wasn't actually enforced past the
    mint step -- a write_cookie_cache failure (disk full, permissions, ...) must also come
    back as a structured failure, not propagate."""
    monkeypatch.setenv("SCRAPER_MEMORY_PREFIX", "prod")
    with patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.dispatch_mint_cookies",
        new_callable=AsyncMock,
        return_value=_MINT_RESULT,
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.scrapebot_client.write_cookie_cache",
        side_effect=OSError("disk full"),
    ), patch(
        "ddp_sync.pipelines.mi_cookie_publish.boto3.client"
    ) as mock_boto_client:
        result = await run_mi_cookie_publish_job()

    mock_boto_client.assert_not_called()
    assert result["success"] is False
    assert result["reason"] == "unexpected_error"
