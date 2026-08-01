"""Tests for the LegBot dispatch client (ddp-agents PLAN-legbot.md Phase 3)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.services.legbot_client import (
    LegBotDispatchError,
    dispatch_bill_changelog,
    dispatch_bill_position_verification,
    dispatch_bill_question,
)


@dataclass
class _FakeSettings:
    cams_base_url: str = "http://localhost:8000"
    cams_api_token: str = "test-token"
    cams_artifacts_dir: str = ""


def _mock_client(*, statuses, task_id="abc123"):
    """Build a mock httpx.AsyncClient whose GET calls return `statuses` in
    order, then keep returning the last one."""
    client = AsyncMock()
    post_response = MagicMock()
    post_response.json.return_value = {"task_id": task_id}
    post_response.raise_for_status.return_value = None
    client.post = AsyncMock(return_value=post_response)

    remaining = list(statuses)

    async def _get(*args, **kwargs):
        status = remaining.pop(0) if remaining else statuses[-1]
        resp = MagicMock()
        resp.json.return_value = {"status": status}
        resp.raise_for_status.return_value = None
        return resp

    client.get = AsyncMock(side_effect=_get)
    return client


def _patch_async_client(mock_client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.legbot_client.httpx.AsyncClient", return_value=cm)


@pytest.mark.asyncio
async def test_missing_artifacts_dir_raises_immediately():
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=""),
    ):
        with pytest.raises(LegBotDispatchError, match="CAMS_ARTIFACTS_DIR"):
            await dispatch_bill_question("https://example.com/bill.pdf", "summary_500char")


@pytest.mark.asyncio
async def test_happy_path_returns_answer(tmp_path):
    task_id = "abc123"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    answer = {"text": "A plain-language summary.", "insufficient_information": False}
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"answer": answer, "backend": "mlx"})
    )

    mock_client = _mock_client(statuses=["queued", "running", "completed"], task_id=task_id)
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        result = await dispatch_bill_question(
            "https://example.com/bill.pdf", "summary_500char"
        )

    assert result == {"answer": answer, "backend": "mlx"}
    # Confirm the dispatched payload matches the shape LegBot's handlers expect
    post_call = mock_client.post.await_args
    assert post_call.kwargs["json"]["bot"] == "legbot"
    assert post_call.kwargs["json"]["task_type"] == "analyze_bill"
    assert post_call.kwargs["json"]["payload"]["question_type"] == "summary_500char"
    assert post_call.kwargs["json"]["payload"]["caller"] == "ddp_sync"


@pytest.mark.asyncio
async def test_verify_bill_position_sends_url_and_claim_only(tmp_path):
    """PLAN-bill-document-provenance.md's Organization Position Research
    addition -- the payload must be exactly {url, claim, question_type,
    caller}. No citation_excerpt/page_text field exists in this payload at
    all (ddp-agents' handlers.py:194-198 requires only url and claim)."""
    task_id = "verify789"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    answer = {
        "verdict": "confirmed",
        "insufficient_information": False,
        "content_looks_incomplete": False,
        "explanation": "The page confirms the organization's stated position.",
    }
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"answer": answer, "backend": "openai"})
    )

    mock_client = _mock_client(statuses=["queued", "completed"], task_id=task_id)
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        result = await dispatch_bill_position_verification(
            "https://example.invalid/org-statement", "Sierra Club supports HB123"
        )

    assert result == {"answer": answer, "backend": "openai"}
    payload = mock_client.post.await_args.kwargs["json"]["payload"]
    assert payload == {
        "url": "https://example.invalid/org-statement",
        "claim": "Sierra Club supports HB123",
        "question_type": "verify_bill_position",
        "caller": "ddp_sync",
    }


@pytest.mark.asyncio
async def test_failed_task_raises(tmp_path):
    mock_client = _mock_client(statuses=["queued", "failed"])
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(tmp_path)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        with pytest.raises(LegBotDispatchError, match="status=failed"):
            await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")


@pytest.mark.asyncio
async def test_changelog_happy_path_sends_two_input_payload(tmp_path):
    task_id = "chg456"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    answer = {
        "sections_added": ["A new penalty provision in Section 4"],
        "sections_removed": [],
        "sections_modified": ["The definitions in Section 2"],
        "policy_implications": "Tightens enforcement.",
        "insufficient_information": False,
    }
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"answer": answer, "backend": "claude"})
    )

    mock_client = _mock_client(statuses=["queued", "completed"], task_id=task_id)
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        result = await dispatch_bill_changelog(
            "https://example.com/old-bill.pdf",
            "--- a/old\n+++ b/new\n@@ -1 +1 @@\n-old line\n+new line",
        )

    assert result == {"answer": answer, "backend": "claude"}
    post_call = mock_client.post.await_args
    payload = post_call.kwargs["json"]["payload"]
    assert payload["question_type"] == "bill_changelog"
    assert payload["old_bill_source"] == "https://example.com/old-bill.pdf"
    assert payload["diff_source"].startswith("--- a/old")
    assert payload["diff_format"] == "unified_diff_v1"
    assert payload["caller"] == "ddp_sync"


@pytest.mark.asyncio
async def test_timeout_raises_without_hanging_forever(tmp_path):
    mock_client = _mock_client(statuses=["queued", "running", "running", "running"])
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(tmp_path)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ), patch(
        "ddp_sync.services.legbot_client.time.monotonic",
        side_effect=[0.0, 0.0, 200.0],  # exceeds the default 120s timeout on the 2nd poll check
    ):
        with pytest.raises(LegBotDispatchError, match="did not finish within"):
            await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")
