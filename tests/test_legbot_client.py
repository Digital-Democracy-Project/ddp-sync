"""Tests for the LegBot dispatch client (ddp-agents PLAN-legbot.md Phase 3)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
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
    legbot_dispatch_timeout_seconds: float = 1200.0


def _mock_client(*, statuses, task_id="abc123", delete_should_fail=False):
    """Build a mock httpx.AsyncClient whose GET calls return `statuses` in
    order, then keep returning the last one. `delete_should_fail` controls
    the mocked DELETE /tasks/{id} cancel call SYNC-20 makes on timeout."""
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

    if delete_should_fail:
        client.delete = AsyncMock(side_effect=httpx.HTTPError("cancel request failed"))
    else:
        delete_response = MagicMock()
        delete_response.raise_for_status.return_value = None
        client.delete = AsyncMock(return_value=delete_response)

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
        side_effect=[0.0, 0.0, 200.0],  # exceeds the explicit 120s timeout on the 2nd poll check
    ):
        with pytest.raises(LegBotDispatchError, match="did not finish within"):
            await dispatch_bill_question(
                "https://example.com/bill.pdf", "pros_cons", timeout_seconds=120.0
            )


@pytest.mark.asyncio
async def test_timeout_seconds_none_resolves_from_settings(tmp_path):
    """A caller passing no explicit timeout_seconds gets
    settings.legbot_dispatch_timeout_seconds dynamically, not a hardcoded
    module constant -- confirmed by setting a distinctly small value on
    _FakeSettings and asserting the poll loop actually times out at that
    value specifically, not some other fixed number.

    MLX/local-model dispatches have no per-call cost the way cloud API
    tokens do, so the real default (1200s, see config.py) is deliberately
    generous rather than a tight guardrail -- found live 2026-08-15 when a
    hardcoded 120s cut off 4 of 8 real bill-artifact dispatches under
    back-to-back sequential load.
    """
    mock_client = _mock_client(statuses=["queued", "running", "running"])
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(
            cams_artifacts_dir=str(tmp_path), legbot_dispatch_timeout_seconds=7.0
        ),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ), patch(
        "ddp_sync.services.legbot_client.time.monotonic",
        side_effect=[0.0, 0.0, 8.0],  # exceeds the settings-resolved 7s deadline
    ):
        with pytest.raises(LegBotDispatchError, match="did not finish within 7.0s"):
            await dispatch_bill_question("https://example.com/bill.pdf", "summary_500char")


@pytest.mark.asyncio
async def test_timeout_cancels_the_orphaned_cams_task(tmp_path):
    """SYNC-20: giving up on a task must not just walk away from it -- the
    CAMS task keeps running orphaned otherwise, indefinitely occupying a
    worker (and, for LegBot, the shared MLX slot). Confirms the DELETE
    call actually happens, with the right task_id and auth header."""
    mock_client = _mock_client(
        statuses=["queued", "running", "running", "running"], task_id="task-to-cancel",
    )
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(tmp_path)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ), patch(
        "ddp_sync.services.legbot_client.time.monotonic",
        side_effect=[0.0, 0.0, 200.0],
    ):
        with pytest.raises(LegBotDispatchError, match="did not finish within"):
            await dispatch_bill_question(
                "https://example.com/bill.pdf", "pros_cons", timeout_seconds=120.0
            )

    mock_client.delete.assert_awaited_once_with(
        "http://localhost:8000/api/v1/tasks/task-to-cancel",
        headers={"Authorization": "Bearer test-token"},
    )


@pytest.mark.asyncio
async def test_cancel_failure_does_not_mask_the_original_timeout_error(tmp_path):
    """A cancel-call failure (CAMS unreachable, 404, etc.) must be logged,
    never raised in place of -- or swallowing -- the real
    LegBotDispatchError this function was already about to raise."""
    mock_client = _mock_client(
        statuses=["queued", "running", "running", "running"],
        delete_should_fail=True,
    )
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(tmp_path)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ), patch(
        "ddp_sync.services.legbot_client.time.monotonic",
        side_effect=[0.0, 0.0, 200.0],
    ):
        with pytest.raises(LegBotDispatchError, match="did not finish within"):
            await dispatch_bill_question(
                "https://example.com/bill.pdf", "pros_cons", timeout_seconds=120.0
            )

    mock_client.delete.assert_awaited_once()
