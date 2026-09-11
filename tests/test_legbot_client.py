"""Tests for the LegBot dispatch client (ddp-agents PLAN-legbot.md Phase 3)."""

from __future__ import annotations

import asyncio

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
    # AGENTS-42: defaults to the real production default (3600s) so tests
    # that don't care about the two-phase timeout at all (happy paths) don't
    # need to think about it. Tests that DO exercise the timeout/cancel path
    # set this explicitly to whatever ceiling they want to hit quickly.
    legbot_queue_wait_timeout_seconds: float = 3600.0

    # SYNC-39: real production default is 1.0. Zero here so the poll loop does
    # not add wall-clock to a suite that fakes every HTTP call anyway -- a test
    # asserting timeout behaviour cares about the deadline, not the cadence.
    # The one test that cares about the interval itself sets it explicitly.
    legbot_poll_interval_seconds: float = 0.0

    # SYNC-60: real production defaults are 5/2.0. Small attempt count and
    # zero backoff here for the same reason as legbot_poll_interval_seconds
    # above -- tests exercising the retry path care about attempt counts and
    # which errors are retried, not real wall-clock backoff.
    legbot_poll_retry_max_attempts: int = 3
    legbot_poll_retry_backoff_seconds: float = 0.0


def _mock_client(
    *, statuses, task_id="abc123", delete_should_fail=False,
    mlx_generation_started_at_by_index=None, omit_generation_field=False,
):
    """Build a mock httpx.AsyncClient whose GET calls return `statuses` in
    order, then keep returning the last one. `delete_should_fail` controls
    the mocked DELETE /tasks/{id} cancel call SYNC-20 makes on timeout.

    `mlx_generation_started_at_by_index` (AGENTS-42): optional dict mapping
    a poll index (0-based, aligned with `statuses`) to the
    mlx_generation_started_at value that poll's response should carry --
    lets a test simulate the marker appearing partway through polling, the
    same field cams/api/routes.py's TaskResponse now always includes.

    `omit_generation_field` (AGENTS-42): when True, the mocked response
    dict has no "mlx_generation_started_at" key at all -- simulates an
    older CAMS deployed before this field existed, as opposed to a newer
    CAMS reporting an empty value for a task that just hasn't started
    generating yet. These two are deliberately handled differently by
    legbot_client.py's own two-phase timeout logic.
    """
    client = AsyncMock()
    post_response = MagicMock()
    post_response.json.return_value = {"task_id": task_id}
    post_response.raise_for_status.return_value = None
    client.post = AsyncMock(return_value=post_response)

    remaining = list(statuses)
    marker_map = mlx_generation_started_at_by_index or {}
    call_index = {"n": 0}

    async def _get(*args, **kwargs):
        idx = call_index["n"]
        call_index["n"] += 1
        status = remaining.pop(0) if remaining else statuses[-1]
        resp = MagicMock()
        body = {"status": status}
        if not omit_generation_field:
            body["mlx_generation_started_at"] = marker_map.get(idx, "")
        resp.json.return_value = body
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


def _http_status_error_response(status_code: int):
    """A MagicMock httpx.Response whose raise_for_status() raises a real
    httpx.HTTPStatusError carrying that status code -- SYNC-60's retry
    logic branches on exc.response.status_code, so a bare MagicMock
    HTTPError (as elsewhere in this file, where the status code never
    matters) isn't enough here."""
    request = httpx.Request("GET", "http://localhost:8000/api/v1/tasks/x")
    real_response = httpx.Response(status_code, request=request)
    resp = MagicMock()
    resp.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError(
            f"{status_code}", request=request, response=real_response,
        )
    )
    return resp


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


# ---------------------------------------------------------------------------
# SYNC-61: on_task_created is awaited with the task_id before any polling, so
# a caller can durably record it in case this process dies partway through
# the poll loop below -- and check_task_status/read_task_result are the
# recovery sweep's own one-shot equivalents of that same poll/read sequence.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_task_created_is_awaited_with_the_task_id_before_polling(tmp_path):
    task_id = "chg789"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    answer = {"insufficient_information": False}
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"answer": answer, "backend": "mlx"})
    )

    calls = []

    async def _on_task_created(created_task_id):
        calls.append(created_task_id)

    mock_client = _mock_client(statuses=["queued", "completed"], task_id=task_id)
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        await dispatch_bill_changelog(
            "https://example.com/old-bill.pdf",
            "--- a/old\n+++ b/new\n@@ -1 +1 @@\n-old line\n+new line",
            on_task_created=_on_task_created,
        )

    assert calls == [task_id]


@pytest.mark.asyncio
async def test_on_task_created_failure_is_not_swallowed():
    """A callback that fails must not be silently absorbed -- see the
    parameter's own docstring: a real dispatch shouldn't be judged to have
    "worked" over a bookkeeping failure the caller has no way to notice."""
    async def _broken_callback(task_id):
        raise RuntimeError("redis is down")

    mock_client = _mock_client(statuses=["queued"], task_id="whatever")
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir="/tmp/unused"),
    ), _patch_async_client(mock_client):
        with pytest.raises(RuntimeError, match="redis is down"):
            await dispatch_bill_changelog(
                "https://example.com/old-bill.pdf", "--- a\n+++ b\n",
                on_task_created=_broken_callback,
            )


@pytest.mark.asyncio
async def test_on_task_created_omitted_is_a_no_op():
    """Every pre-existing caller passes nothing here -- confirm that still
    behaves exactly as before (already covered by every other test in this
    file not passing it, but made explicit once)."""
    task_id = "no-callback"
    mock_client = _mock_client(statuses=["queued", "failed"], task_id=task_id)
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir="/tmp/unused"),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ):
        with pytest.raises(LegBotDispatchError, match="status=failed"):
            await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")


def test_read_task_result_returns_answer_and_backend(tmp_path):
    from ddp_sync.services.legbot_client import read_task_result

    task_id = "read123"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    answer = {"text": "recovered answer", "insufficient_information": False}
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"answer": answer, "backend": "opus"})
    )

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ):
        result = read_task_result(task_id)

    assert result == {"answer": answer, "backend": "opus"}


def test_read_task_result_missing_file_raises():
    from ddp_sync.services.legbot_client import read_task_result

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir="/nonexistent-dir-xyz"),
    ):
        with pytest.raises(LegBotDispatchError, match="Could not read task_result.json"):
            read_task_result("missing-task")


def test_read_task_result_no_answer_key_raises(tmp_path):
    from ddp_sync.services.legbot_client import read_task_result

    task_id = "no-answer"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    (artifacts_dir / task_id / "task_result.json").write_text(json.dumps({"backend": "mlx"}))

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ):
        with pytest.raises(LegBotDispatchError, match="no 'answer' key"):
            read_task_result(task_id)


@pytest.mark.asyncio
async def test_check_task_status_returns_the_raw_response_body():
    from ddp_sync.services.legbot_client import check_task_status

    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "completed", "mlx_generation_started_at": "x"}
    mock_response.raise_for_status = MagicMock()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(),
    ), patch("ddp_sync.services.legbot_client.httpx.AsyncClient", return_value=cm):
        result = await check_task_status("some-task-id")

    assert result == {"status": "completed", "mlx_generation_started_at": "x"}
    get_call = mock_client.get.await_args
    assert get_call.args[0] == "http://localhost:8000/api/v1/tasks/some-task-id"
    assert get_call.kwargs["headers"]["Authorization"] == "Bearer test-token"


@pytest.mark.asyncio
async def test_check_task_status_raises_on_error_response():
    from ddp_sync.services.legbot_client import check_task_status

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError("404", request=MagicMock(), response=MagicMock())
    )
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_client)
    cm.__aexit__ = AsyncMock(return_value=False)

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(),
    ), patch("ddp_sync.services.legbot_client.httpx.AsyncClient", return_value=cm):
        with pytest.raises(httpx.HTTPStatusError):
            await check_task_status("unknown-task-id")


@pytest.mark.asyncio
async def test_timeout_raises_without_hanging_forever(tmp_path):
    """No mlx_generation_started_at marker ever appears in these mocked GET
    responses (empty string every time) -- AGENTS-42's fallback path. The
    queue-wait ceiling (legbot_queue_wait_timeout_seconds), not the
    per-call inference timeout_seconds, is what governs here, since the
    two are indistinguishable from the caller's side until one of them
    fires -- see legbot_client.py's own comment on this exact tradeoff."""
    mock_client = _mock_client(statuses=["queued", "running", "running", "running"])
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(
            cams_artifacts_dir=str(tmp_path), legbot_queue_wait_timeout_seconds=120.0,
        ),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ), patch(
        "ddp_sync.services.legbot_client.time.monotonic",
        side_effect=[0.0, 0.0, 200.0],  # exceeds the 120s queue-wait ceiling on the 2nd poll check
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

    AGENTS-42: legbot_dispatch_timeout_seconds now governs the *inference*
    phase specifically (from when mlx_generation_started_at is first
    observed), not the flat dispatch-to-completion deadline -- this test's
    mocked GET responses show the marker on the very first poll so the
    settings-resolved value is what's actually exercised as the timeout.
    """
    mock_client = _mock_client(
        statuses=["queued", "running"],
        mlx_generation_started_at_by_index={0: "2026-08-19T00:00:00+00:00"},
    )
    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(
            cams_artifacts_dir=str(tmp_path), legbot_dispatch_timeout_seconds=7.0,
        ),
    ), _patch_async_client(mock_client), patch(
        "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
    ), patch(
        "ddp_sync.services.legbot_client.time.monotonic",
        # call1: initial (queue-wait) deadline; call2: iter1 while-check
        # (True); [poll shows marker -> deadline resets] call3: reset point;
        # call4: iter2 while-check (True, 2.0 < 1.0+7.0=8.0); call5: iter3
        # while-check (False, 9.0 >= 8.0) -- exceeds the settings-resolved
        # 7s inference deadline measured from the reset point (1.0), not
        # from dispatch (0.0).
        side_effect=[0.0, 0.0, 1.0, 2.0, 9.0],
    ):
        with pytest.raises(LegBotDispatchError, match="inference timeout 7.0s"):
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
        return_value=_FakeSettings(
            cams_artifacts_dir=str(tmp_path), legbot_queue_wait_timeout_seconds=120.0,
        ),
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
        return_value=_FakeSettings(
            cams_artifacts_dir=str(tmp_path), legbot_queue_wait_timeout_seconds=120.0,
        ),
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


# ---------------------------------------------------------------------------
# SYNC-60: the status poll retries a transient failure (connection-level, or
# a 5xx HTTP response) against the same task_id, instead of treating either
# as an immediate, permanent failure.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_retries_connect_error_then_succeeds(tmp_path):
    """A brief connection-level failure (e.g. CAMS's port not yet accepting
    connections mid-`cams reload`) during polling must not fail the
    dispatch -- retrying the same task_id picks up its real status once
    CAMS answers again."""
    task_id = "task-flaky-conn"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    answer = {"text": "ok", "insufficient_information": False}
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"answer": answer, "backend": "mlx"})
    )

    post_response = MagicMock()
    post_response.json.return_value = {"task_id": task_id}
    post_response.raise_for_status.return_value = None

    call_count = {"n": 0}

    async def _flaky_get(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 2:
            raise httpx.ConnectError("connection refused")
        resp = MagicMock()
        resp.json.return_value = {"status": "completed", "mlx_generation_started_at": ""}
        resp.raise_for_status.return_value = None
        return resp

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=post_response)
    mock_client.get = AsyncMock(side_effect=_flaky_get)
    mock_client.delete = AsyncMock()

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client):
        result = await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")

    assert result == {"answer": answer, "backend": "mlx"}
    assert call_count["n"] == 3  # 2 failures + 1 success
    mock_client.delete.assert_not_awaited()  # never gave up -- no orphan to cancel


@pytest.mark.asyncio
async def test_poll_retries_5xx_then_succeeds(tmp_path):
    """Confirmed live 2026-09-11: a burst of near-simultaneous polls from a
    947-bill backfill hit CAMS's status endpoint with real 500s 618 times
    over ~6 minutes; some of the affected tasks had already finished
    successfully on CAMS's side. A transient 500 must not fail the
    dispatch either."""
    task_id = "task-flaky-500"
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / task_id).mkdir(parents=True)
    answer = {"text": "ok", "insufficient_information": False}
    (artifacts_dir / task_id / "task_result.json").write_text(
        json.dumps({"answer": answer, "backend": "mlx"})
    )

    post_response = MagicMock()
    post_response.json.return_value = {"task_id": task_id}
    post_response.raise_for_status.return_value = None

    call_count = {"n": 0}

    async def _flaky_get(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 2:
            return _http_status_error_response(500)
        resp = MagicMock()
        resp.json.return_value = {"status": "completed", "mlx_generation_started_at": ""}
        resp.raise_for_status.return_value = None
        return resp

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=post_response)
    mock_client.get = AsyncMock(side_effect=_flaky_get)
    mock_client.delete = AsyncMock()

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(artifacts_dir)),
    ), _patch_async_client(mock_client):
        result = await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")

    assert result == {"answer": answer, "backend": "mlx"}
    assert call_count["n"] == 3
    mock_client.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_poll_does_not_retry_a_non_5xx_error_and_it_propagates_unwrapped(tmp_path):
    """A 404 (or any non-5xx) is not a transient condition worth retrying,
    and this ticket must not change what happens to it: no retry attempt,
    no cancel-on-give-up wrapping, propagates exactly as before SYNC-60."""
    call_count = {"n": 0}

    async def _get(*args, **kwargs):
        call_count["n"] += 1
        return _http_status_error_response(404)

    post_response = MagicMock()
    post_response.json.return_value = {"task_id": "task-404"}
    post_response.raise_for_status.return_value = None

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=post_response)
    mock_client.get = AsyncMock(side_effect=_get)
    mock_client.delete = AsyncMock()

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(cams_artifacts_dir=str(tmp_path)),
    ), _patch_async_client(mock_client):
        with pytest.raises(httpx.HTTPStatusError):
            await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")

    assert call_count["n"] == 1
    mock_client.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_poll_retry_budget_exhausted_cancels_and_raises_dispatch_error(tmp_path):
    """A SUSTAINED transient failure (CAMS down for good, or a real,
    ongoing outage) must still give up -- bounded by
    legbot_poll_retry_max_attempts -- through the same cancel-on-give-up +
    LegBotDispatchError path a client-side deadline uses, not hang forever
    or raise a raw httpx exception."""
    post_response = MagicMock()
    post_response.json.return_value = {"task_id": "task-down"}
    post_response.raise_for_status.return_value = None

    call_count = {"n": 0}

    async def _get(*args, **kwargs):
        call_count["n"] += 1
        raise httpx.ConnectError("connection refused")

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=post_response)
    mock_client.get = AsyncMock(side_effect=_get)
    delete_response = MagicMock()
    delete_response.raise_for_status.return_value = None
    mock_client.delete = AsyncMock(return_value=delete_response)

    with patch(
        "ddp_sync.services.legbot_client.get_settings",
        return_value=_FakeSettings(
            cams_artifacts_dir=str(tmp_path), legbot_poll_retry_max_attempts=3,
        ),
    ), _patch_async_client(mock_client):
        with pytest.raises(LegBotDispatchError, match="status poll failed after 3 attempts"):
            await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")

    assert call_count["n"] == 3
    mock_client.delete.assert_awaited_once_with(
        "http://localhost:8000/api/v1/tasks/task-down",
        headers={"Authorization": "Bearer test-token"},
    )


class TestAgents42TwoPhaseTimeout:
    """AGENTS-42: legbot_client.py's poll loop no longer conflates "queued
    behind LegBot's single-instance MLX pool" with "actively generating" --
    both used to read as CAMS task status "running" with one flat
    legbot_dispatch_timeout_seconds deadline measured from dispatch. A
    single crashed MLX request wedging the pool once caused 45 consecutive
    tasks to each burn their full budget waiting for a turn that never
    came (~14h wasted, 2026-08-19). Three scenarios per the ticket:
    (a) marker never appears -> today's flat-timeout shape still applies,
        just governed by the new, more generous queue-wait ceiling instead
        of the old too-short one (see legbot_client.py's own comment on
        why the two are indistinguishable from here until one fires).
    (b) marker appears partway through -> the inference deadline resets
        from that moment, genuinely extending the total wait past what the
        queue-wait ceiling alone would have allowed.
    (c) marker never appears and the queue-wait ceiling itself is
        exceeded -> still bounded, still raises and cancels -- this is not
        an unboundable wait.
    (d) the response has no mlx_generation_started_at key AT ALL (an older
        CAMS deployed before this field existed, not merely a newer CAMS
        reporting an empty value) -> collapses to exactly the old, single
        flat legbot_dispatch_timeout_seconds-from-dispatch behavior, full
        stop -- no deploy-order requirement between ddp-sync and
        ddp-agents (PM-review-folded: (a)/(c) alone would have let a
        genuinely-still-queued task and an old-CAMS response share one
        (much larger) ceiling, silently changing timeout behavior for
        every long-running task during a mismatched rollout).
    """

    @pytest.mark.asyncio
    async def test_a_marker_never_appears_falls_back_to_a_flat_timeout(self, tmp_path):
        """Every mocked GET response carries mlx_generation_started_at=""
        (the _mock_client default) -- this is scenario (a) AND (c) at
        once: no marker ever shows up, and the (small, test-scoped)
        queue-wait ceiling is what eventually fires, bounding the wait."""
        mock_client = _mock_client(statuses=["queued", "running", "running", "running"])
        with patch(
            "ddp_sync.services.legbot_client.get_settings",
            return_value=_FakeSettings(
                cams_artifacts_dir=str(tmp_path), legbot_queue_wait_timeout_seconds=30.0,
            ),
        ), _patch_async_client(mock_client), patch(
            "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
        ), patch(
            "ddp_sync.services.legbot_client.time.monotonic",
            side_effect=[0.0, 0.0, 50.0],  # exceeds the 30s queue-wait ceiling
        ):
            with pytest.raises(
                LegBotDispatchError,
                match=r"queue-wait timeout 30\.0s \(generation never started\)",
            ):
                await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")

        # SYNC-20's cancel-on-timeout behavior is unaffected by this change.
        mock_client.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_b_marker_appearing_partway_resets_the_inference_deadline(self, tmp_path):
        """A small queue-wait ceiling (10s) would, on its own, cut this
        task off at t=10 -- but the marker shows up at the 3rd poll, and
        the resulting inference deadline (reset to +5s from THAT point,
        i.e. t=14.5) is what actually lets the still-in-flight task reach
        its real "completed" poll at t=13, past the original queue-wait
        ceiling. This is the genuine extension the fix exists to provide."""
        task_id = "task-b"
        artifacts_dir = tmp_path / "artifacts"
        (artifacts_dir / task_id).mkdir(parents=True)
        answer = {"text": "ok", "insufficient_information": False}
        (artifacts_dir / task_id / "task_result.json").write_text(
            json.dumps({"answer": answer, "backend": "mlx"})
        )

        mock_client = _mock_client(
            statuses=["queued", "queued", "running", "completed"],
            task_id=task_id,
            mlx_generation_started_at_by_index={2: "2026-08-19T00:00:09+00:00"},
        )
        with patch(
            "ddp_sync.services.legbot_client.get_settings",
            return_value=_FakeSettings(
                cams_artifacts_dir=str(artifacts_dir),
                legbot_dispatch_timeout_seconds=5.0,
                legbot_queue_wait_timeout_seconds=10.0,
            ),
        ), _patch_async_client(mock_client), patch(
            "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
        ), patch(
            "ddp_sync.services.legbot_client.time.monotonic",
            # call1: initial (queue-wait) deadline = 0.0+10.0=10.0
            # call2 (iter1 check, 2.0<10 True) -> poll "queued", no marker
            # call3 (iter2 check, 8.0<10 True) -> poll "queued", no marker
            # call4 (iter3 check, 9.5<10 True) -> poll "running", marker seen
            # call5 (reset point, 9.5) -> new deadline = 9.5+5.0 = 14.5
            # call6 (iter4 check, 13.0<14.5 True) -> poll "completed", done
            side_effect=[0.0, 2.0, 8.0, 9.5, 9.5, 13.0],
        ):
            result = await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")

        assert result == {"answer": answer, "backend": "mlx"}
        # Never timed out/cancelled -- the reset gave it exactly the room
        # it needed past the original 10s queue-wait ceiling.
        mock_client.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_d_field_entirely_absent_uses_legacy_flat_timeout_not_queue_wait(self, tmp_path):
        """An old CAMS's response has no mlx_generation_started_at key at
        all -- this must NOT share the generous queue-wait ceiling scenario
        (a)/(c) use for a newer CAMS's empty-value response. Queue-wait is
        set deliberately huge (3600s, the real production default) while
        the legacy dispatch timeout is small (5s) -- if this incorrectly
        fell through to the queue-wait path, the mocked clock (which never
        exceeds 8.0) would never trigger a timeout at all and the test
        would hang/fail differently."""
        mock_client = _mock_client(statuses=["running"], omit_generation_field=True)
        with patch(
            "ddp_sync.services.legbot_client.get_settings",
            return_value=_FakeSettings(
                cams_artifacts_dir=str(tmp_path),
                legbot_dispatch_timeout_seconds=5.0,
                legbot_queue_wait_timeout_seconds=3600.0,
            ),
        ), _patch_async_client(mock_client), patch(
            "ddp_sync.services.legbot_client.asyncio.sleep", new_callable=AsyncMock
        ), patch(
            "ddp_sync.services.legbot_client.time.monotonic",
            # call1: dispatch_time=0.0; call2: iter1 while-check (0.0<3600
            # True) -> poll has no marker field -> legacy mode, deadline
            # collapses to dispatch_time+5.0=5.0; call3: iter2 while-check
            # (8.0<5.0 False) -> times out at the SMALL legacy deadline,
            # proving the huge queue-wait ceiling was never in play.
            side_effect=[0.0, 0.0, 8.0],
        ):
            with pytest.raises(
                LegBotDispatchError,
                match=r"legacy dispatch timeout 5\.0s \(CAMS response has no mlx_generation_started_at field\)",
            ):
                await dispatch_bill_question("https://example.com/bill.pdf", "pros_cons")

        mock_client.delete.assert_awaited_once()


# --- SYNC-39: the poll interval is configuration, and it is what governs ---

class TestPollInterval:
    """The 5-second constant this replaces cost latency on every call.

    Measured on VA 2026S1 (2026-08-27) from CAMS's own un-quantised
    timestamps: real generation ran a median of 1.72s (n=220) while the client
    observed 5.10s, so ~3.4s of every call was this loop sleeping. That waste
    is unconditional and is what the change is justified on.

    A second, weaker effect is that the interval also sets how long a bill
    leaves its worker idle between calls (13 of 18 cold concept_statements
    prefills were a warm worker taken by another bill). Deliberately NOT
    claimed here: shortening the interval shortens the competitor's cadence
    too, so the steal rate may not improve at all. That is an open question
    this value exists to let someone answer -- see legbot_poll_interval_seconds
    in config.py.
    """

    def test_the_production_default_is_one_second(self):
        """Exact, not a range. /pm-review's point: `< 5.0` passes on 4.9, and
        the product decision was specifically 1.0 -- chosen to cut the ~3.4s
        of per-call polling latency measured on VA 2026S1."""
        from ddp_sync.config import SyncSettings
        assert SyncSettings().legbot_poll_interval_seconds == 1.0

    @pytest.mark.parametrize("bad", ["0", "-1", "nan", "inf", "-0.0", "abc", ""])
    def test_an_unusable_interval_falls_back_instead_of_busy_looping(self, bad, monkeypatch):
        """This is operator-editable, and 0 or a negative would turn the poll
        loop into a hot loop against CAMS. Falls back rather than raising: a
        typo in a .env should not stop the pipeline starting."""
        from ddp_sync.config import get_settings
        monkeypatch.setenv("LEGBOT_POLL_INTERVAL_SECONDS", bad)
        get_settings.cache_clear()
        try:
            assert get_settings().legbot_poll_interval_seconds == 1.0
        finally:
            get_settings.cache_clear()

    def test_it_is_read_from_settings_not_hardcoded(self, monkeypatch):
        from ddp_sync.config import get_settings
        monkeypatch.setenv("LEGBOT_POLL_INTERVAL_SECONDS", "2.5")
        get_settings.cache_clear()
        try:
            assert get_settings().legbot_poll_interval_seconds == 2.5
        finally:
            get_settings.cache_clear()

    @pytest.mark.asyncio
    async def test_the_loop_actually_sleeps_for_the_configured_interval(self, tmp_path):
        """Asserting the default alone would pass on code that still slept 5.

        This captures what asyncio.sleep is really called with, so an edit
        that reintroduces a literal fails here rather than in production.
        """
        task_id = "abc123"
        artifacts_dir = tmp_path / "artifacts"
        (artifacts_dir / task_id).mkdir(parents=True)
        (artifacts_dir / task_id / "task_result.json").write_text(
            json.dumps({"answer": {"text": "x", "insufficient_information": False}})
        )

        slept = []
        real_sleep = asyncio.sleep

        async def _record(d, *a, **k):
            slept.append(d)
            return await real_sleep(0)

        with patch(
            "ddp_sync.services.legbot_client.get_settings",
            return_value=_FakeSettings(
                cams_artifacts_dir=str(artifacts_dir),
                legbot_poll_interval_seconds=0.25,
            ),
        ), _patch_async_client(
            _mock_client(statuses=["queued", "queued", "completed"], task_id=task_id)
        ), patch("ddp_sync.services.legbot_client.asyncio.sleep", new=_record):
            await dispatch_bill_question("bill text", "summary_500char")

        assert slept, "the poll loop never slept -- did the loop change shape?"
        assert all(d == 0.25 for d in slept), f"slept {slept}, expected 0.25s each"
