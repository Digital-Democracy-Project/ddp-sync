"""LegBot dispatch client — ddp-agents' PLAN-legbot.md Phase 3 / ddp-infra's
PLAN-bill-document-provenance.md Phase 8.

Calls CAMS's generic task API (bot="legbot", task_type="analyze_bill") to
get a structured answer about a bill's text — the same interface Agent
Smith's own dispatch_legbot tool uses. No CAMS-side code exists specific to
this caller; this is a second caller of an already-general endpoint.

dispatch_bill_question wires LegBot's single-input question types
(summary_500char, pros_cons, etc.); dispatch_bill_changelog wires the
two-input bill_changelog type (old_bill_source + a precomputed diff, see
PLAN-legbot.md Phase 3) — the caller computes the diff, LegBot fetches its
own copy of old_bill_source to build its two-part prompt.

Scope note: this module dispatches and returns LegBot's structured answer
plus which backend produced it. It does NOT write that answer anywhere
durable — see ddp_sync.pipelines.bill_artifact_generation (ddp-infra Phase 8)
for the piece that persists this into ddp-broker-py's BillArtifact (Phase 6)
and Pinecone.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import httpx
import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

_POLL_INTERVAL_SECONDS = 5
_TERMINAL_STATUSES = ("completed", "failed", "cancelled")


class LegBotDispatchError(Exception):
    """Raised when a LegBot dispatch fails to produce a usable answer.

    Callers decide how to handle this (skip this bill, retry, alert) — this
    function never swallows a failure into a fake/empty result.
    """


async def dispatch_bill_question(
    bill_source: str,
    question_type: str,
    *,
    timeout_seconds: float | None = None,
) -> dict:
    """Dispatch an analyze_bill task to LegBot and return its structured answer.

    Args:
        bill_source: URL to the bill's PDF/HTML, or the raw bill text.
        question_type: one of LegBot's existing question types
            (e.g. "summary_500char", "pros_cons").
        timeout_seconds: how long to poll before giving up. None (the
            default) resolves to settings.legbot_dispatch_timeout_seconds
            at call time.

    Returns:
        See _dispatch_and_await.

    Raises:
        LegBotDispatchError: task failed, timed out, or its result couldn't
            be read from disk. A timeout also triggers a best-effort
            DELETE of the CAMS task before raising (SYNC-20) -- see
            _dispatch_and_await's own docstring.
    """
    return await _dispatch_and_await(
        {
            "bill_source": bill_source,
            "question_type": question_type,
            "caller": "ddp_sync",
        },
        question_type=question_type,
        timeout_seconds=timeout_seconds,
    )


async def dispatch_bill_changelog(
    old_bill_source: str,
    diff_source: str,
    *,
    diff_format: str = "unified_diff_v1",
    timeout_seconds: float | None = None,
) -> dict:
    """Dispatch a bill_changelog task to LegBot and return its structured answer.

    Args:
        old_bill_source: URL to the prior version's PDF/HTML, or its raw
            text. LegBot fetches its own copy of this (see
            ddp-agents' legbot/handlers.py handle_ingest) to build its
            two-part prompt — the caller does not need to pre-fetch it for
            LegBot's sake, only to compute diff_source below.
        diff_source: a precomputed diff between the prior and new version's
            text. LegBot does not re-derive what changed — it explains the
            impact of the changes the diff already identifies.
        diff_format: must match LegBot's one supported format
            ("unified_diff_v1" — the literal output of Python's
            difflib.unified_diff(), unvalidated against a real fixture
            corpus yet; see PLAN-legbot.md AC11a, deliberately deferred).
        timeout_seconds: how long to poll before giving up. None (the
            default) resolves to settings.legbot_dispatch_timeout_seconds
            at call time.

    Returns:
        See _dispatch_and_await. answer["insufficient_information"] is True
        both when LegBot's model judged the bill too short/vague to answer,
        and when handle_ingest skipped the task outright (no prior version
        archived, diff unavailable, unsupported diff format, malformed
        diff) — the latter also sets answer["reason"] to one of those four
        skip reasons.

    Raises:
        LegBotDispatchError: task failed, timed out, or its result couldn't
            be read from disk. A timeout also triggers a best-effort
            DELETE of the CAMS task before raising (SYNC-20) -- see
            _dispatch_and_await's own docstring.
    """
    return await _dispatch_and_await(
        {
            "old_bill_source": old_bill_source,
            "diff_source": diff_source,
            "diff_format": diff_format,
            "question_type": "bill_changelog",
            "caller": "ddp_sync",
        },
        question_type="bill_changelog",
        timeout_seconds=timeout_seconds,
    )


async def dispatch_bill_position_verification(
    url: str,
    claim: str,
    *,
    timeout_seconds: float | None = None,
) -> dict:
    """Dispatch a verify_bill_position task to LegBot and return its
    structured answer — ddp-infra's PLAN-bill-document-provenance.md Phase 8,
    "Organization Position Research" (approved 2026-08-01).

    Args:
        url: the citation URL to check the claim against. LegBot fetches
            this itself (a deliberate, documented exception to its general
            fetch-removal — see ddp-agents' legbot/handlers.py:191-230) —
            the caller does not pre-fetch page text.
        claim: a plain-language statement to judge against the fetched page,
            e.g. an organization name + position + bill identity.
        timeout_seconds: how long to poll before giving up. None (the
            default) resolves to settings.legbot_dispatch_timeout_seconds
            at call time.

    Returns:
        See _dispatch_and_await. answer contains "verdict"
        ("confirmed"/"not_confirmed"), "insufficient_information",
        "content_looks_incomplete", and "explanation" — no
        "citation_excerpt"/"page_text" field exists in this payload or
        answer at all.

    Raises:
        LegBotDispatchError: task failed, timed out, or its result couldn't
            be read from disk. A timeout also triggers a best-effort
            DELETE of the CAMS task before raising (SYNC-20) -- see
            _dispatch_and_await's own docstring.
    """
    return await _dispatch_and_await(
        {
            "url": url,
            "claim": claim,
            "question_type": "verify_bill_position",
            "caller": "ddp_sync",
        },
        question_type="verify_bill_position",
        timeout_seconds=timeout_seconds,
    )


async def _dispatch_and_await(
    payload: dict,
    *,
    question_type: str,
    timeout_seconds: float | None,
) -> dict:
    """Shared dispatch/poll/read-result mechanics for any analyze_bill payload.

    SYNC-20: if the poll loop gives up on timeout, this attempts a
    best-effort DELETE /api/v1/tasks/{task_id} on CAMS before raising --
    without it, the CAMS task keeps running as an orphan no one is
    watching, indefinitely occupying a worker (and, for LegBot, the
    shared MLX slot) even though this caller has already moved on. A
    cancel-call failure is logged but never masks or replaces the
    original LegBotDispatchError. Whether the cancel actually stops the
    work immediately (rather than just updating Redis bookkeeping)
    depends on CAMS's own implementation of that endpoint (ddp-agents'
    AGENTS-16) -- this module doesn't need to know either way, since a
    best-effort cancel is strictly better than none regardless.

    Returns:
        A dict with two keys:
          - "answer": the parsed "answer" dict LegBot's ANALYZE handler
            produced (matches each question type's output_shape,
            config/legbot_questions.yaml).
          - "backend": which router choice served this ("openai"/"mlx"/
            "claude", per ddp-agents' wm_snapshot_keys.py), or None if CAMS
            didn't record one. This is the only model-identifying field
            CAMS's task snapshot currently exposes — it does NOT include a
            precise model string (that's computed in ddp-agents'
            legbot/handlers.py but never written to the snapshot, confirmed
            2026-07-26) or a prompt version. Callers populating
            BillArtifact.model_name/model_version/prompt_version (Phase 6)
            should treat model_version/prompt_version as genuinely unknown
            (null) rather than guessing, until that gap is closed on the
            ddp-agents side.
    """
    settings = get_settings()
    if not settings.cams_artifacts_dir:
        raise LegBotDispatchError(
            "CAMS_ARTIFACTS_DIR is not configured — cannot read LegBot's result."
        )
    if timeout_seconds is None:
        timeout_seconds = settings.legbot_dispatch_timeout_seconds
    queue_wait_timeout_seconds = settings.legbot_queue_wait_timeout_seconds

    headers = {"Authorization": f"Bearer {settings.cams_api_token}"}
    create_payload = {
        "bot": "legbot",
        "task_type": "analyze_bill",
        "payload": payload,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{settings.cams_base_url}/api/v1/tasks", headers=headers, json=create_payload
        )
        resp.raise_for_status()
        task_id = resp.json()["task_id"]
        logger.info(
            "LegBot task dispatched", task_id=task_id, question_type=question_type,
        )

        # AGENTS-42: two-phase deadline, replacing one flat timeout measured
        # from dispatch. `deadline` starts generous
        # (queue_wait_timeout_seconds) so a task genuinely still queued
        # behind LegBot's single-instance MLX pool (ddp-agents'
        # _MLXInstancePool) isn't cut off before it ever gets a turn -- the
        # exact incident that motivated this (one crashed MLX request wedged
        # the pool; 45 consecutive tasks each burned their full
        # legbot_dispatch_timeout_seconds waiting in queue for a turn that
        # never came, ~14h wasted). The moment a poll response first shows
        # ddp-agents' mlx_generation_started_at marker populated (real
        # inference has begun), `deadline` resets to a fresh
        # timeout_seconds-sized window measured from *that* moment -- the
        # task gets its normal full inference budget starting from when
        # generation genuinely began, not from dispatch. If the marker never
        # appears (a non-MLX/Claude-or-OpenAI-routed task that completes
        # quickly regardless, or an older CAMS deployed without this field),
        # `deadline` simply never resets and queue_wait_timeout_seconds is
        # what bounds the wait -- still finite and still cancels the
        # orphaned task on timeout, just not the older, tighter number,
        # since a still-queued task and a task that will never start look
        # identical from here until one of them actually happens.
        status = "queued"
        generation_started_seen = False
        deadline = time.monotonic() + queue_wait_timeout_seconds
        while time.monotonic() < deadline:
            status_resp = await client.get(
                f"{settings.cams_base_url}/api/v1/tasks/{task_id}", headers=headers
            )
            status_resp.raise_for_status()
            body = status_resp.json()
            status = body["status"]
            if status in _TERMINAL_STATUSES:
                break
            if not generation_started_seen and body.get("mlx_generation_started_at"):
                generation_started_seen = True
                deadline = time.monotonic() + timeout_seconds
                logger.info(
                    "LegBot task MLX generation started -- switching from "
                    "queue-wait to inference timeout",
                    task_id=task_id, question_type=question_type,
                    inference_timeout_seconds=timeout_seconds,
                )
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        else:
            # SYNC-20: giving up here must not just walk away from the CAMS
            # task -- without this, it keeps running as an orphan no one is
            # watching, indefinitely occupying a worker (and, for LegBot,
            # the shared MLX slot) even though this caller has already
            # decided to treat it as failed. Confirmed live 2026-08-15/16:
            # 4 abandoned tasks sat RUNNING for 45+ minutes and starved a
            # later, correctly-dispatched run's own tasks of worker
            # capacity. Best-effort: a cancel-call failure must never mask
            # or replace the real error this function is about to raise.
            try:
                cancel_resp = await client.delete(
                    f"{settings.cams_base_url}/api/v1/tasks/{task_id}", headers=headers
                )
                cancel_resp.raise_for_status()
                logger.warning(
                    "LegBot task timed out client-side -- cancelled on CAMS",
                    task_id=task_id, question_type=question_type,
                )
            except httpx.HTTPError as cancel_exc:
                logger.warning(
                    "LegBot task timed out client-side -- cancel request "
                    "itself failed, task may still be running orphaned",
                    task_id=task_id, question_type=question_type,
                    error=str(cancel_exc),
                )
            timeout_desc = (
                f"inference timeout {timeout_seconds}s (generation had started)"
                if generation_started_seen
                else f"queue-wait timeout {queue_wait_timeout_seconds}s (generation never started)"
            )
            raise LegBotDispatchError(
                f"LegBot task {task_id} did not finish within its timeout window "
                f"({timeout_desc}) (last status: {status})"
            )

    if status != "completed":
        raise LegBotDispatchError(f"LegBot task {task_id} ended with status={status}")

    result_path = Path(settings.cams_artifacts_dir) / task_id / "task_result.json"
    try:
        snapshot = json.loads(result_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise LegBotDispatchError(
            f"Could not read task_result.json for {task_id}: {exc}"
        ) from exc

    answer = snapshot.get("answer")
    if answer is None:
        raise LegBotDispatchError(
            f"task_result.json for {task_id} has no 'answer' key: {snapshot}"
        )

    logger.info(
        "LegBot task completed", task_id=task_id, question_type=question_type,
        insufficient_information=answer.get("insufficient_information"),
    )
    return {"answer": answer, "backend": snapshot.get("backend")}
