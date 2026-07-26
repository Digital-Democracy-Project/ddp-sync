"""LegBot dispatch client — ddp-agents' PLAN-legbot.md Phase 3 / ddp-infra's
PLAN-bill-document-provenance.md Phase 8.

Calls CAMS's generic task API (bot="legbot", task_type="analyze_bill") to
get a structured answer about a bill's text — the same interface Agent
Smith's own dispatch_legbot tool uses. No CAMS-side code exists specific to
this caller; this is a second caller of an already-general endpoint.

Only bill_summary/bill_pros_cons (LegBot's existing summary_500char/pros_cons
question types) are wired through this client today — bill_changelog is a
separate capability gated on ddp-infra's own Phase 1 diff computation
landing first (see PLAN-legbot.md Phase 3).

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
# LegBot's own Capability.metadata.latency_estimate is ~60s (a single
# reasoning call, no browser loop) — this leaves real headroom above that,
# not a tight deadline.
_DEFAULT_TIMEOUT_SECONDS = 120.0
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
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
) -> dict:
    """Dispatch an analyze_bill task to LegBot and return its structured answer.

    Args:
        bill_source: URL to the bill's PDF/HTML, or the raw bill text.
        question_type: one of LegBot's existing question types
            (e.g. "summary_500char", "pros_cons").
        timeout_seconds: how long to poll before giving up.

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

    Raises:
        LegBotDispatchError: task failed, timed out, or its result couldn't
            be read from disk.
    """
    settings = get_settings()
    if not settings.cams_artifacts_dir:
        raise LegBotDispatchError(
            "CAMS_ARTIFACTS_DIR is not configured — cannot read LegBot's result."
        )

    headers = {"Authorization": f"Bearer {settings.cams_api_token}"}
    create_payload = {
        "bot": "legbot",
        "task_type": "analyze_bill",
        "payload": {
            "bill_source": bill_source,
            "question_type": question_type,
            "caller": "ddp_sync",
        },
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

        status = "queued"
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            status_resp = await client.get(
                f"{settings.cams_base_url}/api/v1/tasks/{task_id}", headers=headers
            )
            status_resp.raise_for_status()
            status = status_resp.json()["status"]
            if status in _TERMINAL_STATUSES:
                break
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        else:
            raise LegBotDispatchError(
                f"LegBot task {task_id} did not finish within {timeout_seconds}s "
                f"(last status: {status})"
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
