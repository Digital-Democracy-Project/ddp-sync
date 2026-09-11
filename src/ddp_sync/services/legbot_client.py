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
from typing import Awaitable, Callable

import httpx
import structlog

from ddp_sync.config import get_settings

logger = structlog.get_logger()

# SYNC-39: the poll interval used to be a module constant of 5 here. It now
# lives on SyncSettings (legbot_poll_interval_seconds), which carries the
# measurements explaining why that number was costing real time.
# Not underscore-prefixed -- SYNC-61's recovery sweep (bill_artifact_
# generation.py) imports this directly, so both places agree on exactly
# which CAMS statuses are terminal rather than maintaining two separate lists.
TERMINAL_STATUSES = ("completed", "failed", "cancelled")


class _TransientPollFailure(Exception):
    """Internal signal (SYNC-60): _poll_task_status_with_retry exhausted its
    retry budget on a transient failure (5xx or a connection-level error).
    Caught by _dispatch_and_await's poll loop and converted into the same
    cancel-on-give-up + LegBotDispatchError path already used for a
    client-side deadline. Deliberately NOT used for a non-retryable error
    (e.g. a 404) -- that propagates directly and unwrapped, exactly as
    before this ticket (its own AC: no behavior change outside the
    connection-failure/5xx cases).
    """


async def _poll_task_status_with_retry(
    client: httpx.AsyncClient, url: str, headers: dict, *, task_id: str,
) -> dict:
    """GET .../tasks/{task_id} once, retrying a TRANSIENT failure against
    the same task_id (SYNC-60).

    Two failure kinds are retried, both self-resolving conditions that
    don't mean the task itself failed:

    * httpx.RequestError -- the request never got a response at all (e.g.
      CAMS's port briefly not accepting connections during a `cams
      reload`).
    * httpx.HTTPStatusError with a 5xx status -- CAMS answered, but with a
      server error (confirmed live 2026-09-11: a burst of near-simultaneous
      polls from a 947-bill backfill hit CAMS's status endpoint with real
      500s 618 times over ~6 minutes; two of the affected bills had
      genuinely finished successfully on CAMS's side, but ddp-sync had
      already given up on the first 500 and never came back to collect
      them).

    A non-5xx HTTPStatusError (e.g. a 404 -- CAMS has no record of this
    task_id) is NOT retried; there's no reason to expect a different answer
    later, and the caller's own existing handling for that should see it
    immediately, same as before this ticket.

    Bounded by legbot_poll_retry_max_attempts short retries at
    legbot_poll_retry_backoff_seconds apart -- exhausting the budget
    re-raises the last error, so a real, sustained outage still falls
    through to _dispatch_and_await's own existing queue-wait/dispatch
    deadline and cancel-on-give-up behavior unchanged, just no longer
    triggered by a single unlucky poll.
    """
    settings = get_settings()
    attempt = 0
    while True:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code < 500:
                raise
            transient_error: Exception = exc
        except httpx.RequestError as exc:
            transient_error = exc

        attempt += 1
        if attempt >= settings.legbot_poll_retry_max_attempts:
            raise _TransientPollFailure(str(transient_error)) from transient_error
        logger.warning(
            "LegBot status poll hit a transient failure -- retrying same "
            "task_id after a short backoff",
            task_id=task_id, attempt=attempt,
            max_attempts=settings.legbot_poll_retry_max_attempts,
            error=str(transient_error),
        )
        await asyncio.sleep(settings.legbot_poll_retry_backoff_seconds)


async def _cancel_task_best_effort(
    client: httpx.AsyncClient, cams_base_url: str, headers: dict, *,
    task_id: str, question_type: str, reason: str,
) -> None:
    """Best-effort DELETE /api/v1/tasks/{task_id} (SYNC-20) -- shared by
    both ways _dispatch_and_await's poll loop can give up on a task: the
    client-side deadline expiring, and (SYNC-60) a transient status-poll
    failure exhausting its own retry budget. Either way, walking away
    without this leaves the CAMS task running as an orphan no one is
    watching, indefinitely occupying a worker (and, for LegBot, the shared
    MLX slot). A cancel-call failure is logged but never masks or replaces
    the real error the caller is about to raise.
    """
    try:
        cancel_resp = await client.delete(
            f"{cams_base_url}/api/v1/tasks/{task_id}", headers=headers
        )
        cancel_resp.raise_for_status()
        logger.warning(
            f"LegBot task give-up ({reason}) -- cancelled on CAMS",
            task_id=task_id, question_type=question_type,
        )
    except httpx.HTTPError as cancel_exc:
        logger.warning(
            f"LegBot task give-up ({reason}) -- cancel request itself "
            "failed, task may still be running orphaned",
            task_id=task_id, question_type=question_type,
            error=str(cancel_exc),
        )


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
    on_task_created: Callable[[str], Awaitable[None]] | None = None,
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
        on_task_created (SYNC-61): awaited with the CAMS task_id immediately
            after task creation succeeds, before the poll loop begins --
            ddp-sync's own process can die mid-poll (confirmed live: a
            947-bill backfill left 193 finished CAMS answers uncollected
            when the poller crashed, not the task), so recording the task_id
            durably has to happen before any polling, not after. None (the
            default) skips this entirely, matching every caller that
            doesn't need crash recovery. Never blocks or is retried here --
            a callback failure would strand a real dispatch over a
            bookkeeping problem, so it propagates uncaught to the caller
            (who owns the callback and decides how much that failure
            should cost).

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
        on_task_created=on_task_created,
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
        See _dispatch_and_await. On a model-produced answer, answer contains
        "verdict" ("confirmed"/"not_confirmed"),
        "insufficient_information", "content_looks_incomplete", and
        "explanation" — no "citation_excerpt"/"page_text" field exists in
        this payload or answer at all.

        Do not read any of those as a bare subscript. When LegBot degrades
        before reaching the model — a backend error, unparseable JSON, an
        unresolved URL — the answer is only
        {"insufficient_information": True, "reason": "..."}: no "verdict",
        no "explanation", and "reason" instead. That shape is normal and
        expected, not a malfunction, and callers must handle it (SYNC-41,
        where a bare answer["verdict"] cost a bill its whole org-research
        pass).

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
    on_task_created: Callable[[str], Awaitable[None]] | None = None,
) -> dict:
    """Shared dispatch/poll/read-result mechanics for any analyze_bill payload.

    on_task_created (SYNC-61): see dispatch_bill_changelog's own docstring --
    None (the default) here too, so every caller not passed through is
    unaffected.

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
        if on_task_created is not None:
            # SYNC-61: recorded before any polling, deliberately -- the whole
            # point is surviving this process dying somewhere in the loop
            # below, so the record has to exist before that loop starts, not
            # after it (successfully or not) finishes.
            await on_task_created(task_id)

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
        # generation genuinely began, not from dispatch.
        #
        # PM review: absence of the key entirely (an older CAMS deployed
        # before this field existed) is deliberately NOT treated the same
        # as the key being present-but-empty (a newer CAMS reporting a real
        # task that simply hasn't started generating yet) -- those are two
        # different situations a mismatched rollout order could otherwise
        # conflate, and only the second one is what queue_wait_timeout_seconds
        # exists to cover. If the key is missing from the response schema
        # entirely, this collapses to *exactly* the old, single flat
        # legbot_dispatch_timeout_seconds-from-dispatch behavior (no
        # generous queue-wait window at all) -- full backward compatibility
        # for a ddp-sync deployed ahead of its paired ddp-agents PR, with no
        # deploy-order requirement between the two repos.
        status = "queued"
        generation_started_seen = False
        legacy_no_marker_field = False
        dispatch_time = time.monotonic()
        deadline = dispatch_time + queue_wait_timeout_seconds
        while time.monotonic() < deadline:
            # SYNC-60: retries a transient connection failure or 5xx against
            # this same task_id -- see _poll_task_status_with_retry's own
            # docstring. A genuine task-level outcome (CAMS reachable,
            # answering 2xx) is completely unaffected by this change.
            try:
                body = await _poll_task_status_with_retry(
                    client,
                    f"{settings.cams_base_url}/api/v1/tasks/{task_id}",
                    headers,
                    task_id=task_id,
                )
            except _TransientPollFailure as exc:
                # Retry budget exhausted -- this is a real, sustained
                # problem (CAMS down for good, or a genuine outage), not a
                # one-off blip. Give up exactly like the client-side
                # deadline below: cancel the orphaned CAMS task and raise
                # LegBotDispatchError, never a raw httpx exception. A non-
                # retryable error (e.g. a 404) is NOT this exception type --
                # it propagates directly out of this function, unwrapped,
                # exactly as before this ticket.
                await _cancel_task_best_effort(
                    client, settings.cams_base_url, headers,
                    task_id=task_id, question_type=question_type,
                    reason="status poll retries exhausted",
                )
                raise LegBotDispatchError(
                    f"LegBot task {task_id} status poll failed after "
                    f"{settings.legbot_poll_retry_max_attempts} attempts: {exc}"
                ) from exc
            status = body["status"]
            if status in TERMINAL_STATUSES:
                break
            if "mlx_generation_started_at" not in body:
                if not legacy_no_marker_field:
                    legacy_no_marker_field = True
                    deadline = dispatch_time + timeout_seconds
                    logger.info(
                        "LegBot task response has no mlx_generation_started_at "
                        "field -- CAMS predates AGENTS-42, falling back to the "
                        "legacy flat dispatch timeout",
                        task_id=task_id, question_type=question_type,
                    )
            elif not generation_started_seen and body.get("mlx_generation_started_at"):
                generation_started_seen = True
                deadline = time.monotonic() + timeout_seconds
                logger.info(
                    "LegBot task MLX generation started -- switching from "
                    "queue-wait to inference timeout",
                    task_id=task_id, question_type=question_type,
                    inference_timeout_seconds=timeout_seconds,
                )
            await asyncio.sleep(settings.legbot_poll_interval_seconds)
        else:
            # SYNC-20: giving up here must not just walk away from the CAMS
            # task -- without this, it keeps running as an orphan no one is
            # watching, indefinitely occupying a worker (and, for LegBot,
            # the shared MLX slot) even though this caller has already
            # decided to treat it as failed. Confirmed live 2026-08-15/16:
            # 4 abandoned tasks sat RUNNING for 45+ minutes and starved a
            # later, correctly-dispatched run's own tasks of worker
            # capacity. Shared with the SYNC-60 retry-exhausted case above
            # -- see _cancel_task_best_effort's own docstring.
            await _cancel_task_best_effort(
                client, settings.cams_base_url, headers,
                task_id=task_id, question_type=question_type,
                reason="timed out client-side",
            )
            if legacy_no_marker_field:
                timeout_desc = (
                    f"legacy dispatch timeout {timeout_seconds}s "
                    f"(CAMS response has no mlx_generation_started_at field)"
                )
            elif generation_started_seen:
                timeout_desc = f"inference timeout {timeout_seconds}s (generation had started)"
            else:
                timeout_desc = f"queue-wait timeout {queue_wait_timeout_seconds}s (generation never started)"
            raise LegBotDispatchError(
                f"LegBot task {task_id} did not finish within its timeout window "
                f"({timeout_desc}) (last status: {status})"
            )

    if status != "completed":
        raise LegBotDispatchError(f"LegBot task {task_id} ended with status={status}")

    result = read_task_result(task_id)
    logger.info(
        "LegBot task completed", task_id=task_id, question_type=question_type,
        insufficient_information=result["answer"].get("insufficient_information"),
    )
    return result


def read_task_result(task_id: str) -> dict:
    """Read a completed CAMS task's result off the shared local disk.

    SYNC-61: factored out of _dispatch_and_await's own tail so the recovery
    path (a stale-dispatch sweep re-checking a task this process never
    polled to completion itself) can read the exact same file the same way,
    without re-deriving this logic. Both processes share CAMS_ARTIFACTS_DIR
    on the same Mac Studio; confirmed the file has no expiry/cleanup, so a
    completed task's result stays readable indefinitely after the fact.

    Deliberately does not check the task's CAMS status itself -- callers
    (both the normal poll loop above and the recovery sweep) already know
    the task is "completed" before calling this; calling it on anything
    else risks reading a stale result left over from a previous attempt at
    the same task_id, or a partially-written file for a still-running one.

    Returns:
        {"answer": ..., "backend": ...} -- same shape _dispatch_and_await
        itself returns on success.

    Raises:
        LegBotDispatchError: the file is missing/unreadable/not valid JSON,
        has no "answer" key, or "answer" is present but not a dict (seen in
        practice: a truncated/corrupted string where CAMS should have
        written a real object) -- every caller downstream (the live poll
        path in _dispatch_and_await below, generate_and_store_bill_
        changelog's own dispatch/recovery calls) treats a returned answer
        as a dict without checking again, so this is the one place that
        needs to guarantee it actually is one.
    """
    settings = get_settings()
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
    if not isinstance(answer, dict):
        raise LegBotDispatchError(
            f"task_result.json for {task_id} has a non-dict 'answer' "
            f"(got {type(answer).__name__}): {answer!r}"
        )
    return {"answer": answer, "backend": snapshot.get("backend")}


async def check_task_status(task_id: str) -> dict:
    """One-shot CAMS task status check -- GET /api/v1/tasks/{task_id}.

    SYNC-61: the recovery sweep's own equivalent of _dispatch_and_await's
    poll loop, minus the loop -- it needs to ask CAMS "what happened to
    this task" exactly once per sweep, not wait around for a terminal
    status the way a live dispatch does.

    Returns:
        The raw JSON body CAMS returns, e.g. {"status": "completed", ...}.
        Not narrowed to any particular shape -- the caller only ever reads
        "status" today, but this is the same response the normal poll loop
        already handles, so no information is thrown away here that a
        future caller might need.

    Raises:
        httpx.HTTPStatusError: CAMS returned a non-2xx response (including
            404 for a task_id it has no record of at all).
        httpx.RequestError: CAMS was unreachable.
    """
    settings = get_settings()
    headers = {"Authorization": f"Bearer {settings.cams_api_token}"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{settings.cams_base_url}/api/v1/tasks/{task_id}", headers=headers
        )
        resp.raise_for_status()
        return resp.json()
