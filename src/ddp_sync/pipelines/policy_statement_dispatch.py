"""The policy_statement step of the LegBot session pipeline (SYNC-183; ddp-infra PLAN-bill-concept-polling.md section
5.4.11). Modelled on `concept_statement_dispatch.py`: one per-bill unit that `session_pipeline_runner._process_bill_inner`
calls right after concept_statements when `include_policy_statement` is set.

Per bill:

1. **Work.** The broker's own list (`GET /api/policy-matching/work/?jurisdiction=&gov_id=&session=`, BROKER-573 / BROKER-598) says which statements of the bill's
   current published set have no current match row. Nothing here reads sets or keeps a second record of what was done, so a
   statement with a current row is never offered again and a re-run cannot duplicate a row.
2. **Writer.** LegBot's `policy_statement` question (AGENTS-262) returns one draft Policy per statement. It sees no Policy list.
3. **Match.** The broker's CURRENT Policy list (read again for each bill, so a Policy created for the previous bill is a
   candidate) is compared with each draft by Jev (`services/jev_client.py`), 200 Policies per call and then the chunk winners
   against each other (moved from ddp-agents `scripts/policy_walk_whole_list.py`, variant `exclusions`). A best score at or over
   `policy_statement_match_cutoff` is a match; a draft the writer flagged `not_a_position` is excluded.
4. **Concept.** Under the cutoff the draft becomes a new Policy, and Jev picks its Concept the same way over the broker's
   Concept list (`label: definition` plus the exclusions). That pick has not been tested for accuracy; its score is logged
   and stored with the result so SYNC-184's rounds can measure it.
5. **Write.** `POST /api/policy-matching/drafts/` (BROKER-598). The broker creates a new Policy cleared, so the existing
   3-bill rule is the only thing that keeps it off the site.

6. **Judge (SYNC-199; PLAN 5.4.12; off with `POLICY_STATEMENT_JUDGE_SHORTLIST=0`).** A draft that is not a match but has Policies
   scoring at least `policy_statement_judge_floor` (top `policy_statement_judge_shortlist`, taken from the SAME Jev calls) goes
   to LegBot's `resolve_policy`: "existing" files the draft under that Policy (`matched`), "draft" creates it as above and
   tells the broker it `replaces` the chosen one, null creates it as above. A new Policy is added to the bill's in-memory list,
   so the bill's later drafts are judged against it.

Not here, on purpose (Ramon, 2026-11-24, amended 2026-11-26): no duplicate backstop (no `policy:` documents, no embeddings), no
merge (the broker merges, BROKER-610), no shadow mode, no second lock, queue, gate or run record. A failure raises; the caller
counts it per bill.
"""

from __future__ import annotations

import asyncio
import time

import structlog

from ddp_sync.config import get_settings
from ddp_sync.pipelines.bill_artifact_generation import _resolve_bill_source
from ddp_sync.services.broker_client import (
    BrokerClientError,
    list_policy_matching_concepts,
    list_policy_matching_policies,
    list_policy_statement_work,
    write_policy_statement_match,
)
from ddp_sync.services.jev_client import JevScorer
from ddp_sync.services.legbot_client import LegBotDispatchError, dispatch_bill_question, dispatch_resolve_policy

logger = structlog.get_logger()

_QUESTION_TYPE = "policy_statement"
RUBRIC_VERSION = "policy_statement:v1"
CHUNK_SIZE = 200  # Policies per Jev call (AGENTS-261)
# What the broker's draft write keeps (BROKER-598 `_statement` cuts prompt_hash at 64 without saying so, and rejects a Policy
# label over 200 with a 400). The writer's judge_id is stamped as prompt_hash and its LAST characters are the digest that
# tells one prompt from another, so a cut would make a changed prompt look unchanged: fail before writing anything instead.
BROKER_PROMPT_HASH_MAX = 64
BROKER_LABEL_MAX = 200
WRITER_BATCH_MAX = 40  # LegBot's _DRAFT_BATCH_MAX: statements per writer call (a bill has far fewer)


def draft_query(draft: dict) -> str:
    """What Jev compares with each Policy: `label: statement definition`, plus ` (subject: <subject>)` (AGENTS-261's
    `exclusions` variant, which used the draft's subject only)."""
    text = f"{draft['label']}: {draft['statement']} {draft.get('definition', '')}".strip()
    subject = draft.get("subject")
    return f"{text} (subject: {subject})" if isinstance(subject, str) and subject else text


def policy_text(policy: dict) -> str:
    """A Policy as Jev reads it: `label: canonical_statement: definition`, plus ` Does not cover: <exclusions>`."""
    text = ": ".join(policy[k] for k in ("label", "canonical_statement", "definition") if policy.get(k))
    return f"{text} Does not cover: {policy['exclusions']}" if policy.get("exclusions") else text


def concept_text(concept: dict) -> str:
    """A Concept as Jev reads it: `label: definition`, plus ` Does not cover: <exclusions>`."""
    text = ": ".join(concept[k] for k in ("label", "definition") if concept.get(k))
    return f"{text} Does not cover: {concept['exclusions']}" if concept.get("exclusions") else text


async def rank_items(jev: JevScorer, query: str, items: list[dict], text_of) -> tuple[int | None, float, list[tuple[float, int]]]:
    """`best_of`'s answer plus every item's chunk score, best first (ties to the larger id), taken from the same Jev calls:
    the shortlist for the judge (SYNC-199) costs no extra call. A score is the item's probability within its own chunk."""
    if not items:
        return None, 0.0, []
    chunks = [items[i : i + CHUNK_SIZE] for i in range(0, len(items), CHUNK_SIZE)]
    scores = await asyncio.gather(*(jev.score(query, [text_of(x) for x in chunk]) for chunk in chunks))
    ranked = sorted(
        ((s, x["id"]) for sc, chunk in zip(scores, chunks) for s, x in zip(sc, chunk)), reverse=True
    )
    winners = [max(zip(sc, [x["id"] for x in chunk])) for sc, chunk in zip(scores, chunks)]
    if len(winners) == 1:
        score, ident = winners[0]
        return ident, score, ranked
    by_id = {x["id"]: x for x in items}
    final = await jev.score(query, [text_of(by_id[ident]) for _, ident in winners])
    score, ident = max(zip(final, [ident for _, ident in winners]))
    # Chunk scores are not comparable across chunks, so the cross-chunk winner is always first; the rest follow by chunk score.
    return ident, score, [(score, ident)] + [r for r in ranked if r[1] != ident]


async def best_of(jev: JevScorer, query: str, items: list[dict], text_of) -> tuple[int | None, float]:
    """The item Jev likes best for `query` and its score: every `CHUNK_SIZE` items in one call, then the chunk winners
    against each other. One chunk is answered by that one call (a second call would compare a lone winner with "none").
    Ties go to the larger id, as the measured script's `max(zip(scores, ids))` did. An empty list is (None, 0.0)."""
    best_id, score, _ = await rank_items(jev, query, items, text_of)
    return best_id, score


async def match_draft_ranked(
    jev: JevScorer, draft: dict, policies: list[dict], cutoff: float
) -> tuple[str, int | None, float, list[tuple[float, int]]]:
    """`match_draft` plus the Policies' scores, best first (empty for an excluded draft)."""
    if draft["not_a_position"]:
        return "excluded", None, 0.0, []
    best_id, score, ranked = await rank_items(jev, draft_query(draft), policies, policy_text)
    if best_id is not None and score >= cutoff:
        return "matched", best_id, score, ranked
    return "created", best_id, score, ranked


async def match_draft(jev: JevScorer, draft: dict, policies: list[dict], cutoff: float) -> tuple[str, int | None, float]:
    """The Policy decision for one draft: ("excluded", None, 0.0) for a draft that is not a position, ("matched", id, score)
    when the best Policy scores at or over `cutoff`, else ("created", best id, score), meaning a new Policy (its Concept is
    picked after this)."""
    resolution, best_id, score, _ = await match_draft_ranked(jev, draft, policies, cutoff)
    return resolution, best_id, score


def _judge_candidates(ranked: list[tuple[float, int]], policies: list[dict], shortlist: int, floor: float) -> list[dict]:
    """The judge's shortlist: the top `shortlist` Policies scoring at least `floor`, in Jev's order (SYNC-199)."""
    by_id = {p["id"]: p for p in policies}
    return [
        {"id": ident, **{k: by_id[ident].get(k) or "" for k in ("label", "canonical_statement", "definition", "exclusions")}}
        for score, ident in ranked[:shortlist]
        if score >= floor
    ]


def _judged_answer(reply: dict, candidates: list[dict]) -> dict:
    """The judge's answer as the broker stores it. An id it was not shown, or `existing`/`draft` missing or unknown with a
    choice, fails the bill (LegBotDispatchError) rather than being read as "none"."""
    answer = reply.get("answer") if isinstance(reply, dict) else None
    if not isinstance(answer, dict):
        raise LegBotDispatchError(f"resolve_policy returned no answer object: {reply!r}"[:300])
    choice, survivor = answer.get("choice"), answer.get("survivor")
    shown = [c["id"] for c in candidates]
    if choice is not None and (isinstance(choice, bool) or choice not in shown or survivor not in ("existing", "draft")):
        raise LegBotDispatchError(f"resolve_policy answered choice={choice!r} survivor={survivor!r} for candidates {shown}")
    return {
        "choice": choice, "survivor": survivor if choice is not None else None, "reason": answer.get("reason") or "",
        "judge_id": answer.get("judge_id") or "", "prompt_version": answer.get("prompt_version") or "",
        "prompt_hash": answer.get("prompt_hash") or "", "model_name": answer.get("model_name") or "",
        "unusable": bool(answer.get("unusable")), "candidates": [c["id"] for c in candidates],
    }


def _created_policy_id(response: dict | None) -> int | None:
    """The new Policy's id from a `created` write's answer: `created_policy_id` (BROKER-610) or the BROKER-598 shape's
    `created.policy_id`; None when neither is there (the in-bill list is then left as it was)."""
    response = response or {}
    value = response.get("created_policy_id")
    if value is None and isinstance(response.get("created"), dict):
        value = response["created"].get("policy_id")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


async def _write_drafts(
    *,
    work: list[dict],
    drafts: dict[int, dict],
    answer: dict,
    jev: JevScorer,
    policies: list[dict],
    concepts_loader,
    cutoff: float,
    broker_api_base: str | None,
    broker_api_token: str | None,
    counts: dict,
    judge_shortlist: int = 0,
    judge_floor: float = 0.30,
) -> None:
    for position, item in enumerate(work, start=1):
        draft = drafts[position]
        scores = {"policy": 0.0, "best_policy_id": None, "concept": None, "cutoff": cutoff, "list_size": len(policies)}
        payload = {
            "concept_statement_set_id": item["concept_statement_set_id"],
            "statement_index": item["statement_index"],
            "statement_text": item["statement_text"],
            # The broker's prompt_hash holds only the writer's judge_id (no model name), so the writer's own model and
            # prompt version ride inside the stored draft JSON (the broker keeps any keys of `draft` as they come).
            "draft": {**draft, "writer": {"model_name": answer.get("model_name") or "", "prompt_version": answer.get("prompt_version") or ""}},
            "scores": scores,
            "model_name": jev.name,
            "prompt_version": answer.get("prompt_version") or "",
            "prompt_hash": answer["judge_id"],
            "rubric_version": RUBRIC_VERSION,
        }
        resolution, best_id, best_score, ranked = await match_draft_ranked(jev, draft, policies, cutoff)
        judged = None
        if resolution == "created" and judge_shortlist > 0:
            shown = _judge_candidates(ranked, policies, judge_shortlist, judge_floor)
            if shown:
                # A failed call raises LegBotDispatchError: nothing is written for this draft, the bill's step fails (source
                # "legbot" for the run's breaker) and the draft is offered again. It is never read as "the judge said none".
                reply = await dispatch_resolve_policy(
                    {k: draft.get(k) or "" for k in ("label", "statement", "definition", "exclusions")}, shown
                )
                judged = _judged_answer(reply, shown)
                counts["judged"] += 1
                if judged["choice"] is None:
                    counts["judge_none"] += 1
        scores.update(policy=best_score, best_policy_id=best_id)
        if judged and judged["survivor"] == "existing":
            counts["judge_filed"] += 1
            resolution, best_id = "matched", judged["choice"]  # filed under the judge's pick; scores keep Jev's own best
        if resolution == "excluded":
            payload.update(resolution="excluded", policy_id=None, created=None, reason="not a position")
        elif resolution == "matched":
            payload.update(resolution="matched", policy_id=best_id, created=None, reason="")
        else:
            concepts = await concepts_loader()
            concept_id, concept_score = await best_of(jev, draft_query(draft), concepts, concept_text)
            if concept_id is None:
                raise BrokerClientError("the broker's Concept list is empty: a new Policy has no Concept to go under")
            scores["concept"] = concept_score
            payload.update(
                resolution="created",
                policy_id=None,
                created={
                    "label": draft["label"],
                    "canonical_statement": draft["statement"],
                    "definition": draft["definition"],
                    "exclusions": draft.get("exclusions", ""),
                    "concept_id": concept_id,
                },
                reason="",
            )
        if judged:
            payload["judge"] = judged
            if judged["survivor"] == "draft":
                payload["replaces"] = judged["choice"]
                counts["judge_replaced"] += 1
        outcome = payload["resolution"]
        logger.info(
            "policy_statement_decided",
            outcome=outcome,
            set_id=item["concept_statement_set_id"],
            statement_index=item["statement_index"],
            best_policy_id=scores["best_policy_id"],
            policy_score=round(scores["policy"], 4),
            concept_id=(payload["created"] or {}).get("concept_id"),
            concept_score=scores["concept"],
            cutoff=cutoff,
            list_size=len(policies),
            **({"judge_choice": judged["choice"], "judge_survivor": judged["survivor"], "judge_reason": judged["reason"],
                "judge_id": judged["judge_id"], "judge_prompt_version": judged["prompt_version"],
                "judge_unusable": judged["unusable"], "judge_candidates": judged["candidates"]} if judged else {}),
        )
        response = await write_policy_statement_match(payload, broker_api_base=broker_api_base, broker_api_token=broker_api_token)
        counts[outcome] += 1
        if outcome == "created" and judge_shortlist > 0:
            # The bill's later drafts are matched and judged against this Policy too (two near-identical drafts in one bill
            # must not both create one). Only with the judge on, so the judge-off path is exactly as before.
            new_id = _created_policy_id(response)
            if new_id is None:
                logger.warning("policy_statement_created_id_missing", set_id=item["concept_statement_set_id"],
                               statement_index=item["statement_index"])
            else:
                created = payload["created"]
                if "replaces" in payload:
                    policies[:] = [p for p in policies if p["id"] != payload["replaces"]]
                policies.append({"id": new_id, **{k: created.get(k) or "" for k in ("label", "canonical_statement", "definition", "exclusions")},
                                 "concept_id": created["concept_id"]})


async def dispatch_and_store_policy_statements(
    *,
    gov_id: str,
    bill_openstates_id: str,
    jurisdiction_iso2: str,
    session_code: str,
    dry_run: bool = False,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Run the policy_statement step for one bill.

    Returns {"statements": <int that needed work>, "matched": n, "created": n, "excluded": n, "skipped_reason": str | None},
    plus "judged", "judge_filed", "judge_replaced", "judge_none" when the judge is on (SYNC-199).
    `skipped_reason` is "nothing_to_do" (every statement has a current row, or the bill has no published set),
    "no_archived_text" or "insufficient_information" (LegBot could not draft: nothing is written). A `dry_run` reads the work
    list and reports its size; it calls no LegBot, no Jev and writes nothing.

    Raises:
        BrokerClientError / LegBotDispatchError / JevError: propagate unchanged; the caller counts the bill as failed.
        Statements already written stay written; the rest have no row and are offered again by the next run.
    """
    settings = get_settings()
    cutoff = settings.policy_statement_match_cutoff
    judge_shortlist = settings.policy_statement_judge_shortlist
    broker = {"broker_api_base": broker_api_base, "broker_api_token": broker_api_token}
    counts = {"matched": 0, "created": 0, "excluded": 0}
    if judge_shortlist > 0:  # SYNC-199: the judge's counts exist only when it is on, so the judge-off result is unchanged
        counts.update(judged=0, judge_filed=0, judge_replaced=0, judge_none=0)

    work = await list_policy_statement_work(
        gov_id=gov_id, jurisdiction_iso2=jurisdiction_iso2, session_code=session_code, rubric_version=RUBRIC_VERSION,
        **broker
    )
    if any(w.get("gov_id") != gov_id or w.get("session") != session_code for w in work):
        raise BrokerClientError("the broker's work list holds another bill's statements: it did not honour gov_id= and session=")
    work.sort(key=lambda w: (w["concept_statement_set_id"], w["statement_index"]))
    result = {"statements": len(work), **counts, "skipped_reason": None}
    if not work:
        result["skipped_reason"] = "nothing_to_do"
        return result
    if dry_run:
        return result

    # Built first, so a missing key fails the bill before LegBot spends any time on it.
    async with JevScorer() as jev:
        bill_source = await _resolve_bill_source(bill_openstates_id)
        if bill_source is None:
            logger.info("No archived bill text -- skipping policy_statement, no dispatch", gov_id=gov_id)
            result["skipped_reason"] = "no_archived_text"
            return result

        started = time.monotonic()
        # The writer gets the statements in order (index 0 is n 1), WRITER_BATCH_MAX at a time.
        drafts: dict[int, dict] = {}
        answers: list[dict] = []
        for start in range(0, len(work), WRITER_BATCH_MAX):
            batch = work[start : start + WRITER_BATCH_MAX]
            dispatched = await dispatch_bill_question(
                bill_source, _QUESTION_TYPE, statements=[w["statement_text"] for w in batch]
            )
            answer = dispatched["answer"]
            if answer.get("insufficient_information"):
                logger.info("LegBot reported insufficient_information for policy_statement -- nothing written", gov_id=gov_id)
                result["skipped_reason"] = "insufficient_information"
                return result
            by_n = {d["n"]: d for d in answer["drafts"]}
            if sorted(by_n) != list(range(1, len(batch) + 1)):
                raise LegBotDispatchError(f"policy_statement answered drafts {sorted(by_n)} for {len(batch)} statements")
            drafts.update({start + n: d for n, d in by_n.items()})
            answers.append(answer)
        if len({a["judge_id"] for a in answers}) != 1:
            raise LegBotDispatchError("policy_statement batches were judged under different judge ids")

        judge_id = answers[0]["judge_id"]
        if len(judge_id) > BROKER_PROMPT_HASH_MAX:
            raise LegBotDispatchError(
                f"the writer's judge_id is {len(judge_id)} characters and the broker keeps {BROKER_PROMPT_HASH_MAX} "
                f"(prompt_hash): writing it would cut off its digest. Nothing was written."
            )
        too_long = [n for n, d in drafts.items() if not d["not_a_position"] and len(d["label"].strip()) > BROKER_LABEL_MAX]
        if too_long:
            raise LegBotDispatchError(
                f"the writer's label for statement(s) {too_long} is over {BROKER_LABEL_MAX} characters, which the broker "
                f"refuses. Nothing was written."
            )

        policies = await list_policy_matching_policies(**broker)
        concepts: list[dict] | None = None

        async def concepts_loader() -> list[dict]:
            nonlocal concepts
            if concepts is None:
                concepts = await list_policy_matching_concepts(**broker)
            return concepts

        try:
            await _write_drafts(
                work=work, drafts=drafts, answer=answers[0], jev=jev, policies=policies, concepts_loader=concepts_loader,
                cutoff=cutoff, broker_api_base=broker_api_base, broker_api_token=broker_api_token, counts=counts,
                judge_shortlist=judge_shortlist, judge_floor=settings.policy_statement_judge_floor,
            )
        finally:
            logger.info(
                "policy_statement_bill_done", gov_id=gov_id, jev_input_tokens=jev.input_tokens,
                jev_cost_usd=round(jev.cost_usd, 5), seconds=round(time.monotonic() - started, 1), **counts,
            )
        result.update(counts)
        return result
