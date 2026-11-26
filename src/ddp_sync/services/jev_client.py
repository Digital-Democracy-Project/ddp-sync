"""TypeSafe's Jev, the matching tool of the policy_statement step (SYNC-183).

Moved, not redesigned, from ddp-agents `scripts/policy_walk_cheap.py` `JevScorer` (AGENTS-256/261, where it was measured):
a decision model that answers a typed "choice" question with a probability for each option, on TypeSafe's servers. Each call
is one choice among the candidates plus "none", so "none" takes probability away from every candidate and a cutoff can
decline. The only change is httpx's async client.

Native REST API (https://docs.typesafe.ai/api): POST https://api.typesafe.ai/v1/systemone, `Authorization: Bearer <key>`,
body {model, state, questions}; a `choice` question's `criteria` maps each option's key to its description (at most 255
with "none"); the answer carries `probabilities` per key. Price: $0.042 per million input tokens, output free
(https://docs.typesafe.ai/models). The key comes from settings and is never logged.
"""

from __future__ import annotations

import asyncio
from typing import Self

import httpx

from ddp_sync.config import get_settings

URL = "https://api.typesafe.ai/v1/systemone"
PRICE_PER_MILLION_INPUT = 0.042
MAX_OPTIONS = 254  # Jev takes at most 255 options with "none"
_QUESTION = (
    "Which option names the policy area or policy family this poll statement belongs to? Judge the "
    "position the statement proposes, not words it shares with an option. Choose \"none\" when no "
    "option fits."
)
_TIMEOUT_SECONDS = 120.0
# SYNC-196: waits before retry 1, 2 and 3 (so 4 attempts in all, as before for a 429). TypeSafe answered 400 "Unknown model:
# jev-latest" for about 12 minutes on 2026-11-25 although the model was listed, so that one 400 counts as transient; no other
# 4xx is retried. These ride out a blip, not an outage: the run's own stop-early rule handles an outage.
_RETRY_DELAYS = (2.0, 8.0, 30.0)
_ATTEMPTS = 1 + len(_RETRY_DELAYS)


def _transient(resp: httpx.Response) -> bool:
    """SYNC-196: a response worth asking again: a server error, or the "Unknown model" 400 TypeSafe gave during its outage."""
    return resp.status_code >= 500 or (resp.status_code == 400 and "Unknown model" in resp.text)


class JevError(Exception):
    """Jev could not answer (no key, unreachable, an error status or an unusable body). Never turned into a score."""


class JevScorer:
    """One scorer per bill, used as `async with JevScorer() as jev`. `input_tokens` adds up what the calls cost."""

    def __init__(self, *, api_key: str | None = None, model_id: str | None = None):
        key = api_key if api_key is not None else get_settings().typesafe_api_key
        if not key:
            raise JevError("TYPESAFE_API_KEY is not set")
        self._model = model_id or get_settings().typesafe_jev_model
        self.name = f"typesafe:{self._model}"
        self.input_tokens = 0
        self._client = httpx.AsyncClient(timeout=_TIMEOUT_SECONDS, headers={"Authorization": f"Bearer {key}"})

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self._client.aclose()

    @property
    def cost_usd(self) -> float:
        return self.input_tokens * PRICE_PER_MILLION_INPUT / 1e6

    async def score(self, text: str, candidates: list[str]) -> list[float]:
        """The probability Jev gives each candidate (same order) for `text`."""
        if len(candidates) > MAX_OPTIONS:
            raise ValueError(f"{len(candidates)} options: Jev takes at most 255 with \"none\"")
        criteria = {f"o{i}": c for i, c in enumerate(candidates)}
        criteria["none"] = "None of the other options fits the statement."
        body = {
            "model": self._model,
            "state": text,
            "questions": {"place": {"type": "choice", "instructions": _QUESTION, "criteria": criteria}},
        }
        for attempt in range(_ATTEMPTS):
            last = attempt == _ATTEMPTS - 1
            try:
                resp = await self._client.post(URL, json=body)
            except httpx.RequestError as exc:
                if last:
                    raise JevError(f"Jev unreachable: {exc}") from exc
                await asyncio.sleep(_RETRY_DELAYS[attempt])
                continue
            if resp.status_code == 429 and not last:  # rate limit: wait as told (the SDKs do the same)
                await asyncio.sleep(float(resp.headers.get("retry-after") or 2**attempt))
                continue
            if not last and _transient(resp):
                await asyncio.sleep(_RETRY_DELAYS[attempt])
                continue
            break
        if resp.status_code >= 400:
            raise JevError(f"Jev answered {resp.status_code}: {resp.text[:200]}")
        try:
            data = resp.json()
            probs = data["answers"]["place"]["probabilities"]
            self.input_tokens += (data.get("usage") or {}).get("input_tokens", 0)
            return [float(probs.get(f"o{i}", 0.0)) for i in range(len(candidates))]
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise JevError(f"Jev returned an unusable answer: {resp.text[:200]}") from exc
