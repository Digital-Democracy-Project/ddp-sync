"""Shared rate-limiter for sleep-based throttling of API clients.

One shared module for all pipelines that talk to OpenStates / Webflow / Congress.gov.
Two instances against the same upstream do NOT coordinate — share one instance per
(process, upstream) pair to enforce a global budget.

Loads its config from the `rate_limit:` and `retry:` blocks of `sync_schedule.yaml`.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

import structlog
import yaml

logger = structlog.get_logger()


@dataclass
class RateLimitConfig:
    """Rate-limit configuration.

    Defaults align with the OpenStates 30,000 calls/day tier (2 calls/sec).
    """

    requests_per_minute: int = 120
    delay_between_requests_ms: int = 500
    max_retry_attempts: int = 3
    retry_backoff_seconds: int = 5

    @classmethod
    def from_yaml(cls, config_path: Path) -> "RateLimitConfig":
        """Load from a sync_schedule.yaml. **Never raises** — returns defaults
        on any failure (missing file, malformed YAML, missing keys).

        Reads `rate_limit.requests_per_minute` and either
        `rate_limit.delay_between_requests_ms` (preferred) or
        `rate_limit.delay_between_bills_ms` (legacy, accepted for backwards compat).
        Reads `retry.max_attempts` and `retry.backoff_seconds`.

        This contract matches the prior inline ``_load_rate_limit_config()``
        methods in legislator_sync.py and bill_sync.py, both of which logged
        a warning and returned ``RateLimitConfig()`` on any failure rather than
        propagating the exception. Existing pipelines depend on this — do not
        change to raise without auditing all callers.
        """
        if not config_path.exists():
            logger.warning(
                "Rate-limit config not found, using defaults",
                config_path=str(config_path),
            )
            return cls()
        try:
            with open(config_path) as f:
                data = yaml.safe_load(f) or {}
            rate_limit = data.get("rate_limit", {})
            retry = data.get("retry", {})
            return cls(
                requests_per_minute=rate_limit.get("requests_per_minute", 120),
                delay_between_requests_ms=rate_limit.get(
                    "delay_between_requests_ms",
                    rate_limit.get("delay_between_bills_ms", 500),
                ),
                max_retry_attempts=retry.get("max_attempts", 3),
                retry_backoff_seconds=retry.get("backoff_seconds", 5),
            )
        except Exception as e:
            logger.error("Failed to load rate-limit config", error=str(e))
            return cls()


class RateLimiter:
    """Sleep-based per-process rate limiter.

    Enforces both `requests_per_minute` (a steady-rate cap) and
    `delay_between_requests_ms` (a minimum inter-call gap). The effective
    delay is `max(per_request_delay, configured_min_delay)`.

    Concurrency-safe within a single asyncio event loop: an internal
    ``asyncio.Lock`` serializes calls to ``apply()`` so concurrent tasks
    (e.g. ``asyncio.gather()``) cannot bypass the gap by reading the
    timestamp before another task updates it.

    NOT cross-process aware — multi-worker deployments need a Redis-backed
    coordinator. See PLAN-legislator-bio-sync.md "Single-worker assumption".
    """

    def __init__(self, config: RateLimitConfig | None = None):
        self.config = config or RateLimitConfig()
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()
        self._enforced_sleeps: int = 0  # observability counter

    async def apply(self) -> None:
        """Sleep if needed to enforce the rate limit before the next call.

        Holds an asyncio lock for the entire compute-and-sleep window so
        parallel tasks observe the timestamp serialized.
        """
        async with self._lock:
            min_delay_seconds = self.config.delay_between_requests_ms / 1000.0
            per_request_delay = 60.0 / self.config.requests_per_minute
            min_delay_seconds = max(min_delay_seconds, per_request_delay)

            current_time = time.time()
            elapsed = current_time - self._last_request_time

            if elapsed < min_delay_seconds and self._last_request_time > 0:
                sleep_time = min_delay_seconds - elapsed
                logger.debug("Rate limiting", sleep_seconds=round(sleep_time, 2))
                self._enforced_sleeps += 1
                await asyncio.sleep(sleep_time)

            self._last_request_time = time.time()

    @property
    def enforced_sleeps(self) -> int:
        """Number of times apply() actually slept. Useful for runtime
        observability — confirms the limiter is engaging under load."""
        return self._enforced_sleeps
