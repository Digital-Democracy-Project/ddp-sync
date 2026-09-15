"""Unit tests for OPEN-291's scrape-completion-triggers-archive hook.

OPEN-140 made scrape cadence dynamic (weekly -> nightly) but archive scheduling stayed
a completely separate, static per-jurisdiction weekday map with no connection to it --
so a jurisdiction escalated to nightly scraping kept its archive job on its old fixed
day, drifting out of sync. Rather than teaching the archive schedule to understand
"nightly" and building a separate completion-sequencing mechanism (both real,
per-jurisdiction-shaped fixes that would need to stay hand-synced with whatever
dynamic_cadence decides forever), this triggers a jurisdiction's archive job directly
from its own scrape completion -- one hook, inside `_run_scrape()`, the single function
every scrape path (FL, WA, USA, the whole secondary batch) already funnels through.
A jurisdiction opts in purely by being listed in both its own scrape config and
`openstates_archive.jurisdictions` -- no per-jurisdiction code, ever.

Covers:
  * the enabled gate -- archive config disabled means zero calls
  * a jurisdiction not in openstates_archive.jurisdictions is skipped (this is what
    covers Alabama: archived, but not scraped anywhere in ddp-sync, so it never
    reaches this function to begin with -- but the same gate would apply to any
    scraped jurisdiction not opted into archiving)
  * the usa -> us jurisdiction-code mapping
  * a config-load failure is swallowed, never propagates
  * a run_single_archive_job exception is swallowed, never propagates
  * `_run_scrape`: a successful scrape invokes the hook with the scrape's own
    jurisdiction code; a failed scrape does not
  * a hook-body exception can never propagate out of `_run_scrape` and replace an
    already-successful scrape job's own result
  * the debounce inside `_run_archive_with_hook`: a second archive attempt for the
    same jurisdiction within the window is skipped, not run a second time -- this is
    what makes FL/USA's multi-session scrape loop (which would otherwise trigger this
    hook once per session) safe, and what protects against the still-registered
    weekly cron landing close to a hook-triggered run
  * the debounce fails OPEN when Redis is unavailable, never blocks a real archive
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_archive import (
    _acquire_archive_debounce,
    _archive_jurisdiction_code,
    _run_archive_with_hook,
    maybe_trigger_archive_after_scrape,
)
from ddp_sync.pipelines.openstates_scrape import _run_scrape


ARCHIVE_CFG = {"enabled": True, "jurisdictions": ["mi", "us"]}


# ── _archive_jurisdiction_code ──────────────────────────────────────────────────────


def test_usa_maps_to_us():
    assert _archive_jurisdiction_code("usa") == "us"


@pytest.mark.parametrize("jurisdiction", ["mi", "va", "ma", "ut", "az", "nc", "fl", "wa"])
def test_every_other_jurisdiction_is_unchanged(jurisdiction):
    assert _archive_jurisdiction_code(jurisdiction) == jurisdiction


# ── maybe_trigger_archive_after_scrape ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_disabled_archive_config_triggers_nothing():
    with patch(
        "ddp_sync.pipelines.openstates_archive._load_archive_config_for_scrape_trigger",
        return_value={"enabled": False, "jurisdictions": ["mi"]},
    ), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock()
    ) as mock_archive:
        await maybe_trigger_archive_after_scrape("mi")

    mock_archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_jurisdiction_not_in_archive_list_triggers_nothing():
    """Covers Alabama's real shape: archived elsewhere on its own schedule, but not
    scraped by ddp-sync at all -- this is the same gate that would apply to any
    scraped-but-not-archived jurisdiction, Alabama just never reaches this function
    to begin with since it's absent from every scrape config too."""
    with patch(
        "ddp_sync.pipelines.openstates_archive._load_archive_config_for_scrape_trigger",
        return_value={"enabled": True, "jurisdictions": ["mi", "us"]},
    ), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock()
    ) as mock_archive:
        await maybe_trigger_archive_after_scrape("al")

    mock_archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_jurisdiction_in_list_triggers_the_archive_job():
    with patch(
        "ddp_sync.pipelines.openstates_archive._load_archive_config_for_scrape_trigger",
        return_value=ARCHIVE_CFG,
    ), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job",
        new=AsyncMock(return_value={"success": True}),
    ) as mock_archive:
        await maybe_trigger_archive_after_scrape("mi")

    mock_archive.assert_awaited_once_with("mi", ARCHIVE_CFG)


@pytest.mark.asyncio
async def test_usa_scrape_triggers_the_us_archive_job():
    """The one real naming wrinkle: scrape's federal jurisdiction is "usa", archive's
    is "us" -- the trigger must use the archive-side name, not the scrape-side one."""
    with patch(
        "ddp_sync.pipelines.openstates_archive._load_archive_config_for_scrape_trigger",
        return_value=ARCHIVE_CFG,
    ), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job",
        new=AsyncMock(return_value={"success": True}),
    ) as mock_archive:
        await maybe_trigger_archive_after_scrape("usa")

    mock_archive.assert_awaited_once_with("us", ARCHIVE_CFG)


@pytest.mark.asyncio
async def test_config_load_failure_triggers_nothing_and_does_not_raise():
    with patch(
        "ddp_sync.pipelines.openstates_archive._load_archive_config_for_scrape_trigger",
        return_value={},  # the loader's own documented failure return
    ), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job", new=AsyncMock()
    ) as mock_archive:
        await maybe_trigger_archive_after_scrape("mi")  # must not raise

    mock_archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_archive_job_exception_is_swallowed():
    with patch(
        "ddp_sync.pipelines.openstates_archive._load_archive_config_for_scrape_trigger",
        return_value=ARCHIVE_CFG,
    ), patch(
        "ddp_sync.pipelines.openstates_archive.run_single_archive_job",
        new=AsyncMock(side_effect=RuntimeError("fargate blew up")),
    ):
        await maybe_trigger_archive_after_scrape("mi")  # must not raise


# ── _run_scrape: the actual hook call site ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_successful_scrape_invokes_the_hook_with_its_own_jurisdiction():
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        new=AsyncMock(return_value={"success": True, "jurisdiction": "mi"}),
    ), patch(
        "ddp_sync.pipelines.openstates_archive.maybe_trigger_archive_after_scrape",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_scrape("mi", None, "/fake/root")

    assert result["success"] is True
    mock_hook.assert_awaited_once_with("mi")


@pytest.mark.asyncio
async def test_failed_scrape_skips_the_hook():
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        new=AsyncMock(return_value={"success": False, "jurisdiction": "mi", "error": "boom"}),
    ), patch(
        "ddp_sync.pipelines.openstates_archive.maybe_trigger_archive_after_scrape",
        new=AsyncMock(),
    ) as mock_hook:
        result = await _run_scrape("mi", None, "/fake/root")

    assert result["success"] is False
    mock_hook.assert_not_awaited()


@pytest.mark.asyncio
async def test_hook_exception_never_replaces_a_successful_scrape_result():
    with patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        new=AsyncMock(return_value={"success": True, "jurisdiction": "mi"}),
    ), patch(
        "ddp_sync.pipelines.openstates_archive.maybe_trigger_archive_after_scrape",
        new=AsyncMock(side_effect=RuntimeError("archive trigger blew up")),
    ):
        result = await _run_scrape("mi", None, "/fake/root")  # must not raise

    assert result["success"] is True


# ── debounce ─────────────────────────────────────────────────────────────────────────


class _FakeRedisClient:
    """Mirrors the real redis.asyncio client's `SET key val NX EX ttl` contract just
    enough for these tests: the first caller for a given key gets True (acquired);
    every later caller for the same key gets None (already claimed) until it expires --
    TTL expiry itself isn't simulated, these tests only need "claimed vs not"."""

    def __init__(self):
        self._claimed: set[str] = set()

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self._claimed:
            return None
        self._claimed.add(key)
        return True


def _patch_redis(client):
    return patch(
        "ddp_sync.services.redis_store.get_redis_store",
        return_value=type("_Store", (), {"_client": client})(),
    )


@pytest.mark.asyncio
async def test_first_archive_attempt_for_a_jurisdiction_acquires_the_debounce():
    with _patch_redis(_FakeRedisClient()):
        assert await _acquire_archive_debounce("mi") is True


@pytest.mark.asyncio
async def test_second_archive_attempt_within_the_window_is_skipped():
    client = _FakeRedisClient()
    with _patch_redis(client):
        assert await _acquire_archive_debounce("mi") is True
        assert await _acquire_archive_debounce("mi") is False


@pytest.mark.asyncio
async def test_debounce_is_per_jurisdiction_not_global():
    client = _FakeRedisClient()
    with _patch_redis(client):
        assert await _acquire_archive_debounce("mi") is True
        assert await _acquire_archive_debounce("us") is True


@pytest.mark.asyncio
async def test_debounce_fails_open_when_redis_unavailable():
    """No live Redis connection (the real state in a bare unit-test process, and a
    real possibility in production) must never block a real archive -- this feature
    didn't exist before OPEN-291, and archiving must never become LESS reliable
    than it was without it."""
    with _patch_redis(type("_Store", (), {"_client": None})()):
        assert await _acquire_archive_debounce("mi") is True


@pytest.mark.asyncio
async def test_debounce_fails_open_when_the_redis_call_itself_raises():
    class _BrokenClient:
        async def set(self, *a, **k):
            raise RuntimeError("connection reset")

    with _patch_redis(_BrokenClient()):
        assert await _acquire_archive_debounce("mi") is True


@pytest.mark.asyncio
async def test_run_archive_with_hook_skips_a_debounced_duplicate():
    """The actual integration point: a second _run_archive_with_hook call for the
    same jurisdiction inside the debounce window is skipped cleanly -- success,
    not an error, and the underlying archive branches are never invoked a second
    time."""
    client = _FakeRedisClient()
    with _patch_redis(client), patch(
        "ddp_sync.pipelines.openstates_archive._run_archive",
        new=AsyncMock(return_value={"success": True, "jurisdiction": "mi"}),
    ) as mock_run_archive, patch(
        "ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive",
        new=AsyncMock(),
    ):
        first = await _run_archive_with_hook("mi", "/fake/root", config={})
        second = await _run_archive_with_hook("mi", "/fake/root", config={})

    assert first["success"] is True
    assert first.get("skipped") is None
    assert second == {"success": True, "jurisdiction": "mi", "skipped": "debounced"}
    mock_run_archive.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_archive_with_hook_is_not_debounced_across_different_jurisdictions():
    client = _FakeRedisClient()
    with _patch_redis(client), patch(
        "ddp_sync.pipelines.openstates_archive._run_archive",
        new=AsyncMock(return_value={"success": True}),
    ) as mock_run_archive, patch(
        "ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive",
        new=AsyncMock(),
    ):
        await _run_archive_with_hook("mi", "/fake/root", config={})
        await _run_archive_with_hook("us", "/fake/root", config={})

    assert mock_run_archive.await_count == 2
