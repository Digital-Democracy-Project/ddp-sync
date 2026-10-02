"""SYNC-83: the knowledge-base embedding hook (PLAN-enterprise-search.md 5.6).

Mocked Pinecone/Redis/api-v3 throughout; nothing here can reach a real index. Covers the
ticket's acceptance list: a new version yields correctly labelled text and diff documents,
a rerun writes nothing, votes are never embedded (SYNC-94), a shrinking
document leaves no surplus chunks, a Pinecone failure leaves the cache un-advanced and the
next run retries -- plus stage-unknown handling, the hook's gates and the watermark.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from structlog.testing import capture_logs

from ddp_sync.config import SyncSettings
from ddp_sync.ingestion.pipeline import IngestionResult
from ddp_sync.pipelines import knowledge_base_embedding as kb
from ddp_sync.pipelines.openstates_archive import (
    _knowledge_base_embedding_eligible,
    _maybe_embed_knowledge_base,
    _run_archive_with_hook,
)

pytestmark = pytest.mark.asyncio

OCD = "0b1c2d3e-0000-0000-0000-000000000001"


class FakeRedis:
    is_available = True

    def __init__(self):
        self.versions: dict[str, dict] = {}
        self.watermarks: dict[str, str] = {}
        self.set_calls = 0

    async def get_bill_version(self, key):
        return self.versions.get(key)

    async def set_bill_version(self, key, data):
        self.set_calls += 1
        self.versions[key] = data
        return True

    async def get_kb_embed_watermark(self, jurisdiction):
        return self.watermarks.get(jurisdiction.lower())

    async def set_kb_embed_watermark(self, jurisdiction, iso):
        self.watermarks[jurisdiction.lower()] = iso
        return True


class FakePipeline:
    """Records ingest_document calls; chunk count is 1 per 100 characters (minimum 1)."""

    def __init__(self):
        self.calls: list[tuple[str, str, dict, bool]] = []
        self.fail_keys: set[str] = set()

    async def ingest_document(self, content, metadata, skip_duplicates=True):
        self.calls.append((metadata.document_id, content, metadata.to_dict(), skip_duplicates))
        if metadata.document_id in self.fail_keys:
            return IngestionResult(1, 3, 0, ["Upsert failed: boom"])
        n = max(1, len(content) // 100)
        return IngestionResult(1, n, n, [])

    def reset_hash_cache(self):
        pass

    @property
    def keys(self) -> list[str]:
        return [c[0] for c in self.calls]


def _settings() -> SyncSettings:
    return SyncSettings(knowledge_base_index_name="ddp-knowledge-base")


def _embedder(redis=None, pipeline=None):
    redis = redis or FakeRedis()
    pipeline = pipeline or FakePipeline()
    return kb.KnowledgeBaseEmbedder(_settings(), pipeline=pipeline, redis_store=redis), redis, pipeline


def _version(doc_id, note, stage, ordinal, text, diff=None, date="2026-01-01", unknown=False):
    v = {
        "note": note, "date": date, "archived_document_id": doc_id,
        "version_stage": stage, "version_ordinal": ordinal,
        "links": [{"url": f"https://x/{doc_id}", "media_type": "application/pdf",
                   **({} if unknown else {"raw_text": text})}],
    }
    if unknown:
        v["archived_raw_text"] = text
    if diff:
        v["diff_from_previous_version"] = diff
    return v


def _bill(versions, votes=None):
    return {
        "id": f"ocd-bill/{OCD}", "identifier": "HB 1", "title": "A bill", "session": "2026",
        "jurisdiction": {"name": "Florida"}, "sources": [{"url": "https://leg.example/hb1"}],
        "versions": versions, "votes": votes or [],
    }


TEXT_A = "introduced text " * 20
TEXT_B = "amended text " * 30
DIFF_B = "--- a\n+++ b\n" + "@@ change @@\n" * 10


async def test_new_version_produces_labelled_text_and_diff_documents():
    emb, redis, pipe = _embedder()
    bill = _bill([
        _version(11, "Introduced", "introduced", 0, TEXT_A),
        _version(12, "Committee Substitute", "amendment", 1, TEXT_B, diff=DIFF_B, date="2026-02-01"),
    ])
    stats = await emb.embed_bill(OCD, "fl", bill)

    assert pipe.keys == [
        f"bill-text:{OCD}:11", f"bill-text:{OCD}:12", f"bill-version-diff:{OCD}:12"
    ]
    _, _, meta, _ = pipe.calls[1]
    assert meta["document_type"] == "bill-text"
    assert meta["document_id"] == "12"  # the archive id, overriding the pipeline key (documented)
    assert (meta["version_note"], meta["version_date"], meta["version_stage"], meta["version_ordinal"]) == (
        "Committee Substitute", "2026-02-01", "amendment", 1)
    assert (meta["ocd_bill_id"], meta["jurisdiction"], meta["session_code"], meta["gov_id"]) == (
        OCD, "FL", "2026", "HB 1")
    assert meta["source_url"] == "https://leg.example/hb1"
    _, diff_content, diff_meta, _ = pipe.calls[2]
    assert diff_content == DIFF_B  # verbatim
    assert diff_meta["document_type"] == "bill-version-diff"
    assert (diff_meta["from_document_id"], diff_meta["from_version_note"]) == ("11", "Introduced")
    assert diff_meta["document_id"] == "12"
    assert stats["undone"] == [] and stats["documents"] == 2 and stats["diffs"] == 1

    cached = redis.versions[OCD]
    assert cached["schema"] == kb.CACHE_SCHEMA
    assert set(cached["documents"]) == {"11", "12"}
    assert cached["documents"]["12"]["diff_chunks"] >= 1


async def test_rerun_writes_nothing():
    emb, redis, pipe = _embedder()
    bill = _bill([_version(11, "Introduced", "introduced", 0, TEXT_A),
                  _version(12, "Sub", "amendment", 1, TEXT_B, diff=DIFF_B)])
    await emb.embed_bill(OCD, "fl", bill)
    pipe.calls.clear()
    sets = redis.set_calls
    stats = await emb.embed_bill(OCD, "fl", bill)
    assert pipe.calls == [] and redis.set_calls == sets
    assert stats["documents"] == stats["diffs"] == 0


async def test_new_version_only_embeds_the_new_version():
    emb, redis, pipe = _embedder()
    v1 = _version(11, "Introduced", "introduced", 0, TEXT_A)
    await emb.embed_bill(OCD, "fl", _bill([v1]))
    pipe.calls.clear()
    await emb.embed_bill(OCD, "fl", _bill([v1, _version(12, "Sub", "amendment", 1, TEXT_B, diff=DIFF_B)]))
    assert pipe.keys == [f"bill-text:{OCD}:12", f"bill-version-diff:{OCD}:12"]


async def test_identical_text_in_two_versions_is_embedded_for_both():
    """skip_duplicates must be False: a content-hash skip would leave a version with no vectors
    while the cache claims it is embedded."""
    emb, _, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _bill([
        _version(11, "Introduced", "introduced", 0, TEXT_A),
        _version(12, "Enrolled", "final_passage", 1, TEXT_A),
    ]))
    assert pipe.keys == [f"bill-text:{OCD}:11", f"bill-text:{OCD}:12"]
    assert all(c[3] is False for c in pipe.calls)


async def test_votes_are_never_embedded_and_a_vote_change_writes_nothing():
    """SYNC-94: votes are structured data that changes regularly; Votebot reads them directly."""
    emb, redis, pipe = _embedder()
    versions = [_version(11, "Introduced", "introduced", 0, TEXT_A)]
    votes = [{"start_date": "2026-02-01"}]
    stats = await emb.embed_bill(OCD, "fl", _bill(versions, votes))
    assert pipe.keys == [f"bill-text:{OCD}:11"]
    assert "votes" not in stats and "votes" not in redis.versions[OCD]

    pipe.calls.clear()
    await emb.embed_bill(OCD, "fl", _bill(versions, votes + [{"start_date": "2026-03-05"}]))
    assert pipe.calls == []  # a new vote is not a reason to write anything


async def test_an_older_cache_entry_that_still_carries_votes_is_kept_working_and_drops_the_field():
    """CACHE_SCHEMA stays 2 on purpose (bumping it would re-embed everything already paid for), so
    an entry written before SYNC-94 may hold a `votes` field. Its documents must still count as
    embedded, and the next write must not carry the dead field forward."""
    emb, redis, pipe = _embedder()
    redis.versions[OCD] = {
        "schema": kb.CACHE_SCHEMA, "votes": {"fingerprint": "1:2026-02-01", "chunks": 3},
        "documents": {"11": {"text_hash": kb.content_hash(TEXT_A), "chunks": 1}},
    }
    await emb.embed_bill(OCD, "fl", _bill([
        _version(11, "Introduced", "introduced", 0, TEXT_A),
        _version(12, "Sub", "amendment", 1, TEXT_B),
    ]))
    assert pipe.keys == [f"bill-text:{OCD}:12"]  # version 11 was already embedded: not redone
    assert "votes" not in redis.versions[OCD] and set(redis.versions[OCD]["documents"]) == {"11", "12"}


async def test_shrinking_document_leaves_no_surplus_chunks():
    emb, redis, pipe = _embedder()
    long_text = "x" * 500  # 5 chunks in the fake
    await emb.embed_bill(OCD, "fl", _bill([_version(11, "Introduced", "introduced", 0, long_text)]))
    assert redis.versions[OCD]["documents"]["11"]["chunks"] == 5

    vs = MagicMock()
    vs.delete = AsyncMock()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        await emb.embed_bill(OCD, "fl", _bill([_version(11, "Introduced", "introduced", 0, "x" * 300)]))
    vs.delete.assert_awaited_once_with(
        ids=[f"bill-text:{OCD}:11-chunk-3", f"bill-text:{OCD}:11-chunk-4"]
    )
    assert redis.versions[OCD]["documents"]["11"]["chunks"] == 3


async def test_pinecone_failure_leaves_cache_unadvanced_and_next_run_retries():
    emb, redis, pipe = _embedder()
    bill = _bill([_version(11, "Introduced", "introduced", 0, TEXT_A),
                  _version(12, "Sub", "amendment", 1, TEXT_B, diff=DIFF_B)])
    pipe.fail_keys = {f"bill-text:{OCD}:12"}
    with capture_logs() as logs:
        stats = await emb.embed_bill(OCD, "fl", bill)
    assert stats["undone"] and "boom" in stats["undone"][0]
    assert OCD not in redis.versions  # not advanced, even though document 11 was written
    warnings = [e for e in logs if e["event"] == "knowledge_base_embedding_work_left_undone"]
    assert warnings and warnings[0]["log_level"] == "warning"

    pipe.fail_keys = set()
    pipe.calls.clear()
    stats = await emb.embed_bill(OCD, "fl", bill)
    assert stats["undone"] == []
    assert f"bill-text:{OCD}:12" in pipe.keys
    assert set(redis.versions[OCD]["documents"]) == {"11", "12"}


async def test_exception_from_pipeline_is_undone_not_raised():
    emb, redis, pipe = _embedder()
    pipe.ingest_document = AsyncMock(side_effect=RuntimeError("openai down"))
    stats = await emb.embed_bill(OCD, "fl", _bill([_version(11, "Introduced", "introduced", 0, TEXT_A)]))
    assert "openai down" in stats["undone"][0] and OCD not in redis.versions


async def test_stage_unknown_version_is_embedded_labelled_unknown_with_no_diff_or_ordinal():
    emb, _, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _bill([
        _version(9, "Fiscal Note", "unknown", None, TEXT_B, diff=DIFF_B, unknown=True),
        _version(11, "Introduced", "introduced", 0, TEXT_A),
    ]))
    assert pipe.keys == [f"bill-text:{OCD}:9", f"bill-text:{OCD}:11"]  # no diff document
    meta = pipe.calls[0][2]
    assert meta["version_stage"] == "unknown" and "version_ordinal" not in meta


async def test_diff_is_labelled_from_the_previous_classifiable_version_not_an_unknown_one():
    emb, _, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _bill([
        _version(9, "Fiscal Note", "unknown", None, TEXT_B, unknown=True),
        _version(11, "Introduced", "introduced", 0, TEXT_A),
        _version(12, "Sub", "amendment", 1, TEXT_B, diff=DIFF_B),
    ]))
    diff_meta = pipe.calls[-1][2]
    assert diff_meta["from_document_id"] == "11"


async def test_version_without_archived_text_is_skipped_and_without_id_is_undone():
    emb, redis, pipe = _embedder()
    no_text = _version(11, "Introduced", "introduced", 0, "")
    stats = await emb.embed_bill(OCD, "fl", _bill([no_text]))
    assert stats["no_text_yet"] == 1 and stats["undone"] == [] and pipe.calls == []

    orphan = _version(None, "Sub", "amendment", 1, TEXT_B)
    stats = await emb.embed_bill(OCD, "fl", _bill([orphan]))
    assert "archived_document_id" in stats["undone"][0] and pipe.calls == []


async def test_legacy_or_unset_index_is_refused():
    with pytest.raises(ValueError):
        kb.KnowledgeBaseEmbedder(SyncSettings(), pipeline=FakePipeline(), redis_store=FakeRedis())
    with pytest.raises(ValueError):
        kb.KnowledgeBaseEmbedder(
            SyncSettings(knowledge_base_index_name="votebot-large"),
            pipeline=FakePipeline(), redis_store=FakeRedis(),
        )
    emb, _, _ = _embedder()
    assert emb.settings.pinecone_index_name == "ddp-knowledge-base"


# --- run level: watermark ------------------------------------------------------------------------


def _patch_client(ids, complete=True, bills=None):
    listed = AsyncMock(return_value=(ids, complete) if ids is not None else None)
    fetch = AsyncMock(side_effect=lambda i, **kw: (bills or {}).get(i))
    return (
        patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.list_touched_bill_ids", listed),
        patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.fetch_bill_for_embedding", fetch),
        listed,
    )


async def _run(redis, embedder, ids, complete=True, bills=None):
    p1, p2, listed = _patch_client(ids, complete, bills)
    started = datetime(2026, 9, 30, 5, 0, tzinfo=timezone.utc)
    with p1, p2, patch("ddp_sync.pipelines.knowledge_base_embedding.get_redis_store", return_value=redis):
        totals = await kb.embed_archived_bills(
            "fl", started, settings=_settings(), api_base="http://api", embedder=embedder
        )
    return totals, listed, started


async def test_watermark_advances_only_when_nothing_was_left_undone():
    emb, redis, pipe = _embedder()
    bills = {OCD: _bill([_version(11, "Introduced", "introduced", 0, TEXT_A)])}
    totals, _, started = await _run(redis, emb, [OCD], bills=bills)
    assert totals["complete"] and redis.watermarks["fl"] == started.isoformat()


async def test_failed_bill_keeps_watermark_and_next_run_rescans_from_it():
    emb, redis, pipe = _embedder()
    redis.watermarks["fl"] = "2026-09-29T05:00:00+00:00"
    bills = {OCD: _bill([_version(11, "Introduced", "introduced", 0, TEXT_A)])}
    pipe.fail_keys = {f"bill-text:{OCD}:11"}
    totals, listed, _ = await _run(redis, emb, [OCD], bills=bills)
    assert not totals["complete"] and totals["failed_bills"] == 1
    assert redis.watermarks["fl"] == "2026-09-29T05:00:00+00:00"
    # the scan started from the older watermark, not this run's archive start
    assert listed.await_args.kwargs["since"].isoformat() == "2026-09-29T05:00:00+00:00"


async def test_unreadable_bill_and_listing_failure_are_undone_not_raised():
    emb, redis, _ = _embedder()
    totals, _, _ = await _run(redis, emb, [OCD], bills={})  # fetch returns None
    assert totals["failed_bills"] == 1 and not totals["complete"] and "fl" not in redis.watermarks
    totals, _, _ = await _run(redis, emb, None)  # listing failed
    assert not totals["complete"] and "fl" not in redis.watermarks


async def test_truncated_listing_is_not_complete():
    emb, redis, _ = _embedder()
    totals, _, _ = await _run(redis, emb, [], complete=False)
    assert not totals["complete"] and "fl" not in redis.watermarks


async def test_redis_down_embeds_nothing_and_warns():
    emb, redis, pipe = _embedder()
    redis.is_available = False
    with capture_logs() as logs:
        totals, listed, _ = await _run(redis, emb, [OCD])
    assert totals["bills"] == 0 and pipe.calls == [] and listed.await_count == 0
    assert any(e["event"] == "knowledge_base_embedding_work_left_undone" for e in logs)


# --- hook gates and wiring ---------------------------------------------------------------------


async def test_eligibility_is_yaml_policy_default_off():
    assert not _knowledge_base_embedding_eligible("fl", None)
    assert not _knowledge_base_embedding_eligible("fl", {"knowledge_base_embedding": {"enabled": False, "jurisdictions": ["fl"]}})
    cfg = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["FL", "us"]}}
    assert _knowledge_base_embedding_eligible("fl", cfg) and _knowledge_base_embedding_eligible("US", cfg)
    assert not _knowledge_base_embedding_eligible("va", cfg)


_CFG = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"]}}
_STARTED = datetime(2026, 9, 30, 5, 0, tzinfo=timezone.utc)


async def test_hook_does_nothing_unless_the_index_setting_is_also_set():
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=SyncSettings()), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=AsyncMock()) as run:
        await _maybe_embed_knowledge_base("fl", _STARTED, _CFG)
        run.assert_not_awaited()
        await _maybe_embed_knowledge_base("fl", _STARTED, None)
        run.assert_not_awaited()


async def test_hook_runs_with_local_api_on_the_mac_and_rds_api_elsewhere():
    mac = SyncSettings(knowledge_base_index_name="ddp-knowledge-base", cams_api_token="t",
                       local_openstates_api_base="http://local", local_openstates_api_key="lk")
    ec2 = SyncSettings(knowledge_base_index_name="ddp-knowledge-base",
                       rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    for settings, base, key in ((mac, "http://local", "lk"), (ec2, "http://rds", "rk")):
        with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=settings), \
             patch("ddp_sync.pipelines.knowledge_base_entities.embed_legislators", new=AsyncMock()), \
             patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=AsyncMock()) as run:
            await _maybe_embed_knowledge_base("fl", _STARTED, _CFG)
        kwargs = run.await_args.kwargs
        assert (kwargs["api_base"], kwargs["api_key"]) == (base, key)


async def test_archive_wrapper_runs_both_hooks_independently_and_never_changes_the_result():
    ok = {"success": True, "jurisdiction": "fl"}
    with patch("ddp_sync.pipelines.openstates_archive._acquire_archive_debounce", new=AsyncMock(return_value=True)), \
         patch("ddp_sync.pipelines.openstates_archive._run_archive", new=AsyncMock(return_value=ok)), \
         patch("ddp_sync.pipelines.openstates_archive._get_root", return_value="/x"), \
         patch("ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive",
               new=AsyncMock(side_effect=RuntimeError("legbot boom"))) as legbot, \
         patch("ddp_sync.pipelines.openstates_archive._maybe_embed_knowledge_base",
               new=AsyncMock(side_effect=RuntimeError("kb boom"))) as kb_hook:
        result = await _run_archive_with_hook("fl", config=_CFG)
    assert result == ok
    legbot.assert_awaited_once()
    kb_hook.assert_awaited_once()
    assert kb_hook.await_args.args[2] is _CFG


async def test_failed_archive_triggers_neither_hook():
    bad = {"success": False, "jurisdiction": "fl"}
    with patch("ddp_sync.pipelines.openstates_archive._acquire_archive_debounce", new=AsyncMock(return_value=True)), \
         patch("ddp_sync.pipelines.openstates_archive._run_archive", new=AsyncMock(return_value=bad)), \
         patch("ddp_sync.pipelines.openstates_archive._get_root", return_value="/x"), \
         patch("ddp_sync.pipelines.openstates_archive._maybe_trigger_legbot_for_archive", new=AsyncMock()) as legbot, \
         patch("ddp_sync.pipelines.openstates_archive._maybe_embed_knowledge_base", new=AsyncMock()) as kb_hook:
        await _run_archive_with_hook("fl", config=_CFG)
    legbot.assert_not_awaited()
    kb_hook.assert_not_awaited()


# --- api-v3 client reads and Redis helpers -------------------------------------------------------


async def test_list_touched_bill_ids_paginates_dedups_and_strips_prefix():
    from ddp_sync.services import local_openstates_client as c

    pages = [
        {"results": [{"id": "ocd-bill/aaa"}, {"id": "ocd-bill/bbb"}], "pagination": {"max_page": 2}},
        {"results": [{"id": "ocd-bill/bbb"}, {"id": "ocd-bill/ccc"}], "pagination": {"max_page": 2}},
    ]
    get = AsyncMock(side_effect=pages)
    with patch.object(c, "_get_json_with_retry", get):
        out = await c.list_touched_bill_ids("fl", since=_STARTED, api_base="http://api")
    assert out == (["aaa", "bbb", "ccc"], True)
    first = get.await_args_list[0].args[1]
    assert first["jurisdiction"] == "FL" and "document_updated_since" in first


async def test_list_touched_bill_ids_failure_semantics():
    from ddp_sync.services import local_openstates_client as c

    page1 = {"results": [{"id": "ocd-bill/a"}, {"id": "ocd-bill/b"}], "pagination": {"max_page": 9}}
    with patch.object(c, "_get_json_with_retry", AsyncMock(side_effect=[page1, None])):
        # a later page failing keeps what was read but reports the listing incomplete
        assert await c.list_touched_bill_ids("fl", since=_STARTED, api_base="x") == (["a", "b"], False)
    with patch.object(c, "_get_json_with_retry", AsyncMock(return_value=None)):
        assert await c.list_touched_bill_ids("fl", since=_STARTED, api_base="x") is None
    assert await c.list_touched_bill_ids("fl", since=_STARTED, api_base="") is None


async def test_legacy_shaped_cache_entry_is_ignored_not_trusted():
    """The legacy webflow-keyed entries share the ddp:bill_version: prefix but not the key space
    (webflow ids vs ocd uuids); an entry without schema 2 is treated as never embedded."""
    emb, redis, pipe = _embedder()
    redis.versions[OCD] = {"version_date": "2026-01-01", "version_note": "Introduced", "chunk_count": 99}
    await emb.embed_bill(OCD, "fl", _bill([_version(11, "Introduced", "introduced", 0, TEXT_A)]))
    assert pipe.keys == [f"bill-text:{OCD}:11"]
    assert redis.versions[OCD]["schema"] == kb.CACHE_SCHEMA


async def test_real_pipeline_chunk_ids_use_the_full_document_key_despite_the_metadata_override():
    from ddp_sync.ingestion.pipeline import IngestionPipeline

    real = IngestionPipeline(
        SyncSettings(pinecone_index_name="ddp-knowledge-base", openai_api_key="test-not-a-real-key")
    )
    upserted = []

    async def fake_upsert(documents, batch_size=100):
        upserted.extend(documents)
        return len(documents)

    real.vector_store.upsert_documents = fake_upsert
    emb = kb.KnowledgeBaseEmbedder(_settings(), pipeline=real, redis_store=FakeRedis())
    await emb.embed_bill(OCD, "fl", _bill([_version(11, "Introduced", "introduced", 0, TEXT_A)]))
    assert upserted and upserted[0].id == f"bill-text:{OCD}:11-chunk-0"
    assert upserted[0].metadata["document_id"] == "11"  # archive id, as retrieval filters expect


async def test_fetch_bill_for_embedding_requests_versions_and_sources_but_not_votes():
    from ddp_sync.services import local_openstates_client as c

    get = AsyncMock(return_value={"id": "ocd-bill/aaa"})
    with patch.object(c, "_get_json_with_retry", get):
        assert await c.fetch_bill_for_embedding("aaa", api_base="http://api", api_key="k") == {"id": "ocd-bill/aaa"}
    url, params, headers, _ = get.await_args.args
    assert url == "http://api/bills/ocd-bill/aaa" and headers == {"x-api-key": "k"}
    assert params == [("include", "versions"), ("include", "sources")]


async def test_redis_store_set_bill_version_reports_success_and_watermark_roundtrip():
    from ddp_sync.services.redis_store import RedisStore

    store = RedisStore()
    assert await store.set_bill_version("k", {}) is False  # no client: reported, not silent
    client = MagicMock()
    client.set = AsyncMock()
    client.get = AsyncMock(return_value="2026-09-30T05:00:00+00:00")
    store._client = client
    assert await store.set_bill_version("k", {"a": 1}) is True
    assert await store.set_kb_embed_watermark("FL", "2026-09-30T05:00:00+00:00") is True
    assert client.set.await_args.args[0] == "ddp:kb_embed:since:fl"
    assert await store.get_kb_embed_watermark("FL") == "2026-09-30T05:00:00+00:00"
    client.set = AsyncMock(side_effect=RuntimeError("down"))
    assert await store.set_bill_version("k", {}) is False


# --- SYNC-90: EmbedScope, the cache-race guard, merge-on-write ------------------------------------

TEXT_C = "enrolled text " * 25
DIFF_C = "--- c\n+++ d\n" + "@@ more @@\n" * 10
_VOTES = [{"start_date": "2026-02-01"}]


def _three_versions():
    return [
        _version(1, "Filed", "introduced", 0, TEXT_A),
        _version(2, "Amended", "amendment", 1, TEXT_B, diff=DIFF_B),
        _version(3, "Enrolled", "enacted", 2, TEXT_C, diff=DIFF_C),
    ]


def _text(n):
    return kb.text_document_key(OCD, n)


def _diff(n):
    return kb.diff_document_key(OCD, n)


async def test_scope_current_embeds_only_the_current_version_text():
    emb, _, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _bill(_three_versions(), _VOTES),
                         kb.EmbedScope(text="current", diffs=False))
    assert pipe.keys == [_text(3)]


async def test_scope_diffs_only():
    emb, _, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _bill(_three_versions(), _VOTES),
                         kb.EmbedScope(text=None, diffs=True))
    assert pipe.keys == [_diff(2), _diff(3)]


async def test_scope_all_text_embeds_every_version_and_nothing_else():
    emb, _, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _bill(_three_versions(), _VOTES),
                         kb.EmbedScope(text="all", diffs=False))
    assert pipe.keys == [_text(1), _text(2), _text(3)]


async def test_default_scope_is_everything_unchanged_for_the_live_hook():
    emb, _, pipe = _embedder()
    stats = await emb.embed_bill(OCD, "fl", _bill(_three_versions(), _VOTES))
    assert sorted(pipe.keys) == sorted([_text(1), _text(2), _text(3), _diff(2), _diff(3)])  # no votes document
    assert stats["chars"] > 0 and stats["raced"] == 0


async def test_stage_unknown_is_never_the_current_version():
    unknown_only = [_version(7, "Mystery", "unknown", None, TEXT_A, unknown=True)]
    emb, _, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _bill(unknown_only), kb.EmbedScope(text="current", diffs=False))
    assert pipe.keys == []
    mixed = unknown_only + [_version(8, "Filed", "introduced", 0, TEXT_B)]
    emb, _, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _bill(mixed), kb.EmbedScope(text="current", diffs=False))
    assert pipe.keys == [_text(8)]


async def test_a_narrow_pass_then_a_full_pass_never_rewrites_what_the_first_wrote():
    emb, _, pipe = _embedder()
    bill = _bill(_three_versions(), _VOTES)
    await emb.embed_bill(OCD, "fl", bill, kb.EmbedScope(text="current", diffs=False))
    first = list(pipe.keys)
    await emb.embed_bill(OCD, "fl", bill)
    assert first == [_text(3)]
    assert pipe.keys.count(_text(3)) == 1  # not re-embedded by the full pass
    assert sorted(pipe.keys[1:]) == sorted([_text(1), _text(2), _diff(2), _diff(3)])
    again = len(pipe.keys)
    stats = await emb.embed_bill(OCD, "fl", bill)
    assert len(pipe.keys) == again and stats["documents"] == stats["diffs"] == 0


class _RacingRedis(FakeRedis):
    """After this call's first read, another writer records document 1 as embedded with newer text."""

    def __init__(self):
        super().__init__()
        self.reads = 0

    async def get_bill_version(self, key):
        self.reads += 1
        if self.reads == 2:  # the guard's read, just before the first upsert
            self.versions[key] = {"schema": kb.CACHE_SCHEMA,
                                  "documents": {"1": {"text_hash": "live-newer", "chunks": 9}}}
        return self.versions.get(key)


async def test_a_document_changed_by_another_writer_is_skipped_and_its_entry_survives():
    redis = _RacingRedis()
    emb, redis, pipe = _embedder(redis=redis)
    stats = await emb.embed_bill(OCD, "fl", _bill(_three_versions()),
                                 kb.EmbedScope(text="all", diffs=False))
    assert _text(1) not in pipe.keys  # the older text never overwrote the newer live write
    assert _text(2) in pipe.keys and _text(3) in pipe.keys
    assert stats["raced"] == 1 and stats["undone"] == []
    docs = redis.versions[OCD]["documents"]
    assert docs["1"]["text_hash"] == "live-newer"  # kept, not clobbered by our cache write
    assert "text_hash" in docs["2"] and "text_hash" in docs["3"]


async def test_read_target_follows_the_mac_split():
    mac = SyncSettings(local_openstates_api_base="http://local", local_openstates_api_key="lk",
                       rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    assert kb.read_target(mac, mac_capable=True) == ("http://local", "lk")
    assert kb.read_target(mac, mac_capable=False) == ("http://rds", "rk")


async def test_list_touched_bill_ids_passes_the_session_filter():
    from ddp_sync.services import local_openstates_client as c

    get = AsyncMock(return_value={"results": [{"id": "ocd-bill/aaa"}], "pagination": {"max_page": 1}})
    with patch.object(c, "_get_json_with_retry", get):
        await c.list_touched_bill_ids("fl", since=_STARTED, api_base="http://api", session="2026")
        await c.list_touched_bill_ids("fl", since=_STARTED, api_base="http://api")
    assert get.await_args_list[0].args[1]["session"] == "2026"
    assert "session" not in get.await_args_list[1].args[1]


class _BlipRedis(FakeRedis):
    """`get_bill_version` returns None (what a Redis error looks like) on the read numbers in `blips`."""

    def __init__(self, blips):
        super().__init__()
        self.blips = set(blips)
        self.reads = 0

    async def get_bill_version(self, key):
        self.reads += 1
        if self.reads in self.blips:
            return None
        return self.versions.get(key)


def _prior_cache():
    return {"schema": kb.CACHE_SCHEMA,
            "documents": {"1": {"text_hash": kb.content_hash(TEXT_A), "chunks": 1}}}


async def test_a_redis_blip_before_the_write_is_undone_not_raced():
    """The guard's read returning None for an entry that existed must not look like another writer:
    that would silently skip a changed version (and let the watermark advance past it)."""
    redis = _BlipRedis(blips={2})
    redis.versions[OCD] = _prior_cache()
    emb, redis, _ = _embedder(redis=redis)
    with capture_logs() as logs:
        stats = await emb.embed_bill(OCD, "fl", _bill(_three_versions()),
                                     kb.EmbedScope(text="all", diffs=False))
    assert stats["raced"] == 0
    assert any("unreadable before write" in u for u in stats["undone"])
    assert any(e["event"] == "knowledge_base_embedding_work_left_undone" for e in logs)
    assert redis.versions[OCD] == _prior_cache()  # cache untouched: the next run redoes it


async def test_a_redis_blip_at_the_final_write_does_not_erase_the_other_entries():
    emb, redis, _ = _embedder(redis=_BlipRedis(blips={4}))  # read 1 start, 2-3 guards (docs 2, 3), 4 final
    redis.versions[OCD] = _prior_cache()
    with capture_logs() as logs:
        stats = await emb.embed_bill(OCD, "fl", _bill(_three_versions()),
                                     kb.EmbedScope(text="all", diffs=False))
    assert any("unreadable at write" in u for u in stats["undone"])
    assert any(e["event"] == "knowledge_base_embedding_work_left_undone" for e in logs)
    assert redis.versions[OCD] == _prior_cache()  # not replaced by a cache holding only this call's documents
    assert redis.set_calls == 0


async def test_no_cache_at_start_is_not_mistaken_for_a_blip():
    emb, _, pipe = _embedder(redis=_BlipRedis(blips=set()))
    stats = await emb.embed_bill(OCD, "fl", _bill(_three_versions()),
                                 kb.EmbedScope(text="all", diffs=False))
    assert stats["undone"] == [] and stats["raced"] == 0 and len(pipe.keys) == 3
