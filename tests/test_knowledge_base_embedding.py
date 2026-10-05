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
        self.persistent_keys: set[str] = set()  # entries written with persistent=True (no expiry)
        self.unreadable = False  # find_unrecorded_bill_versions behaves as if Redis failed
        self.unreadable_records = False  # get_bill_versions behaves as if Redis failed

    async def get_bill_version(self, key):
        return self.versions.get(key)

    async def set_bill_version(self, key, data, *, persistent=False, only_if_absent=False):
        if only_if_absent and key in self.versions:
            return False
        self.set_calls += 1
        self.versions[key] = data
        if persistent:
            self.persistent_keys.add(key)
        return True

    async def get_bill_versions(self, ids):
        return None if self.unreadable_records else {i: self.versions.get(i) for i in ids}

    async def find_unrecorded_bill_versions(self, ids, *, persist_expiring=True):
        self.persist_expiring_seen = persist_expiring
        if self.unreadable:
            return None
        return [i for i in ids if i not in self.versions]

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
    stats = await emb.embed_bill(OCD, "fl", bill, kb.EmbedScope(diffs=True))

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
    await emb.embed_bill(OCD, "fl", _bill([v1]), kb.EmbedScope(diffs=True))
    pipe.calls.clear()
    await emb.embed_bill(OCD, "fl", _bill([v1, _version(12, "Sub", "amendment", 1, TEXT_B, diff=DIFF_B)]), kb.EmbedScope(diffs=True))
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
    ]), kb.EmbedScope(diffs=True))
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


async def test_checked_in_yaml_enrolls_the_seven_embedding_jurisdictions_and_not_nc():
    """On since 2026-10-05. The per-host gate is KNOWLEDGE_BASE_INDEX_NAME (see the test below), so the shared file
    being on is inert on a host that does not set it."""
    from pathlib import Path

    import yaml

    cfg = yaml.safe_load((Path(__file__).parent.parent / "config" / "sync_schedule.yaml").read_text())["openstates_archive"]
    for jurisdiction in ("fl", "us", "va", "mi", "wa", "az", "ut"):
        assert _knowledge_base_embedding_eligible(jurisdiction, cfg), jurisdiction
    assert not _knowledge_base_embedding_eligible("nc", cfg)  # NC has no embedded people or backfill


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


async def test_default_scope_embeds_every_version_but_no_diffs_and_no_votes():
    """Ramon, 2026-10-05: version diffs are not embedded (as with votes, SYNC-94); the live hook and every pass
    that takes the default scope write bill text only."""
    emb, _, pipe = _embedder()
    stats = await emb.embed_bill(OCD, "fl", _bill(_three_versions(), _VOTES))
    assert sorted(pipe.keys) == sorted([_text(1), _text(2), _text(3)])
    assert stats["diffs"] == 0 and stats["chars"] > 0 and stats["raced"] == 0


async def test_a_diff_is_written_only_when_the_scope_asks_for_it():
    emb, _, pipe = _embedder()
    stats = await emb.embed_bill(OCD, "fl", _bill(_three_versions(), _VOTES), kb.EmbedScope(diffs=True))
    assert sorted(pipe.keys) == sorted([_text(1), _text(2), _text(3), _diff(2), _diff(3)])
    assert stats["diffs"] == 2


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
    assert sorted(pipe.keys[1:]) == sorted([_text(1), _text(2)])  # the default scope writes no diffs
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


# --- SYNC-95: reconcile bills the version cache has no record of ---------------------------------------

OCD2 = "0b1c2d3e-0000-0000-0000-000000000002"
OCD3 = "0b1c2d3e-0000-0000-0000-000000000003"


def _one_version_bill(ocd, text=TEXT_A, doc_id=11):
    bill = _bill([_version(doc_id, "Introduced", "introduced", 0, text)])
    bill["id"] = f"ocd-bill/{ocd}"
    return bill


async def _run_reconcile(redis, embedder, *, touched, everything, bills, cap, watermark_listing=None):
    """The touched window (a `since` after the epoch) and the full listing (`since` = the epoch) answer
    differently, as api-v3 does."""
    async def listed(jurisdiction, *, since, **kw):
        return (list(everything), True) if since == kb._EPOCH else (list(touched), True)

    list_mock = AsyncMock(side_effect=listed)
    fetch = AsyncMock(side_effect=lambda i, **kw: bills.get(i))
    started = _STARTED
    with patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.list_touched_bill_ids", list_mock), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.fetch_bill_for_embedding", fetch), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.get_redis_store", return_value=redis):
        totals = await kb.embed_archived_bills(
            "fl", started, settings=_settings(), api_base="http://api", embedder=embedder,
            reconcile_max_bills=cap,
        )
    return totals, list_mock, fetch


async def test_reconcile_embeds_an_unchanged_bill_that_was_never_embedded():
    emb, redis, pipe = _embedder()
    redis.versions[OCD] = {"schema": kb.CACHE_SCHEMA, "documents": {"11": {"text_hash": "x", "chunks": 1}}}
    totals, _, fetch = await _run_reconcile(
        redis, emb, touched=[], everything=[OCD, OCD2], bills={OCD2: _one_version_bill(OCD2)}, cap=10)
    assert pipe.keys == [f"bill-text:{OCD2}:11"]
    assert [c.args[0] for c in fetch.await_args_list] == [OCD2]  # the recorded bill is never even read
    assert totals["reconcile"] == {"missing": 1, "selected": 1, "embedded": 1, "nothing_to_embed": 0, "failed": 0}
    assert OCD2 in redis.persistent_keys  # the new record does not expire


async def test_reconcile_is_off_by_default_and_then_lists_only_the_touched_window():
    emb, redis, pipe = _embedder()
    totals, listed, _ = await _run_reconcile(
        redis, emb, touched=[], everything=[OCD2], bills={OCD2: _one_version_bill(OCD2)}, cap=0)
    assert pipe.keys == [] and "reconcile" not in totals
    assert listed.await_count == 1 and listed.await_args.kwargs["since"] != kb._EPOCH


async def test_reconcile_cap_takes_a_slice_and_the_next_run_takes_the_next():
    emb, redis, pipe = _embedder()
    everything = [OCD, OCD2, OCD3]
    bills = {i: _one_version_bill(i) for i in everything}
    with capture_logs() as logs:
        first, _, _ = await _run_reconcile(redis, emb, touched=[], everything=everything, bills=bills, cap=2)
    assert len(pipe.keys) == 2 and len(set(pipe.keys)) == 2  # a slice of two of the three, whichever the random pick chose
    assert (first["reconcile"]["missing"], first["reconcile"]["selected"]) == (3, 2)
    backlog = [e for e in logs if e["event"] == "knowledge_base_reconcile_backlog"]
    assert backlog and backlog[0]["remaining"] == 1 and backlog[0]["cap"] == 2
    second, _, _ = await _run_reconcile(redis, emb, touched=[], everything=everything, bills=bills, cap=2)
    assert second["reconcile"]["missing"] == 1  # the embedded two dropped out; only the third is left
    assert sorted(pipe.keys) == sorted(f"bill-text:{i}:11" for i in everything)


async def test_reconcile_does_nothing_when_redis_cannot_say_what_is_recorded():
    """An outage must never look like 'nothing is embedded', or every bill would be re-embedded."""
    emb, redis, pipe = _embedder()
    redis.unreadable = True
    bills = {OCD: _one_version_bill(OCD), OCD2: _one_version_bill(OCD2)}
    with capture_logs() as logs:
        totals, _, _ = await _run_reconcile(redis, emb, touched=[OCD], everything=[OCD, OCD2], bills=bills, cap=10)
    assert pipe.keys == [f"bill-text:{OCD}:11"]  # the touched bill is still embedded; OCD2 is not guessed at
    assert totals["reconcile"]["selected"] == 0
    assert any(e["event"] == "knowledge_base_reconcile_skipped" for e in logs)


async def test_reconcile_skips_bills_the_touched_pass_already_handled():
    emb, redis, pipe = _embedder()
    totals, _, fetch = await _run_reconcile(
        redis, emb, touched=[OCD], everything=[OCD, OCD2],
        bills={OCD: _one_version_bill(OCD), OCD2: _one_version_bill(OCD2)}, cap=10)
    assert pipe.keys == [f"bill-text:{OCD}:11", f"bill-text:{OCD2}:11"]
    assert [c.args[0] for c in fetch.await_args_list] == [OCD, OCD2]  # OCD once, from the touched pass
    assert totals["reconcile"]["missing"] == 1


async def test_a_bill_with_nothing_to_embed_is_recorded_so_it_is_not_read_every_night():
    emb, redis, pipe = _embedder()
    empty = _bill([_version(11, "Introduced", "introduced", 0, "")])  # no archived text yet
    empty["id"] = f"ocd-bill/{OCD}"
    first, _, _ = await _run_reconcile(redis, emb, touched=[], everything=[OCD], bills={OCD: empty}, cap=10)
    assert pipe.keys == [] and first["reconcile"]["nothing_to_embed"] == 1
    assert redis.versions[OCD]["documents"] == {} and OCD in redis.persistent_keys
    again, _, fetch2 = await _run_reconcile(redis, emb, touched=[], everything=[OCD], bills={OCD: empty}, cap=10)
    assert fetch2.await_count == 0 and again["reconcile"]["missing"] == 0
    # when text later arrives, the touched pass embeds it normally against the empty record
    await _run_reconcile(redis, emb, touched=[OCD], everything=[OCD], bills={OCD: _one_version_bill(OCD)}, cap=10)
    assert pipe.keys == [f"bill-text:{OCD}:11"]


async def test_a_reconcile_failure_keeps_no_record_and_does_not_hold_back_the_watermark():
    emb, redis, pipe = _embedder()
    pipe.fail_keys = {f"bill-text:{OCD}:11"}
    totals, _, _ = await _run_reconcile(
        redis, emb, touched=[], everything=[OCD], bills={OCD: _one_version_bill(OCD)}, cap=10)
    assert totals["reconcile"]["failed"] == 1 and OCD not in redis.versions
    assert totals["complete"] is True and "fl" in redis.watermarks  # the touched pass was clean
    pipe.fail_keys = set()
    retry, _, _ = await _run_reconcile(
        redis, emb, touched=[], everything=[OCD], bills={OCD: _one_version_bill(OCD)}, cap=10)
    assert retry["reconcile"]["embedded"] == 1 and OCD in redis.versions  # the retry wrote it (the first attempt failed)


async def test_embed_bill_writes_its_record_without_an_expiry():
    emb, redis, _ = _embedder()
    await emb.embed_bill(OCD, "fl", _one_version_bill(OCD))
    assert OCD in redis.persistent_keys


async def test_the_reconcile_cap_comes_from_yaml_and_anything_odd_means_off():
    from ddp_sync.pipelines.openstates_archive import _reconcile_max_bills

    def cfg(value):
        return {"knowledge_base_embedding": {"reconcile": {"max_bills_per_run": value}}}

    assert _reconcile_max_bills(None) == 0 and _reconcile_max_bills({}) == 0
    assert _reconcile_max_bills({"knowledge_base_embedding": {}}) == 0
    assert _reconcile_max_bills(cfg(25)) == 25 and _reconcile_max_bills(cfg(0)) == 0
    for odd in ("many", -1, True, 1.5, None):
        assert _reconcile_max_bills(cfg(odd)) == 0, odd


async def test_the_hook_passes_the_reconcile_cap_to_the_run():
    config = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"],
                                           "reconcile": {"max_bills_per_run": 25}}}
    settings = SyncSettings(knowledge_base_index_name="ddp-knowledge-base",
                            rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=AsyncMock()) as run:
        await _maybe_embed_knowledge_base("fl", _STARTED, config)
    assert run.await_args.kwargs["reconcile_max_bills"] == 25


async def test_the_checked_in_yaml_ships_with_the_reconcile_pass_off():
    from pathlib import Path

    import yaml

    cfg = yaml.safe_load((Path(__file__).parent.parent / "config" / "sync_schedule.yaml").read_text())["openstates_archive"]
    assert cfg["knowledge_base_embedding"]["reconcile"]["max_bills_per_run"] == 0


# --- SYNC-95: the record check in RedisStore -------------------------------------------------------------


class _PipeClient:
    """Just enough of redis.asyncio for `find_unrecorded_bill_versions`: ttl() and persist() queue ops
    on a pipeline whose execute() returns their answers."""

    def __init__(self, ttls, fail_on=None):
        self.ttls, self.fail_on, self.persisted, self.pipelines = ttls, fail_on, [], 0

    def pipeline(self, transaction=False):
        client = self
        client.pipelines += 1
        if client.fail_on is not None and client.pipelines >= client.fail_on:
            raise RuntimeError("redis down")

        class _Pipe:
            def __init__(self):
                self.ops = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            def ttl(self, key):
                self.ops.append(("ttl", key))

            def persist(self, key):
                self.ops.append(("persist", key))
                client.persisted.append(key)

            async def execute(self):
                return [client.ttls[k] if op == "ttl" else 1 for op, k in self.ops]

        return _Pipe()


async def test_find_unrecorded_bill_versions_reports_missing_and_makes_expiring_entries_persistent():
    from ddp_sync.services.redis_store import BILL_VERSION_PREFIX, RedisStore

    store = RedisStore()
    assert await store.find_unrecorded_bill_versions(["a"]) is None  # no client: unknown, not "all missing"
    ttls = {f"{BILL_VERSION_PREFIX}a": -2, f"{BILL_VERSION_PREFIX}b": 5000, f"{BILL_VERSION_PREFIX}c": -1,
            f"{BILL_VERSION_PREFIX}d": -2}
    store._client = _PipeClient(ttls)
    assert await store.find_unrecorded_bill_versions(["a", "b", "c", "d"]) == ["a", "d"]
    assert store._client.persisted == [f"{BILL_VERSION_PREFIX}b"]  # only the entry that still had an expiry


async def test_find_unrecorded_bill_versions_is_unknown_when_redis_fails_and_reads_in_chunks():
    from ddp_sync.services.redis_store import BILL_VERSION_PREFIX, RedisStore

    store = RedisStore()
    store._client = _PipeClient({}, fail_on=1)
    assert await store.find_unrecorded_bill_versions(["a"]) is None
    ids = [f"id{i}" for i in range(1500)]
    store._client = _PipeClient({f"{BILL_VERSION_PREFIX}{i}": -1 for i in ids})
    assert await store.find_unrecorded_bill_versions(ids) == []
    assert store._client.pipelines == 2  # 1,000 then 500


async def test_set_bill_version_persistent_has_no_expiry_and_the_default_keeps_the_legacy_one():
    from ddp_sync.services.redis_store import BILL_VERSION_TTL, RedisStore

    store = RedisStore()
    store._client = MagicMock()
    store._client.set = AsyncMock()
    await store.set_bill_version("k", {"a": 1})
    assert store._client.set.await_args.kwargs["ex"] == BILL_VERSION_TTL
    await store.set_bill_version("k", {"a": 1}, persistent=True)
    assert store._client.set.await_args.kwargs["ex"] is None
    assert store._client.set.await_args.kwargs["nx"] is False  # the default write replaces, as it always did


async def test_set_bill_version_only_if_absent_is_one_atomic_set_nx_and_reports_whether_it_wrote():
    from ddp_sync.services.redis_store import RedisStore

    store = RedisStore()
    store._client = MagicMock()
    store._client.set = AsyncMock(return_value=True)
    assert await store.set_bill_version("k", {}, persistent=True, only_if_absent=True) is True
    assert store._client.set.await_args.kwargs["nx"] is True
    store._client.set = AsyncMock(return_value=None)  # redis-py answers None when NX found the key
    assert await store.set_bill_version("k", {}, persistent=True, only_if_absent=True) is False


async def test_find_unrecorded_bill_versions_is_unknown_when_a_later_chunk_fails_not_a_partial_list():
    """The first 1,000 ids were checked and 'a' was missing; the second chunk fails. Returning what the
    first chunk found would be a partial answer, so the whole check reports unknown."""
    from ddp_sync.services.redis_store import BILL_VERSION_PREFIX, RedisStore

    store = RedisStore()
    ids = ["a"] + [f"id{i}" for i in range(1499)]
    store._client = _PipeClient({f"{BILL_VERSION_PREFIX}{i}": (-2 if i == "a" else -1) for i in ids}, fail_on=2)
    assert await store.find_unrecorded_bill_versions(ids) is None


async def test_the_nothing_to_embed_marker_never_replaces_a_record_written_in_the_meantime():
    emb, redis, _ = _embedder()
    real = {"schema": kb.CACHE_SCHEMA, "documents": {"11": {"text_hash": "real", "chunks": 3}}}

    async def embed_then_a_real_record_appears(ocd, jurisdiction, bill, scope=kb.SCOPE_ALL, **kwargs):
        redis.versions[ocd] = real  # another writer finishes this bill while we were reading it
        return {"documents": 0, "diffs": 0, "chunks": 0, "undone": []}

    emb.embed_bill = embed_then_a_real_record_appears
    totals, _, _ = await _run_reconcile(redis, emb, touched=[], everything=[OCD], bills={OCD: _one_version_bill(OCD)}, cap=10)
    assert redis.versions[OCD] is real and totals["reconcile"]["nothing_to_embed"] == 1


async def test_bills_that_always_fail_cannot_starve_the_rest_of_a_capped_backlog():
    """As many always-failing bills as the cap, listed first: a front slice would retry only them, forever."""
    import random

    random.seed(7)
    emb, redis, pipe = _embedder()
    bad = [OCD, OCD2]
    good = [f"0b1c2d3e-0000-0000-0000-0000000001{n:02d}" for n in range(8)]
    everything = bad + good  # the failing bills are first in api-v3's order
    bills = {i: _one_version_bill(i) for i in everything}
    pipe.fail_keys = {f"bill-text:{i}:11" for i in bad}
    for _ in range(40):
        await _run_reconcile(redis, emb, touched=[], everything=everything, bills=bills, cap=2)
        if len(redis.versions) == len(good):
            break
    assert set(redis.versions) == set(good)  # every other bill was reached; the failing ones keep no record


async def test_a_partial_listing_is_still_safe_to_reconcile_against():
    """`list_touched_bill_ids` flags an incomplete listing; every bill that IS on it and has no record
    really is unrecorded, so the pass embeds those and merely misses the bills the listing never reached."""
    emb, redis, pipe = _embedder()

    async def listed(jurisdiction, *, since, **kw):
        return ([OCD2], False) if since == kb._EPOCH else ([], True)

    with patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.list_touched_bill_ids", AsyncMock(side_effect=listed)), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.fetch_bill_for_embedding",
               AsyncMock(return_value=_one_version_bill(OCD2))), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.get_redis_store", return_value=redis):
        totals = await kb.embed_archived_bills("fl", _STARTED, settings=_settings(), api_base="http://api",
                                               embedder=emb, reconcile_max_bills=5)
    assert pipe.keys == [f"bill-text:{OCD2}:11"] and totals["reconcile"]["embedded"] == 1


async def test_only_the_knowledge_base_path_writes_bill_versions_without_an_expiry():
    """The legacy webflow-keyed entries must keep their 90-day TTL (a source scan, as SYNC-42 does)."""
    from pathlib import Path

    src = Path(__file__).parent.parent / "src"
    users = {str(p.relative_to(src)) for p in src.rglob("*.py")
             if "persistent=True" in p.read_text() and p.name != "redis_store.py"}
    assert users == {"ddp_sync/pipelines/knowledge_base_embedding.py"}


# --- SYNC-95: the dry run -----------------------------------------------------------------------------


def _sized_bill(ocd, text_chars, diff_chars=0):
    """A bill with one classifiable version of `text_chars` characters (and, with `diff_chars`, a stored diff)."""
    bill = _bill([_version(11, "Introduced", "introduced", 0, "t" * text_chars,
                           diff="d" * diff_chars if diff_chars else None)])
    bill["id"] = f"ocd-bill/{ocd}"
    return bill


async def _plan(redis, *, everything, bills, sample_size=20, listing=True):
    list_mock = AsyncMock(return_value=(list(everything), True) if listing else None)
    fetch = AsyncMock(side_effect=lambda i, **kw: bills.get(i))
    with patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.list_touched_bill_ids", list_mock), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.fetch_bill_for_embedding", fetch), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.get_redis_store", return_value=redis):
        plan = await kb.plan_reconcile("fl", api_base="http://api", sample_size=sample_size, run_id="r1")
    return plan, fetch


async def test_the_dry_run_counts_unrecorded_bills_and_estimates_cost_from_a_sample_and_writes_nothing():
    _, redis, pipe = _embedder()
    redis.versions[OCD] = {"schema": kb.CACHE_SCHEMA, "documents": {}}  # recorded: not counted
    bills = {OCD2: _sized_bill(OCD2, 4000), OCD3: _sized_bill(OCD3, 8000, diff_chars=400)}
    plan, fetch = await _plan(redis, everything=[OCD, OCD2, OCD3], bills=bills)
    assert (plan["listed"], plan["unrecorded"], plan["sampled"]) == (3, 2, 2) and plan["run_id"] == "r1"
    # mean of the sample is 6,000 characters and 1 document (the stored diff is not embedded, so it is not
    # counted); two unrecorded bills in all
    est = plan["estimate"]
    assert est["documents"] == 2 and est["tokens"] == round(12000 / 4)
    assert est["chunks"] == round(12000 * 0.279 / 1000) and est["usd"] == round(3000 / 1e6 * 0.13, 2)
    assert est["largest_sampled_bill_chars"] == 8000
    assert pipe.calls == [] and redis.set_calls == 0  # nothing embedded, nothing written
    assert redis.persist_expiring_seen is False  # not even the persistence repair
    assert sorted(c.args[0] for c in fetch.await_args_list) == sorted([OCD2, OCD3])  # only the unrecorded were read


async def test_the_dry_run_reads_only_a_sample_of_a_large_backlog():
    _, redis, _ = _embedder()
    ids = [f"0b1c2d3e-0000-0000-0000-0000000002{n:02d}" for n in range(30)]
    plan, fetch = await _plan(redis, everything=ids, bills={i: _sized_bill(i, 1000) for i in ids}, sample_size=5)
    assert plan["unrecorded"] == 30 and plan["sampled"] == 5 and fetch.await_count == 5
    assert plan["estimate"]["documents"] == 30  # one document each, extrapolated to all thirty


async def test_the_dry_run_reports_failures_instead_of_raising_or_guessing():
    _, redis, _ = _embedder()
    plan, _ = await _plan(redis, everything=[OCD], bills={}, listing=False)
    assert plan["status"] == "error" and plan["error"] == "listing_failed"
    redis.unreadable = True
    plan, _ = await _plan(redis, everything=[OCD], bills={})
    assert plan["status"] == "error" and plan["error"] == "records_unreadable" and "unrecorded" not in plan
    redis.is_available = False
    plan, _ = await _plan(redis, everything=[OCD], bills={})
    assert plan["error"] == "redis_unavailable"


async def test_deleting_one_bills_record_makes_the_next_pass_embed_exactly_that_bill():
    """SYNC-95's done-when: delete a record, run the pass, and only what is missing is embedded."""
    emb, redis, pipe = _embedder()
    everything = [OCD, OCD2, OCD3]
    bills = {i: _one_version_bill(i) for i in everything}
    await _run_reconcile(redis, emb, touched=[], everything=everything, bills=bills, cap=10)
    assert len(pipe.keys) == 3
    del redis.versions[OCD2]
    again, _, fetch = await _run_reconcile(redis, emb, touched=[], everything=everything, bills=bills, cap=10)
    assert again["reconcile"]["missing"] == 1 and again["reconcile"]["embedded"] == 1
    assert pipe.keys[3:] == [f"bill-text:{OCD2}:11"]  # one new document; the two recorded bills were not read
    assert [c.args[0] for c in fetch.await_args_list] == [OCD2]


async def test_find_unrecorded_bill_versions_with_persist_expiring_off_only_reads():
    from ddp_sync.services.redis_store import BILL_VERSION_PREFIX, RedisStore

    store = RedisStore()
    store._client = _PipeClient({f"{BILL_VERSION_PREFIX}a": 5000, f"{BILL_VERSION_PREFIX}b": -2})
    assert await store.find_unrecorded_bill_versions(["a", "b"], persist_expiring=False) == ["b"]
    assert store._client.persisted == []  # the expiring entry was left alone


# --- the reconcile-plan trigger route ---------------------------------------------------------------------


def _plan_route(path, *, settings=None, scheduler=None):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    settings = settings or SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base",
        local_openstates_api_base="http://local", local_openstates_api_key="lk",
        rds_openstates_api_base="http://rds", rds_openstates_api_key="rk",
    )
    sched = scheduler or MagicMock()
    if scheduler is None:
        sched._sync_config = {"openstates_archive": {"knowledge_base_embedding": {"jurisdictions": ["fl", "us"]}}}
    run = AsyncMock()
    with patch("ddp_sync.scheduler.get_scheduler", return_value=sched), \
         patch("ddp_sync.config.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.openstates_archive._mac_capable", return_value=True), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.plan_reconcile", new=run):
        resp = TestClient(app).post(path)
    return resp, run


async def test_the_reconcile_plan_route_starts_a_dry_run_with_the_mac_read_split_and_a_findable_run_id():
    resp, run = _plan_route("/trigger/knowledge-base-reconcile/FL")
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["dry_run"] is True and body["run_id"].startswith("FL-kb-reconcile-plan-")
    args, kwargs = run.await_args
    assert args == ("fl",) and kwargs["run_id"] == body["run_id"]
    assert (kwargs["api_base"], kwargs["api_key"]) == ("http://local", "lk")


async def test_the_reconcile_plan_route_refuses_what_it_cannot_run():
    resp, run = _plan_route("/trigger/knowledge-base-reconcile/ca")
    assert resp.status_code == 404 and "not enrolled" in resp.text
    resp, run = _plan_route("/trigger/knowledge-base-reconcile/fl", settings=SyncSettings())
    assert resp.status_code == 503
    resp, run = _plan_route("/trigger/knowledge-base-reconcile/fl", settings=SyncSettings(
        knowledge_base_index_name="votebot-large", local_openstates_api_base="http://x"))
    assert resp.status_code == 503 and "legacy index" in resp.text
    resp, run = _plan_route("/trigger/knowledge-base-reconcile/fl", settings=SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base", local_openstates_api_base=""))
    assert resp.status_code == 503 and "read path" in resp.text
    run.assert_not_awaited()


async def test_the_dry_run_says_when_its_sample_is_weak_instead_of_looking_fine():
    _, redis, _ = _embedder()
    ids = [OCD, OCD2, OCD3]
    # one readable bill, one that cannot be read, one whose data is malformed
    bills = {OCD: _sized_bill(OCD, 4000), OCD3: {"versions": "not-a-list-of-versions"}}
    with capture_logs() as logs:
        plan, _ = await _plan(redis, everything=ids, bills=bills)
    assert plan["status"] == "ok" and (plan["sampled"], plan["sample_failed"]) == (1, 2)
    assert plan["estimate"]["documents"] == 3  # the one usable bill's size, extrapolated to all three
    assert sum(e["event"] == "knowledge_base_reconcile_plan_sample_failed" for e in logs) == 2

    plan, _ = await _plan(redis, everything=ids, bills={})  # nothing could be read
    assert plan["status"] == "error" and plan["error"] == "sample_unreadable"
    assert (plan["sampled"], plan["sample_failed"], plan["unrecorded"]) == (0, 3, 3) and "estimate" not in plan


async def test_the_dry_run_marks_an_incomplete_listing_as_a_lower_bound():
    _, redis, _ = _embedder()
    list_mock = AsyncMock(return_value=([OCD], False))
    fetch = AsyncMock(return_value=_sized_bill(OCD, 1000))
    with patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.list_touched_bill_ids", list_mock), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.fetch_bill_for_embedding", fetch), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.get_redis_store", return_value=redis):
        plan = await kb.plan_reconcile("fl", api_base="http://api")
    assert plan["status"] == "incomplete" and plan["listing_complete"] is False
    assert "lower bounds" in plan["note"] and plan["unrecorded"] == 1 and "estimate" in plan


async def test_the_dry_run_with_nothing_unrecorded_reads_no_bill():
    _, redis, _ = _embedder()
    redis.versions[OCD] = {"schema": kb.CACHE_SCHEMA, "documents": {}}
    plan, fetch = await _plan(redis, everything=[OCD], bills={})
    assert plan["status"] == "ok" and plan["unrecorded"] == 0 and plan["sampled"] == 0
    assert fetch.await_count == 0 and "estimate" not in plan


async def test_the_dry_run_never_builds_an_embedder_or_writes_even_when_everything_fails():
    def boom(*a, **kw):
        raise AssertionError("the dry run must not embed")

    _, redis, _ = _embedder()
    with patch.object(kb, "KnowledgeBaseEmbedder", side_effect=boom), patch.object(kb, "IngestionPipeline", side_effect=boom):
        await _plan(redis, everything=[OCD], bills={}, listing=False)  # listing fails
        redis.unreadable = True
        await _plan(redis, everything=[OCD], bills={})  # records unreadable
        redis.unreadable = False
        await _plan(redis, everything=[OCD, OCD2], bills={})  # no sample can be read
    assert redis.set_calls == 0 and redis.versions == {} and redis.watermarks == {}


@pytest.mark.parametrize("name, version", [
    ("normal text and a stored diff", _version(11, "Sub", "amendment", 1, "t" * 500, diff="d" * 120)),
    ("text only, no diff", _version(11, "Sub", "amendment", 1, "t" * 500)),
    ("an empty diff is not a document", {**_version(11, "Sub", "amendment", 1, "t" * 500), "diff_from_previous_version": ""}),
    ("a diff on a stage-unknown version is skipped", _version(11, "Odd", "unknown", None, "t" * 500, diff="d" * 120, unknown=True)),
    ("no text yet", _version(11, "Sub", "amendment", 1, "")),
    ("text but no archived document id", {**_version(11, "Sub", "amendment", 1, "t" * 500), "archived_document_id": None}),
])
async def test_the_dry_runs_size_matches_what_embed_bill_really_writes(name, version):
    """The estimator repeats embed_bill's skip rules, so prove they agree on every one (and catch drift)."""
    emb, _, _ = _embedder()
    bill = _bill([version])
    stats = await emb.embed_bill(OCD, "fl", bill)
    documents, characters = kb._embeddable_size(bill)
    assert (documents, characters) == (stats["documents"], stats["chars"]), name
    assert stats["diffs"] == 0, name


async def test_the_reconcile_plan_route_needs_the_same_api_key_as_its_siblings():
    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    def requires_key(path):
        route = next(r for r in router.routes if getattr(r, "path", "") == path)
        return any(d.call is api_key_auth for d in route.dependant.dependencies)

    assert requires_key("/trigger/knowledge-base-reconcile/{jurisdiction}")
    assert requires_key("/trigger/knowledge-base-backfill/{jurisdiction}")  # the sibling it mirrors


# --- SYNC-95: orphaned vectors ---------------------------------------------------------------------------


def _vector_store():
    vs = MagicMock()
    vs.delete = AsyncMock()
    return vs


def _deleted_ids(vs):
    return [i for call in vs.delete.await_args_list for i in call.kwargs["ids"]]


def _recorded(**docs):
    """A cache entry that records documents by archived id: _recorded(**{"11": (3, 1)}) = 3 text chunks and 1 diff chunk."""
    return {"schema": kb.CACHE_SCHEMA, "documents": {
        aid: {"text_hash": "h", "chunks": c, "diff_hash": "d" if dc else None, "diff_chunks": dc}
        for aid, (c, dc) in docs.items()}}


async def test_an_orphaned_document_is_deleted_by_exact_id_and_dropped_from_the_record():
    """The picker re-chose row 13 for the version that was recorded as 11; 12 is untouched."""
    emb, redis, pipe = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (3, 1), "12": (1, 0)})
    redis.versions[OCD]["documents"]["12"]["text_hash"] = kb.content_hash("t" * 50)  # recorded as it is now
    vs = _vector_store()
    bill = _bill([_version(12, "Sub", "amendment", 1, "t" * 50), _version(13, "Intro", "introduced", 0, TEXT_A)])
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", bill, max_orphans=10)
    assert sorted(_deleted_ids(vs)) == sorted(
        [f"bill-text:{OCD}:11-chunk-{n}" for n in range(3)] + [f"bill-version-diff:{OCD}:11-chunk-0"])
    assert stats["orphans"] == 1 and stats["undone"] == []
    entry = redis.versions[OCD]["documents"]
    assert set(entry) == {"12", "13"} and "11" not in entry  # 13 was embedded; the orphan is gone
    assert f"bill-text:{OCD}:13" in pipe.keys and f"bill-text:{OCD}:12" not in pipe.keys  # 12 unchanged: not redone


async def test_orphan_removal_is_off_unless_asked():
    emb, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (3, 0)})
    vs = _vector_store()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", _bill([_version(13, "Intro", "introduced", 0, TEXT_A)]))
    assert vs.delete.await_count == 0 and stats["orphans"] == 0 and "11" in redis.versions[OCD]["documents"]


async def test_a_response_that_lists_no_archived_ids_removes_nothing():
    emb, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (3, 0)})
    vs = _vector_store()
    for versions in ([], [{"note": "Introduced", "date": "2026-01-01", "links": []}]):  # none, or none with an id
        with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
            stats = await emb.embed_bill(OCD, "fl", _bill(versions), max_orphans=10)
        assert stats["orphans"] == 0
    assert vs.delete.await_count == 0 and "11" in redis.versions[OCD]["documents"]


async def test_a_version_api_v3_still_lists_without_text_is_not_an_orphan():
    emb, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (3, 0)})
    vs = _vector_store()
    no_text_now = _version(11, "Introduced", "introduced", 0, "")  # id still listed, text momentarily absent
    has_text = _version(12, "Sub", "amendment", 1, TEXT_B)  # a second version, so the response is not "no ids at all"
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", _bill([no_text_now, has_text]), max_orphans=10)
    assert vs.delete.await_count == 0 and stats["orphans"] == 0 and "11" in redis.versions[OCD]["documents"]


async def test_a_failed_orphan_delete_keeps_the_record_so_the_next_run_retries():
    emb, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (3, 0)})
    vs = _vector_store()
    vs.delete = AsyncMock(side_effect=RuntimeError("pinecone down"))
    bill = _bill([_version(13, "Intro", "introduced", 0, TEXT_A)])
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", bill, max_orphans=10)
    assert stats["orphans"] == 0 and any("not removed" in u for u in stats["undone"])
    assert "11" in redis.versions[OCD]["documents"]  # still recorded; the cache was not advanced
    vs.delete = AsyncMock()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        retry = await emb.embed_bill(OCD, "fl", bill, max_orphans=10)
    assert retry["orphans"] == 1 and "11" not in redis.versions[OCD]["documents"]


async def test_the_circuit_breaker_stops_orphan_removal_after_too_many_in_one_run():
    emb, redis, _ = _embedder()
    ids = [f"0b1c2d3e-0000-0000-0000-0000000003{n:02d}" for n in range(5)]
    for i in ids:
        redis.versions[i] = _recorded(**{"11": (1, 0)})  # each bill recorded one document that api-v3 no longer lists
    bills = {i: _one_version_bill(i, doc_id=99) for i in ids}  # ...and now lists a different id
    vs = _vector_store()
    started = _STARTED
    list_mock = AsyncMock(return_value=(ids, True))
    fetch = AsyncMock(side_effect=lambda i, **kw: bills[i])
    with patch.object(kb, "MAX_ORPHAN_DOCUMENTS_PER_RUN", 2), capture_logs() as logs, \
         patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.list_touched_bill_ids", list_mock), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.local_openstates_client.fetch_bill_for_embedding", fetch), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.get_redis_store", return_value=redis), \
         patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        totals = await kb.embed_archived_bills("fl", started, settings=_settings(), api_base="http://api",
                                               embedder=emb, delete_orphans=True)
    assert totals["orphans"] == 2 and len(_deleted_ids(vs)) == 2  # it stopped at the cap
    assert totals["orphans_over_budget"] == 3  # the other three stay recorded
    assert all(i in redis.versions and "11" in redis.versions[i]["documents"] for i in ids[2:])
    paused = [e for e in logs if e["event"] == "knowledge_base_orphan_removal_paused"]
    assert len(paused) == 1 and paused[0]["cap"] == 2 and paused[0]["left_recorded"] == 3


def test_orphan_removal_needs_a_literal_true_in_yaml():
    from ddp_sync.pipelines.openstates_archive import _delete_orphans

    def cfg(value):
        return {"knowledge_base_embedding": {"delete_orphans": value}}

    assert _delete_orphans(cfg(True)) is True
    for odd in (False, "true", "yes", 1, None):
        assert _delete_orphans(cfg(odd)) is False, odd
    assert _delete_orphans(None) is False and _delete_orphans({}) is False


async def test_the_hook_passes_the_orphan_flag_and_the_yaml_ships_it_off():
    from pathlib import Path

    import yaml

    config = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"], "delete_orphans": True}}
    settings = SyncSettings(knowledge_base_index_name="ddp-knowledge-base",
                            rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=AsyncMock()) as run:
        await _maybe_embed_knowledge_base("fl", _STARTED, config)
    assert run.await_args.kwargs["delete_orphans"] is True
    cfg = yaml.safe_load((Path(__file__).parent.parent / "config" / "sync_schedule.yaml").read_text())["openstates_archive"]
    assert cfg["knowledge_base_embedding"]["delete_orphans"] is False


async def test_the_orphan_budget_is_a_hard_cap_even_inside_one_bill():
    emb, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (1, 0), "12": (1, 0), "13": (1, 0)})
    vs = _vector_store()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", _bill([_version(99, "Intro", "introduced", 0, TEXT_A)]), max_orphans=2)
    assert stats["orphans"] == 2 and stats["orphans_over_budget"] == 1 and len(_deleted_ids(vs)) == 2
    assert set(redis.versions[OCD]["documents"]) == {"13", "99"}  # one orphan left recorded, for a later run
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", _bill([_version(99, "Intro", "introduced", 0, TEXT_A)]), max_orphans=0)
    assert stats["orphans"] == 0 and stats["orphans_over_budget"] == 1  # a zero budget detects but never deletes


async def test_an_orphan_another_writer_changed_since_it_was_read_is_skipped_not_deleted():
    _, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (3, 0)})

    class _Concurrent(FakeRedis):
        async def get_bill_version(self, key):
            self.reads = getattr(self, "reads", 0) + 1
            if self.reads == 2:  # the re-read just before the delete: someone rewrote document 11 meanwhile
                self.versions[key]["documents"]["11"] = {"text_hash": "rewritten", "chunks": 5, "diff_chunks": 0}
            return self.versions.get(key)

    racing = _Concurrent()
    racing.versions = redis.versions
    emb2, _, _ = _embedder(redis=racing)
    vs = _vector_store()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb2.embed_bill(OCD, "fl", _bill([_version(13, "Intro", "introduced", 0, TEXT_A)]), max_orphans=10)
    assert vs.delete.await_count == 0 and stats["orphans"] == 0 and stats["raced"] == 1
    assert racing.versions[OCD]["documents"]["11"]["chunks"] == 5  # the other writer's entry survives


async def test_the_final_merge_keeps_an_entry_a_writer_changed_after_the_orphan_was_deleted():
    redis = FakeRedis()
    redis.versions[OCD] = _recorded(**{"11": (3, 0)})

    class _LateWriter(FakeRedis):
        async def get_bill_version(self, key):
            self.reads = getattr(self, "reads", 0) + 1
            if self.reads == 4:  # the merge's fresh read, after the delete (read 1 initial, 2 the new document's guard, 3 the orphan re-read)
                self.versions[key]["documents"]["11"] = {"text_hash": "late", "chunks": 7, "diff_chunks": 0}
            return self.versions.get(key)

    late = _LateWriter()
    late.versions = redis.versions
    emb, _, _ = _embedder(redis=late)
    vs = _vector_store()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", _bill([_version(13, "Intro", "introduced", 0, TEXT_A)]), max_orphans=10)
    assert stats["orphans"] == 1 and "13" in late.versions[OCD]["documents"]
    assert late.versions[OCD]["documents"]["11"]["chunks"] == 7  # not popped: it no longer matched what was deleted


async def test_orphan_removal_converges_after_each_partial_failure():
    """Text deleted then the diff delete fails; then the second of two orphans fails; then a cache write fails."""
    emb, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (2, 1), "12": (1, 0)})
    bill = _bill([_version(13, "Intro", "introduced", 0, TEXT_A)])
    deleted = []

    def store(fail_when):
        vs = MagicMock()

        async def delete(ids):
            if fail_when(ids):
                raise RuntimeError("pinecone down")
            deleted.extend(ids)

        vs.delete = delete
        return vs

    # 1: the text delete of 11 works, its diff delete fails -> nothing advances, both stay recorded
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=store(lambda ids: ids[0].startswith("bill-version-diff"))):
        first = await emb.embed_bill(OCD, "fl", bill, max_orphans=10)
    assert first["orphans"] >= 0 and first["undone"] and set(redis.versions[OCD]["documents"]) == {"11", "12"}
    # 2: now document 12's delete fails after 11 fully succeeded
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=store(lambda ids: ids[0].startswith(f"bill-text:{OCD}:12"))):
        second = await emb.embed_bill(OCD, "fl", bill, max_orphans=10)
    assert second["undone"] and set(redis.versions[OCD]["documents"]) == {"11", "12"}  # the cache was not advanced
    # 3: every delete works, but the cache write fails once: vectors are gone, the record still lists them
    real_set = redis.set_bill_version
    calls = {"n": 0}

    async def flaky_set(key, data, **kw):
        calls["n"] += 1
        return False if calls["n"] == 1 else await real_set(key, data, **kw)

    redis.set_bill_version = flaky_set
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=store(lambda ids: False)):
        third = await emb.embed_bill(OCD, "fl", bill, max_orphans=10)
        assert third["undone"] and "11" in redis.versions[OCD]["documents"]
        fourth = await emb.embed_bill(OCD, "fl", bill, max_orphans=10)  # a rerun deletes the (absent) ids again, and converges
    assert fourth["undone"] == [] and set(redis.versions[OCD]["documents"]) == {"13"}
    assert deleted  # Pinecone saw exact-id deletes throughout; deleting an absent id is a no-op there


async def test_an_archived_id_returned_as_a_number_matches_the_string_key_in_the_record():
    emb, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (1, 0)})
    redis.versions[OCD]["documents"]["11"]["text_hash"] = kb.content_hash(TEXT_A)
    vs = _vector_store()
    version = _version(11, "Intro", "introduced", 0, TEXT_A)
    assert isinstance(version["archived_document_id"], int)  # api-v3 sends the row's integer id
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", _bill([version]), max_orphans=10)
    assert stats["orphans"] == 0 and vs.delete.await_count == 0 and "11" in redis.versions[OCD]["documents"]


async def test_an_id_missing_from_a_non_empty_response_is_an_orphan_because_the_response_lists_every_version():
    """api-v3 returns every version of a bill in one detail response (no pagination), each with the id of the
    row its picker chose, so absence from a non-empty response is meaningful. This pins that contract."""
    emb, redis, _ = _embedder()
    redis.versions[OCD] = _recorded(**{"11": (1, 0), "12": (1, 0)})
    redis.versions[OCD]["documents"]["12"]["text_hash"] = kb.content_hash(TEXT_A)
    vs = _vector_store()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        stats = await emb.embed_bill(OCD, "fl", _bill([_version(12, "Intro", "introduced", 0, TEXT_A)]), max_orphans=10)
    assert stats["orphans"] == 1 and set(redis.versions[OCD]["documents"]) == {"12"}


# --- SYNC-95: the api-v3 ledger consumer (OPEN-319) ---------------------------------------------------------

U1, U2 = "2026-10-01T00:00:00+00:00", "2026-10-02T00:00:00+00:00"


def _stamped(version, updated_at):
    return {**version, "archived_updated_at": updated_at}


def _ledger_bill(ocd, *docs):
    """A bill whose versions are `docs` = (archived id, text, updated_at); its ledger entry is `_ledger_of`."""
    bill = _bill([_stamped(_version(i, f"V{i}", "introduced" if n == 0 else "amendment", n, t), u)
                  for n, (i, t, u) in enumerate(docs)])
    bill["id"] = f"ocd-bill/{ocd}"
    return bill


def _ledger_of(bills):
    return {b["id"].removeprefix("ocd-bill/"): {str(v["archived_document_id"]): v["archived_updated_at"]
                                                 for v in b["versions"]} for b in bills}


async def _run_ledger(redis, embedder, bills, *, cap=10, ledger="derive", complete=True, delete_orphans=False,
                      touched=()):
    by_id = {b["id"].removeprefix("ocd-bill/"): b for b in bills}
    listing = (_ledger_of(bills), complete) if ledger == "derive" else ledger
    touched_mock = AsyncMock(return_value=(list(touched), True))
    fetch = AsyncMock(side_effect=lambda i, **kw: by_id.get(i))
    c = "ddp_sync.pipelines.knowledge_base_embedding"
    with patch(f"{c}.local_openstates_client.list_embedding_ledger", AsyncMock(return_value=listing)), \
         patch(f"{c}.local_openstates_client.list_touched_bill_ids", touched_mock), \
         patch(f"{c}.local_openstates_client.fetch_bill_for_embedding", fetch), \
         patch(f"{c}.get_redis_store", return_value=redis):
        totals = await kb.embed_archived_bills(
            "fl", _STARTED, settings=_settings(), api_base="http://api", embedder=embedder,
            ledger_max_bills=cap, delete_orphans=delete_orphans,
        )
    return totals, fetch, touched_mock


async def test_embed_bill_stamps_the_source_row_and_a_changed_stamp_with_the_same_text_writes_only_the_stamp():
    emb, redis, pipe = _embedder()
    await emb.embed_bill(OCD, "fl", _ledger_bill(OCD, (11, TEXT_A, U1)))
    assert redis.versions[OCD]["documents"]["11"]["source_updated_at"] == U1
    writes = redis.set_calls
    await emb.embed_bill(OCD, "fl", _ledger_bill(OCD, (11, TEXT_A, U1)))
    assert redis.set_calls == writes  # nothing changed: nothing written
    pipe.calls.clear()
    stats = await emb.embed_bill(OCD, "fl", _ledger_bill(OCD, (11, TEXT_A, U2)))
    assert pipe.calls == [] and stats["documents"] == 0  # same text: no embedding spend
    assert redis.versions[OCD]["documents"]["11"]["source_updated_at"] == U2


async def test_a_stamp_is_never_written_for_text_that_is_not_recorded_in_sync():
    emb, redis, pipe = _embedder()
    redis.versions[OCD] = {"schema": kb.CACHE_SCHEMA, "documents": {"11": {"text_hash": "stale", "chunks": 1}}}
    pipe.fail_keys.add(f"bill-text:{OCD}:11")
    await emb.embed_bill(OCD, "fl", _ledger_bill(OCD, (11, TEXT_A, U1)))
    assert "source_updated_at" not in redis.versions[OCD]["documents"]["11"]


def test_ledger_findings_compare_documents_stamps_and_orphans():
    ok = {"schema": kb.CACHE_SCHEMA, "documents": {"11": {"text_hash": "h", "source_updated_at": U1}}}
    ledger = {"a": {"11": U1}, "b": {"11": U1, "12": U1}, "c": {"11": U2}, "d": {"11": U1},
              "e": {"11": None}, "f": {"11": U1}, "g": {"11": U1}}
    records = {"a": ok, "b": ok, "c": ok, "d": None,
               "e": {"schema": kb.CACHE_SCHEMA, "documents": {"11": {"text_hash": "h"}}},   # no stamp, ledger has none either
               "f": {"schema": kb.CACHE_SCHEMA, "documents": {"11": {"text_hash": "h"}}},   # unstamped, ledger has one
               "g": {"schema": 1, "documents": {"11": {"text_hash": "h", "source_updated_at": U1}}}}  # other schema
    found = kb.ledger_findings(ledger, records, delete_orphans=False)
    assert "a" not in found and "e" not in found  # in sync; an unknown source time is not a change
    assert found["b"] == {"missing": 1, "changed": 0, "orphaned": 0}
    assert found["c"] == {"missing": 0, "changed": 1, "orphaned": 0}
    assert found["d"] == {"missing": 1, "changed": 0, "orphaned": 0}  # no record: everything is missing
    assert found["f"] == {"missing": 0, "changed": 1, "orphaned": 0}  # one cheap check, then it is stamped
    assert found["g"] == {"missing": 1, "changed": 0, "orphaned": 0}
    extra = {"schema": kb.CACHE_SCHEMA, "documents": {"11": {"text_hash": "h", "source_updated_at": U1},
                                                      "99": {"text_hash": "h"}}}
    assert kb.ledger_findings({"a": {"11": U1}}, {"a": extra}, delete_orphans=False) == {}
    assert kb.ledger_findings({"a": {"11": U1}}, {"a": extra}, delete_orphans=True) == {
        "a": {"missing": 0, "changed": 0, "orphaned": 1}}


async def test_ledger_run_embeds_a_bill_nobody_touched_and_never_reads_the_watermark_path():
    emb, redis, pipe = _embedder()
    bills = [_ledger_bill(OCD, (11, TEXT_A, U1)), _ledger_bill(OCD2, (21, TEXT_B, U1))]
    totals, _, touched = await _run_ledger(redis, emb, bills)
    assert sorted(pipe.keys) == [f"bill-text:{OCD}:11", f"bill-text:{OCD2}:21"]
    assert touched.await_count == 0 and redis.watermarks == {}  # the watermark is neither read nor written
    assert totals["complete"] is True and totals["ledger"]["bills_to_check"] == 2 and totals["mode"] == "ledger"


async def test_deleting_one_documents_record_re_embeds_exactly_that_document():
    emb, redis, pipe = _embedder()
    bill = _ledger_bill(OCD, (11, TEXT_A, U1), (12, TEXT_B, U1))
    await _run_ledger(redis, emb, [bill])
    pipe.calls.clear()
    del redis.versions[OCD]["documents"]["12"]
    totals, _, _ = await _run_ledger(redis, emb, [bill])
    assert pipe.keys == [f"bill-text:{OCD}:12"]  # not 11, not the diff of anything else
    assert totals["ledger"]["missing_documents"] == 1


async def test_a_steady_state_run_reads_nothing_and_a_changed_row_costs_one_read_and_no_embedding():
    emb, redis, pipe = _embedder()
    bill = _ledger_bill(OCD, (11, TEXT_A, U1))
    await _run_ledger(redis, emb, [bill])
    pipe.calls.clear()
    totals, fetch, _ = await _run_ledger(redis, emb, [bill])
    assert fetch.await_count == 0 and pipe.calls == [] and totals["complete"] is True
    changed = _ledger_bill(OCD, (11, TEXT_A, U2))
    totals, fetch, _ = await _run_ledger(redis, emb, [changed])
    assert fetch.await_count == 1 and pipe.calls == [] and totals["ledger"]["changed_documents"] == 1
    totals, fetch, _ = await _run_ledger(redis, emb, [changed])
    assert fetch.await_count == 0  # stamped: quiet again


async def test_a_changed_text_under_a_changed_row_is_re_embedded():
    emb, redis, pipe = _embedder()
    await _run_ledger(redis, emb, [_ledger_bill(OCD, (11, TEXT_A, U1))])
    pipe.calls.clear()
    await _run_ledger(redis, emb, [_ledger_bill(OCD, (11, TEXT_B, U2))])
    assert pipe.keys == [f"bill-text:{OCD}:11"]


async def test_ledger_cap_attempts_a_slice_and_is_not_complete_until_the_rest_are_reached():
    emb, redis, pipe = _embedder()
    bills = [_ledger_bill(i, (11, TEXT_A, U1)) for i in (OCD, OCD2, OCD3)]
    with capture_logs() as logs:
        first, _, _ = await _run_ledger(redis, emb, bills, cap=2)
    assert len(pipe.keys) == 2 and first["complete"] is False
    assert any(e["event"] == "knowledge_base_reconcile_backlog" and e["remaining"] == 1 for e in logs)
    second, _, _ = await _run_ledger(redis, emb, bills, cap=2)
    assert len(set(pipe.keys)) == 3 and second["complete"] is True


async def test_a_bill_that_fails_every_time_does_not_starve_the_others_and_blocks_complete():
    emb, redis, pipe = _embedder()
    pipe.fail_keys.add(f"bill-text:{OCD}:11")
    bills = [_ledger_bill(OCD, (11, TEXT_A, U1)), _ledger_bill(OCD2, (21, TEXT_A, U1))]
    for _ in range(12):  # a random slice of 1: the healthy bill is reached with near certainty
        totals, _, _ = await _run_ledger(redis, emb, bills, cap=1)
    assert f"bill-text:{OCD2}:21" in pipe.keys
    assert totals["complete"] is False


async def test_a_partial_ledger_is_used_but_never_complete():
    emb, redis, pipe = _embedder()
    bills = [_ledger_bill(OCD, (11, TEXT_A, U1))]
    totals, _, _ = await _run_ledger(redis, emb, bills, complete=False)
    assert pipe.keys == [f"bill-text:{OCD}:11"] and totals["complete"] is False


async def test_without_a_ledger_or_records_the_run_falls_back_to_the_watermark_path():
    emb, redis, pipe = _embedder()
    bill = _one_version_bill(OCD)
    with capture_logs() as logs:
        totals, _, touched = await _run_ledger(redis, emb, [bill], ledger=None, touched=[OCD])
    assert touched.await_count == 1 and pipe.keys == [f"bill-text:{OCD}:11"] and redis.watermarks
    assert any(e["event"] == "knowledge_base_ledger_unavailable" for e in logs)
    assert totals["mode"] == "watermark_fallback"
    redis.watermarks.clear()
    redis.unreadable_records = True  # an outage is not "everything is missing"
    pipe.calls.clear()
    redis.versions.clear()
    _, _, touched = await _run_ledger(redis, emb, [_ledger_bill(OCD2, (21, TEXT_A, U1))])
    assert pipe.keys == [] and touched.await_count == 1


async def test_ledger_run_removes_orphans_only_when_asked_and_within_the_budget():
    emb, redis, _ = _embedder()
    bill = _ledger_bill(OCD, (12, "t" * 50, U1))
    redis.versions[OCD] = _recorded(**{"11": (3, 0)})
    redis.versions[OCD]["documents"]["11"]["text_hash"] = "old"
    vs = _vector_store()
    with patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        await _run_ledger(redis, emb, [bill])
        assert "11" in redis.versions[OCD]["documents"] and vs.delete.await_count == 0  # embedded 12; 11 left alone
        on, _, _ = await _run_ledger(redis, emb, [bill], delete_orphans=True)
    assert on["orphans"] == 1 and "11" not in redis.versions[OCD]["documents"]
    assert sorted(_deleted_ids(vs)) == sorted(f"bill-text:{OCD}:11-chunk-{n}" for n in range(3))


async def test_ledger_run_is_off_at_zero_and_leaves_the_watermark_path_in_force():
    emb, redis, _ = _embedder()
    totals, _, touched = await _run_ledger(redis, emb, [_ledger_bill(OCD, (11, TEXT_A, U1))], cap=0, touched=[OCD])
    assert touched.await_count == 1 and "ledger" not in totals


async def test_list_embedding_ledger_pages_by_cursor_and_reads_nothing_it_cannot_vouch_for():
    from ddp_sync.services import local_openstates_client as c

    page = lambda ids, nxt: {"results": [{"ocd_bill_id": i, "session": "2026", "documents": [
        {"archived_document_id": 7, "updated_at": U1}, {"archived_document_id": 8, "updated_at": None}]} for i in ids],
        "next_after": nxt}
    get = AsyncMock(side_effect=[page(["a", "b"], "ocd-bill/b"), page(["c"], None)])
    with patch.object(c, "_get_json_with_retry", get):
        out = await c.list_embedding_ledger("FL", api_base="http://api", api_key="k")
    assert out == ({"a": {"7": U1, "8": None}, "b": {"7": U1, "8": None}, "c": {"7": U1, "8": None}}, True)
    first, second = (call.args[1] for call in get.await_args_list)
    assert first["jurisdiction"] == "fl" and "after" not in first and second["after"] == "ocd-bill/b"
    with patch.object(c, "_get_json_with_retry", AsyncMock(side_effect=[page(["a"], "x"), None])):
        assert await c.list_embedding_ledger("fl", api_base="x") == ({"a": {"7": U1, "8": None}}, False)
    with patch.object(c, "_get_json_with_retry", AsyncMock(return_value=None)):  # an api-v3 without the endpoint
        assert await c.list_embedding_ledger("fl", api_base="x") is None
    bad = {"results": [{"ocd_bill_id": "a", "documents": [{"updated_at": U1}]}], "next_after": None}
    with patch.object(c, "_get_json_with_retry", AsyncMock(return_value=bad)):
        assert await c.list_embedding_ledger("fl", api_base="x") is None  # a malformed entry: nothing vouched for
    assert await c.list_embedding_ledger("fl", api_base="") is None


async def test_redis_store_get_bill_versions_reads_many_and_is_unknown_on_failure():
    from ddp_sync.services.redis_store import BILL_VERSION_PREFIX, RedisStore

    store = RedisStore()
    assert await store.get_bill_versions(["a"]) is None

    class _Client:
        def __init__(self, raws, boom=False):
            self.raws, self.boom = raws, boom

        def pipeline(self, transaction=False):
            client = self

            class _Pipe:
                async def __aenter__(self):
                    self.keys = []
                    return self

                async def __aexit__(self, *exc):
                    return False

                def get(self, key):
                    self.keys.append(key)

                async def execute(self):
                    if client.boom:
                        raise RuntimeError("down")
                    return [client.raws.get(k) for k in self.keys]

            return _Pipe()

    store._client = _Client({f"{BILL_VERSION_PREFIX}a": '{"schema": 2}', f"{BILL_VERSION_PREFIX}c": "not json",
                             f"{BILL_VERSION_PREFIX}d": "[1]"})
    assert await store.get_bill_versions(["a", "b", "c", "d"]) == {"a": {"schema": 2}, "b": None, "c": None, "d": None}
    store._client = _Client({}, boom=True)
    assert await store.get_bill_versions(["a"]) is None


async def test_the_ledger_plan_counts_what_disagrees_and_writes_nothing():
    _, redis, pipe = _embedder()
    bills = [_ledger_bill(OCD, (11, TEXT_A, U1)), _ledger_bill(OCD2, (21, TEXT_B, U1))]
    redis.versions[OCD] = {"schema": kb.CACHE_SCHEMA, "documents": {"11": {"text_hash": "h", "source_updated_at": U1}}}
    by_id = {b["id"].removeprefix("ocd-bill/"): b for b in bills}
    c = "ddp_sync.pipelines.knowledge_base_embedding"
    with patch(f"{c}.local_openstates_client.list_embedding_ledger", AsyncMock(return_value=(_ledger_of(bills), True))), \
         patch(f"{c}.local_openstates_client.fetch_bill_for_embedding", AsyncMock(side_effect=lambda i, **kw: by_id[i])), \
         patch(f"{c}.get_redis_store", return_value=redis):
        plan = await kb.plan_reconcile("fl", api_base="http://api", use_ledger=True)
        assert (plan["status"], plan["unrecorded"], plan["missing_documents"]) == ("ok", 1, 1)
        assert plan["changed_documents"] == 0 and plan["estimate"]["documents"] == 1
        assert redis.persist_expiring_seen is False and pipe.calls == [] and redis.set_calls == 0
    with patch(f"{c}.local_openstates_client.list_embedding_ledger", AsyncMock(return_value=None)), \
         patch(f"{c}.get_redis_store", return_value=redis):
        assert (await kb.plan_reconcile("fl", api_base="x", use_ledger=True))["error"] == "ledger_unavailable"


async def test_the_ledger_cap_comes_from_yaml_the_hook_passes_it_and_the_yaml_ships_no_cap():
    from pathlib import Path

    import yaml

    from ddp_sync.pipelines.openstates_archive import _ledger_max_bills

    def cfg(v):
        return {"knowledge_base_embedding": {"ledger": {"max_bills_per_run": v}}}

    assert _ledger_max_bills(None) == 0 and _ledger_max_bills({"knowledge_base_embedding": {}}) == 0
    assert _ledger_max_bills(cfg(40)) == 40
    for odd in ("many", -1, True, 1.5, None):
        assert _ledger_max_bills(cfg(odd)) == 0, odd
    config = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"], **cfg(40)["knowledge_base_embedding"]}}
    settings = SyncSettings(knowledge_base_index_name="ddp-knowledge-base",
                            rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=AsyncMock()) as run:
        await _maybe_embed_knowledge_base("fl", _STARTED, config)
    assert run.await_args.kwargs["ledger_max_bills"] == 40
    shipped = yaml.safe_load((Path(__file__).parent.parent / "config" / "sync_schedule.yaml").read_text())["openstates_archive"]
    assert shipped["knowledge_base_embedding"]["ledger"]["max_bills_per_run"] == 1_000_000  # "no cap", set 2026-10-05


async def test_a_stamp_is_not_advanced_when_anything_else_in_the_bill_was_left_undone():
    """The record is written only when every Pinecone write for the bill succeeded, so a stamp can never get
    ahead of a failed one: the next run still finds the bill disagreeing."""
    emb, redis, pipe = _embedder()
    bill = _ledger_bill(OCD, (11, TEXT_A, U1), (12, TEXT_B, U1))
    await _run_ledger(redis, emb, [bill])
    pipe.calls.clear()
    changed = _ledger_bill(OCD, (11, TEXT_A, U2), (12, TEXT_C, U2))
    pipe.fail_keys.add(f"bill-text:{OCD}:12")
    totals, _, _ = await _run_ledger(redis, emb, [changed])
    assert totals["complete"] is False and totals["failed_bills"] == 1
    assert {d["source_updated_at"] for d in redis.versions[OCD]["documents"].values()} == {U1}  # nothing advanced
    pipe.fail_keys.clear()
    totals, _, _ = await _run_ledger(redis, emb, [changed])
    assert totals["complete"] is True
    assert {d["source_updated_at"] for d in redis.versions[OCD]["documents"].values()} == {U2}


# --- SYNC-95: the backlog alert --------------------------------------------------------------------------


def test_the_alert_threshold_comes_from_yaml_and_anything_odd_means_off():
    from ddp_sync.pipelines.openstates_archive import _alert_backlog_over

    def cfg(v):
        return {"knowledge_base_embedding": {"alert_backlog_over": v}}

    assert _alert_backlog_over(None) == 0 and _alert_backlog_over({"knowledge_base_embedding": {}}) == 0
    assert _alert_backlog_over(cfg(50)) == 50
    for odd in ("lots", -1, True, 2.5, None):
        assert _alert_backlog_over(cfg(odd)) == 0, odd


def test_the_backlog_alert_text_for_a_fallback_a_backlog_and_a_healthy_run():
    from ddp_sync.pipelines.openstates_archive import _backlog_alert_text

    ledger = lambda check, selected, failed: {"mode": "ledger", "ledger": {
        "bills_to_check": check, "selected": selected, "failed": failed}}
    assert _backlog_alert_text("fl", {"mode": "watermark_fallback"}, 0) is None  # off means off
    assert "could not read api-v3's ledger" in _backlog_alert_text("fl", {"mode": "watermark_fallback"}, 10)
    assert _backlog_alert_text("fl", {"mode": "watermark"}, 10) is None  # the ledger was never asked for
    assert _backlog_alert_text("fl", ledger(100, 100, 0), 10) is None  # everything reached
    assert _backlog_alert_text("fl", ledger(100, 95, 3), 10) is None  # 5 over the cap + 3 failed = 8, not over 10
    text = _backlog_alert_text("fl", ledger(100, 90, 4), 10)  # 10 + 4 = 14
    assert "14 bills" in text and "4 failed" in text and "10 over the per-run cap" in text


async def test_the_hook_posts_the_alert_only_when_there_is_one_and_never_raises():
    config = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"], "alert_backlog_over": 5}}
    settings = SyncSettings(knowledge_base_index_name="ddp-knowledge-base",
                            rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    base = "ddp_sync.pipelines.openstates_archive"
    with patch(f"{base}.get_settings", return_value=settings), \
         patch(f"{base}._post_slack_alert") as post, \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills",
               new=AsyncMock(return_value={"mode": "watermark_fallback"})):
        await _maybe_embed_knowledge_base("fl", _STARTED, config)
        assert post.call_count == 1 and "fl" in post.call_args.args[0]
    with patch(f"{base}.get_settings", return_value=settings), \
         patch(f"{base}._post_slack_alert") as post, \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills",
               new=AsyncMock(return_value={"mode": "ledger", "ledger": {"bills_to_check": 0, "selected": 0, "failed": 0}})):
        await _maybe_embed_knowledge_base("fl", _STARTED, config)
        post.assert_not_called()


def test_the_slack_post_never_raises_and_the_yaml_ships_the_alert_off(monkeypatch):
    from pathlib import Path

    import yaml

    from ddp_sync.pipelines import openstates_archive as oa

    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    oa._post_slack_alert("x")  # no token: logged, not raised
    monkeypatch.setenv("SLACK_BOT_TOKEN", "t")
    with patch.object(oa.requests, "post", side_effect=RuntimeError("net")):
        oa._post_slack_alert("x")
    with patch.object(oa.requests, "post") as post:
        post.return_value.ok, post.return_value.json.return_value = True, {"ok": True}
        oa._post_slack_alert("hello")
    assert post.call_args.kwargs["json"]["text"] == ":warning: hello"
    shipped = yaml.safe_load((Path(__file__).parent.parent / "config" / "sync_schedule.yaml").read_text())["openstates_archive"]
    assert shipped["knowledge_base_embedding"]["alert_backlog_over"] == 0


async def test_a_malformed_totals_dict_cannot_raise_into_the_hook_and_the_threshold_is_exclusive():
    from ddp_sync.pipelines.openstates_archive import _backlog_alert_text

    config = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"], "alert_backlog_over": 5}}
    settings = SyncSettings(knowledge_base_index_name="ddp-knowledge-base",
                            rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    base = "ddp_sync.pipelines.openstates_archive"
    for bad in ({"mode": "ledger", "ledger": {"bills_to_check": 3}},
                {"mode": "ledger", "ledger": {"bills_to_check": None, "selected": 1, "failed": 0}},
                {"mode": "ledger", "ledger": {"bills_to_check": "9", "selected": 1, "failed": 0}}):
        with patch(f"{base}.get_settings", return_value=settings), \
             patch(f"{base}._post_slack_alert") as post, \
             patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=AsyncMock(return_value=bad)), \
             capture_logs() as logs:
            await _maybe_embed_knowledge_base("fl", _STARTED, config)  # does not raise
        post.assert_not_called()
        assert any(e["event"] == "knowledge_base_alert_error" for e in logs)
    ledger = lambda check, selected, failed: {"ledger": {"bills_to_check": check, "selected": selected, "failed": failed}}
    assert _backlog_alert_text("fl", ledger(15, 10, 0), 5) is None  # exactly the threshold: no alert
    assert _backlog_alert_text("fl", ledger(16, 10, 0), 5) is not None
    assert _backlog_alert_text("fl", ledger(14, 10, 2), 5) is not None  # 4 over the cap + 2 failed = 6
