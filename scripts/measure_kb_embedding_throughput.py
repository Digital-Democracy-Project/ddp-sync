#!/usr/bin/env python
"""SYNC-89: measure real embedding throughput into the NEW knowledge-base index.

Embeds N archived bill-version documents (stratified across jurisdictions, read from an
OpenStates Postgres) through `IngestionPipeline.ingest_document` into the index named by
KNOWLEDGE_BASE_INDEX_NAME, prints/writes the measurements, and (with --cleanup) deletes exactly
the vectors it wrote, by id list. It refuses to run against the legacy index.

Required env: OPENSTATES_MEASURE_DSN (read-only use), OPENAI_API_KEY, PINECONE_API_KEY,
KNOWLEDGE_BASE_INDEX_NAME. Never prints keys. Does not touch Redis or send alerts. Builds its
settings from the environment on purpose instead of calling get_settings(), which would try AWS
Secrets Manager first: a measurement run must not depend on (or pick up) a host's credentials.

Refuses to start unless the target index is empty, because --cleanup deletes the ids this run
wrote and those ids are deterministic (an existing vector with the same id would be overwritten
and then deleted). The full list of ids is written to --ids-file before anything can go wrong.

    OPENSTATES_MEASURE_DSN=postgresql://... KNOWLEDGE_BASE_INDEX_NAME=ddp-knowledge-base \
      python scripts/measure_kb_embedding_throughput.py --docs 1000 --sequential 300 \
        --concurrency 4 --cleanup --out results.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time

import asyncpg
import httpx
from openai import AsyncOpenAI

from ddp_sync.config import SyncSettings, knowledge_base_settings
from ddp_sync.ingestion.metadata import DocumentMetadata
from ddp_sync.ingestion.pipeline import IngestionPipeline

PRICE_PER_MILLION_TOKENS = 0.13  # text-embedding-3-large list price; verify before relying on it

SAMPLE_SQL = """
SELECT id, bill_id, version_note, version_date, raw_text, jurisdiction FROM (
  SELECT d.id, d.bill_id, d.version_note, d.version_date, d.raw_text, j.name AS jurisdiction,
         row_number() OVER (PARTITION BY j.name ORDER BY md5(d.id::text || $2)) AS rn
  FROM ddp_bill_version_document d
  JOIN opencivicdata_bill b ON b.id = d.bill_id
  JOIN opencivicdata_legislativesession s ON s.id = b.legislative_session_id
  JOIN opencivicdata_jurisdiction j ON j.id = s.jurisdiction_id
  WHERE NOT d.is_error AND length(d.raw_text) > 200
) t WHERE rn <= $1
"""


async def load_sample(dsn: str, per_jurisdiction: int, seed: str) -> list[dict]:
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction(readonly=True):
            rows = await conn.fetch(SAMPLE_SQL, per_jurisdiction, seed)
            stats = await conn.fetchrow(
                "SELECT count(*) AS n, avg(length(raw_text)) AS avg_chars FROM ddp_bill_version_document"
            )
    finally:
        await conn.close()
    return [dict(r) for r in rows], dict(stats)


class Counters:
    def __init__(self) -> None:
        self.embed_calls = 0
        self.embed_seconds = 0.0
        self.tokens = 0
        self.http_429 = 0
        self.upsert_calls = 0
        self.upsert_seconds = 0.0
        self.metadata_bytes = 0
        self.vectors = 0


def instrument(pipeline: IngestionPipeline, settings: SyncSettings, c: Counters) -> None:
    """Wrap the real embed/upsert calls with counters; behaviour is unchanged. A 429 hook on the
    HTTP client counts every rate-limit response, including the ones the OpenAI SDK retries
    internally (which never surface as exceptions)."""

    async def on_response(resp: httpx.Response) -> None:
        if resp.status_code == 429:
            c.http_429 += 1

    svc = pipeline.vector_store.embedding_service
    svc.client = AsyncOpenAI(
        api_key=settings.openai_api_key,
        http_client=httpx.AsyncClient(event_hooks={"response": [on_response]}, timeout=600),
    )
    orig_create = svc.client.embeddings.create

    async def create(*a, **kw):
        t = time.monotonic()
        resp = await orig_create(*a, **kw)
        c.embed_calls += 1
        c.embed_seconds += time.monotonic() - t
        c.tokens += resp.usage.total_tokens if resp.usage else 0
        return resp

    svc.client.embeddings.create = create

    index = pipeline.vector_store.index
    orig_upsert = index.upsert

    def upsert(*, vectors, namespace=None, **kw):
        t = time.monotonic()
        out = orig_upsert(vectors=vectors, namespace=namespace, **kw)
        c.upsert_calls += 1
        c.upsert_seconds += time.monotonic() - t
        c.vectors += len(vectors)
        c.metadata_bytes += sum(len(json.dumps(v["metadata"])) for v in vectors)
        return out

    index.upsert = upsert


async def run_phase(pipeline, rows, concurrency: int, results: list[dict]) -> dict:
    sem = asyncio.Semaphore(concurrency)
    t0 = time.monotonic()

    async def one(row: dict) -> None:
        ocd = row["bill_id"].removeprefix("ocd-bill/")
        doc_id = f"bill-text:{ocd}:{row['id']}"
        meta = DocumentMetadata(
            document_id=doc_id,
            document_type="bill-text",
            source="sync-89-throughput-measurement",
            jurisdiction=row["jurisdiction"],
            extra={"ocd_bill_id": ocd, "document_id": str(row["id"])},
        )
        async with sem:
            t = time.monotonic()
            try:
                r = await pipeline.ingest_document(row["raw_text"], meta, skip_duplicates=False)
                err = r.errors
                chunks = r.chunks_upserted
            except Exception as e:  # noqa: BLE001 -- a measurement must record, not crash
                err, chunks = [f"{type(e).__name__}: {e}"], 0
            results.append(
                {
                    "document_id": doc_id,
                    "jurisdiction": row["jurisdiction"],
                    "chars": len(row["raw_text"]),
                    "chunks": chunks,
                    "seconds": round(time.monotonic() - t, 3),
                    "errors": err,
                }
            )

    await asyncio.gather(*[one(r) for r in rows])
    return {"docs": len(rows), "seconds": time.monotonic() - t0}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=1000)
    ap.add_argument("--sequential", type=int, default=300, help="docs in the first, sequential phase")
    ap.add_argument("--concurrency", type=int, default=4, help="concurrency of the second phase")
    ap.add_argument("--seed", default="sync-89")
    ap.add_argument("--cleanup", action="store_true")
    ap.add_argument("--ids-file", default="sync89_written_ids.json")
    ap.add_argument("--out", default="sync89_results.json")
    args = ap.parse_args()

    settings = SyncSettings(
        openai_api_key=os.environ["OPENAI_API_KEY"],
        pinecone_api_key=os.environ["PINECONE_API_KEY"],
        knowledge_base_index_name=os.environ["KNOWLEDGE_BASE_INDEX_NAME"],
    )
    kb = knowledge_base_settings(settings)  # raises if unset or equal to the legacy index
    if kb.pinecone_index_name == "votebot-large":
        raise SystemExit("refusing to run against votebot-large")

    per_j = -(-args.docs // 10) + 20
    sample, pop = await load_sample(os.environ["OPENSTATES_MEASURE_DSN"], per_j, args.seed)
    sample.sort(key=lambda r: __import__("hashlib").md5(f"{args.seed}{r['id']}".encode()).hexdigest())
    sample = sample[: args.docs]
    print(f"sample: {len(sample)} docs; avg chars {statistics.mean(len(r['raw_text']) for r in sample):.0f} "
          f"(population avg {float(pop['avg_chars']):.0f})")

    pipeline = IngestionPipeline(kb)
    c = Counters()
    instrument(pipeline, kb, c)
    idx = pipeline.vector_store.index
    before = idx.describe_index_stats().total_vector_count
    if before:
        raise SystemExit(f"{kb.pinecone_index_name} already holds {before} vectors; refusing to measure")

    results: list[dict] = []
    phases = {}
    seq_rows, conc_rows = sample[: args.sequential], sample[args.sequential :]
    for name, rows, conc in (("sequential", seq_rows, 1), ("concurrent", conc_rows, args.concurrency)):
        if not rows:
            continue
        snap = (c.tokens, c.http_429, c.vectors, c.embed_seconds, c.upsert_seconds)
        start = len(results)
        p = await run_phase(pipeline, rows, conc, results)
        p.update(
            concurrency=conc,
            tokens=c.tokens - snap[0],
            http_429=c.http_429 - snap[1],
            vectors=c.vectors - snap[2],
            embed_seconds=round(c.embed_seconds - snap[3], 1),
            upsert_seconds=round(c.upsert_seconds - snap[4], 1),
            failures=sum(1 for r in results[start:] if r["errors"]),
        )
        p["docs_per_min"] = round(p["docs"] / p["seconds"] * 60, 1)
        p["tokens_per_min"] = round(p["tokens"] / p["seconds"] * 60)
        p["seconds"] = round(p["seconds"], 1)
        phases[name] = p
        print(name, p)

    # Record every id that may have been written BEFORE anything else can go wrong.
    ids = [f"{r['document_id']}-chunk-{i}" for r in results for i in range(r["chunks"])]
    with open(args.ids_file, "w") as f:  # noqa: ASYNC230 -- one-shot script
        json.dump(ids, f)

    await asyncio.sleep(15)  # serverless stats are eventually consistent
    after = idx.describe_index_stats().total_vector_count
    chunks = [r["chunks"] for r in results]
    summary = {
        "index": kb.pinecone_index_name,
        "docs": len(results),
        "failures": sum(1 for r in results if r["errors"]),
        "chunks_total": sum(chunks),
        "chunks_per_doc_mean": round(statistics.mean(chunks), 2),
        "chunks_per_doc_median": statistics.median(chunks),
        "chunks_per_doc_max": max(chunks),
        "chars_per_doc_mean": round(statistics.mean(r["chars"] for r in results)),
        "population_avg_chars": round(float(pop["avg_chars"])),
        "tokens_total": c.tokens,
        "http_429": c.http_429,
        "cost_usd": round(c.tokens / 1e6 * PRICE_PER_MILLION_TOKENS, 4),
        "index_vectors_before": before,
        "index_vectors_after": after,
        "index_growth": after - before,
        "avg_metadata_bytes_per_vector": round(c.metadata_bytes / max(c.vectors, 1)),
        "raw_vector_bytes": 3072 * 4,
        "embed_calls": c.embed_calls,
        "upsert_calls": c.upsert_calls,
        "phases": phases,
        "by_jurisdiction_docs": {},
    }
    for r in results:
        summary["by_jurisdiction_docs"][r["jurisdiction"]] = summary["by_jurisdiction_docs"].get(r["jurisdiction"], 0) + 1
    with open(args.out, "w") as f:  # noqa: ASYNC230 -- one-shot script
        json.dump({"summary": summary, "docs": results}, f)
    print(json.dumps(summary, indent=1))

    if args.cleanup:
        vs = pipeline.vector_store
        for i in range(0, len(ids), 500):
            await vs.delete(ids=ids[i : i + 500])
        await asyncio.sleep(15)
        print("vectors after cleanup:", idx.describe_index_stats().total_vector_count)


if __name__ == "__main__":
    asyncio.run(main())
