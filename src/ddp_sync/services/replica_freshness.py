"""OPEN-275: the pre-dispatch freshness check (ddp-infra
PLAN-rds-local-postgres-replication.md §5, §7.7) -- confirms LegBot's dispatch would read the
SAME bill-version-document content through the Mac's local Postgres replica that RDS itself
currently holds, before dispatch proceeds. This is the §5 "option 2" resolution of the SYNC-59
interlock: SYNC-59's own trigger condition stays "RDS load complete" unchanged; this check is
what actually confirms the local replica has caught up to that same point, immediately before
each bill dispatch, rather than SYNC-59 itself gating on replica lag.

Content match is the authoritative signal, not row presence/non-null alone (plan §7.7,
correction from v0.4/pm-review round 3): this project's own os-text-extract reextract/
refresh-extraction tooling updates raw_text on an EXISTING row in place without changing its
natural key, so a stale local row can pass a presence-only check while carrying superseded text.

Fails closed throughout: any error resolving the RDS credential, connecting, querying, or
reading the local replica is treated identically to "not fresh" -- never "proceed anyway."
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import asyncpg
import structlog

from ddp_sync.services.local_openstates_client import get_bill_version_document_text
from ddp_sync.services.rds_credentials import resolve_rds_database_url

logger = structlog.get_logger()

# Bounded so a hung/unreachable RDS blocks one bill's dispatch, not the whole batch run
# indefinitely -- matches this plan's other RDS-facing scripts' own use of a short, explicit
# timeout (ops/postgres-replica's statement_timeout convention) rather than an unbounded wait.
_RDS_QUERY_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class FreshnessResult:
    """is_fresh=True means: the local replica's raw_text for this bill's current RDS-side latest
    version-document row matches RDS's content exactly. Any other outcome is is_fresh=False,
    with `reason` describing which of the fail-closed cases applied -- callers should treat every
    is_fresh=False identically (skip/defer this bill for this pass, per plan §5/§7.7), not branch
    on `reason`; it exists for logging, not control flow.
    """

    is_fresh: bool
    reason: str


async def check_bill_version_freshness(bill_openstates_id: str) -> FreshnessResult:
    """The AC5 freshness check itself (plan §4, §7.7).

    Args:
        bill_openstates_id: bare UUID (no "ocd-bill/" prefix), matching every other function in
            local_openstates_client.py's convention -- callers already have this on hand.

    Steps (plan §7.7, run in this order every time -- no LSN pre-check short-circuit; per the
    plan's own v0.3 correction, that pre-check is not itself authoritative, and skipping straight
    to the authoritative content comparison is simpler and not meaningfully more expensive for a
    per-bill, not per-batch, check):
    1. Resolve the current RDS-side latest version-document row for this bill (natural key +
       content hash) via the dispatch-time RDS credential (plan §3.4 item 3) -- never
       `ddp_local_replication`, which is reserved for the subscription itself.
    2. Look up that EXACT natural key (not just "whatever local thinks is latest") on the local
       replica, via the existing api-v3-mediated read path.
    3. Compare content hashes.
    """
    rds_url, rds_error = resolve_rds_database_url()
    if rds_url is None:
        logger.warning(
            "replica_freshness: RDS credential unavailable -- failing closed",
            bill_openstates_id=bill_openstates_id,
            error=rds_error,
        )
        return FreshnessResult(False, f"rds_credential_unavailable: {rds_error}")

    bill_id = f"ocd-bill/{bill_openstates_id}"

    try:
        conn = await asyncpg.connect(rds_url, timeout=_RDS_QUERY_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 -- any connection failure fails closed identically
        logger.warning(
            "replica_freshness: could not connect to RDS -- failing closed",
            bill_openstates_id=bill_openstates_id,
            error=str(exc),
        )
        return FreshnessResult(False, f"rds_connection_failed: {exc}")

    try:
        row = await conn.fetchrow(
            """
            SELECT version_note, version_date, source_url, md5(raw_text) AS raw_text_hash
            FROM ddp_bill_version_document
            WHERE bill_id = $1
            ORDER BY version_date DESC
            LIMIT 1
            """,
            bill_id,
            timeout=_RDS_QUERY_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001 -- a query timeout/error is "not ready", not a crash
        logger.warning(
            "replica_freshness: RDS query failed -- failing closed",
            bill_openstates_id=bill_openstates_id,
            error=str(exc),
        )
        return FreshnessResult(False, f"rds_query_failed: {exc}")
    finally:
        await conn.close()

    if row is None:
        # Not itself a staleness signal -- this bill may simply not be RDS-fed at all yet
        # (§3.6's separate allowlist gating is the primary guard for that case). Still fails
        # closed here regardless, since dispatch has no source of truth to compare against.
        return FreshnessResult(False, "no_version_document_row_on_rds")

    local_text = await get_bill_version_document_text(
        bill_openstates_id,
        version_note=row["version_note"],
        version_date=row["version_date"],
        source_url=row["source_url"],
    )
    if local_text is None:
        return FreshnessResult(False, "row_not_yet_replicated_locally")

    local_hash = hashlib.md5(local_text.encode("utf-8")).hexdigest()
    if local_hash != row["raw_text_hash"]:
        return FreshnessResult(False, "content_hash_mismatch_stale_local_text")

    return FreshnessResult(True, "content_matches")
