# Considered and rejected: auto-detecting RDS jurisdiction coverage instead of a static list

**Date:** 2026-09-16
**Audience:** whoever next onboards a jurisdiction to the RDS replica, or revisits this decision.

## The ask

Avoid needing to update `ddp_openstates_jurisdictions` in Secrets Manager every time a new
jurisdiction is onboarded to the RDS-backed replica -- have the routing auto-detect coverage
instead of relying on a maintained list.

## What was tested

Queried `http://10.0.0.11:8002` directly for a jurisdiction NOT in the current list (California)
and compared against a known-loaded one (Florida):

| Jurisdiction | `/jurisdictions/{code}` | `/bills?jurisdiction=` | `latest_bill_update` |
|---|---|---|---|
| FL (loaded) | `200`, real metadata | `200`, real results | `2026-09-13...` (fresh) |
| CA (not loaded) | `200`, real metadata | `200`, **empty** (`total_items: 0`) | `2021-01-01T00:00:00` (placeholder-looking) |

## Why a naive auto-fallback doesn't work

Querying an unloaded jurisdiction doesn't error or 404 -- it returns a valid `200` with an empty
bill list. That's indistinguishable at the HTTP level from "this jurisdiction is loaded but has
no bills matching the current filter" (a real, legitimate case). A naive "try RDS, fall back to
public API on empty results" would silently return **zero bills instead of real public-API
data** for any not-yet-onboarded jurisdiction -- strictly worse than today, not better.

`latest_bill_update`'s staleness looked like it might be a usable signal (a suspicious round
`2021-01-01T00:00:00` for CA vs. a genuinely fresh timestamp for FL), but this was only checked
against 2 data points -- not confirmed as a guaranteed convention of the RDS-loading pipeline,
and would need sign-off from whoever owns that pipeline before being trusted for routing
decisions. Also would add caching/TTL complexity to avoid an extra HTTP call per bill fetch.

## Decision

**Keep the static list.** Simplicity and correctness (never silently substituting empty results
for real data) outweigh the maintenance cost of updating `ddp_openstates_jurisdictions` by hand.
This mirrors the same jurisdiction-list convention this repo's SYNC-6 already used to keep in
sync with ddp-broker-py's own `DDP_OPENSTATES_JURISDICTIONS`.

**Runbook reminder for onboarding a new jurisdiction to RDS:** once RDS actually has real,
current data for it, add the two-letter code to `ddp_openstates_jurisdictions` in this host's
Secrets Manager secret (`ddp-sync/credentials`). No code or restart-flag changes needed beyond
that -- the routing helpers already read this list live via `get_settings()`.
