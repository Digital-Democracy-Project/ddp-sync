# Sign-off: RDS OpenStates routing deployed, verified end-to-end, three bugs found and fixed

**Date:** 2026-09-16 (evening/night session)
**Branch:** `feat/rds-openstates-routing-standalone`, currently at commit `e909339`, **live on
the votebot/ddp-api EC2 host right now**.
**Audience:** Ramon / whoever picks this thread up next.

## Where this thread started and where it ended

Started from `point-votebot-ec2-at-production-api-v3-20260915.md`'s plan to pull this host to
`main`. Ended somewhere different, for good reason (see the intermediate notes in this branch
for the full reasoning): a standalone branch reimplementing just the OpenStates-routing
capability this host needs, explicitly *not* a stopgap to reconcile with `main` later, given
this host's confirmed retirement (~Dec 2026, Webflow removal) and the ddp-broker-py Flow 2
dependency `main` would otherwise have introduced with zero benefit to this host.

**This is deployed and live right now.** Checked out on the host, `pip install .`'d, restarted,
verified.

## What's actually running

- `rds_openstates_api_base=http://10.0.0.11:8002` + `ddp_openstates_jurisdictions=["FL","WA",
  "US","VA","MI","MA","UT","AZ","NC"]` in Secrets Manager (both confirmed set correctly, after
  catching and fixing a key-name typo along the way — see `correction-local-vs-rds-openstates-*`
  and follow-ups).
- `rds_openstates_api_key` reused as-is (already existed, already confirmed working) — no new
  key was ever needed.
- `bill_sync.py`, `legislator_sync.py`, `openstates_people.py`, `federal_legislator_cache.py`,
  `ingestion/sources/openstates.py` all route the 9 listed jurisdictions to the RDS replica,
  everything else to the public API, exactly as designed.
- This host's Flow 2 (Pinecone bill-version tracking) is **unchanged** — still local Redis, zero
  ddp-broker-py dependency, by design, permanently, per the retirement decision.
- 6 OpenStates scrape/patch jobs (FL/WA/USA scrape, secondary states, people refresh, apply
  local patches) are now **disabled** (`openstates_scrape.enabled: false` in
  `sync_schedule.yaml`) — these had been silently failing every single day since this host's
  original `6a97206` pin (2026-07-02), unrelated to anything in this thread, just never noticed
  until someone asked how many scheduler jobs were registered.

## Bugs found and fixed during deploy (all same night, all fixed before/immediately after
## going live)

1. **API key leak, `bill_sync.py`'s own logging** (`ef55b8e`) — logged the full response URL
   including the RDS replica's query-param `apikey=`. Caught before any real scheduled job hit
   the RDS path; zero production log exposure from this one.
2. **API key leak, httpx's own built-in request logger** (`e909339`) — a real Zapier-triggered
   production request (HR6509) actually logged the key in cleartext into `journalctl` via
   httpx's `logger.info('HTTP Request: %s %s ...', ...)`, independent of fix #1 (httpx passes
   `request.url` as an object, not a string, so a naive redaction filter would have silently
   missed it — caught that in local testing before shipping the fix). Confirmed via
   CloudWatch-agent-config and rsyslog-config inspection that this host doesn't ship logs
   anywhere -- the one real exposure stayed local to this box, at the same access level as
   Secrets Manager itself. No key rotation performed (judgment call — flagging in case anyone
   wants to reconsider).
3. **6 silently-failing OpenStates scrape jobs** (`1a6622a`) — see above. Unrelated to this
   thread's original goal, found by accident, fixed with a one-line config change.

## Verified end-to-end with a real production request

You (Ramon) triggered a real Zapier call for **HR6509** (SAFE Drugs Act of 2025) mid-session.
Full trace confirmed: bill metadata correctly routed through the RDS replica
(`jurisdiction=us` → `10.0.0.11:8002`, real `200 OK`), organization mapping built (51 paginated
Webflow calls), bill embedded and upserted to Pinecone (`bill-webflow-*`), bill text PDF fetched
and embedded (`bill-pdf-*`) — **from `govinfo.gov`, the public site, not RDS/S3** (a live,
concrete confirmation of the SYNC-67 gap already filed), 4 chunks total, 12.19s, full success.

## Open items, unchanged from earlier notes

- **SYNC-67** (Jira): bill document text still live-fetches from the public legislative site;
  RDS routing only covers metadata. Not blocking, not urgent, already ticketed.
- The retirement + Pinecone-into-ddp-broker-EC2 consolidation plan itself still needs its own
  tracked ticket/plan doc before December — flagged again here since it hasn't been created yet
  as far as this thread knows.
- Whether to rotate `rds_openstates_api_key` given the confirmed local-only journalctl exposure
  above — left as a judgment call, not resolved.

## Bottom line

Deploy is live, tested against real production traffic, and working correctly. Nothing left
mid-flight. This thread can close unless something new comes up.
