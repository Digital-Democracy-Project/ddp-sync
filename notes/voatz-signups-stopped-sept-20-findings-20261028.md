# Finding: Voatz has returned no new signups since 2026-09-20 (ops session, 2026-10-28)

From the ddp-api session (Claude Code, on the ddp-api/ddp-sync host), investigating the report that the Voatz -> Brevo process "hasn't updated any new users since Sept 20th". **Record only. No build is requested.** The cause looks to be on the Voatz or signup-flow side, which this host cannot fix. Nothing was changed, restarted or deployed. No emails or other personal data are quoted below.

## Short answer

**The Voatz -> Brevo sync is working. Voatz has stopped returning new signups.** The newest signup Voatz returns for any org is **2026-09-20 ~10:49 UTC**. Brevo matches Voatz in every org, so there is nothing for the sync to add.

## 1. The sync job is healthy

- The job is ddp-sync's `voatz_brevo` pipeline (`src/ddp_sync/pipelines/voatz_brevo.py`), scheduled every 30 minutes, running in `ddp-sync.service` on this host. ddp-api only proxies `/trigger/user-sync`; it holds no sync code.
- Journal, 2026-09-26 through 2026-10-28 (the journal does not go back further): every run completes for all 9 orgs. Diff is "0 to add" on every run.
- Voatz user counts are frozen. Example per-day counts: Utah 6017, Massachusetts 4520 (unchanged the whole period); Federal 23753 on Sept 26, now 23751. Federal's drop is three single-contact removals (Oct 16, Oct 25, Oct 28), not growth.
- Only two failed runs, both single-org and self-recovered on the next run: Federal on Sept 27 (Brevo 503) and Massachusetts on Oct 19 (Brevo read timeout).
- `voatz_brevo.py` has no commits since at least 2026-09-01. Pagination (`limit 1000` plus `minId`) looks correct.

## 2. What Voatz returns (the evidence for "no new signups")

Read-only fetch of the same Voatz endpoints the job uses (section 5), newest `timestamp` per org:

| Org | Newest signup (UTC) | Signups since Sept 10 |
|---|---|---|
| Federal | 2026-09-20 10:49 | 3, all on Sept 20 |
| Florida | 2026-09-20 10:49 | 3, the same three |
| Arizona | 2026-04-10 | 0 |
| Massachusetts | 2026-05-19 | 0 |
| Michigan | 2026-04-27 | 0 |
| Nevada | 2026-07-26 | 0 |
| Utah | 2026-04-13 | 0 |
| Virginia | 2026-08-22 | 0 |
| Washington | 2026-04-06 | 0 |

- Fetched 2026-10-28. User counts matched the journal (Federal 23750, Florida 1893, Utah 6017, etc.).
- The Federal and Florida signups share a timestamp, so one signup flow wrote to both orgs.

## 3. Does it apply to all jurisdictions?

**Not established.** Only Federal and Florida show activity after Sept 10, and nothing after Sept 20. The other seven orgs were already silent for months before Sept 20 (Apr to Aug). So Sept 20 is the stopping point for Federal/Florida only; the others were either already broken, or just low-volume.

**Not yet checked:** the signup rate before Sept 20. A per-month, per-org count of signups for 2026 would show whether five quiet weeks is an outage or a normal lull for Federal/Florida, and when each of the other orgs actually went quiet. This is a counts-only read of the same endpoint; it was offered but not run.

## 4. Deploys near Sept 20

None on this host.

| Repo | Commits Sept 18 to 25 | Last commit before |
|---|---|---|
| ddp-api | none | Sept 17 (docs wording) |
| ddp-sync | none | Sept 17 (OpenStates routing, log redaction, dry-run script) |
| votebot | none | Sept 15 (OpenStates routing) |
| FillWebflowFields | none | Feb 2026 |

- The Sept 15 to 17 commits are OpenStates routing, log redaction and docs. None touch Voatz or signups.
- Service restarts in the journal: ddp-sync Sept 27 01:52 and 18:48 (picked up legislator-sync fixes SYNC-76/77/78 only); ddp-api Oct 6 00:38, Oct 7 18:32 and Oct 7 20:56. The journal starts Sept 26, so Sept 18 to 25 restarts are not visible; the git history is the evidence for that window.
- Signups had already stopped before the first of these restarts, so none can be the cause.
- **Not checked:** ddp-next (the signup form and front end), which is not on this host, and anything Voatz changed on their side.

## 5. Exactly what was called (this session, 2026-10-28, ~20:50 to 21:20 UTC)

Two throwaway scripts in the session scratchpad, run once each using ddp-sync's own production Voatz credentials and its own functions:

- `POST https://vapi-vrb.nimsim.com/voatz/organizations/users/login` via `get_voatz_tokens()`. This creates a session on Voatz's side, so it is the one call that is not purely a read.
- `POST https://vapi-vrb.nimsim.com/voatz/customers/delegate/signups/byorg` via `fetch_voatz_users()`, with `{"organizationId": <id>, "limit": 1000}` and `minId` paging. It only reads.

The first script covered Federal, Florida and Utah; the second covered all nine orgs. No other Voatz endpoint was called, nothing was written to Voatz or Brevo, no ddp-api route was used, and only counts and timestamps were printed.

## 6. Likely causes, not yet distinguished

1. The signup flow feeding Voatz is broken (ddp-next signup form, a Voatz integration, or an API credential), so new signups never reach Voatz.
2. Voatz changed or broke the `signups/byorg` endpoint so it no longer returns recent records.
3. A data or processing delay on Voatz's side.

## 7. Suggested next checks (none done, none requiring a build)

- Ask whoever manages Voatz, or check the Voatz admin dashboard, whether signups after Sept 20 exist there. Present in Voatz but missing from `byorg` points to cause 2; absent in Voatz points to cause 1.
- Test one signup end to end with a throwaway account.
- Check whether the ddp-next signup flow, or any Voatz-side integration, changed around Sept 20.
- Optionally run the counts-only per-month breakdown in section 3.
- Voatz may offer another endpoint that shows newer signups; worth asking them.
