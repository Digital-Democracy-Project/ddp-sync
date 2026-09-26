# Please check this instance directly: is it the source of Sept 20's 30K public OpenStates API calls?

**Ask, for whoever's running as the prod agent on the votebot/ddp-api EC2 instance**
(`i-09381338adcbb35e2`, VPC IP `172.31.81.51`, WireGuard mesh IP `10.0.0.1`): Ramon saw 30,000+
unexpected calls to the public OpenStates API (`v3.openstates.org`) on the OpenStates account
dashboard for Sunday, 2026-09-20. This is the ground truth that kicked off the investigation --
nothing below has independently re-derived that number, it's the real external signal being
chased.

Full investigation thread is on **`ddp-broker-py`'s** `notes/ops-handoff` branch (this repo's own
handoff channel doesn't have it -- cross-repo investigation, started from the broker side). Key
notes there, newest first: `30k-calls-focus-votebot-ddp-api-20260926.md`,
`30k-calls-narrowed-to-votebot-ddp-api-instance-20260926.md`,
`legislator-bio-sync-correction-scheduled-job-ruled-out-20260926.md`. Short version of where it
stands:

## Ruled out already, with direct evidence

1. `ddp-broker-py`'s own Voatz-triggered fetch pipeline -- only 7 real outbound calls the whole
   day (checked its intact logs).
2. The EC2-broker `ddp-sync` instance (the one co-located with `ddp-broker-py`) -- all its
   Webflow-CMS jobs (`legislator_bio_sync`, `legislator_sync`, `bill_sync`, `organization_sync`,
   `voatz_sync`) are disabled there by design, confirmed via `git log -S` (flag + enforcing
   scheduler gate both predate the build that was live on 2026-09-20) and `journalctl`. An
   earlier pass in this thread also mis-flagged 32,322 log lines mentioning "openstates.org" as
   evidence of real traffic -- those were debug-level URL-string-parsing lines with zero network
   calls behind them. Worth knowing about since it's an easy trap to repeat: a text-search hit on
   a URL substring in logs is not proof of an actual outbound request.
3. The Mac Studio instance -- reachable and healthy, but only 3 scheduled jobs total. Not
   plausible as the source of a Webflow-wide sweep, though not checked with hard evidence either.

## Why this instance is the leading candidate

Per this repo's own `CLAUDE.md` ("ddp-sync runs as three independent instances" section), this
instance's whole purpose is keeping the (deprecated) Webflow CMS current across *every*
jurisdiction -- exactly `legislator_bio_sync`'s `jurisdiction: null` scope, and the only one of
the three instances not limited to the 7 DDP-tracked jurisdictions. A curl from the broker side
found this instance's `/health` reachable and healthy over the WireGuard mesh, reporting 14
scheduled jobs -- but that's all that could be checked without shell access. **Nothing here is
confirmed yet -- this is elimination + architecture, not observed traffic.**

Also worth flagging explicitly, since it matters for how you check this: per `CLAUDE.md`, this
instance runs its own **permanently-diverged branch** (`feat/rds-openstates-routing-standalone`),
not `main`. Its actual `legislator_bio_sync` scheduling config, env-flag names, and gating logic
may not match what's in `main`'s `sync_schedule.yaml`/`scheduler.py` at all -- don't assume the
`main`-branch code the rest of this investigation has been reading applies here. Check what's
actually deployed on this box.

## Specific things to check, with direct host access

1. **Real outbound request volume on 2026-09-20.** `docker logs` (or wherever this instance's
   `ddp-sync` output goes) for that date, filtered for actual `v3.openstates.org` GET/POST calls
   -- not a substring count on log lines (see the false-positive above). If the logs don't go
   back that far, note that explicitly rather than reporting an absence as a negative.
2. **Is the equivalent of `LEGISLATOR_BIO_SYNC_ENABLED` set here, and to what?** Check this
   instance's actual `.env` / container environment / Secrets Manager config -- whatever this
   diverged branch actually calls that flag, if it still has one. Architecturally this is the one
   instance where it's *expected* to be enabled (that's this instance's whole job), so "enabled"
   here wouldn't itself be a bug -- just confirms or denies it as the source.
3. **Its real scheduler config**: `sync_day`, `jurisdiction` scope, `historical_since`, whatever
   this branch's own config file calls them -- confirm it matches (or doesn't) the
   `jurisdiction: null`, `sync_day: sunday` shape `main`'s YAML has, since this branch may have
   its own copy that's drifted.
4. **Did it actually run on Sept 20 specifically** -- a completed-run log entry, a metrics/status
   endpoint showing last-run timestamps (the `/health` response's `flows` section on this
   instance showed a `daily_bill_sync` completion timestamp when checked from the broker side --
   there may be an equivalent for the bio/Webflow sync worth checking the same way), or anything
   else that pins the date rather than just confirming the job exists and runs weekly in general.

Whatever you find, please reply on **`ddp-broker-py`'s** `notes/ops-handoff` branch (where the
rest of this thread lives), not this one -- that's where Ramon and the other prod agent are
tracking it.
