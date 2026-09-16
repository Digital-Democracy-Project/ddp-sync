# Found + fixed: 6 OpenStates scrape/patch jobs have been failing daily since this host's pin

**Date:** 2026-09-16
**Branch:** `feat/rds-openstates-routing-standalone`, fix at commit `1a6622a`.
**Audience:** anyone touching this host's scheduler config, or archaeology later.

## What was found

Asked "how many jobs were registered" after a routine restart -- 20, more than the ~14 this host
actually needs. `journalctl` showed 6 of them (`OpenStates: apply local patches`, `FL scrape`,
`WA scrape`, `USA scrape`, `secondary states`, `people refresh`) have been **failing every single
day** since this host's `6a97206` pin (2026-07-02), always within the same second they start:

```
openstates_patch_refresh: error error="[Errno 2] No such file or directory: '/Users/agentsmith/Developer/repos/ddp-open-states'"
openstates_scrape: failed returncode=127 stderr_tail='/bin/bash: /Users/agentsmith/Developer/repos/ddp-open-states/run-scrape.sh: No such file or directory'
```

`/Users/agentsmith/...` is a Mac Studio path -- these jobs assume a local `openstates_root`/
CAMS/LegBot environment this EC2 host has never had. Each job catches its own error and still
reports `"executed successfully"` to APScheduler -- the same silent-failure shape this repo's own
`TROUBLESHOOTING.md` already documents for a previously-removed job ("Hourly Content Update
Removed"). Practically harmless (fails in ~0 seconds, no resource cost) but pure log noise that's
been accumulating unnoticed for ~2.5 months.

## Fix

One-line config change, no code touched: `config/sync_schedule.yaml`'s `openstates_scrape.enabled`
flipped `true` -> `false`. `scheduler.py`'s `_register_openstates_scrape_jobs()` already had an
early-return gate on this exact flag (`if not config.get("enabled", False): return`) -- this
cleanly skips all 6 job registrations, confirmed via `journalctl`:
`openstates_scrape: disabled in config — skipping`. Job count dropped 20 -> 14, matching exactly
this host's real work (Daily Bill Sync, Weekly Legislator Sync, Weekly Legislator Bio Sync,
Monthly Organization Sync, 2x Voatz/Brevo, 6x Webflow batch, Weekly VoteBot eval).

Since `config/sync_schedule.yaml` lives in the repo itself (not Secrets Manager) and this branch
only ever deploys to this one host, this change has zero effect on the Mac Studio or ddp-broker
EC2 instances, which run different branches of the same repo.
